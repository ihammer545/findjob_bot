import axios from 'axios'

const MAX_RPM = 240
const DELAY = Math.ceil(60000 / MAX_RPM)
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

async function updateCountries() {
  const API_URL = process.env.BOTPRESS_API_URL
  const BOT_ID = process.env.BOTPRESS_BOT_ID
  const WORKSPACE_ID = process.env.BOTPRESS_WORKSPACE_ID
  const BP_TOKEN = process.env.BOTPRESS_API_TOKEN
  const OPENAI_API_KEY = process.env.OPENAI_API_KEY

  const HEADERS = {
    Authorization: `Bearer ${BP_TOKEN}`,
    'x-bot-id': BOT_ID,
    'x-workspace-id': WORKSPACE_ID,
    'Content-Type': 'application/json'
  }

  const results = []
  let gptCalls = 0
  const rowsToRetry = []

  const isValidField = val => val && !/null|unknown|not sure|don't know|invalid|n\/a/i.test(val)

  try {
    const fetchResponse = await axios.post(`${API_URL}/rows/find`, {
      limit: 1000
    }, { headers: HEADERS })

    const rows = fetchResponse?.data?.rows
    if (!Array.isArray(rows)) {
      throw new Error('Botpress response format error: rows is not an array')
    }

    console.log(`✅ Данные из Botpress получены: ${rows.length} строк(и)`)

    const rowsToUpdate = []
    const rowsToDelete = []

    for (const row of rows) {
      const rowId = row.id
      const city = row.City?.trim()
      const requirements = row.Requirements?.trim()

      if (!city && !requirements) {
        rowsToDelete.push(rowId)
        results.push(`🗑️ Marked row ${rowId} for deletion (No City or Requirements)`)
        continue
      }

      gptCalls++
      await sleep(DELAY)

      let Country = ''
      let Region = ''
      let PhoneNumber = ''

      try {
        const gptUnifiedResp = await axios.post(
          'https://api.openai.com/v1/chat/completions',
          {
            model: 'gpt-4o-mini',
            temperature: 0,
            max_tokens: 150,
            messages: [
              {
                role: 'system',
                content: `You are a data extractor for job listings. Based on the input, extract the following:\n\n- Country: Determine the country of the job location using the city name if provided, or the full text if not. Answer only if you are confident, otherwise return \"null\".\n- Region: Determine the region/state/voivodeship/land/province the city belongs to, in English. If unsure, return \"null\".\n- Phone number: Extract the phone number from the text. Return only digits, no symbols or spaces. If missing, return \"null\".\n\nRespond strictly in this JSON format:\n{\n  \"Country\": \"...\",\n  \"Region\": \"...\",\n  \"Phone number\": \"...\"\n}`
              },
              {
                role: 'user',
                content: `City: '${city}'\nText: ${requirements}`
              }
            ]
          },
          {
            headers: {
              Authorization: `Bearer ${OPENAI_API_KEY}`,
              'Content-Type': 'application/json'
            }
          }
        )

        let parsedJson = {}
        try {
          parsedJson = JSON.parse(gptUnifiedResp.data?.choices?.[0]?.message?.content)
        } catch (parseErr) {
          console.error('❌ JSON parse error:', parseErr)
          results.push(`❌ JSON parse error in row ${rowId}`)
          rowsToRetry.push(row)
          continue
        }

        Country = parsedJson?.Country?.trim()
        Region = parsedJson?.Region?.trim()
        PhoneNumber = parsedJson?.['Phone number']?.trim()

      } catch (gptErr) {
        console.error(`❌ GPT API error for row ${rowId}:`, gptErr.response?.data || gptErr.message)
        results.push(`❌ GPT error for row ${rowId}: ${gptErr.message}`)
        rowsToRetry.push(row)
        continue
      }

      if (!isValidField(Country)) {
        results.push(`❌ Could not determine country for row ${rowId}`)
        rowsToRetry.push(row)
        continue
      }

      const updatedRow = { id: rowId, Country }

      if (isValidField(Region)) {
        updatedRow.Region = Region
      } else {
        results.push(`⚠️ GPT вернул сомнительное значение региона для row ${rowId}: '${Region}' — не обновляем поле Region`)
      }

      if (isValidField(PhoneNumber)) {
        updatedRow['Phone number'] = PhoneNumber
      } else {
        results.push(`⚠️ GPT не смог извлечь номер телефона для row ${rowId}: '${PhoneNumber}' — не обновляем поле Phone number`)
      }

      rowsToUpdate.push(updatedRow)
      results.push(`📝 Prepared row ${rowId} update: ${JSON.stringify(updatedRow)}`)
    }

    if (rowsToUpdate.length > 0) {
      try {
        const updateResp = await axios.put(`${API_URL}/rows`, { rows: rowsToUpdate }, { headers: HEADERS })
        console.log(`✅ Записей обновлено: ${rowsToUpdate.length}`)
        results.push(`✅ Updated ${rowsToUpdate.length} rows.`)

        if (updateResp.data?.errors?.length) {
          console.warn(`⚠️ Ошибки при обновлении:`, updateResp.data.errors)
          results.push(`⚠️ Ошибки при обновлении:\n${JSON.stringify(updateResp.data.errors)}`)
        }
      } catch (updateErr) {
        console.error(`❌ Ошибка при обновлении строк:`, updateErr.response?.data || updateErr.message)
        results.push(`❌ Update error: ${updateErr.message}`)
      }
    } else {
      console.log(`⚠️ Обновлять нечего.`)
    }

    if (rowsToDelete.length > 0) {
      try {
        const deleteUrl = `${API_URL}/rows/delete`
        await axios.post(deleteUrl, { ids: rowsToDelete }, { headers: HEADERS })
        console.log(`🗑️ Удалено записей: ${rowsToDelete.length}`)
        results.push(`🗑️ Deleted ${rowsToDelete.length} rows.`)
      } catch (deleteErr) {
        console.error(`❌ Ошибка при удалении строк:`, deleteErr.response?.data || deleteErr.message)
        results.push(`❌ Delete error: ${deleteErr.message}`)
      }
    } else {
      console.log(`ℹ️ Нет записей для удаления.`)
    }

    console.log(`📤 Всего GPT-вызовов: ${gptCalls}`)
    return results

  } catch (err) {
    console.error('❌ Unexpected error in updateCountries:', err)
    results.push(`❌ Unexpected error: ${err.message}`)
    return results
  }
}

export default updateCountries;
