import axios from 'axios'

const MAX_RPM = 400
const DELAY = Math.ceil(60000 / MAX_RPM)
const BATCH_SIZE = 50

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

async function callGPTWithRetry(rowId, requirements) {
  const payload = {
    model: 'gpt-4o-mini',
    temperature: 0,
    max_tokens: 150,
    messages: [
      {
        role: 'system',
        content: `You are a data extractor for job listings. Based on the input, extract the following:

- City: Determine the city of the job location based on the text. If unsure, return "null".
- Country: Determine the country of the job location using the city name if provided, or the full text if not. Answer only if you are confident, otherwise return "null".
- Region: Determine the region/state/voivodeship/land/province the city belongs to, in English. If unsure, return "null".
- Phone number: Extract the phone number from the text. Return only digits, no symbols or spaces. If missing, return "null".

Respond strictly in this JSON format:
{
  "City": "...",
  "Country": "...",
  "Region": "...",
  "Phone number": "..."
}`
      },
      {
        role: 'user',
        content: `Text: ${requirements}`
      }
    ]
  }

  const config = {
    headers: {
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
      'Content-Type': 'application/json'
    },
    timeout: 15000
  }

  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      console.log(`🤖 GPT запрос (попытка ${attempt}) для row ${rowId}`)
      const response = await axios.post('https://api.openai.com/v1/chat/completions', payload, config)
      return response
    } catch (err) {
      if (attempt === 2) throw err
      console.warn(`⚠️ GPT попытка ${attempt} не удалась для row ${rowId}, повтор...`)
    }
  }
}

async function updateCountries() {
  const API_URL = process.env.BOTPRESS_API_URL
  const BOT_ID = process.env.BOTPRESS_BOT_ID
  const WORKSPACE_ID = process.env.BOTPRESS_WORKSPACE_ID
  const BP_TOKEN = process.env.BOTPRESS_API_TOKEN

  const HEADERS = {
    Authorization: `Bearer ${BP_TOKEN}`,
    'x-bot-id': BOT_ID,
    'x-workspace-id': WORKSPACE_ID,
    'Content-Type': 'application/json'
  }

  const results = []
  let gptCalls = 0
  const rowsToRetry = []
  let replacedCityCount = 0
  let filledEmptyCityCount = 0
  let unknownCityCount = 0

  const isValidField = val => val && !/null|unknown|not sure|don't know|invalid|n\/a/i.test(val)

  try {
    let allRows = []
    const pageSize = 1000
    let page = 0

    while (true) {
      const fetchResponse = await axios.post(`${API_URL}/rows/find`, {
        limit: pageSize,
        offset: page * pageSize
      }, { headers: HEADERS })

      const rows = fetchResponse?.data?.rows
      if (!Array.isArray(rows) || rows.length === 0) break

      allRows = allRows.concat(rows)
      console.log(`📥 Загружено ${rows.length} строк (offset ${page * pageSize})`)
      page++
    }

    const rowsToDelete = []
    let batchRows = []

    for (let i = 0; i < allRows.length; i++) {
      const row = allRows[i]
      const rowId = row.id
      const cityField = row.City?.trim()
      const requirements = row.Requirements?.trim()

      console.log(`➡️ Обработка строки ${i + 1} из ${allRows.length} (ID: ${rowId})`)

      if (!cityField && !requirements) {
        rowsToDelete.push(rowId)
        results.push(`🗑️ Marked row ${rowId} for deletion (No City or Requirements)`)
        continue
      }

      gptCalls++
      await sleep(DELAY)

      let Country = '', Region = '', PhoneNumber = '', DetectedCity = ''
      let gptResponse

      try {
        gptResponse = await callGPTWithRetry(rowId, requirements)
      } catch (gptErr) {
        const errorMsg = gptErr.code === 'ECONNABORTED'
          ? `⏱️ Таймаут GPT для row ${rowId}`
          : `❌ GPT error for row ${rowId}: ${gptErr.message}`

        console.error(errorMsg)
        results.push(errorMsg)
        rowsToRetry.push(row)
        continue
      }

      let parsed = {}
      try {
        parsed = JSON.parse(gptResponse.data?.choices?.[0]?.message?.content)
      } catch (parseErr) {
        console.error('❌ JSON parse error:', parseErr)
        results.push(`❌ JSON parse error in row ${rowId}`)
        rowsToRetry.push(row)
        continue
      }

      DetectedCity = parsed?.City?.trim()
      Country = parsed?.Country?.trim()
      Region = parsed?.Region?.trim()
      PhoneNumber = parsed?.['Phone number']?.trim()

      if (!isValidField(Country)) {
        results.push(`❌ Could not determine country for row ${rowId}`)
        rowsToRetry.push(row)
        continue
      }

      const updatedRow = { id: rowId, Country }

      if (isValidField(Region)) {
        updatedRow.Region = Region
      } else {
        results.push(`⚠️ Не обновляем Region для row ${rowId} — значение сомнительно: '${Region}'`)
      }

      if (isValidField(PhoneNumber)) {
        updatedRow['Phone number'] = PhoneNumber
      } else {
        results.push(`⚠️ Не обновляем PhoneNumber для row ${rowId} — значение сомнительно: '${PhoneNumber}'`)
      }

      if (isValidField(DetectedCity)) {
        if (!cityField) {
          updatedRow.City = DetectedCity
          filledEmptyCityCount++
          results.push(`✅ Записали City для row ${rowId} → '${DetectedCity}' (раньше было пусто)`)
        } else if (cityField.toLowerCase() !== DetectedCity.toLowerCase()) {
          updatedRow.City = DetectedCity
          replacedCityCount++
          results.push(`🔁 Заменили City в row ${rowId}: было '${cityField}', стало '${DetectedCity}'`)
        }
      } else {
        unknownCityCount++
        results.push(`⚠️ GPT не смог определить город для row ${rowId}`)
      }

      batchRows.push(updatedRow)

      if (batchRows.length === BATCH_SIZE) {
        try {
          console.log(`⬆️ Отправка батча в Botpress (${BATCH_SIZE} строк)...`)
          await axios.put(`${API_URL}/rows`, { rows: batchRows }, { headers: HEADERS })
          console.log(`✅ Обновлено строк в батче: ${BATCH_SIZE}`)
        } catch (err) {
          console.error(`❌ Ошибка при обновлении батча:`, err.response?.data || err.message)
          results.push(`❌ Ошибка батча: ${err.message}`)
        }
        batchRows = []
      }
    }

    if (batchRows.length > 0) {
      try {
        console.log(`⬆️ Отправка последнего батча в Botpress (${batchRows.length} строк)...`)
        await axios.put(`${API_URL}/rows`, { rows: batchRows }, { headers: HEADERS })
        console.log(`✅ Обновлены остаточные строки: ${batchRows.length}`)
      } catch (err) {
        console.error(`❌ Ошибка при обновлении остатка:`, err.response?.data || err.message)
        results.push(`❌ Ошибка остатка: ${err.message}`)
      }
    }

    if (rowsToDelete.length > 0) {
      try {
        await axios.post(`${API_URL}/rows/delete`, { ids: rowsToDelete }, { headers: HEADERS })
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
    console.log(`📊 Статистика по городам:`)
    console.log(`   🔁 Заменено городов: ${replacedCityCount}`)
    console.log(`   🆕 Заполнено пустых: ${filledEmptyCityCount}`)
    console.log(`   ❓ Не удалось определить: ${unknownCityCount}`)
    console.log(`   🔁 В rowsToRetry: ${rowsToRetry.length}`)

    return results
  } catch (err) {
    console.error('❌ Unexpected error in updateCountries:', err)
    results.push(`❌ Unexpected error: ${err.message}`)
    return results
  }
}

export default updateCountries
