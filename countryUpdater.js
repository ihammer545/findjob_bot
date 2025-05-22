const axios = require('axios')

async function updateCountries() {
  const API_URL = process.env.BOTPRESS_API_URL       // https://api.botpress.cloud/v1/tables/TicketsTable/rows
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

  // 1. Получаем строки
  const fetchResponse = await axios.post(`${API_URL}/find`, {
    filter: {
      id: { $in: [4085, 4086, 4087] }
    },
    limit: 10
  }, { headers: HEADERS })

  const rows = fetchResponse.data.rows
  console.log(`✅ Данные из Botpress получены: ${rows.length} строк(и)`)

  const rowsToUpdate = []
  const rowsToDelete = []
  let gptCalls = 0

  for (const row of rows) {
    const rowId = row.id
    const city = row.City?.trim()

    if (!city) {
      rowsToDelete.push(rowId)
      results.push(`🗑️ Marked row ${rowId} for deletion (City is null)`)
      continue
    }

    // GPT-запрос
    gptCalls++

    const gptResp = await axios.post(
      'https://api.openai.com/v1/chat/completions',
      {
        model: 'gpt-4o-mini',
        temperature: 0,
        max_tokens: 10,
        messages: [
          {
            role: 'system',
            content: 'You are a helpful assistant. Given a city name (or a country name in any language), respond strictly with the name of the country it belongs to in English. Only return the country name (e.g. "Austria", "Ukraine").'
          },
          {
            role: 'user',
            content: `Which country does the city or place '${city}' belong to?`
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

    const gptCountry = gptResp.data?.choices?.[0]?.message?.content?.trim()

    if (!gptCountry || gptCountry.toLowerCase() === 'unknown') {
      results.push(`❌ Could not determine country for '${city}'`)
      continue
    }

    const updatedRow = {
      id: rowId,
      Country: gptCountry
    }

    if (city.toLowerCase() === gptCountry.toLowerCase()) {
      updatedRow.City = gptCountry
    }

    rowsToUpdate.push(updatedRow)
    results.push(`📝 Prepared row ${rowId} update: ${JSON.stringify(updatedRow)}`)
  }

  // 2. Обновление
  if (rowsToUpdate.length > 0) {
    const updateResp = await axios.put(API_URL, { rows: rowsToUpdate }, { headers: HEADERS })
    results.push(`✅ Updated ${rowsToUpdate.length} rows.`)
    console.log(`✅ Записей обновлено: ${rowsToUpdate.length}`)

    if (updateResp.data?.errors?.length) {
      results.push(`⚠️ Ошибки при обновлении:\n${JSON.stringify(updateResp.data.errors)}`)
    }
  } else {
    console.log(`⚠️ Обновлять нечего.`)
  }

  // 3. Удаление
  if (rowsToDelete.length > 0) {
    await axios.post(`${API_URL}/delete`, { ids: rowsToDelete }, { headers: HEADERS })
    results.push(`🗑️ Deleted ${rowsToDelete.length} rows.`)
    console.log(`🗑️ Удалено записей: ${rowsToDelete.length}`)
  } else {
    console.log(`ℹ️ Нет записей для удаления.`)
  }

  console.log(`📤 Всего GPT-вызовов: ${gptCalls}`)

  return results
}


export default updateCountries;

