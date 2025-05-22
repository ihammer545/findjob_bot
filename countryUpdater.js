import axios from 'axios'

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
  let gptCalls = 0

  try {
    // 1. Получение строк
    const fetchResponse = await axios.post(`${API_URL}/rows/find`, {
      filter: {
        id: { $in: [4085, 4086, 4087] }
      },
      limit: 10
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

      if (!city) {
        rowsToDelete.push(rowId)
        results.push(`🗑️ Marked row ${rowId} for deletion (City is null)`)
        continue
      }

      gptCalls++
      let gptCountry = ''
      let gptCity = ''

     try {
  // 1. Получаем страну
  const gptCountryResp = await axios.post(
    'https://api.openai.com/v1/chat/completions',
    {
      model: 'gpt-4o-mini',
      temperature: 0,
      max_tokens: 10,
      messages: [
        {
          role: 'system',
          content: 'You are a helpful assistant. Given a city name or place, respond strictly with the name of the country it belongs to in English. Only return the country name (e.g. "Austria").'
        },
        {
          role: 'user',
          content: `Which country does '${city}' belong to?`
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
  gptCountry = gptCountryResp.data?.choices?.[0]?.message?.content?.trim()

  // 2. Получаем английское название города
  const gptCityResp = await axios.post(
    'https://api.openai.com/v1/chat/completions',
    {
      model: 'gpt-4o-mini',
      temperature: 0,
      max_tokens: 20,
      messages: [
        {
          role: 'system',
          content: 'Return the English name of the given city or place. Only return the city name (e.g. "Vienna").'
        },
        {
          role: 'user',
          content: `What is the English name for '${city}'?`
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
  gptCity = gptCityResp.data?.choices?.[0]?.message?.content?.trim()

} catch (gptErr) {
  console.error(`❌ GPT API error for city '${city}':`, gptErr.response?.data || gptErr.message)
  results.push(`❌ GPT error for '${city}': ${gptErr.message}`)
  rowsToDelete.push(rowId)
  continue
}

      if (!gptCountry || gptCountry.toLowerCase().includes('invalid') || gptCountry.toLowerCase() === 'unknown') {
        results.push(`❌ Could not determine country for '${city}'`)
        rowsToDelete.push(rowId)
        continue
      }

     const updatedRow = {
  id: rowId,
  Country: gptCountry
}

// Защита от мусора в ответе GPT по городу
if (gptCity && !/unknown|don\'?t know|invalid|not sure/i.test(gptCity)) {
  updatedRow.City = gptCity
} else {
  results.push(`⚠️ GPT вернул сомнительное значение города для '${city}': '${gptCity}' — не обновляем поле City`)
}


      

      rowsToUpdate.push(updatedRow)
      results.push(`📝 Prepared row ${rowId} update: ${JSON.stringify(updatedRow)}`)
    }

    // 2. Обновление строк
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

    // 3. Удаление строк
    if (rowsToDelete.length > 0) {
      try {
        const deleteUrl = `${API_URL}/rows/delete`  // исправленный путь
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
