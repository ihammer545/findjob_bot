// Обновлённый updateCountries с фильтрацией по дате публикации

import axios from 'axios'

const MAX_RPM = 200
const DELAY = Math.ceil(60000 / MAX_RPM)
const BATCH_SIZE = 50
const FORCE_FLUSH_INTERVAL = 300 // строк без сброса батча

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

async function callGPTWithRetry(rowId, requirements) {
  const payload = {
    model: 'gpt-5-mini',
    temperature: 0,
    max_tokens: 150,
    messages: [
      {
        role: 'system',
        content: `You are a a strict data extractor for job listings. Based on the input, extract the following:

- City: Return only the name of a city, town, or village where the job takes place. Do not return the name of a country. Use English names only. If you cannot find a specific city, return "null". Examples of valid cities: Vienna, Berlin, Warsaw. Examples of invalid answers: Germany, Austria, Poland.
- Country: Determine the country of the job location using the city name if provided, or the full text if not. Return the name in English only. Answer only if you are confident, otherwise return "null".
- Region: Determine the region/state/voivodeship/land/province the city belongs to. Return the name in English only. If unsure, return "null".
- Phone number: Extract the phone number from the text. Return only digits, no symbols or spaces. If missing, return "null".
Be strict and avoid guessing. If information is missing or ambiguous, return "null".

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

  for (let attempt = 1; attempt <= 2; attempt++) {
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), 15000)

    try {
   //   console.log(`🤖 GPT запрос (попытка ${attempt}) для row ${rowId}`)
      const response = await fetch('https://api.openai.com/v1/chat/completions', {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload),
        signal: controller.signal
      })

      clearTimeout(timeout)
      const data = await response.json()
      return data
    } catch (err) {
      clearTimeout(timeout)
      if (err.name === 'AbortError') {
        console.error(`⏱️ GPT timeout (AbortError) for row ${rowId}`)
      }
      if (attempt === 2) throw err
      console.warn(`⚠️ GPT попытка ${attempt} не удалась для row ${rowId}, повтор...`)
    }
  }
}

async function updateCountries(targetDate, alldates = false, singleId = null) {
  const isAllDates = alldates === true || alldates === 'true';
  const isDebugMode = !!singleId;
  let filterObject = {};

  const API_URL = process.env.BOTPRESS_API_URL;
  const BOT_ID = process.env.BOTPRESS_BOT_ID;
  const WORKSPACE_ID = process.env.BOTPRESS_WORKSPACE_ID;
  const BP_TOKEN = process.env.BOTPRESS_API_TOKEN;

  const HEADERS = {
    Authorization: `Bearer ${BP_TOKEN}`,
    'x-bot-id': BOT_ID,
    'x-workspace-id': WORKSPACE_ID,
    'Content-Type': 'application/json'
  };

  const pageSize = 100;
  const isValidField = val => val && !/null|unknown|not sure|don't know|invalid|n\/a/i.test(val);

  // 🔧 Встроенная функция логов отладки
  const logDebug = (...args) => {
    if (isDebugMode) {
      console.log(...args);
    }
  };

  let batchRows = [];
  let processed = 0;
  let lastRowId = null;
  let batchSinceFlush = 0;
  const failedRows = [];

  if (singleId) {
    filterObject = { id: Number(singleId) };
    console.log('🔍 Фильтр по ID:', filterObject);
  } else if (isAllDates) {
    filterObject = {};
  } else {
    if (!targetDate) {
      throw new Error('Date is required when alldates is false');
    }
    const dateFilter = new Date(targetDate).toISOString().split('T')[0];
    filterObject = {
      'Publish Date': {
        $gte: `${dateFilter}T00:00:00.000Z`,
        $lte: `${dateFilter}T23:59:59.999Z`
      }
    };
  }

  let intervalId = null;

  if (!isDebugMode) {
  intervalId = setInterval(() => {
    console.log(`🧭 Watchdog: Обработано ${processed} строк, Последняя rowId: ${lastRowId}`);
  }, 30000);
}

  try {
    let page = 0;
    while (true) {
      const fetchResponse = await axios.post(`${API_URL}/rows/find`, {
        limit: pageSize,
        offset: page * pageSize,
        filter: filterObject,
        orderBy: 'id',
        orderDirection: 'asc'
      }, { headers: HEADERS });

      const rows = fetchResponse?.data?.rows || [];
      if (rows.length === 0) break;

      for (let row of rows) {
        const rowId = row.id;
        lastRowId = rowId;
        processed++;
        batchSinceFlush++;

        const cityField = row.City?.trim();
        const requirements = row.Requirements?.trim();

        if (!cityField && !requirements) continue;

        await sleep(DELAY);

        let gptData;
        try {
          gptData = await callGPTWithRetry(rowId, requirements);
        } catch (err) {
          console.error(`❌ GPT error for row ${rowId}: ${err.message}`);
          failedRows.push(rowId);
          continue;
        }

        const content = gptData.choices?.[0]?.message?.content?.trim();

        logDebug(`📄 Обработка строки ID: ${rowId}`);
        logDebug(`🤖 GPT ответ для ID ${rowId}: ${content}`);

        if (!content?.startsWith('{')) {
          console.error(`❌ Invalid JSON from GPT in row ${rowId}`);
          failedRows.push(rowId);
          continue;
        }

        let parsed;
        try {
          parsed = JSON.parse(content);
        } catch (e) {
          console.error(`❌ JSON parse error for row ${rowId}`);
          failedRows.push(rowId);
          continue;
        }

        const DetectedCity = parsed?.City?.trim();
        const Country = parsed?.Country?.trim();
        const Region = parsed?.Region?.trim();
        const PhoneNumber = parsed?.['Phone number']?.trim();

        const updatedRow = { id: rowId };

        if (isValidField(Country)) {
          updatedRow.Country = Country;
        } else {
          logDebug(`⏭️ Страна не обновляется (GPT вернул null) для ID ${rowId}`);
        }

        updatedRow.Region = isValidField(Region) ? Region : 'null';
        updatedRow['Phone number'] = isValidField(PhoneNumber) ? PhoneNumber : 'null';

        if (!cityField || cityField.toLowerCase() !== DetectedCity?.toLowerCase()) {
          const cityClean = DetectedCity?.trim().toLowerCase();
          const countryClean = Country?.trim().toLowerCase();

          if (isValidField(DetectedCity) && cityClean !== countryClean) {
            updatedRow.City = DetectedCity;
          } else {
            updatedRow.City = 'null';
          }
        }

        logDebug(`📤 Обновляем в Botpress:`, updatedRow);

        batchRows.push(updatedRow);

        if (batchRows.length >= BATCH_SIZE || batchSinceFlush >= FORCE_FLUSH_INTERVAL) {
          try {
            console.log(`⬆️ Отправка батча (${batchRows.length} строк)...`);
            await axios.put(`${API_URL}/rows`, { rows: batchRows }, { headers: HEADERS });
            console.log(`✅ Обновлено: ${batchRows.length} строк`);
          } catch (err) {
            console.error(`❌ Ошибка при отправке батча:`, err.response?.data || err.message);
          }
          batchRows = [];
          batchSinceFlush = 0;
        }
      }

      page++;
    }

    if (batchRows.length > 0) {
      try {
        console.log(`⬆️ Отправка финального батча (${batchRows.length} строк)...`);
        await axios.put(`${API_URL}/rows`, { rows: batchRows }, { headers: HEADERS });
        console.log(`✅ Финальный батч обновлен`);
      } catch (err) {
        console.error(`❌ Ошибка финального батча:`, err.response?.data || err.message);
      }
    }

    if (failedRows.length > 0) {
      console.warn(`⚠️ Не обработано строк: ${failedRows.length}`);
      console.warn(failedRows);
    }

    if (intervalId) clearInterval(intervalId);
console.log(`🏁 Обработка завершена. Всего строк: ${processed}`);



  } catch (err) {
    console.error('❌ Unexpected error in updateCountries:', err);
  }
}


export default updateCountries
