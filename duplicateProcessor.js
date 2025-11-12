// duplicateProcessor.js
import fetch from 'node-fetch';

async function fetchAllTickets() {
  const allRows = [];
  let offset = 0;
  const limit = 1000;

  console.log('🔁 [1] Начинаем загрузку всех тикетов из Botpress...');

  try {
    while (true) {
      console.log(`📦 [1.${offset / limit + 1}] Загружаем партию offset=${offset}...`);

      const response = await fetch("https://api.botpress.cloud/v1/tables/TicketsTable/rows/find", {
        method: "POST",
        headers: {
          "Authorization": `bearer ${process.env.BOTPRESS_API_TOKEN}`,
          "x-bot-id": process.env.BOTPRESS_BOT_ID,
          "Content-Type": "application/json"
        },
        body: JSON.stringify({ limit, offset })
      });

      if (!response.ok) {
        const text = await response.text();
        console.error('❌ Ошибка от Botpress:', text);
        break;
      }

      const json = await response.json();
      const rows = json.rows || [];

      if (rows.length === 0) {
        console.log('✅ [1.X] Все записи загружены.');
        break;
      }

      allRows.push(...rows);
      offset += limit;
      await new Promise(r => setTimeout(r, 200)); // Предохранитель от rate limit
    }
  } catch (err) {
    console.error('❌ Ошибка при загрузке из Botpress:', err);
  }

  console.log(`📊 [2] Всего загружено ${allRows.length} записей`);
  return allRows;
}

export async function processDuplicatesAndSendWebhook(webhookUrl) {
  let gptRequests = 0;
  console.time('⏱️ Обработка заняла');

  try {
    const tickets = await fetchAllTickets();

    if (!tickets.length) {
      console.log('⚠️ [2.1] Нет данных для обработки.');
      return;
    }

    console.log('📚 [3] Группировка по категориям и подкатегориям...');
    const groups = groupBy(tickets, t => `${t["Job categories"]}|||${t["Job sub categories"]}|||${t["City"]}`);
    const toDelete = new Set();
let batchToDelete = [];
const BATCH_SIZE = 50;

async function flushBatch() {
  if (batchToDelete.length === 0) return;

  const payload = {
    duplicates: batchToDelete,
    total: tickets.length,
    found: toDelete.size,
    timestamp: new Date().toISOString()
  };

  console.log(`📤 Отправка партии из ${batchToDelete.length} дубликатов на вебхук...`);

  const webhookResponse = await fetch(webhookUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });

  console.log(`📬 Вебхук ответил со статусом: ${webhookResponse.status}`);

  batchToDelete = []; // Очистить партию
}

//подсчитываем количество сравнений
let totalComparisons = 0;

for (const groupTickets of Object.values(groups)) {
  const n = groupTickets.length;
  if (n >= 2) {
    totalComparisons += (n * (n - 1)) / 2;
  }
}

console.log(`📈 Ожидается примерно ${totalComparisons.toLocaleString()} сравнений`);

    for (const [groupKey, groupTickets] of Object.entries(groups)) {
      console.log(`🔍 [4] Обрабатываем группу: ${groupKey} (${groupTickets.length} записей)`);

        if (groupTickets.length < 2) {
    console.log(`ℹ️ Пропускаем группу "${groupKey}" — слишком мало записей (${groupTickets.length})`);
    continue;
  }


      const seenPairs = new Set();

      for (let i = 0; i < groupTickets.length; i++) {
        const t1 = groupTickets[i];
        for (let j = i + 1; j < groupTickets.length; j++) {
          const t2 = groupTickets[j];
          const key = [t1.id, t2.id].sort().join('-');
          if (seenPairs.has(key)) continue;
          seenPairs.add(key);

          const text1 = (t1.Requirements || '').slice(0, 500);
          const text2 = (t2.Requirements || '').slice(0, 500);

          const jaccard = jaccardSimilarity(text1, text2);
          const lev = levenshteinSimilarity(text1, text2);

          console.log(`🔗 [5] Сравнение ${t1.id} vs ${t2.id} — Jaccard: ${jaccard.toFixed(3)}, Levenshtein: ${lev.toFixed(3)}`);

          if (jaccard >= 0.08 && lev >= 0.55) {
            console.log(`🤖 [6] GPT проверка дубликата: ${t1.id} vs ${t2.id}`);
            gptRequests++;
            const isDuplicate = await isLikelyDuplicateGPT(text1, text2);

            if (isDuplicate) {
              let toRemove = t2;
              if (t1.Username === 'Anonymous participant' && t2.Username !== 'Anonymous participant') {
                toRemove = t1;
              } else if (t2.Username === 'Anonymous participant' && t1.Username !== 'Anonymous participant') {
                toRemove = t2;
              }
              if (!toDelete.has(toRemove.id)) {
  toDelete.add(toRemove.id);
  batchToDelete.push(toRemove.id);

  if (batchToDelete.length >= BATCH_SIZE) {
    await flushBatch();
  }
}


              
              console.log(`🗑️ [7] Добавлено к удалению: ${toRemove.id}`);
            } else {
              console.log(`✅ [7] GPT: НЕ дубликат`);
            }
          }
        }
      }
    }

    const payload = {
      duplicates: Array.from(toDelete),
      total: tickets.length,
      found: toDelete.size,
      timestamp: new Date().toISOString()
    };

    // 🔄 Дополнительная проверка дубликатов по номеру телефона
console.log("📞 [4.2] Запускаем дополнительную проверку по номеру телефона...");


// Проверка на мусорные значения в номере телефона
const badPhoneRecords = tickets.filter(t => {
  const phone = (t["Phone number"] || "").trim();
  return phone === "" || phone.toLowerCase() === "null" || phone === "—";
});

if (badPhoneRecords.length > 0) {
  console.log(`⚠️ Найдено ${badPhoneRecords.length} записей с пустыми или некорректными номерами телефонов (не будут включены в группировку):`);
  for (const bad of badPhoneRecords.slice(0, 10)) {
    console.log(`  - ID: ${bad.id}, phone: "${bad["Phone number"]}"`);
  }
  if (badPhoneRecords.length > 10) {
    console.log(`  ...и еще ${badPhoneRecords.length - 10} таких записей`);
  }
}

// Группируем по категориям, подкатегориям и номеру телефона
const phoneGroups = groupBy(tickets.filter(t => {
  const phone = (t["Phone number"] || "").trim();
  return /^\+?\d{6,}$/.test(phone); // +420123456 или 380991234567 и т.д.
}), t =>
  `${t["Job categories"]}|||${t["Job sub categories"]}|||${t["Phone number"]}`
);

for (const [groupKey, phoneGroup] of Object.entries(phoneGroups)) {
  if (phoneGroup.length < 2) continue;

  console.log(`📲 Проверка группы по телефону: ${groupKey} (${phoneGroup.length} записей)`);

  const seenPairs = new Set();

  for (let i = 0; i < phoneGroup.length; i++) {
    const t1 = phoneGroup[i];
    for (let j = i + 1; j < phoneGroup.length; j++) {
      const t2 = phoneGroup[j];

      const key = [t1.id, t2.id].sort().join('-');
      if (seenPairs.has(key) || toDelete.has(t1.id) || toDelete.has(t2.id)) continue;
      seenPairs.add(key);

      const text1 = (t1.Requirements || '').slice(0, 1000);
      const text2 = (t2.Requirements || '').slice(0, 1000);

      console.log(`🤖 [4.2.GPT] Проверка пары по телефону: ${t1.id} vs ${t2.id}`);
      gptRequests++;
      const isDuplicate = await isLikelyDuplicateGPT(text1, text2);

      if (isDuplicate) {
        let toRemove = t2;
        if (t1.Username === 'Anonymous participant' && t2.Username !== 'Anonymous participant') {
          toRemove = t1;
        } else if (t2.Username === 'Anonymous participant' && t1.Username !== 'Anonymous participant') {
          toRemove = t2;
        }

       
    if (!toDelete.has(toRemove.id)) {
  toDelete.add(toRemove.id);
  batchToDelete.push(toRemove.id);
  console.log(`🗑️ [4.2] Телефон-группа: добавлено к удалению: ${toRemove.id}`);

  if (batchToDelete.length >= BATCH_SIZE) {
    await flushBatch();
  }
}
        
      } else {
        console.log(`✅ [4.2] GPT: НЕ дубликат по телефону`);
      }
    }
  }
}

// Финальная отправка, если что-то осталось
await flushBatch();


    
  } catch (err) {
    console.error('❌ Ошибка в основном процессе:', err);
  }

  console.timeEnd('⏱️ Обработка заняла');
  console.log(`📊 Обращений к GPT: ${gptRequests}`);
}

// Утилита группировки
function groupBy(arr, fn) {
  return arr.reduce((acc, item) => {
    const key = fn(item);
    if (!acc[key]) acc[key] = [];
    acc[key].push(item);
    return acc;
  }, {});
}

function jaccardSimilarity(a, b) {
  const setA = new Set(a.toLowerCase().split(/\s+/));
  const setB = new Set(b.toLowerCase().split(/\s+/));
  const intersection = new Set([...setA].filter(x => setB.has(x)));
  const union = new Set([...setA, ...setB]);
  return intersection.size / union.size;
}

function levenshteinSimilarity(a, b) {
  a = a.toLowerCase().replace(/\s+/g, ' ').trim();
  b = b.toLowerCase().replace(/\s+/g, ' ').trim();
  if (a === b) return 1;

  const matrix = Array.from({ length: a.length + 1 }, () => Array(b.length + 1).fill(0));
  for (let i = 0; i <= a.length; i++) matrix[i][0] = i;
  for (let j = 0; j <= b.length; j++) matrix[0][j] = j;

  for (let i = 1; i <= a.length; i++) {
    for (let j = 1; j <= b.length; j++) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      matrix[i][j] = Math.min(
        matrix[i - 1][j] + 1,
        matrix[i][j - 1] + 1,
        matrix[i - 1][j - 1] + cost
      );
    }
  }

  const distance = matrix[a.length][b.length];
  return 1 - distance / Math.max(a.length, b.length);
}

async function isLikelyDuplicateGPT(textA, textB) {
  try {
    const response = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${process.env.OPENAI_API_KEY}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        model: "gpt-5-mini",
        temperature: 0,
        max_tokens: 3,
        messages: [
          {
            role: "system",
            content: "Ты помощник, который сравнивает два текста. Ответь строго 'yes', если это одна и та же вакансия, или 'no', если это разные предложения. Никаких пояснений."
          },
          {
            role: "user",
            content: `Текст 1:\n${textA}\n\nТекст 2:\n${textB}\n\nЭто дубликаты?`
          }
        ]
      })
    });

    const result = await response.json();

    if (!result.choices || !result.choices[0]?.message?.content) {
      console.warn('⚠️ Ответ от GPT некорректен или пуст:', JSON.stringify(result));
      return false;
    }

    const answer = result.choices[0].message.content.trim().toLowerCase();
    return answer === 'yes';
  } catch (err) {
    console.error('❌ Ошибка при запросе к OpenAI:', err);
    return false;
  }
}

export default processDuplicatesAndSendWebhook;
