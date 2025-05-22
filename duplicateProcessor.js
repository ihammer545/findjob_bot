// duplicateProcessor.js
import fetch from 'node-fetch';

// Основная функция
export async function processDuplicatesAndSendWebhook(webhookUrl) {
  let gptRequests = 0;
  console.time('⏱️ Обработка заняла');

  try {
    const response = await fetch("https://api.botpress.cloud/v1/tables/TicketsTable/rows/find", {
      method: "POST",
      headers: {
        "Authorization": `bearer ${process.env.BOTPRESS_API_TOKEN}`,
        "x-bot-id": process.env.BOTPRESS_BOT_ID,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ limit: 1000 })
    });

    if (!response.ok) {
      const text = await response.text();
      console.error('❌ Ошибка от Botpress:', text);
      return;
    }

    const json = await response.json();
    const tickets = json.rows || [];

    const groups = groupBy(tickets, t => `${t["Job categories"]}|||${t["Job sub categories"]}`);
    const toDelete = new Set();

    for (const groupTickets of Object.values(groups)) {
      const seenPairs = new Set();

      for (let i = 0; i < groupTickets.length; i++) {
        const t1 = groupTickets[i];
        for (let j = i + 1; j < groupTickets.length; j++) {
          const t2 = groupTickets[j];
          const key = [t1.id, t2.id].sort().join('-');
          if (seenPairs.has(key)) continue;
          seenPairs.add(key);

          const text1 = (t1.Requirements || '').slice(0, 400);
          const text2 = (t2.Requirements || '').slice(0, 400);

          const jaccard = jaccardSimilarity(text1, text2);
          if (jaccard >= 0.08) {
            const lev = levenshteinSimilarity(text1, text2);
            if (lev >= 0.55) {
              gptRequests++;
              const isDuplicate = await isLikelyDuplicateGPT(text1, text2);
              if (!isDuplicate) continue;

              let toRemove = t2;
              if (t1.Username === 'Anonymous participant' && t2.Username !== 'Anonymous participant') {
                toRemove = t1;
              } else if (t2.Username === 'Anonymous participant' && t1.Username !== 'Anonymous participant') {
                toRemove = t2;
              }
              toDelete.add(toRemove.id);
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

    const webhookResponse = await fetch(webhookUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });

    console.log('📤 Отправлено на вебхук:', webhookResponse.status);
  } catch (err) {
    console.error('❌ Ошибка в фоновом процессе:', err);
  }

  console.timeEnd('⏱️ Обработка заняла');
  console.log(`📊 Количество обращений к GPT: ${gptRequests}`);
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
        model: "gpt-4o-mini",
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
    const answer = result.choices?.[0]?.message?.content?.trim().toLowerCase();
    return answer === 'yes';
  } catch (err) {
    console.error('❌ Ошибка при запросе к OpenAI:', err);
    return false;
  }
}
