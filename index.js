import express from 'express';
import fetch from 'node-fetch';

const app = express();
const port = process.env.PORT || 10000;

app.use(express.json());

// Проверка доступности сервиса
app.get('/', (req, res) => {
  res.send('✅ OK');
});

// Асинхронная проверка дублей, ответ сразу
app.post('/check', async (req, res) => {
  const webhookUrl = 'https://hook.eu2.make.com/g8kdpfddlaq70l31olejqx8ibtcpba9a';
  console.log('✅ Получен запрос на проверку дублей');
  res.send('🟢 Задача принята в обработку');

  // Фоновая обработка
  setTimeout(() => {
    processDuplicatesAndSendWebhook(webhookUrl);
  }, 0);
});

// --- Основная логика поиска дублей ---
async function processDuplicatesAndSendWebhook(webhookUrl) {
  try {
    console.log('⚙️ Начинаем фоновую обработку');
    console.time('⏱️ Время обработки');

    const response = await fetch("https://api.botpress.cloud/v1/tables/TicketsTable/rows/find", {
      method: "POST",
      headers: {
        "Authorization": "bearer bp_pat_04UwT9GFhWqTk8w0pP2lozgJ73Z9SgRdtjw3",
        "x-bot-id": "278175a2-b203-4af3-a6be-b2952f74edec",
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ limit: 1000 })
    });

    if (!response.ok) {
      const text = await response.text();
      console.error('❌ Ошибка от Botpress:', text);
      return;
    }

    const { rows: tickets = [] } = await response.json();
    const groups = groupBy(tickets, t => `${t["Job categories"]}|||${t["Job sub categories"]}`);
    const toDelete = new Set();

    for (const groupTickets of Object.values(groups)) {
      const seenPairs = new Set();
      for (let i = 0; i < groupTickets.length; i++) {
        const t1 = normalizeText(groupTickets[i].Requirements);
        for (let j = i + 1; j < groupTickets.length; j++) {
          const t2 = normalizeText(groupTickets[j].Requirements);
          const key = [groupTickets[i].id, groupTickets[j].id].sort().join('-');
          if (seenPairs.has(key)) continue;
          seenPairs.add(key);

          const jaccard = jaccardSimilarity(t1, t2);
          if (jaccard < 0.35) continue;

          const lev = levenshteinSimilarity(t1, t2);
          if (lev >= 0.83) {
            let toRemove = groupTickets[j];
            if (
              groupTickets[i].Username === 'Anonymous participant' &&
              groupTickets[j].Username !== 'Anonymous participant'
            ) {
              toRemove = groupTickets[i];
            } else if (
              groupTickets[j].Username === 'Anonymous participant' &&
              groupTickets[i].Username !== 'Anonymous participant'
            ) {
              toRemove = groupTickets[j];
            }
            toDelete.add(toRemove.id);
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
    console.timeEnd('⏱️ Время обработки');

  } catch (err) {
    console.error('❌ Ошибка в фоновом процессе:', err);
  }
}

// --- Утилиты ---

function normalizeText(text) {
  return (text || '')
    .toLowerCase()
    .replace(/tel\s*\+?\d{6,}/gi, '') // Удаление номера телефона
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, 400); // Обрезка до 400 символов
}

function groupBy(arr, fn) {
  return arr.reduce((acc, item) => {
    const key = fn(item);
    if (!acc[key]) acc[key] = [];
    acc[key].push(item);
    return acc;
  }, {});
}

function levenshteinSimilarity(a, b) {
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

function jaccardSimilarity(a, b) {
  const setA = new Set(a.split(/\s+/));
  const setB = new Set(b.split(/\s+/));
  const intersection = new Set([...setA].filter(x => setB.has(x)));
  const union = new Set([...setA, ...setB]);
  return intersection.size / union.size;
}

// --- Запуск сервера ---
app.listen(port, '0.0.0.0', () => {
  console.log(`✅ Server is running on http://0.0.0.0:${port}`);
});
