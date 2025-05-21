import express from 'express';
import fetch from 'node-fetch';

const app = express();
const port = process.env.PORT || 10000;
if (!port) {
  throw new Error("PORT is not defined");
}

app.use(express.json());

// app.get('/', async (req, res) => {
//   try {
//     const response = await fetch("https://api.botpress.cloud/v1/tables/TicketsTable/rows/find", {
//       method: "POST",
//       headers: {
//         "Authorization": "bearer bp_pat_04UwT9GFhWqTk8w0pP2lozgJ73Z9SgRdtjw3",
//         "x-bot-id": "278175a2-b203-4af3-a6be-b2952f74edec",
//         "Content-Type": "application/json"
//       },
//       body: JSON.stringify({ limit: 1000 })
//     });

//     if (!response.ok) {
//       const text = await response.text();
//       return res.status(response.status).send(`❌ Ошибка Botpress:\n${text}`);
//     }

//     const result = await response.json();
//     const tickets = result.rows || [];

//     const groups = groupBy(tickets, t => `${t["Job categories"]}|||${t["Job sub categories"]}`);
//     const toDelete = new Set();

//     for (const groupTickets of Object.values(groups)) {
//       const seenPairs = new Set();
//       for (let i = 0; i < groupTickets.length; i++) {
//         const t1 = groupTickets[i];
//         for (let j = i + 1; j < groupTickets.length; j++) {
//           const t2 = groupTickets[j];
//           const key = [t1.id, t2.id].sort().join('-');
//           if (seenPairs.has(key)) continue;
//           seenPairs.add(key);

//           const sim = similarity(t1.Requirements, t2.Requirements);
//           if (sim >= 0.85) {
//             let toRemove = t2;
//             if (t1.Username === 'Anonymous participant' && t2.Username !== 'Anonymous participant') {
//               toRemove = t1;
//             } else if (t2.Username === 'Anonymous participant' && t1.Username !== 'Anonymous participant') {
//               toRemove = t2;
//             }
//             toDelete.add(toRemove.id);
//           }
//         }
//       }
//     }

//     return res.json({ duplicates: Array.from(toDelete) });

//   } catch (err) {
//     return res.status(500).send(`❌ Внутренняя ошибка:\n${err.message}`);
//   }
// });

app.get('/', (req, res) => {
  res.send('✅ OK');
});



app.listen(port, '0.0.0.0', () => {
  console.log(`✅ Server is running on http://0.0.0.0:${port}`);
});

// --- Утилиты ---
function groupBy(arr, fn) {
  return arr.reduce((acc, item) => {
    const key = fn(item);
    if (!acc[key]) acc[key] = [];
    acc[key].push(item);
    return acc;
  }, {});
}

function similarity(a, b) {
  a = (a || '').toLowerCase().replace(/\s+/g, ' ').trim();
  b = (b || '').toLowerCase().replace(/\s+/g, ' ').trim();
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
