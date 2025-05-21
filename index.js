app.get('/', async (req, res) => {
  try {
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
      return res.status(response.status).send(`❌ Ошибка Botpress:\n${text}`);
    }

    const result = await response.json();
    const tickets = result.rows || [];

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

          const sim = similarity(t1.Requirements, t2.Requirements);
          if (sim >= 0.85) {
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

    return res.json({ duplicates: Array.from(toDelete) }); // ← Переместили сюда

  } catch (err) {
    return res.status(500).send(`❌ Внутренняя ошибка:\n${err.message}`);
  }
});
