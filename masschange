// masschange.js
// Массовая замена значения в конкретном поле Botpress по равенству

import express from 'express'
import axios from 'axios'

const app = express()
app.use(express.json())

// --- Конфиг из env ---
const API_URL = process.env.BOTPRESS_API_URL         // например: https://api.botpress.cloud/v1
const BOT_ID = process.env.BOTPRESS_BOT_ID
const WORKSPACE_ID = process.env.BOTPRESS_WORKSPACE_ID
const BP_TOKEN = process.env.BOTPRESS_API_TOKEN

if (!API_URL || !BOT_ID || !WORKSPACE_ID || !BP_TOKEN) {
  console.error('❌ Missing Botpress env vars: BOTPRESS_API_URL, BOTPRESS_BOT_ID, BOTPRESS_WORKSPACE_ID, BOTPRESS_API_TOKEN')
  process.exit(1)
}

const HEADERS = {
  Authorization: `Bearer ${BP_TOKEN}`,
  'x-bot-id': BOT_ID,
  'x-workspace-id': WORKSPACE_ID,
  'Content-Type': 'application/json'
}

const PAGE_SIZE = 100        // постраничная выборка
const BATCH_SIZE = 100       // размер батча на обновление

// Утилита безопасного чтения значения поля
const getFieldValue = (row, field) => {
  // поддержим точки в имени поля вроде "Phone number"
  return row?.[field]
}

// --- основной эндпоинт ---
// можно вызывать как:
// GET  /masschange?field=Region&from=null&to=Lower Austria
// POST /masschange  { "field":"Region", "from":"null", "to":"Lower Austria" }
app.all('/masschange', async (req, res) => {
  try {
    // читаем параметры из query или body (body имеет приоритет)
    const field = (req.body.field ?? req.query.field ?? '').trim()
    const from = (req.body.from ?? req.query.from ?? '')
    const to = (req.body.to ?? req.query.to ?? '')

    // опциональные флаги:
    const caseInsensitive = (req.body.caseInsensitive ?? req.query.caseInsensitive ?? 'false') === 'true'
    const dryRun = (req.body.dryRun ?? req.query.dryRun ?? 'false') === 'true'

    if (!field) {
      return res.status(400).json({ error: 'Missing "field" parameter' })
    }
    if (from === '') {
      return res.status(400).json({ error: 'Missing "from" parameter' })
    }
    if (to === '') {
      return res.status(400).json({ error: 'Missing "to" parameter' })
    }

    console.log(`🚀 MASSCHANGE start | field="${field}" | from="${from}" | to="${to}" | caseInsensitive=${caseInsensitive} | dryRun=${dryRun}`)

    let page = 0
    let totalScanned = 0
    let totalMatched = 0
    let totalUpdated = 0
    let lastRowId = null

    const batch = []

    // Внутренняя функция отправки накопленного батча
    async function flushBatch() {
      if (batch.length === 0) return
      if (dryRun) {
        console.log(`🧪 DRY-RUN: пропускаем PUT (батч ${batch.length})`)
        batch.length = 0
        return
      }
      console.log(`⬆️ PUT /rows — отправка батча (${batch.length})...`)
      try {
        await axios.put(`${API_URL}/rows`, { rows: batch }, { headers: HEADERS })
        totalUpdated += batch.length
        console.log(`✅ Обновлено строк: +${batch.length} (итого ${totalUpdated})`)
      } catch (err) {
        console.error('❌ Ошибка при обновлении батча:', err.response?.data || err.message)
      } finally {
        batch.length = 0
      }
    }

    // Подготовим фильтр. Большинство установок Botpress позволяют равенство простым значением.
    // Если сервер не поддерживает сложные операторы OR, мы делаем постфильтрацию на клиенте (см. ниже).
    const baseFilter = { [field]: from }

    while (true) {
      const payload = {
        limit: PAGE_SIZE,
        offset: page * PAGE_SIZE,
        filter: baseFilter,
        orderBy: 'id',
        orderDirection: 'asc'
      }

      let rows = []
      try {
        const resp = await axios.post(`${API_URL}/rows/find`, payload, { headers: HEADERS })
        rows = resp?.data?.rows ?? []
      } catch (err) {
        console.error('❌ Ошибка запроса /rows/find:', err.response?.data || err.message)
        break
      }

      if (rows.length === 0) break

      for (const row of rows) {
        totalScanned++
        lastRowId = row.id

        const currentVal = getFieldValue(row, field)
        let isMatch

        if (caseInsensitive && typeof currentVal === 'string' && typeof from === 'string') {
          isMatch = currentVal.toLowerCase() === from.toLowerCase()
        } else {
          isMatch = currentVal === from
        }

        // На случай если серверная фильтрация не идеальна — дублируем проверку
        if (!isMatch) {
          continue
        }

        totalMatched++

        // Пропускаем, если уже равно целевому значению
        if (currentVal === to || (caseInsensitive && typeof currentVal === 'string' && currentVal.toLowerCase() === String(to).toLowerCase())) {
          continue
        }

        const updatedRow = { id: row.id, [field]: to }
        batch.push(updatedRow)

        if (batch.length >= BATCH_SIZE) {
          await flushBatch()
        }
      }

      page++
      console.log(`📄 Page ${page} processed | scanned: ${totalScanned} | matched: ${totalMatched} | lastRowId: ${lastRowId}`)
    }

    // финальный батч
    await flushBatch()

    const summary = {
      field,
      from,
      to,
      dryRun,
      caseInsensitive,
      scanned: totalScanned,
      matched: totalMatched,
      updated: totalUpdated
    }

    console.log('🏁 MASSCHANGE done:', summary)
    return res.json(summary)
  } catch (e) {
    console.error('❌ MASSCHANGE unexpected error:', e)
    return res.status(500).json({ error: e.message || 'Unexpected error' })
  }
})

// Запуск сервера, если файл исполняется напрямую
if (process.argv[1] === new URL(import.meta.url).pathname) {
  const PORT = process.env.PORT || 3000
  app.listen(PORT, () => {
    console.log(`✅ Masschange server listening on http://localhost:${PORT}`)
  })
}

export default app
