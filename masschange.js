// masschange.js
import { Router } from 'express'
import axios from 'axios'

const router = Router()

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

const PAGE_SIZE = 100
const BATCH_SIZE = 100

// эндпоинт массовой замены
router.all('/masschange', async (req, res) => {
  try {
    const field = (req.body.field ?? req.query.field ?? '').trim()
    const from = req.body.from ?? req.query.from
    const to = req.body.to ?? req.query.to

    if (!field || from === undefined || to === undefined) {
      return res.status(400).json({ error: 'Missing parameters: field, from, to' })
    }

    console.log(`🚀 MASSCHANGE start | field="${field}" | from="${from}" | to="${to}"`)

    let page = 0
    let updated = 0
    let matched = 0
    const batch = []

    async function flush() {
      if (batch.length > 0) {
        await axios.put(`${API_URL}/rows`, { rows: batch }, { headers: HEADERS })
        console.log(`✅ Updated batch of ${batch.length}`)
        updated += batch.length
        batch.length = 0
      }
    }

    while (true) {
      const resp = await axios.post(`${API_URL}/rows/find`, {
        limit: PAGE_SIZE,
        offset: page * PAGE_SIZE,
        filter: { [field]: from },
        orderBy: 'id',
        orderDirection: 'asc'
      }, { headers: HEADERS })

      const rows = resp.data.rows || []
      if (rows.length === 0) break

      for (const row of rows) {
        matched++
        batch.push({ id: row.id, [field]: to })
        if (batch.length >= BATCH_SIZE) await flush()
      }
      page++
    }

    await flush()

    return res.json({ matched, updated })
  } catch (e) {
    console.error('❌ MASSCHANGE error:', e)
    return res.status(500).json({ error: e.message })
  }
})

export default router
