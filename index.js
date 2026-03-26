// goldapp-rates-proxy/index.js
// Fetches Ambicaa via Firebase REST every minute and inserts into gold_rates
// Kalinga is handled by GoldApp cron-job.org separately

import { createClient } from '@supabase/supabase-js'
import { io } from 'socket.io-client'
import express from 'express'
import https from 'https'

const app  = express()
const PORT = process.env.PORT || 3000

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
)

const state = {
  ambica_sell_rate: null,
  aamlin_sell_rate: null,
  ambica_updated:   null,
  aamlin_updated:   null,
}

// ── Ambicaa via Firebase REST ─────────────────────────────────────────────────
const FIREBASE_URL = 'https://rsbl-spot-gold-silver-prices.firebaseio.com/liverates/GOLDBLR999IND.json'

async function fetchAmbicaaFirebase() {
  return new Promise((resolve) => {
    https.get(FIREBASE_URL, (res) => {
      let data = ''
      res.on('data', chunk => data += chunk)
      res.on('end', () => {
        try {
          const json = JSON.parse(data)
          const sell = parseFloat(json?.Sell || json?.Ask || 0)
          if (sell > 100000) {
            state.ambica_sell_rate = sell
            state.ambica_updated   = new Date().toISOString()
            console.log('[Ambicaa] Rate:', sell)
          }
        } catch (e) { console.error('[Ambicaa] Parse error:', e.message) }
        resolve()
      })
    }).on('error', (e) => { console.error('[Ambicaa] Error:', e.message); resolve() })
  })
}

// ── Aamlin via Socket.IO (best effort) ────────────────────────────────────────
function connectAamlin() {
  const socket = io('http://starlinebulltech.in:10001', {
    transports: ['polling', 'websocket'],
    reconnection: true,
    reconnectionDelay: 30000,
    timeout: 15000,
  })
  socket.on('connect', () => { console.log('[Aamlin] Connected'); socket.emit('room', 'aamlinspot') })
  socket.on('message', (data) => {
    try {
      const rateArr = data?.Rate || data?.rate || []
      for (const rate of rateArr) {
        const sym = rate.Symbol || rate.symbol || ''
        if (/gold\s*999\s*ind/i.test(sym)) {
          const sell = parseFloat(rate.Ask || rate.ask || 0)
          if (sell > 100000) { state.aamlin_sell_rate = sell; state.aamlin_updated = new Date().toISOString(); console.log('[Aamlin] Rate:', sell) }
          break
        }
      }
    } catch (e) {}
  })
  socket.on('connect_error', () => {})
}

// ── Save to Supabase — INSERT new row ─────────────────────────────────────────
async function saveToSupabase() {
  if (!state.ambica_sell_rate && !state.aamlin_sell_rate) {
    console.log('[Supabase] No rates yet')
    return
  }
  const { error } = await supabase.from('gold_rates').insert({
    ambica_sell_rate: state.ambica_sell_rate,
    aamlin_sell_rate: state.aamlin_sell_rate || null,
    fetched_at:       new Date().toISOString(),
  })
  if (error) console.error('[Supabase] Error:', error.message)
  else console.log('[Supabase] Inserted — Ambicaa:', state.ambica_sell_rate, '| Aamlin:', state.aamlin_sell_rate)
}

async function tick() {
  await fetchAmbicaaFirebase()
  await saveToSupabase()
}

app.get('/rates', (_req, res) => res.json({ ...state, server_time: new Date().toISOString() }))
app.get('/health', (_req, res) => res.json({ status: 'ok', uptime: process.uptime() }))

app.listen(PORT, () => {
  console.log(`[Server] Port ${PORT}`)
  connectAamlin()
  tick()
  setInterval(tick, 60 * 1000)
})