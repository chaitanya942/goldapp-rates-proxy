// goldapp-rates-proxy/index.js
// Maintains persistent Socket.IO connections to Ambicaa and Aamlin
// Saves latest rates to Supabase every minute
// Exposes GET /rates for health checking

import { io } from 'socket.io-client'
import { createClient } from '@supabase/supabase-js'
import express from 'express'

const app  = express()
const PORT = process.env.PORT || 3000

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
)

// In-memory latest rates
const state = {
  ambica_sell_rate: null,
  aamlin_sell_rate: null,
  ambica_updated:   null,
  aamlin_updated:   null,
}

// ── Ambicaa ───────────────────────────────────────────────────────────────────
function connectAmbicaa() {
  console.log('[Ambicaa] Connecting...')
  const socket = io('http://dashboard.ambicaaspot.com:10001', {
    transports:        ['websocket', 'polling'],
    reconnection:      true,
    reconnectionDelay: 3000,
    timeout:           10000,
  })

  socket.on('connect', () => {
    console.log('[Ambicaa] Connected — joining room ambicaaspot')
    socket.emit('room', 'ambicaaspot')
  })

  socket.on('message', (data) => {
    try {
      const rateArr = data?.Rate || data?.rate || []
      for (const rate of rateArr) {
        const sym = rate.Symbol || rate.symbol || ''
        if (sym.toUpperCase().includes('IND-GOLD') && sym.includes('1KG')) {
          const sell = parseFloat(rate.Ask || rate.ask || 0)
          if (sell > 100000) {
            state.ambica_sell_rate = sell
            state.ambica_updated   = new Date().toISOString()
            console.log('[Ambicaa] Rate updated:', sell)
          }
          break
        }
      }
    } catch (e) { console.error('[Ambicaa] Parse error:', e.message) }
  })

  socket.on('disconnect', (reason) => console.log('[Ambicaa] Disconnected:', reason))
  socket.on('connect_error', (err)  => console.error('[Ambicaa] Connect error:', err.message))
}

// ── Aamlin ────────────────────────────────────────────────────────────────────
function connectAamlin() {
  console.log('[Aamlin] Connecting...')
  const socket = io('http://starlinebulltech.in:10001', {
    transports:        ['websocket', 'polling'],
    reconnection:      true,
    reconnectionDelay: 3000,
    timeout:           10000,
  })

  socket.on('connect', () => {
    console.log('[Aamlin] Connected — joining room aamlinspot')
    socket.emit('room', 'aamlinspot')
  })

  socket.on('message', (data) => {
    try {
      const rateArr = data?.Rate || data?.rate || []
      for (const rate of rateArr) {
        const sym = rate.Symbol || rate.symbol || ''
        if (/gold\s*999\s*ind/i.test(sym)) {
          const sell = parseFloat(rate.Ask || rate.ask || 0)
          if (sell > 100000) {
            state.aamlin_sell_rate = sell
            state.aamlin_updated   = new Date().toISOString()
            console.log('[Aamlin] Rate updated:', sell)
          }
          break
        }
      }
    } catch (e) { console.error('[Aamlin] Parse error:', e.message) }
  })

  socket.on('disconnect', (reason) => console.log('[Aamlin] Disconnected:', reason))
  socket.on('connect_error', (err)  => console.error('[Aamlin] Connect error:', err.message))
}

// ── Save to Supabase every minute ─────────────────────────────────────────────
// Inserts a new row — Kalinga rate will be null here (handled by GoldApp cron)
// GoldApp fetch-gold-rates will update the latest row with Kalinga rate
async function saveToSupabase() {
  if (!state.ambica_sell_rate && !state.aamlin_sell_rate) {
    console.log('[Supabase] No rates yet, skipping save')
    return
  }
  const { error } = await supabase.from('gold_rates').insert({
    ambica_sell_rate: state.ambica_sell_rate,
    aamlin_sell_rate: state.aamlin_sell_rate,
    fetched_at:       new Date().toISOString(),
  })
  if (error) console.error('[Supabase] Insert error:', error.message)
  else console.log('[Supabase] Saved — Ambicaa:', state.ambica_sell_rate, '| Aamlin:', state.aamlin_sell_rate)
}

// ── Express ───────────────────────────────────────────────────────────────────
app.get('/rates', (req, res) => {
  res.json({
    ambica_sell_rate: state.ambica_sell_rate,
    aamlin_sell_rate: state.aamlin_sell_rate,
    ambica_updated:   state.ambica_updated,
    aamlin_updated:   state.aamlin_updated,
    server_time:      new Date().toISOString(),
  })
})

app.get('/health', (_req, res) => res.json({ status: 'ok', uptime: process.uptime() }))

// ── Start ─────────────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`[Server] Running on port ${PORT}`)
  connectAmbicaa()
  connectAamlin()
  setInterval(saveToSupabase, 60 * 1000)
  // Initial save after 10s to let connections establish
  setTimeout(saveToSupabase, 10000)
})
