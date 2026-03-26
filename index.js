// goldapp-rates-proxy/index.js
// Ambicaa: Firebase Realtime Database — GOLDBLR999IND → Sell
// Aamlin: Socket.IO — blocked, skip for now (handled browser-side)

import { createClient } from '@supabase/supabase-js'
import { io } from 'socket.io-client'
import express from 'express'
import https from 'https'
import http from 'http'

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

// ── Ambicaa via Firebase REST API ─────────────────────────────────────────────
// Firebase Realtime Database has a simple REST API — no SDK needed
// GET https://{project}.firebaseio.com/{path}.json
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
            console.log('[Ambicaa Firebase] Rate:', sell)
          } else {
            console.log('[Ambicaa Firebase] Invalid rate:', sell, '| Raw:', data.slice(0, 100))
          }
        } catch (e) { console.error('[Ambicaa Firebase] Parse error:', e.message) }
        resolve()
      })
    }).on('error', (e) => {
      console.error('[Ambicaa Firebase] Fetch error:', e.message)
      resolve()
    })
  })
}

// ── Aamlin via Socket.IO (best effort) ────────────────────────────────────────
function connectAamlin() {
  console.log('[Aamlin] Attempting connection...')
  const socket = io('http://starlinebulltech.in:10001', {
    transports:        ['polling', 'websocket'],
    reconnection:      true,
    reconnectionDelay: 30000,
    timeout:           15000,
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
            console.log('[Aamlin] Rate:', sell)
          }
          break
        }
      }
    } catch (e) { console.error('[Aamlin] Parse error:', e.message) }
  })

  socket.on('connect_error', (err) => console.log('[Aamlin] Unavailable (handled browser-side):', err.message.slice(0,30)))
  socket.on('disconnect', (reason) => console.log('[Aamlin] Disconnected:', reason))
}

// ── Save to Supabase ──────────────────────────────────────────────────────────
async function saveToSupabase() {
  if (!state.ambica_sell_rate && !state.aamlin_sell_rate) {
    console.log('[Supabase] No rates yet, skipping')
    return
  }

  // Update the most recent gold_rates row with Ambicaa + Aamlin rates
  const { data: latest, error: fetchErr } = await supabase
    .from('gold_rates')
    .select('id')
    .order('fetched_at', { ascending: false })
    .limit(1)
    .single()

  if (fetchErr || !latest?.id) {
    console.error('[Supabase] Could not find latest row:', fetchErr?.message)
    return
  }

  const updateData = {}
  if (state.ambica_sell_rate) updateData.ambica_sell_rate = state.ambica_sell_rate
  if (state.aamlin_sell_rate) updateData.aamlin_sell_rate = state.aamlin_sell_rate

  const { error } = await supabase
    .from('gold_rates')
    .update(updateData)
    .eq('id', latest.id)

  if (error) console.error('[Supabase] Update error:', error.message)
  else console.log('[Supabase] Saved — Ambicaa:', state.ambica_sell_rate, '| Aamlin:', state.aamlin_sell_rate)
}

// ── Main loop: fetch Ambicaa every 60s ───────────────────────────────────────
async function tick() {
  await fetchAmbicaaFirebase()
  await saveToSupabase()
}

// ── Express ───────────────────────────────────────────────────────────────────
app.get('/rates', (_req, res) => res.json({
  ambica_sell_rate: state.ambica_sell_rate,
  aamlin_sell_rate: state.aamlin_sell_rate,
  ambica_updated:   state.ambica_updated,
  aamlin_updated:   state.aamlin_updated,
  server_time:      new Date().toISOString(),
}))

app.get('/health', (_req, res) => res.json({ status: 'ok', uptime: process.uptime() }))

// ── Start ─────────────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`[Server] Running on port ${PORT}`)
  connectAamlin()
  // Fetch immediately then every 60s
  tick()
  setInterval(tick, 60 * 1000)
})