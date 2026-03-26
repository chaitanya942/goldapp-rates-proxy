import express from 'express'
import { io } from 'socket.io-client'
import { createClient } from '@supabase/supabase-js'

const app = express()
app.use(express.json())

const PORT = process.env.PORT || 3000
const SUPABASE_URL = process.env.SUPABASE_URL
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY
const SAVE_INTERVAL_MS = Number(process.env.SAVE_INTERVAL_MS || 60000)
const HTTP_TIMEOUT_MS = Number(process.env.HTTP_TIMEOUT_MS || 15000)
const RECONNECT_BASE_MS = Number(process.env.RECONNECT_BASE_MS || 2000)
const RECONNECT_MAX_MS = Number(process.env.RECONNECT_MAX_MS || 30000)
const AUTO_DISCOVERY_TIMEOUT_MS = Number(process.env.AUTO_DISCOVERY_TIMEOUT_MS || 12000)

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY')
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { autoRefreshToken: false, persistSession: false },
})

const state = {
  startedAt: new Date().toISOString(),
  lastSaveAt: null,
  lastSaveResult: null,
  latestRates: {
    ambica_sell_rate: null,
    aamlin_sell_rate: null,
  },
  minuteBucket: null,
  vendors: {},
}

function nowIso() {
  return new Date().toISOString()
}

function minuteBucket(date = new Date()) {
  const d = new Date(date)
  d.setSeconds(0, 0)
  return d.toISOString()
}

function jitter(ms, ratio = 0.2) {
  const delta = Math.floor(ms * ratio)
  return ms + Math.floor(Math.random() * (delta * 2 + 1)) - delta
}

function backoff(attempt) {
  const exp = Math.min(RECONNECT_BASE_MS * Math.pow(2, attempt), RECONNECT_MAX_MS)
  return jitter(exp)
}

function pickNumber(value) {
  if (value === null || value === undefined || value === '') return null
  const n = Number(String(value).replace(/,/g, '').trim())
  return Number.isFinite(n) ? n : null
}

function deepFindArrayByKey(obj, targetKey) {
  if (!obj || typeof obj !== 'object') return null

  if (Array.isArray(obj)) {
    for (const item of obj) {
      const found = deepFindArrayByKey(item, targetKey)
      if (found) return found
    }
    return null
  }

  if (Array.isArray(obj[targetKey])) return obj[targetKey]

  for (const value of Object.values(obj)) {
    const found = deepFindArrayByKey(value, targetKey)
    if (found) return found
  }

  return null
}

function deepFindFirstObjectMatch(obj, predicate) {
  if (!obj || typeof obj !== 'object') return null

  if (Array.isArray(obj)) {
    for (const item of obj) {
      const found = deepFindFirstObjectMatch(item, predicate)
      if (found) return found
    }
    return null
  }

  if (predicate(obj)) return obj

  for (const value of Object.values(obj)) {
    const found = deepFindFirstObjectMatch(value, predicate)
    if (found) return found
  }

  return null
}

function defaultRateExtractor(vendorName, payload) {
  const rules = {
    ambica: {
      matcher: (item) => {
        const name = String(item?.Commodity ?? item?.Symbol ?? item?.symbol ?? item?.name ?? '').toUpperCase()
        return name.includes('IND-GOLD[999]-1KG') || (name.includes('IND-GOLD') && name.includes('1KG'))
      },
      fields: ['Ask', 'ask', 'SELL', 'Sell', 'rate'],
    },
    aamlin: {
      matcher: (item) => {
        const name = String(item?.Commodity ?? item?.Symbol ?? item?.symbol ?? item?.name ?? '').toUpperCase()
        return name.includes('GOLD 999 IND')
      },
      fields: ['Ask', 'ask', 'SELL', 'Sell', 'rate'],
    },
  }

  const rule = rules[vendorName]
  if (!rule) return null

  const rateArray =
    deepFindArrayByKey(payload, 'Rate') ||
    deepFindArrayByKey(payload, 'rate') ||
    []

  if (Array.isArray(rateArray) && rateArray.length) {
    const found = rateArray.find((item) => rule.matcher(item))
    if (found) {
      for (const key of rule.fields) {
        const value = pickNumber(found?.[key])
        if (value !== null && value > 100000) return value
      }
    }
  }

  const objectMatch = deepFindFirstObjectMatch(payload, rule.matcher)
  if (objectMatch) {
    for (const key of rule.fields) {
      const value = pickNumber(objectMatch?.[key])
      if (value !== null && value > 100000) return value
    }
  }

  return null
}

function candidateConfigsForVendor(vendor) {
  const common = [
    { path: '/socket.io/', transports: ['polling'] },
    { path: '/socket.io', transports: ['polling'] },
    { path: '/socket.io/', transports: ['websocket', 'polling'] },
    { path: '/socket.io', transports: ['websocket', 'polling'] },
  ]

  if (vendor.name === 'ambica') {
    const bases = [
      'http://dashboard.ambicaaspot.com:10001',
      'https://dashboard.ambicaaspot.com:10001',
      'http://dashboard.ambicaaspot.com',
      'https://dashboard.ambicaaspot.com',
    ]

    return bases.flatMap((baseUrl) =>
      common.map((cfg) => ({ ...cfg, baseUrl }))
    )
  }

  if (vendor.name === 'aamlin') {
    const bases = [
      vendor.manualBaseUrl,
      'http://starlinebulltech.in:10001',
      'https://starlinebulltech.in:10001',
      'http://starlinebulltech.in',
      'https://starlinebulltech.in',
      'http://aamlinspot.in',
      'https://aamlinspot.in',
      'http://www.aamlinspot.in',
      'https://www.aamlinspot.in',
    ].filter(Boolean)

    const extraPaths = [
      '/socket.io/',
      '/socket.io',
      '/ws/socket.io/',
      '/ws/socket.io',
      '/socket/socket.io/',
      '/socket/socket.io',
    ]

    return [...new Set(bases)].flatMap((baseUrl) =>
      extraPaths.flatMap((path) => [
        { baseUrl, path, transports: ['polling'] },
        { baseUrl, path, transports: ['websocket', 'polling'] },
      ])
    )
  }

  return []
}

async function probeCandidate(vendor, candidate) {
  return new Promise((resolve) => {
    const start = Date.now()

    const socket = io(candidate.baseUrl, {
      path: candidate.path,
      transports: candidate.transports,
      timeout: Math.min(HTTP_TIMEOUT_MS, AUTO_DISCOVERY_TIMEOUT_MS),
      reconnection: false,
      forceNew: true,
      autoConnect: true,
      withCredentials: false,
    })

    let settled = false

    const finish = (result) => {
      if (settled) return
      settled = true
      try { socket.removeAllListeners() } catch {}
      try { socket.close() } catch {}
      resolve({
        ...candidate,
        ok: !!result.ok,
        reason: result.reason || null,
        latencyMs: Date.now() - start,
      })
    }

    const timer = setTimeout(() => {
      finish({ ok: false, reason: 'timeout' })
    }, AUTO_DISCOVERY_TIMEOUT_MS)

    socket.on('connect', () => {
      clearTimeout(timer)
      finish({ ok: true, reason: 'connected' })
    })

    socket.on('connect_error', (err) => {
      clearTimeout(timer)
      finish({ ok: false, reason: err?.message || 'connect_error' })
    })

    socket.on('error', (err) => {
      clearTimeout(timer)
      finish({ ok: false, reason: err?.message || 'error' })
    })
  })
}

async function discoverWorkingConfig(vendor) {
  const candidates = candidateConfigsForVendor(vendor)

  for (const candidate of candidates) {
    const result = await probeCandidate(vendor, candidate)
    vendor.discoveryLog.unshift({ at: nowIso(), ...result })
    vendor.discoveryLog = vendor.discoveryLog.slice(0, 20)

    if (result.ok) return candidate
  }

  return null
}

function updateRate(column, value, vendorName) {
  const num = pickNumber(value)
  if (num === null) return false

  state.latestRates[column] = num
  state.minuteBucket = minuteBucket()

  const vendor = state.vendors[vendorName]
  if (vendor) {
    vendor.lastRate = num
    vendor.lastRateAt = nowIso()
  }

  return true
}

async function saveRatesToSupabase() {
  if (!state.latestRates.ambica_sell_rate && !state.latestRates.aamlin_sell_rate) {
    console.log('[Supabase] No rates yet, skipping save')
    return
  }

  const payload = {
    fetched_at: nowIso(),
    ambica_sell_rate: state.latestRates.ambica_sell_rate,
    aamlin_sell_rate: state.latestRates.aamlin_sell_rate,
  }

  const { error } = await supabase
    .from('gold_rates')
    .insert(payload)

  if (error) {
    state.lastSaveAt = nowIso()
    state.lastSaveResult = { ok: false, error: error.message, payload }
    console.error('[Supabase] Insert error:', error.message)
    return
  }

  state.lastSaveAt = nowIso()
  state.lastSaveResult = { ok: true, payload }
  console.log('[Supabase] Saved — Ambicaa:', state.latestRates.ambica_sell_rate, '| Aamlin:', state.latestRates.aamlin_sell_rate)
}

function startSaveLoop() {
  setInterval(async () => {
    try {
      await saveRatesToSupabase()
    } catch (err) {
      console.error('[save] failed', err?.message || err)
    }
  }, SAVE_INTERVAL_MS)
}

function vendorTemplate({ name, room, column, manualBaseUrl = null }) {
  return {
    name,
    room,
    column,
    manualBaseUrl,
    socket: null,
    status: 'idle',
    connectedAt: null,
    disconnectedAt: null,
    lastMessageAt: null,
    lastPayloadSample: null,
    lastError: null,
    lastRate: null,
    lastRateAt: null,
    attempts: 0,
    reconnectTimer: null,
    activeConfig: null,
    discoveryLog: [],
  }
}

function registerVendors() {
  state.vendors.ambica = vendorTemplate({
    name: 'ambica',
    room: 'ambicaaspot',
    column: 'ambica_sell_rate',
  })

  state.vendors.aamlin = vendorTemplate({
    name: 'aamlin',
    room: 'aamlinspot',
    column: 'aamlin_sell_rate',
    manualBaseUrl: process.env.AAMLIN_SOCKET_URL || null,
  })
}

function attachVendorListeners(vendor, socket) {
  const handlePayload = (payload, sourceEvent = 'message') => {
    vendor.lastMessageAt = nowIso()
    vendor.lastPayloadSample = payload

    const rate = defaultRateExtractor(vendor.name, payload)
    if (rate !== null) {
      updateRate(vendor.column, rate, vendor.name)
      console.log(`[${vendor.name}] rate update from ${sourceEvent}:`, rate)
    }
  }

  socket.on('connect', () => {
    vendor.status = 'connected'
    vendor.connectedAt = nowIso()
    vendor.lastError = null
    vendor.attempts = 0

    console.log(`[${vendor.name}] connected`, vendor.activeConfig)

    try {
      socket.emit('join', vendor.room)
      socket.emit('room', vendor.room)
      socket.emit('subscribe', vendor.room)
      console.log(`[${vendor.name}] join/subscription emitted for room ${vendor.room}`)
    } catch (err) {
      console.warn(`[${vendor.name}] room emit failed`, err?.message || err)
    }
  })

  socket.on('message', (payload) => handlePayload(payload, 'message'))
  socket.on('rates', (payload) => handlePayload(payload, 'rates'))
  socket.on('tick', (payload) => handlePayload(payload, 'tick'))
  socket.on('broadcast', (payload) => handlePayload(payload, 'broadcast'))

  socket.onAny((event, payload) => {
    if (['connect', 'disconnect', 'message', 'rates', 'tick', 'broadcast'].includes(event)) return

    if (payload && typeof payload === 'object') {
      const extracted = defaultRateExtractor(vendor.name, payload)
      if (extracted !== null) {
        handlePayload(payload, event)
      }
    }
  })

  socket.on('disconnect', (reason) => {
    vendor.status = 'disconnected'
    vendor.disconnectedAt = nowIso()
    vendor.lastError = reason
    console.warn(`[${vendor.name}] disconnected:`, reason)
    scheduleReconnect(vendor, false)
  })

  socket.on('connect_error', (err) => {
    vendor.status = 'error'
    vendor.lastError = err?.message || 'connect_error'
    console.warn(`[${vendor.name}] connect_error:`, vendor.lastError)
    scheduleReconnect(vendor, true)
  })

  socket.on('error', (err) => {
    vendor.status = 'error'
    vendor.lastError = err?.message || 'error'
    console.warn(`[${vendor.name}] error:`, vendor.lastError)
  })
}
if (vendor.name === 'aamlin') {
  vendor.activeConfig = {
    baseUrl: 'wss://starlinebulltech.in:10001',
    path: '/socket.io/',
    transports: ['websocket'],
  }
}

async function connectVendor(vendor, forceRediscovery = false) {
  if (vendor.socket) {
    try { vendor.socket.removeAllListeners() } catch {}
    try { vendor.socket.close() } catch {}
    vendor.socket = null
  }

  vendor.status = 'discovering'

  if (!vendor.activeConfig || forceRediscovery) {
    const discovered = await discoverWorkingConfig(vendor)

    if (!discovered) {
      vendor.status = 'discovery_failed'
      vendor.lastError = 'No working socket endpoint found'
      console.error(`[${vendor.name}] discovery failed`)
      scheduleReconnect(vendor, true)
      return
    }

    vendor.activeConfig = discovered
    console.log(`[${vendor.name}] discovered config`, discovered)
  }

  vendor.status = 'connecting'

  const socket = io(vendor.activeConfig.baseUrl, {
    path: vendor.activeConfig.path,
    transports: vendor.activeConfig.transports,
    timeout: HTTP_TIMEOUT_MS,
    reconnection: false,
    forceNew: true,
    autoConnect: true,
    withCredentials: false,
  })

  vendor.socket = socket
  attachVendorListeners(vendor, socket)
}

function scheduleReconnect(vendor, forceRediscovery = false) {
  if (vendor.reconnectTimer) return

  vendor.attempts += 1
  const delay = backoff(vendor.attempts)

  vendor.reconnectTimer = setTimeout(async () => {
    vendor.reconnectTimer = null
    if (forceRediscovery) vendor.activeConfig = null
    await connectVendor(vendor, forceRediscovery)
  }, delay)

  console.log(`[${vendor.name}] reconnect scheduled in ${delay}ms`)
}

async function bootstrapConnections() {
  await connectVendor(state.vendors.ambica, true)
  await connectVendor(state.vendors.aamlin, true)
}

app.get('/health', (_req, res) => {
  const vendorHealth = Object.fromEntries(
    Object.entries(state.vendors).map(([name, vendor]) => [
      name,
      {
        status: vendor.status,
        connectedAt: vendor.connectedAt,
        disconnectedAt: vendor.disconnectedAt,
        lastMessageAt: vendor.lastMessageAt,
        lastRate: vendor.lastRate,
        lastRateAt: vendor.lastRateAt,
        lastError: vendor.lastError,
        activeConfig: vendor.activeConfig,
        attempts: vendor.attempts,
        discoveryLog: vendor.discoveryLog.slice(0, 5),
      },
    ])
  )

  res.json({
    ok: true,
    startedAt: state.startedAt,
    now: nowIso(),
    latestRates: state.latestRates,
    lastSaveAt: state.lastSaveAt,
    lastSaveResult: state.lastSaveResult,
    vendors: vendorHealth,
  })
})

app.get('/rates', (_req, res) => {
  res.json({
    fetched_at: nowIso(),
    ...state.latestRates,
  })
})

app.post('/rediscover/:vendor', async (req, res) => {
  const vendor = state.vendors[req.params.vendor]

  if (!vendor) {
    return res.status(404).json({ ok: false, error: 'Unknown vendor' })
  }

  try {
    await connectVendor(vendor, true)
    res.json({
      ok: true,
      vendor: req.params.vendor,
      activeConfig: vendor.activeConfig,
    })
  } catch (err) {
    res.status(500).json({
      ok: false,
      error: err?.message || 'rediscovery_failed',
    })
  }
})

app.get('/', (_req, res) => {
  res.send('Gold rates proxy is running')
})

registerVendors()
startSaveLoop()

app.listen(PORT, async () => {
  console.log(`[Server] Running on port ${PORT}`)
  await bootstrapConnections()
})
