import express from 'express'
import { io } from 'socket.io-client'
import { createClient } from '@supabase/supabase-js'

const app = express()
app.use(express.json())

const PORT = process.env.PORT || 3000

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY,
  { auth: { autoRefreshToken: false, persistSession: false } }
)

const state = {
  ambica: { rate: null, socket: null },
  aamlin: { rate: null, socket: null },
}

// ------------------ RATE EXTRACTOR ------------------
function extractRate(payload, keyword) {
  if (!payload) return null

  const str = JSON.stringify(payload).toUpperCase()

  if (!str.includes(keyword)) return null

  const numbers = str.match(/\d{5,}/g)
  if (!numbers) return null

  return Number(numbers[0])
}

// ------------------ AMBICAA ------------------
function connectAmbica() {
  const socket = io("http://dashboard.ambicaaspot.com:10001", {
    path: "/socket.io/",
    transports: ["polling"],
  })

  socket.on("connect", () => {
    console.log("✅ Ambica connected")
    socket.emit("join", "ambicaaspot")
  })

  socket.onAny((event, data) => {
    const rate = extractRate(data, "IND-GOLD")
    if (rate) {
      state.ambica.rate = rate
      console.log("📈 Ambica rate:", rate)
    }
  })

  socket.on("disconnect", () => {
    console.log("❌ Ambica disconnected")
    setTimeout(connectAmbica, 3000)
  })

  state.ambica.socket = socket
}

// ------------------ AAMLIN ------------------
function connectAamlin() {
  const socket = io("wss://starlinebulltech.in:10001", {
    path: "/socket.io/",
    transports: ["polling"],

    extraHeaders: {
      Origin: "http://www.aamlinspot.in",
      "User-Agent": "Mozilla/5.0",
    },
  })

  socket.on("connect", () => {
    console.log("✅ Aamlin connected")
    socket.emit("join", "aamlinspot")
  })

  socket.onAny((event, data) => {
    const rate = extractRate(data, "GOLD 999 IND")
    if (rate) {
      state.aamlin.rate = rate
      console.log("📈 Aamlin rate:", rate)
    }
  })

  socket.on("connect_error", (err) => {
    if (!state.aamlin.lastErrorLogged) {
  console.log("❌ Aamlin error:", err.message)
  state.aamlin.lastErrorLogged = true
}
    setTimeout(connectAamlin, 5000)
  })

  socket.on("disconnect", () => {
    console.log("❌ Aamlin disconnected")
    setTimeout(connectAamlin, 5000)
  })

  state.aamlin.socket = socket
}

// ------------------ SAVE LOOP ------------------
setInterval(async () => {
  if (!state.ambica.rate && !state.aamlin.rate) {
    console.log("No rates yet")
    return
  }

  const payload = {
    fetched_at: new Date().toISOString(),
    ambica_sell_rate: state.ambica.rate,
    aamlin_sell_rate: state.aamlin.rate,
  }

  const { error } = await supabase.from("gold_rates").insert(payload)

  if (error) {
    console.log("❌ Supabase error:", error.message)
  } else {
    console.log("✅ Saved:", payload)
  }
}, 60000)

// ------------------ ROUTES ------------------
app.get("/health", (req, res) => {
  res.json({
    ambica: state.ambica.rate,
    aamlin: state.aamlin.rate,
  })
})

app.get("/", (req, res) => {
  res.send("Gold rates proxy running")
})

// ------------------ START ------------------
app.listen(PORT, () => {
  console.log("🚀 Server started")

  connectAmbica()
  connectAamlin()
})