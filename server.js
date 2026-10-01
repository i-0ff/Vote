// CYBER-VOTE MATRIX — live voting server (Express + Socket.IO)
const express = require("express");
const http = require("http");
const crypto = require("crypto");
const { Server } = require("socket.io");

// Set these as Environment Variables on Render (defaults are only for local testing)
const ADMIN_ID = process.env.ADMIN_ID || "admin";
const ADMIN_PASS = process.env.ADMIN_PASS || "vote2026";
const PORT = process.env.PORT || 3000;
const COLORS = ["#ff3366", "#00ff88", "#00f0ff", "#ff9900"];
const GRACE_MS = 120000; // keep a dropped player (phone locked) for 2 minutes

const app = express();
const server = http.createServer(app);
const io = new Server(server, { pingInterval: 15000, pingTimeout: 25000 });
app.use(express.static("public"));
app.get("/health", (_q, r) => r.send("ok"));

const sessions = new Map(); // token -> {name,color,sid,timer}
const adminTokens = new Set();
const fails = new Map(); // ip -> {n, until}
let round = null; // {id,target,prompt,open,broadcast,votes:Map}
let history = [];
let logs = [];

const log = (m) => { logs.unshift({ t: Date.now(), m }); logs.length = Math.min(logs.length, 80); };
const hash = (s) => crypto.createHash("sha256").update(String(s)).digest();
const same = (a, b) => crypto.timingSafeEqual(hash(a), hash(b));
const playersList = () => [...sessions.values()].map((p) => ({ name: p.name, color: p.color, online: !!p.sid }));
const votesArr = () => (round ? [...round.votes.values()] : []);
const pubRound = () => round && { id: round.id, target: round.target, prompt: round.prompt, open: round.open, broadcast: round.broadcast };

function stateFor(sock) {
  const base = { players: playersList(), round: pubRound() };
  if (sock.data.admin) return { ...base, admin: true, votes: votesArr(), history, logs };
  const t = sock.data.token, s = t && sessions.get(t);
  const mine = s && round && round.votes.get(t);
  return { ...base, admin: false, joined: !!s, myVote: mine ? mine.vote : null, votes: round && round.broadcast ? votesArr() : [] };
}
let queued = false;
function push() {
  if (queued) return;
  queued = true;
  setTimeout(() => { queued = false; for (const [, s] of io.sockets.sockets) s.emit("state", stateFor(s)); }, 40);
}
function endRound() {
  if (!round || !round.open) return;
  round.open = false;
  history.unshift({ id: round.id, target: round.target, prompt: round.prompt, time: Date.now(), votes: votesArr() });
  history = history.slice(0, 200);
  log(`Vote on ${round.target} ended and saved.`);
}

io.on("connection", (socket) => {
  const ip = socket.handshake.headers["x-forwarded-for"]?.split(",")[0] || socket.handshake.address;
  socket.emit("state", stateFor(socket));

  /* ---- players ---- */
  socket.on("join", (d = {}, cb = () => {}) => {
    const token = d.token;
    if (typeof token !== "string" || token.length < 16 || token.length > 64) return cb({ ok: false, error: "Bad session." });
    let s = sessions.get(token);
    if (!s) {
      const name = String(d.name || "").trim().toUpperCase().slice(0, 16);
      if (!name) return cb({ ok: false, error: "Enter a call-sign." });
      if (name === "GLOBAL") return cb({ ok: false, error: "That name is reserved." });
      if ([...sessions.values()].some((p) => p.name === name)) return cb({ ok: false, error: "That call-sign is already in use." });
      if (sessions.size >= 200) return cb({ ok: false, error: "Room is full." });
      s = { name, color: COLORS.includes(d.color) ? d.color : COLORS[2], sid: null, timer: null };
      sessions.set(token, s);
      log(`${name} joined.`);
    }
    if (s.timer) { clearTimeout(s.timer); s.timer = null; }
    s.sid = socket.id;
    socket.data.token = token;
    cb({ ok: true, name: s.name, color: s.color });
    push();
  });

  socket.on("leave", () => {
    const t = socket.data.token, s = sessions.get(t);
    if (s) { if (s.timer) clearTimeout(s.timer); sessions.delete(t); log(`${s.name} left.`); }
    socket.data.token = null;
    push();
  });

  socket.on("vote", (v) => {
    const t = socket.data.token, s = sessions.get(t);
    if (!s || !round || !round.open || (v !== "YES" && v !== "NO")) return;
    if (s.name === round.target || round.votes.has(t)) return; // no self-vote, no double vote
    round.votes.set(t, { vote: v, name: s.name, color: s.color, ts: Date.now() });
    log(`${s.name} voted ${v}.`);
    push();
  });

  /* ---- admin ---- */
  socket.on("admin:login", (d = {}, cb = () => {}) => {
    const f = fails.get(ip) || { n: 0, until: 0 };
    if (Date.now() < f.until) return cb({ ok: false, error: "Too many attempts. Wait 30 seconds." });
    if (same(d.id, ADMIN_ID) && same(d.pass, ADMIN_PASS)) {
      fails.delete(ip);
      const tok = crypto.randomBytes(24).toString("hex");
      adminTokens.add(tok);
      socket.data.admin = true;
      cb({ ok: true, token: tok });
      push();
    } else {
      f.n++; if (f.n >= 5) { f.until = Date.now() + 30000; f.n = 0; }
      fails.set(ip, f);
      setTimeout(() => cb({ ok: false, error: "Wrong ID or password." }), 500);
    }
  });
  socket.on("admin:resume", (tok, cb = () => {}) => {
    if (typeof tok === "string" && adminTokens.has(tok)) { socket.data.admin = true; cb({ ok: true }); push(); }
    else cb({ ok: false });
  });
  socket.on("admin:logout", (tok) => { adminTokens.delete(tok); socket.data.admin = false; });

  const adm = (fn) => (...a) => { if (socket.data.admin) fn(...a); };
  socket.on("admin:start", adm((d = {}, cb = () => {}) => {
    const target = d.target === "GLOBAL" || [...sessions.values()].some((p) => p.name === d.target) ? d.target : null;
    if (!target) return cb({ ok: false, error: "That player is no longer here." });
    endRound();
    const prompt = String(d.prompt || "").trim().slice(0, 120) || "Vote now";
    round = { id: Date.now(), target, prompt, open: true, broadcast: round ? round.broadcast : false, votes: new Map() };
    log(`Vote started on ${target}: ${prompt}`);
    cb({ ok: true }); push();
  }));
  socket.on("admin:end", adm(() => { endRound(); push(); }));
  socket.on("admin:broadcast", adm(() => { if (round) { round.broadcast = !round.broadcast; log(`Broadcast ${round.broadcast ? "ON" : "OFF"}.`); push(); } }));
  socket.on("admin:clear", adm(() => { round = null; history = []; logs = []; log("Everything cleared."); push(); }));

  socket.on("disconnect", () => {
    const t = socket.data.token, s = sessions.get(t);
    if (s && s.sid === socket.id) {
      s.sid = null;
      s.timer = setTimeout(() => { sessions.delete(t); push(); }, GRACE_MS);
      push();
    }
  });
});

server.listen(PORT, "0.0.0.0", () => console.log(`Cyber-Vote running on port ${PORT}`));
