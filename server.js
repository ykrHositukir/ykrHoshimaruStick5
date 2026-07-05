/**
 * Stick Fight Online - WebSocket relay + game host server
 *
 * 通常:  npm start          → 同一WiFi / LAN
 * 別WiFi: npm run tunnel     → ポート開放不要（インターネット経由）
 *
 * Open: http://localhost:8765
 */
const http = require("http");
const fs = require("fs");
const path = require("path");
const os = require("os");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8765;
const HOST = "0.0.0.0";
const USE_TUNNEL = process.argv.includes("--tunnel") || process.env.TUNNEL === "1";
const MAX_ROOMS = 200;
const ROOM_TTL_MS = 30 * 60 * 1000;

const MIME = {
  ".html": "text/html; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".gif": "image/gif",
  ".svg": "image/svg+xml",
  ".json": "application/json; charset=utf-8",
  ".ico": "image/x-icon",
};

const rooms = new Map();
let tunnelInfo = { http: null, ws: null };

function getLocalIPs() {
  const ips = [];
  const nets = os.networkInterfaces();
  for (const name of Object.keys(nets)) {
    for (const net of nets[name] || []) {
      if (net.family === "IPv4" && !net.internal) ips.push(net.address);
    }
  }
  return ips;
}

function genCode() {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let code = "";
  for (let i = 0; i < 4; i++) code += chars[Math.floor(Math.random() * chars.length)];
  return rooms.has(code) ? genCode() : code;
}

function send(ws, data) {
  if (ws.readyState === 1) ws.send(JSON.stringify(data));
}

function broadcast(room, data, exceptWs) {
  room.members.forEach((m) => {
    if (m.ws !== exceptWs) send(m.ws, data);
  });
}

function roomInfo(room) {
  return {
    code: room.code,
    hostSlot: room.hostSlot,
    members: room.members.map((m) => ({
      slot: m.slot,
      name: m.name,
      ready: m.ready,
      isHost: m.slot === room.hostSlot,
    })),
    playing: room.playing,
  };
}

function cleanupRooms() {
  const now = Date.now();
  for (const [code, room] of rooms) {
    if (now - room.lastActive > ROOM_TTL_MS) rooms.delete(code);
  }
}
setInterval(cleanupRooms, 60000);

function serveStatic(req, res) {
  let urlPath = req.url.split("?")[0];
  if (urlPath === "/") urlPath = "/index.html";
  const rel = urlPath.replace(/^\/+/, "");
  const filePath = path.normalize(path.join(__dirname, rel));
  if (!filePath.startsWith(__dirname)) {
    res.writeHead(403);
    res.end("Forbidden");
    return;
  }
  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404);
      res.end("Not found");
      return;
    }
    const ext = path.extname(filePath).toLowerCase();
    res.writeHead(200, { "Content-Type": MIME[ext] || "application/octet-stream" });
    res.end(data);
  });
}

const server = http.createServer((req, res) => {
  if (req.url === "/api/info" || req.url.startsWith("/api/info?")) {
    res.writeHead(200, {
      "Content-Type": "application/json; charset=utf-8",
      "Access-Control-Allow-Origin": "*",
    });
    res.end(
      JSON.stringify({
        port: PORT,
        ips: getLocalIPs(),
        tunnel: tunnelInfo.http,
        tunnelWs: tunnelInfo.ws,
        tunnelEnabled: USE_TUNNEL,
      })
    );
    return;
  }
  serveStatic(req, res);
});

const wss = new WebSocketServer({ server });

wss.on("connection", (ws) => {
  let member = null;

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }

    if (msg.type === "create") {
      if (rooms.size >= MAX_ROOMS) {
        send(ws, { type: "error", message: "サーバーが満員です" });
        return;
      }
      const code = genCode();
      const name = (msg.name || "Host").slice(0, 12);
      member = { ws, slot: 0, name, ready: true, roomCode: code };
      const room = {
        code,
        hostSlot: 0,
        members: [member],
        playing: false,
        lastActive: Date.now(),
      };
      rooms.set(code, room);
      send(ws, { type: "joined", ...roomInfo(room), yourSlot: 0 });
      return;
    }

    if (msg.type === "join") {
      const code = (msg.code || "").toUpperCase().slice(0, 4);
      const room = rooms.get(code);
      if (!room) {
        send(ws, { type: "error", message: "ルームが見つかりません" });
        return;
      }
      if (room.playing) {
        send(ws, { type: "error", message: "試合中のルームです" });
        return;
      }
      if (room.members.length >= 4) {
        send(ws, { type: "error", message: "ルームが満員です" });
        return;
      }
      const used = new Set(room.members.map((m) => m.slot));
      let slot = 0;
      while (used.has(slot) && slot < 4) slot++;
      const name = (msg.name || `P${slot + 1}`).slice(0, 12);
      member = { ws, slot, name, ready: false, roomCode: code };
      room.members.push(member);
      room.lastActive = Date.now();
      send(ws, { type: "joined", ...roomInfo(room), yourSlot: slot });
      broadcast(room, { type: "lobby", ...roomInfo(room) });
      return;
    }

    if (!member) return;
    const room = rooms.get(member.roomCode || msg.room);
    if (!room) return;
    room.lastActive = Date.now();

    if (msg.type === "ready") {
      member.ready = !!msg.ready;
      broadcast(room, { type: "lobby", ...roomInfo(room) });
      return;
    }

    if (msg.type === "start") {
      if (member.slot !== room.hostSlot) return;
      const humans = room.members.length;
      if (humans < 2) {
        send(ws, { type: "error", message: "2人以上必要です" });
        return;
      }
      room.playing = true;
      const startMsg = {
        type: "start",
        stageSeed: Date.now(),
        humanSlots: room.members.map((m) => m.slot),
        roomSettings: msg.roomSettings || null,
      };
      room.members.forEach((m) => send(m.ws, startMsg));
      return;
    }

    if (msg.type === "input") {
      if (member.slot !== msg.slot) return;
      const host = room.members.find((m) => m.slot === room.hostSlot);
      if (host) {
        send(host.ws, { type: "input", slot: msg.slot, input: msg.input, seq: msg.seq || 0 });
      }
      return;
    }

    if (msg.type === "state") {
      if (member.slot !== room.hostSlot) return;
      broadcast(room, { type: "state", state: msg.state }, ws);
      return;
    }

    if (msg.type === "chat") {
      broadcast(room, {
        type: "chat",
        slot: member.slot,
        name: member.name,
        text: (msg.text || "").slice(0, 80),
      });
    }
  });

  ws.on("close", () => {
    if (!member) return;
    for (const [code, room] of rooms) {
      const idx = room.members.findIndex((m) => m.ws === ws);
      if (idx === -1) continue;
      const wasHost = member.slot === room.hostSlot;
      room.members.splice(idx, 1);
      if (room.members.length === 0) {
        rooms.delete(code);
      } else {
        if (wasHost) {
          room.hostSlot = room.members[0].slot;
          room.members[0].ready = true;
          room.playing = false;
        }
        broadcast(room, { type: "lobby", ...roomInfo(room) });
        if (room.playing) broadcast(room, { type: "host_left" });
      }
      break;
    }
  });
});

async function startTunnel() {
  try {
    const localtunnel = require("localtunnel");
    const tunnel = await localtunnel({ port: Number(PORT) });
    tunnelInfo.http = tunnel.url;
    tunnelInfo.ws = tunnel.url.replace(/^http:/, "ws:").replace(/^https:/, "wss:");
    console.log("");
    console.log("  *** ポート開放不要・インターネット公開 ***");
    console.log(`  公開URL:     ${tunnelInfo.http}`);
    console.log(`  友達に共有:  ${tunnelInfo.ws}`);
    console.log("  → 友達はサーバー欄に上のWebSocket URLを入力");
    tunnel.on("close", () => {
      tunnelInfo = { http: null, ws: null };
      console.log("Tunnel closed. Reconnecting in 5s...");
      setTimeout(startTunnel, 5000);
    });
  } catch (e) {
    console.warn("Tunnel failed:", e.message);
    console.warn("Try: npm install   then: npm run tunnel");
  }
}

server.listen(PORT, HOST, () => {
  const ips = getLocalIPs();
  console.log("Stick Fight Online server running");
  console.log(`  Game:      http://localhost:${PORT}`);
  ips.forEach((ip) => console.log(`  LAN game:  http://${ip}:${PORT}`));
  if (USE_TUNNEL) {
    startTunnel();
  } else {
    console.log("");
    console.log("  別WiFiの友達と遊ぶ（ポート開放不要）:");
    console.log("    npm run tunnel");
  }
});
