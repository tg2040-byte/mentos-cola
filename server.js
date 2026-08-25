/**
 * server.js
 * メントスコーラ「投げ込め！」ゲームの中継サーバー。
 *
 * - 静的ファイル配信（public/ 以下）
 * - WebSocketで PC(display) と スマホ(controller) をルームコードでペアリングし、
 *   スマホの「投げる」動作（スイングの強さ・向き）をリアルタイムでPCへ中継する
 * - PC側はコーラの位置を難易度ごとに変え、届いた投球データで着弾判定・演出を行う
 *
 * 起動: node server.js
 * PCブラウザ:  http://<このPCのIP>:8787/display.html
 * スマホブラウザ: http://<このPCのIP>:8787/controller.html
 */

const http = require('http');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');

const PORT = process.env.PORT || 8787;
const PUBLIC_DIR = path.join(__dirname, 'public');

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.ico': 'image/x-icon',
};

const httpServer = http.createServer((req, res) => {
  let reqPath = decodeURIComponent(req.url.split('?')[0]);
  if (reqPath === '/') reqPath = '/display.html';
  const filePath = path.join(PUBLIC_DIR, reqPath);

  // ディレクトリトラバーサル対策
  if (!filePath.startsWith(PUBLIC_DIR)) {
    res.writeHead(403);
    res.end('Forbidden');
    return;
  }

  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('Not Found: ' + reqPath);
      return;
    }
    const ext = path.extname(filePath);
    res.writeHead(200, { 'Content-Type': MIME[ext] || 'application/octet-stream' });
    res.end(data);
  });
});

const wss = new WebSocket.Server({ server: httpServer });

/** roomCode -> { display: ws|null, controller: ws|null } */
const rooms = new Map();

function generateRoomCode() {
  let code;
  do {
    code = String(Math.floor(1000 + Math.random() * 9000)); // 4桁
  } while (rooms.has(code));
  return code;
}

function send(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function cleanupSocket(ws) {
  for (const [code, room] of rooms.entries()) {
    let changed = false;
    if (room.display === ws) { room.display = null; changed = true; }
    if (room.controller === ws) { room.controller = null; changed = true; }
    if (changed) {
      // 相手に切断を通知
      send(room.display, { type: 'peer_disconnected' });
      send(room.controller, { type: 'peer_disconnected' });
      if (!room.display && !room.controller) {
        rooms.delete(code);
      }
    }
  }
}

wss.on('connection', (ws) => {
  ws.roomCode = null;
  ws.role = null;

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch (e) {
      return;
    }

    switch (msg.type) {
      case 'create_room': {
        const code = generateRoomCode();
        rooms.set(code, { display: ws, controller: null });
        ws.roomCode = code;
        ws.role = 'display';
        send(ws, { type: 'room_created', code });
        break;
      }

      case 'join_room': {
        const room = rooms.get(msg.code);
        if (!room || !room.display) {
          send(ws, { type: 'join_error', message: 'ルームが見つかりません' });
          return;
        }
        room.controller = ws;
        ws.roomCode = msg.code;
        ws.role = 'controller';
        send(ws, { type: 'joined_ok', code: msg.code });
        send(room.display, { type: 'controller_joined' });
        break;
      }

      case 'request_start': {
        const room = rooms.get(ws.roomCode);
        if (!room) return;
        // 少し先の時刻を「開始時刻」として両者に同時配信し、体感の同期ズレを減らす
        const startAt = Date.now() + 600;
        const timeoutMs = msg.timeoutMs || 8000;
        const difficulty = msg.difficulty || 'normal';
        send(room.display, { type: 'round_start', startAt, timeoutMs, difficulty });
        send(room.controller, { type: 'round_start', startAt, timeoutMs, difficulty });
        break;
      }

      case 'throw': {
        // スマホが検知した投球（強さ・左右方向）をPCへ中継
        const room = rooms.get(ws.roomCode);
        if (!room) return;
        send(room.display, { type: 'throw_received', power: msg.power, dirX: msg.dirX });
        break;
      }

      case 'throw_timeout': {
        // 制限時間内に投球が検知できなかった場合
        const room = rooms.get(ws.roomCode);
        if (!room) return;
        send(room.display, { type: 'throw_timeout' });
        break;
      }

      case 'reset_round': {
        const room = rooms.get(ws.roomCode);
        if (!room) return;
        send(room.display, { type: 'reset_round' });
        send(room.controller, { type: 'reset_round' });
        break;
      }

      default:
        break;
    }
  });

  ws.on('close', () => cleanupSocket(ws));
  ws.on('error', () => cleanupSocket(ws));
});

httpServer.listen(PORT, () => {
  console.log(`メントスコーラゲーム サーバー起動: http://localhost:${PORT}`);
  console.log(`PC(表示側):    http://192.168.11.6:${PORT}/display.html`);
  console.log(`スマホ(操作側): http://192.168.11.6:${PORT}/controller.html`);
});
