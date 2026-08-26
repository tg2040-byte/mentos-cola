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

// 挑戦者の記録（サーバーのメモリ上に保持。プロセス再起動で消える点に注意）
/** @type {Array<{id:number, name:string, timeMs:number, difficulty:string, ts:number}>} */
const leaderboard = [];
let nextScoreId = 1;
const MAX_LEADERBOARD_ENTRIES = 200;

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

function broadcastLeaderboard() {
  const payload = { type: 'leaderboard_update', leaderboard };
  wss.clients.forEach((client) => send(client, payload));
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
        send(ws, { type: 'leaderboard_update', leaderboard });
        break;
      }

      case 'new_challenger': {
        // PC側は繋ぎっぱなしのまま、新しい挑戦者のスマホ用に新しいルームコードを発行する。
        // 前の挑戦者のスマホとのペアリングは切り離す。
        const oldRoom = rooms.get(ws.roomCode);
        if (oldRoom) {
          send(oldRoom.controller, { type: 'peer_disconnected' });
          if (oldRoom.controller) oldRoom.controller.roomCode = null;
          rooms.delete(ws.roomCode);
        }
        const code = generateRoomCode();
        rooms.set(code, { display: ws, controller: null });
        ws.roomCode = code;
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
        // タイムアタック開始。命中するまで何度でも投げ続けられるので、
        // ここでは「開始時刻」だけ両者に同時配信し、あとはスマホ側が自走して
        // 投げる→結果を受け取る→(外れたら)再待機、を繰り返す。
        const room = rooms.get(ws.roomCode);
        if (!room) return;
        const startAt = Date.now() + 600;
        const difficulty = msg.difficulty || 'normal';
        const name = (msg.name || '').toString().slice(0, 20) || '名無し';
        send(room.display, { type: 'round_start', startAt, difficulty, name });
        send(room.controller, { type: 'round_start', startAt, difficulty, name });
        break;
      }

      case 'throw': {
        // スマホが検知した投球（強さ・左右方向）をPCへ中継
        const room = rooms.get(ws.roomCode);
        if (!room) return;
        send(room.display, { type: 'throw_received', power: msg.power, dirX: msg.dirX });
        break;
      }

      case 'throw_result': {
        // PC側で判定した命中/外れの結果をスマホへ中継。
        // 外れなら、スマホは自動で次の1投の待機に戻る。
        const room = rooms.get(ws.roomCode);
        if (!room) return;
        // hitの場合、PC側が計測した正式なタイムも一緒に渡す（スマホ側で別に計測すると
        // カウントダウン分のズレが出るため、表示は必ずPC側の値を使わせる）
        send(room.controller, { type: 'throw_result', result: msg.result, elapsedMs: msg.elapsedMs });
        break;
      }

      case 'submit_score': {
        // 命中してタイムアタック終了。記録をランキングに追加して全員に配信する。
        const timeMs = Number(msg.timeMs);
        if (!Number.isFinite(timeMs) || timeMs <= 0) return;
        const entry = {
          id: nextScoreId++,
          name: (msg.name || '').toString().slice(0, 20) || '名無し',
          timeMs,
          difficulty: msg.difficulty || 'normal',
          ts: Date.now(),
        };
        leaderboard.push(entry);
        leaderboard.sort((a, b) => a.timeMs - b.timeMs);
        if (leaderboard.length > MAX_LEADERBOARD_ENTRIES) {
          leaderboard.length = MAX_LEADERBOARD_ENTRIES;
        }
        broadcastLeaderboard();
        break;
      }

      case 'get_leaderboard': {
        send(ws, { type: 'leaderboard_update', leaderboard });
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
  console.log(`メントスコーラゲーム サーバー起動 (port ${PORT})`);

  if (process.env.RENDER_EXTERNAL_URL) {
    // Render等、公開URLが環境変数で分かる場合はそれを案内する
    const base = process.env.RENDER_EXTERNAL_URL;
    console.log(`PC(表示側):    ${base}/display.html`);
    console.log(`スマホ(操作側): ${base}/controller.html`);
  } else {
    // ローカル起動時は、同じWi-Fi内のPCのIPアドレスでアクセスする
    console.log(`PC(表示側):    http://localhost:${PORT}/display.html`);
    console.log(`スマホ(操作側): http://<このPCのIP>:${PORT}/controller.html  ※PCのIPアドレスに置き換えてください`);
  }
});
