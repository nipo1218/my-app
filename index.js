// Render用 WebSocketサーバー (Node.js)
// じっけんしつ - 軽量版

const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// プレイヤー管理
const players = new Map(); // id -> playerData
const sockets = new Map(); // id -> socket
const socketToId = new Map(); // socket -> id

// ヘルスチェック用エンドポイント
app.get('/', (req, res) => {
  res.send(`
    <!DOCTYPE html>
    <html>
    <head><title>じっけんしつ Server</title></head>
    <body style="font-family: sans-serif; text-align: center; padding: 50px;">
      <h1>🏠 じっけんしつ WebSocket Server</h1>
      <p>Status: <span style="color: green;">Running on Render</span></p>
      <p>Connected players: <strong>${players.size}</strong></p>
    </body>
    </html>
  `);
});

app.get('/health', (req, res) => {
  res.json({ status: 'ok', players: players.size, timestamp: new Date().toISOString() });
});

app.get('/api/players', (req, res) => {
  res.json(Object.fromEntries(players));
});

// 全クライアントにブロードキャスト
function broadcast(message, excludeId) {
  const data = JSON.stringify(message);
  for (const [id, socket] of sockets) {
    if (id !== excludeId && socket.readyState === 1) { // WebSocket.OPEN = 1
      socket.send(data);
    }
  }
}

// 特定のクライアントに送信
function sendTo(id, message) {
  const socket = sockets.get(id);
  if (socket && socket.readyState === 1) {
    socket.send(JSON.stringify(message));
  }
}

wss.on('connection', (socket) => {
  const playerId = crypto.randomUUID().slice(0, 8);
  sockets.set(playerId, socket);
  socketToId.set(socket, playerId);
  
  console.log(`Connected: ${playerId} (Total: ${sockets.size})`);

  // 接続成功を通知 + 既存プレイヤー一覧を送信
  sendTo(playerId, {
    type: 'connected',
    id: playerId,
    players: Object.fromEntries(players)
  });

  socket.on('message', (message) => {
    const senderId = socketToId.get(socket);
    if (!senderId) return;

    try {
      const payload = JSON.parse(message.toString());
      const type = payload.type;
      const data = payload.data;

      switch (type) {
        case 'join':
          // プレイヤー参加
          const newPlayer = {
            id: senderId,
            x: data.x,
            y: data.y,
            name: data.name,
            charId: data.charId,
            isSitting: data.isSitting || false,
            msg: ''
          };
          players.set(senderId, newPlayer);
          
          // 他のプレイヤーに通知
          broadcast({ type: 'join', id: senderId, data: newPlayer }, senderId);
          break;

        case 'update':
          // 位置更新（差分のみ）
          const player = players.get(senderId);
          if (player) {
            player.x = data.x;
            player.y = data.y;
            player.name = data.name;
            player.charId = data.charId;
            player.isSitting = data.isSitting;
            
            // 他プレイヤーに転送
            broadcast({ type: 'update', id: senderId, data: data }, senderId);
          }
          break;

        case 'chat':
          // チャットメッセージ
          const chatPlayer = players.get(senderId);
          if (chatPlayer) {
            chatPlayer.msg = data.msg;
            broadcast({ type: 'chat', id: senderId, data: { msg: data.msg } });
            
            // 5秒後にメッセージをクリア（サーバー側）
            setTimeout(() => {
              if (players.has(senderId)) {
                players.get(senderId).msg = '';
              }
            }, 5000);
          }
          break;
      }
    } catch (err) {
      console.error('Message error:', err);
    }
  });

  socket.on('close', () => {
    const id = socketToId.get(socket);
    if (id) {
      players.delete(id);
      sockets.delete(id);
      socketToId.delete(socket);
      console.log(`Disconnected: ${id} (Total: ${sockets.size})`);

      // 切断通知を全員に送信
      broadcast({ type: 'leave', id: id });
    }
  });

  socket.on('error', (err) => {
    console.error('WebSocket error:', err);
  });
});

const PORT = process.env.PORT || 10000;
server.listen(PORT, () => {
  console.log(`🚀 じっけんしつ Server running on port ${PORT}`);
});