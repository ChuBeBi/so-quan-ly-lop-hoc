const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
require('dotenv').config();

const app = express();
app.use(cors());
app.use(express.json({ limit: '15mb' }));

// Phục vụ các file tĩnh (ưu tiên root, sau đó đến thư mục public)
app.use(express.static(__dirname));
app.use(express.static(path.join(__dirname, 'public')));

// Trả về file index.html khi truy cập trang chủ
app.get('/', (req, res) => {
  const fileToServe = fs.existsSync(path.join(__dirname, 'index.html'))
    ? path.join(__dirname, 'index.html')
    : path.join(__dirname, 'public', 'index.html');
  res.sendFile(fileToServe);
});

const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://127.0.0.1:27017/so_quan_ly_lop_cong_nghe';

mongoose.connect(MONGODB_URI)
  .then(() => console.log('✅ Đã kết nối MongoDB thành công'))
  .catch(err => console.error('❌ Lỗi kết nối MongoDB:', err));

const ClassroomStateSchema = new mongoose.Schema({
  roomKey: { type: String, required: true, unique: true, index: true },
  updatedAt: { type: Date, default: Date.now },
  state: { type: mongoose.Schema.Types.Mixed, required: true },
  backups: [
    {
      savedAt: { type: Date, default: Date.now },
      state: { type: mongoose.Schema.Types.Mixed }
    }
  ]
});

const ClassroomState = mongoose.model('ClassroomState', ClassroomStateSchema);

// Map lưu trữ các kết nối Server-Sent Events (SSE) theo roomKey để đồng bộ tức thì
// roomKey -> Set of response objects
const sseClientsByRoom = new Map();

function broadcastToRoom(roomKey, data) {
  const clients = sseClientsByRoom.get(roomKey);
  if (!clients || clients.size === 0) return;
  const payload = `data: ${JSON.stringify(data)}\n\n`;
  for (const client of clients) {
    try {
      client.write(payload);
    } catch (e) {
      clients.delete(client);
    }
  }
}

// GET /api/sync/stream?roomKey=gv_cong_nghe_2026 (Real-time Live Sync qua SSE)
app.get('/api/sync/stream', (req, res) => {
  const roomKey = req.query.roomKey || 'gv_cong_nghe_2026';

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no'); // Tắt buffering trên Nginx/Render proxy
  if (res.flushHeaders) res.flushHeaders();

  if (!sseClientsByRoom.has(roomKey)) {
    sseClientsByRoom.set(roomKey, new Set());
  }
  const clients = sseClientsByRoom.get(roomKey);
  clients.add(res);

  // Gửi thông báo kết nối thành công
  res.write(`data: ${JSON.stringify({ type: 'CONNECTED', roomKey, time: new Date().toISOString() })}\n\n`);

  // Heartbeat mỗi 20 giây để giữ kết nối không bị Render ngắt
  const heartbeatTimer = setInterval(() => {
    try {
      res.write(': heartbeat\n\n');
    } catch (e) {
      clearInterval(heartbeatTimer);
    }
  }, 20000);

  req.on('close', () => {
    clearInterval(heartbeatTimer);
    clients.delete(res);
    if (clients.size === 0) {
      sseClientsByRoom.delete(roomKey);
    }
  });
});

// GET /api/sync/status?roomKey=gv_cong_nghe_2026 (Kiểm tra mốc thời gian cập nhật nhanh)
app.get('/api/sync/status', async (req, res) => {
  try {
    const roomKey = req.query.roomKey || 'gv_cong_nghe_2026';
    const doc = await ClassroomState.findOne({ roomKey }).select('roomKey updatedAt');
    if (!doc) {
      return res.status(404).json({ message: 'Chưa có dữ liệu' });
    }
    return res.json({ roomKey: doc.roomKey, updatedAt: doc.updatedAt });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

// GET /api/sync?roomKey=gv_cong_nghe_2026 (Lấy toàn bộ dữ liệu mới nhất)
app.get('/api/sync', async (req, res) => {
  try {
    const roomKey = req.query.roomKey || 'gv_cong_nghe_2026';
    const doc = await ClassroomState.findOne({ roomKey });
    if (!doc) {
      return res.status(404).json({ message: 'Chưa có dữ liệu cho mã sổ này' });
    }
    return res.json({
      roomKey: doc.roomKey,
      updatedAt: doc.updatedAt,
      state: doc.state
    });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

// GET /api/sync/backups?roomKey=gv_cong_nghe_2026 (Danh sách các bản sao lưu trên Cloud)
app.get('/api/sync/backups', async (req, res) => {
  try {
    const roomKey = req.query.roomKey || 'gv_cong_nghe_2026';
    const doc = await ClassroomState.findOne({ roomKey });
    if (!doc || !doc.backups) {
      return res.json({ backups: [] });
    }
    const list = doc.backups.map((b, idx) => ({
      index: idx,
      savedAt: b.savedAt
    }));
    return res.json({ roomKey: doc.roomKey, backups: list });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

// POST /api/sync (Lưu dữ liệu và phát tín hiệu Real-time tới các máy khác ngay lập tức)
app.post('/api/sync', async (req, res) => {
  try {
    const { roomKey, state, sourceId } = req.body;
    if (!roomKey || !state) {
      return res.status(400).json({ error: 'Thiếu roomKey hoặc state' });
    }

    const updatedAt = new Date();
    const existing = await ClassroomState.findOne({ roomKey });
    if (existing) {
      existing.backups.unshift({ savedAt: new Date(), state: existing.state });
      if (existing.backups.length > 30) {
        existing.backups.length = 30;
      }
      existing.state = state;
      existing.updatedAt = updatedAt;
      await existing.save();
    } else {
      await ClassroomState.create({
        roomKey,
        updatedAt,
        state,
        backups: [{ savedAt: new Date(), state }]
      });
    }

    // Phát tín hiệu Real-time lập tức tới tất cả các máy khác đang mở
    broadcastToRoom(roomKey, {
      type: 'SYNC_UPDATE',
      roomKey,
      updatedAt: updatedAt.toISOString(),
      sourceId: sourceId || null,
      state
    });

    return res.json({ ok: true, roomKey, updatedAt: updatedAt.toISOString() });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 Server Quản Lý Lớp Học đang chạy tại cổng ${PORT}`);
});
