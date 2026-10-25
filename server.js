const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');
const path = require('path');
require('dotenv').config();

const app = express();
app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.static(path.join(__dirname, 'public')));

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

// GET /api/sync?roomKey=gv_cong_nghe_2026
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

// POST /api/sync
app.post('/api/sync', async (req, res) => {
  try {
    const { roomKey, state } = req.body;
    if (!roomKey || !state) {
      return res.status(400).json({ error: 'Thiếu roomKey hoặc state' });
    }

    const existing = await ClassroomState.findOne({ roomKey });
    if (existing) {
      existing.backups.unshift({ savedAt: new Date(), state: existing.state });
      if (existing.backups.length > 20) {
        existing.backups.length = 20;
      }
      existing.state = state;
      existing.updatedAt = new Date();
      await existing.save();
    } else {
      await ClassroomState.create({
        roomKey,
        updatedAt: new Date(),
        state,
        backups: [{ savedAt: new Date(), state }]
      });
    }

    return res.json({ ok: true, roomKey, updatedAt: new Date().toISOString() });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 Server Quản Lý Lớp Học đang chạy tại cổng ${PORT}`);
});
