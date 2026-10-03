# -*- coding: utf-8 -*-
"""
液位数据接收服务器 + Web 展示 + 历史数据浏览 + 存水量计算

运行:  python3 dtu_server.py
浏览:  http://服务器IP:5000
"""

import socket
import sqlite3
import threading
import time
from contextlib import closing

from flask import Flask, jsonify, render_template_string, request, Response

# ==================== 配置区 ====================
FULL_SCALE_M      = 6.0          # 液位计量程（米）
AD_4MA            = 655          # 4mA 对应 AD 值
AD_20MA           = 3276         # 20mA 对应 AD 值
LEVEL_MODE        = "4-20mA"     # "4-20mA" 或 "ratio"

# --- 池子参数（用于计算存水量）---
# 已知：5 米液位 = 750 立方
# 底面积 = 750 ÷ 5 = 150 m²
POOL_AREA_M2      = 150.0        # 池子底面积（平方米）
POOL_MAX_LEVEL_M  = 5.0          # 池子有效深度（米），液位超过此值不再增加存水量

DTU_PORT          = 8088
WEB_PORT          = 5000
DEVICE_ADDR       = 1
VERIFY_CRC        = True
STORE_MIN_INTERVAL = 1.0
DB_PATH           = "level.db"
KEEP_DAYS         = 30
MAX_POINTS        = 800
TABLE_MAX_ROWS    = 3000
# ================================================

app = Flask(__name__)

latest = {"ad": 0, "level": 0.0, "ts": 0.0, "online": False}
latest_lock = threading.Lock()
_last_store_ts = 0.0


# ---------------- 数据库 ----------------
def db_connect():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with closing(db_connect()) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS level_data (
                id    INTEGER PRIMARY KEY AUTOINCREMENT,
                ts    REAL    NOT NULL,
                ad    INTEGER NOT NULL,
                level REAL    NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_level_ts ON level_data(ts)")
        conn.commit()


def db_insert(ts, ad, level):
    with closing(db_connect()) as conn:
        conn.execute(
            "INSERT INTO level_data (ts, ad, level) VALUES (?, ?, ?)",
            (ts, ad, level),
        )
        conn.commit()


def db_query(start_ts, end_ts=None, limit=None):
    if end_ts is None:
        end_ts = time.time() + 1
    with closing(db_connect()) as conn:
        rows = conn.execute(
            "SELECT ts, ad, level FROM level_data WHERE ts >= ? AND ts <= ? ORDER BY ts",
            (start_ts, end_ts),
        ).fetchall()
    return rows, limit


def db_query_stats(start_ts, end_ts=None):
    if end_ts is None:
        end_ts = time.time() + 1
    with closing(db_connect()) as conn:
        row = conn.execute(
            "SELECT MAX(level) as max_l, MIN(level) as min_l, AVG(level) as avg_l, "
            "COUNT(*) as cnt FROM level_data WHERE ts >= ? AND ts <= ?",
            (start_ts, end_ts),
        ).fetchone()
    if row and row["max_l"] is not None:
        return {
            "max": round(row["max_l"], 3),
            "min": round(row["min_l"], 3),
            "avg": round(row["avg_l"], 3),
            "count": row["cnt"],
        }
    return None


def db_cleanup():
    cutoff = time.time() - KEEP_DAYS * 86400
    try:
        with closing(db_connect()) as conn:
            n = conn.execute("DELETE FROM level_data WHERE ts < ?", (cutoff,)).rowcount
            conn.commit()
        if n:
            print(f"[{time.ctime()}] 清理过期数据 {n} 条")
    except Exception as e:
        print(f"清理失败: {e}")


def downsample(rows, max_points):
    n = len(rows)
    if n <= max_points or max_points <= 1:
        return rows
    step = (n - 1) / (max_points - 1)
    out, last = [], -1
    for i in range(max_points):
        idx = int(round(i * step))
        if idx != last:
            out.append(rows[idx])
            last = idx
    return out


# ---------------- 数据换算 ----------------
def ad_to_level(ad):
    if LEVEL_MODE == "ratio":
        ratio = ad / 4095.0
    else:
        ratio = (ad - AD_4MA) / float(AD_20MA - AD_4MA)
    ratio = max(0.0, min(1.0, ratio))
    return round(ratio * FULL_SCALE_M, 3)


def level_to_volume(level):
    """液位 -> 存水量（m³）。超过池子最大深度时封顶"""
    if level <= 0:
        return 0.0
    lv = min(level, POOL_MAX_LEVEL_M)
    return lv * POOL_AREA_M2


# ---------------- Modbus RTU 解析 ----------------
def crc16_modbus(data):
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc


def parse_frame(frame):
    if len(frame) < 7:
        return None
    if frame[0] != DEVICE_ADDR or frame[1] != 0x03:
        return None
    byte_count = frame[2]
    if byte_count < 2 or len(frame) != 3 + byte_count + 2:
        return None
    if VERIFY_CRC:
        if crc16_modbus(frame[:-2]) != (frame[-2] | (frame[-1] << 8)):
            return None
    return (frame[3] << 8) | frame[4]


def extract_frames(buf):
    head = bytes([DEVICE_ADDR, 0x03])
    frames = []
    while True:
        idx = buf.find(head)
        if idx < 0:
            buf = buf[-1:] if buf[-1:] == head[:1] else b""
            break
        if idx > 0:
            buf = buf[idx:]
        if len(buf) < 3:
            break
        byte_count = buf[2]
        if byte_count < 2 or byte_count > 250:
            buf = buf[2:]
            continue
        frame_len = 3 + byte_count + 2
        if len(buf) < frame_len:
            break
        frames.append(buf[:frame_len])
        buf = buf[frame_len:]
    return frames, buf


# ---------------- 数据处理 ----------------
def handle_sample(ad):
    global _last_store_ts
    level = ad_to_level(ad)
    now = time.time()

    with latest_lock:
        latest.update(ad=ad, level=level, ts=now, online=True)

    if now - _last_store_ts >= STORE_MIN_INTERVAL:
        _last_store_ts = now
        try:
            db_insert(now, ad, level)
        except Exception as e:
            print(f"入库失败: {e}")

    vol = level_to_volume(level)
    print(f"[{time.strftime('%H:%M:%S')}] AD={ad}  液位={level:.3f} m  存水={vol:.1f} m³")


def mark_offline():
    with latest_lock:
        latest["online"] = False


# ---------------- DTU 监听 ----------------
def dtu_listener():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("0.0.0.0", DTU_PORT))
    s.listen(5)
    print(f"[{time.ctime()}] 正在监听 DTU 端口 {DTU_PORT} ...")

    while True:
        conn, addr = s.accept()
        conn.settimeout(300)
        print(f"[{time.ctime()}] DTU 已连接: {addr}")
        buffer = b""
        try:
            while True:
                data = conn.recv(1024)
                if not data:
                    break
                buffer += data
                frames, buffer = extract_frames(buffer)
                for frame in frames:
                    ad = parse_frame(frame)
                    if ad is not None:
                        handle_sample(ad)
        except socket.timeout:
            print(f"[{time.ctime()}] DTU 超时无数据")
        except Exception as e:
            print(f"[{time.ctime()}] 连接异常: {e}")
        finally:
            conn.close()
            mark_offline()
            print(f"[{time.ctime()}] DTU 断开，等待重连 ...")


# ---------------- Web 界面 ----------------
INDEX_HTML = r"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>调节池液位监测</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-zoom@2.0.1/dist/chartjs-plugin-zoom.min.js"></script>
<style>
  :root {
    --bg-color: #F5F7FA;
    --card-bg: #FFFFFF;
    --text-main: #1F2937;
    --text-sub: #6B7280;
    --text-mute: #9CA3AF;
    --accent-blue: #3B82F6;
    --accent-green: #10B981;
    --accent-red: #EF4444;
    --accent-teal: #14B8A6;
    --border: #E5E7EB;
    --shadow: 0 10px 25px -5px rgba(0, 0, 0, 0.05), 0 8px 10px -6px rgba(0, 0, 0, 0.01);
    --radius: 16px;
    --grid-color: rgba(156, 163, 175, 0.15);
  }
  body.dark-mode {
    --bg-color: #111827;
    --card-bg: #1F2937;
    --text-main: #F9FAFB;
    --text-sub: #9CA3AF;
    --text-mute: #6B7280;
    --border: #374151;
    --shadow: 0 10px 25px -5px rgba(0, 0, 0, 0.3), 0 8px 10px -6px rgba(0, 0, 0, 0.2);
    --grid-color: rgba(156, 163, 175, 0.12);
  }
  * { box-sizing: border-box; }
  body {
    margin: 0; padding: 32px 24px; min-height: 100vh;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    background-color: var(--bg-color); color: var(--text-main);
    -webkit-font-smoothing: antialiased;
    transition: background-color 0.3s, color 0.3s;
  }
  .wrap { max-width: 1280px; margin: 0 auto; }
  .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 24px; }
  h1 { font-size: 22px; font-weight: 600; margin: 0; letter-spacing: -0.5px; }
  .theme-btn {
    background: var(--card-bg); border: 1px solid var(--border); color: var(--text-sub);
    padding: 6px 14px; border-radius: 999px; cursor: pointer; font-size: 13px; font-weight: 500;
    transition: all 0.2s;
  }
  .theme-btn:hover { border-color: var(--accent-blue); color: var(--accent-blue); }

  .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 20px; margin-bottom: 20px; }
  .card {
    background: var(--card-bg); border-radius: var(--radius);
    padding: 22px 24px; box-shadow: var(--shadow);
    display: flex; flex-direction: column; justify-content: center;
    transition: background-color 0.3s, box-shadow 0.3s;
  }
  .card .label { font-size: 13px; color: var(--text-sub); font-weight: 500; margin-bottom: 8px; letter-spacing: 0.5px; }
  .card .value { font-size: 36px; font-weight: 600; color: var(--text-main); line-height: 1.2; }
  .card .value .unit { font-size: 16px; font-weight: 400; color: var(--text-sub); margin-left: 2px; }
  .card .sub { font-size: 13px; color: var(--text-mute); margin-top: 6px; }
  .card.volume .value { color: var(--accent-teal); }
  .dot { display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 8px; vertical-align: middle; }
  .dot.on { background: var(--accent-green); box-shadow: 0 0 0 3px rgba(16, 185, 129, 0.15); }
  .dot.off { background: var(--accent-red); box-shadow: 0 0 0 3px rgba(239, 68, 68, 0.15); }

  .chart-box {
    background: var(--card-bg); border-radius: var(--radius);
    padding: 24px; box-shadow: var(--shadow);
    transition: background-color 0.3s, box-shadow 0.3s;
  }
  .chart-header {
    display: flex; flex-direction: column; gap: 12px; margin-bottom: 16px;
  }
  .chart-header .title-row {
    display: flex; justify-content: space-between; align-items: center;
    flex-wrap: wrap; gap: 12px;
  }
  .chart-header .title { color: var(--text-main); font-size: 16px; font-weight: 600; }
  .view-actions { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }

  .time-btns {
    display: flex; gap: 6px; flex-wrap: wrap;
  }
  .time-btns button {
    background: var(--bg-color); border: 1px solid transparent; color: var(--text-sub);
    padding: 5px 12px; border-radius: 999px; cursor: pointer; font-size: 12.5px;
    font-weight: 500; transition: all 0.2s ease;
  }
  .time-btns button:hover { background: #E5E7EB; color: var(--text-main); }
  body.dark-mode .time-btns button:hover { background: #374151; }
  .time-btns button.active {
    background: var(--accent-blue); color: #FFFFFF;
    box-shadow: 0 4px 12px rgba(59, 130, 246, 0.3);
  }

  .view-btns { display: flex; gap: 6px; }
  .view-btns button, .yaxis-btn {
    background: var(--bg-color); border: 1px solid transparent; color: var(--text-sub);
    padding: 6px 14px; border-radius: 999px; cursor: pointer; font-size: 13px;
    font-weight: 500; transition: all 0.2s ease;
  }
  .view-btns button:hover, .yaxis-btn:hover { background: #E5E7EB; color: var(--text-main); }
  body.dark-mode .view-btns button:hover, body.dark-mode .yaxis-btn:hover { background: #374151; }
  .view-btns button.active {
    background: var(--accent-blue); color: #FFFFFF;
    box-shadow: 0 4px 12px rgba(59, 130, 246, 0.3);
  }
  .yaxis-btn.active { background: var(--accent-green); color: #FFF; box-shadow: 0 4px 12px rgba(16,185,129,0.3); }
  .export-btn {
    background: var(--accent-blue); border: none; color: #FFF;
    padding: 6px 14px; border-radius: 999px; cursor: pointer; font-size: 13px; font-weight: 500;
    transition: opacity 0.2s;
  }
  .export-btn:hover { opacity: 0.85; }

  .custom-panel {
    display: none; gap: 12px; align-items: center; flex-wrap: wrap;
    padding: 14px 18px; background: var(--bg-color); border-radius: 12px;
    margin-bottom: 16px; font-size: 13px; color: var(--text-sub);
  }
  .custom-panel.show { display: flex; }
  .custom-panel label { display: flex; align-items: center; gap: 6px; }
  .custom-panel input[type="datetime-local"] {
    padding: 6px 10px; border: 1px solid var(--border); border-radius: 8px;
    background: var(--card-bg); color: var(--text-main); font-size: 13px;
    font-family: inherit;
  }
  .custom-panel button {
    background: var(--accent-blue); border: none; color: #FFF;
    padding: 6px 16px; border-radius: 8px; cursor: pointer; font-size: 13px;
  }

  #chartWrap { position: relative; height: 440px; width: 100%; }
  #chartWrap.hidden { display: none; }
  #tableWrap { display: none; max-height: 440px; overflow-y: auto; border-radius: 10px; border: 1px solid var(--border); }
  #tableWrap.show { display: block; }

  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  thead th {
    position: sticky; top: 0; background: var(--bg-color); color: var(--text-sub);
    padding: 10px 14px; text-align: left; font-weight: 500;
    border-bottom: 1px solid var(--border);
  }
  tbody td { padding: 8px 14px; border-bottom: 1px solid var(--grid-color); color: var(--text-main); }
  tbody tr:last-child td { border-bottom: none; }
  tbody tr:hover { background: var(--bg-color); }
  .td-level { font-weight: 600; color: var(--accent-blue); }
  .td-vol { font-weight: 600; color: var(--accent-teal); }

  .stats-bar {
    display: flex; flex-wrap: wrap; gap: 24px; margin-top: 20px;
    padding-top: 18px; border-top: 1px solid var(--border);
    font-size: 13px; color: var(--text-sub);
  }
  .stats-bar .stat { display: flex; align-items: baseline; gap: 6px; }
  .stats-bar .stat b { color: var(--text-main); font-weight: 600; font-size: 15px; }

  @media (max-width: 700px) {
    body { padding: 16px; }
    .card .value { font-size: 28px; }
    #chartWrap { height: 300px; }
    .stats-bar { gap: 16px; }
  }
</style>
</head>
<body>
<div class="wrap">
  <div class="header">
    <h1>调节池 · 液位监测</h1>
    <button class="theme-btn" id="themeToggle">🌙 暗黑模式</button>
  </div>

  <div class="cards">
    <div class="card">
      <div class="label">当前液位</div>
      <div class="value"><span id="level">--</span><span class="unit">m</span></div>
      <div class="sub" id="updated">--</div>
    </div>
    <div class="card volume" id="volumeCard">
      <div class="label">当前存水量</div>
      <div class="value"><span id="volume">--</span><span class="unit">m³</span></div>
      <div class="sub" id="volumeSub">--</div>
    </div>
    <div class="card">
      <div class="label">近 1 小时波动</div>
      <div class="value"><span id="range1h">--</span><span class="unit">m</span></div>
      <div class="sub">最高 <span id="hmax">--</span> · 最低 <span id="hmin">--</span> m</div>
    </div>
    <div class="card">
      <div class="label">24 小时极值</div>
      <div class="value" style="font-size:22px;line-height:1.6">
        <span style="color:var(--accent-red);font-size:20px">▲</span> <span id="max24">--</span> m<br>
        <span style="color:var(--accent-green);font-size:20px">▼</span> <span id="min24">--</span> m
      </div>
    </div>
    <div class="card">
      <div class="label">DTU 状态</div>
      <div class="value" style="font-size:22px;font-weight:500"><span class="dot off" id="dot"></span><span id="status">未连接</span></div>
      <div class="sub" id="sampleCount">--</div>
    </div>
  </div>

  <div class="chart-box">
    <div class="chart-header">
      <div class="title-row">
        <div class="title">历史数据</div>
        <div class="view-actions">
          <div class="view-btns" id="viewBtns">
            <button data-view="chart" class="active">曲线</button>
            <button data-view="table">表格</button>
          </div>
          <button class="yaxis-btn" id="yaxisBtn">Y轴: 自动</button>
          <button class="export-btn" id="exportBtn">导出 CSV</button>
        </div>
      </div>
      <div class="time-btns" id="timeBtns">
        <button data-h="1" class="active">1小时</button>
        <button data-h="3">3小时</button>
        <button data-h="6">6小时</button>
        <button data-h="12">12小时</button>
        <button data-h="24">24小时</button>
        <button data-h="48">2天</button>
        <button data-h="72">3天</button>
        <button data-h="168">7天</button>
        <button data-h="360">15天</button>
        <button data-h="720">1个月</button>
        <button id="customBtn">自定义</button>
      </div>
    </div>

    <div class="custom-panel" id="customPanel">
      <label>起始 <input type="datetime-local" id="customStart" step="60"></label>
      <label>结束 <input type="datetime-local" id="customEnd" step="60"></label>
      <button id="applyCustom">应用</button>
    </div>

    <div id="chartWrap"><canvas id="chart"></canvas></div>
    <div id="tableWrap">
      <table>
        <thead>
          <tr>
            <th style="width:30%">时间</th>
            <th style="width:20%">AD 值</th>
            <th style="width:25%">液位 (m)</th>
            <th style="width:25%">存水量 (m³)</th>
          </tr>
        </thead>
        <tbody id="tableBody"></tbody>
      </table>
    </div>

    <div class="stats-bar">
      <div class="stat">最高 <b id="statMax">--</b> m</div>
      <div class="stat">最低 <b id="statMin">--</b> m</div>
      <div class="stat">平均 <b id="statAvg">--</b> m</div>
      <div class="stat">波动 <b id="statRange">--</b> m</div>
      <div class="stat">样本 <b id="statCount">--</b> 条</div>
    </div>
  </div>
</div>

<script>
const FULL_SCALE = {{ full_scale }};
const POOL_AREA = {{ pool_area }};
const POOL_MAX_LEVEL = {{ pool_max_level }};
let hours = 1;
let mode = 'quick';
let customStart = null;
let customEnd = null;
let currentView = 'chart';
let yAuto = true;
let currentSpanHours = 1;

const el = id => document.getElementById(id);
const pad = n => String(n).padStart(2, '0');

if (POOL_AREA <= 0) {
  el('volumeCard').style.display = 'none';
}

// ===== 存水量换算（与后端一致，超过池子深度封顶）=====
function levelToVolume(level) {
  if (level <= 0) return 0;
  const lv = Math.min(level, POOL_MAX_LEVEL);
  return lv * POOL_AREA;
}

function fmtFull(ts) {
  const d = new Date(ts * 1000);
  return d.getFullYear() + '-' + pad(d.getMonth() + 1) + '-' + pad(d.getDate()) +
         ' ' + pad(d.getHours()) + ':' + pad(d.getMinutes()) + ':' + pad(d.getSeconds());
}
function smartTimeFormat(ts, spanHours) {
  const d = new Date(ts * 1000);
  if (spanHours <= 6) {
    return pad(d.getHours()) + ':' + pad(d.getMinutes()) + ':' + pad(d.getSeconds());
  } else if (spanHours <= 24) {
    return pad(d.getHours()) + ':' + pad(d.getMinutes());
  } else if (spanHours <= 24 * 15) {
    return pad(d.getMonth() + 1) + '-' + pad(d.getDate()) + ' ' +
           pad(d.getHours()) + ':' + pad(d.getMinutes());
  } else {
    return pad(d.getMonth() + 1) + '-' + pad(d.getDate());
  }
}
function toLocalInput(ts) {
  const d = new Date(ts * 1000);
  return d.getFullYear() + '-' + pad(d.getMonth() + 1) + '-' + pad(d.getDate()) +
         'T' + pad(d.getHours()) + ':' + pad(d.getMinutes());
}
function fmtVolume(m3) {
  if (m3 >= 1000) {
    return m3.toLocaleString('zh-CN', { maximumFractionDigits: 1 });
  } else if (m3 >= 10) {
    return m3.toFixed(1);
  } else {
    return m3.toFixed(2);
  }
}

const extremeLabelPlugin = {
  id: 'extremeLabel',
  afterDatasetsDraw(chart) {
    const ctx = chart.ctx;
    const labels = chart.data.labels;
    const span = currentSpanHours;

    [1, 2].forEach(dsIdx => {
      const meta = chart.getDatasetMeta(dsIdx);
      if (!meta || meta.hidden) return;
      const data = chart.data.datasets[dsIdx].data;
      const color = dsIdx === 1 ? '#EF4444' : '#10B981';
      const isTop = dsIdx === 1;

      meta.data.forEach((pt, i) => {
        if (data[i] === null || data[i] === undefined || !pt) return;
        const ts = parseFloat(labels[i]);
        if (isNaN(ts)) return;

        const text = smartTimeFormat(ts, span);
        ctx.save();
        ctx.font = '600 11px -apple-system, "Microsoft YaHei", sans-serif';
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';

        const tw = ctx.measureText(text).width;
        const padX = 6;
        const boxW = tw + padX * 2;
        const boxH = 18;
        const boxX = pt.x - boxW / 2;
        const boxY = isTop ? pt.y - 12 - boxH : pt.y + 12;

        const r = 4;
        ctx.beginPath();
        ctx.moveTo(boxX + r, boxY);
        ctx.lineTo(boxX + boxW - r, boxY);
        ctx.quadraticCurveTo(boxX + boxW, boxY, boxX + boxW, boxY + r);
        ctx.lineTo(boxX + boxW, boxY + boxH - r);
        ctx.quadraticCurveTo(boxX + boxW, boxY + boxH, boxX + boxW - r, boxY + boxH);
        ctx.lineTo(boxX + r, boxY + boxH);
        ctx.quadraticCurveTo(boxX, boxY + boxH, boxX, boxY + boxH - r);
        ctx.lineTo(boxX, boxY + r);
        ctx.quadraticCurveTo(boxX, boxY, boxX + r, boxY);
        ctx.closePath();
        ctx.fillStyle = color;
        ctx.fill();

        ctx.fillStyle = '#FFFFFF';
        ctx.fillText(text, pt.x, boxY + boxH / 2 + 0.5);
        ctx.restore();
      });
    });
  }
};

const chart = new Chart(el('chart').getContext('2d'), {
  type: 'line',
  data: {
    labels: [],
    datasets: [
      {
        label: '液位',
        data: [],
        borderColor: '#3B82F6',
        backgroundColor: (ctx) => {
          const c = ctx.chart.ctx;
          const area = ctx.chart.chartArea;
          if (!area) return 'rgba(59, 130, 246, 0.1)';
          const g = c.createLinearGradient(0, area.top, 0, area.bottom);
          g.addColorStop(0, 'rgba(59, 130, 246, 0.28)');
          g.addColorStop(0.6, 'rgba(59, 130, 246, 0.06)');
          g.addColorStop(1, 'rgba(59, 130, 246, 0)');
          return g;
        },
        borderWidth: 2.2,
        pointRadius: 0,
        pointHoverRadius: 5,
        pointHoverBackgroundColor: '#3B82F6',
        pointHoverBorderColor: '#FFFFFF',
        pointHoverBorderWidth: 2,
        tension: 0.35,
        fill: true,
        order: 3
      },
      {
        label: '最高点',
        data: [],
        borderColor: '#EF4444',
        backgroundColor: '#EF4444',
        pointRadius: 5,
        pointHoverRadius: 7,
        pointBorderColor: '#FFFFFF',
        pointBorderWidth: 2,
        showLine: false,
        order: 1
      },
      {
        label: '最低点',
        data: [],
        borderColor: '#10B981',
        backgroundColor: '#10B981',
        pointRadius: 5,
        pointHoverRadius: 7,
        pointBorderColor: '#FFFFFF',
        pointBorderWidth: 2,
        showLine: false,
        order: 2
      }
    ]
  },
  options: {
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 300 },
    interaction: { mode: 'index', intersect: false },
    layout: { padding: { top: 40, right: 20, bottom: 10, left: 6 } },
    scales: {
      x: {
        ticks: {
          color: '#9CA3AF',
          maxTicksLimit: 8,
          maxRotation: 0,
          autoSkip: true,
          font: { size: 12 },
          callback: function(value) {
            let label;
            if (typeof this.getLabelForValue === 'function') {
              label = this.getLabelForValue(value);
            } else {
              label = chart.data.labels[value];
            }
            const ts = parseFloat(label);
            if (isNaN(ts)) return label || '';
            return smartTimeFormat(ts, currentSpanHours);
          }
        },
        grid: { display: false, drawBorder: false }
      },
      y: {
        beginAtZero: false,
        ticks: {
          color: '#9CA3AF',
          callback: v => v.toFixed(2) + ' m',
          font: { size: 12 },
          padding: 8
        },
        grid: {
          color: 'rgba(156, 163, 175, 0.18)',
          drawBorder: false,
          borderDash: [4, 4]
        }
      }
    },
    plugins: {
      legend: { display: false },
      tooltip: {
        backgroundColor: 'rgba(17, 24, 39, 0.94)',
        titleColor: '#E5E7EB',
        titleFont: { size: 12, weight: '600' },
        bodyColor: '#FFFFFF',
        bodyFont: { size: 14, weight: 'bold' },
        padding: 12,
        cornerRadius: 8,
        displayColors: false,
        callbacks: {
          title: function(items) {
            const idx = items[0].dataIndex;
            const label = chart.data.labels[idx];
            const ts = parseFloat(label);
            if (!isNaN(ts)) return fmtFull(ts);
            return items[0].label;
          },
          label: function(ctx) {
            if (ctx.datasetIndex === 0) {
              let s = '液位: ' + ctx.parsed.y.toFixed(3) + ' m';
              if (POOL_AREA > 0) {
                s += '  存水: ' + fmtVolume(levelToVolume(ctx.parsed.y)) + ' m³';
              }
              return s;
            }
            return (ctx.datasetIndex === 1 ? '▲ 最高: ' : '▼ 最低: ') + ctx.parsed.y.toFixed(3) + ' m';
          }
        }
      },
      zoom: {
        pan: { enabled: true, mode: 'x' },
        zoom: {
          wheel: { enabled: true, speed: 0.1 },
          pinch: { enabled: true },
          mode: 'x'
        }
      }
    }
  },
  plugins: [extremeLabelPlugin]
});

function computeYRange(points) {
  if (!yAuto) return { min: 0, max: FULL_SCALE };
  if (!points.length) return { min: 0, max: FULL_SCALE };
  let lo = Infinity, hi = -Infinity;
  for (const p of points) {
    if (p[1] < lo) lo = p[1];
    if (p[1] > hi) hi = p[1];
  }
  const span = hi - lo;
  const pad = Math.max(span * 0.3, 0.05);
  let min = lo - pad;
  let max = hi + pad;
  if (max - min < 0.2) {
    const mid = (max + min) / 2;
    min = mid - 0.1;
    max = mid + 0.1;
  }
  if (min < 0) min = 0;
  if (max > FULL_SCALE) max = FULL_SCALE;
  return { min, max };
}

function buildQuery() {
  if (mode === 'custom' && customStart && customEnd) {
    return 'start=' + customStart + '&end=' + customEnd;
  }
  return 'hours=' + hours;
}

async function refresh() {
  try {
    const res = await fetch('/api/history?' + buildQuery());
    const data = await res.json();

    const L = data.latest;
    if (L.ts > 0) {
      el('level').textContent = L.level.toFixed(3);
      el('updated').textContent = '更新于 ' + fmtFull(L.ts);

      if (POOL_AREA > 0) {
        const vol = levelToVolume(L.level);
        el('volume').textContent = fmtVolume(vol);
        let subText = '≈ ' + fmtVolume(vol) + ' 吨';
        if (L.level >= POOL_MAX_LEVEL) {
          subText += ' · 已满';
        } else {
          const pct = (L.level / POOL_MAX_LEVEL * 100).toFixed(0);
          subText += ' · 池容 ' + pct + '%';
        }
        el('volumeSub').textContent = subText;
      }
    }

    const R1 = data.recent_stats;
    if (R1 && R1.count > 1) {
      el('range1h').textContent = (R1.max - R1.min).toFixed(3);
      el('hmax').textContent = R1.max.toFixed(2);
      el('hmin').textContent = R1.min.toFixed(2);
    } else {
      el('range1h').textContent = '--';
      el('hmax').textContent = '--';
      el('hmin').textContent = '--';
    }

    const R24 = data.day_stats;
    if (R24) {
      el('max24').textContent = R24.max.toFixed(3);
      el('min24').textContent = R24.min.toFixed(3);
    } else {
      el('max24').textContent = '--';
      el('min24').textContent = '--';
    }

    const online = L.online && (Date.now() / 1000 - L.ts < 60);
    el('dot').className = 'dot ' + (online ? 'on' : 'off');
    el('status').textContent = online ? '在线' : '离线';
    if (R1 && R1.count) el('sampleCount').textContent = '近1h ' + R1.count + ' 条';

    const points = data.points;
    currentSpanHours = mode === 'quick' ? hours : (customEnd - customStart) / 3600;

    chart.data.labels = points.map(p => String(Math.round(p[0])));
    chart.data.datasets[0].data = points.map(p => p[1]);

    if (points.length > 1) {
      let maxIdx = 0, minIdx = 0;
      for (let i = 1; i < points.length; i++) {
        if (points[i][1] > points[maxIdx][1]) maxIdx = i;
        if (points[i][1] < points[minIdx][1]) minIdx = i;
      }
      const diff = points[maxIdx][1] - points[minIdx][1];
      if (diff > 0.005) {
        chart.data.datasets[1].data = points.map((p, i) => i === maxIdx ? p[1] : null);
        chart.data.datasets[2].data = points.map((p, i) => i === minIdx ? p[1] : null);
      } else {
        chart.data.datasets[1].data = [];
        chart.data.datasets[2].data = [];
      }
    } else {
      chart.data.datasets[1].data = [];
      chart.data.datasets[2].data = [];
    }

    const yRange = computeYRange(points);
    chart.options.scales.y.min = yRange.min;
    chart.options.scales.y.max = yRange.max;

    chart.update('none');

    const tbody = el('tableBody');
    tbody.innerHTML = '';
    if (points.length > 0) {
      const frag = document.createDocumentFragment();
      for (let i = points.length - 1; i >= 0; i--) {
        const [ts, level] = points[i];
        const tr = document.createElement('tr');
        const adVal = Math.round((level / FULL_SCALE) * (3276 - 655) + 655);
        const vol = levelToVolume(level);
        tr.innerHTML = '<td>' + fmtFull(ts) + '</td>' +
                       '<td>' + adVal + '</td>' +
                       '<td class="td-level">' + level.toFixed(3) + '</td>' +
                       '<td class="td-vol">' + fmtVolume(vol) + '</td>';
        frag.appendChild(tr);
      }
      tbody.appendChild(frag);
    }

    const RS = data.range_stats;
    if (RS) {
      el('statMax').textContent = RS.max.toFixed(3);
      el('statMin').textContent = RS.min.toFixed(3);
      el('statAvg').textContent = RS.avg.toFixed(3);
      el('statRange').textContent = (RS.max - RS.min).toFixed(3);
      el('statCount').textContent = RS.count;
    } else {
      ['statMax','statMin','statAvg','statRange','statCount'].forEach(id => el(id).textContent = '--');
    }
  } catch (e) {
    console.error(e);
  }
}

el('yaxisBtn').addEventListener('click', () => {
  yAuto = !yAuto;
  el('yaxisBtn').textContent = yAuto ? 'Y轴: 自动' : 'Y轴: 0-' + FULL_SCALE + 'm';
  el('yaxisBtn').classList.toggle('active', !yAuto);
  refresh();
});

el('timeBtns').addEventListener('click', e => {
  const btn = e.target.closest('button');
  if (!btn) return;
  if (btn.id === 'customBtn') {
    el('customPanel').classList.toggle('show');
    if (el('customPanel').classList.contains('show')) {
      const now = Math.floor(Date.now() / 1000);
      if (!el('customStart').value) el('customStart').value = toLocalInput(now - 3600);
      if (!el('customEnd').value)   el('customEnd').value   = toLocalInput(now);
    }
    return;
  }
  hours = Number(btn.dataset.h);
  mode = 'quick';
  [...el('timeBtns').children].forEach(b => {
    if (b.id !== 'customBtn') b.classList.toggle('active', b === btn);
  });
  el('customBtn').classList.remove('active');
  el('customPanel').classList.remove('show');
  refresh();
});

el('applyCustom').addEventListener('click', () => {
  const s = el('customStart').value;
  const e2 = el('customEnd').value;
  if (!s || !e2) { alert('请填写完整的起始和结束时间'); return; }
  const sTs = Math.floor(new Date(s).getTime() / 1000);
  const eTs = Math.floor(new Date(e2).getTime() / 1000);
  if (isNaN(sTs) || isNaN(eTs) || sTs >= eTs) { alert('时间范围无效'); return; }
  customStart = sTs;
  customEnd = eTs;
  mode = 'custom';
  [...el('timeBtns').children].forEach(b => b.classList.remove('active'));
  el('customBtn').classList.add('active');
  refresh();
});

el('viewBtns').addEventListener('click', e => {
  const btn = e.target.closest('button');
  if (!btn) return;
  currentView = btn.dataset.view;
  [...el('viewBtns').children].forEach(b => b.classList.toggle('active', b === btn));
  if (currentView === 'chart') {
    el('chartWrap').classList.remove('hidden');
    el('tableWrap').classList.remove('show');
    chart.update('none');
  } else {
    el('chartWrap').classList.add('hidden');
    el('tableWrap').classList.add('show');
  }
});

el('exportBtn').addEventListener('click', () => {
  window.open('/api/export?' + buildQuery());
});

el('themeToggle').addEventListener('click', () => {
  document.body.classList.toggle('dark-mode');
  const isDark = document.body.classList.contains('dark-mode');
  el('themeToggle').textContent = isDark ? '☀️ 明亮模式' : '🌙 暗黑模式';
  chart.update('none');
});

refresh();
setInterval(refresh, 5000);
</script>
</body>
</html>
"""


# ---------------- 路由 ----------------
def parse_range(args):
    now = time.time()
    if args.get("start") and args.get("end"):
        try:
            start = float(args.get("start"))
            end = float(args.get("end"))
            if end <= start:
                raise ValueError
            if end - start > KEEP_DAYS * 86400:
                start = end - KEEP_DAYS * 86400
            return start, end, (end - start) / 3600
        except (TypeError, ValueError):
            pass
    try:
        hours = float(args.get("hours", 1))
    except (TypeError, ValueError):
        hours = 1
    hours = max(0.05, min(hours, KEEP_DAYS * 24))
    return now - hours * 3600, now, hours


@app.route("/")
def index():
    return render_template_string(
        INDEX_HTML,
        full_scale=FULL_SCALE_M,
        pool_area=POOL_AREA_M2,
        pool_max_level=POOL_MAX_LEVEL_M,
    )


@app.route("/api/history")
def api_history():
    start, end, _ = parse_range(request.args)

    rows, _ = db_query(start, end)
    sampled = downsample(rows, MAX_POINTS)

    with latest_lock:
        snap = dict(latest)

    recent_stats = db_query_stats(time.time() - 3600)
    day_stats    = db_query_stats(time.time() - 86400)
    range_stats  = db_query_stats(start, end)

    return jsonify({
        "latest": snap,
        "points": [[r["ts"], r["level"]] for r in sampled],
        "recent_stats": recent_stats,
        "day_stats":    day_stats,
        "range_stats":  range_stats,
    })


@app.route("/api/export")
def api_export():
    start, end, hours = parse_range(request.args)

    rows, _ = db_query(start, end)
    sampled = downsample(rows, TABLE_MAX_ROWS)

    def generate():
        if POOL_AREA_M2 > 0:
            yield "时间,原始AD值,液位(米),存水量(m³)\n"
            for r in sampled:
                t_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(r["ts"]))
                vol = level_to_volume(r["level"])
                yield f"{t_str},{r['ad']},{r['level']},{vol:.2f}\n"
        else:
            yield "时间,原始AD值,液位(米)\n"
            for r in sampled:
                t_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(r["ts"]))
                yield f"{t_str},{r['ad']},{r['level']}\n"

    fname = f"level_{time.strftime('%Y%m%d_%H%M', time.localtime(start))}_{time.strftime('%Y%m%d_%H%M', time.localtime(end))}.csv"
    return Response(
        generate(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={fname}"}
    )


# ---------------- 启动 ----------------
if __name__ == "__main__":
    init_db()
    db_cleanup()

    threading.Thread(target=dtu_listener, daemon=True).start()

    print(f"[{time.ctime()}] Web 界面已启动: http://本机IP:{WEB_PORT}")
    print(f"[{time.ctime()}] 池子参数: 底面积 {POOL_AREA_M2} m², 有效深度 {POOL_MAX_LEVEL_M} m, "
          f"最大存水量 {POOL_AREA_M2 * POOL_MAX_LEVEL_M} m³")
    app.run(host="0.0.0.0", port=WEB_PORT, threaded=True, debug=False, use_reloader=False)