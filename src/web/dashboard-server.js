/**
 * Web Dashboard Server
 * 
 * 提供系统状态、信号源排名、虚拟仓位表现的 Web 界面
 */

import http from 'http';
import https from 'https';
import fs from 'fs';
import { URL, fileURLToPath } from 'url';
import { dirname, join, isAbsolute } from 'path';
import Database from 'better-sqlite3';
import dotenv from 'dotenv';

dotenv.config();

const PORT = process.env.PORT || 3000;
const dbPath = process.env.DB_PATH || './data/sentiment_arb.db';
const DASHBOARD_TOKEN = process.env.DASHBOARD_TOKEN || '';
const getExperimentLeaderboard = () => [];
const listRecentExperiments = () => [];

/**
 * 验证敏感 API 的访问令牌
 * 需要设置环境变量 DASHBOARD_TOKEN，否则敏感端点被禁用
 */
function checkAuth(req, url, res) {
  if (!DASHBOARD_TOKEN) {
    res.writeHead(403, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'DASHBOARD_TOKEN not configured. Set DASHBOARD_TOKEN env var to enable this endpoint.' }));
    return false;
  }
  const token = url.searchParams.get('token') || req.headers['x-dashboard-token'] || '';
  if (token !== DASHBOARD_TOKEN) {
    res.writeHead(401, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Invalid or missing token' }));
    return false;
  }
  return true;
}

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const projectRoot = join(__dirname, '../..');
const resolvedDbPath = isAbsolute(dbPath) ? dbPath : join(projectRoot, dbPath);

// 内存日志缓冲（保留最近 10000 条）
const MAX_LOG_LINES = 10000;
const logBuffer = [];
const originalConsoleLog = console.log;
const originalConsoleError = console.error;
const originalConsoleWarn = console.warn;

// 日志文件路径
const logsDir = join(projectRoot, 'logs');
const runtimeLogPath = join(logsDir, 'runtime.log');

// 确保日志目录存在
try {
  if (!fs.existsSync(logsDir)) {
    fs.mkdirSync(logsDir, { recursive: true });
  }
} catch (e) { /* ignore */ }

function captureLog(level, args) {
  const timestamp = new Date().toISOString();
  const message = args.map(a => typeof a === 'object' ? JSON.stringify(a) : String(a)).join(' ');
  const logLine = { timestamp, level, message };

  // 内存缓冲
  logBuffer.push(logLine);
  if (logBuffer.length > MAX_LOG_LINES) {
    logBuffer.shift();
  }

  // 写入文件（追加模式）
  try {
    fs.appendFileSync(runtimeLogPath, `[${timestamp}] [${level}] ${message}\n`);
  } catch (e) { /* ignore */ }
}

console.log = (...args) => {
  captureLog('INFO', args);
  originalConsoleLog.apply(console, args);
};

console.error = (...args) => {
  captureLog('ERROR', args);
  originalConsoleError.apply(console, args);
};

console.warn = (...args) => {
  captureLog('WARN', args);
  originalConsoleWarn.apply(console, args);
};

let db;
function getDb() {
  if (!db) {
    try {
      db = new Database(resolvedDbPath);
    } catch (e) {
      console.error('❌ Failed to open database:', e.message);
    }
  }
  return db;
}

function getPaperDbPath() {
  const paperDbPath = process.env.PAPER_DB || './data/paper_trades.db';
  return isAbsolute(paperDbPath) ? paperDbPath : join(projectRoot, paperDbPath);
}

function getTableColumns(database, tableName) {
  return new Set(database.prepare(`PRAGMA table_info(${tableName})`).all().map(row => row.name));
}

function cleanupOpenPaperPositions({ reason = 'manual_cleanup', pnlPct = 0 } = {}) {
  const paperDbPath = getPaperDbPath();
  if (!fs.existsSync(paperDbPath)) {
    const error = new Error(`Paper trades database not found at ${paperDbPath}`);
    error.statusCode = 404;
    throw error;
  }

  const paperDb = new Database(paperDbPath);
  try {
    const tableExists = paperDb.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name='paper_trades'`).get();
    if (!tableExists) {
      const error = new Error('paper_trades table not found');
      error.statusCode = 404;
      throw error;
    }

    const columns = getTableColumns(paperDb, 'paper_trades');
    const openRows = paperDb.prepare(
      `SELECT id, symbol, strategy_stage FROM paper_trades WHERE exit_reason IS NULL ORDER BY id ASC`
    ).all();

    if (!openRows.length) {
      return {
        dbPath: paperDbPath,
        updated: 0,
        reason,
        pnlPct,
        openBefore: 0,
        symbols: [],
      };
    }

    const assignments = [
      `exit_price = COALESCE(exit_price, 0)`,
      `exit_ts = ?`,
      `exit_reason = ?`,
      `pnl_pct = ?`,
    ];
    const includeStageOutcome = columns.has('stage_outcome');
    if (includeStageOutcome) assignments.push(`stage_outcome = ?`);
    if (columns.has('trailing_active')) assignments.push(`trailing_active = 0`);
    if (columns.has('exit_execution_json')) assignments.push(`exit_execution_json = NULL`);
    if (columns.has('exit_quote_failures')) assignments.push(`exit_quote_failures = 0`);
    if (columns.has('last_exit_quote_failure')) assignments.push(`last_exit_quote_failure = NULL`);

    const exitTs = Math.floor(Date.now() / 1000);
    const updateStmt = paperDb.prepare(`
      UPDATE paper_trades
      SET ${assignments.join(',\n          ')}
      WHERE id = ?
    `);

    const updateTxn = paperDb.transaction((rows) => {
      for (const row of rows) {
        const stage = row.strategy_stage || 'stage1';
        const params = [exitTs, reason, pnlPct];
        if (includeStageOutcome) {
          params.push(`${stage}_${reason}`);
        }
        params.push(row.id);
        updateStmt.run(...params);
      }
    });
    updateTxn(openRows);

    return {
      dbPath: paperDbPath,
      updated: openRows.length,
      reason,
      pnlPct,
      openBefore: openRows.length,
      symbols: openRows.slice(0, 20).map(row => row.symbol || `id:${row.id}`),
    };
  } finally {
    paperDb.close();
  }
}

/**
 * HTML 模板
 */
function renderDashboard(data) {
  return `
<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Sentiment Arbitrage Dashboard</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { 
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
      color: #e4e4e4;
      min-height: 100vh;
      padding: 20px;
    }
    .container { max-width: 1400px; margin: 0 auto; }
    h1 { 
      text-align: center; 
      margin-bottom: 30px; 
      color: #00d9ff;
      font-size: 2.5em;
      text-shadow: 0 0 20px rgba(0, 217, 255, 0.3);
    }
    .grid { 
      display: grid; 
      grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); 
      gap: 20px; 
      margin-bottom: 30px;
    }
    .card {
      background: rgba(255, 255, 255, 0.05);
      border-radius: 15px;
      padding: 20px;
      border: 1px solid rgba(255, 255, 255, 0.1);
      backdrop-filter: blur(10px);
    }
    .card h2 {
      color: #00d9ff;
      margin-bottom: 15px;
      font-size: 1.2em;
      display: flex;
      align-items: center;
      gap: 10px;
    }
    .stat-grid {
      display: grid;
      grid-template-columns: repeat(2, 1fr);
      gap: 15px;
    }
    .stat {
      background: rgba(0, 0, 0, 0.2);
      padding: 15px;
      border-radius: 10px;
      text-align: center;
    }
    .stat-value {
      font-size: 2em;
      font-weight: bold;
      color: #00ff88;
    }
    .stat-value.negative { color: #ff4757; }
    .stat-value.neutral { color: #ffa502; }
    .stat-label { color: #888; font-size: 0.9em; margin-top: 5px; }
    
    table {
      width: 100%;
      border-collapse: collapse;
      margin-top: 10px;
    }
    th, td {
      padding: 12px 8px;
      text-align: left;
      border-bottom: 1px solid rgba(255, 255, 255, 0.1);
    }
    th { color: #00d9ff; font-weight: 600; }
    tr:hover { background: rgba(255, 255, 255, 0.05); }
    
    .badge {
      display: inline-block;
      padding: 4px 8px;
      border-radius: 4px;
      font-size: 0.8em;
      font-weight: 600;
    }
    .badge-green { background: rgba(0, 255, 136, 0.2); color: #00ff88; }
    .badge-yellow { background: rgba(255, 165, 2, 0.2); color: #ffa502; }
    .badge-red { background: rgba(255, 71, 87, 0.2); color: #ff4757; }
    
    .exit-strategy {
      background: rgba(0, 217, 255, 0.1);
      border-radius: 10px;
      padding: 15px;
      margin-top: 10px;
    }
    .exit-strategy h3 { color: #00d9ff; margin-bottom: 10px; }
    .exit-rule {
      display: flex;
      justify-content: space-between;
      padding: 8px 0;
      border-bottom: 1px solid rgba(255, 255, 255, 0.05);
    }
    .exit-rule:last-child { border-bottom: none; }
    
    .refresh-info {
      text-align: center;
      color: #666;
      margin-top: 20px;
      font-size: 0.9em;
    }
    
    .pnl-positive { color: #00ff88; }
    .pnl-negative { color: #ff4757; }
    
    .token-address {
      font-family: monospace;
      font-size: 0.85em;
      color: #888;
    }
  </style>
  <!-- 移除 meta refresh，改用 AJAX 轮询 -->
  <script>
    const _token=new URLSearchParams(window.location.search).get('token')||'';
    const _q=_token?'?token='+encodeURIComponent(_token):'';
    const _a=_token?'&token='+encodeURIComponent(_token):'';
    // SOL 余额轮询（每 30 秒，不刷页面）
    async function refreshSolBalance() {
      try {
        const r = await fetch('/api/wallet-balance'+_q);
        const d = await r.json();
        const el = document.getElementById('stat-sol-balance');
        if (el) {
          if (d.balance !== null && d.balance !== undefined) {
            el.textContent = d.balance.toFixed(4);
            el.style.color = d.balance < 0.5 ? '#ff4757' : d.balance < 1 ? '#ffa502' : '#00ff88';
          } else {
            el.textContent = '未知';
          }
        }
      } catch (e) { /* ignore */ }
    }

    // 自动刷新页面 (保持表格数据最新)
    document.addEventListener('DOMContentLoaded', () => {
      refreshSolBalance();
      setInterval(refreshSolBalance, 30000);
      // 每 60 秒刷新一次整页
      setInterval(() => {
        location.reload();
      }, 60000);
    });

    // 立即手动刷新
    function manualRefresh() {
      location.reload();
    }

    // 暂停/恢复交易
    async function toggleTrading(action) {
      if (action === 'pause' && !confirm('确认暂停交易？将暂停4小时。')) return;
      if (action === 'resume' && !confirm('确认恢复交易？连亏计数将重置。')) return;
      try {
        const endpoint = action === 'pause' ? '/api/pause-trading' : '/api/resume-trading';
        const r = await fetch(endpoint + _q, { method: 'POST' });
        const d = await r.json();
        if (d.success) {
          alert(d.message);
          location.reload();
        } else {
          alert('操作失败: ' + (d.error || '未知错误'));
        }
      } catch (e) {
        alert('请求失败: ' + e.message);
      }
    }

    async function resetDailyLoss() {
      if (!confirm('确认重置今日亏损统计起点？\n历史数据不会被删除，仅从当前时间重新开始计算。')) return;
      try {
        const r = await fetch('/api/reset-daily-loss' + _q, { method: 'POST' });
        const d = await r.json();
        if (d.success) {
          alert(d.message);
          location.reload();
        } else {
          alert('操作失败: ' + (d.error || '未知错误'));
        }
      } catch (e) {
        alert('请求失败: ' + e.message);
      }
    }
  </script>
</head>
<body>
  <div class="container">
    <h1>🤖 Sentiment Arbitrage Dashboard <button onclick="manualRefresh()" style="font-size:0.5em;padding:5px 15px;cursor:pointer;background:#00d9ff;border:none;border-radius:5px;color:#000;">🔄 更新</button> <button onclick="location.reload()" style="font-size:0.5em;padding:5px 15px;cursor:pointer;background:#666;border:none;border-radius:5px;color:#fff;margin-left:5px;">↻ 整页刷新</button></h1>
    
    <!-- 系统概览 -->
    <div class="grid">
      <div class="card">
        <h2>📊 系统状态</h2>
        <div class="stat-grid">
          <div class="stat">
            <div class="stat-value" id="stat-mode">${data.overview.mode}</div>
            <div class="stat-label">运行模式</div>
          </div>
          <div class="stat">
            <div class="stat-value" id="stat-channels">${data.overview.channels}</div>
            <div class="stat-label">监控频道</div>
          </div>
          <div class="stat">
            <div class="stat-value" id="stat-signals">${data.overview.signals_today}</div>
            <div class="stat-label">今日信号</div>
          </div>
          <div class="stat">
            <div class="stat-value" id="stat-positions">${data.overview.positions_open}</div>
            <div class="stat-label">持仓数量</div>
          </div>
          <div class="stat">
            <div class="stat-value ${(data.risk.daily_pnl_sol || 0) >= 0 ? '' : ((data.risk.daily_pnl_sol || 0) <= -0.5 ? 'negative' : 'neutral')}" id="stat-pnl">
              ${(data.risk.daily_pnl_sol || 0) >= 0 ? '+' : ''}${(data.risk.daily_pnl_sol || 0).toFixed(2)}
            </div>
            <div class="stat-label">今日 SOL 盈亏</div>
          </div>
          <div class="stat">
            <div class="stat-value ${data.risk.is_paused ? 'negative' : ''}" id="stat-trading-status">
              ${data.risk.is_paused ? '已暂停' : '正常'}
            </div>
            <div class="stat-label">风控状态</div>
            <div style="margin-top:8px;display:flex;gap:6px;justify-content:center;flex-wrap:wrap">
              <button id="btn-pause" onclick="toggleTrading('pause')" style="font-size:0.75em;padding:4px 10px;cursor:pointer;background:#ff4757;border:none;border-radius:4px;color:#fff;${data.risk.is_paused ? 'display:none' : ''}">⏸ 暂停</button>
              <button id="btn-resume" onclick="toggleTrading('resume')" style="font-size:0.75em;padding:4px 10px;cursor:pointer;background:#2ed573;border:none;border-radius:4px;color:#fff;${data.risk.is_paused ? '' : 'display:none'}">▶ 恢复</button>
              <button onclick="resetDailyLoss()" style="font-size:0.75em;padding:4px 10px;cursor:pointer;background:#ffa502;border:none;border-radius:4px;color:#fff;" title="重置今日亏损统计起点，不删除历史数据">🔄 重置今日亏损</button>
            </div>
          </div>
          <div class="stat">
            <div class="stat-value" id="stat-sol-balance">--</div>
            <div class="stat-label">钱包余额 (SOL)</div>
          </div>
        </div>
      </div>
      
      <div class="card">
        <h2>💰 虚拟收益统计</h2>
        <div class="stat-grid">
          <div class="stat">
            <div class="stat-value ${(data.performance.total_pnl || 0) >= 0 ? '' : 'negative'}">${(data.performance.total_pnl || 0) >= 0 ? '+' : ''}${(data.performance.total_pnl || 0).toFixed(1)}%</div>

            <div class="stat-label">总收益率</div>
          </div>
          <div class="stat">
            <div class="stat-value ${data.performance.win_rate >= 50 ? '' : 'neutral'}">${data.performance.win_rate.toFixed(1)}%</div>
            <div class="stat-label">胜率</div>
          </div>
          <div class="stat">
            <div class="stat-value">${data.performance.total_trades}</div>
            <div class="stat-label">总交易数</div>
          </div>
          <div class="stat">
            <div class="stat-value">${data.performance.avg_pnl >= 0 ? '+' : ''}${data.performance.avg_pnl.toFixed(2)}%</div>
            <div class="stat-label">平均收益</div>
          </div>
        </div>
      </div>

      <!-- 观察池概览 -->
      <div class="card">
        <h2>🔭 三级观察池 (Waiting Room)</h2>
        <div class="stat-grid">
          <div class="stat">
            <div class="stat-value" style="color: #ffda44;">${data.observationPool.counts.gold}</div>
            <div class="stat-label">🥇 金池 (待毕业)</div>
          </div>
          <div class="stat">
            <div class="stat-value" style="color: #c0c0c0;">${data.observationPool.counts.silver}</div>
            <div class="stat-label">🥈 银池 (观察中)</div>
          </div>
          <div class="stat">
            <div class="stat-value" style="color: #cd7f32;">${data.observationPool.counts.bronze}</div>
            <div class="stat-label">🥉 铜池 (海选)</div>
          </div>
          <div class="stat">
            <div class="stat-value">${data.observationPool.counts.total}</div>
            <div class="stat-label">当前总数</div>
          </div>
        </div>
        <div style="margin-top: 15px; font-size: 0.85em; color: #888; text-align: center;">
          🥇金池：5min | 🥈银池：8min | 🥉铜池：10min
        </div>
      </div>
    </div>

    <!-- ==================== v7.4 新增模块状态卡片 ==================== -->
    <div class="grid" style="margin-bottom: 20px;">
      <!-- Hunter Performance -->
      <div class="card">
        <h2>🎯 猎人表现 (Hunter Performance)</h2>
        <table style="font-size: 0.9em;">
          <thead>
            <tr>
              <th>类型</th>
              <th>交易数</th>
              <th>胜率</th>
              <th>平均收益</th>
              <th>仓位倍数</th>
            </tr>
          </thead>
          <tbody>
            ${Object.entries(data.hunterPerformance).filter(([type]) => type !== 'UNKNOWN' || data.hunterPerformance[type].trades > 0).map(([type, stats]) => {
    const emoji = type === 'FOX' ? '🦊' : type === 'TURTLE' ? '🐢' : type === 'WOLF' ? '🐺' : '❓';
    const winRate = stats.trades > 0 ? (stats.wins / stats.trades * 100) : 0;
    return `
                <tr>
                  <td>${emoji} ${type}</td>
                  <td>${stats.trades}</td>
                  <td><span class="badge ${winRate >= 50 ? 'badge-green' : winRate >= 30 ? 'badge-yellow' : 'badge-red'}">${winRate.toFixed(1)}%</span></td>
                  <td class="${stats.avgPnl >= 0 ? 'pnl-positive' : 'pnl-negative'}">${stats.avgPnl >= 0 ? '+' : ''}${stats.avgPnl.toFixed(2)}%</td>
                  <td><strong>${stats.multiplier}x</strong></td>
                </tr>
              `;
  }).join('')}
            ${Object.values(data.hunterPerformance).every(s => s.trades === 0) ? '<tr><td colspan="5" style="text-align:center;color:#666;">等待猎人信号数据...</td></tr>' : ''}
          </tbody>
        </table>
        <div style="margin-top: 10px; font-size: 0.8em; color: #888;">
          FOX=金狗猎人 | TURTLE=波段玩家 | WOLF=稳定盈利
        </div>
      </div>

      <!-- Signal Source Distribution -->
      <div class="card">
        <h2>📡 信号来源分布 (v7.4 Lineage)</h2>
        <table style="font-size: 0.9em;">
          <thead>
            <tr>
              <th>来源</th>
              <th>信号数</th>
              <th>胜率</th>
              <th>平均收益</th>
            </tr>
          </thead>
          <tbody>
            ${Object.entries(data.signalSources).filter(([_, stats]) => stats.count > 0).map(([source, stats]) => {
    const emoji = source === 'ultra_sniper_v2' ? '🎯' : source === 'shadow_v2' ? '🥷' : source === 'flash_scout' ? '⚡' : '🔭';
    const displayName = source === 'ultra_sniper_v2' ? 'Ultra Sniper V2' :
      source === 'shadow_v2' ? 'Shadow Protocol V2' :
        source === 'flash_scout' ? 'Flash Scout' : 'Tiered Observer';
    return `
                <tr>
                  <td>${emoji} ${displayName}</td>
                  <td>${stats.count}</td>
                  <td><span class="badge ${stats.winRate >= 50 ? 'badge-green' : stats.winRate >= 30 ? 'badge-yellow' : 'badge-red'}">${stats.winRate.toFixed(1)}%</span></td>
                  <td class="${stats.avgPnl >= 0 ? 'pnl-positive' : 'pnl-negative'}">${stats.avgPnl >= 0 ? '+' : ''}${stats.avgPnl.toFixed(2)}%</td>
                </tr>
              `;
  }).join('')}
            ${Object.values(data.signalSources).every(s => s.count === 0) ? '<tr><td colspan="4" style="text-align:center;color:#666;">等待信号来源数据...</td></tr>' : ''}
          </tbody>
        </table>
        <div style="margin-top: 10px; font-size: 0.8em; color: #888;">
          v7.4 信号血统追踪 | 数据随交易积累
        </div>
      </div>

      <!-- API Gateway 健康状态 -->
      <div class="card">
        <h2>🛡️ API 网关健康 (v7.4.1)</h2>
        <div class="stat-grid" style="grid-template-columns: repeat(3, 1fr);">
          <div class="stat">
            <div class="stat-value" style="font-size:1.5em;">${data.apiHealth.gmgn.circuitBreaker ? '🔴 熔断中' : '🟢 正常'}</div>
            <div class="stat-label">GMGN Gateway</div>
          </div>
          <div class="stat">
            <div class="stat-value" style="font-size:1.2em;">${data.apiHealth.gmgn.requestsToday || 0}</div>
            <div class="stat-label">今日请求数</div>
          </div>
          <div class="stat">
            <div class="stat-value" style="font-size:1.2em;">${data.apiHealth.gmgn.rateLimited || 0}</div>
            <div class="stat-label">限流次数</div>
          </div>
        </div>
        <div style="margin-top: 10px; font-size: 0.8em; color: #888;">
          令牌桶: 10/s | 熔断阈值: 5次失败 | 冷却: 60秒
        </div>
      </div>
    </div>

    <!-- 观察池详情列表 -->
    <div class="card" style="margin-bottom: 20px;">
      <h2>🔬 实时观察队列 (实时动态更新)</h2>
      <table style="font-size: 0.9em;">
        <thead>
          <tr>
            <th>池级</th>
            <th>代币</th>
            <th>链</th>
            <th>分数</th>
            <th>观察时长</th>
            <th>聪明钱 (初→现)</th>
            <th>价格变化</th>
            <th>特征标签</th>
          </tr>
        </thead>
        <tbody>
          ${data.observationPool.tokens.map(t => `
            <tr>
              <td>
                <span class="badge ${t.tier === 'GOLD' ? 'badge-green' : t.tier === 'SILVER' ? 'badge-yellow' : ''}" 
                      style="${t.tier === 'BRONZE' ? 'background:rgba(205,127,50,0.2);color:#cd7f32;' : ''}">
                  ${t.tier === 'GOLD' ? '🥇 GOLD' : t.tier === 'SILVER' ? '🥈 SILVER' : '🥉 BRONZE'}
                </span>
              </td>
              <td><strong>${t.symbol}</strong></td>
              <td><span class="badge ${t.chain === 'SOL' ? 'badge-green' : 'badge-yellow'}">${t.chain}</span></td>
              <td><strong>${t.score}</strong></td>
              <td>${t.observeMinutes} min</td>
              <td>${t.smInitial} → ${t.smCurrent} (${(t.smCurrent - t.smInitial) >= 0 ? '+' : ''}${t.smCurrent - t.smInitial})</td>
              <td class="${parseFloat(t.priceChange) >= 0 ? 'pnl-positive' : 'pnl-negative'}">${parseFloat(t.priceChange) >= 0 ? '+' : ''}${t.priceChange}%</td>
              <td><span class="badge ${t.tag === 'GOLDEN' ? 'badge-green' : 'badge-yellow'}">${t.tag}</span></td>
            </tr>
          `).join('')}
          ${data.observationPool.tokens.length === 0 ? '<tr><td colspan="8" style="text-align:center;color:#666;">观察池当前为空，寻找信号中...</td></tr>' : ''}
        </tbody>
      </table>
    </div>
    
    <!-- 信号源排名 -->
    <div class="card" style="margin-bottom: 20px;">
      <h2>🏆 信号源排名 (按胜率)</h2>
      <table>
        <thead>
          <tr>
            <th>排名</th>
            <th>信号源</th>
            <th>信号数</th>
            <th>胜率</th>
            <th>平均收益</th>
            <th>最佳</th>
            <th>最差</th>
          </tr>
        </thead>
        <tbody>
          ${data.sources.map((s, i) => `
            <tr>
              <td>${i + 1}</td>
              <td>${s.source_id || 'Unknown'}</td>
              <td>${s.total_signals}</td>
              <td><span class="badge ${(s.win_rate || 0) >= 50 ? 'badge-green' : (s.win_rate || 0) >= 30 ? 'badge-yellow' : 'badge-red'}">${(s.win_rate || 0).toFixed(1)}%</span></td>
              <td class="${(s.avg_pnl || 0) >= 0 ? 'pnl-positive' : 'pnl-negative'}">${(s.avg_pnl || 0) >= 0 ? '+' : ''}${(s.avg_pnl || 0).toFixed(2)}%</td>
              <td class="pnl-positive">+${(s.best_pnl || 0).toFixed(1)}%</td>
              <td class="pnl-negative">${(s.worst_pnl || 0).toFixed(1)}%</td>
            </tr>
          `).join('')}
          ${data.sources.length === 0 ? '<tr><td colspan="7" style="text-align:center;color:#666;">暂无数据，系统运行中...</td></tr>' : ''}
        </tbody>
      </table>
    </div>
    
    <!-- 虚拟仓位 -->
    <div class="card" style="margin-bottom: 20px;">
      <h2>📈 虚拟仓位表现</h2>
      <table>
        <thead>
          <tr>
            <th>代币</th>
            <th>链</th>
            <th>实时PnL</th>
            <th>止盈策略</th>
            <th>买入</th>
            <th>已卖出</th>
            <th>入场价</th>
            <th>收益率</th>
            <th>剩余</th>
            <th>状态</th>
            <th>入场时间</th>
            <th>退出时间</th>
            <th>持仓时长</th>
          </tr>
        </thead>

        <tbody>
          ${data.positions.map(p => {
    // v8.0 修复：对于 partial 状态，显示 last_partial_sell_price
    const isPartial = p.status === 'partial';
    const isBreakeven = isPartial && p.breakeven_done === 1;

    // 卖出价格：partial 用 last_partial_sell_price，closed 用 exit_price
    const sellPrice = isPartial ? p.last_partial_sell_price : (p.exit_price || null);
    const exitPrice = sellPrice || p.breakeven_price || p.price_15m || p.price_5m || null;

    // PnL 计算：使用卖出时的价格
    let displayPnl = p.pnl_percent || 0;
    if (isPartial && p.last_partial_sell_price && p.entry_price) {
      // partial 状态：显示卖出时的收益率
      displayPnl = ((p.last_partial_sell_price - p.entry_price) / p.entry_price * 100);
    } else if (isBreakeven && p.breakeven_price && p.entry_price) {
      displayPnl = ((p.breakeven_price - p.entry_price) / p.entry_price * 100);
    }

    const remainingPercent = p.remaining_percent != null ? p.remaining_percent : 100;
    // 买入金额
    const buyAmount = p.position_size_native ? p.position_size_native.toFixed(3) : '-';
    const buyUnit = p.chain === 'SOL' ? 'SOL' : 'BNB';
    const buyUsd = p.position_size_usd ? ('$' + p.position_size_usd.toFixed(0)) : '';

    // v8.0 修复：已卖出金额 = 入场成本 * 卖出% * (1 + 收益率)
    const soldPercent = 100 - remainingPercent;
    const soldMultiplier = displayPnl > 0 ? (1 + displayPnl / 100) : 1;
    const soldAmount = p.position_size_native && soldPercent > 0
      ? (p.position_size_native * soldPercent / 100 * soldMultiplier).toFixed(3)
      : '-';
    const soldUsd = p.position_size_usd && soldPercent > 0
      ? ('$' + (p.position_size_usd * soldPercent / 100 * soldMultiplier).toFixed(0))
      : '';
    // 数据库存的是 UTC 时间，需要加 'Z' 后缀才能正确转换为本地时间
    const parseUTC = (t) => {
      if (!t) return null;
      if (typeof t === 'number') return new Date(t * 1000);
      const str = String(t);
      return new Date(str.includes('Z') || str.includes('+') ? str : str + 'Z');
    };
    const entryTime = p.entry_time ? parseUTC(p.entry_time).toLocaleString('zh-CN') : '-';
    // v9.3: 退出时间和持仓时长
    const exitTime = p.exit_time ? parseUTC(p.exit_time).toLocaleString('zh-CN') : '-';
    const entryDate = p.entry_time ? parseUTC(p.entry_time) : null;
    const exitDate = p.exit_time ? parseUTC(p.exit_time) : new Date();
    let holdDuration = '-';
    if (entryDate) {
      const mins = Math.round((exitDate - entryDate) / 60000);
      if (mins >= 60) {
        holdDuration = Math.floor(mins / 60) + 'h' + (mins % 60) + 'm';
      } else {
        holdDuration = mins + 'min';
      }
    }
    // 状态显示
    let statusText = p.status || '-';
    let statusClass = 'badge-yellow';
    if (isBreakeven) {
      // 已完成翻倍出本
      statusText = '💰已出本';
      statusClass = 'badge-green';
    } else if (isPartial) {
      // partial 但未完成翻倍出本（中途止盈）
      statusText = 'partial';
      statusClass = displayPnl >= 0 ? 'badge-green' : 'badge-yellow';
    } else if (p.status === 'closed') {
      statusClass = displayPnl >= 0 ? 'badge-green' : 'badge-red';
    } else if (p.status === 'open') {
      statusText = 'open';
    }
    // v7.4 猎人类型
    const hunterType = p.signal_hunter_type || '-';
    const hunterEmoji = hunterType === 'FOX' ? '🦊' : hunterType === 'TURTLE' ? '🐢' : hunterType === 'WOLF' ? '🐺' : '';
    // v7.4 信号来源
    const signalSource = p.signal_source || p.entry_source || '-';
    const sourceShort = signalSource.replace('ultra_sniper_v2', 'Ultra').replace('shadow_v2', 'Shadow').replace('flash_scout', 'Flash').replace('tiered_observer', 'Observer');

    // v8.0 实时 PnL 和止盈策略
    const livePnl = p.current_pnl != null ? p.current_pnl : 0;
    const tierStrategy = p.tier_strategy || '';
    // 提取止盈策略的简短显示 (例如从 "等待止盈 (TIER_A: 当前 +10.7%, 目标 +150%, 持仓 26min)" 提取 "TIER_A +150%")
    const tierMatch = tierStrategy.match(/\((TIER_[SsAaBbCc]|默认|DEFAULT)[^)]*目标 \+?(\d+)%/i);
    const tierShort = tierMatch ? `${tierMatch[1].toUpperCase()} +${tierMatch[2]}%` : (p.status === 'open' ? '监控中' : '-');

    return `
            <tr>
              <td>
                <div><strong>${p.symbol || 'Unknown'}</strong></div>
                <div class="token-address" style="font-size:0.7em;word-break:break-all;">${(p.token_ca || 'N/A').slice(0, 12)}...</div>
              </td>
              <td><span class="badge ${p.chain === 'SOL' ? 'badge-green' : 'badge-yellow'}">${p.chain || '-'}</span></td>
              <td class="${livePnl >= 0 ? 'pnl-positive' : 'pnl-negative'}" style="font-weight:bold;">
                ${p.status === 'open' ? ((livePnl >= 0 ? '+' : '') + livePnl.toFixed(1) + '%') : '-'}
              </td>
              <td style="font-size:0.8em;">
                <span class="badge ${tierShort.includes('TIER_S') ? 'badge-green' : tierShort.includes('TIER_A') ? 'badge-green' : 'badge-yellow'}">${tierShort}</span>
              </td>
              <td style="white-space:nowrap;">
                <div>${buyAmount} ${buyUnit}</div>
                <div style="font-size:0.8em;color:#888;">${buyUsd}</div>
              </td>
              <td style="white-space:nowrap;">
                ${soldPercent > 0 ? `<div class="pnl-positive">${soldAmount} ${buyUnit}</div><div style="font-size:0.8em;color:#888;">${soldUsd}</div>` : '-'}
              </td>
              <td>$${p.entry_price ? p.entry_price.toFixed(10) : 'N/A'}</td>
              <td class="${displayPnl >= 0 ? 'pnl-positive' : 'pnl-negative'}">
                ${displayPnl !== 0 ? ((displayPnl >= 0 ? '+' : '') + displayPnl.toFixed(1) + '%') : '-'}
              </td>
              <td>${remainingPercent < 100 ? (remainingPercent.toFixed(0) + '%') : '100%'}</td>
              <td><span class="badge ${statusClass}">${statusText}</span></td>
              <td style="font-size:0.85em;">${entryTime}</td>
              <td style="font-size:0.85em;">${exitTime}</td>
              <td style="font-size:0.85em;">${holdDuration}</td>
            </tr>
          `}).join('')}
          ${data.positions.length === 0 ? '<tr><td colspan="13" style="text-align:center;color:#666;">暂无仓位数据，等待 DeBot 信号通过验证...</td></tr>' : ''}

        </tbody>
      </table>
    </div>
    
    <!-- v18 最近信号记录 -->
    <div class="card" style="margin-bottom: 20px;">
      <h2>🔥 最近信号 (v18 指标过滤)</h2>
      <table>
        <thead>
          <tr>
            <th>代币</th>
            <th>MC</th>
            <th>Super</th>
            <th>SupΔ</th>
            <th>Trade</th>
            <th>结果</th>
          </tr>
        </thead>
        <tbody>
          ${data.recent_scores.map(s => `
            <tr>
              <td>${s.symbol}</td>
              <td>${s.mc}</td>
              <td>${s.superCurrent}</td>
              <td>${s.superDelta}</td>
              <td>${s.tradeCurrent}</td>
              <td><span class="badge ${s.passed ? 'badge-green' : 'badge-red'}">${s.status}</span></td>
            </tr>
          `).join('')}
          ${data.recent_scores.length === 0 ? '<tr><td colspan="6" style="text-align:center;color:#666;">等待新信号...</td></tr>' : ''}
        </tbody>
      </table>
    </div>

    <!-- v18 出场策略 ASYMMETRIC -->
    <div class="card">
      <h2>⚙️ 策略 v18：非对称收割 (ASYMMETRIC)</h2>
      <div class="grid" style="grid-template-columns: repeat(4, 1fr);">
        <div class="exit-strategy">
          <h3>🛑 止损</h3>
          <div class="exit-rule"><span>止损线</span><span class="pnl-negative">${data.config.exitStrategy.stopLoss}</span></div>
          <div class="exit-rule"><span>死水超时</span><span class="pnl-negative">${data.config.exitStrategy.deadWater}</span></div>
          <div class="exit-rule"><span>最大持仓</span><span class="pnl-negative">${data.config.exitStrategy.maxHold}</span></div>
        </div>
        <div class="exit-strategy">
          <h3>💰 分批止盈</h3>
          <div class="exit-rule"><span>TP1</span><span class="pnl-positive">${data.config.exitStrategy.tp1}</span></div>
          <div class="exit-rule"><span>TP2</span><span class="pnl-positive">${data.config.exitStrategy.tp2}</span></div>
        </div>
        <div class="exit-strategy">
          <h3>🚀 高倍止盈</h3>
          <div class="exit-rule"><span>TP3</span><span class="pnl-positive">${data.config.exitStrategy.tp3}</span></div>
          <div class="exit-rule"><span>TP4</span><span class="pnl-positive">${data.config.exitStrategy.tp4}</span></div>
        </div>
        <div class="exit-strategy">
          <h3>📊 仓位</h3>
          <div class="exit-rule"><span>单笔仓位</span><span class="pnl-positive">${data.config.position.sizeSol} SOL</span></div>
          <div class="exit-rule"><span>在险上限</span><span>${data.config.position.maxAtRisk} 个</span></div>
          <div class="exit-rule"><span>Moonbag</span><span>不占槽位</span></div>
        </div>
      </div>
    </div>
    
    <!-- v18 入场过滤条件 -->
    <div class="card" style="margin-top: 20px;">
      <h2>🎯 v18 入场过滤条件</h2>
      <div class="grid" style="grid-template-columns: repeat(4, 1fr);">
        <div class="exit-strategy">
          <h3>📊 市值 & ATH</h3>
          <div class="exit-rule"><span>Market Cap</span><span>${data.config.entryFilters.marketCap}</span></div>
          <div class="exit-rule"><span>ATH</span><span>${data.config.entryFilters.athOnly}</span></div>
        </div>
        <div class="exit-strategy">
          <h3>🔥 Super Index</h3>
          <div class="exit-rule"><span>Super_cur</span><span>${data.config.entryFilters.superIndex}</span></div>
          <div class="exit-rule"><span>SupΔ</span><span>${data.config.entryFilters.superDelta}</span></div>
        </div>
        <div class="exit-strategy">
          <h3>📈 Trade & Address</h3>
          <div class="exit-rule"><span>Trade_cur</span><span>${data.config.entryFilters.tradeCurrent}</span></div>
          <div class="exit-rule"><span>TΔ</span><span>${data.config.entryFilters.tradeDelta}</span></div>
          <div class="exit-rule"><span>Addr_cur</span><span>${data.config.entryFilters.addressCurrent}</span></div>
        </div>
        <div class="exit-strategy">
          <h3>🛡️ 安全 & 防追高</h3>
          <div class="exit-rule"><span>Sec_cur</span><span>${data.config.entryFilters.securityCurrent}</span></div>
          <div class="exit-rule"><span>防追高</span><span>实时MC &gt; 信号MC×1.2 拒绝</span></div>
          <div class="exit-rule"><span>Freeze/Mint</span><span>必须 DISABLED</span></div>
        </div>
      </div>
    </div>
    
    <!-- AI 复盘与策略调整 -->
    <div class="card" style="margin-top: 20px;">
      <h2>🤖 AI 复盘与策略调整</h2>
      <div class="grid" style="grid-template-columns: 1fr 2fr;">
        <div class="stat" style="text-align: left; padding: 15px;">
          <h3 style="color: #00d9ff; margin-bottom: 10px;">⚡ 当前状态</h3>
          <div style="margin-bottom: 8px;">交易状态: <span class="badge ${data.risk.is_paused ? 'badge-red' : 'badge-green'}">${data.risk.is_paused ? '已暂停' : '运行中'}</span></div>
          <div style="margin-bottom: 8px;">今日SOL盈亏: <span class="${data.risk.daily_pnl_sol >= 0 ? 'pnl-positive' : 'pnl-negative'}">${data.risk.daily_pnl_sol >= 0 ? '+' : ''}${(data.risk.daily_pnl_sol || 0).toFixed(4)} SOL</span></div>
          <div>今日BNB盈亏: <span class="${data.risk.daily_pnl_bnb >= 0 ? 'pnl-positive' : 'pnl-negative'}">${data.risk.daily_pnl_bnb >= 0 ? '+' : ''}${(data.risk.daily_pnl_bnb || 0).toFixed(4)} BNB</span></div>
        </div>
        <div>
          <h3 style="color: #00d9ff; margin-bottom: 10px;">📊 动态阈值 (AI 自动调整)</h3>
          <table style="font-size: 0.9em;">
            <thead>
              <tr>
                <th>参数</th>
                <th>当前值</th>
                <th>更新时间</th>
                <th>更新者</th>
              </tr>
            </thead>
            <tbody>
              ${data.thresholds.map(t => `
                <tr>
                  <td><code>${t.key}</code></td>
                  <td><strong>${t.value}</strong></td>
                  <td style="font-size:0.85em;">${t.updated_at || '-'}</td>
                  <td><span class="badge ${t.updated_by === 'AI_AUTO_REVIEW' ? 'badge-green' : 'badge-yellow'}">${t.updated_by}</span></td>
                </tr>
              `).join('')}
              ${data.thresholds.length === 0 ? '<tr><td colspan="4" style="text-align:center;color:#666;">暂无阈值数据</td></tr>' : ''}
            </tbody>
          </table>
        </div>
      </div>
      
      <!-- AI 复盘历史记录 -->
      <div style="margin-top: 20px;">
        <h3 style="color: #00d9ff; margin-bottom: 10px;">📜 AI 复盘历史</h3>
        <table style="font-size: 0.9em;">
          <thead>
            <tr>
              <th>时间</th>
              <th>触发原因</th>
              <th>交易数</th>
              <th>胜率</th>
              <th>关键洞察</th>
              <th>优先行动</th>
            </tr>
          </thead>
          <tbody>
            ${data.reviewHistory.map(r => `
              <tr>
                <td style="font-size:0.85em;">${r.review_time || '-'}</td>
                <td><span class="badge ${r.trigger_reason === 'consecutive_losses' ? 'badge-red' : 'badge-yellow'}">${r.trigger_reason === 'consecutive_losses' ? '连续亏损' : r.trigger_reason}</span></td>
                <td>${r.trade_count || 0}</td>
                <td><span class="badge ${r.win_rate >= 50 ? 'badge-green' : 'badge-red'}">${(r.win_rate || 0).toFixed(1)}%</span></td>
                <td style="max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="${r.ai_key_insight || ''}">${r.ai_key_insight || '-'}</td>
                <td style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="${r.priority_action || ''}">${r.priority_action || '-'}</td>
              </tr>
            `).join('')}
            ${data.reviewHistory.length === 0 ? '<tr><td colspan="6" style="text-align:center;color:#666;">尚未进行过 AI 复盘</td></tr>' : ''}
          </tbody>
        </table>
      </div>
    </div>
    
    <div class="refresh-info">
      数据每30秒自动更新 (不刷新页面) | 最后更新: <span id="last-refresh-time">${new Date().toLocaleString('zh-CN')}</span>
    </div>
  </div>
</body>
</html>
`;
}

/**
 * v18 策略配置（与 premium-signal-engine.js 保持同步）
 * 不再使用 CrossValidator 评分，改用信号指标直接过滤
 */
const V18_STRATEGY_CONFIG = {
  // 入场条件（信号指标硬过滤）
  entryFilters: {
    marketCap: '30K - 300K',
    superIndex: '80 - 1000',
    superDelta: '≥ 5',
    tradeCurrent: '≥ 1',
    tradeDelta: '≥ 1',
    addressCurrent: '≥ 3',
    securityCurrent: '≥ 15',
    athOnly: 'ATH#1 直接入场'
  },
  // 仓位
  position: {
    sizeSol: parseFloat(process.env.PREMIUM_POSITION_SOL || '0.06'),
    maxAtRisk: 5,
    moonbagNotCounted: true
  },
  // 出场策略 ASYMMETRIC
  exitStrategy: {
    tp1: '+45-60%区间峰值 → 卖60% (SL移至0%)',
    tp2: '+100% → 卖50%剩余',
    tp3: '+200% → 卖50%剩余',
    tp4: '+500% → 卖80%剩余 → Moonbag',
    stopLoss: '-35%',
    deadWater: '15分钟无波动',
    maxHold: '30分钟'
  }
};

/**
 * 获取仪表盘数据
 */
function getDashboardData() {
  const data = {
    overview: {
      mode: 'SHADOW',
      channels: 0,
      signals_today: 0,
      positions_open: 0
    },
    performance: {
      total_pnl: 0,
      win_rate: 0,
      total_trades: 0,
      avg_pnl: 0
    },
    sources: [],
    positions: [],
    recent_scores: [],
    risk: {
      daily_pnl_sol: 0,
      daily_pnl_bnb: 0,
      is_paused: false,
      consecutive_losses: 0
    },
    thresholds: [],
    reviewHistory: [],
    observationPool: { tokens: [], counts: { total: 0, gold: 0, silver: 0, bronze: 0 } },
    config: V18_STRATEGY_CONFIG,
    // v7.4 新增模块状态
    hunterPerformance: {
      FOX: { trades: 0, wins: 0, avgPnl: 0, multiplier: 1.2 },
      TURTLE: { trades: 0, wins: 0, avgPnl: 0, multiplier: 1.5 },
      WOLF: { trades: 0, wins: 0, avgPnl: 0, multiplier: 1.0 },
      UNKNOWN: { trades: 0, wins: 0, avgPnl: 0, multiplier: 1.0 }
    },
    signalSources: {
      ultra_sniper_v2: { count: 0, winRate: 0, avgPnl: 0 },
      shadow_v2: { count: 0, winRate: 0, avgPnl: 0 },
      flash_scout: { count: 0, winRate: 0, avgPnl: 0 },
      tiered_observer: { count: 0, winRate: 0, avgPnl: 0 }
    },
    apiHealth: {
      gmgn: { status: 'unknown', circuitBreaker: false, requestsToday: 0 },
      debot: { status: 'unknown', lastSuccess: null }
    },
    autonomy: {
      enabled: !!global.__autonomySidecar,
      status: global.__autonomySidecar?.getStatus?.() || null,
      leaderboard: getExperimentLeaderboard(),
      premiumEngine: global.__premiumEngine?.getStats?.() || null
    }
  };

  if (!db) return data;

  try {
    // 系统概览
    // 检查 telegram_channels 表是否存在
    const tcExists = db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name='telegram_channels'`).get();
    if (tcExists) {
      const channels = db.prepare(`SELECT COUNT(*) as c FROM telegram_channels WHERE active = 1`).get();
      data.overview.channels = channels?.c || 0;
    } else {
      data.overview.channels = 0;
    }

    // 今日信号 = 今日买入的交易数（来自 DeBot/CrossValidator）
    const signalsToday = db.prepare(`
      SELECT COUNT(*) as c FROM positions 
      WHERE DATE(entry_time) = DATE('now')
    `).get();
    data.overview.signals_today = signalsToday?.c || 0;

    // v7.5 只统计 open 状态，partial 不占仓位（与 RiskManager 逻辑一致）
    const openPositions = db.prepare(`SELECT COUNT(*) as c FROM positions WHERE status = 'open'`).get();
    data.overview.positions_open = openPositions?.c || 0;

    // 虚拟收益统计（只用 positions 表 - DeBot 验证通过的交易）
    const perfStats = db.prepare(`
      SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END) as wins,
        AVG(pnl_percent) as avg_pnl,
        SUM(pnl_percent) as total_pnl
      FROM positions 
      WHERE status = 'closed'
    `).get();

    if (perfStats && perfStats.total > 0) {
      data.performance.total_trades = perfStats.total;
      data.performance.win_rate = (perfStats.wins / perfStats.total) * 100;
      data.performance.avg_pnl = perfStats.avg_pnl || 0;
      data.performance.total_pnl = perfStats.total_pnl || 0;
    }

    // 风险管理状态 (修复时区问题 + pnl_native 可能为空)
    // 当 pnl_native 为空时，使用 pnl_percent * position_size_native / 100 估算
    const dailyPnL = db.prepare(`
      SELECT 
        chain,
        SUM(COALESCE(pnl_native, pnl_percent * position_size_native / 100)) as total_pnl
      FROM positions 
      WHERE status = 'closed'
      AND exit_time >= datetime('now', '-11 hours', 'start of day', '+11 hours')
      GROUP BY chain
    `).all();
    for (const row of dailyPnL) {
      if (row.chain === 'SOL') data.risk.daily_pnl_sol = row.total_pnl;
      if (row.chain === 'BSC') data.risk.daily_pnl_bnb = row.total_pnl;
    }

    const pauseState = db.prepare(`
      SELECT value, expires_at FROM system_state WHERE key = 'trading_paused'
    `).get();
    if (pauseState && pauseState.expires_at > Date.now() / 1000) {
      data.risk.is_paused = true;
    }

    // 获取动态阈值配置
    try {
      const thresholds = db.prepare(`
        SELECT key, value, updated_at, updated_by 
        FROM dynamic_thresholds 
        ORDER BY updated_at DESC
      `).all();
      data.thresholds = thresholds || [];
    } catch (e) {
      console.log('Dashboard thresholds query error:', e.message);
    }

    // 获取 AI 复盘历史
    try {
      const reviews = db.prepare(`
        SELECT review_time, trigger_reason, trade_count, win_rate, 
               ai_key_insight, priority_action
        FROM ai_review_history 
        ORDER BY review_time DESC
        LIMIT 10
      `).all();
      data.reviewHistory = reviews || [];
    } catch (e) {
      console.log('Dashboard review history query error:', e.message);
    }

    // 获取观察池状态 (从 JSON 文件)
    try {
      const poolPath = join(projectRoot, 'data', 'observation_pool.json');
      if (fs.existsSync(poolPath)) {
        const poolJson = fs.readFileSync(poolPath, 'utf8');
        data.observationPool = JSON.parse(poolJson);
      }
    } catch (e) {
      console.log('Dashboard pool query error:', e.message);
    }

    // 信号源排名（v7.4 使用 signal_source 字段）
    try {
      const sources = db.prepare(`
        SELECT
          COALESCE(signal_source, entry_source, 'unknown') as source_id,
          COUNT(*) as total_signals,
          ROUND(AVG(pnl_percent), 2) as avg_pnl,
          ROUND(MAX(pnl_percent), 2) as best_pnl,
          ROUND(MIN(CASE WHEN pnl_percent < 0 THEN pnl_percent END), 2) as worst_pnl,
          ROUND(SUM(CASE WHEN pnl_percent > 0 THEN 1.0 ELSE 0 END) / COUNT(*) * 100, 1) as win_rate
        FROM positions
        WHERE status = 'closed'
        GROUP BY source_id
        ORDER BY win_rate DESC, total_signals DESC
        LIMIT 20
      `).all();
      data.sources = sources || [];
    } catch (e) {
      console.log('Dashboard ranking query error:', e.message);
    }

    // 虚拟仓位：只显示 DeBot 验证通过的交易（positions 表）
    const positions = db.prepare(`
      SELECT 
        p.*,
        CASE 
          WHEN p.status = 'open' THEN 
            ROUND((julianday('now') - julianday(p.entry_time)) * 24 * 60) || ' min'
          ELSE 
            ROUND((julianday(p.exit_time) - julianday(p.entry_time)) * 24 * 60) || ' min'
        END as hold_time
      FROM positions p
      ORDER BY 
        CASE 
          WHEN p.status = 'open' THEN 0 
          WHEN p.status = 'partial' THEN 1 
          ELSE 2 
        END,
        p.entry_time DESC
      LIMIT 100
    `).all();
    data.positions = positions || [];

    // 最近信号记录 — 从 premium_signals 表获取真实数据
    try {
      const recentSignals = database.prepare(`
        SELECT symbol, market_cap, hard_gate_status, signal_type, is_ath, parse_status, executed, gate_result, timestamp
        FROM premium_signals ORDER BY id DESC LIMIT 15
      `).all();
      data.recent_scores = recentSignals.map(s => {
        const gateResult = (() => {
          try { return s.gate_result ? JSON.parse(s.gate_result) : null; } catch { return null; }
        })();
        const mc = s.market_cap ? `$${(s.market_cap / 1000).toFixed(1)}K` : '?';
        const signalLabel = (s.signal_type || (s.is_ath ? 'ATH' : 'NEW_TRENDING') || '').toUpperCase();
        const passed = (gateResult?.status || '').toUpperCase() === 'PASS' || s.hard_gate_status === 'PASS';
        return {
          symbol: s.symbol || '?',
          mc,
          superCurrent: signalLabel === 'ATH' ? 'ATH' : 'NT',
          superDelta: s.parse_status || '-',
          tradeCurrent: signalLabel,
          passed,
          status: passed
            ? (s.executed ? 'BUY' : 'PASS')
            : (gateResult?.status || s.hard_gate_status || 'SKIP').replace('NOT_ATH_PREBUY_KLINE_', '')
        };
      });
    } catch (e) {
      data.recent_scores = [];
    }

    // ==================== v7.4 新增查询 ====================

    // 1. Hunter Performance (按猎人类型统计)
    try {
      const hunterStats = db.prepare(`
        SELECT
          COALESCE(signal_hunter_type, 'UNKNOWN') as hunter_type,
          COUNT(*) as trades,
          SUM(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END) as wins,
          AVG(pnl_percent) as avg_pnl
        FROM positions
        WHERE status = 'closed'
        GROUP BY signal_hunter_type
      `).all();

      for (const row of hunterStats) {
        const type = row.hunter_type || 'UNKNOWN';
        if (data.hunterPerformance[type]) {
          data.hunterPerformance[type].trades = row.trades;
          data.hunterPerformance[type].wins = row.wins;
          data.hunterPerformance[type].avgPnl = row.avg_pnl || 0;
        }
      }
    } catch (e) {
      console.log('Dashboard hunter performance query error:', e.message);
    }

    // 2. Signal Source Distribution (按信号来源统计)
    try {
      const sourceStats = db.prepare(`
        SELECT
          COALESCE(signal_source, 'tiered_observer') as source,
          COUNT(*) as count,
          SUM(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END) as wins,
          AVG(pnl_percent) as avg_pnl
        FROM positions
        WHERE status = 'closed'
        GROUP BY signal_source
      `).all();

      for (const row of sourceStats) {
        const source = row.source || 'tiered_observer';
        if (data.signalSources[source]) {
          data.signalSources[source].count = row.count;
          data.signalSources[source].winRate = row.count > 0 ? (row.wins / row.count * 100) : 0;
          data.signalSources[source].avgPnl = row.avg_pnl || 0;
        }
      }
    } catch (e) {
      console.log('Dashboard signal source query error:', e.message);
    }

    // 3. API Gateway 健康状态 (从 v7.4.2 持久化文件读取)
    try {
      const gatewayStatsPath = join(projectRoot, 'data', 'gmgn_gateway_stats.json');
      if (fs.existsSync(gatewayStatsPath)) {
        const gatewayStats = JSON.parse(fs.readFileSync(gatewayStatsPath, 'utf8'));
        data.apiHealth.gmgn = {
          status: gatewayStats.circuitBreaker ? 'circuit_open' : 'ok',
          circuitBreaker: gatewayStats.circuitBreaker || false,
          requestsToday: gatewayStats.requestsToday || 0,
          rateLimited: gatewayStats.rateLimited || 0,
          lastUpdate: gatewayStats.timestamp
        };
      }
    } catch (e) {
      console.log('Dashboard gateway stats read error:', e.message);
    }

  } catch (error) {
    console.error('❌ Get dashboard data error:', error.message);
  }

  return data;
}

// ==================== v7.3 API 数据函数 ====================

/**
 * v7.3 获取模块健康数据
 */
function getModuleHealthData(windowDays = 7) {
  const cutoff = new Date(Date.now() - windowDays * 24 * 60 * 60 * 1000).toISOString();

  const result = {
    timestamp: new Date().toISOString(),
    windowDays,
    modules: [],
    summary: {}
  };

  try {
    // 从 module_performance 表获取数据
    let modulePerf = [];
    try {
      modulePerf = db.prepare(`
        SELECT * FROM module_performance
        WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM module_performance)
        AND window_days = ?
      `).all(windowDays);
    } catch (e) {
      // 表可能不存在，尝试从 positions 表直接计算
    }

    // 如果没有 module_performance 数据，从 positions 表计算
    if (modulePerf.length === 0) {
      modulePerf = db.prepare(`
        SELECT
          entry_source as module_name,
          COUNT(*) as total_trades,
          SUM(CASE WHEN exit_pnl_percent >= 50 THEN 1 ELSE 0 END) as win_count,
          SUM(CASE WHEN exit_pnl_percent >= 50 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as win_rate,
          AVG(exit_pnl_percent) as avg_pnl,
          SUM(exit_pnl_percent) as total_pnl,
          MAX(exit_pnl_percent) as best_pnl,
          MIN(exit_pnl_percent) as worst_pnl
        FROM positions
        WHERE status = 'closed'
        AND exit_time >= ?
        AND entry_source IS NOT NULL
        GROUP BY entry_source
      `).all(cutoff);
    }

    result.modules = modulePerf.map(m => ({
      name: m.module_name,
      trades: m.total_trades,
      winRate: m.win_rate?.toFixed(1) || 0,
      avgPnl: m.avg_pnl?.toFixed(1) || 0,
      totalPnl: m.total_pnl?.toFixed(0) || 0,
      status: (m.win_rate || 0) < 30 ? 'CRITICAL' :
        (m.win_rate || 0) < 40 ? 'WARNING' :
          (m.win_rate || 0) >= 50 ? 'EXCELLENT' : 'HEALTHY'
    }));

    // 计算总体统计
    const totalTrades = modulePerf.reduce((sum, m) => sum + (m.total_trades || 0), 0);
    const totalPnl = modulePerf.reduce((sum, m) => sum + (m.total_pnl || 0), 0);

    result.summary = {
      totalModules: modulePerf.length,
      totalTrades,
      avgPnl: totalTrades > 0 ? (totalPnl / totalTrades).toFixed(1) : 0,
      healthyCount: result.modules.filter(m => m.status === 'HEALTHY' || m.status === 'EXCELLENT').length,
      warningCount: result.modules.filter(m => m.status === 'WARNING').length,
      criticalCount: result.modules.filter(m => m.status === 'CRITICAL').length
    };

  } catch (e) {
    result.error = e.message;
  }

  return result;
}

/**
 * v7.3 获取 AI 叙事有效性数据
 */
function getNarrativeEffectivenessData() {
  const result = {
    timestamp: new Date().toISOString(),
    effective: null,
    tiers: [],
    correlation: null,
    recommendation: null
  };

  try {
    // 使用 intention_tier 字段（实际字段名）
    const data = db.prepare(`
      SELECT
        intention_tier as ai_narrative_tier,
        AVG(exit_pnl_percent) as avg_pnl,
        COUNT(*) as trades,
        SUM(CASE WHEN exit_pnl_percent >= 50 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as win_rate
      FROM positions
      WHERE status = 'closed'
      AND intention_tier IS NOT NULL
      AND exit_time >= datetime('now', '-7 days')
      GROUP BY intention_tier
      HAVING COUNT(*) >= 3
      ORDER BY
        CASE intention_tier
          WHEN 'TIER_S' THEN 1
          WHEN 'TIER_A' THEN 2
          WHEN 'TIER_B' THEN 3
          WHEN 'TIER_C' THEN 4
          ELSE 5
        END
    `).all();

    result.tiers = data.map(d => ({
      tier: d.ai_narrative_tier,
      trades: d.trades,
      avgPnl: d.avg_pnl?.toFixed(1) || 0,
      winRate: d.win_rate?.toFixed(1) || 0
    }));

    if (data.length >= 2) {
      // 检查单调性
      const tierOrder = ['TIER_S', 'TIER_A', 'TIER_B', 'TIER_C'];
      const orderedData = tierOrder
        .map(t => data.find(d => d.ai_narrative_tier === t))
        .filter(Boolean);

      const orderedPnl = orderedData.map(d => d.avg_pnl || 0);

      let monotonic = true;
      for (let i = 1; i < orderedPnl.length; i++) {
        if (orderedPnl[i] >= orderedPnl[i - 1]) {
          monotonic = false;
          break;
        }
      }

      // 简化相关性计算
      const tierScore = { 'TIER_S': 4, 'TIER_A': 3, 'TIER_B': 2, 'TIER_C': 1 };
      const points = orderedData.map(d => ({
        x: tierScore[d.ai_narrative_tier],
        y: d.avg_pnl || 0
      }));

      const n = points.length;
      if (n >= 2) {
        const sumX = points.reduce((a, p) => a + p.x, 0);
        const sumY = points.reduce((a, p) => a + p.y, 0);
        const sumXY = points.reduce((a, p) => a + p.x * p.y, 0);
        const sumX2 = points.reduce((a, p) => a + p.x * p.x, 0);
        const sumY2 = points.reduce((a, p) => a + p.y * p.y, 0);

        const numerator = n * sumXY - sumX * sumY;
        const denominator = Math.sqrt((n * sumX2 - sumX * sumX) * (n * sumY2 - sumY * sumY));

        result.correlation = denominator !== 0 ? (numerator / denominator).toFixed(2) : 0;
      }

      result.effective = monotonic && parseFloat(result.correlation) > 0.3;
      result.recommendation = result.effective ?
        '叙事评分有效，保持使用' :
        '叙事评分效果不明显，考虑调整';
    }

  } catch (e) {
    result.error = e.message;
  }

  return result;
}

/**
 * v7.3 获取 A/B 测试数据
 */
function getABTestData(windowDays = 14) {
  const cutoff = new Date(Date.now() - windowDays * 24 * 60 * 60 * 1000).toISOString();

  const result = {
    timestamp: new Date().toISOString(),
    windowDays,
    groups: [],
    difference: null,
    significant: null
  };

  try {
    const groups = db.prepare(`
      SELECT
        experiment_group,
        COUNT(*) as trades,
        AVG(exit_pnl_percent) as avg_pnl,
        SUM(CASE WHEN exit_pnl_percent >= 50 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as win_rate,
        SUM(exit_pnl_percent) as total_pnl
      FROM positions
      WHERE status = 'closed'
      AND exit_time >= ?
      AND experiment_group IS NOT NULL
      GROUP BY experiment_group
    `).all(cutoff);

    result.groups = groups.map(g => ({
      name: g.experiment_group,
      trades: g.trades,
      avgPnl: g.avg_pnl?.toFixed(1) || 0,
      winRate: g.win_rate?.toFixed(1) || 0,
      totalPnl: g.total_pnl?.toFixed(0) || 0
    }));

    const control = groups.find(g => g.experiment_group === 'control');
    const treatment = groups.find(g => g.experiment_group === 'treatment');

    if (control && treatment) {
      result.difference = {
        avgPnl: ((treatment.avg_pnl || 0) - (control.avg_pnl || 0)).toFixed(1),
        winRate: ((treatment.win_rate || 0) - (control.win_rate || 0)).toFixed(1)
      };

      // 简化显著性判断
      result.significant = control.trades >= 20 && treatment.trades >= 20 &&
        Math.abs(parseFloat(result.difference.avgPnl)) > 5;
    }

  } catch (e) {
    result.error = e.message;
  }

  return result;
}

/**
 * v7.3 获取拒绝信号数据
 */
function getRejectedSignalsData(windowDays = 7) {
  const cutoff = new Date(Date.now() - windowDays * 24 * 60 * 60 * 1000).toISOString();

  const result = {
    timestamp: new Date().toISOString(),
    windowDays,
    stages: [],
    summary: {}
  };

  try {
    // 检查表是否存在
    const tableExists = db.prepare(`
      SELECT name FROM sqlite_master WHERE type='table' AND name='rejected_signals'
    `).get();

    if (!tableExists) {
      result.error = 'rejected_signals 表不存在';
      return result;
    }

    const stages = db.prepare(`
      SELECT
        rejection_stage,
        COUNT(*) as total,
        SUM(CASE WHEN tracking_completed = 1 THEN 1 ELSE 0 END) as tracked,
        AVG(CASE WHEN tracking_completed = 1 THEN would_have_profit ELSE NULL END) as avg_avoided_pnl,
        SUM(CASE WHEN would_have_profit < 0 THEN 1 ELSE 0 END) as correct_rejections,
        SUM(CASE WHEN would_have_profit < -20 THEN 1 ELSE 0 END) as dodged_big_loss,
        SUM(CASE WHEN would_have_profit > 50 THEN 1 ELSE 0 END) as missed_big_gain
      FROM rejected_signals
      WHERE created_at >= ?
      GROUP BY rejection_stage
    `).all(cutoff);

    result.stages = stages.map(s => ({
      stage: s.rejection_stage,
      total: s.total,
      tracked: s.tracked,
      avgAvoidedPnl: s.avg_avoided_pnl?.toFixed(1) || 'N/A',
      accuracy: s.tracked > 0 ? ((s.correct_rejections / s.tracked) * 100).toFixed(1) : 'N/A',
      dodgedBigLoss: s.dodged_big_loss || 0,
      missedBigGain: s.missed_big_gain || 0
    }));

    // 总体统计
    const totalRejected = stages.reduce((sum, s) => sum + s.total, 0);
    const totalTracked = stages.reduce((sum, s) => sum + (s.tracked || 0), 0);
    const totalCorrect = stages.reduce((sum, s) => sum + (s.correct_rejections || 0), 0);

    result.summary = {
      totalRejected,
      totalTracked,
      overallAccuracy: totalTracked > 0 ? ((totalCorrect / totalTracked) * 100).toFixed(1) : 'N/A'
    };

  } catch (e) {
    result.error = e.message;
  }

  return result;
}

/**
 * HTTP 服务器
 */
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);

  if (url.pathname === '/' || url.pathname === '/health' || url.pathname === '/ping') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', message: 'Sentiment Arbitrage API Running', timestamp: Date.now() }));
    return;
  } else if (url.pathname === '/dashboard') {
    res.writeHead(302, { 'Location': '/premium' });
    res.end();
  } else if (url.pathname === '/api/status') {
    if (!checkAuth(req, url, res)) return;
    const data = getDashboardData();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(data, null, 2));
  } else if (url.pathname === '/api/autonomy-status') {
    if (!checkAuth(req, url, res)) return;
    const payload = {
      autonomy: global.__autonomySidecar?.getStatus?.() || { enabled: false },
      leaderboard: getExperimentLeaderboard(),
      recentExperiments: listRecentExperiments()
    };
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(payload, null, 2));
  } else if (url.pathname === '/api/module-health') {
    // v7.3 模块健康状态 API
    if (!checkAuth(req, url, res)) return;
    const windowDays = parseInt(url.searchParams.get('window')) || 7;
    const moduleHealth = getModuleHealthData(windowDays);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(moduleHealth, null, 2));
  } else if (url.pathname === '/api/narrative-effectiveness') {
    // v7.3 AI 叙事有效性 API
    if (!checkAuth(req, url, res)) return;
    const narrativeData = getNarrativeEffectivenessData();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(narrativeData, null, 2));
  } else if (url.pathname === '/api/ab-test') {
    // v7.3 A/B 测试状态 API
    if (!checkAuth(req, url, res)) return;
    const windowDays = parseInt(url.searchParams.get('window')) || 14;
    const abTestData = getABTestData(windowDays);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(abTestData, null, 2));
  } else if (url.pathname === '/api/rejected-signals') {
    // v7.3 拒绝信号统计 API
    if (!checkAuth(req, url, res)) return;
    const windowDays = parseInt(url.searchParams.get('window')) || 7;
    const rejectedData = getRejectedSignalsData(windowDays);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(rejectedData, null, 2));
  } else if (url.pathname === '/api/channel-history') {
    // Fetch Telegram channel message history for backtest
    if (!checkAuth(req, url, res)) return;
    try {
      const tg = global.__telegramService;
      if (!tg || !tg.client) {
        res.writeHead(503, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Telegram service not available' }));
        return;
      }
      const limit = Math.min(parseInt(url.searchParams.get('limit')) || 200, 3000);
      const history = await tg.getChannelHistory(limit);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(history, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    }
  } else if (url.pathname === '/api/shadow-pnl') {
    // Premium Channel Shadow PnL API
    if (!checkAuth(req, url, res)) return;
    try {
      const d = getDb();
      if (!d) throw new Error('Database not ready');

      // 单独查询所有未关闭仓位（不受 LIMIT 限制）
      const open = d.prepare(`
        SELECT symbol, score, entry_mc, entry_time, exit_pnl, high_pnl, low_pnl, exit_reason, closed, closed_at
        FROM shadow_pnl WHERE closed = 0 ORDER BY entry_time DESC
      `).all();

      // 查询最近已关闭的交易
      const closed = d.prepare(`
        SELECT symbol, score, entry_mc, entry_time, exit_pnl, high_pnl, low_pnl, exit_reason, closed, closed_at
        FROM shadow_pnl WHERE closed = 1 ORDER BY entry_time DESC LIMIT 200
      `).all();

      // 统计所有已关闭交易（不受 LIMIT 限制）
      const allStats = d.prepare(`
        SELECT
          COUNT(*) as total,
          SUM(CASE WHEN exit_pnl > 0 THEN 1 ELSE 0 END) as wins,
          SUM(CASE WHEN exit_pnl <= 0 THEN 1 ELSE 0 END) as losses,
          AVG(exit_pnl) as avgPnl,
          SUM(exit_pnl) as totalPnl
        FROM shadow_pnl WHERE closed = 1
      `).get();

      const winRate = allStats.total > 0 ? (allStats.wins / allStats.total * 100) : 0;

      // 按 exit_reason 分组
      const byReason = {};
      for (const r of closed) {
        const reason = (r.exit_reason || 'UNKNOWN').replace(/\(.*\)/, '');
        if (!byReason[reason]) byReason[reason] = { count: 0, totalPnl: 0 };
        byReason[reason].count++;
        byReason[reason].totalPnl += r.exit_pnl || 0;
      }

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        summary: {
          total: allStats.total || 0,
          wins: allStats.wins || 0,
          losses: allStats.losses || 0,
          winRate: +winRate.toFixed(1),
          avgPnl: +(allStats.avgPnl || 0).toFixed(1),
          totalPnl: +(allStats.totalPnl || 0).toFixed(1)
        },
        open: open.map(r => ({ ...r, entry_mc_k: +(r.entry_mc / 1000).toFixed(1) })),
        recent: closed.slice(0, 50).map(r => ({ ...r, entry_mc_k: +(r.entry_mc / 1000).toFixed(1) })),
        byReason
      }, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    }
  } else if (url.pathname === '/premium') {
    // Premium Channel Dashboard 页面
    if (!checkAuth(req, url, res)) return;
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(renderPremiumDashboard());
  } else if (url.pathname === '/api/live-positions') {
    // 实盘交易记录 API
    if (!checkAuth(req, url, res)) return;
    try {
      const d = getDb();
      if (!d) throw new Error('Database not ready');

      // 检查 live_positions 表是否存在
      const tableExists = d.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name='live_positions'`).get();
      if (!tableExists) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ summary: { total: 0, wins: 0, losses: 0, winRate: 0, totalPnl: 0, totalSolSpent: 0, totalSolReceived: 0 }, open: [], recent: [] }));
        return;
      }

      const open = d.prepare(`
        SELECT token_ca, symbol, entry_mc, entry_sol, token_amount, high_pnl, low_pnl, status, entry_time,
               sold_pct, total_sol_received, tp1_triggered, tp2_triggered, tp3_triggered, tp4_triggered, moonbag_active
        FROM live_positions WHERE status = 'open' ORDER BY entry_time DESC
      `).all();

      const closed = d.prepare(`
        SELECT token_ca, symbol, entry_mc, entry_sol, exit_pnl, high_pnl, low_pnl, exit_reason, status, entry_time, closed_at, total_sol_received,
               entry_price, token_amount, token_decimals
        FROM live_positions WHERE status = 'closed' ORDER BY entry_time DESC LIMIT 50
      `).all();

      const allStats = d.prepare(`
        SELECT
          COUNT(*) as total,
          SUM(CASE WHEN exit_pnl > 0 THEN 1 ELSE 0 END) as wins,
          SUM(CASE WHEN exit_pnl <= 0 THEN 1 ELSE 0 END) as losses,
          AVG(exit_pnl) as avgPnl,
          SUM(exit_pnl) as totalPnl,
          SUM(entry_sol) as totalSolSpent,
          SUM(CASE WHEN total_sol_received >= 0 THEN total_sol_received ELSE 0 END) as totalSolReceived
        FROM live_positions WHERE status = 'closed'
      `).get();

      const winRate = allStats.total > 0 ? (allStats.wins / allStats.total * 100) : 0;
      // 计算真实总 PnL（基于实际 SOL 进出）
      const realTotalPnl = allStats.totalSolSpent > 0
        ? ((allStats.totalSolReceived - allStats.totalSolSpent) / allStats.totalSolSpent) * 100
        : 0;

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        summary: {
          total: allStats.total || 0,
          wins: allStats.wins || 0,
          losses: allStats.losses || 0,
          winRate: +winRate.toFixed(1),
          avgPnl: +(allStats.avgPnl || 0).toFixed(1),
          totalPnl: +(allStats.totalPnl || 0).toFixed(1),
          realTotalPnl: +realTotalPnl.toFixed(1),
          totalSolSpent: +(allStats.totalSolSpent || 0).toFixed(4),
          totalSolReceived: +(allStats.totalSolReceived || 0).toFixed(4)
        },
        open: open.map(r => ({ ...r, entry_mc_k: +(r.entry_mc / 1000).toFixed(1) })),
        recent: closed.map(r => {
          // 计算实际 SOL 收益（total_sol_received < 0 表示不可追踪，用 exit_pnl）
          const solRecv = (r.total_sol_received != null && r.total_sol_received >= 0) ? r.total_sol_received : 0;
          const realPnl = (solRecv > 0 && r.entry_sol > 0)
            ? ((solRecv - r.entry_sol) / r.entry_sol * 100)
            : (r.exit_pnl || 0);
          // 计算峰值捕获率
          const captureRate = r.high_pnl > 0 ? (realPnl / r.high_pnl * 100) : 0;
          // 计算损失
          const loss = r.high_pnl - realPnl;

          return {
            ...r,
            entry_mc_k: +(r.entry_mc / 1000).toFixed(1),
            real_pnl: +realPnl.toFixed(1),
            capture_rate: +captureRate.toFixed(1),
            loss: +loss.toFixed(1)
          };
        })
      }, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    }
  } else if (url.pathname === '/api/close-position') {
    // 手动关闭持仓 API — 需要 POST + token 认证
    if (req.method !== 'POST') {
      res.writeHead(405, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Method not allowed. Use POST.' }));
      return;
    }
    if (!checkAuth(req, url, res)) return;
    try {
      const d = getDb();
      if (!d) throw new Error('Database not ready');

      const ca = url.searchParams.get('ca');
      const reason = url.searchParams.get('reason') || 'MANUAL_CLOSE';

      if (!ca) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Missing ca parameter' }));
        return;
      }

      // 检查是否存在
      const pos = d.prepare(`SELECT * FROM live_positions WHERE token_ca = ? AND status = 'open'`).get(ca);
      if (!pos) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Position not found or already closed' }));
        return;
      }

      // 关闭持仓
      d.prepare(`
        UPDATE live_positions
        SET status = 'closed',
            exit_reason = ?,
            exit_pnl = -100,
            closed_at = ?,
            total_sol_received = 0
        WHERE token_ca = ? AND status = 'open'
      `).run(reason, Date.now(), ca);

      console.log(`🔧 [手动关闭] ${pos.symbol} (${ca.substring(0, 8)}...) - ${reason}`);

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        success: true,
        message: `Position ${pos.symbol} closed`,
        ca: ca,
        reason: reason
      }));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    }
  } else if (url.pathname === '/api/pause-trading') {
    // 手动暂停交易
    if (req.method !== 'POST') {
      res.writeHead(405, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Use POST' }));
      return;
    }
    if (!checkAuth(req, url, res)) return;
    try {
      const hours = parseInt(url.searchParams.get('hours') || '4');
      const rm = global.__riskManager;
      if (rm) {
        rm.manualPause(hours);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: true, message: `交易已暂停 ${hours} 小时`, pausedUntil: rm.state.pausedUntil }));
      } else {
        // fallback: 直接写 DB
        const d = getDb();
        const pauseUntil = Math.floor(Date.now() / 1000) + hours * 3600;
        d.prepare(`INSERT OR REPLACE INTO system_state (key, value, expires_at) VALUES ('trading_paused', 'true', ?)`).run(pauseUntil);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: true, message: `交易已暂停 ${hours} 小时 (DB only)`, pausedUntil: new Date(pauseUntil * 1000) }));
      }
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    }
  } else if (url.pathname === '/api/resume-trading') {
    // 手动恢复交易
    if (req.method !== 'POST') {
      res.writeHead(405, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Use POST' }));
      return;
    }
    if (!checkAuth(req, url, res)) return;
    try {
      const rm = global.__riskManager;
      if (rm) {
        rm.resumeTrading();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: true, message: '交易已恢复，连亏计数已重置' }));
      } else {
        const d = getDb();
        d.prepare(`DELETE FROM system_state WHERE key = 'trading_paused'`).run();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: true, message: '交易已恢复 (DB only)' }));
      }
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    }
  } else if (url.pathname === '/api/trading-status') {
    // 获取交易状态
    if (!checkAuth(req, url, res)) return;
    try {
      const rm = global.__riskManager;
      if (rm) {
        const status = rm.getStatus();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ paused: !!status.pausedUntil, pausedUntil: status.pausedUntil, consecutiveLosses: status.consecutiveLosses, canTrade: status.canTrade, dailyNetPnl: status.dailyNetPnlSol, dailyLossLimit: status.dailyLossLimitSol }));
      } else {
        const d = getDb();
        const row = d.prepare(`SELECT value, expires_at FROM system_state WHERE key = 'trading_paused'`).get();
        const paused = row && row.expires_at > Date.now() / 1000;
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ paused, pausedUntil: paused ? new Date(row.expires_at * 1000) : null }));
      }
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    }
  } else if (url.pathname === '/api/reset-daily-loss') {
    // 重置今日亏损统计起点（用于"重新开始"，不删除历史数据）
    if (req.method !== 'POST') {
      res.writeHead(405, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Use POST' }));
      return;
    }
    if (!checkAuth(req, url, res)) return;
    try {
      const rm = global.__riskManager;
      if (rm) {
        rm.resetDailyLoss();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: true, message: `今日亏损统计已重置，从 ${new Date().toLocaleString()} 起重新计算` }));
      } else {
        const d = getDb();
        d.prepare(`INSERT OR REPLACE INTO system_state (key, value) VALUES ('daily_loss_reset_ts', ?)`).run(Date.now().toString());
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: true, message: '今日亏损统计已重置 (DB only)' }));
      }
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    }

  } else if (url.pathname === '/api/reset-live-data') {
    // 清空实盘交易数据，重新开始
    if (req.method !== 'POST') {
      res.writeHead(405, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Use POST' }));
      return;
    }
    if (!checkAuth(req, url, res)) return;
    try {
      const d = getDb();
      const count = d.prepare(`SELECT COUNT(*) as c FROM live_positions`).get().c;
      d.prepare(`DELETE FROM live_positions`).run();
      try { d.prepare(`DELETE FROM system_state WHERE key = 'trading_paused'`).run(); } catch(e) { /* table may not exist */ }
      const rm = global.__riskManager;
      if (rm) {
        rm.state.pausedUntil = null;
        rm.state.consecutiveLosses = 0;
        rm._circuitBreakerTriggered = false;
        rm._circuitBreakerLogged = false;
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ success: true, message: `已清空 ${count} 条实盘记录，风控状态已重置` }));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    }
  } else if (url.pathname === '/api/paper-trades/cleanup') {
    if (req.method !== 'POST') {
      res.writeHead(405, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Use POST' }));
      return;
    }
    if (!checkAuth(req, url, res)) return;
    try {
      const reason = (url.searchParams.get('reason') || 'manual_cleanup').trim() || 'manual_cleanup';
      const pnlPctRaw = url.searchParams.get('pnl_pct') || '0';
      const pnlPct = Number(pnlPctRaw);
      if (!Number.isFinite(pnlPct)) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'pnl_pct must be a valid number' }));
        return;
      }
      const result = cleanupOpenPaperPositions({ reason, pnlPct });
      console.log(`🧹 Cleaned ${result.updated} open paper positions reason=${reason} pnlPct=${pnlPct} db=${result.dbPath}`);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ success: true, ...result }));
    } catch (e) {
      res.writeHead(e.statusCode || 500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    }
    return;
  } else if (url.pathname === '/api/download/database') {
    // 数据库下载端点 — 需要 token 认证
    if (!checkAuth(req, url, res)) return;
    const filePath = resolvedDbPath;
    if (!fs.existsSync(filePath)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Database file not found' }));
      return;
    }
    const stats = fs.statSync(filePath);
    res.writeHead(200, {
      'Content-Type': 'application/octet-stream',
      'Content-Disposition': 'attachment; filename="sentiment_arb.db"',
      'Content-Length': stats.size
    });
    const fileStream = fs.createReadStream(filePath);
    fileStream.pipe(res);
    return;
  } else if (url.pathname === '/api/download/kline_cache') {
    // K线数据库下载 — 需要 token 认证
    if (!checkAuth(req, url, res)) return;
    const klineDbPath = join(projectRoot, 'data', 'kline_cache.db');
    if (!fs.existsSync(klineDbPath)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Kline cache database not found' }));
      return;
    }
    const stats = fs.statSync(klineDbPath);
    res.writeHead(200, {
      'Content-Type': 'application/octet-stream',
      'Content-Disposition': 'attachment; filename="kline_cache.db"',
      'Content-Length': stats.size
    });
    const fileStream = fs.createReadStream(klineDbPath);
    fileStream.pipe(res);
    return;
  } else if (url.pathname === '/api/download/paper_trades') {
    // Paper trades数据库下载 — 需要 token 认证
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = join(projectRoot, 'data', 'paper_trades.db');
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    const stats = fs.statSync(paperDbPath);
    res.writeHead(200, {
      'Content-Type': 'application/octet-stream',
      'Content-Disposition': 'attachment; filename="paper_trades.db"',
      'Content-Length': stats.size
    });
    const fileStream = fs.createReadStream(paperDbPath);
    fileStream.pipe(res);
    return;
  } else if (url.pathname === '/api/export') {
    // v10: 导出所有DB数据为JSON（用于回测分析） — 需要 token 认证
    if (!checkAuth(req, url, res)) return;
    try {
      const database = getDb();
      if (!database) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Database not available' }));
        return;
      }
      const tables = ['premium_signals', 'tokens', 'trades', 'live_positions', 'rejected_signals', 'passed_signals', 'hunter_signals', 'signal_source_performance', 'autonomy_runs', 'strategy_experiments', 'paper_trade_records'];
      const exportData = { exported_at: new Date().toISOString(), tables: {} };
      // 支持分页: ?before_id=X 拉取 id < X 的历史数据
      const beforeId = url.searchParams.get('before_id');
      const exportLimit = parseInt(url.searchParams.get('limit') || '1000');
      for (const table of tables) {
        try {
          let rows;
          if (beforeId && table === 'premium_signals') {
            rows = database.prepare(`SELECT * FROM ${table} WHERE id < ? ORDER BY id DESC LIMIT ?`).all(parseInt(beforeId), exportLimit);
          } else {
            rows = database.prepare(`SELECT * FROM ${table} ORDER BY rowid DESC LIMIT ?`).all(exportLimit);
          }
          exportData.tables[table] = { count: rows.length, rows };
        } catch (e) {
          exportData.tables[table] = { count: 0, rows: [], error: e.message };
        }
      }
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(exportData, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    }
    return;
  } else if (url.pathname === '/api/signals/stream') {
    // SSE (Server-Sent Events) endpoint for real-time signal streaming
    if (!checkAuth(req, url, res)) return;
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'Access-Control-Allow-Origin': '*',
    });
    res.write('data: {"event":"connected","timestamp":"' + new Date().toISOString() + '"}\n\n');

    // Register this SSE client
    if (!global.__sseClients) global.__sseClients = new Set();
    global.__sseClients.add(res);
    console.log(`📡 SSE client connected (total: ${global.__sseClients.size})`);

    // Keep-alive ping every 30s
    const keepAlive = setInterval(() => {
      try { res.write(':ping\n\n'); } catch (e) { /* client disconnected */ }
    }, 30000);

    req.on('close', () => {
      clearInterval(keepAlive);
      if (global.__sseClients) global.__sseClients.delete(res);
      console.log(`📡 SSE client disconnected (total: ${global.__sseClients?.size || 0})`);
    });
    return;
  } else if (url.pathname === '/api/wallet-balance') {
    // 钱包 SOL 余额查询
    if (!checkAuth(req, url, res)) return;
    try {
      const executor = global.__executor;
      if (executor && executor.walletAddress) {
        const balance = await executor.getSolBalance();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ balance: +balance.toFixed(4), wallet: executor.walletAddress.substring(0, 8) + '...' + executor.walletAddress.slice(-4) }));
        return;
      }
      // fallback: 用环境变量
      const walletAddr = process.env.TRADE_WALLET_ADDRESS || process.env.WALLET_ADDRESS || '';
      const rpcUrl = process.env.SOLANA_RPC_URL || 'https://api.mainnet-beta.solana.com';
      if (!walletAddr) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ balance: null, error: 'TRADE_WALLET_ADDRESS not set' }));
        return;
      }
      const rpcBody = JSON.stringify({
        jsonrpc: '2.0', id: 1, method: 'getBalance',
        params: [walletAddr, { commitment: 'confirmed' }]
      });
      const rpcRes = await new Promise((resolve, reject) => {
        const isHttps = rpcUrl.startsWith('https');
        const urlObj = new URL(rpcUrl);
        const mod = isHttps ? https : http;
        const req = mod.request({
          hostname: urlObj.hostname, port: urlObj.port || (isHttps ? 443 : 80),
          path: urlObj.pathname, method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(rpcBody) }
        }, r => {
          let data = '';
          r.on('data', c => data += c);
          r.on('end', () => resolve(JSON.parse(data)));
        });
        req.on('error', reject);
        req.setTimeout(5000, () => { req.destroy(); reject(new Error('RPC timeout')); });
        req.write(rpcBody);
        req.end();
      });
      const lamports = rpcRes?.result?.value ?? null;
      const balance = lamports !== null ? +(lamports / 1e9).toFixed(4) : null;
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ balance, wallet: walletAddr.substring(0, 8) + '...' + walletAddr.slice(-4) }));
    } catch (e) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ balance: null, error: e.message }));
    }
    return;
  } else if (url.pathname === '/api/logs') {
    // 最近日志 API（JSON格式）
    if (!checkAuth(req, url, res)) return;
    const limit = parseInt(url.searchParams?.get('limit') || '100');
    const level = url.searchParams?.get('level'); // 可选过滤: INFO, ERROR, WARN
    let logs = logBuffer.slice(-limit);
    if (level) {
      logs = logs.filter(l => l.level === level.toUpperCase());
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ count: logs.length, logs }, null, 2));
    return;
  } else if (url.pathname === '/api/logs/download') {
    // 日志下载端点（完整日志文件）
    if (!checkAuth(req, url, res)) return;
    if (fs.existsSync(runtimeLogPath)) {
      const stats = fs.statSync(runtimeLogPath);
      res.writeHead(200, {
        'Content-Type': 'text/plain; charset=utf-8',
        'Content-Disposition': `attachment; filename="runtime-logs-${new Date().toISOString().slice(0,19).replace(/:/g,'-')}.txt"`,
        'Content-Length': stats.size
      });
      const fileStream = fs.createReadStream(runtimeLogPath);
      fileStream.pipe(res);
    } else {
      // fallback 到内存缓冲
      const content = logBuffer.map(l => `[${l.timestamp}] [${l.level}] ${l.message}`).join('\n');
      res.writeHead(200, {
        'Content-Type': 'text/plain; charset=utf-8',
        'Content-Disposition': `attachment; filename="runtime-logs-${new Date().toISOString().slice(0,19).replace(/:/g,'-')}.txt"`
      });
      res.end(content);
    }
    return;
  } else if (url.pathname === '/api/logs/paper-trader') {
    // Paper trader Python 进程日志
    if (!checkAuth(req, url, res)) return;
    const paperTraderLogPath = process.env.PAPER_TRADER_LOG || '/app/data/paper-trader.log';
    const tailLines = parseInt(url.searchParams?.get('lines') || '500');
    if (fs.existsSync(paperTraderLogPath)) {
      try {
        const content = fs.readFileSync(paperTraderLogPath, 'utf-8');
        const lines = content.split('\n');
        const tail = lines.slice(-tailLines).join('\n');
        res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.end(tail);
      } catch (e) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: e.message }));
      }
    } else {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: `paper-trader.log not found at ${paperTraderLogPath}` }));
    }
    return;
  } else if (url.pathname === '/logs') {
    // 日志查看页面（HTML）
    if (!checkAuth(req, url, res)) return;
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(`<!DOCTYPE html>
<html><head><title>Runtime Logs</title>
<style>
body{font-family:monospace;background:#1a1a2e;color:#e4e4e4;padding:20px;margin:0}
h1{color:#00d9ff}pre{background:#111;padding:15px;border-radius:8px;overflow-x:auto;max-height:80vh;overflow-y:auto}
.INFO{color:#00ff88}.ERROR{color:#ff4444}.WARN{color:#ffda44}
.controls{margin-bottom:15px}
.controls button,.controls select{padding:8px 16px;margin-right:10px;border-radius:4px;border:none;cursor:pointer}
.controls button{background:#00d9ff;color:#1a1a2e;font-weight:bold}
</style></head><body>
<h1>📋 Runtime Logs</h1>
<div class="controls">
  <button onclick="refresh()">🔄 刷新</button>
  <button onclick="download()">📥 下载</button>
  <select id="level" onchange="refresh()">
    <option value="">全部级别</option>
    <option value="INFO">INFO</option>
    <option value="WARN">WARN</option>
    <option value="ERROR">ERROR</option>
  </select>
  <select id="limit" onchange="refresh()">
    <option value="100">最近100条</option>
    <option value="500">最近500条</option>
    <option value="1000">全部(1000)</option>
  </select>
</div>
<pre id="logs">加载中...</pre>
<script>
const _token=new URLSearchParams(window.location.search).get('token')||'';
const _a=_token?'&token='+encodeURIComponent(_token):'';
async function refresh(){
  const level=document.getElementById('level').value;
  const limit=document.getElementById('limit').value;
  const res=await fetch('/api/logs?limit='+limit+(level?'&level='+level:'')+_a);
  const data=await res.json();
  document.getElementById('logs').innerHTML=data.logs.map(l=>
    '<span class="'+l.level+'">['+l.timestamp.slice(11,19)+'] ['+l.level+'] '+l.message.replace(/</g,'&lt;').replace(/>/g,'&gt;')+'</span>'
  ).join('\\n');
}
function download(){window.location='/api/logs/download';}
refresh();setInterval(refresh,10000);
</script></body></html>`);
    return;
  } else if (url.pathname === '/health') {
    // 健康检查 + 数据库状态
    try {
      const d = getDb();
      let dbStatus = { connected: false };

      if (d) {
        const shadowCount = d.prepare('SELECT COUNT(*) as cnt FROM shadow_pnl').get();
        const latestShadow = d.prepare('SELECT MAX(entry_time) as latest FROM shadow_pnl').get();
        const tradesCount = d.prepare('SELECT COUNT(*) as cnt FROM trades').get();
        const closedCount = d.prepare('SELECT COUNT(*) as cnt FROM shadow_pnl WHERE closed=1').get();
        const openCount = d.prepare('SELECT COUNT(*) as cnt FROM shadow_pnl WHERE closed=0').get();

        const latestTime = latestShadow?.latest ? new Date(latestShadow.latest).toISOString() : null;

        dbStatus = {
          connected: true,
          shadow_pnl: {
            total: shadowCount?.cnt || 0,
            closed: closedCount?.cnt || 0,
            open: openCount?.cnt || 0,
            latest_entry: latestTime
          },
          trades: tradesCount?.cnt || 0
        };
      }

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        status: 'ok',
        timestamp: new Date().toISOString(),
        db: dbStatus,
        uptime_seconds: Math.floor(process.uptime())
      }, null, 2));
    } catch (e) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        status: 'ok',
        timestamp: new Date().toISOString(),
        db: { connected: false, error: e.message },
        uptime_seconds: Math.floor(process.uptime())
      }, null, 2));
    }
  } else {
    res.writeHead(404);
    res.end('Not Found');
  }
});

/**
 * Premium Channel Dashboard 页面
 */
function renderPremiumDashboard() {
  return `<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Premium Channel Dashboard</title>
  <style>
    *{box-sizing:border-box;margin:0;padding:0}
    body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:linear-gradient(135deg,#1a1a2e,#16213e);color:#e4e4e4;min-height:100vh;padding:20px}
    .container{max-width:1400px;margin:0 auto}
    h1{text-align:center;margin-bottom:20px;color:#00d9ff;font-size:2em;text-shadow:0 0 20px rgba(0,217,255,0.3)}
    .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:15px;margin-bottom:25px}
    .card{background:rgba(255,255,255,0.05);border-radius:12px;padding:18px;border:1px solid rgba(255,255,255,0.1)}
    .card h2{color:#00d9ff;margin-bottom:12px;font-size:1.1em}
    .big-num{font-size:2.2em;font-weight:bold;text-align:center}
    .green{color:#00ff88}.red{color:#ff4444}.yellow{color:#ffda44}.blue{color:#00d9ff}.orange{color:#ff9944}
    .label{text-align:center;color:#888;font-size:0.85em;margin-top:4px}
    table{width:100%;border-collapse:collapse;font-size:0.85em}
    th{color:#00d9ff;text-align:left;padding:8px 6px;border-bottom:1px solid rgba(255,255,255,0.1)}
    td{padding:6px;border-bottom:1px solid rgba(255,255,255,0.05)}
    .pnl-pos{color:#00ff88}.pnl-neg{color:#ff4444}
    .badge{padding:2px 8px;border-radius:10px;font-size:0.8em}
    .badge-green{background:rgba(0,255,136,0.15);color:#00ff88}
    .badge-red{background:rgba(255,68,68,0.15);color:#ff4444}
    .badge-yellow{background:rgba(255,218,68,0.15);color:#ffda44}
    .refresh-btn{position:fixed;top:15px;right:15px;background:#00d9ff;color:#1a1a2e;border:none;padding:8px 16px;border-radius:8px;cursor:pointer;font-weight:bold}
    .open-tag{background:rgba(0,217,255,0.15);color:#00d9ff;padding:2px 6px;border-radius:4px;font-size:0.75em}
    .live-tag{background:rgba(255,153,68,0.2);color:#ff9944;padding:2px 6px;border-radius:4px;font-size:0.75em}
  </style>
</head>
<body>
  <button class="refresh-btn" onclick="loadData()">刷新</button>
  <div class="container">
    <h1>💎 Premium Channel Dashboard</h1>
    <!-- 钱包余额 - 始终显示 -->
    <div style="text-align:center;margin-bottom:15px">
      <span style="font-size:1.8em;font-weight:bold;color:#00d9ff" id="wallet-sol">--</span>
      <span style="color:#888;font-size:0.9em;margin-left:6px">SOL</span>
    </div>
    <!-- 交易控制面板 -->
    <div style="display:flex;gap:10px;justify-content:center;margin-bottom:15px;align-items:center">
      <span id="trading-status" style="font-size:0.9em;color:#888">加载中...</span>
      <button id="btn-pause-p" onclick="toggleTrading('pause')" style="padding:6px 16px;cursor:pointer;background:#ff4757;border:none;border-radius:6px;color:#fff;font-weight:bold;display:none">⏸ 暂停交易</button>
      <button id="btn-resume-p" onclick="toggleTrading('resume')" style="padding:6px 16px;cursor:pointer;background:#2ed573;border:none;border-radius:6px;color:#fff;font-weight:bold;display:none">▶ 恢复交易</button>
      <button onclick="resetDailyLoss()" style="padding:6px 16px;cursor:pointer;background:#ffa502;border:none;border-radius:6px;color:#fff;font-weight:bold;" title="重置今日亏损统计起点，不删除历史数据">🔄 重置今日亏损</button>
    </div>

    <!-- 实盘交易 -->
    <div id="live-content">
      <div class="grid" id="live-summary"></div>
      <div class="card" style="margin-bottom:20px"><h2>🟢 实盘持仓</h2><table id="live-open-table"><thead><tr><th>代币</th><th>入场MC</th><th>仓位(SOL)</th><th>已卖/剩余</th><th>已收回SOL</th><th>TP状态</th><th>最高</th><th>最低</th><th>⏱️持有时间</th></tr></thead><tbody></tbody></table></div>
      <div class="card"><h2>📋 实盘交易记录</h2><table id="live-recent-table"><thead><tr><th>代币</th><th>入场MC</th><th>仓位</th><th>实际PnL</th><th>峰值</th><th>损失</th><th>捕获率</th><th>出场原因</th><th>⏱️持仓</th><th>时间</th></tr></thead><tbody></tbody></table></div>
      <div style="text-align:center;margin-top:20px"><button id="btn-reset" onclick="resetLiveData()" style="padding:8px 20px;cursor:pointer;background:#ff6348;border:none;border-radius:6px;color:#fff;font-weight:bold">🗑 清空实盘数据重新开始</button></div>
    </div>
  </div>
  <script>

    const _token=new URLSearchParams(window.location.search).get('token')||'';
    const _q=_token?'?token='+encodeURIComponent(_token):'';
    async function loadData(){
      try{
        // 实盘数据
        const liveRes=await fetch('/api/live-positions'+_q);
        const live=await liveRes.json();
        const ls=live.summary;
        const solSpent=(ls.totalSolSpent||0);
        const solRecv=(ls.totalSolReceived||0);
        const netSol=solRecv-solSpent;
        document.getElementById('live-summary').innerHTML=
          '<div class="card"><div class="big-num '+(ls.winRate>=60?'green':ls.winRate>=40?'yellow':'red')+'">'+(ls.winRate||0)+'%</div><div class="label">胜率 ('+(ls.wins||0)+'W/'+(ls.losses||0)+'L / '+(ls.total||0)+'笔)</div></div>'+
          '<div class="card"><div class="big-num '+(netSol>=0?'green':'red')+'">'+(netSol>=0?'+':'')+netSol.toFixed(4)+'</div><div class="label">净盈亏 SOL</div></div>'+
          '<div class="card"><div class="big-num orange">'+(solSpent).toFixed(4)+'</div><div class="label">总投入 SOL</div></div>'+
          '<div class="card"><div class="big-num '+(solRecv>=solSpent?'green':'red')+'">'+(solRecv).toFixed(4)+'</div><div class="label">总收回 SOL</div></div>'+
          '<div class="card"><div class="big-num blue">'+live.open.length+'</div><div class="label">当前持仓</div></div>';

        const lotb=document.querySelector('#live-open-table tbody');
        lotb.innerHTML=live.open.map(r=>{
          const holdSec=r.entry_time?Math.floor((Date.now()-new Date(r.entry_time).getTime())/1000):0;
          const holdStr=holdSec>=3600?Math.floor(holdSec/3600)+'h'+Math.floor((holdSec%3600)/60)+'m':(holdSec>=60?Math.floor(holdSec/60)+'m'+holdSec%60+'s':holdSec+'s');
          const soldPct=r.sold_pct||0;
          const remainPct=100-soldPct;
          const solRecv=(r.total_sol_received||0).toFixed(4);
          const tps=[];
          if(r.tp1_triggered)tps.push('TP1✅');
          if(r.tp2_triggered)tps.push('TP2✅');
          if(r.tp3_triggered)tps.push('TP3✅');
          if(r.tp4_triggered)tps.push('TP4✅');
          if(r.moonbag_active)tps.push('🌙');
          const tpStr=tps.length?tps.join(' '):'—';
          const soldColor=soldPct>0?'color:#ffa502':'color:#666';
          const recvColor=(r.total_sol_received||0)>0?'color:#2ed573':'color:#666';
          return '<tr><td>$'+r.symbol+' <span class="live-tag">LIVE</span></td><td>$'+r.entry_mc_k+'K</td><td>'+r.entry_sol+'</td><td style="'+soldColor+'">'+soldPct+'%已卖 / '+remainPct+'%剩余</td><td style="'+recvColor+'">'+solRecv+'</td><td>'+tpStr+'</td><td class="pnl-pos">+'+(r.high_pnl||0).toFixed(1)+'%</td><td class="pnl-neg">'+(r.low_pnl||0).toFixed(1)+'%</td><td style="color:#00d9ff;font-weight:bold">'+holdStr+'</td></tr>';
        }).join('');

        const lrtb=document.querySelector('#live-recent-table tbody');
        lrtb.innerHTML=live.recent.map(r=>{
          const realPnlCls=(r.real_pnl||0)>0?'pnl-pos':'pnl-neg';
          const captureCls=(r.capture_rate||0)>70?'pnl-pos':(r.capture_rate||0)>40?'':'pnl-neg';
          const t=r.closed_at?new Date(r.closed_at).toLocaleString('zh-CN',{hour:'2-digit',minute:'2-digit'}):'';
          let holdStr='-';
          if(r.entry_time&&r.closed_at){const hs=Math.floor((new Date(r.closed_at).getTime()-new Date(r.entry_time).getTime())/1000);holdStr=hs>=3600?Math.floor(hs/3600)+'h'+Math.floor((hs%3600)/60)+'m':(hs>=60?Math.floor(hs/60)+'m'+(hs%60)+'s':hs+'s');}
          return '<tr><td>$'+r.symbol+'</td><td>$'+r.entry_mc_k+'K</td><td>'+r.entry_sol+'</td><td class="'+realPnlCls+'">'+(r.real_pnl>=0?'+':'')+(r.real_pnl||0).toFixed(1)+'%</td><td class="pnl-pos">+'+(r.high_pnl||0).toFixed(1)+'%</td><td class="pnl-neg">-'+(r.loss||0).toFixed(1)+'%</td><td class="'+captureCls+'">'+(r.capture_rate||0).toFixed(0)+'%</td><td>'+(r.exit_reason||'-')+'</td><td>'+holdStr+'</td><td>'+t+'</td></tr>';
        }).join('');
      }catch(e){document.getElementById('summary').innerHTML='<div class="card"><div class="big-num red">加载失败</div></div>';}
    }

    // 交易状态轮询
    async function refreshTradingStatus(){
      try{
        const r=await fetch('/api/trading-status'+_q);
        const d=await r.json();
        const el=document.getElementById('trading-status');
        const btnP=document.getElementById('btn-pause-p');
        const btnR=document.getElementById('btn-resume-p');
        const canTrade=d.canTrade;
        const isPaused=d.paused;
        const isBlocked=canTrade&&!canTrade.allowed;
        const lossInfo=d.consecutiveLosses?' | 连亏:'+d.consecutiveLosses:'';
        const dailyInfo=d.dailyNetPnl!==undefined?' | 今日:'+(d.dailyNetPnl>=0?'+':'')+d.dailyNetPnl+'/'+(-d.dailyLossLimit)+' SOL':'';
        if(isPaused){
          const until=d.pausedUntil?new Date(d.pausedUntil).toLocaleString('zh-CN'):'';
          el.innerHTML='🔴 <span style="color:#ff4757">交易已暂停</span>'+(until?' (至 '+until+')':'')+lossInfo+dailyInfo;
          btnP.style.display='none';btnR.style.display='inline-block';
        }else if(isBlocked){
          el.innerHTML='🟡 <span style="color:#ffa502">交易受限</span> — '+(canTrade.reason||'')+lossInfo+dailyInfo;
          btnP.style.display='none';btnR.style.display='inline-block';
        }else{
          el.innerHTML='🟢 <span style="color:#2ed573">交易正常</span>'+lossInfo+dailyInfo;
          btnP.style.display='inline-block';btnR.style.display='none';
        }
      }catch(e){}
    }

    async function toggleTrading(action){
      if(action==='pause'&&!confirm('确认暂停交易？将暂停4小时。'))return;
      if(action==='resume'&&!confirm('确认恢复交易？连亏计数将重置。'))return;
      try{
        const ep=action==='pause'?'/api/pause-trading':'/api/resume-trading';
        const r=await fetch(ep+_q,{method:'POST'});
        const d=await r.json();
        if(d.success){alert(d.message);refreshTradingStatus();}
        else alert('失败: '+(d.error||'未知'));
      }catch(e){alert('请求失败: '+e.message);}
    }

    async function resetDailyLoss(){
      if(!confirm('确认重置今日亏损统计起点？\n历史数据不会被删除，仅从当前时间重新开始计算。'))return;
      try{
        const r=await fetch('/api/reset-daily-loss'+_q,{method:'POST'});
        const d=await r.json();
        if(d.success){alert(d.message);location.reload();}
        else alert('失败: '+(d.error||'未知'));
      }catch(e){alert('请求失败: '+e.message);}
    }

    async function resetLiveData(){
      if(!confirm('⚠️ 确认清空所有实盘交易数据？此操作不可撤销！'))return;
      if(!confirm('再次确认：将删除所有实盘交易记录并重置风控状态'))return;
      try{
        const r=await fetch('/api/reset-live-data'+_q,{method:'POST'});
        const d=await r.json();
        if(d.success){alert(d.message);location.reload();}
        else alert('失败: '+(d.error||'未知'));
      }catch(e){alert('请求失败: '+e.message);}
    }

    async function refreshWallet(){
      try{
        const r=await fetch('/api/wallet-balance'+_q);
        const d=await r.json();
        const el=document.getElementById('wallet-sol');
        if(d.balance!==null&&d.balance!==undefined){
          el.textContent=Number(d.balance).toFixed(4);
          el.style.color='#00d9ff';
        }else{
          el.textContent=d.error||'无法获取';
          el.style.color='#ff4757';
          el.style.fontSize='0.8em';
        }
      }catch(e){
        document.getElementById('wallet-sol').textContent='连接失败';
      }
    }

    loadData();
    refreshTradingStatus();
    refreshWallet();
    setInterval(loadData,15000);
    setInterval(refreshTradingStatus,10000);
    setInterval(refreshWallet,8000);
  </script>
</body>
</html>`;
}

/**
 * 启动服务器
 */
export function startDashboardServer(attempt = 0) {
  // Always use the configured PORT — never increment. On cloud platforms (Zeabur),
  // PORT is fixed and health checks only probe that exact port. Binding to PORT+N
  // means health checks fail, causing an infinite restart loop.
  const targetPort = parseInt(PORT);
  const MAX_ATTEMPTS = 10;
  const retryDelayMs = Math.min(3000 * (attempt + 1), 15000);

  server.removeAllListeners('error');

  server.on('error', (error) => {
    if (error?.code === 'EADDRINUSE' || error?.code === 'EPERM') {
      if (attempt < MAX_ATTEMPTS) {
        console.warn(`⚠️ Port ${targetPort} in use (${error.code}), retry ${attempt + 1}/${MAX_ATTEMPTS} in ${retryDelayMs}ms...`);
        setTimeout(() => startDashboardServer(attempt + 1), retryDelayMs);
      } else {
        console.error(`❌ Failed to bind port ${targetPort} after ${MAX_ATTEMPTS} attempts — health checks will fail.`);
      }
      return;
    }
    console.error(`❌ Dashboard server error: ${error.message}`);
  });

  try {
    server.listen(targetPort, '0.0.0.0', () => {
      console.log(`🌐 Dashboard server running at http://0.0.0.0:${targetPort}`);
    });
  } catch (error) {
    console.error(`❌ Sync listen error:`, error);
  }

  return server;
}

// 直接运行时启动服务器
// 兼容 PM2 启动方式 (process.env.name 在 ecosystem.config.cjs 中定义)
if (import.meta.url === `file://${process.argv[1]}` || process.env.name === 'dashboard') {
  startDashboardServer();
}

export default { startDashboardServer };
