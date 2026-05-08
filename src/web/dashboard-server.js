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
import { exec } from 'child_process';
import {
  applyFinalBlocker,
  chooseFinalBlocker,
  finalBlockerFromEvent,
  finalBlockerFromMissed,
  finalBlockerFromTrade,
} from './lifecycle-summary-utils.js';
import { summarizePremiumSignalGateHealth } from './data-source-health-utils.js';

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

function parseUnixishTime(value) {
  if (!value) return null;
  const trimmed = String(value).trim();
  if (!trimmed) return null;
  if (/^\d+(\.\d+)?$/.test(trimmed)) {
    const numeric = Number(trimmed);
    if (!Number.isFinite(numeric)) return null;
    return numeric > 1_000_000_000_000 ? Math.floor(numeric / 1000) : Math.floor(numeric);
  }
  const parsedMs = Date.parse(trimmed);
  if (Number.isNaN(parsedMs)) return null;
  return Math.floor(parsedMs / 1000);
}

function windowedSinceTs(url, defaultHours = 6) {
  const explicit = parseUnixishTime(url.searchParams.get('since') || url.searchParams.get('since_ts'));
  if (explicit) return explicit;
  const all = String(url.searchParams.get('all') || '').toLowerCase();
  if (['1', 'true', 'yes'].includes(all)) return null;
  const hours = Math.max(1, Math.min(parseInt(url.searchParams.get('hours') || String(defaultHours), 10) || defaultHours, 168));
  return Math.floor(Date.now() / 1000) - hours * 3600;
}

function missedAttributionTimeWhere(sinceTs, alias = '') {
  if (!sinceTs) return '';
  const prefix = alias ? `${alias}.` : '';
  return `WHERE (
    COALESCE(${prefix}signal_ts, 0) >= @since
    OR COALESCE(${prefix}created_event_ts, 0) >= @since
    OR COALESCE(${prefix}baseline_ts, 0) >= @since
  )`;
}

function maskedSecret(value) {
  const text = String(value || '');
  if (!text) return null;
  if (text.length <= 8) return `${text.slice(0, 2)}***`;
  return `${text.slice(0, 4)}***${text.slice(-4)}`;
}

function heliusConfigHealth() {
  const apiKey = process.env.HELIUS_API_KEY || '';
  const rpcUrl = process.env.HELIUS_RPC_URL || (apiKey ? `https://mainnet.helius-rpc.com/?api-key=${apiKey}` : '');
  const urlKey = (() => {
    try {
      return new URL(rpcUrl).searchParams.get('api-key') || '';
    } catch {
      return '';
    }
  })();
  return {
    api_key_present: Boolean(apiKey),
    api_key_masked: maskedSecret(apiKey),
    rpc_url_present: Boolean(rpcUrl),
    rpc_url_has_api_key: Boolean(urlKey),
    rpc_url_key_masked: maskedSecret(urlKey),
    rpc_url_key_matches_api_key: Boolean(apiKey && urlKey && apiKey === urlKey),
  };
}

async function probeHeliusRpcLive(timeoutMs = 5000) {
  const config = heliusConfigHealth();
  const apiKey = process.env.HELIUS_API_KEY || '';
  const rpcUrl = process.env.HELIUS_RPC_URL || (apiKey ? `https://mainnet.helius-rpc.com/?api-key=${apiKey}` : '');
  if (!rpcUrl) {
    return { ok: false, status: 'disabled', config };
  }
  const started = Date.now();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(rpcUrl, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ jsonrpc: '2.0', id: 'health', method: 'getHealth' }),
      signal: controller.signal,
    });
    const text = await response.text();
    let payload = null;
    try { payload = text ? JSON.parse(text) : null; } catch {}
    const rpcError = payload && payload.error ? payload.error : null;
    return {
      ok: response.ok && !rpcError,
      status: response.ok && !rpcError ? 'ok' : 'error',
      http_status: response.status,
      latency_ms: Date.now() - started,
      rpc_result: payload && payload.result !== undefined ? payload.result : null,
      rpc_error_code: rpcError ? rpcError.code : null,
      rpc_error_message: rpcError ? rpcError.message : null,
      body_excerpt: rpcError ? null : String(text || '').slice(0, 160),
      config,
    };
  } catch (e) {
    return {
      ok: false,
      status: e.name === 'AbortError' ? 'timeout' : 'error',
      latency_ms: Date.now() - started,
      error: e.message,
      config,
    };
  } finally {
    clearTimeout(timer);
  }
}

function tierCaseSql(expr, prefix = '') {
  return `
          SUM(CASE WHEN ${expr} >= 1.0 THEN 1 ELSE 0 END) AS ${prefix}gold_n,
          SUM(CASE WHEN ${expr} >= 0.5 AND ${expr} < 1.0 THEN 1 ELSE 0 END) AS ${prefix}silver_n,
          SUM(CASE WHEN ${expr} >= 0.25 AND ${expr} < 0.5 THEN 1 ELSE 0 END) AS ${prefix}bronze_n,
          SUM(CASE WHEN ${expr} < 0.25 THEN 1 ELSE 0 END) AS ${prefix}sub25_n`;
}

function parseJsonObject(value) {
  if (!value || typeof value !== 'string') return {};
  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

function firstValue(...values) {
  for (const value of values) {
    if (value !== undefined && value !== null && String(value).trim() !== '') return value;
  }
  return null;
}

function usableSymbol(value) {
  if (value === undefined || value === null) return null;
  const symbol = String(value).trim();
  if (!symbol || symbol.toUpperCase() === 'UNKNOWN') return null;
  return symbol;
}

function normalizeUnixishMs(value) {
  if (value === undefined || value === null || value === '') return null;
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return null;
  return numeric > 1_000_000_000_000 ? Math.floor(numeric) : Math.floor(numeric * 1000);
}

function inferEntryMode(row) {
  const monitorState = parseJsonObject(row.monitor_state_json);
  const lottoState = parseJsonObject(row.lotto_state_json);
  const entryAudit = parseJsonObject(row.entry_execution_audit_json);
  const entryDecision = lottoState.entryDecision || {};
  const monitorContract = monitorState.entryDecisionContract || {};
  const auditContract = entryAudit.entryDecisionContract || {};
  return String(firstValue(
    row.entry_mode,
    monitorState.entryMode,
    monitorState.entry_mode,
    monitorState.smartEntryReason,
    monitorState.passReason,
    monitorContract.entry_mode,
    auditContract.entry_mode,
    entryDecision.entry_mode,
    lottoState.entry_mode,
    row.signal_route ? `${String(row.signal_route).toLowerCase()}_unknown` : null,
    row.strategy_stage,
    'unknown'
  ));
}

function entryModeBucket(entryMode, positionSizeSol) {
  const mode = String(entryMode || '').toLowerCase();
  const size = Number(positionSizeSol || 0);
  if (mode.includes('gmgn') && mode.includes('tiny_scout')) return 'gmgn_tiny_scout';
  if (mode.includes('tiny_scout')) return 'tiny_scout';
  if (mode.includes('tiny_probe')) return 'tiny_scout';
  if (mode.includes('probe') && size > 0 && size <= 0.005) return 'tiny_scout';
  if (mode.includes('scout') && size > 0 && size <= 0.005) return 'tiny_scout';
  if (mode.includes('scout') || mode.includes('probe')) return 'scout';
  return 'primary';
}

function athRecoveryFamilyFor(entryMode, monitorState = {}) {
  if (monitorState.athRecoveryFamily) return String(monitorState.athRecoveryFamily);
  const mode = String(entryMode || '');
  if (mode === 'ath_reclaim_after_failure_tiny_probe') return 'recent_failure_reclaim';
  if (mode === 'ath_matrix_dissonance_tiny_probe') return 'matrix_dissonance';
  if (mode === 'ath_micro_reclaim_tiny_probe') return 'micro_reclaim';
  return null;
}

function lifecycleSummaryKey(row) {
  return row.lifecycle_id || `${row.token_ca || 'unknown'}:${row.signal_ts || ''}`;
}

function decisionStatus(row) {
  const decision = String(row.decision || '').toLowerCase();
  const eventType = String(row.event_type || '').toLowerCase();
  if (row.component === 'execution_api' && decision === 'filled_paper') return 'entered';
  if (eventType.includes('exit') || eventType.includes('close')) return 'closed';
  if (['reject', 'skip', 'abort', 'remove', 'expire', 'block', 'fail'].includes(decision)) return 'blocked';
  if (decision === 'wait') return 'waiting';
  if (['pending', 'pass', 'arm', 'registered', 'candidate', 'received', 'warn'].includes(decision)) return 'active';
  return decision || 'unknown';
}

function roundNumber(value, digits = 3) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  const factor = 10 ** digits;
  return Math.round(n * factor) / factor;
}

function priceUnitAuditForTrade(row) {
  const entryAudit = parseJsonObject(row.entry_execution_audit_json);
  const exitAudit = parseJsonObject(row.exit_execution_audit_json);
  const monitorState = parseJsonObject(row.monitor_state_json);
  const warnings = [];

  const entryUnit = firstValue(entryAudit.effectivePriceUnit, entryAudit.entryPriceUnit, monitorState.entryPriceUnit);
  const triggerUnit = firstValue(monitorState.entryTriggerPriceUnit, entryAudit.entryTriggerPriceUnit);
  const quoteUnit = firstValue(monitorState.entryQuotePriceUnit, entryAudit.entryQuotePriceUnit);
  const exitUnit = firstValue(exitAudit.effectivePriceUnit, exitAudit.effectiveExitPriceUnit, monitorState.exitPriceUnit);
  const pnlUnit = firstValue(entryAudit.pnlUnit, exitAudit.pnlUnit, monitorState.pnlUnit);
  const accountingUnit = firstValue(entryAudit.accountingUnit, exitAudit.accountingUnit, monitorState.accountingUnit);

  if (row.entry_price != null && entryUnit !== 'SOL_PER_TOKEN') warnings.push('entry_price_unit_missing_or_not_sol_per_token');
  if (row.trigger_price != null && triggerUnit && triggerUnit !== 'SOL_PER_TOKEN') warnings.push('trigger_price_unit_not_sol_per_token');
  if (row.trigger_price != null && !triggerUnit) warnings.push('trigger_price_unit_missing');
  if (row.exit_price != null && Number(row.synthetic_close || 0) !== 1 && exitUnit && exitUnit !== 'SOL_PER_TOKEN') warnings.push('exit_price_unit_not_sol_per_token');
  if (row.exit_price != null && Number(row.synthetic_close || 0) !== 1 && !exitUnit) warnings.push('exit_price_unit_missing');
  if (row.pnl_pct != null && pnlUnit && pnlUnit !== 'RATIO_DECIMAL') warnings.push('pnl_unit_not_ratio_decimal');
  if (row.pnl_pct != null && !pnlUnit) warnings.push('pnl_unit_missing');
  if (accountingUnit && accountingUnit !== 'SOL') warnings.push('accounting_unit_not_sol');
  if (entryAudit.entryPriceUsd != null) warnings.push('legacy_entryPriceUsd_key_present');

  const entryEffective = Number(entryAudit.effectivePrice);
  if (Number.isFinite(entryEffective) && Number.isFinite(Number(row.entry_price))) {
    const gap = Math.abs(entryEffective - Number(row.entry_price));
    const denom = Math.max(Math.abs(entryEffective), Math.abs(Number(row.entry_price)), 1e-18);
    if (gap / denom > 0.000001) warnings.push('entry_price_differs_from_execution_effective_price');
  }

  return {
    trade_id: row.id,
    token_ca: row.token_ca,
    symbol: row.symbol,
    entry_unit: entryUnit || null,
    trigger_unit: triggerUnit || null,
    quote_unit: quoteUnit || null,
    exit_unit: exitUnit || null,
    pnl_unit: pnlUnit || null,
    accounting_unit: accountingUnit || null,
    accounting_source: firstValue(exitAudit.accountingSource, monitorState.accountingSource),
    synthetic_close: Number(row.synthetic_close || 0) === 1,
    warnings,
  };
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

function formatLogArg(arg) {
  if (arg instanceof Error) {
    const detail = {
      name: arg.name,
      message: arg.message,
      stack: arg.stack,
    };
    if (arg.code) detail.code = arg.code;
    if (arg.cause) detail.cause = arg.cause instanceof Error
      ? { name: arg.cause.name, message: arg.cause.message, stack: arg.cause.stack }
      : arg.cause;
    return JSON.stringify(detail);
  }
  if (typeof arg === 'object' && arg !== null) {
    try {
      return JSON.stringify(arg);
    } catch (e) {
      return String(arg);
    }
  }
  return String(arg);
}

function captureLog(level, args) {
  const timestamp = new Date().toISOString();
  const message = args.map(formatLogArg).join(' ');
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
  } else if (url.pathname === '/api/paper/data-source-policy') {
    if (!checkAuth(req, url, res)) return;
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({
      generated_at: new Date().toISOString(),
      policy_version: 'v1_explicit_fail_modes',
      principles: [
        'Execution price and PnL must use SOL/token quote truth, not GMGN/DexScreener USD marks.',
        'Entry quote failure is fail-closed: do not enter without a fresh Jupiter-compatible quote.',
        'GMGN unavailable is fail-soft: do not boost or rescue, but do not reject the base signal only because GMGN is down.',
        'DexScreener trend unavailable is route-dependent: LOTTO defensive gates should wait/expire; optional watchlist guards may warn and fail-open.',
        'Exit quote failure does not synthesize profit; it records quote failure and keeps monitoring unless a trapped/no-route fail-safe triggers.',
      ],
      boundaries: [
        { boundary: 'entry_execution_quote', source: 'Jupiter/shared quote', unavailable: 'fail_closed', action: 'retry within quote window, then drop pending entry' },
        { boundary: 'entry_timing_price', source: 'Redis/shared quote/DexScreener/GeckoTerminal', unavailable: 'fail_closed', action: 'SmartEntry rejects no_price' },
        { boundary: 'gmgn_lotto_policy', source: 'GMGN readonly enrichment', unavailable: 'fail_soft', action: 'allow base route, disable GMGN boost/tiny rescue' },
        { boundary: 'lotto_defense_snapshot', source: 'DexScreener + Helius', unavailable: 'defensive_wait_or_expire', action: 'do not treat missing liquidity/activity as a positive signal' },
        { boundary: 'watchlist_optional_fire_guard', source: 'DexScreener', unavailable: 'warn_fail_open', action: 'do not block solely on missing optional MC/liquidity guard' },
        { boundary: 'exit_trigger_price', source: 'fresh SOL/token price snapshot', unavailable: 'hold_and_log', action: 'do not close from stale or missing mark price' },
        { boundary: 'exit_execution_quote', source: 'Jupiter/shared quote', unavailable: 'fail_safe_after_retries', action: 'record quote failure; trapped/no-route fail-safe can synthetic-close later' },
      ],
    }, null, 2));
    return;
  } else if (url.pathname === '/api/paper/price-unit-audit') {
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    let paperDb;
    try {
      const limit = Math.max(1, Math.min(parseInt(url.searchParams.get('limit') || '250', 10) || 250, 1000));
      const sinceTs = parseUnixishTime(url.searchParams.get('since') || url.searchParams.get('since_ts'));
      paperDb = new Database(paperDbPath, { readonly: true });
      const tableNames = new Set(paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name));
      if (!tableNames.has('paper_trades')) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'paper_trades table not found' }));
        return;
      }
      const cols = getTableColumns(paperDb, 'paper_trades');
      const where = sinceTs ? 'WHERE entry_ts >= @since' : '';
      const rows = paperDb.prepare(`
        SELECT id, token_ca, symbol, entry_ts, exit_ts, entry_price,
               ${cols.has('trigger_price') ? 'trigger_price' : 'NULL AS trigger_price'},
               exit_price, pnl_pct,
               ${cols.has('synthetic_close') ? 'synthetic_close' : '0 AS synthetic_close'},
               ${cols.has('entry_execution_audit_json') ? 'entry_execution_audit_json' : 'NULL AS entry_execution_audit_json'},
               ${cols.has('exit_execution_audit_json') ? 'exit_execution_audit_json' : 'NULL AS exit_execution_audit_json'},
               ${cols.has('monitor_state_json') ? 'monitor_state_json' : 'NULL AS monitor_state_json'}
        FROM paper_trades
        ${where}
        ORDER BY id DESC
        LIMIT @limit
      `).all(sinceTs ? { since: sinceTs, limit } : { limit });
      const audits = rows.map(priceUnitAuditForTrade);
      const counters = {
        sampled_trades: audits.length,
        clean_n: audits.filter((row) => row.warnings.length === 0).length,
        warning_n: audits.filter((row) => row.warnings.length > 0).length,
        entry_sol_per_token_n: audits.filter((row) => row.entry_unit === 'SOL_PER_TOKEN').length,
        exit_sol_per_token_n: audits.filter((row) => row.exit_unit === 'SOL_PER_TOKEN').length,
        pnl_ratio_decimal_n: audits.filter((row) => row.pnl_unit === 'RATIO_DECIMAL').length,
        accounting_sol_n: audits.filter((row) => row.accounting_unit === 'SOL').length,
        synthetic_close_n: audits.filter((row) => row.synthetic_close).length,
      };
      const warningCounts = {};
      for (const audit of audits) {
        for (const warning of audit.warnings) {
          warningCounts[warning] = (warningCounts[warning] || 0) + 1;
        }
      }
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        audit_version: 'v1_price_unit_contract',
        db_path: paperDbPath,
        filters: {
          since_ts: sinceTs,
          since_iso: sinceTs ? new Date(sinceTs * 1000).toISOString() : null,
          limit,
        },
        canonical_units: {
          entry_price: 'SOL_PER_TOKEN',
          trigger_price: 'SOL_PER_TOKEN',
          exit_price: 'SOL_PER_TOKEN unless synthetic_close=1',
          pnl_pct: 'RATIO_DECIMAL stored in DB, displayed as percent by APIs',
          accounting: 'SOL',
          market_context: 'USD fields are allowed only as context, not fill/PnL truth',
        },
        status: counters.warning_n > 0 ? 'warn' : 'ok',
        counters,
        warning_counts: warningCounts,
        warnings: audits.filter((row) => row.warnings.length > 0).slice(0, 100),
      }, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (paperDb) paperDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/paper/provider-live-health') {
    if (!checkAuth(req, url, res)) return;
    try {
      const timeoutMs = Math.max(1000, Math.min(parseInt(url.searchParams.get('timeout_ms') || '5000', 10) || 5000, 15000));
      const helius = await probeHeliusRpcLive(timeoutMs);
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        providers: {
          helius,
        },
      }, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    }
    return;
  } else if (url.pathname === '/api/paper/data-source-health') {
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    let paperDb;
    let signalDb;
    try {
      const nowSec = Math.floor(Date.now() / 1000);
      const sinceTs = windowedSinceTs(url, 6) || (nowSec - 6 * 60 * 60);
      paperDb = new Database(paperDbPath, { readonly: true });
      const tableNames = new Set(paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name));
      const health = {
        status: 'ok',
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        window: {
          since_ts: sinceTs,
          since_iso: new Date(sinceTs * 1000).toISOString(),
        },
        fail_modes: {
          fail_closed_entry: [],
          fail_soft_enrichment: [],
          hold_and_log_exit: [],
          warn_fail_open_optional: [],
        },
        counters: {},
        external_alpha_health: [],
        premium_signal_gate_health: null,
        provider_config_health: {
          helius: heliusConfigHealth(),
        },
        signal_db_path: resolvedDbPath,
        open_exit_quote_risk: null,
        missed_attribution_coverage: null,
        notes: [],
      };

      if (tableNames.has('paper_decision_events')) {
        const eventCounts = paperDb.prepare(`
          SELECT
            SUM(CASE WHEN component = 'execution_api' AND event_type = 'entry_quote' AND decision = 'fail' THEN 1 ELSE 0 END) AS entry_quote_fail_n,
            SUM(CASE WHEN component = 'execution_api' AND event_type = 'exit_quote' AND decision = 'fail' THEN 1 ELSE 0 END) AS exit_quote_fail_n,
            SUM(CASE WHEN component = 'smart_entry' AND decision = 'reject' AND reason = 'no_price' THEN 1 ELSE 0 END) AS smart_entry_no_price_n,
            SUM(CASE WHEN component = 'entry_readiness' AND event_type = 'watchlist_fire_deferred' THEN 1 ELSE 0 END) AS readiness_defer_n,
            SUM(CASE WHEN component = 'entry_readiness' AND event_type = 'watchlist_fire_expired' THEN 1 ELSE 0 END) AS readiness_expire_n,
            SUM(CASE WHEN reason LIKE '%rate_limited%' OR reason LIKE '%429%' THEN 1 ELSE 0 END) AS rate_limited_n,
            COUNT(*) AS total_events
          FROM paper_decision_events
          WHERE event_ts >= @since
        `).get({ since: sinceTs });
        health.counters = {
          total_events: eventCounts.total_events || 0,
          entry_quote_fail_n: eventCounts.entry_quote_fail_n || 0,
          exit_quote_fail_n: eventCounts.exit_quote_fail_n || 0,
          smart_entry_no_price_n: eventCounts.smart_entry_no_price_n || 0,
          readiness_defer_n: eventCounts.readiness_defer_n || 0,
          readiness_expire_n: eventCounts.readiness_expire_n || 0,
          rate_limited_n: eventCounts.rate_limited_n || 0,
        };
        health.fail_modes.fail_closed_entry = paperDb.prepare(`
          SELECT component, event_type, reason, data_source, COUNT(*) AS n, MAX(event_ts) AS last_event_ts
          FROM paper_decision_events
          WHERE event_ts >= @since
            AND (
              (component = 'execution_api' AND event_type = 'entry_quote' AND decision = 'fail')
              OR (component = 'smart_entry' AND decision = 'reject' AND reason = 'no_price')
            )
          GROUP BY component, event_type, reason, data_source
          ORDER BY n DESC, last_event_ts DESC
          LIMIT 20
        `).all({ since: sinceTs });
        health.fail_modes.hold_and_log_exit = paperDb.prepare(`
          SELECT reason, data_source, COUNT(*) AS n, MAX(event_ts) AS last_event_ts
          FROM paper_decision_events
          WHERE event_ts >= @since
            AND component = 'execution_api'
            AND event_type = 'exit_quote'
            AND decision = 'fail'
          GROUP BY reason, data_source
          ORDER BY n DESC, last_event_ts DESC
          LIMIT 20
        `).all({ since: sinceTs });
        health.fail_modes.warn_fail_open_optional = paperDb.prepare(`
          SELECT component, event_type, reason, data_source, COUNT(*) AS n, MAX(event_ts) AS last_event_ts
          FROM paper_decision_events
          WHERE event_ts >= @since
            AND decision IN ('warn', 'wait')
            AND (
              reason LIKE '%data%'
              OR reason LIKE '%dex%'
              OR reason LIKE '%liquidity%'
              OR reason LIKE '%readiness%'
            )
          GROUP BY component, event_type, reason, data_source
          ORDER BY n DESC, last_event_ts DESC
          LIMIT 20
        `).all({ since: sinceTs });
      } else {
        health.notes.push('paper_decision_events table missing; decision-level health unavailable');
      }

      if (tableNames.has('paper_trades')) {
        const tradeCols = getTableColumns(paperDb, 'paper_trades');
        if (tradeCols.has('exit_quote_failures') && tradeCols.has('last_exit_quote_failure')) {
          health.open_exit_quote_risk = paperDb.prepare(`
            SELECT
              COUNT(*) AS open_with_failures_n,
              MAX(exit_quote_failures) AS max_exit_quote_failures,
              SUM(CASE WHEN last_exit_quote_failure = 'no_route' THEN 1 ELSE 0 END) AS no_route_open_n,
              SUM(CASE WHEN last_exit_quote_failure = 'token_not_tradable' THEN 1 ELSE 0 END) AS token_not_tradable_open_n
            FROM paper_trades
            WHERE exit_reason IS NULL
              AND COALESCE(exit_quote_failures, 0) > 0
          `).get();
        }
      }

      if (tableNames.has('external_alpha_health')) {
        health.external_alpha_health = paperDb.prepare(`
          SELECT
            source,
            last_run_ts,
            last_success_ts,
            @now - COALESCE(last_run_ts, 0) AS last_run_age_sec,
            @now - COALESCE(last_success_ts, 0) AS last_success_age_sec,
            candidate_count,
            recorded_count,
            momentum_confirmed_count,
            error_count,
            last_error,
            updated_at
          FROM external_alpha_health
          ORDER BY updated_at DESC
        `).all({ now: nowSec });
        health.fail_modes.fail_soft_enrichment = health.external_alpha_health
          .filter((row) => !row.last_success_ts || row.last_success_age_sec > 15 * 60 || row.last_error)
          .map((row) => ({
            source: row.source,
            last_success_age_sec: row.last_success_age_sec,
            candidate_count: row.candidate_count,
            recorded_count: row.recorded_count,
            error_count: row.error_count,
            last_error: row.last_error,
          }));
      } else {
        health.notes.push('external_alpha_health table missing; GMGN scout health unavailable');
      }

      if (fs.existsSync(resolvedDbPath)) {
        try {
          signalDb = new Database(resolvedDbPath, { readonly: true });
          const signalTables = new Set(signalDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name));
          if (signalTables.has('premium_signals')) {
            const signalCols = getTableColumns(signalDb, 'premium_signals');
            const timestampExpr = signalCols.has('timestamp')
              ? "CASE WHEN timestamp > 1000000000000 THEN CAST(timestamp / 1000 AS INTEGER) ELSE CAST(timestamp AS INTEGER) END"
              : "0";
            const gateRows = signalDb.prepare(`
              SELECT
                id,
                symbol,
                token_ca,
                ${signalCols.has('timestamp') ? 'timestamp' : 'NULL AS timestamp'},
                ${signalCols.has('hard_gate_status') ? 'hard_gate_status' : 'NULL AS hard_gate_status'},
                ${signalCols.has('gate_result') ? 'gate_result' : 'NULL AS gate_result'}
              FROM premium_signals
              WHERE ${timestampExpr} >= @since
              ORDER BY ${timestampExpr} DESC, id DESC
              LIMIT 500
            `).all({ since: sinceTs });
            health.premium_signal_gate_health = summarizePremiumSignalGateHealth(gateRows);
            if (health.premium_signal_gate_health.status !== 'ok') {
              health.notes.push('premium_signals gate_result shows upstream provider issues');
            }
          } else {
            health.notes.push('premium_signals table missing; upstream signal gate health unavailable');
          }
        } catch (e) {
          health.notes.push(`premium_signals gate health unavailable: ${e.message}`);
        }
      } else {
        health.notes.push('sentiment database missing; upstream signal gate health unavailable');
      }

      if (tableNames.has('paper_missed_signal_attribution')) {
        const missedCols = getTableColumns(paperDb, 'paper_missed_signal_attribution');
        const tradableMissedExpr = missedCols.has('tradable_missed')
          ? "SUM(CASE WHEN tradable_missed = 1 THEN 1 ELSE 0 END)"
          : "NULL";
        health.missed_attribution_coverage = paperDb.prepare(`
          SELECT
            COUNT(*) AS total_n,
            SUM(CASE WHEN baseline_price IS NOT NULL THEN 1 ELSE 0 END) AS baseline_n,
            SUM(CASE WHEN status = 'baseline_missing' THEN 1 ELSE 0 END) AS baseline_missing_n,
            SUM(CASE WHEN pnl_5m IS NOT NULL THEN 1 ELSE 0 END) AS pnl_5m_n,
            ${tradableMissedExpr} AS tradable_missed_n
          FROM paper_missed_signal_attribution
          WHERE COALESCE(signal_ts, created_event_ts, baseline_ts, 0) >= @since
        `).get({ since: sinceTs });
      }

      const warnReasons = [];
      if ((health.counters.entry_quote_fail_n || 0) > 0) warnReasons.push('entry_quote_failures_present');
      if ((health.counters.smart_entry_no_price_n || 0) > 5) warnReasons.push('smart_entry_no_price_spike');
      if ((health.counters.exit_quote_fail_n || 0) > 0) warnReasons.push('exit_quote_failures_present');
      if (health.premium_signal_gate_health && (health.premium_signal_gate_health.counters.rate_limited_n || 0) > 0) warnReasons.push('premium_signal_provider_rate_limited');
      if (health.premium_signal_gate_health && (health.premium_signal_gate_health.counters.invalid_api_key_n || 0) > 0) warnReasons.push('premium_signal_provider_auth_failed');
      if (health.premium_signal_gate_health && (health.premium_signal_gate_health.counters.unknown_data_blocked_n || 0) > 0) warnReasons.push('premium_signal_unknown_data_blocks_present');
      if (health.fail_modes.fail_soft_enrichment.length > 0) warnReasons.push('external_alpha_degraded');
      if (health.open_exit_quote_risk && (health.open_exit_quote_risk.open_with_failures_n || 0) > 0) warnReasons.push('open_positions_have_exit_quote_failures');
      if (health.missed_attribution_coverage && health.missed_attribution_coverage.total_n > 0) {
        const baselinePct = (health.missed_attribution_coverage.baseline_n || 0) / health.missed_attribution_coverage.total_n;
        if (baselinePct < 0.8) warnReasons.push('missed_attribution_baseline_coverage_low');
      }
      if (warnReasons.length) {
        health.status = warnReasons.some((reason) => reason.includes('entry_quote') || reason.includes('open_positions')) ? 'warn' : 'degraded';
        health.warn_reasons = warnReasons;
      }

      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(health, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (paperDb) paperDb.close(); } catch {}
      try { if (signalDb) signalDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/paper/entry-mode-performance') {
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    let paperDb;
    try {
      const limit = Math.max(1, Math.min(parseInt(url.searchParams.get('limit') || '1000', 10) || 1000, 10000));
      const sinceTs = parseUnixishTime(url.searchParams.get('since') || url.searchParams.get('since_ts'));
      paperDb = new Database(paperDbPath, { readonly: true });
      const hasTable = paperDb.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='paper_trades'").get();
      if (!hasTable) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'paper_trades table not found' }));
        return;
      }
      const cols = getTableColumns(paperDb, 'paper_trades');
      const selectCols = [
        'id', 'symbol', 'token_ca', 'entry_ts', 'exit_ts', 'exit_reason', 'pnl_pct', 'peak_pnl',
        'position_size_sol', 'signal_route', 'signal_type', 'strategy_stage',
        cols.has('entry_mode') ? 'entry_mode' : 'NULL AS entry_mode',
        cols.has('monitor_state_json') ? 'monitor_state_json' : 'NULL AS monitor_state_json',
        cols.has('lotto_state_json') ? 'lotto_state_json' : 'NULL AS lotto_state_json',
        cols.has('entry_execution_audit_json') ? 'entry_execution_audit_json' : 'NULL AS entry_execution_audit_json',
      ];
      const whereSql = sinceTs ? 'WHERE entry_ts >= @since' : '';
      const rows = paperDb.prepare(`
        SELECT ${selectCols.join(', ')}
        FROM paper_trades
        ${whereSql}
        ORDER BY entry_ts DESC, id DESC
        LIMIT @limit
      `).all(sinceTs ? { since: sinceTs, limit } : { limit });

      const groups = new Map();
      const recent = [];
      for (const row of rows) {
        const entryAudit = parseJsonObject(row.entry_execution_audit_json);
        const monitorState = parseJsonObject(row.monitor_state_json);
        const entryMode = inferEntryMode(row);
        const bucket = entryModeBucket(entryMode, row.position_size_sol);
        const key = `${bucket}:${entryMode}`;
        const athRecoveryFamily = athRecoveryFamilyFor(entryMode, monitorState);
        const parentBlockReason = firstValue(monitorState.parentBlockReason, monitorState.parent_block_reason);
        const recoveryProbeReason = firstValue(monitorState.recoveryProbeReason, monitorState.recovery_probe_reason);
        const closed = row.exit_ts != null || row.exit_reason != null;
        const pnl = row.pnl_pct == null ? null : Number(row.pnl_pct);
        const peak = row.peak_pnl == null ? null : Number(row.peak_pnl);
        const entryQuoteSuccess = entryAudit.success === true || entryAudit.routeAvailable === true;
        const entryQuoteFailure = Boolean(entryAudit.failureReason) || entryAudit.success === false || entryAudit.routeAvailable === false;
        if (!groups.has(key)) {
          groups.set(key, {
            bucket,
            entry_mode: entryMode,
            total: 0,
            open: 0,
            closed: 0,
            wins: 0,
            losses: 0,
            total_pnl: 0,
            total_peak: 0,
            pnl_n: 0,
            peak_n: 0,
            total_position_size_sol: 0,
            position_n: 0,
            est_pnl_sol: 0,
            entry_quote_success_n: 0,
            entry_quote_failure_n: 0,
            ath_recovery_family: athRecoveryFamily,
            parent_block_reasons: {},
            recovery_probe_reasons: {},
          });
        }
        const g = groups.get(key);
        g.total += 1;
        if (closed) g.closed += 1;
        else g.open += 1;
        if (pnl != null && Number.isFinite(pnl)) {
          g.pnl_n += 1;
          g.total_pnl += pnl;
          if (closed && pnl > 0) g.wins += 1;
          if (closed && pnl <= 0) g.losses += 1;
          if (row.position_size_sol) g.est_pnl_sol += pnl * Number(row.position_size_sol || 0);
        }
        if (peak != null && Number.isFinite(peak)) {
          g.peak_n += 1;
          g.total_peak += peak;
        }
        if (row.position_size_sol != null) {
          g.position_n += 1;
          g.total_position_size_sol += Number(row.position_size_sol || 0);
        }
        if (entryQuoteSuccess) g.entry_quote_success_n += 1;
        if (entryQuoteFailure) g.entry_quote_failure_n += 1;
        if (athRecoveryFamily && !g.ath_recovery_family) g.ath_recovery_family = athRecoveryFamily;
        if (parentBlockReason) g.parent_block_reasons[parentBlockReason] = (g.parent_block_reasons[parentBlockReason] || 0) + 1;
        if (recoveryProbeReason) g.recovery_probe_reasons[recoveryProbeReason] = (g.recovery_probe_reasons[recoveryProbeReason] || 0) + 1;
        if (recent.length < 50) {
          recent.push({
            id: row.id,
            symbol: row.symbol,
            token_ca: row.token_ca,
            entry_ts: row.entry_ts,
            exit_ts: row.exit_ts,
            exit_reason: row.exit_reason,
            signal_route: row.signal_route,
            strategy_stage: row.strategy_stage,
            entry_mode: entryMode,
            bucket,
            position_size_sol: row.position_size_sol,
            pnl_pct: pnl == null ? null : roundNumber(pnl * 100, 2),
            peak_pnl_pct: peak == null ? null : roundNumber(peak * 100, 2),
            entry_quote_success: entryQuoteSuccess,
            entry_quote_failure_reason: entryAudit.failureReason || null,
            ath_recovery_family: athRecoveryFamily,
            parent_block_reason: parentBlockReason,
            recovery_probe_reason: recoveryProbeReason,
          });
        }
      }
      const byMode = Array.from(groups.values()).map((g) => ({
        bucket: g.bucket,
        entry_mode: g.entry_mode,
        total: g.total,
        open: g.open,
        closed: g.closed,
        wins: g.wins,
        losses: g.losses,
        win_rate_pct: g.closed ? roundNumber((g.wins / g.closed) * 100, 1) : null,
        avg_pnl_pct: g.pnl_n ? roundNumber((g.total_pnl / g.pnl_n) * 100, 2) : null,
        avg_peak_pnl_pct: g.peak_n ? roundNumber((g.total_peak / g.peak_n) * 100, 2) : null,
        avg_position_size_sol: g.position_n ? roundNumber(g.total_position_size_sol / g.position_n, 4) : null,
        est_pnl_sol: roundNumber(g.est_pnl_sol, 5),
        avg_ev_sol_per_trade: g.total ? roundNumber(g.est_pnl_sol / g.total, 6) : null,
        ath_recovery_family: g.ath_recovery_family || null,
        parent_block_reasons: g.parent_block_reasons,
        recovery_probe_reasons: g.recovery_probe_reasons,
        entry_quote_success_n: g.entry_quote_success_n,
        entry_quote_failure_n: g.entry_quote_failure_n,
        entry_quote_success_rate_pct: (g.entry_quote_success_n + g.entry_quote_failure_n)
          ? roundNumber((g.entry_quote_success_n / (g.entry_quote_success_n + g.entry_quote_failure_n)) * 100, 1)
          : null,
      })).sort((a, b) => {
        if (a.bucket !== b.bucket) return a.bucket.localeCompare(b.bucket);
        return b.total - a.total;
      });
      const bucketSummary = {};
      for (const g of byMode) {
        if (!bucketSummary[g.bucket]) bucketSummary[g.bucket] = { total: 0, closed: 0, open: 0, est_pnl_sol: 0 };
        bucketSummary[g.bucket].total += g.total;
        bucketSummary[g.bucket].closed += g.closed;
        bucketSummary[g.bucket].open += g.open;
        bucketSummary[g.bucket].est_pnl_sol += g.est_pnl_sol || 0;
      }
      for (const summary of Object.values(bucketSummary)) {
        summary.est_pnl_sol = roundNumber(summary.est_pnl_sol, 5);
      }
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        filters: {
          since_ts: sinceTs,
          since_iso: sinceTs ? new Date(sinceTs * 1000).toISOString() : null,
          limit,
        },
        bucket_summary: bucketSummary,
        by_entry_mode: byMode,
        recent,
      }, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (paperDb) paperDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/paper/lifecycle-summary') {
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    let paperDb;
    try {
      const limit = Math.max(1, Math.min(parseInt(url.searchParams.get('limit') || '100', 10) || 100, 500));
      const eventLimit = Math.max(limit, Math.min(parseInt(url.searchParams.get('event_limit') || '5000', 10) || 5000, 50000));
      const sinceTs = windowedSinceTs(url, 6);
      const statusFilter = (url.searchParams.get('status') || 'all').toLowerCase();
      paperDb = new Database(paperDbPath, { readonly: true });
      const tableNames = new Set(paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name));
      if (!tableNames.has('paper_decision_events')) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'paper_decision_events table not found' }));
        return;
      }
      const eventWhere = sinceTs ? 'WHERE event_ts >= @since' : '';
      const events = paperDb.prepare(`
        SELECT id, event_ts, signal_id, token_ca, symbol, lifecycle_id, trade_id,
               signal_ts, strategy_stage, route, component, event_type, decision,
               reason, data_source, lifecycle_state, vitality_score, entry_bias, payload_json
        FROM paper_decision_events
        ${eventWhere}
        ORDER BY event_ts DESC, id DESC
        LIMIT @eventLimit
      `).all(sinceTs ? { since: sinceTs, eventLimit } : { eventLimit });

      const summaries = new Map();
      const latestAthByToken = new Map();
      for (const event of events) {
        const key = lifecycleSummaryKey(event);
        const payload = parseJsonObject(event.payload_json);
        const routeLabel = String(firstValue(event.route, payload.watchlist_type, event.reason) || '').toUpperCase();
        const signalTsMs = normalizeUnixishMs(event.signal_ts);
        if (event.token_ca && routeLabel === 'ATH' && signalTsMs) {
          const previous = latestAthByToken.get(event.token_ca);
          if (!previous || signalTsMs > previous.signal_ts_ms) {
            latestAthByToken.set(event.token_ca, {
              signal_id: event.signal_id,
              signal_ts: event.signal_ts,
              signal_ts_ms: signalTsMs,
              lifecycle_id: event.lifecycle_id,
              symbol: usableSymbol(event.symbol) || (previous || {}).symbol || event.symbol || null,
              event_id: event.id,
              event_ts: event.event_ts,
            });
          } else if (previous && !usableSymbol(previous.symbol) && usableSymbol(event.symbol)) {
            previous.symbol = usableSymbol(event.symbol);
          }
        }
        const entryMode = firstValue(
          payload.entry_mode,
          payload.entryMode,
          (payload.entryDecisionContract || {}).entry_mode,
          (payload.entry_readiness_policy || {}).entry_mode
        );
        if (!summaries.has(key)) {
          summaries.set(key, {
            key,
            lifecycle_id: event.lifecycle_id,
            token_ca: event.token_ca,
            symbol: event.symbol,
            signal_id: event.signal_id,
            signal_ts: event.signal_ts,
            route: event.route,
            strategy_stage: event.strategy_stage,
            first_event_ts: event.event_ts,
            last_event_ts: event.event_ts,
            event_count: 0,
            final_status: decisionStatus(event),
            final_decision: event.decision,
            final_component: event.component,
            final_event_type: event.event_type,
            final_reason: event.reason,
            final_data_source: event.data_source,
            final_event_id: event.id,
            final_blocker: null,
            lifecycle_state: event.lifecycle_state,
            vitality_score: event.vitality_score,
            entry_bias: event.entry_bias,
            entry_mode: entryMode,
            has_trade: false,
            trade_count: 0,
            max_missed_pnl_pct: null,
            tradable_missed: null,
            tradability_status: null,
          });
        }
        const summary = summaries.get(key);
        summary.final_blocker = chooseFinalBlocker(
          summary.final_blocker,
          finalBlockerFromEvent(event, payload)
        );
        summary.event_count += 1;
        summary.first_event_ts = Math.min(summary.first_event_ts, event.event_ts);
        summary.last_event_ts = Math.max(summary.last_event_ts, event.event_ts);
        if (!summary.entry_mode && entryMode) summary.entry_mode = entryMode;
      }

      if (tableNames.has('paper_trades')) {
        const tradeCols = getTableColumns(paperDb, 'paper_trades');
        const tradeWhere = sinceTs ? 'WHERE entry_ts >= @since' : '';
        const tradeRows = paperDb.prepare(`
          SELECT id, lifecycle_id, token_ca, symbol, signal_ts, signal_route, signal_type,
                 strategy_stage, entry_ts, exit_ts, exit_reason, pnl_pct, peak_pnl,
                 position_size_sol,
                 ${tradeCols.has('entry_mode') ? 'entry_mode' : 'NULL AS entry_mode'},
                 ${tradeCols.has('monitor_state_json') ? 'monitor_state_json' : 'NULL AS monitor_state_json'},
                 ${tradeCols.has('lotto_state_json') ? 'lotto_state_json' : 'NULL AS lotto_state_json'},
                 ${tradeCols.has('entry_execution_audit_json') ? 'entry_execution_audit_json' : 'NULL AS entry_execution_audit_json'}
          FROM paper_trades
          ${tradeWhere}
          ORDER BY entry_ts DESC, id DESC
          LIMIT @eventLimit
        `).all(sinceTs ? { since: sinceTs, eventLimit } : { eventLimit });
        for (const trade of tradeRows) {
          const key = lifecycleSummaryKey(trade);
          if (!summaries.has(key)) {
            summaries.set(key, {
              key,
              lifecycle_id: trade.lifecycle_id,
              token_ca: trade.token_ca,
              symbol: trade.symbol,
              signal_ts: trade.signal_ts,
              route: trade.signal_route || trade.signal_type,
              strategy_stage: trade.strategy_stage,
              first_event_ts: trade.entry_ts,
              last_event_ts: trade.exit_ts || trade.entry_ts,
              event_count: 0,
              final_status: trade.exit_ts || trade.exit_reason ? 'closed' : 'entered',
              final_decision: trade.exit_ts || trade.exit_reason ? 'closed' : 'filled_paper',
              final_component: 'paper_trades',
              final_event_type: trade.exit_ts || trade.exit_reason ? 'trade_closed' : 'trade_open',
              final_reason: trade.exit_reason || 'open_position',
              final_blocker: finalBlockerFromTrade(trade),
              has_trade: true,
              trade_count: 0,
            });
          }
          const summary = summaries.get(key);
          summary.has_trade = true;
          summary.trade_count = (summary.trade_count || 0) + 1;
          summary.trade_id = trade.id;
          summary.entry_ts = trade.entry_ts;
          summary.exit_ts = trade.exit_ts;
          summary.exit_reason = trade.exit_reason;
          summary.pnl_pct = trade.pnl_pct == null ? null : roundNumber(Number(trade.pnl_pct) * 100, 2);
          summary.peak_pnl_pct = trade.peak_pnl == null ? null : roundNumber(Number(trade.peak_pnl) * 100, 2);
          summary.position_size_sol = trade.position_size_sol;
          if (summary.entry_mode) summary.event_entry_mode = summary.event_entry_mode || summary.entry_mode;
          summary.entry_mode = inferEntryMode(trade);
          summary.entry_mode_bucket = entryModeBucket(summary.entry_mode, trade.position_size_sol);
          summary.final_status = trade.exit_ts || trade.exit_reason ? 'closed' : 'entered';
          summary.final_decision = summary.final_status;
          summary.final_component = 'paper_trades';
          summary.final_event_type = summary.final_status === 'closed' ? 'trade_closed' : 'trade_open';
          summary.final_reason = trade.exit_reason || 'open_position';
          summary.final_blocker = finalBlockerFromTrade(trade);
        }
      }

      if (tableNames.has('paper_missed_signal_attribution')) {
        const missedCols = getTableColumns(paperDb, 'paper_missed_signal_attribution');
        const missedWhere = sinceTs ? 'WHERE COALESCE(signal_ts, created_event_ts, baseline_ts, 0) >= @since' : '';
        const missedRows = paperDb.prepare(`
          SELECT lifecycle_id, token_ca, symbol, signal_ts, route, component,
                 reject_reason, COALESCE(max_pnl_recorded, pnl_24h, pnl_60m, pnl_15m, pnl_5m, NULL) AS max_pnl,
                 ${missedCols.has('tradable_missed') ? 'tradable_missed' : 'NULL AS tradable_missed'},
                 ${missedCols.has('tradability_status') ? 'tradability_status' : 'NULL AS tradability_status'},
                 ${missedCols.has('would_stop_before_peak') ? 'would_stop_before_peak' : 'NULL AS would_stop_before_peak'}
          FROM paper_missed_signal_attribution
          ${missedWhere}
          ORDER BY COALESCE(max_pnl_recorded, pnl_24h, pnl_60m, pnl_15m, pnl_5m, -999) DESC
          LIMIT @eventLimit
        `).all(sinceTs ? { since: sinceTs, eventLimit } : { eventLimit });
        for (const missed of missedRows) {
          const key = lifecycleSummaryKey(missed);
          if (!summaries.has(key)) {
            summaries.set(key, {
              key,
              lifecycle_id: missed.lifecycle_id,
              token_ca: missed.token_ca,
              symbol: missed.symbol,
              signal_ts: missed.signal_ts,
              route: missed.route,
              first_event_ts: null,
              last_event_ts: null,
              event_count: 0,
              final_status: 'missed_only',
              final_component: missed.component,
              final_reason: missed.reject_reason,
              final_blocker: finalBlockerFromMissed(missed),
              has_trade: false,
              trade_count: 0,
            });
          }
          const summary = summaries.get(key);
          if (!summary.has_trade) {
            summary.final_blocker = chooseFinalBlocker(summary.final_blocker, finalBlockerFromMissed(missed));
          }
          const maxPnl = missed.max_pnl == null ? null : Number(missed.max_pnl);
          if (maxPnl != null && Number.isFinite(maxPnl)) {
            const prev = summary.max_missed_pnl_pct == null ? -Infinity : summary.max_missed_pnl_pct / 100;
            if (maxPnl > prev) {
              summary.max_missed_pnl_pct = roundNumber(maxPnl * 100, 2);
              summary.missed_component = missed.component;
              summary.missed_reason = missed.reject_reason;
              summary.tradable_missed = missed.tradable_missed;
              summary.tradability_status = missed.tradability_status;
              summary.would_stop_before_peak = missed.would_stop_before_peak;
            }
          }
        }
      }

      for (const summary of summaries.values()) {
        const latestAth = summary.token_ca ? latestAthByToken.get(summary.token_ca) : null;
        if (!latestAth) continue;
        summary.latest_ath_signal_id = latestAth.signal_id;
        summary.latest_ath_signal_ts = latestAth.signal_ts;
        summary.latest_ath_signal_ts_ms = latestAth.signal_ts_ms;
        summary.latest_ath_lifecycle_id = latestAth.lifecycle_id;
        summary.latest_ath_symbol = latestAth.symbol;

        if (String(summary.route || '').toUpperCase() === 'ATH') {
          const anchorTsMs = normalizeUnixishMs(summary.signal_ts);
          summary.anchor_signal_ts_ms = anchorTsMs;
          summary.anchor_is_latest_ath = Boolean(anchorTsMs && latestAth.signal_ts_ms && anchorTsMs >= latestAth.signal_ts_ms);
          summary.anchor_lag_sec = anchorTsMs && latestAth.signal_ts_ms && latestAth.signal_ts_ms > anchorTsMs
            ? Math.round((latestAth.signal_ts_ms - anchorTsMs) / 1000)
            : 0;
        }
      }

      let list = Array.from(summaries.values());
      for (const item of list) {
        applyFinalBlocker(item);
      }
      if (statusFilter !== 'all') {
        list = list.filter((item) => String(item.final_status || '').toLowerCase() === statusFilter);
      }
      const anchorMismatchCount = list.filter((item) => item.anchor_is_latest_ath === false).length;
      const counts = {};
      const byFinalGate = {};
      const byFinalBlocker = {};
      for (const item of list) {
        counts[item.final_status || 'unknown'] = (counts[item.final_status || 'unknown'] || 0) + 1;
        const blocker = item.final_blocker || {};
        const gateKey = `${blocker.component || item.final_component || '-'}:${blocker.reason || item.final_reason || '-'}`;
        const blockerKey = item.final_blocker_key || `${blocker.stage || 'unknown'}:${gateKey}`;
        if (!byFinalGate[gateKey]) {
          byFinalGate[gateKey] = {
            component: blocker.component || item.final_component || '-',
            reason: blocker.reason || item.final_reason || '-',
            n: 0,
            max_missed_pnl_pct: null,
            tradable_n: 0,
          };
        }
        if (!byFinalBlocker[blockerKey]) {
          byFinalBlocker[blockerKey] = {
            key: blockerKey,
            status: blocker.status || item.final_status || 'unknown',
            stage: blocker.stage || 'unknown',
            component: blocker.component || item.final_component || '-',
            reason: blocker.reason || item.final_reason || '-',
            n: 0,
            max_missed_pnl_pct: null,
            tradable_n: 0,
          };
        }
        byFinalGate[gateKey].n += 1;
        byFinalBlocker[blockerKey].n += 1;
        if (item.max_missed_pnl_pct != null) {
          byFinalGate[gateKey].max_missed_pnl_pct = Math.max(
            byFinalGate[gateKey].max_missed_pnl_pct == null ? -Infinity : byFinalGate[gateKey].max_missed_pnl_pct,
            item.max_missed_pnl_pct
          );
          byFinalBlocker[blockerKey].max_missed_pnl_pct = Math.max(
            byFinalBlocker[blockerKey].max_missed_pnl_pct == null ? -Infinity : byFinalBlocker[blockerKey].max_missed_pnl_pct,
            item.max_missed_pnl_pct
          );
        }
        if (Number(item.tradable_missed || 0) === 1) byFinalGate[gateKey].tradable_n += 1;
        if (Number(item.tradable_missed || 0) === 1) byFinalBlocker[blockerKey].tradable_n += 1;
      }
      for (const gate of Object.values(byFinalGate)) {
        if (gate.max_missed_pnl_pct === -Infinity) gate.max_missed_pnl_pct = null;
      }
      for (const blocker of Object.values(byFinalBlocker)) {
        if (blocker.max_missed_pnl_pct === -Infinity) blocker.max_missed_pnl_pct = null;
      }
      list.sort((a, b) => {
        const missedDelta = (b.max_missed_pnl_pct ?? -99999) - (a.max_missed_pnl_pct ?? -99999);
        if (missedDelta !== 0) return missedDelta;
        return (b.last_event_ts || b.entry_ts || 0) - (a.last_event_ts || a.entry_ts || 0);
      });
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        filters: {
          since_ts: sinceTs,
          since_iso: sinceTs ? new Date(sinceTs * 1000).toISOString() : null,
          status: statusFilter,
          limit,
          event_limit: eventLimit,
        },
        status_counts: counts,
        anchor_mismatch_count: anchorMismatchCount,
        by_final_blocker: Object.values(byFinalBlocker).sort((a, b) => b.n - a.n).slice(0, 100),
        by_final_gate: Object.values(byFinalGate).sort((a, b) => b.n - a.n).slice(0, 100),
        lifecycles: list.slice(0, limit),
      }, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (paperDb) paperDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/paper/missed-attribution') {
    // Paper missed-dog attribution summary — 需要 token 认证
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    let paperDb;
    try {
      const limit = Math.max(1, Math.min(parseInt(url.searchParams.get('limit') || '25', 10) || 25, 200));
      const sinceTs = windowedSinceTs(url, 6);
      const whereSql = missedAttributionTimeWhere(sinceTs);
      const whereParams = sinceTs ? { since: sinceTs } : {};
      paperDb = new Database(paperDbPath, { readonly: true });
      const tableNames = new Set(
        paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name)
      );
      if (!tableNames.has('paper_missed_signal_attribution')) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'paper_missed_signal_attribution table not found' }));
        return;
      }
      const missedCols = new Set(
        paperDb.prepare("PRAGMA table_info(paper_missed_signal_attribution)").all().map((row) => row.name)
      );
      const hasTradability = missedCols.has('tradable_missed');
      const hasDecisionEvents = tableNames.has('paper_decision_events');
      const spreadAbortExistsSql = hasDecisionEvents ? `
            EXISTS (
              SELECT 1
              FROM paper_decision_events e
              WHERE e.token_ca = paper_missed_signal_attribution.token_ca
                AND e.component = 'execution_guard'
                AND e.reason IN ('entry_edge_spread_too_high', 'spread_guard')
                AND e.event_ts >= COALESCE(paper_missed_signal_attribution.signal_ts, paper_missed_signal_attribution.created_event_ts, paper_missed_signal_attribution.baseline_ts, 0) - 60
                AND e.event_ts <= COALESCE(paper_missed_signal_attribution.signal_ts, paper_missed_signal_attribution.created_event_ts, paper_missed_signal_attribution.baseline_ts, 0) + 3600
            )` : '0';
      const quoteExecutableBaseExpression = hasTradability ? `
          CASE
            WHEN tradable_missed = 1
             AND COALESCE(would_stop_before_peak, 0) != 1
             AND NOT (${spreadAbortExistsSql})
            THEN 1 ELSE 0
          END` : 'NULL';
      const quoteExecutableSelect = 'quote_executable_proxy,';
      const tradabilitySelect = hasTradability ? `
          tradable_missed,
          tradability_status,
          tradability_reason,
          tradable_peak_pnl,
          tradable_peak_horizon,
          time_to_peak_sec,
          mae_before_peak_pnl,
          would_stop_before_peak,
          stop_floor_pnl,
          first_tradable_horizon,
          first_tradable_pnl,
          ${quoteExecutableSelect}` : `
          NULL AS tradable_missed,
          NULL AS tradability_status,
          NULL AS tradability_reason,
          NULL AS tradable_peak_pnl,
          NULL AS tradable_peak_horizon,
          NULL AS time_to_peak_sec,
          NULL AS mae_before_peak_pnl,
          NULL AS would_stop_before_peak,
          NULL AS stop_floor_pnl,
          NULL AS first_tradable_horizon,
          NULL AS first_tradable_pnl,
          NULL AS quote_executable_proxy,`;
      const tradabilityAgg = hasTradability ? `
          SUM(CASE WHEN tradable_missed = 1 THEN 1 ELSE 0 END) AS tradable_n,
          SUM(CASE WHEN tradable_missed = 1 AND COALESCE(would_stop_before_peak, 0) != 1 THEN 1 ELSE 0 END) AS clean_tradable_n,
          SUM(CASE WHEN tradable_missed = 1 AND COALESCE(would_stop_before_peak, 0) != 1 AND NOT (${spreadAbortExistsSql}) THEN 1 ELSE 0 END) AS quote_executable_proxy_n,
          SUM(CASE WHEN tradability_status = 'would_stop_before_peak' THEN 1 ELSE 0 END) AS stop_before_peak_n,` : `
          NULL AS tradable_n,
          NULL AS clean_tradable_n,
          NULL AS quote_executable_proxy_n,
          NULL AS stop_before_peak_n,`;
      const maxPnlExpr = 'COALESCE(max_pnl_recorded, pnl_24h, pnl_60m, pnl_15m, pnl_5m, -999)';
      const topDogs = paperDb.prepare(`
        SELECT
          symbol,
          token_ca,
          route,
          component,
          reject_reason,
          pnl_5m,
          pnl_15m,
          pnl_60m,
          pnl_24h,
          max_pnl_recorded,
          min_pnl_recorded,
          ${tradabilitySelect}
          status,
          updated_at
        FROM (
          SELECT
            *,
            ${quoteExecutableBaseExpression} AS quote_executable_proxy
          FROM paper_missed_signal_attribution
          ${whereSql}
        ) paper_missed_signal_attribution
        ORDER BY ${maxPnlExpr} DESC
        LIMIT @limit
      `).all({ ...whereParams, limit });
      const byGate = paperDb.prepare(`
        SELECT
          COALESCE(route, '-') AS route,
          component,
          reject_reason,
          COUNT(*) AS n,
          ${tierCaseSql('COALESCE(max_pnl_recorded, pnl_60m, pnl_15m, pnl_5m, 0)')},
          SUM(CASE WHEN COALESCE(max_pnl_recorded, pnl_60m, pnl_15m, pnl_5m, 0) >= 0.5 THEN 1 ELSE 0 END) AS dog50_n,
          SUM(CASE WHEN COALESCE(max_pnl_recorded, pnl_60m, pnl_15m, pnl_5m, 0) >= 1.0 THEN 1 ELSE 0 END) AS dog100_n,
          ${tradabilityAgg}
          AVG(pnl_5m) AS avg_5m,
          AVG(pnl_15m) AS avg_15m,
          AVG(pnl_60m) AS avg_60m,
          AVG(pnl_24h) AS avg_24h
        FROM paper_missed_signal_attribution
        ${whereSql}
        GROUP BY COALESCE(route, '-'), component, reject_reason
        ORDER BY dog100_n DESC, dog50_n DESC, n DESC
        LIMIT @limit
      `).all({ ...whereParams, limit });
      const eventTierSummary = paperDb.prepare(`
        SELECT
          COUNT(*) AS total_n,
          ${tierCaseSql('COALESCE(max_pnl_recorded, pnl_60m, pnl_15m, pnl_5m, 0)')},
          ${hasTradability ? `
          SUM(CASE WHEN tradable_missed = 1 THEN 1 ELSE 0 END) AS tradable_n,
          SUM(CASE WHEN tradable_missed = 1 AND COALESCE(would_stop_before_peak, 0) != 1 THEN 1 ELSE 0 END) AS clean_tradable_n,
          SUM(CASE WHEN tradable_missed = 1 AND COALESCE(would_stop_before_peak, 0) != 1 AND NOT (${spreadAbortExistsSql}) THEN 1 ELSE 0 END) AS quote_executable_proxy_n,
          SUM(CASE WHEN COALESCE(would_stop_before_peak, 0) = 1 THEN 1 ELSE 0 END) AS stop_before_peak_n` : `
          NULL AS tradable_n,
          NULL AS clean_tradable_n,
          NULL AS quote_executable_proxy_n,
          NULL AS stop_before_peak_n`}
        FROM paper_missed_signal_attribution
        ${whereSql}
      `).get(whereParams);
      const uniqueTierSummary = paperDb.prepare(`
        WITH per_token AS (
          SELECT
            token_ca,
            COALESCE(MAX(symbol), '?') AS symbol,
            MIN(COALESCE(signal_ts, created_event_ts, baseline_ts, 0)) AS first_event_ts,
            MAX(COALESCE(max_pnl_recorded, pnl_60m, pnl_15m, pnl_5m, 0)) AS max_pnl,
            ${hasTradability ? `
            MAX(COALESCE(tradable_missed, 0)) AS tradable_missed,
            MAX(COALESCE(would_stop_before_peak, 0)) AS would_stop_before_peak,
            MAX(CASE WHEN tradable_missed = 1 AND COALESCE(would_stop_before_peak, 0) != 1 AND NOT (${spreadAbortExistsSql}) THEN 1 ELSE 0 END) AS quote_executable_proxy` : `
            NULL AS tradable_missed,
            NULL AS would_stop_before_peak,
            NULL AS quote_executable_proxy`}
          FROM paper_missed_signal_attribution
          ${whereSql}
          GROUP BY token_ca
        )
        SELECT
          COUNT(*) AS total_n,
          ${tierCaseSql('max_pnl')},
          ${hasTradability ? `
          SUM(CASE WHEN tradable_missed = 1 THEN 1 ELSE 0 END) AS tradable_n,
          SUM(CASE WHEN tradable_missed = 1 AND COALESCE(would_stop_before_peak, 0) != 1 THEN 1 ELSE 0 END) AS clean_tradable_n,
          SUM(CASE WHEN quote_executable_proxy = 1 THEN 1 ELSE 0 END) AS quote_executable_proxy_n,
          SUM(CASE WHEN COALESCE(would_stop_before_peak, 0) = 1 THEN 1 ELSE 0 END) AS stop_before_peak_n` : `
          NULL AS tradable_n,
          NULL AS clean_tradable_n,
          NULL AS quote_executable_proxy_n,
          NULL AS stop_before_peak_n`}
        FROM per_token
      `).get(whereParams);
      const athEventTierSummary = paperDb.prepare(`
        SELECT
          COUNT(*) AS total_n,
          ${tierCaseSql('COALESCE(max_pnl_recorded, pnl_60m, pnl_15m, pnl_5m, 0)')},
          ${hasTradability ? `
          SUM(CASE WHEN tradable_missed = 1 THEN 1 ELSE 0 END) AS tradable_n,
          SUM(CASE WHEN tradable_missed = 1 AND COALESCE(would_stop_before_peak, 0) != 1 THEN 1 ELSE 0 END) AS clean_tradable_n,
          SUM(CASE WHEN tradable_missed = 1 AND COALESCE(would_stop_before_peak, 0) != 1 AND NOT (${spreadAbortExistsSql}) THEN 1 ELSE 0 END) AS quote_executable_proxy_n,
          SUM(CASE WHEN COALESCE(would_stop_before_peak, 0) = 1 THEN 1 ELSE 0 END) AS stop_before_peak_n` : `
          NULL AS tradable_n,
          NULL AS clean_tradable_n,
          NULL AS quote_executable_proxy_n,
          NULL AS stop_before_peak_n`}
        FROM paper_missed_signal_attribution
        ${whereSql ? `${whereSql} AND COALESCE(route, '') = 'ATH'` : "WHERE COALESCE(route, '') = 'ATH'"}
      `).get(whereParams);
      const athUniqueTierSummary = paperDb.prepare(`
        WITH per_token AS (
          SELECT
            token_ca,
            MAX(COALESCE(max_pnl_recorded, pnl_60m, pnl_15m, pnl_5m, 0)) AS max_pnl,
            ${hasTradability ? `
            MAX(COALESCE(tradable_missed, 0)) AS tradable_missed,
            MAX(COALESCE(would_stop_before_peak, 0)) AS would_stop_before_peak,
            MAX(CASE WHEN tradable_missed = 1 AND COALESCE(would_stop_before_peak, 0) != 1 AND NOT (${spreadAbortExistsSql}) THEN 1 ELSE 0 END) AS quote_executable_proxy` : `
            NULL AS tradable_missed,
            NULL AS would_stop_before_peak,
            NULL AS quote_executable_proxy`}
          FROM paper_missed_signal_attribution
          ${whereSql ? `${whereSql} AND COALESCE(route, '') = 'ATH'` : "WHERE COALESCE(route, '') = 'ATH'"}
          GROUP BY token_ca
        )
        SELECT
          COUNT(*) AS total_n,
          ${tierCaseSql('max_pnl')},
          ${hasTradability ? `
          SUM(CASE WHEN tradable_missed = 1 THEN 1 ELSE 0 END) AS tradable_n,
          SUM(CASE WHEN tradable_missed = 1 AND COALESCE(would_stop_before_peak, 0) != 1 THEN 1 ELSE 0 END) AS clean_tradable_n,
          SUM(CASE WHEN quote_executable_proxy = 1 THEN 1 ELSE 0 END) AS quote_executable_proxy_n,
          SUM(CASE WHEN COALESCE(would_stop_before_peak, 0) = 1 THEN 1 ELSE 0 END) AS stop_before_peak_n` : `
          NULL AS tradable_n,
          NULL AS clean_tradable_n,
          NULL AS quote_executable_proxy_n,
          NULL AS stop_before_peak_n`}
        FROM per_token
      `).get(whereParams);
      let athRecoveryActions = [];
      if (hasDecisionEvents) {
        const recoveryWhere = sinceTs ? 'AND event_ts >= @since' : '';
        athRecoveryActions = paperDb.prepare(`
          SELECT
            reason AS recovery_action,
            decision,
            COUNT(*) AS n,
            COUNT(DISTINCT token_ca) AS unique_tokens
          FROM paper_decision_events
          WHERE component = 'ath_recovery'
            ${recoveryWhere}
          GROUP BY reason, decision
          ORDER BY n DESC
          LIMIT @limit
        `).all({ ...whereParams, limit });
      }
      const topUniqueDogs = paperDb.prepare(`
        WITH ranked AS (
          SELECT
            *,
            ${quoteExecutableBaseExpression} AS quote_executable_proxy,
            COALESCE(max_pnl_recorded, pnl_24h, pnl_60m, pnl_15m, pnl_5m, -999) AS max_pnl,
            ROW_NUMBER() OVER (
              PARTITION BY token_ca
              ORDER BY COALESCE(max_pnl_recorded, pnl_24h, pnl_60m, pnl_15m, pnl_5m, -999) DESC
            ) AS rn
          FROM paper_missed_signal_attribution
          ${whereSql}
        )
        SELECT
          symbol,
          token_ca,
          route,
          component,
          reject_reason,
          pnl_5m,
          pnl_15m,
          pnl_60m,
          pnl_24h,
          max_pnl_recorded,
          min_pnl_recorded,
          ${tradabilitySelect}
          status,
          updated_at
        FROM ranked
        WHERE rn = 1
        ORDER BY max_pnl DESC
        LIMIT @limit
      `).all({ ...whereParams, limit });
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        limit,
        filters: {
          since_ts: sinceTs,
          since_iso: sinceTs ? new Date(sinceTs * 1000).toISOString() : null,
          tier_definition: 'gold>=100%, silver=50-100%, bronze=25-50% max/peak pnl',
        },
        tier_summary: {
          event_rows: eventTierSummary,
          unique_tokens: uniqueTierSummary,
          ath_event_rows: athEventTierSummary,
          ath_unique_tokens: athUniqueTierSummary,
        },
        top_dogs: topDogs,
        top_unique_dogs: topUniqueDogs,
        by_gate: byGate,
        ath_recovery_actions: athRecoveryActions,
      }, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (paperDb) paperDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/stats/missed-gates') {
    // Most expensive gates: which SmartEntry reject reasons caused the most missed gold/silver dogs.
    // Query paper_decision_events (timing rejects) joined against paper_missed_signal_attribution.
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    let paperDb;
    try {
      const hoursBack = Math.max(1, Math.min(parseInt(url.searchParams.get('hours') || '24', 10) || 24, 168));
      const sinceTs = Math.floor(Date.now() / 1000) - hoursBack * 3600;
      paperDb = new Database(paperDbPath, { readonly: true });
      const tableNames = new Set(
        paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((r) => r.name)
      );
      const hasEvents = tableNames.has('paper_decision_events');
      const hasMissed = tableNames.has('paper_missed_signal_attribution');
      const missedCols = hasMissed
        ? new Set(paperDb.prepare('PRAGMA table_info(paper_missed_signal_attribution)').all().map((r) => r.name))
        : new Set();
      const hasTradability = missedCols.has('tradable_missed') && missedCols.has('tradable_peak_pnl');

      // Gate stats from decision_events
      let gateRows = [];
      if (hasEvents) {
        gateRows = paperDb.prepare(`
          SELECT
            reason,
            COUNT(*) AS total_rejects,
            COUNT(DISTINCT token_ca) AS unique_tokens
          FROM paper_decision_events
          WHERE component = 'smart_entry'
            AND event_type = 'timing_decision'
            AND decision = 'reject'
            AND event_ts >= @since
          GROUP BY reason
          ORDER BY total_rejects DESC
        `).all({ since: sinceTs });
      }

      // Missed attribution by reject_reason
      let missedRows = [];
      if (hasMissed && hasTradability) {
        missedRows = paperDb.prepare(`
          SELECT
            reject_reason,
            COUNT(*) AS missed_n,
            SUM(CASE WHEN tradable_missed = 1 AND COALESCE(would_stop_before_peak, 0) = 0 THEN 1 ELSE 0 END) AS clean_missed,
            SUM(CASE WHEN COALESCE(tradable_peak_pnl, 0) >= 0.5 THEN 1 ELSE 0 END) AS gold_missed,
            SUM(CASE WHEN COALESCE(tradable_peak_pnl, 0) >= 0.25 AND COALESCE(tradable_peak_pnl, 0) < 0.5 THEN 1 ELSE 0 END) AS silver_missed,
            ROUND(MAX(COALESCE(tradable_peak_pnl, 0)) * 100, 1) AS max_missed_pct
          FROM paper_missed_signal_attribution
          WHERE COALESCE(signal_ts, created_event_ts, 0) >= @since
            AND reject_reason IS NOT NULL
          GROUP BY reject_reason
          ORDER BY gold_missed DESC, silver_missed DESC, clean_missed DESC
        `).all({ since: sinceTs });
      }

      // Merge: attach missed-attribution counts to gate stats
      const missedByReason = {};
      for (const r of missedRows) missedByReason[r.reject_reason] = r;
      const merged = gateRows.map((g) => {
        const m = missedByReason[g.reason] || {};
        return {
          reason: g.reason,
          total_rejects: g.total_rejects,
          unique_tokens: g.unique_tokens,
          missed_n: m.missed_n || 0,
          clean_missed: m.clean_missed || 0,
          gold_missed: m.gold_missed || 0,
          silver_missed: m.silver_missed || 0,
          max_missed_pct: m.max_missed_pct || null,
        };
      });
      // Add any missed-attribution reasons not in decision_events
      for (const r of missedRows) {
        if (!gateRows.find((g) => g.reason === r.reject_reason)) {
          merged.push({
            reason: r.reject_reason,
            total_rejects: 0,
            unique_tokens: 0,
            missed_n: r.missed_n,
            clean_missed: r.clean_missed,
            gold_missed: r.gold_missed,
            silver_missed: r.silver_missed,
            max_missed_pct: r.max_missed_pct,
          });
        }
      }
      merged.sort((a, b) => (b.gold_missed + b.silver_missed) - (a.gold_missed + a.silver_missed) || b.total_rejects - a.total_rejects);

      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        window_hours: hoursBack,
        since_ts: sinceTs,
        gates: merged,
      }, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (paperDb) paperDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/paper/external-alpha-health') {
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    let paperDb;
    try {
      paperDb = new Database(paperDbPath, { readonly: true });
      const tableNames = new Set(
        paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name)
      );
      const stateCount = tableNames.has('external_alpha_state')
        ? paperDb.prepare("SELECT COUNT(*) AS n FROM external_alpha_state").get().n
        : null;
      const snapshotCount = tableNames.has('external_alpha_snapshots')
        ? paperDb.prepare("SELECT COUNT(*) AS n FROM external_alpha_snapshots").get().n
        : null;
      const health = tableNames.has('external_alpha_health')
        ? paperDb.prepare("SELECT * FROM external_alpha_health ORDER BY updated_at DESC").all()
        : [];
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        state_count: stateCount,
        snapshot_count: snapshotCount,
        health,
      }, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (paperDb) paperDb.close(); } catch {}
    }
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
        exec(`tail -n ${tailLines} ${paperTraderLogPath}`, { maxBuffer: 1024 * 1024 * 50 }, (error, stdout, stderr) => {
          if (error) {
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: error.message }));
            return;
          }
          res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
          res.end(stdout);
        });
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
