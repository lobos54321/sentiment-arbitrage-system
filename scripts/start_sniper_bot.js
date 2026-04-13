/**
 * 独立本地狙击者进程（Sniper Bot）
 * 
 * 架构：
 * - 不起本地 Telegram 监听（无需 session token）
 * - 直接订阅远程服务器的 SSE 实时信号流：
 *   https://sentiment-arbitrage.zeabur.app/api/signals/stream?token=mytoken54321
 * - 信号进来后送入 MomentumResonance.js 的独立沙箱逻辑
 * - 完全不影响服务器或本地主系统任何代码
 * 
 * 本地启动方式：
 *   node scripts/start_sniper_bot.js
 * 或放后台：
 *   pm2 start scripts/start_sniper_bot.js --name "sniper-bot"
 */

import dotenv from 'dotenv';
import Database from 'better-sqlite3';
import https from 'https';
import { PremiumSignalEngine } from '../src/engines/premium-signal-engine.js';
import { executeSniperWatchlistEntry, evaluateSniperExit } from '../src/strategies/MomentumResonance.js';

dotenv.config();

// ─── 配置 ──────────────────────────────────────────────────────────
const SERVER_URL   = process.env.SNIPER_SERVER_URL  || 'https://sentiment-arbitrage.zeabur.app';
const SERVER_TOKEN = process.env.DASHBOARD_TOKEN    || 'mytoken54321';
const SSE_PATH     = `/api/signals/stream?token=${SERVER_TOKEN}`;
const DB_PATH      = process.env.DB_PATH            || './server_sentiment_arb.db';  // 本地 DB（只读/WAL共享）

const RECONNECT_DELAY_MS = 5_000;  // SSE 断线后 5 秒重连

// ─── 统计 ──────────────────────────────────────────────────────────
const stats = {
  signals_received:    0,
  ath_blocked:         0,
  mc_vol_filtered:     0,
  sandbox_passed:      0,
  sandbox_rejected:    0,
  errors:              0,
  started_at:          new Date().toISOString(),
};

// ─── 初始化 ────────────────────────────────────────────────────────
console.log('\n' + '═'.repeat(60));
console.log('🔫  Sniper Bot (Momentum Resonance) — 本地独立进程');
console.log('═'.repeat(60));
console.log(`📡 信号源: ${SERVER_URL}${SSE_PATH}`);
console.log(`📦 DB: ${DB_PATH}`);
console.log('═'.repeat(60) + '\n');

// 复用本地 DB 做 ATH 历史持久化（WAL 模式支持多进程并发读写）
const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');

// 构造一个轻量级 Engine 仅用于提供内部工具函数
const miniEngine = new PremiumSignalEngine({ DB_PATH }, db);

const systemContext = {
  livePositionMonitor: { positions: new Map() },
  shadowMode:          true,   // 设为 false + 传入真实执行器即可转实盘
  shadowTracker:       { hasOpenPosition: () => false },
  stats,
  _backfillPrebuyKlines:    miniEngine._backfillPrebuyKlines?.bind(miniEngine)    || (async () => ({ enough: false })),
  _waitForFreshLocalKlines: miniEngine._waitForFreshLocalKlines?.bind(miniEngine) || (async () => null),
  _checkKline:              miniEngine._checkKline.bind(miniEngine),
  signalHistory:            miniEngine.signalHistory,
  _saveAthCounts:           miniEngine._saveAthCounts?.bind(miniEngine)           || (() => {}),
  saveSignalRecord:         (signal, reason) => {
    console.log(`  📝 [记录] $${signal.symbol} → ${reason}`);
  },
};

// ─── 信号处理 ──────────────────────────────────────────────────────
async function handleSignal(signal) {
  stats.signals_received++;
  const symbol = signal.symbol || signal.token_ca?.substring(0, 8) || '?';
  const ca     = signal.token_ca;
  if (!ca) return;

  const mc  = signal.market_cap  || 0;
  const vol = signal.volume_24h  || 0;

  console.log('\n' + '─'.repeat(56));
  console.log(`🔥 [#${stats.signals_received}] $${symbol}  MC:$${(mc/1000).toFixed(1)}K  Vol:$${(vol/1000).toFixed(1)}K`);
  console.log(`   CA: ${ca}`);
  console.log(`   is_ath=${signal.is_ath}  signal_type=${signal.signal_type}`);
  console.log('─'.repeat(56));

  try {
    const decision = await executeSniperWatchlistEntry(ca, signal, systemContext);

    if (decision.action === 'READY_TO_BUY') {
      stats.sandbox_passed++;
      console.log(`\n🎯 [买入指令] $${symbol} 沙箱通过，准备执行!\n`);
      // ── 实盘接入点 ──────────────────────────────────────────
      // await jupiterExecutor.buy(ca, 0.06, { mc });
      // await livePositionMonitor.addPosition(ca, symbol, entryPrice, ...);
      // ───────────────────────────────────────────────────────
    } else if (decision.reason === 'block_ath_entirely' || decision.reason === 'block_ever_ath') {
      stats.ath_blocked++;
    } else if (decision.reason === 'mc_vol_filter') {
      stats.mc_vol_filtered++;
    } else {
      stats.sandbox_rejected++;
    }
  } catch (err) {
    stats.errors++;
    console.error(`❌ 处理信号时错误: ${err.message}`);
  }

  // 定期打印统计
  if (stats.signals_received % 10 === 0) printStats();
}

// ─── SSE 连接 ──────────────────────────────────────────────────────
function connectSSE() {
  console.log(`\n🔌 正在连接 SSE 信号流...`);

  const options = {
    hostname: new URL(SERVER_URL).hostname,
    path:     SSE_PATH,
    method:   'GET',
    headers:  { 'Accept': 'text/event-stream', 'Cache-Control': 'no-cache' },
  };

  const req = https.request(options, (res) => {
    if (res.statusCode !== 200) {
      console.error(`❌ SSE 连接失败: HTTP ${res.statusCode}`);
      res.resume();
      scheduleReconnect();
      return;
    }

    console.log(`✅ SSE 已连接，等待实时信号推送...\n`);
    let buffer = '';

    res.on('data', (chunk) => {
      buffer += chunk.toString();
      const lines = buffer.split('\n');
      buffer = lines.pop(); // 保留未完整的最后一行

      for (const line of lines) {
        if (line.startsWith('data: ')) {
          const raw = line.slice(6).trim();
          if (!raw || raw === ':ping') continue;
          try {
            const msg = JSON.parse(raw);
            // 过滤：只处理 NEW_TRENDING 信号
            if (msg.event === 'signal' && msg.signal) {
              const sig = msg.signal;
              if (sig.signal_type === 'NEW_TRENDING' || (!sig.is_ath && !sig.signal_type)) {
                handleSignal(sig).catch(e => console.error('❌ handleSignal crash:', e.message));
              } else if (sig.is_ath || sig.signal_type === 'ATH') {
                console.log(`📈 [忽略 ATH] $${sig.symbol}`);
              } else if (msg.event === 'connected') {
                console.log(`📡 SSE 握手确认: ${msg.timestamp}`);
              }
            }
          } catch (e) {
            // 非 JSON 行（如 :ping）静默跳过
          }
        }
      }
    });

    res.on('end', () => {
      console.warn('\n⚠️  SSE 连接断开，即将重连...');
      scheduleReconnect();
    });

    res.on('error', (err) => {
      console.error(`❌ SSE 响应流错误: ${err.message}`);
      scheduleReconnect();
    });
  });

  req.on('error', (err) => {
    console.error(`❌ SSE 请求错误: ${err.message}`);
    scheduleReconnect();
  });

  req.setTimeout(90_000, () => {
    // 90 秒无数据视为超时（SSE ping 每 30s 一次，90s 内没信号也正常）
    // 保持连接活跃，不重连
  });

  req.end();
}

function scheduleReconnect() {
  console.log(`🔄 ${RECONNECT_DELAY_MS / 1000}s 后重新连接...`);
  setTimeout(connectSSE, RECONNECT_DELAY_MS);
}

// ─── 统计打印 ──────────────────────────────────────────────────────
function printStats() {
  console.log('\n' + '━'.repeat(56));
  console.log('📊 Sniper Bot 运行统计');
  console.log('━'.repeat(56));
  console.log(`  启动时间:     ${stats.started_at}`);
  console.log(`  收到信号:     ${stats.signals_received}`);
  console.log(`  ATH 拦截:     ${stats.ath_blocked}`);
  console.log(`  流动性过滤:   ${stats.mc_vol_filtered}`);
  console.log(`  沙箱通过✅:   ${stats.sandbox_passed}`);
  console.log(`  沙箱拒绝❌:   ${stats.sandbox_rejected}`);
  console.log(`  处理错误:     ${stats.errors}`);
  console.log('━'.repeat(56) + '\n');
}

// 每 5 分钟打印一次统计
setInterval(printStats, 5 * 60 * 1000);

// ─── 优雅退出 ──────────────────────────────────────────────────────
process.on('SIGINT',  () => { printStats(); console.log('\n👋 Sniper Bot 停止'); process.exit(0); });
process.on('SIGTERM', () => { printStats(); console.log('\n👋 Sniper Bot 停止'); process.exit(0); });

// ─── 启动 ──────────────────────────────────────────────────────────
connectSSE();
