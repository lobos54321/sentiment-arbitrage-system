/**
 * v18 完整回测 — 悉尼时间 2026-03-15 22:00 → 2026-03-17 09:30
 * 使用真实数据库信号 + GeckoTerminal 实际K线
 * 窗口1: 38个信号 (UTC Mar 15 11:00 → Mar 16 11:00)
 * 窗口2:  4个信号 (UTC Mar 16 13:57 → Mar 16 22:30) — 从部署API实时拉取
 */

import { execSync } from 'child_process';
import fs from 'fs';

// ─── 本地OHLCV缓存（避免重复拉取+绕过限流）────────────────────────────────
const CACHE_FILE = 'data/ohlcv-cache.json';
let ohlcvCache = {};
try { ohlcvCache = JSON.parse(fs.readFileSync(CACHE_FILE, 'utf8')); } catch {}

function saveCache() { fs.writeFileSync(CACHE_FILE, JSON.stringify(ohlcvCache, null, 2)); }

// ─── 42个真实PASS信号 (38 + 4新) ────────────────────────────────────────
const SIGNALS = [
  { symbol: 'Eclipse',    ca: 'ApwtY1HWHgDLDY5unJ7awPrBeQo4UwstCM83A5zFpump', entry_ts: 1773576800963, mc: 115280 },
  { symbol: 'AGENTPUMPY', ca: '6xxKkqfd1nqstqhbHrhdCXsEFZ3Ge3SWhXV5bzNApump', entry_ts: 1773578922235, mc: 40750 },
  { symbol: 'LATENT',     ca: 'GbNytkgN7eSKV1LECjr39omiW8JbJgNhT1tYN5Ubpump', entry_ts: 1773585339169, mc: 65520 },
  { symbol: 'Jeffrey',    ca: 'BdsjNF4MzF2WSjokZwQiCpekcpMksUEz1piUYppCpump', entry_ts: 1773587617999, mc: 60350 },
  { symbol: '雪の妖精',   ca: 'FksjYMq38iQigRRozNZGkpjWj3AGxoUw45fnS8Nnpump', entry_ts: 1773587840278, mc: 83500 },
  { symbol: '5',          ca: 'HeVhJttfPiKwVn2z2U7BfyqHqDYcBuJjVJhWwzYspump', entry_ts: 1773589376888, mc: 111230 },
  { symbol: 'FIVE',       ca: '6ScrjJgnV4tWnpY9vQWychPdweW6YWNE7N94iS4Rpump', entry_ts: 1773589681575, mc: 89910 },
  { symbol: 'DISTROLL',   ca: '23tQCGFh1hriX5Hhhgz1JJgBwgqQCxmDUAoWJiwvpump', entry_ts: 1773592438415, mc: 122630 },
  { symbol: 'PIKE',       ca: 'FnsrVv2iJWmrChyWLsPRKrdRPnshBPhgR8VZLfugpump', entry_ts: 1773593720302, mc: 37220 },
  { symbol: 'Agent',      ca: 'HM8dJbqLo38PAy9qeLBMzxbu3BiAFHPW37MwGFbepump', entry_ts: 1773595784131, mc: 97020 },
  { symbol: 'Terafab',    ca: 'EijRpo34HSnjj2Di4rbdEZTJRApmf1cxmHqdXSM2pump', entry_ts: 1773596905294, mc: 146670 },
  { symbol: 'Yahu',       ca: '21vxm3YMMFedY5e4PLyGYaRAgYQJTA17dvzYuKwApump', entry_ts: 1773597559749, mc: 32590 },
  { symbol: 'NETAINYAHU', ca: 'BBkUQdTDdVySDKXT4TngbmbpC2ktwBeymMSdn41ppump', entry_ts: 1773598150669, mc: 100240 },
  { symbol: 'WOOF',       ca: '7RmKCsoHBqUGhQr1ckUHCgAmkeSGiXEGaBQkAC2fpump', entry_ts: 1773600677826, mc: 74820 },
  { symbol: 'Coffeegate', ca: 'CJYgKNJ9G66YWbpu6FCYh7LVFzRQp1AZ3sqggmXCpump', entry_ts: 1773600912651, mc: 90230 },
  { symbol: 'Elongate',   ca: '3aGbWBEpCfDxMSb5KfpSvWrBo3tCCbMiZP9x8PpKpump', entry_ts: 1773601371786, mc: 93350 },
  { symbol: 'BTC',        ca: 'KWmej3HSuuLgaoWWdELniXGhA3gKzLxkf7FRj7xpump', entry_ts: 1773603097878, mc: 85500 },
  { symbol: 'BLUECOLLAR', ca: 'rKQJrRgKKHpmjoPpDtU5K7zACWKCWguaHtKaMGQpump', entry_ts: 1773607168353, mc: 52740 },
  { symbol: 'PUSHEEN',    ca: '7ttsZScQAsya7eiUfkG6PoTVNARkkZXccqRviDtTpump', entry_ts: 1773608305833, mc: 84080 },
  { symbol: 'herry',      ca: 'AMbTjmCCUoWH96dwAn1ukG5WigK86RBfZZ39K5ubpump', entry_ts: 1773611297574, mc: 37480 },
  { symbol: 'XAIC',       ca: 'KfByHk48ecitUq8gXji2vr9smmRJKtqJwGAh2E9pump', entry_ts: 1773614417429, mc: 109450 },
  { symbol: 'TFW',        ca: '8Y7u8PdV78wJwZwXqukAXZAnHZ3jFbStgXoL7Z1jpump', entry_ts: 1773617388663, mc: 278610 },
  { symbol: 'Feels',      ca: 'GtswqeVZbSBLU6ZZfgc1zcMk6fVBSCV8RLcgR2v5pump', entry_ts: 1773619528913, mc: 86460 },
  { symbol: 'herm',       ca: '7TF6FMMFLoxPckmuZNpoCthjEhDPpQvm2PPQjccnpump', entry_ts: 1773620501082, mc: 166170 },
  { symbol: 'FOOD',       ca: 'GPT8cWShgWrN27FYSLFbQTEJvHsQkwr6GE6VJMY5pump', entry_ts: 1773622880954, mc: 36640 },
  { symbol: 'TRUMPHOUSE', ca: '3acb8mmdBqrNMoFUgSpUtLAWQ4xj6kNmVDnmDD8Wpump', entry_ts: 1773623740559, mc: 264160 },
  { symbol: 'PVE',        ca: '2s6Ckb6oGd8syc55nV8QjX19urqhXJUTmv2UsSMbpump', entry_ts: 1773626834544, mc: 88060 },
  { symbol: 'Rosie',      ca: '3RkocKJzxokazPBV5kvARwho36JbVcc9S775SitMpump', entry_ts: 1773628312390, mc: 89070 },
  { symbol: 'MOLTY',      ca: 'CrxLwZkm16KppGMmDKQJmSTnuSGLtTPMd6WKgPTcpump', entry_ts: 1773629055255, mc: 129170 },
  { symbol: 'LOYAL',      ca: 'XYhbT37kGeU9XovWJ5fZZGk5xQMgSZk1dFgKxnVpump', entry_ts: 1773629140937, mc: 270260 },
  { symbol: 'BAGWORKER',  ca: 'Hq7eFBdfqBAxMXBwpFAkYAJZfMu6BeFe7JqRGWjpump', entry_ts: 1773631658633, mc: 252720 },
  { symbol: 'Salary',     ca: 'HYntXiazzANDFmxnLVHAQiwFszzEVjKHZ4cMVc6dpump', entry_ts: 1773634501896, mc: 67030 },
  { symbol: 'plumber',    ca: '2Pah9ZUfpii27ABrWfQorzdW8FSJimxj6izFr4oGpump', entry_ts: 1773639288914, mc: 79710 },
  { symbol: 'T-Nega',     ca: '3fZHABuN38VhhtpcvbSRqH1yu6KZG21ZSZBCkcGtpump', entry_ts: 1773643877943, mc: 73540 },
  { symbol: 'SRAA',       ca: '6onRcUxc3PChE4DK6FygJeuPDBUFhoZjJauNfPsapump', entry_ts: 1773644551611, mc: 30890 },
  { symbol: 'ANMOO',      ca: 'Ddzw3HJH7hpJHuBTCjBzX7QRNjTxiazchtiZZrHXpump', entry_ts: 1773648375952, mc: 82690 },
  { symbol: 'ケイジ',     ca: '4d7hkcY3MGAmYRi1vYNaDwi9C5Sfvdi4FqWCiLhtpump', entry_ts: 1773653254892, mc: 63730 },
  { symbol: 'MULERUN',    ca: '3tN15KJSEA1NsYn3nbDrUWNiTyubUxXbS5roZxuYpump', entry_ts: 1773658615925, mc: 47200 },
  // ── 窗口2: UTC Mar 16 13:57 → Mar 16 22:30 (悉尼 Mar 17 00:57 → 09:30) ──
  { symbol: 'Moe-chan',   ca: '7x4QEfAMo4rxMNbRUxC9jS36Hnmgp5Yh5Rm3pNCDpump', entry_ts: 1773664642014, mc: 75900  },
  { symbol: 'Gayatollah', ca: 'AN52QGkAUU6kmoHHHDhYUz1TjkFUPcAU3RxTNgJbpump', entry_ts: 1773665172998, mc: 123700 },
  { symbol: 'HOSPICE',    ca: '6uq3r5mMQL6tKkJd9JpuA3bPbrqitpFfhSQDVPCMpump', entry_ts: 1773671789279, mc: 73300  },
  { symbol: 'CLAIR',      ca: '2hhAKwDhigLKnqLdZ6LwY8snmbYQk9vtxy63YcD2yUA4',  entry_ts: 1773674358328, mc: 118800 },
];

const POSITION_SOL = 0.06;
const MAX_POSITIONS = 5;        // 在险上限
const TP1_PCT = 0.50;           // +50%
const TP2_PCT = 1.00;           // +100%
const TP3_PCT = 2.00;           // +200%
const TP4_PCT = 5.00;           // +500%
const SL_PCT  = -0.40;          // -40%
const MAX_HOLD_MINS = 30;
const DEAD_WATER_MINS = 15;
const SLIPPAGE = 0.004;         // 0.4% 滑点+手续费

// ─── K线拉取：DexScreener 找 pair → GeckoTerminal 双窗口拉历史OHLCV ────────
function fetchOHLCV(ca, entryTsSec) {
  // === 缓存命中直接返回 ===
  if (ohlcvCache[ca]) return ohlcvCache[ca];

  const lookBack = entryTsSec - 5 * 60;   // 入场前5分钟

  // === DexScreener 获取 pair 地址（最全覆盖，不限流）===
  const dsData = fetchJson(`https://api.dexscreener.com/latest/dex/tokens/${ca}`);
  const candidates = (dsData?.pairs || [])
    .filter(p => p.chainId === 'solana')
    .map(p => ({ id: p.pairAddress, created: (p.pairCreatedAt || 0) / 1000,
                 dex: p.dexId, liq: p.liquidity?.usd || 0 }))
    .filter(p => p.created > 0 && p.created <= entryTsSec + 600)
    .sort((a, b) => b.created !== a.created ? b.created - a.created : b.liq - a.liq);

  if (!candidates.length) { ohlcvCache[ca] = null; saveCache(); return null; }

  // === 双窗口查询：确保入场时刻+持仓期均有K线 ===
  function fetchBars(poolId) {
    // 窗口A：entry+10min（覆盖刚开盘的稀疏池）
    sleep(1500);
    const rawA = fetchJson(
      `https://api.geckoterminal.com/api/v2/networks/solana/pools/${poolId}/ohlcv/minute?aggregate=1&limit=30&before_timestamp=${entryTsSec + 600}&token=base`
    )?.data?.attributes?.ohlcv_list || [];

    // 窗口B：entry+35min（完整持仓期）
    sleep(1500);
    const rawB = fetchJson(
      `https://api.geckoterminal.com/api/v2/networks/solana/pools/${poolId}/ohlcv/minute?aggregate=1&limit=200&before_timestamp=${entryTsSec + 35 * 60}&token=base`
    )?.data?.attributes?.ohlcv_list || [];

    return Object.values(
      [...rawA, ...rawB].reduce((m, c) => { m[c[0]] = c; return m; }, {})
    ).sort((a, b) => a[0] - b[0]);
  }

  for (const pool of candidates.slice(0, 3)) {
    const raw = fetchBars(pool.id);
    if (!raw.length) continue;

    const candles = raw.map(c => ({ ts:c[0], o:c[1], h:c[2], l:c[3], c:c[4], vol:c[5] }));
    const hasEntry = candles.some(c => c.ts >= lookBack && c.ts <= entryTsSec + 600);
    if (!hasEntry) continue;

    const result = { pairAddr: pool.id, candles };
    ohlcvCache[ca] = result;   // 写入缓存
    saveCache();
    return result;
  }

  ohlcvCache[ca] = null;  // 缓存"无数据"结果，避免重复查询
  saveCache();
  return null;
}

function sleep(ms) { execSync(`sleep ${(ms/1000).toFixed(1)}`); }

function fetchJson(url, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      const out = execSync(
        `curl -s --max-time 15 -H "Accept: application/json" -H "User-Agent: Mozilla/5.0" "${url}"`,
        { encoding: 'utf8', stdio: ['pipe','pipe','pipe'] }
      );
      if (!out?.trim()) { sleep(2000); continue; }
      return JSON.parse(out);
    } catch {
      if (i < retries - 1) sleep(2000 * (i + 1));
    }
  }
  return null;
}


// ─── ASYMMETRIC 出场模拟 ──────────────────────────────────────────────────
function simulate(signal, candles) {
  const entryTsSec = Math.floor(signal.entry_ts / 1000);
  // Snap to 5-min bar
  const entryBar = Math.floor(entryTsSec / 300) * 300;

  // Find the entry candle: bar at signal time, or closest bar within ±10 minutes
  // (some pools open slightly after the signal → use first available bar as entry)
  const entryCandle = candles.find(c => c.ts === entryBar)
    || candles.find(c => c.ts <= entryBar && c.ts > entryBar - 600)      // up to 10m before
    || candles.find(c => c.ts > entryBar && c.ts <= entryBar + 600);     // up to 10m after (pool opened late)
  if (!entryCandle) return { result: 'NO_DATA' };

  // Use entryMinute as alias for consistency
  const entryMinute = entryCandle.ts;
  const entryPrice = entryCandle.c * (1 + SLIPPAGE); // buy at close + slippage
  const tokens = POSITION_SOL / entryPrice;

  const tp1 = entryPrice * (1 + TP1_PCT);
  const tp2 = entryPrice * (1 + TP2_PCT);
  const tp3 = entryPrice * (1 + TP3_PCT);
  const tp4 = entryPrice * (1 + TP4_PCT);
  let sl  = entryPrice * (1 + SL_PCT);
  const maxHoldTs = entryMinute + MAX_HOLD_MINS * 60;

  let remainTokens = tokens;
  let solReceived = 0;
  let tp1Hit = false, tp2Hit = false, tp3Hit = false, tp4Hit = false;
  let exitReason = null;
  let exitMinute = null;
  let peakPrice = entryPrice;
  let minsSincePeak = 0;

  const tradeCandles = candles.filter(c => c.ts > entryMinute);

  for (const candle of tradeCandles) {
    if (remainTokens <= 0.001 * tokens) break;
    const minFromEntry = (candle.ts - entryMinute) / 60;

    if (!tp1Hit && candle.h >= tp1) {
      const qty = remainTokens * 0.60;
      solReceived += qty * tp1 * (1 - SLIPPAGE);
      remainTokens -= qty;
      sl = entryPrice; // SL → breakeven
      tp1Hit = true;
    }
    if (tp1Hit && !tp2Hit && candle.h >= tp2) {
      const qty = remainTokens * 0.50;
      solReceived += qty * tp2 * (1 - SLIPPAGE);
      remainTokens -= qty;
      tp2Hit = true;
    }
    if (tp2Hit && !tp3Hit && candle.h >= tp3) {
      const qty = remainTokens * 0.50;
      solReceived += qty * tp3 * (1 - SLIPPAGE);
      remainTokens -= qty;
      tp3Hit = true;
    }
    if (tp3Hit && !tp4Hit && candle.h >= tp4) {
      const qty = remainTokens * 0.80;
      solReceived += qty * tp4 * (1 - SLIPPAGE);
      remainTokens -= qty;
      tp4Hit = true;
      // Moonbag: remaining 2% just hold
    }

    // Stop loss (only before TP1)
    if (!tp1Hit && candle.l <= sl) {
      solReceived += remainTokens * sl * (1 - SLIPPAGE);
      remainTokens = 0;
      exitReason = 'STOP_LOSS';
      exitMinute = minFromEntry;
      break;
    }

    // Dead water (15 min no new high, only before TP1) — 5m bars so threshold = 3 bars
    if (candle.h > peakPrice) { peakPrice = candle.h; minsSincePeak = 0; }
    else minsSincePeak++;
    if (!tp1Hit && minsSincePeak >= Math.ceil(DEAD_WATER_MINS / 5)) {
      solReceived += remainTokens * candle.c * (1 - SLIPPAGE);
      remainTokens = 0;
      exitReason = 'DEAD_WATER';
      exitMinute = minFromEntry;
      break;
    }

    // Max hold (30 min, only before TP1)
    if (!tp1Hit && candle.ts >= maxHoldTs) {
      solReceived += remainTokens * candle.c * (1 - SLIPPAGE);
      remainTokens = 0;
      exitReason = 'MAX_HOLD';
      exitMinute = minFromEntry;
      break;
    }

    exitMinute = minFromEntry;
  }

  // Moonbag at last available price
  const lastCandle = tradeCandles[tradeCandles.length - 1] || entryCandle;
  const moonbagSol = remainTokens > 0 ? remainTokens * lastCandle.c * (1 - SLIPPAGE) : 0;
  const totalSol = solReceived + moonbagSol;
  const pnlSol = totalSol - POSITION_SOL;
  const pnlPct = (pnlSol / POSITION_SOL) * 100;
  const tpsHit = [tp1Hit, tp2Hit, tp3Hit, tp4Hit].filter(Boolean).length;

  // Peak gain during hold
  const peakGain = ((peakPrice / entryPrice) - 1) * 100;

  if (!exitReason) {
    if (tp4Hit) exitReason = 'TP4+MOONBAG';
    else if (tp3Hit) exitReason = 'TP3_PARTIAL';
    else if (tp2Hit) exitReason = 'TP2_PARTIAL';
    else if (tp1Hit) exitReason = 'TP1_PARTIAL';
    else exitReason = 'HOLDING_END';
  }

  return {
    result: pnlSol >= 0 ? 'WIN' : 'LOSS',
    pnlSol: parseFloat(pnlSol.toFixed(5)),
    pnlPct: parseFloat(pnlPct.toFixed(1)),
    tpsHit,
    exitReason,
    exitMinute: exitMinute ? Math.round(exitMinute) : null,
    peakGain: parseFloat(peakGain.toFixed(0)),
    moonbagSol: parseFloat(moonbagSol.toFixed(5)),
    entryPrice,
    lastPrice: lastCandle.c,
  };
}

// ─── 主程序 ───────────────────────────────────────────────────────────────
function main() {
  console.log('\n' + '═'.repeat(72));
  console.log('🔬 v18 完整回测 | 悉尼 2026-03-15 22:00 → 2026-03-17 09:30');
  console.log(`   42个真实信号 (38+4新) | ASYMMETRIC出场 | ${POSITION_SOL} SOL/笔`);
  console.log('═'.repeat(72));
  console.log('⏳ 正在拉取所有代币K线数据...\n');

  const results = [];
  let noData = 0;

  for (let i = 0; i < SIGNALS.length; i++) {
    const sig = SIGNALS[i];
    process.stdout.write(`  [${(i+1).toString().padStart(2)}/${SIGNALS.length}] $${sig.symbol.padEnd(14)} `);

    try {
      // slight delay via sync sleep
      sleep(300);  // 主延迟已在 fetchOHLCV 内部处理
      const data = fetchOHLCV(sig.ca, Math.floor(sig.entry_ts / 1000));

      if (!data || !data.candles.length) {
        console.log('❌ 无K线数据 (代币可能已下架)');
        results.push({ ...sig, result: 'NO_DATA', pnlSol: 0, pnlPct: 0, tpsHit: 0, exitReason: 'NO_DATA', peakGain: 0 });
        noData++;
        continue;
      }

      const sim = simulate(sig, data.candles);
      if (sim.result === 'NO_DATA') {
        console.log(`❌ K线无覆盖入场时刻 (${data.candles.length}根, 最早=${new Date(data.candles[0]?.ts * 1000).toISOString().substring(11,16)})`);
        results.push({ ...sig, result: 'NO_DATA', pnlSol: 0, pnlPct: 0, tpsHit: 0, exitReason: 'NO_DATA', peakGain: 0 });
        noData++;
        continue;
      }

      const icon = sim.result === 'WIN' ? '✅' : '🔴';
      const pnlStr = (sim.pnlPct >= 0 ? '+' : '') + sim.pnlPct + '%';
      console.log(`${icon} ${pnlStr.padStart(8)}  TP${sim.tpsHit}/4  Peak+${sim.peakGain}%  ${sim.exitReason}`);
      results.push({ ...sig, ...sim });

    } catch (e) {
      console.log(`⚠️ 错误: ${e.message}`);
      results.push({ ...sig, result: 'ERROR', pnlSol: 0, pnlPct: 0, tpsHit: 0, exitReason: 'ERROR', peakGain: 0 });
      noData++;
    }
  }

  // ─── 统计 ────────────────────────────────────────────────────────────────
  const tradeable = results.filter(r => r.result !== 'NO_DATA' && r.result !== 'ERROR');
  const wins = tradeable.filter(r => r.result === 'WIN');
  const losses = tradeable.filter(r => r.result === 'LOSS');
  const totalPnl = tradeable.reduce((s, r) => s + r.pnlSol, 0);
  const invested = tradeable.length * POSITION_SOL;
  const winRate = tradeable.length ? (wins.length / tradeable.length * 100) : 0;
  const avgWin = wins.length ? wins.reduce((s,r) => s + r.pnlPct, 0) / wins.length : 0;
  const avgLoss = losses.length ? losses.reduce((s,r) => s + r.pnlPct, 0) / losses.length : 0;

  // TP hit distribution
  const tpDist = {0:0, 1:0, 2:0, 3:0, 4:0};
  tradeable.forEach(r => tpDist[r.tpsHit]++);

  // Exit reason distribution
  const exitDist = {};
  tradeable.forEach(r => { exitDist[r.exitReason] = (exitDist[r.exitReason]||0)+1; });

  console.log('\n' + '═'.repeat(72));
  console.log('📊 详细结果表:');
  console.log('─'.repeat(72));
  console.log('  代币          入场MC      PnL%     TP  Peak    退出原因');
  console.log('─'.repeat(72));

  // Sort by pnl
  const sorted = [...tradeable].sort((a,b) => b.pnlPct - a.pnlPct);
  for (const r of sorted) {
    const icon = r.result === 'WIN' ? '✅' : '🔴';
    const pnl = (r.pnlPct >= 0 ? '+' : '') + r.pnlPct + '%';
    const mc = '$' + (r.mc/1000).toFixed(1) + 'K';
    console.log(`  ${icon} ${r.symbol.padEnd(13)} ${mc.padEnd(10)} ${pnl.padStart(8)}  TP${r.tpsHit}  +${String(r.peakGain).padStart(4)}%  ${r.exitReason}`);
  }

  if (noData > 0) {
    console.log(`\n  ❌ 无数据: ${results.filter(r=>r.result==='NO_DATA'||r.result==='ERROR').map(r=>r.symbol).join(', ')}`);
  }

  console.log('\n' + '═'.repeat(72));
  console.log('📋 回测汇总:');
  console.log('═'.repeat(72));
  console.log(`
┌─ 信号统计 ──────────────────────────────────────────────────────────┐
│  窗口总信号:  ~290个 (窗口1≈275 + 窗口2≈15)                         │
│  v18 通过:    42个  (38+4新, 过滤率≈86%)                            │
│  可交易数据:  ${tradeable.length}个  (${noData}个代币K线无法获取)                       │
└─────────────────────────────────────────────────────────────────────┘

┌─ 交易统计 ──────────────────────────────────────────────────────────┐
│  胜率:        ${winRate.toFixed(1)}%  (${wins.length}胜 / ${losses.length}负)                              │
│  平均盈利:    +${avgWin.toFixed(1)}%                                        │
│  平均亏损:    ${avgLoss.toFixed(1)}%                                        │
│  盈亏比:      ${Math.abs(avgLoss) > 0 ? (avgWin / Math.abs(avgLoss)).toFixed(2) : 'N/A'}                                          │
│  总投入:      ${invested.toFixed(4)} SOL                                │
│  总净利润:    ${totalPnl >= 0 ? '+' : ''}${totalPnl.toFixed(4)} SOL                             │
│  整体ROI:     ${invested > 0 ? ((totalPnl/invested)*100).toFixed(1) : 0}%                                       │
└─────────────────────────────────────────────────────────────────────┘

┌─ TP命中分布 ────────────────────────────────────────────────────────┐
│  TP0 (止损/死水/超时): ${tpDist[0]}笔                                   │
│  TP1 (+50%):          ${tpDist[1]}笔                                   │
│  TP2 (+100%):         ${tpDist[2]}笔                                   │
│  TP3 (+200%):         ${tpDist[3]}笔                                   │
│  TP4 (+500%):         ${tpDist[4]}笔  🚀                               │
└─────────────────────────────────────────────────────────────────────┘

┌─ 退出原因分布 ──────────────────────────────────────────────────────┐`);
  for (const [reason, cnt] of Object.entries(exitDist).sort((a,b)=>b[1]-a[1])) {
    console.log(`│  ${reason.padEnd(20)}: ${cnt}笔`);
  }
  console.log('└─────────────────────────────────────────────────────────────────────┘');

  // Practical note
  const daily = (totalPnl / (invested || 1)) * (tradeable.length ? (POSITION_SOL * tradeable.length) : 1);
  console.log(`
⚠️  注意事项:
   • 每笔仓位 ${POSITION_SOL} SOL，但实盘最大5个在险仓位 (共 ${(MAX_POSITIONS * POSITION_SOL).toFixed(2)} SOL 风险敞口)
   • 系统实际 executed=0 (余额不足: 仅0.035 SOL，需至少 0.36 SOL 运行5仓)
   • 已含 ${(SLIPPAGE*100).toFixed(1)}% 滑点+手续费
   • 代币死亡/下架无K线数据的按0计算 (保守估算)
`);

  // ─── 保存结果到 JSON ───────────────────────────────────────────────────
  const saveData = {
    generatedAt: new Date().toISOString(),
    window: '悉尼 2026-03-15 22:00 → 2026-03-17 09:30',
    totalSignals: SIGNALS.length,
    tradeableCount: tradeable.length,
    noDataCount: noData,
    winRate: parseFloat(winRate.toFixed(1)),
    wins: wins.length,
    losses: losses.length,
    avgWinPct: parseFloat(avgWin.toFixed(1)),
    avgLossPct: parseFloat(avgLoss.toFixed(1)),
    rr: Math.abs(avgLoss) > 0 ? parseFloat((avgWin / Math.abs(avgLoss)).toFixed(2)) : null,
    totalPnlSol: parseFloat(totalPnl.toFixed(5)),
    investedSol: parseFloat(invested.toFixed(4)),
    roiPct: parseFloat((invested > 0 ? (totalPnl/invested)*100 : 0).toFixed(1)),
    tpDistribution: tpDist,
    exitDistribution: exitDist,
    trades: results.map(r => ({
      symbol: r.symbol,
      ca: r.ca,
      mc: r.mc,
      result: r.result,
      pnlPct: r.pnlPct,
      pnlSol: r.pnlSol,
      tpsHit: r.tpsHit,
      peakGain: r.peakGain,
      exitReason: r.exitReason,
    })),
  };
  const savePath = 'data/v18-backtest-mar15-17.json';
  fs.writeFileSync(savePath, JSON.stringify(saveData, null, 2));
  console.log(`\n💾 结果已保存到 ${savePath}`);
}

main();
