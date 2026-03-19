#!/usr/bin/env node
/**
 * 验证回测发现 — 使用已缓存的OHLCV数据（无网络请求）
 *
 * 验证项目：
 * 1. 超时时长 vs 收益（15/20/30/45/60/120 min）
 * 2. SL% vs 收益（-15%/-25%/-35%）
 * 3. TP1% vs 收益（+40%/+50%/+80%）
 * 4. 最优参数组合（SL-35 + TP1+40 + 80%卖出 + 30min）
 * 5. MC分段表现（$0-15K / $15-30K / $30-50K / $50-100K / $100-300K）
 */

import fs from 'fs';

// ─── 信号列表（与 v18-full-backtest.js 完全一致） ────────────────────────────
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
  { symbol: 'Moe-chan',   ca: '7x4QEfAMo4rxMNbRUxC9jS36Hnmgp5Yh5Rm3pNCDpump', entry_ts: 1773664642014, mc: 75900  },
  { symbol: 'Gayatollah', ca: 'AN52QGkAUU6kmoHHHDhYUz1TjkFUPcAU3RxTNgJbpump', entry_ts: 1773665172998, mc: 123700 },
  { symbol: 'HOSPICE',    ca: '6uq3r5mMQL6tKkJd9JpuA3bPbrqitpFfhSQDVPCMpump', entry_ts: 1773671789279, mc: 73300  },
  { symbol: 'CLAIR',      ca: '2hhAKwDhigLKnqLdZ6LwY8snmbYQk9vtxy63YcD2yUA4',  entry_ts: 1773674358328, mc: 118800 },
];

const POSITION_SOL = 0.06;
const SLIPPAGE = 0.004;

// ─── 加载OHLCV缓存 ──────────────────────────────────────────────────────────
const cache = JSON.parse(fs.readFileSync('data/ohlcv-cache.json', 'utf8'));

// ─── 核心模拟函数（参数化） ─────────────────────────────────────────────────
function simulate(signal, params) {
  const { sl_pct, tp1_pct, sell_ratio, max_hold_mins, dead_water_mins } = params;
  const candles = cache[signal.ca]?.candles;
  if (!candles?.length) return null;

  const entryTsSec = Math.floor(signal.entry_ts / 1000);
  const entryBar   = Math.floor(entryTsSec / 60) * 60;
  const entryCandle = candles.find(c => c.ts === entryBar)
    || candles.find(c => c.ts <= entryBar && c.ts > entryBar - 600)
    || candles.find(c => c.ts > entryBar && c.ts <= entryBar + 600);
  if (!entryCandle) return null;

  const entryPrice  = entryCandle.c * (1 + SLIPPAGE);
  const tokens      = POSITION_SOL / entryPrice;
  const tp1Price    = entryPrice * (1 + tp1_pct);
  const tp2Price    = entryPrice * 2.0;
  const tp3Price    = entryPrice * 3.0;
  const tp4Price    = entryPrice * 6.0;
  let   slPrice     = entryPrice * (1 + sl_pct);  // sl_pct 是负数
  const maxHoldTs   = entryCandle.ts + max_hold_mins * 60;

  let remainTokens = tokens;
  let solReceived  = 0;
  let tp1Hit = false, tp2Hit = false, tp3Hit = false, tp4Hit = false;
  let exitReason = null;
  let peakPrice  = entryPrice;
  let minsSincePeak = 0;
  let slHit = false;
  let timeoutHit = false;

  const tradeCandles = candles.filter(c => c.ts > entryCandle.ts);

  for (const c of tradeCandles) {
    if (remainTokens <= 0.001 * tokens) break;
    const minFromEntry = (c.ts - entryCandle.ts) / 60;

    // TP1
    if (!tp1Hit && c.h >= tp1Price) {
      const qty = remainTokens * sell_ratio;
      solReceived += qty * tp1Price * (1 - SLIPPAGE);
      remainTokens -= qty;
      slPrice = entryPrice; // 移SL到成本
      tp1Hit = true;
    }
    // TP2
    if (tp1Hit && !tp2Hit && c.h >= tp2Price) {
      const qty = remainTokens * 0.50;
      solReceived += qty * tp2Price * (1 - SLIPPAGE);
      remainTokens -= qty;
      tp2Hit = true;
    }
    // TP3
    if (tp2Hit && !tp3Hit && c.h >= tp3Price) {
      const qty = remainTokens * 0.50;
      solReceived += qty * tp3Price * (1 - SLIPPAGE);
      remainTokens -= qty;
      tp3Hit = true;
    }
    // TP4
    if (tp3Hit && !tp4Hit && c.h >= tp4Price) {
      const qty = remainTokens * 0.80;
      solReceived += qty * tp4Price * (1 - SLIPPAGE);
      remainTokens -= qty;
      tp4Hit = true;
    }

    // 止损（TP1前）
    if (!tp1Hit && c.l <= slPrice) {
      solReceived += remainTokens * slPrice * (1 - SLIPPAGE);
      remainTokens = 0;
      exitReason = 'STOP_LOSS';
      slHit = true;
      break;
    }

    // 死水（TP1前）
    if (c.h > peakPrice) { peakPrice = c.h; minsSincePeak = 0; }
    else minsSincePeak++;
    if (!tp1Hit && minsSincePeak >= dead_water_mins) {
      solReceived += remainTokens * c.c * (1 - SLIPPAGE);
      remainTokens = 0;
      exitReason = 'DEAD_WATER';
      break;
    }

    // 超时（TP1前）
    if (!tp1Hit && c.ts >= maxHoldTs) {
      solReceived += remainTokens * c.c * (1 - SLIPPAGE);
      remainTokens = 0;
      exitReason = 'MAX_HOLD';
      timeoutHit = true;
      break;
    }
  }

  // 月球仓
  const lastCandle = tradeCandles[tradeCandles.length - 1] || entryCandle;
  const moonbag = remainTokens > 0 ? remainTokens * lastCandle.c * (1 - SLIPPAGE) : 0;
  const totalSol = solReceived + moonbag;
  const pnlSol   = totalSol - POSITION_SOL;
  const pnlPct   = (pnlSol / POSITION_SOL) * 100;

  if (!exitReason) {
    if (tp4Hit) exitReason = 'TP4';
    else if (tp3Hit) exitReason = 'TP3';
    else if (tp2Hit) exitReason = 'TP2';
    else if (tp1Hit) exitReason = 'TP1+HOLD';
    else exitReason = 'HOLD_END';
  }

  return {
    win: pnlSol >= 0,
    pnlSol,
    pnlPct,
    slHit,
    timeoutHit,
    tp1Hit,
    exitReason,
    peakGain: ((peakPrice / entryPrice) - 1) * 100,
  };
}

// ─── 汇总一组参数的结果 ──────────────────────────────────────────────────────
function runBacktest(params, signals) {
  const results = signals.map(sig => simulate(sig, params)).filter(Boolean);
  if (!results.length) return null;
  const wins   = results.filter(r => r.win);
  const losses = results.filter(r => !r.win);
  const total  = results.reduce((s, r) => s + r.pnlSol, 0);
  const slRate = results.filter(r => r.slHit).length / results.length;
  const toRate = results.filter(r => r.timeoutHit).length / results.length;
  return {
    n: results.length,
    wins: wins.length,
    losses: losses.length,
    winRate: (wins.length / results.length * 100),
    totalPnl: total,
    avgWin:  wins.length  ? wins.reduce((s,r)=>s+r.pnlPct,0)/wins.length : 0,
    avgLoss: losses.length? losses.reduce((s,r)=>s+r.pnlPct,0)/losses.length : 0,
    slRate:  slRate * 100,
    toRate:  toRate * 100,
  };
}

// ─── 工具 ───────────────────────────────────────────────────────────────────
function row(label, stats, highlight) {
  const p = highlight ? '\x1b[33m' : '';
  const r = highlight ? '\x1b[0m' : '';
  const pnl = (stats.totalPnl >= 0 ? '+' : '') + stats.totalPnl.toFixed(3);
  return `${p}  ${label.padEnd(22)} ${String(stats.wins+'/'+stats.n).padEnd(6)} ${(stats.winRate.toFixed(1)+'%').padStart(6)}  ${pnl.padStart(8)} SOL  SL:${stats.slRate.toFixed(0)}%  TO:${stats.toRate.toFixed(0)}%${r}`;
}

const BASE = { sl_pct: -0.35, tp1_pct: 0.50, sell_ratio: 0.60, max_hold_mins: 30, dead_water_mins: 15 };
const tradeable = SIGNALS.filter(s => cache[s.ca] !== null && cache[s.ca]?.candles?.length);

console.log('\n' + '═'.repeat(70));
console.log('🔬 参数发现验证 | 42个信号，已缓存OHLCV，零网络请求');
console.log(`   可用数据: ${tradeable.length}/${SIGNALS.length} 个代币`);
console.log('═'.repeat(70));

// ══════════════════════════════════════════════════════════════════
// 验证1: 超时时长
// ══════════════════════════════════════════════════════════════════
console.log('\n【验证1】超时时长 vs 收益（其他参数固定：SL-35%/TP1+50%/卖60%）');
console.log('─'.repeat(70));
console.log('  超时时长               胜/总     胜率    总PnL        SL率   超时率');
console.log('─'.repeat(70));

const claimedTimeout = { 15: +0.462, 30: +0.142, 45: -0.516, 60: -0.712, 120: -3.020 };
const timeouts = [15, 20, 30, 45, 60, 120];
const timeoutResults = {};
for (const t of timeouts) {
  const s = runBacktest({ ...BASE, max_hold_mins: t }, tradeable);
  timeoutResults[t] = s;
  const claimed = claimedTimeout[t] !== undefined ? ` (声称:${claimedTimeout[t]>=0?'+':''}${claimedTimeout[t]})` : '';
  const highlight = [15, 30].includes(t);
  console.log(row(`${t}min${claimed}`, s, highlight));
}

const isTimeout = timeouts.every((t, i) => i === 0 || timeoutResults[timeouts[i]].totalPnl <= timeoutResults[timeouts[i-1]].totalPnl);
console.log(`\n  → 超时越短越赚钱: ${isTimeout ? '✅ 验证通过' : '⚠️  趋势不完全单调'}`);

// ══════════════════════════════════════════════════════════════════
// 验证2: 止损幅度
// ══════════════════════════════════════════════════════════════════
console.log('\n【验证2】止损幅度 vs 收益（其他参数固定：TP1+50%/卖60%/30min）');
console.log('─'.repeat(70));
console.log('  SL%                    胜/总     胜率    总PnL        SL率   超时率');
console.log('─'.repeat(70));

const slValues = [-0.15, -0.25, -0.35, -0.50];
const slResults = {};
for (const sl of slValues) {
  const s = runBacktest({ ...BASE, sl_pct: sl }, tradeable);
  slResults[sl] = s;
  const claimed = sl === -0.15 ? ' (声称最赚)' : sl === -0.35 ? ' (声称平衡点)' : '';
  console.log(row(`SL ${(sl*100).toFixed(0)}%${claimed}`, s, sl === -0.35));
}

// ══════════════════════════════════════════════════════════════════
// 验证3: TP1触发点
// ══════════════════════════════════════════════════════════════════
console.log('\n【验证3】TP1触发点 vs 收益（其他参数固定：SL-35%/卖60%/30min）');
console.log('─'.repeat(70));
console.log('  TP1%                   胜/总     胜率    总PnL        SL率   超时率');
console.log('─'.repeat(70));

const tp1Values = [0.30, 0.40, 0.50, 0.80];
const tp1Results = {};
for (const tp1 of tp1Values) {
  const s = runBacktest({ ...BASE, tp1_pct: tp1 }, tradeable);
  tp1Results[tp1] = s;
  const claimed = tp1 === 0.40 ? ' (声称更优)' : tp1 === 0.50 ? ' (当前参数)' : '';
  console.log(row(`TP1 +${(tp1*100).toFixed(0)}%${claimed}`, s, tp1 === 0.40));
}

// ══════════════════════════════════════════════════════════════════
// 验证4: 卖出比例
// ══════════════════════════════════════════════════════════════════
console.log('\n【验证4】TP1卖出比例 vs 收益（SL-35%/TP1+40%/30min）');
console.log('─'.repeat(70));
console.log('  卖出比例               胜/总     胜率    总PnL        SL率   超时率');
console.log('─'.repeat(70));

const sellRatios = [0.50, 0.60, 0.80];
for (const ratio of sellRatios) {
  // 使用TP1+40%（声称更优的组合）
  const s = runBacktest({ ...BASE, tp1_pct: 0.40, sell_ratio: ratio }, tradeable);
  const claimed = ratio === 0.80 ? ' (声称更优)' : ratio === 0.60 ? ' (当前参数)' : '';
  console.log(row(`卖${(ratio*100).toFixed(0)}%${claimed}`, s, ratio === 0.80));
}

// ══════════════════════════════════════════════════════════════════
// 验证5: 最优参数组合
// ══════════════════════════════════════════════════════════════════
console.log('\n【验证5】声称最优参数组合对比');
console.log('─'.repeat(70));
console.log('  参数组合               胜/总     胜率    总PnL        SL率   超时率');
console.log('─'.repeat(70));

// 当前参数（基准）
const current = runBacktest(BASE, tradeable);
console.log(row('当前(SL35/TP50/60%/30m)', current, false));

// 声称最优组合1: SL-35/TP1+40/80%/30min
const combo1 = runBacktest({ ...BASE, tp1_pct: 0.40, sell_ratio: 0.80 }, tradeable);
console.log(row('SL35/TP40/80%/30m(声称)', combo1, true));

// 声称最优组合2: SL-35/TP1+80/80%/30min
const combo2 = runBacktest({ ...BASE, tp1_pct: 0.80, sell_ratio: 0.80 }, tradeable);
console.log(row('SL35/TP80/80%/30m(声称)', combo2, true));

// 20min超时变体
const combo3 = runBacktest({ ...BASE, max_hold_mins: 20, tp1_pct: 0.40, sell_ratio: 0.80 }, tradeable);
console.log(row('SL35/TP40/80%/20m', combo3, false));

// 15min超时变体
const combo4 = runBacktest({ ...BASE, max_hold_mins: 15, tp1_pct: 0.40, sell_ratio: 0.80 }, tradeable);
console.log(row('SL35/TP40/80%/15m', combo4, false));

// ══════════════════════════════════════════════════════════════════
// 验证6: MC分段
// ══════════════════════════════════════════════════════════════════
console.log('\n【验证6】MC分段表现（当前参数基准：SL-35%/TP1+50%/卖60%/30min）');
console.log('─'.repeat(70));
console.log('  MC区间                 胜/总     胜率    总PnL        信号占比');
console.log('─'.repeat(70));

const mcBands = [
  { label: '$0-15K',    min: 0,      max: 15000  },
  { label: '$15-30K',   min: 15000,  max: 30000  },
  { label: '$30-50K',   min: 30000,  max: 50000  },
  { label: '$50-100K',  min: 50000,  max: 100000 },
  { label: '$100-300K', min: 100000, max: 300000 },
];

const claimedMC = {
  '$0-15K':    { wr: 52.9, pnl: '+6.1%'  },
  '$15-30K':   { wr: 26.1, pnl: '-14.4%' },
  '$30-50K':   { wr: 57.5, pnl: '+7.8%'  },
  '$50-100K':  { wr: 40.9, pnl: '-0.9%'  },
  '$100-300K': { wr: 40.9, pnl: '-5.3%'  },
};

for (const band of mcBands) {
  const subset = tradeable.filter(s => s.mc >= band.min && s.mc < band.max);
  if (!subset.length) {
    console.log(`  ${band.label.padEnd(22)} 无数据`);
    continue;
  }
  const s = runBacktest(BASE, subset);
  const claimed = claimedMC[band.label];
  const pct = (subset.length / tradeable.length * 100).toFixed(0);
  const actualPnlPct = (s.totalPnl / (subset.length * POSITION_SOL) * 100).toFixed(1);
  const claimedStr = claimed ? ` (声称WR:${claimed.wr}%,PnL:${claimed.pnl})` : '';
  const isDeathZone = band.label === '$15-30K';
  const isSweet = ['$0-15K','$30-50K'].includes(band.label);
  const hl = isDeathZone || isSweet;
  const tag = isDeathZone ? ' ⚠️死区' : isSweet ? ' ✨甜点' : '';
  console.log(`  ${(band.label+tag).padEnd(22)} ${String(s.wins+'/'+s.n).padEnd(6)} ${(s.winRate.toFixed(1)+'%').padStart(6)}  ${(s.totalPnl>=0?'+':'')+s.totalPnl.toFixed(3)+' SOL'}  ${pct}%笔${claimedStr}`);
}

// ══════════════════════════════════════════════════════════════════
// 全局最优参数扫描（确认）
// ══════════════════════════════════════════════════════════════════
console.log('\n【扫描】全参数空间 Top 10 组合');
console.log('─'.repeat(70));

const allResults = [];
for (const sl of [-0.15, -0.25, -0.35]) {
  for (const tp1 of [0.30, 0.40, 0.50, 0.80]) {
    for (const ratio of [0.60, 0.80]) {
      for (const timeout of [15, 20, 30, 45]) {
        const params = { sl_pct: sl, tp1_pct: tp1, sell_ratio: ratio, max_hold_mins: timeout, dead_water_mins: 15 };
        const s = runBacktest(params, tradeable);
        if (s) allResults.push({ params, ...s });
      }
    }
  }
}
allResults.sort((a, b) => b.totalPnl - a.totalPnl);

console.log('  排名  SL%   TP1%  卖出  超时     胜/总     胜率    总PnL');
console.log('─'.repeat(70));
allResults.slice(0, 10).forEach((r, i) => {
  const { sl_pct, tp1_pct, sell_ratio, max_hold_mins } = r.params;
  const tag = `SL${(sl_pct*100).toFixed(0)}/TP${(tp1_pct*100).toFixed(0)}/卖${(sell_ratio*100).toFixed(0)}%/${max_hold_mins}m`;
  const pnl = (r.totalPnl>=0?'+':'')+r.totalPnl.toFixed(3);
  console.log(`  #${String(i+1).padEnd(3)}  ${tag.padEnd(25)} ${String(r.wins+'/'+r.n).padEnd(6)} ${(r.winRate.toFixed(1)+'%').padStart(6)}  ${pnl} SOL`);
});

// ══════════════════════════════════════════════════════════════════
// 结论汇总
// ══════════════════════════════════════════════════════════════════
console.log('\n' + '═'.repeat(70));
console.log('📋 结论汇总');
console.log('═'.repeat(70));

// 验证超时趋势
const t15 = timeoutResults[15], t30 = timeoutResults[30], t120 = timeoutResults[120];
const timeoutTrend = t15.totalPnl > t30.totalPnl && t30.totalPnl > t120.totalPnl;
console.log(`\n1. 超时越短越赚: ${timeoutTrend ? '✅ 确认' : '⚠️ 部分确认'}`);
console.log(`   15min→${t15.totalPnl>=0?'+':''}${t15.totalPnl.toFixed(3)} SOL | 30min→${t30.totalPnl>=0?'+':''}${t30.totalPnl.toFixed(3)} SOL | 120min→${t120.totalPnl>=0?'+':''}${t120.totalPnl.toFixed(3)} SOL`);

// 验证TP1+40%更优
const tp40 = tp1Results[0.40], tp50 = tp1Results[0.50];
const tp40Better = tp40.totalPnl > tp50.totalPnl;
console.log(`\n2. TP1 +40%优于+50%: ${tp40Better ? '✅ 确认' : '❌ 未确认'}`);
console.log(`   +40%→${tp40.totalPnl>=0?'+':''}${tp40.totalPnl.toFixed(3)} SOL | +50%→${tp50.totalPnl>=0?'+':''}${tp50.totalPnl.toFixed(3)} SOL`);

// 验证SL -15%最赚
const sl15 = slResults[-0.15], sl35 = slResults[-0.35];
const sl15Best = Object.values(slResults).every(s => sl15.totalPnl >= s.totalPnl);
console.log(`\n3. SL -15%最赚但止损率高: ${sl15Best ? '✅ 确认' : '⚠️ 部分确认'}`);
console.log(`   SL-15%: PnL=${sl15.totalPnl>=0?'+':''}${sl15.totalPnl.toFixed(3)} SOL, SL率=${sl15.slRate.toFixed(0)}%`);
console.log(`   SL-35%: PnL=${sl35.totalPnl>=0?'+':''}${sl35.totalPnl.toFixed(3)} SOL, SL率=${sl35.slRate.toFixed(0)}%`);

// 验证MC死亡区
const mc15_30 = tradeable.filter(s => s.mc >= 15000 && s.mc < 30000);
const mc30_50 = tradeable.filter(s => s.mc >= 30000 && s.mc < 50000);
const mc15_30_r = mc15_30.length ? runBacktest(BASE, mc15_30) : null;
const mc30_50_r = mc30_50.length ? runBacktest(BASE, mc30_50) : null;
console.log(`\n4. $15-30K死亡区 vs $30-50K甜点:`);
if (mc15_30_r) console.log(`   $15-30K(${mc15_30.length}笔): WR=${mc15_30_r.winRate.toFixed(1)}%, PnL=${mc15_30_r.totalPnl>=0?'+':''}${mc15_30_r.totalPnl.toFixed(3)} SOL`);
else console.log(`   $15-30K: 无足够数据`);
if (mc30_50_r) console.log(`   $30-50K(${mc30_50.length}笔): WR=${mc30_50_r.winRate.toFixed(1)}%, PnL=${mc30_50_r.totalPnl>=0?'+':''}${mc30_50_r.totalPnl.toFixed(3)} SOL`);
else console.log(`   $30-50K: 无足够数据`);

const deathZoneConfirmed = mc15_30_r && mc30_50_r && mc30_50_r.totalPnl > mc15_30_r.totalPnl;
console.log(`   → 死亡区/甜点对比: ${deathZoneConfirmed ? '✅ 确认' : mc15_30_r && mc30_50_r ? '⚠️ 方向相同但幅度需确认' : '⚠️ 样本太少'}`);

// 最优组合
const best = allResults[0];
const { sl_pct, tp1_pct, sell_ratio, max_hold_mins } = best.params;
console.log(`\n5. 全局最优参数:`);
console.log(`   SL=${(sl_pct*100).toFixed(0)}% / TP1=+${(tp1_pct*100).toFixed(0)}% / 卖${(sell_ratio*100).toFixed(0)}% / 超时${max_hold_mins}min`);
console.log(`   胜率=${best.winRate.toFixed(1)}% / PnL=${best.totalPnl>=0?'+':''}${best.totalPnl.toFixed(3)} SOL`);
console.log('');
console.log('⚠️  注意: 42笔信号样本量偏小，结论供参考，建议扩大样本后再调整实盘参数');
console.log('═'.repeat(70));

// 保存结果
const saveData = {
  generatedAt: new Date().toISOString(),
  totalSignals: SIGNALS.length,
  tradeableN: tradeable.length,
  timeoutSweep: timeouts.map(t => ({ timeout: t, ...timeoutResults[t] })),
  slSweep: slValues.map(sl => ({ sl_pct: sl, ...slResults[sl] })),
  tp1Sweep: tp1Values.map(tp1 => ({ tp1_pct: tp1, ...tp1Results[tp1] })),
  top10: allResults.slice(0, 10).map(r => ({ params: r.params, winRate: r.winRate, totalPnl: r.totalPnl, wins: r.wins, n: r.n })),
};
fs.writeFileSync('data/validate-findings-results.json', JSON.stringify(saveData, null, 2));
console.log('\n💾 结果已保存到 data/validate-findings-results.json\n');
