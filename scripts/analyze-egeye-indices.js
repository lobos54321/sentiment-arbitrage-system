/**
 * Egeye AI 指数分析脚本
 *
 * 目标：找出哪些指数能有效区分银狗/金狗 vs 垃圾，给 v21 过滤器提供数据支撑
 *
 * 流程：
 *   1. 从 Egeye 频道拉取历史 🔥New Trending 信号
 *   2. 解析全部 8 个 AI 指数 + 基础字段 (MC, holders, top10, vol)
 *   3. DexScreener 查当前 MC，算涨幅倍数
 *   4. 按结果分4档：金狗(≥5x) / 银狗(2-5x) / 铜狗(1-2x) / 垃圾(<1x 或已死)
 *   5. 对每个指数做分布分析 + 找最优过滤阈值
 *   6. 输出推荐阈值表
 *
 * 运行：
 *   node scripts/analyze-egeye-indices.js
 *   node scripts/analyze-egeye-indices.js --limit 500   (拉更多历史)
 *   node scripts/analyze-egeye-indices.js --dry          (只解析不查价格，快速验证)
 */

import { TelegramClient } from 'telegram';
import { StringSession } from 'telegram/sessions/index.js';
import dotenv from 'dotenv';
import axios from 'axios';

dotenv.config();

// ─── 配置 ────────────────────────────────────────────────────────────────────
const TARGET_CHANNEL_ID = parseInt(process.env.PREMIUM_CHANNEL_ID || '3636518327');
const args = process.argv.slice(2);
const MESSAGE_LIMIT = parseInt(args[args.indexOf('--limit') + 1] || '400');
const DRY_RUN = args.includes('--dry');
const SLEEP_MS = 600;  // DexScreener 限速

// 结果分档定义
const TIERS = {
  GOLD:   { label: '🥇 金狗',  min: 5.0,  color: '\x1b[33m' },
  SILVER: { label: '🥈 银狗',  min: 2.0,  color: '\x1b[37m' },
  COPPER: { label: '🥉 铜狗',  min: 1.0,  color: '\x1b[90m' },
  TRASH:  { label: '🗑️  垃圾',  min: 0,    color: '\x1b[31m' },
};
const RESET = '\x1b[0m';

// 要分析的全部字段
const INDEX_KEYS = [
  'super_index', 'ai_index', 'trade_index',
  'security_index', 'address_index',
  'viral_index', 'media_index', 'sentiment_index',
];
const BASIC_KEYS = ['market_cap', 'holders', 'top10_pct', 'volume_24h'];

// ─── 解析函数 ─────────────────────────────────────────────────────────────────

const SOL_ADDR_RE = /\b([1-9A-HJ-NP-Za-km-z]{32,44})\b/g;
const FALSE_POS_RE = /^(https?|discord|telegram|dexscreener|twitter|solscan)/i;

function parseNumber(str, suffix) {
  const n = parseFloat((str || '').replace(/,/g, ''));
  if (isNaN(n)) return 0;
  const s = (suffix || '').toUpperCase();
  return s === 'K' ? n * 1e3 : s === 'M' ? n * 1e6 : s === 'B' ? n * 1e9 : n;
}

function extractSolCA(text) {
  SOL_ADDR_RE.lastIndex = 0;
  let m;
  while ((m = SOL_ADDR_RE.exec(text)) !== null) {
    const a = m[1];
    if (a.length < 32 || FALSE_POS_RE.test(a) || a.includes('.')) continue;
    return a;
  }
  return null;
}

/**
 * 从信号文本解析所有 8 个 Egeye AI 指数
 * 返回 { super_index: number|null, ai_index: number|null, ... }
 */
function parseIndices(text) {
  const defs = [
    ['super_index',     'Super Index'],
    ['ai_index',        'AI Index'],
    ['trade_index',     'Trade Index'],
    ['security_index',  'Security Index'],
    ['address_index',   'Address Index'],
    ['sentiment_index', 'Sentiment Index'],
    ['viral_index',     'Viral Index'],
    ['media_index',     'Media Index'],
  ];

  const result = {};
  for (const [key, label] of defs) {
    const esc = label.replace(/\s+/g, '\\s+');

    // 格式1: Label： 128🔮
    const reSingle = new RegExp(esc + '[：:]\\s*(\\d+)\\s*🔮', 'i');
    const mSingle = text.match(reSingle);
    if (mSingle) { result[key] = parseInt(mSingle[1]); continue; }

    // 格式2 (ATH): Label：(signal)116🔮 --> (current)124🔮
    const reDelta = new RegExp(esc + '[：:]\\s*[\\(（]signal[\\)）]\\s*x?(\\d+).*?[\\(（]current[\\)）]\\s*x?(\\d+)', 'i');
    const mDelta = text.match(reDelta);
    if (mDelta) { result[key] = parseInt(mDelta[1]); continue; } // 用 signal 时刻值

    result[key] = null; // 该条消息没有这个字段
  }
  return result;
}

/** 完整解析一条 🔥New Trending 消息 */
function parseSignal(text, msgDate) {
  if (!text || !text.includes('🔥') || text.includes('📈New ATH')) return null;

  const ca = extractSolCA(text);
  if (!ca) return null;

  const sym  = (text.match(/SYMBOL[：:]\s*\$(\S+)/i) || [])[1] || null;
  const mcM  = text.match(/MC[：:]\s*\$?([\d,.]+)\s*([KMBkmb])?/i);
  const hM   = text.match(/Holders[：:]\s*([\d,]+)/i);
  const volM = text.match(/Vol24H[：:]\s*\$?([\d,.]+)\s*([KMBkmb])?/i);
  const t10M = text.match(/Top10[：:]\s*([\d.]+)%/i);
  const ageM = text.match(/Age[：:]\s*(\S+)/i);

  const indices = parseIndices(text);
  const indexCount = Object.values(indices).filter(v => v !== null).length;

  return {
    token_ca:   ca,
    symbol:     sym,
    market_cap: mcM  ? parseNumber(mcM[1],  mcM[2])  : 0,
    holders:    hM   ? parseInt(hM[1].replace(/,/g, '')) : 0,
    volume_24h: volM ? parseNumber(volM[1], volM[2]) : 0,
    top10_pct:  t10M ? parseFloat(t10M[1]) : 0,
    age:        ageM ? ageM[1] : '',
    freeze_ok:  /freezeAuthority[：:]\s*✅/i.test(text),
    mint_ok:    /mintAuthority\w*[：:]\s*✅/i.test(text),
    indices,
    index_count: indexCount,
    msg_date:   msgDate,
    raw:        text,
  };
}

// ─── DexScreener 查询 ─────────────────────────────────────────────────────────

async function getDexMC(ca) {
  try {
    const r = await axios.get(
      `https://api.dexscreener.com/latest/dex/tokens/${ca}`,
      { timeout: 8000, headers: { 'User-Agent': 'Mozilla/5.0' } }
    );
    const pairs = r.data?.pairs;
    if (!pairs || pairs.length === 0) return null;
    const best = pairs.sort((a, b) => (b.liquidity?.usd || 0) - (a.liquidity?.usd || 0))[0];
    return {
      mc:  best.marketCap || best.fdv || 0,
      liq: best.liquidity?.usd || 0,
    };
  } catch {
    return null;
  }
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ─── 统计工具 ─────────────────────────────────────────────────────────────────

function percentile(arr, p) {
  if (arr.length === 0) return null;
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = (p / 100) * (sorted.length - 1);
  const lo = Math.floor(idx), hi = Math.ceil(idx);
  return +(sorted[lo] + (sorted[hi] - sorted[lo]) * (idx - lo)).toFixed(1);
}

function mean(arr) {
  if (arr.length === 0) return null;
  return +(arr.reduce((s, v) => s + v, 0) / arr.length).toFixed(1);
}

function classifyOutcome(multiple) {
  if (multiple === null) return 'UNKNOWN';
  if (multiple >= 5.0) return 'GOLD';
  if (multiple >= 2.0) return 'SILVER';
  if (multiple >= 1.0) return 'COPPER';
  return 'TRASH';
}

/**
 * 对单个指数，扫描所有可能的阈值 (低于阈值则拒绝)
 * 找出使 "保留银狗+金狗比例" 最高，同时 "拒绝铜狗+垃圾比例" 也较高的最优阈值
 *
 * 评分函数：F = (silver+gold保留率) * 0.6 + (copper+trash过滤率) * 0.4
 *   — 我们更在乎不漏掉好信号，所以好信号保留权重 > 垃圾过滤权重
 */
function findBestThreshold(records, indexKey) {
  const valid = records.filter(r => r.indices[indexKey] !== null && r.outcome !== 'UNKNOWN');
  if (valid.length < 10) return null;

  const values = valid.map(r => r.indices[indexKey]);
  const minV = Math.min(...values);
  const maxV = Math.max(...values);

  // 扫描候选阈值 (步长1)
  let best = null;
  for (let thresh = minV; thresh <= maxV; thresh++) {
    const passed  = valid.filter(r => r.indices[indexKey] >= thresh);
    const blocked = valid.filter(r => r.indices[indexKey] <  thresh);

    if (passed.length === 0) continue;

    const goodPassed  = passed.filter(r => r.outcome === 'GOLD' || r.outcome === 'SILVER').length;
    const badPassed   = passed.filter(r => r.outcome === 'COPPER' || r.outcome === 'TRASH').length;
    const badBlocked  = blocked.filter(r => r.outcome === 'COPPER' || r.outcome === 'TRASH').length;
    const goodBlocked = blocked.filter(r => r.outcome === 'GOLD'   || r.outcome === 'SILVER').length;

    const totalGood = valid.filter(r => r.outcome === 'GOLD' || r.outcome === 'SILVER').length;
    const totalBad  = valid.filter(r => r.outcome === 'COPPER' || r.outcome === 'TRASH').length;

    const goodRetentionRate = totalGood > 0 ? goodPassed / totalGood : 1;
    const badFilterRate     = totalBad  > 0 ? badBlocked / totalBad  : 0;

    // 误杀率超过 30% 就不考虑
    if (goodBlocked / (totalGood || 1) > 0.30) continue;

    const score = goodRetentionRate * 0.6 + badFilterRate * 0.4;

    if (!best || score > best.score) {
      best = {
        threshold: thresh,
        score,
        passed: passed.length,
        blocked: blocked.length,
        goodRetentionRate: +(goodRetentionRate * 100).toFixed(1),
        badFilterRate:     +(badFilterRate     * 100).toFixed(1),
        goodBlocked,
        badBlocked,
      };
    }
  }
  return best;
}

// ─── 主逻辑 ───────────────────────────────────────────────────────────────────

async function main() {
  console.log('═'.repeat(70));
  console.log('🔬 Egeye AI 指数分析 — 寻找垃圾信号过滤最优阈值');
  console.log(`   拉取消息数: ${MESSAGE_LIMIT}${DRY_RUN ? '  [DRY RUN，不查 DexScreener]' : ''}`);
  console.log('═'.repeat(70));

  // ── 1. 连接 Telegram ────────────────────────────────────────────────────────
  const apiId      = parseInt(process.env.TELEGRAM_API_ID  || '0');
  const apiHash    = process.env.TELEGRAM_API_HASH  || '';
  const sessionStr = process.env.TELEGRAM_SESSION   || '';

  if (!apiId || !apiHash || !sessionStr) {
    console.error('❌ 缺少 Telegram 凭证 (TELEGRAM_API_ID / TELEGRAM_API_HASH / TELEGRAM_SESSION)');
    process.exit(1);
  }

  const client = new TelegramClient(
    new StringSession(sessionStr), apiId, apiHash, { connectionRetries: 5 }
  );
  await client.connect();
  console.log('✅ Telegram 已连接\n');

  // 找频道
  const dialogs = await client.getDialogs({ limit: 200 });
  let channelEntity = null;
  for (const d of dialogs) {
    const eid = d.entity?.id?.value !== undefined
      ? Number(d.entity.id.value) : Number(d.entity?.id);
    if (eid === TARGET_CHANNEL_ID) { channelEntity = d.entity; break; }
  }
  if (!channelEntity) {
    console.error(`❌ 找不到频道 ID=${TARGET_CHANNEL_ID}`);
    await client.disconnect();
    process.exit(1);
  }
  console.log(`📢 频道: ${channelEntity.title}\n`);

  // ── 2. 拉取并解析历史消息 ─────────────────────────────────────────────────
  const messages = await client.getMessages(channelEntity, { limit: MESSAGE_LIMIT });
  await client.disconnect();
  console.log(`📨 共拉取 ${messages.length} 条消息\n`);

  const signals = [];
  const seenCAs = new Set();
  for (const msg of messages) {
    if (!msg.message) continue;
    const s = parseSignal(msg.message, new Date(msg.date * 1000));
    if (!s || seenCAs.has(s.token_ca)) continue;
    seenCAs.add(s.token_ca);
    signals.push(s);
  }
  console.log(`🔥 解析出 ${signals.length} 个去重 New Trending 信号`);

  // 统计有指数的信号数量
  const withIndices = signals.filter(s => s.index_count >= 3);
  console.log(`📊 其中含 ≥3 个 AI 指数的信号: ${withIndices.length} 个\n`);

  if (signals.length === 0) process.exit(0);

  // ── 3. DexScreener 查当前 MC，算涨幅倍数 ────────────────────────────────────
  const records = [];
  if (DRY_RUN) {
    // Dry run：只分析指数分布，不查价格
    for (const s of signals) {
      records.push({ ...s, current_mc: null, multiple: null, outcome: 'UNKNOWN' });
    }
    console.log('ℹ️  DRY RUN：跳过 DexScreener 查询，只分析指数分布\n');
  } else {
    console.log(`🌐 开始查询 DexScreener（${signals.length} 个代币，间隔 ${SLEEP_MS}ms）...\n`);
    for (let i = 0; i < signals.length; i++) {
      const s = signals[i];
      process.stdout.write(`  [${i + 1}/${signals.length}] $${(s.symbol || s.token_ca.slice(0, 8)).padEnd(12)} `);

      const dex = await getDexMC(s.token_ca);
      let multiple = null;
      let outcome  = 'UNKNOWN';

      if (dex && dex.mc > 0 && s.market_cap > 0) {
        multiple = dex.mc / s.market_cap;
        outcome  = classifyOutcome(multiple);
        const tierInfo = TIERS[outcome];
        process.stdout.write(
          `入场MC=$${(s.market_cap/1000).toFixed(1)}K → 当前=$${(dex.mc/1000).toFixed(1)}K ` +
          `× ${multiple.toFixed(2)} ${tierInfo.color}${tierInfo.label}${RESET}\n`
        );
      } else if (!dex || dex.mc === 0) {
        outcome = 'TRASH'; // 代币已死/下架，视为垃圾
        multiple = 0;
        process.stdout.write(`已下架/无数据 → ${TIERS.TRASH.color}${TIERS.TRASH.label}${RESET}\n`);
      } else {
        process.stdout.write(`入场MC缺失，跳过\n`);
      }

      records.push({ ...s, current_mc: dex?.mc || null, multiple, outcome });
      await sleep(SLEEP_MS);
    }
  }

  // ── 4. 汇总结果分布 ──────────────────────────────────────────────────────────
  const knownRecords = records.filter(r => r.outcome !== 'UNKNOWN');
  const byTier = {
    GOLD:   knownRecords.filter(r => r.outcome === 'GOLD'),
    SILVER: knownRecords.filter(r => r.outcome === 'SILVER'),
    COPPER: knownRecords.filter(r => r.outcome === 'COPPER'),
    TRASH:  knownRecords.filter(r => r.outcome === 'TRASH'),
  };

  console.log('\n' + '═'.repeat(70));
  console.log('📊 整体结果分布');
  console.log('═'.repeat(70));
  const total = knownRecords.length;
  for (const [k, tier] of Object.entries(TIERS)) {
    const n = byTier[k].length;
    const pct = total > 0 ? (n / total * 100).toFixed(1) : '0';
    const bar = '█'.repeat(Math.round(n / total * 30 || 0));
    console.log(`${tier.color}${tier.label.padEnd(12)}${RESET} ${String(n).padStart(4)} 个  ${pct.padStart(5)}%  ${bar}`);
  }
  console.log(`${'合计'.padEnd(12)} ${String(total).padStart(4)} 个`);

  const silverGoldPct = total > 0
    ? ((byTier.GOLD.length + byTier.SILVER.length) / total * 100).toFixed(1)
    : '0';
  console.log(`\n当前银狗+金狗比例: ${silverGoldPct}%  (目标: 60%+)`);

  // ── 5. 每个指数的分布分析 ─────────────────────────────────────────────────
  console.log('\n' + '═'.repeat(70));
  console.log('📈 各 AI 指数在不同结果档的分布');
  console.log('═'.repeat(70));

  const indexStats = {};
  for (const key of INDEX_KEYS) {
    const coverage = knownRecords.filter(r => r.indices[key] !== null).length;
    if (coverage < 5) continue; // 样本不足，跳过

    const distByTier = {};
    for (const tier of ['GOLD', 'SILVER', 'COPPER', 'TRASH']) {
      const vals = byTier[tier]
        .map(r => r.indices[key])
        .filter(v => v !== null);
      distByTier[tier] = {
        n:    vals.length,
        mean: mean(vals),
        p25:  percentile(vals, 25),
        p50:  percentile(vals, 50),
        p75:  percentile(vals, 75),
      };
    }

    indexStats[key] = { coverage, distByTier };

    console.log(`\n▸ ${key.replace(/_/g, ' ').toUpperCase()}  (有数据: ${coverage}/${total})`);
    console.log(`  ${'档位'.padEnd(8)} ${'n'.padStart(4)} ${'均值'.padStart(7)} ${'P25'.padStart(7)} ${'中位'.padStart(7)} ${'P75'.padStart(7)}`);
    console.log(`  ${'─'.repeat(50)}`);
    for (const [tier, d] of Object.entries(distByTier)) {
      const info = TIERS[tier];
      if (d.n === 0) continue;
      console.log(
        `  ${info.color}${info.label.padEnd(8)}${RESET}` +
        ` ${String(d.n).padStart(4)}` +
        ` ${String(d.mean ?? '-').padStart(7)}` +
        ` ${String(d.p25  ?? '-').padStart(7)}` +
        ` ${String(d.p50  ?? '-').padStart(7)}` +
        ` ${String(d.p75  ?? '-').padStart(7)}`
      );
    }
  }

  // ── 6. 基础字段分布分析 ────────────────────────────────────────────────────
  console.log('\n' + '─'.repeat(70));
  console.log('📋 基础字段分布 (MC / Holders / Top10 / Vol)');
  console.log('─'.repeat(70));

  for (const key of BASIC_KEYS) {
    const coverage = knownRecords.filter(r => (r[key] || 0) > 0).length;
    if (coverage < 5) continue;

    console.log(`\n▸ ${key.toUpperCase()}  (有数据: ${coverage}/${total})`);
    console.log(`  ${'档位'.padEnd(8)} ${'n'.padStart(4)} ${'均值'.padStart(10)} ${'P25'.padStart(10)} ${'中位'.padStart(10)} ${'P75'.padStart(10)}`);
    console.log(`  ${'─'.repeat(60)}`);
    for (const [tier, arr] of Object.entries(byTier)) {
      const vals = arr.map(r => r[key] || 0).filter(v => v > 0);
      if (vals.length === 0) continue;
      const info = TIERS[tier];
      console.log(
        `  ${info.color}${info.label.padEnd(8)}${RESET}` +
        ` ${String(vals.length).padStart(4)}` +
        ` ${String(mean(vals)).padStart(10)}` +
        ` ${String(percentile(vals, 25)).padStart(10)}` +
        ` ${String(percentile(vals, 50)).padStart(10)}` +
        ` ${String(percentile(vals, 75)).padStart(10)}`
      );
    }
  }

  // ── 7. 最优阈值分析 ───────────────────────────────────────────────────────
  if (!DRY_RUN && knownRecords.length >= 20) {
    console.log('\n' + '═'.repeat(70));
    console.log('🎯 各指数最优过滤阈值推荐');
    console.log('   (评分: 银金保留率×0.6 + 铜垃圾过滤率×0.4，误杀率不超过30%)');
    console.log('═'.repeat(70));
    console.log(
      `  ${'指数'.padEnd(18)} ${'推荐阈值'.padStart(8)} ${'银金保留'.padStart(10)} ${'垃圾过滤'.padStart(10)} ${'误杀'.padStart(8)} ${'过滤量'.padStart(8)}`
    );
    console.log('  ' + '─'.repeat(66));

    const recommendations = {};
    for (const key of INDEX_KEYS) {
      const result = findBestThreshold(knownRecords, key);
      if (!result) {
        console.log(`  ${key.padEnd(18)} ${'数据不足'.padStart(8)}`);
        continue;
      }

      const missRate = result.goodBlocked;
      const filterEff = result.badFilterRate;

      // 只有过滤率 > 10% 才有推荐意义
      const worthy = filterEff >= 10;
      const marker = worthy ? '✅' : '⚠️ ';

      console.log(
        `  ${marker} ${key.padEnd(16)} ` +
        `${String(result.threshold).padStart(8)} ` +
        `${(result.goodRetentionRate + '%').padStart(10)} ` +
        `${(result.badFilterRate + '%').padStart(10)} ` +
        `${String(result.goodBlocked + '个').padStart(8)} ` +
        `${String(result.blocked + '个').padStart(8)}`
      );

      if (worthy) {
        recommendations[key] = result.threshold;
      }
    }

    // ── 8. 输出可直接复制到代码的配置 ────────────────────────────────────────
    if (Object.keys(recommendations).length > 0) {
      console.log('\n' + '═'.repeat(70));
      console.log('📋 可直接粘贴到 premium-signal-engine.js 的过滤代码：');
      console.log('═'.repeat(70));
      console.log();
      for (const [key, thresh] of Object.entries(recommendations)) {
        const niceName = key.replace(/_/g, ' ');
        console.log(`    // ${niceName}: 建议阈值 ${thresh}`);
        console.log(`    const ${key.replace(/_([a-z])/g, (_, c) => c.toUpperCase())}Idx = this._getIdxVal(signal.indices?.${key} ?? signal.${key});`);
        console.log(`    if (${key.replace(/_([a-z])/g, (_, c) => c.toUpperCase())}Idx !== null && ${key.replace(/_([a-z])/g, (_, c) => c.toUpperCase())}Idx < ${thresh}) {`);
        console.log(`      return { action: 'SKIP', reason: '${key}_too_low', value: ${key.replace(/_([a-z])/g, (_, c) => c.toUpperCase())}Idx };`);
        console.log(`    }`);
        console.log();
      }
    }

    // ── 9. 模拟：如果按推荐阈值过滤，结果如何变化 ───────────────────────────
    if (Object.keys(recommendations).length > 0) {
      console.log('═'.repeat(70));
      console.log('🔮 模拟：应用推荐阈值后的预期效果');
      console.log('═'.repeat(70));

      const afterFilter = knownRecords.filter(r => {
        for (const [key, thresh] of Object.entries(recommendations)) {
          const v = r.indices[key];
          if (v !== null && v < thresh) return false;
        }
        return true;
      });

      const aft = {
        GOLD:   afterFilter.filter(r => r.outcome === 'GOLD').length,
        SILVER: afterFilter.filter(r => r.outcome === 'SILVER').length,
        COPPER: afterFilter.filter(r => r.outcome === 'COPPER').length,
        TRASH:  afterFilter.filter(r => r.outcome === 'TRASH').length,
      };
      const aftTotal = afterFilter.length;
      const aftSG = aft.GOLD + aft.SILVER;

      console.log(`\n  过滤前: ${total} 个信号 → 银+金 ${silverGoldPct}%`);
      console.log(`  过滤后: ${aftTotal} 个信号 → 银+金 ${aftTotal > 0 ? (aftSG / aftTotal * 100).toFixed(1) : 0}%`);
      console.log(`  淘汰了: ${total - aftTotal} 个信号`);
      console.log(`    其中铜狗: ${byTier.COPPER.length - aft.COPPER} 个`);
      console.log(`    其中垃圾: ${byTier.TRASH.length  - aft.TRASH}  个`);
      console.log(`    其中银狗误杀: ${byTier.SILVER.length - aft.SILVER} 个`);
      console.log(`    其中金狗误杀: ${byTier.GOLD.length   - aft.GOLD}   个`);
    }
  }

  // ── 10. 指数覆盖率报告 ────────────────────────────────────────────────────
  console.log('\n' + '─'.repeat(70));
  console.log('📡 各指数在历史信号中的覆盖率');
  console.log('─'.repeat(70));
  for (const key of INDEX_KEYS) {
    const n = signals.filter(s => s.indices[key] !== null).length;
    const pct = (n / signals.length * 100).toFixed(1);
    const bar = '█'.repeat(Math.round(n / signals.length * 20));
    console.log(`  ${key.padEnd(20)} ${String(n).padStart(4)}/${signals.length}  ${pct.padStart(5)}%  ${bar}`);
  }

  console.log('\n✅ 分析完成\n');
}

main().catch(err => {
  console.error('❌ Fatal:', err.message);
  process.exit(1);
});
