/**
 * 频道历史获取脚本 — 用于NOT_ATH回测数据收集
 * 
 * 用法:
 *   TELEGRAM_API_ID=xxx TELEGRAM_API_HASH=xxx TELEGRAM_SESSION=xxx \
 *   node scripts/fetch-channel-history.mjs [--days 3] [--output data/channel-history.json]
 *
 * 或配置 .env 文件后直接运行:
 *   node scripts/fetch-channel-history.mjs
 *
 * 输出格式: data/channel-history.json
 * {
 *   "fetched_at": "...",
 *   "channel": "Egeye AI Gems 100X Vip", 
 *   "date_range": {"from": "...", "to": "..."},
 *   "total_messages": 851,
 *   "unique_tokens": 498,
 *   "signals": [
 *     {
 *       "ts": 1773576800963,
 *       "type": "NEW_TRENDING" | "ATH",
 *       "is_ath": false | true,
 *       "symbol": "...",
 *       "token_ca": "...",
 *       "market_cap": ...,
 *       "indices": {...}
 *     }
 *   ]
 * }
 */

import { TelegramClient } from 'telegram';
import { StringSession } from 'telegram/sessions/index.js';
import dotenv from 'dotenv';
import fs from 'fs';
import path from 'path';
import https from 'https';
import { fileURLToPath } from 'url';

dotenv.config();

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUTPUT_FILE = path.join(__dirname, '../data/channel-history.json');
const OHLCV_CACHE = path.join(__dirname, '../data/ohlcv-cache.json');

const TARGET_CHANNEL_ID = parseInt(process.env.PREMIUM_CHANNEL_ID || '3636518327');
const SOL_ADDRESS_RE = /\b([1-9A-HJ-NP-Za-km-z]{32,44})\b/g;

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// 解析New Trending信号
function parseNewTrending(text, date) {
  if (!text.includes('🔥') || !text.includes('New Trending')) return null;
  
  const symbolMatch = text.match(/SYMBOL[：:]\s*\$(\S+)/i);
  const mcMatch = text.match(/MC[：:]\s*\$?([\d,.]+)\s*([KMBkmb])?/i);
  const indicesMatch = parseIndices(text);
  
  let mc = 0;
  if (mcMatch) {
    mc = parseFloat(mcMatch[1].replace(/,/g, ''));
    if (mcMatch[2]) {
      mc *= {'k':1e3,'m':1e6,'b':1e9}[mcMatch[2].toLowerCase()] || 1;
    }
  }
  
  const ca = extractSolCA(text);
  if (!ca) return null;
  
  return {
    ts: date.getTime(),
    type: 'NEW_TRENDING',
    is_ath: false,
    symbol: symbolMatch?.[1] || 'UNKNOWN',
    token_ca: ca,
    market_cap: mc,
    indices: indicesMatch,
    freeze_ok: /freezeAuthority[：:]\s*✅/i.test(text),
    mint_ok: /mintAuthority\w*[：:]\s*✅/i.test(text),
  };
}

// 解析ATH信号
function parseATH(text, date) {
  if (!text.includes('📈') || !text.includes('ATH')) return null;
  
  const match = text.match(/\$(\S+)\s+is\s+up\s+\*{0,2}([\d.]+)(%|X)\*{0,2}/i);
  if (!match) return null;
  
  let gainPct = parseFloat(match[2]);
  if (match[3].toUpperCase() === 'X') gainPct = (gainPct - 1) * 100;
  
  const mcMatch = text.match(/\$([\d,.]+)\s*([KMBkmb])?\s*[—\-]+>\s*\$([\d,.]+)\s*([KMBkmb])?/);
  let mcTo = 0;
  if (mcMatch) {
    mcTo = parseFloat(mcMatch[3].replace(/,/g, ''));
    if (mcMatch[4]) mcTo *= {'k':1e3,'m':1e6,'b':1e9}[mcMatch[4].toLowerCase()] || 1;
  }
  
  const ca = extractSolCA(text);
  if (!ca) return null;
  
  return {
    ts: date.getTime(),
    type: 'ATH',
    is_ath: true,
    symbol: match[1],
    token_ca: ca,
    market_cap: mcTo,
    gain_pct: gainPct,
    indices: parseIndices(text),
  };
}

function parseIndices(text) {
  const result = {};
  const names = [
    ['super_index', 'Super Index'],
    ['ai_index', 'AI Index'],
    ['trade_index', 'Trade Index'],
    ['security_index', 'Security Index'],
    ['address_index', 'Address Index'],
  ];
  for (const [key, label] of names) {
    const escaped = label.replace(/\s+/g, '\\s*');
    const re = new RegExp(escaped + '[：:]\\s*[\\(（]signal[\\)）]\\s*x?(\\d+).*?[\\(（]current[\\)）]\\s*x?(\\d+)', 'i');
    const m = text.match(re);
    if (m) {
      result[key] = { signal: parseInt(m[1]), current: parseInt(m[2]) };
    }
  }
  return Object.keys(result).length > 0 ? result : null;
}

function extractSolCA(text) {
  const matches = [...text.matchAll(SOL_ADDRESS_RE)].map(m => m[1]);
  for (const addr of matches) {
    if (addr.length >= 32 && addr.length <= 44 && new Set(addr).size >= 10 
        && !addr.startsWith('http')) {
      return addr;
    }
  }
  return null;
}

// 获取OHLCV
function fetchJson(url) {
  return new Promise((resolve) => {
    https.get(url, { timeout: 15000, headers: {'User-Agent': 'Mozilla/5.0'} }, (res) => {
      let data = '';
      res.on('data', d => data += d);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); } catch { resolve(null); }
      });
    }).on('error', () => resolve(null)).on('timeout', () => resolve(null));
  });
}

async function fetchOHLCV(ca, entryTsSec) {
  await sleep(800);
  const ds = await fetchJson(`https://api.dexscreener.com/latest/dex/tokens/${ca}`);
  if (!ds?.pairs?.length) return null;
  
  const candidates = ds.pairs
    .filter(p => p.chainId === 'solana' && p.pairCreatedAt)
    .sort((a, b) => Math.abs(a.pairCreatedAt/1000 - entryTsSec) - Math.abs(b.pairCreatedAt/1000 - entryTsSec));
  
  if (!candidates.length) return null;
  const poolId = candidates[0].pairAddress;
  
  const bars = {};
  for (const windowEnd of [entryTsSec + 600, entryTsSec + 3600]) {
    await sleep(1500);
    const d = await fetchJson(
      `https://api.geckoterminal.com/api/v2/networks/solana/pools/${poolId}/ohlcv/minute` +
      `?aggregate=1&limit=200&before_timestamp=${windowEnd}&token=base`
    );
    const list = d?.data?.attributes?.ohlcv_list || [];
    for (const bar of list) {
      const ts = parseInt(bar[0]);
      if (!bars[ts]) bars[ts] = { ts, o: bar[1], h: bar[2], l: bar[3], c: bar[4], vol: bar[5] };
    }
  }
  
  const candles = Object.values(bars).sort((a, b) => a.ts - b.ts);
  if (!candles.length) return null;
  
  return {
    ca,
    pairAddr: poolId,
    symbol: candidates[0].baseToken?.symbol || '',
    candles,
  };
}

// 主函数
async function main() {
  const apiId = parseInt(process.env.TELEGRAM_API_ID || '0');
  const apiHash = process.env.TELEGRAM_API_HASH || '';
  const sessionString = process.env.TELEGRAM_SESSION || '';
  
  if (!apiId || !apiHash || !sessionString) {
    console.error('❌ 缺少 Telegram 凭证！');
    console.error('');
    console.error('请在 .env 中设置：');
    console.error('  TELEGRAM_API_ID=你的API_ID');
    console.error('  TELEGRAM_API_HASH=你的API_HASH');
    console.error('  TELEGRAM_SESSION=你的会话字符串');
    console.error('');
    console.error('获取方式：');
    console.error('  1. 访问 https://my.telegram.org 申请 API ID');
    console.error('  2. 运行: node scripts/authenticate-telegram.js 获取会话字符串');
    console.error('');
    console.error('💡 如果有 Zeabur DASHBOARD_TOKEN，可以直接导出数据库：');
    console.error('   DASHBOARD_TOKEN=xxx node scripts/export-zeabur-signals.mjs');
    process.exit(1);
  }
  
  console.log('🔌 连接 Telegram...');
  const session = new StringSession(sessionString);
  const client = new TelegramClient(session, apiId, apiHash, { connectionRetries: 5 });
  
  await client.connect();
  console.log('✅ Telegram 已连接\n');
  
  // 找频道
  const dialogs = await client.getDialogs({ limit: 300 });
  let channelEntity = null;
  for (const d of dialogs) {
    const id = d.entity?.id?.value ?? d.entity?.id;
    if (String(id) === String(TARGET_CHANNEL_ID)) {
      channelEntity = d.entity;
      break;
    }
  }
  
  if (!channelEntity) {
    console.error(`❌ 找不到频道 ID: ${TARGET_CHANNEL_ID}`);
    await client.disconnect();
    process.exit(1);
  }
  
  console.log(`📢 频道: ${channelEntity.title}`);
  
  // 拉历史消息 (最多3000条，覆盖约3天)
  const LIMIT = 3000;
  console.log(`📨 拉取最近 ${LIMIT} 条消息...`);
  
  const messages = await client.getMessages(channelEntity, { limit: LIMIT });
  console.log(`✅ 获取 ${messages.length} 条消息\n`);
  
  // 解析信号
  const signals = [];
  const seenCAs = new Set();
  let ath_count = 0, trending_count = 0;
  
  for (const msg of messages) {
    const text = msg.message || msg.text || '';
    if (!text) continue;
    
    const date = new Date(msg.date * 1000);
    
    let signal = null;
    if (text.includes('🔥') && text.includes('New Trending')) {
      signal = parseNewTrending(text, date);
      if (signal) trending_count++;
    } else if (text.includes('📈') && text.includes('ATH')) {
      signal = parseATH(text, date);
      if (signal) ath_count++;
    }
    
    if (signal?.token_ca) {
      if (!seenCAs.has(signal.token_ca)) {
        seenCAs.add(signal.token_ca);
        signals.push(signal);
      }
    }
  }
  
  console.log(`📊 解析结果:`);
  console.log(`   ATH信号: ${ath_count}个`);
  console.log(`   New Trending信号: ${trending_count}个`);
  console.log(`   去重后unique token: ${signals.length}个\n`);
  
  // 按时间排序
  signals.sort((a, b) => a.ts - b.ts);
  
  // 时间范围
  const dates = signals.map(s => new Date(s.ts));
  const dateRange = {
    from: new Date(Math.min(...dates)).toISOString(),
    to: new Date(Math.max(...dates)).toISOString(),
  };
  
  // 保存信号列表
  const output = {
    fetched_at: new Date().toISOString(),
    channel: channelEntity.title,
    date_range: dateRange,
    total_messages: messages.length,
    unique_tokens: signals.length,
    ath_signals: signals.filter(s => s.is_ath).length,
    trending_signals: signals.filter(s => !s.is_ath).length,
    signals,
  };
  
  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(output, null, 2));
  console.log(`💾 信号已保存到 ${OUTPUT_FILE}`);
  
  // 获取OHLCV
  console.log('\n📈 获取K线数据...');
  const cache = fs.existsSync(OHLCV_CACHE) 
    ? JSON.parse(fs.readFileSync(OHLCV_CACHE, 'utf8')) 
    : {};
  
  let fetched = 0, failed = 0;
  for (let i = 0; i < signals.length; i++) {
    const sig = signals[i];
    if (cache[sig.token_ca] !== undefined) {
      process.stdout.write('.');
      continue;
    }
    
    process.stdout.write(`\n  [${i+1}/${signals.length}] ${sig.symbol} (${sig.type})... `);
    const data = await fetchOHLCV(sig.token_ca, sig.ts / 1000);
    
    if (data?.candles?.length) {
      cache[sig.token_ca] = data;
      fetched++;
      process.stdout.write(`✅ ${data.candles.length}根K线`);
    } else {
      cache[sig.token_ca] = null;
      failed++;
      process.stdout.write('❌ 无数据');
    }
    
    // 每10个保存一次
    if ((i + 1) % 10 === 0) {
      fs.writeFileSync(OHLCV_CACHE, JSON.stringify(cache, null, 2));
    }
  }
  
  fs.writeFileSync(OHLCV_CACHE, JSON.stringify(cache, null, 2));
  console.log(`\n\n✅ K线获取完成: ${fetched}个成功, ${failed}个失败`);
  console.log(`📊 OHLCV缓存: ${Object.keys(cache).length}个代币`);
  
  await client.disconnect();
  
  console.log('\n🎯 下一步: node scripts/backtest-not-ath.mjs');
}

main().catch(console.error);
