/**
 * Premium Channel 回测脚本
 *
 * 拉取频道历史 🔥New Trending 信号，跑 Claude AI 分析，
 * 然后查询当前价格/市值，对比信号时的市值，计算胜率和收益。
 *
 * 运行: node scripts/backtest-premium-channel.js
 */

import { TelegramClient } from 'telegram';
import { StringSession } from 'telegram/sessions/index.js';
import dotenv from 'dotenv';
import axios from 'axios';
import ClaudeAnalyst from '../src/utils/claude-analyst.js';
import { generatePremiumBuyPrompt } from '../src/prompts/premium-signal-prompts.js';
import { SolanaSnapshotService } from '../src/inputs/chain-snapshot-sol.js';

dotenv.config();

const TARGET_CHANNEL_ID = parseInt(process.env.PREMIUM_CHANNEL_ID || '3636518327');
const MESSAGE_LIMIT = 200;
const SOL_ADDRESS_RE = /\b([1-9A-HJ-NP-Za-km-z]{32,44})\b/g;

// ==========================================
// 消息解析
// ==========================================

function parseNewTrending(text) {
  if (!text || !text.includes('🔥')) return null;
  if (text.includes('📈New ATH')) return null;

  const signal = { chain: 'SOL', source: 'premium_channel', description: text, timestamp: Date.now() };

  const symbolMatch = text.match(/SYMBOL[：:]\s*\$(\S+)/i);
  if (symbolMatch) signal.symbol = symbolMatch[1];

  const mcMatch = text.match(/MC[：:]\s*([\d,.]+)(K|M)?/i);
  if (mcMatch) {
    let mc = parseFloat(mcMatch[1].replace(/,/g, ''));
    if (mcMatch[2]?.toUpperCase() === 'K') mc *= 1000;
    if (mcMatch[2]?.toUpperCase() === 'M') mc *= 1000000;
    signal.market_cap = mc;
  }

  const holdersMatch = text.match(/Holders[：:]\s*([\d,]+)/i);
  if (holdersMatch) signal.holders = parseInt(holdersMatch[1].replace(/,/g, ''));

  const volMatch = text.match(/Vol24H[：:]\s*\$?([\d,.]+)(K|M)?/i);
  if (volMatch) {
    let vol = parseFloat(volMatch[1].replace(/,/g, ''));
    if (volMatch[2]?.toUpperCase() === 'K') vol *= 1000;
    if (volMatch[2]?.toUpperCase() === 'M') vol *= 1000000;
    signal.volume_24h = vol;
  }

  const top10Match = text.match(/Top10[：:]\s*([\d.]+)%/i);
  if (top10Match) signal.top10_pct = parseFloat(top10Match[1]);

  signal.freeze_ok = /freezeAuthority[：:]\s*✅/i.test(text);
  signal.mint_ok = /mintAuthority\w*[：:]\s*✅/i.test(text);

  const ageMatch = text.match(/Age[：:]\s*(\S+)/i);
  if (ageMatch) signal.age = ageMatch[1];

  // Token CA
  const addresses = [];
  let match;
  while ((match = SOL_ADDRESS_RE.exec(text)) !== null) {
    const addr = match[1];
    if (addr.length >= 32 && addr.length <= 44 && new Set(addr).size >= 10) {
      addresses.push(addr);
    }
  }
  if (addresses.length === 0) return null;
  signal.token_ca = addresses[0];

  return signal;
}

// ==========================================
// DexScreener 查询当前价格
// ==========================================

async function getCurrentData(tokenCA) {
  try {
    const resp = await axios.get(`https://api.dexscreener.com/latest/dex/tokens/${tokenCA}`, { timeout: 10000 });
    const pairs = resp.data?.pairs;
    if (!pairs || pairs.length === 0) return null;
    const best = pairs.sort((a, b) => (b.liquidity?.usd || 0) - (a.liquidity?.usd || 0))[0];
    return {
      price_usd: parseFloat(best.priceUsd || 0),
      market_cap: best.marketCap || best.fdv || 0,
      liquidity_usd: best.liquidity?.usd || 0,
      volume_24h: best.volume?.h24 || 0
    };
  } catch {
    return null;
  }
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ==========================================
// 主回测逻辑
// ==========================================

async function main() {
  console.log('═'.repeat(80));
  console.log('💎 PREMIUM CHANNEL BACKTEST');
  console.log('═'.repeat(80));

  // 连接 Telegram
  const apiId = parseInt(process.env.TELEGRAM_API_ID || '0');
  const apiHash = process.env.TELEGRAM_API_HASH || '';
  const sessionString = process.env.TELEGRAM_SESSION || '';

  const session = new StringSession(sessionString);
  const client = new TelegramClient(session, apiId, apiHash, { connectionRetries: 5 });
  await client.connect();
  console.log('✅ Telegram 已连接\n');

  // 找频道
  const dialogs = await client.getDialogs({ limit: 200 });
  let channelEntity = null;
  for (const d of dialogs) {
    if (d.entity?.id?.toString() === TARGET_CHANNEL_ID.toString()) {
      channelEntity = d.entity;
      break;
    }
  }
  if (!channelEntity) {
    console.error('❌ 找不到频道');
    await client.disconnect();
    process.exit(1);
  }
  console.log(`📢 频道: ${channelEntity.title}\n`);

  // 拉历史消息
  const messages = await client.getMessages(channelEntity, { limit: MESSAGE_LIMIT });
  console.log(`📨 拉取了 ${messages.length} 条消息\n`);

  // 解析 🔥 信号（去重）
  const signals = [];
  const seenCAs = new Set();
  for (const msg of messages) {
    if (!msg.message) continue;
    const signal = parseNewTrending(msg.message);
    if (signal && !seenCAs.has(signal.token_ca)) {
      signal.msg_date = new Date(msg.date * 1000);
      seenCAs.add(signal.token_ca);
      signals.push(signal);
    }
  }
  console.log(`🔥 解析出 ${signals.length} 个独立 New Trending 信号\n`);

  if (signals.length === 0) {
    await client.disconnect();
    process.exit(0);
  }

  // 初始化 AI (Claude) + 链上快照
  ClaudeAnalyst.init();
  const snapshotService = new SolanaSnapshotService({});

  // 逐个回测
  const results = [];
  for (let i = 0; i < signals.length; i++) {
    const signal = signals[i];
    const shortCA = signal.token_ca.substring(0, 8);
    console.log(`\n[${i + 1}/${signals.length}] $${signal.symbol || shortCA} | MC: $${signal.market_cap ? (signal.market_cap / 1000).toFixed(1) + 'K' : '?'} | ${signal.msg_date.toLocaleString('zh-CN')}`);

    // 预检
    if (signal.freeze_ok === false || signal.mint_ok === false) {
      console.log('  ❌ 预检失败 (freeze/mint)');
      results.push({ signal, action: 'PRECHECK_FAIL', reason: 'freeze/mint' });
      continue;
    }

    try {
      // 1. 链上快照
      let snapshot = null;
      try {
        snapshot = await snapshotService.getSnapshot(signal.token_ca);
        console.log(`  📡 快照: LP=${snapshot.lp_status} | 流动性=${snapshot.liquidity ? (snapshot.liquidity).toFixed(1) + ' SOL' : '?'} | Top10=${snapshot.top10_percent || '?'}% | 持有人=${snapshot.holder_count || '?'} | 洗盘=${snapshot.wash_flag}`);
      } catch (e) {
        console.log(`  ⚠️ 快照失败: ${e.message}`);
      }

      // 2. DexScreener 交易数据（买卖笔数、流动性、交易量）
      let gmgnData = null;
      try {
        const dexRes = await axios.get(`https://api.dexscreener.com/latest/dex/tokens/${signal.token_ca}`, { timeout: 8000 });
        const pair = dexRes.data?.pairs?.[0];
        if (pair) {
          gmgnData = {
            smart_money_buys: 0, // DexScreener 没有聪明钱数据
            smart_money_amount: 0,
            buy_count_24h: pair.txns?.h24?.buys || 0,
            sell_count_24h: pair.txns?.h24?.sells || 0,
            buy_count_1h: pair.txns?.h1?.buys || 0,
            sell_count_1h: pair.txns?.h1?.sells || 0,
            buy_count_5m: pair.txns?.m5?.buys || 0,
            sell_count_5m: pair.txns?.m5?.sells || 0,
            unique_wallets_24h: 0,
            volume_24h: pair.volume?.h24 || 0,
            price_change_5m: pair.priceChange?.m5 || 0,
            price_change_1h: pair.priceChange?.h1 || 0,
            price_change_24h: pair.priceChange?.h24 || 0,
            holder_count: 0,
            liquidity_usd: pair.liquidity?.usd || 0,
            market_cap_dex: pair.marketCap || 0,
            pair_created: pair.pairCreatedAt || 0,
          };
          const buyRatio = gmgnData.sell_count_24h > 0 ? (gmgnData.buy_count_24h / gmgnData.sell_count_24h).toFixed(2) : '∞';
          console.log(`  📊 DexScreener: 买=${gmgnData.buy_count_24h}/卖=${gmgnData.sell_count_24h} (比=${buyRatio}) | 量=$${(gmgnData.volume_24h/1000).toFixed(1)}K | 流动性=$${(gmgnData.liquidity_usd/1000).toFixed(1)}K | 5m=${gmgnData.price_change_5m}% | 1h=${gmgnData.price_change_1h}%`);
        }
      } catch (e) {
        console.log(`  ⚠️ DexScreener失败: ${e.message}`);
      }

      // 3. 量化评分（不依赖 AI）
      let score = 0;
      const scoreDetails = [];

      // 买卖比
      if (gmgnData) {
        const buyRatio = gmgnData.sell_count_24h > 0 ? gmgnData.buy_count_24h / gmgnData.sell_count_24h : 0;
        if (buyRatio > 1.5) { score += 25; scoreDetails.push(`买卖比${buyRatio.toFixed(2)}(+25)`); }
        else if (buyRatio > 1.2) { score += 15; scoreDetails.push(`买卖比${buyRatio.toFixed(2)}(+15)`); }
        else if (buyRatio < 0.7) { score -= 20; scoreDetails.push(`买卖比${buyRatio.toFixed(2)}(-20)`); }

        // 交易量
        if (gmgnData.volume_24h > 50000) { score += 20; scoreDetails.push(`量$${(gmgnData.volume_24h/1000).toFixed(0)}K(+20)`); }
        else if (gmgnData.volume_24h > 20000) { score += 10; scoreDetails.push(`量$${(gmgnData.volume_24h/1000).toFixed(0)}K(+10)`); }
        else if (gmgnData.volume_24h < 5000) { score -= 10; scoreDetails.push(`量低(-10)`); }

        // 流动性
        const liqUsd = gmgnData.liquidity_usd > 0 ? gmgnData.liquidity_usd : (snapshot?.liquidity ? snapshot.liquidity * 150 : 0);
        if (liqUsd > 10000) { score += 15; scoreDetails.push(`流动性$${(liqUsd/1000).toFixed(0)}K(+15)`); }
        else if (liqUsd > 3000) { score += 5; scoreDetails.push(`流动性$${(liqUsd/1000).toFixed(0)}K(+5)`); }
        else if (liqUsd < 1000 && gmgnData.volume_24h < 10000) { score -= 15; scoreDetails.push(`流动性不足(-15)`); }
      }

      // 市值甜蜜区
      if (signal.market_cap) {
        if (signal.market_cap >= 5000 && signal.market_cap <= 60000) { score += 15; scoreDetails.push(`MC甜蜜区(+15)`); }
        else if (signal.market_cap > 200000) { score -= 20; scoreDetails.push(`MC过高(-20)`); }
      }

      // 安全检查
      if (snapshot) {
        if (snapshot.wash_flag === 'HIGH') { score -= 25; scoreDetails.push(`洗盘HIGH(-25)`); }
        else if (snapshot.wash_flag === 'MEDIUM') { score -= 5; scoreDetails.push(`洗盘MED(-5)`); }
        if (snapshot.top10_percent > 50) { score -= 20; scoreDetails.push(`Top10>${snapshot.top10_percent}%(-20)`); }
        if (snapshot.key_risk_wallets?.length >= 3) { score -= 15; scoreDetails.push(`风险钱包${snapshot.key_risk_wallets.length}(-15)`); }
      }

      // Freeze/Mint
      if (signal.freeze_ok && signal.mint_ok) { score += 10; scoreDetails.push(`安全✅(+10)`); }

      // 决策
      let action = 'SKIP';
      if (score >= 60) action = 'BUY_FULL';
      else if (score >= 40) action = 'BUY_HALF';

      console.log(`  📈 评分: ${score} [${scoreDetails.join(' | ')}] → ${action}`);

      const aiResult = { action, confidence: score, narrative_tier: '-', narrative_reason: scoreDetails.join(', ') };

      if (action === 'SKIP') {
        results.push({ signal, action: 'SKIP', reason: `评分${score}`, aiResult });
        await sleep(500);
        continue;
      }

      // 查询当前价格
      await sleep(500);
      const current = await getCurrentData(signal.token_ca);

      let pnl = null;
      if (current && current.market_cap > 0 && signal.market_cap > 0) {
        pnl = ((current.market_cap - signal.market_cap) / signal.market_cap) * 100;
        const icon = pnl > 0 ? '🟢' : '🔴';
        console.log(`  ${icon} 入场MC: $${(signal.market_cap / 1000).toFixed(1)}K → 当前MC: $${(current.market_cap / 1000).toFixed(1)}K | PnL: ${pnl >= 0 ? '+' : ''}${pnl.toFixed(1)}%`);
      } else {
        console.log('  ⚫ 代币已下架或无数据');
        pnl = -100;
      }

      results.push({ signal, action: aiResult.action, aiResult, current, pnl });
      await sleep(1500);

    } catch (error) {
      console.log(`  ❌ 错误: ${error.message}`);
      results.push({ signal, action: 'ERROR', reason: error.message });
    }
  }

  await client.disconnect();

  // ==========================================
  // 统计结果
  // ==========================================

  console.log('\n\n' + '═'.repeat(80));
  console.log('📊 回测结果');
  console.log('═'.repeat(80));

  const traded = results.filter(r => r.action === 'BUY_FULL' || r.action === 'BUY_HALF');
  const skipped = results.filter(r => r.action === 'SKIP');
  const precheck = results.filter(r => r.action === 'PRECHECK_FAIL');
  const errors = results.filter(r => r.action === 'ERROR');
  const withPnl = traded.filter(r => r.pnl !== undefined && r.pnl !== null);
  const winners = withPnl.filter(r => r.pnl > 0);
  const losers = withPnl.filter(r => r.pnl <= 0);

  console.log(`\n总信号: ${signals.length}`);
  console.log(`预检失败: ${precheck.length}`);
  console.log(`AI 通过 (会买入): ${traded.length}`);
  console.log(`AI 跳过: ${skipped.length}`);
  console.log(`错误: ${errors.length}`);

  if (withPnl.length > 0) {
    const winRate = (winners.length / withPnl.length * 100).toFixed(1);
    const avgPnl = (withPnl.reduce((s, r) => s + r.pnl, 0) / withPnl.length).toFixed(1);
    const maxWin = Math.max(...withPnl.map(r => r.pnl));
    const maxLoss = Math.min(...withPnl.map(r => r.pnl));
    const avgWin = winners.length > 0 ? (winners.reduce((s, r) => s + r.pnl, 0) / winners.length).toFixed(1) : 0;
    const avgLoss = losers.length > 0 ? (losers.reduce((s, r) => s + r.pnl, 0) / losers.length).toFixed(1) : 0;

    console.log(`\n─── 交易统计 ───`);
    console.log(`有数据的交易: ${withPnl.length}`);
    console.log(`胜率: ${winRate}% (${winners.length}W / ${losers.length}L)`);
    console.log(`平均 PnL: ${avgPnl}%`);
    console.log(`平均盈利: +${avgWin}% | 平均亏损: ${avgLoss}%`);
    console.log(`最大盈利: +${maxWin.toFixed(1)}% | 最大亏损: ${maxLoss.toFixed(1)}%`);

    if (parseFloat(avgLoss) !== 0) {
      const rr = (Math.abs(parseFloat(avgWin)) / Math.abs(parseFloat(avgLoss))).toFixed(2);
      console.log(`盈亏比: ${rr}`);
    }

    // ─── 止损模拟 ───
    console.log(`\n─── 止损模拟 ───`);
    for (const sl of [-20, -30, -40, -50]) {
      const slPnls = withPnl.map(r => Math.max(r.pnl, sl));
      const slWinners = slPnls.filter(p => p > 0);
      const slLosers = slPnls.filter(p => p <= 0);
      const slAvgPnl = (slPnls.reduce((s, p) => s + p, 0) / slPnls.length).toFixed(1);
      const slAvgWin = slWinners.length > 0 ? (slWinners.reduce((s, p) => s + p, 0) / slWinners.length).toFixed(1) : 0;
      const slAvgLoss = slLosers.length > 0 ? (slLosers.reduce((s, p) => s + p, 0) / slLosers.length).toFixed(1) : 0;
      const slRR = parseFloat(slAvgLoss) !== 0 ? (Math.abs(parseFloat(slAvgWin)) / Math.abs(parseFloat(slAvgLoss))).toFixed(2) : '∞';
      const slWinRate = (slWinners.length / slPnls.length * 100).toFixed(1);
      const totalReturn = slPnls.reduce((s, p) => s + p, 0).toFixed(1);
      console.log(`  止损${sl}%: 胜率=${slWinRate}% | 平均PnL=${slAvgPnl}% | 盈亏比=${slRR} | 总回报=${totalReturn}%`);
    }

    // ─── 评分门槛对比 ───
    console.log(`\n─── 评分门槛对比 ───`);
    for (const threshold of [0, 20, 30, 40, 50, 60]) {
      const filtered = withPnl.filter(r => (r.aiResult?.confidence || 0) >= threshold);
      if (filtered.length === 0) { console.log(`  门槛>=${threshold}: 无交易`); continue; }
      const fWinners = filtered.filter(r => r.pnl > 0);
      const fAvgPnl = (filtered.reduce((s, r) => s + r.pnl, 0) / filtered.length).toFixed(1);
      const fWinRate = (fWinners.length / filtered.length * 100).toFixed(1);
      // 带 -30% 止损
      const fSlPnls = filtered.map(r => Math.max(r.pnl, -30));
      const fSlAvg = (fSlPnls.reduce((s, p) => s + p, 0) / fSlPnls.length).toFixed(1);
      const fSlTotal = fSlPnls.reduce((s, p) => s + p, 0).toFixed(1);
      console.log(`  门槛>=${threshold}: ${filtered.length}笔 | 胜率=${fWinRate}% | 平均PnL=${fAvgPnl}% | 止损后平均=${fSlAvg}% | 止损后总回报=${fSlTotal}%`);
    }
  }

  // 详细列表
  console.log(`\n─── 详细交易 ───`);
  for (const r of traded) {
    const sym = (r.signal.symbol || r.signal.token_ca.substring(0, 8)).padEnd(15);
    const pnlStr = r.pnl !== undefined && r.pnl !== null ? `${r.pnl >= 0 ? '+' : ''}${r.pnl.toFixed(1)}%` : 'N/A';
    const icon = r.pnl > 0 ? '🟢' : r.pnl <= -50 ? '🔴' : '🟡';
    console.log(`${icon} $${sym} ${r.action.padEnd(10)} 置信度:${String(r.aiResult?.confidence || 0).padEnd(4)} 叙事:${r.aiResult?.narrative_tier || '?'} PnL:${pnlStr}`);
  }

  if (skipped.length > 0) {
    console.log('\n─── AI 跳过的信号 ───');
    for (const r of skipped) {
      const sym = (r.signal.symbol || r.signal.token_ca.substring(0, 8)).padEnd(15);
      console.log(`⏭️  $${sym} ${r.reason || r.aiResult?.narrative_reason || '未知'}`);
    }
  }

  console.log('\n' + '═'.repeat(80));
}

main().catch(error => {
  console.error('❌ Fatal:', error);
  process.exit(1);
});
