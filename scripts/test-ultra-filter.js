/**
 * 测试 Ultra Human Sniper Filter 效果
 */

import fs from 'fs';

const DEVICE_ID = '1d29f750-687f-42e1-851d-59a43e5d2ffa';
const CLIENT_ID = 'gmgn_web_test';

async function fetchApi(url, headers) {
    try {
        const response = await fetch(url, { headers });
        if (!response.ok) return { error: response.status };
        return await response.json();
    } catch (e) {
        return { error: e.message };
    }
}

// Ultra Human Sniper Filter 配置
const filterConfig = {
    maxDailyTxs: 50,
    minProfitPerTrade: 20,
    minWinRate: 0.45,
    maxWinRate: 0.95,
    minAvgHoldTime: 5,
    debugFilter: true
};

function ultraHumanSniperFilter(wallet) {
    const w = wallet;
    const address = w.wallet_address || w.address || 'unknown';
    const shortAddr = address.slice(0, 8);

    const txs1d = w.txs_1d || 0;
    const buy1d = w.buy_1d || 0;
    const sell1d = w.sell_1d || 0;
    const winrate1d = w.winrate_1d || 0;
    const winrate7d = w.winrate_7d || 0;
    const pnl1d = parseFloat(w.realized_profit_1d) || 0;
    const pnl7d = parseFloat(w.realized_profit_7d) || 0;
    const goldenDogs = w.pnl_gt_5x_num_7d || 0;
    const severeLosses = w.pnl_lt_minus_dot5_num_7d || 0;
    const avgHoldTime = w.avg_holding_period_1d || 0;
    const followCount = w.follow_count || 0;
    const tags = w.tags || [];
    const name = w.twitter_name || w.name || shortAddr;

    const profitPerTrade = txs1d > 0 ? pnl1d / txs1d : 0;
    const buySellRatio = sell1d > 0 ? buy1d / sell1d : buy1d;

    // ============ 第一层: 硬性剔除 ============
    if (txs1d > filterConfig.maxDailyTxs) {
        return { pass: false, reason: 'high_frequency', name, txs: txs1d };
    }

    if (winrate1d >= 0.99 && txs1d > 10) {
        return { pass: false, reason: 'fake_winrate', name, winrate: winrate1d, txs: txs1d };
    }

    if (pnl1d <= 0) {
        return { pass: false, reason: 'negative_pnl', name, pnl: pnl1d };
    }

    if (buySellRatio > 0.85 && buySellRatio < 1.15 && txs1d > 20) {
        return { pass: false, reason: 'bot_pattern', name, buySellRatio, txs: txs1d };
    }

    const blacklistTags = ['mev', 'sandwich', 'bot', 'sniper_bot', 'copy_trader'];
    const hasBlacklistTag = tags.some(t =>
        blacklistTags.some(bt => t.toLowerCase().includes(bt))
    );
    if (hasBlacklistTag) {
        return { pass: false, reason: 'blacklist_tag', name, tags };
    }

    // ============ 第二层: 质量筛选 ============
    if (profitPerTrade < filterConfig.minProfitPerTrade) {
        return { pass: false, reason: 'low_profit_per_trade', name, profitPerTrade };
    }

    if (winrate1d < filterConfig.minWinRate || winrate1d > filterConfig.maxWinRate) {
        return { pass: false, reason: 'abnormal_winrate', name, winrate: winrate1d };
    }

    if (pnl7d <= 0) {
        return { pass: false, reason: 'negative_7d_pnl', name, pnl7d };
    }

    if (avgHoldTime > 0 && avgHoldTime < filterConfig.minAvgHoldTime) {
        return { pass: false, reason: 'short_hold_time', name, avgHoldTime };
    }

    // ============ 第三层: 评分 ============
    let score = 50;
    score += Math.min(profitPerTrade / 4, 25);
    score += Math.min(goldenDogs * 8, 40);
    if (winrate1d >= 0.60 && winrate1d <= 0.85) score += 15;
    if (winrate7d >= 0.60) score += 10;
    if (followCount >= 5 && followCount <= 150) score += 10;
    else if (followCount > 300) score -= 10;
    if (severeLosses === 0) score += 10;
    else if (severeLosses > 3) score -= 15;
    if (avgHoldTime >= 30) score += 15;

    return {
        pass: true,
        name,
        score,
        metrics: {
            profitPerTrade,
            goldenDogs,
            winrate1d,
            winrate7d,
            pnl1d,
            pnl7d,
            txs1d,
            avgHoldTime,
            followCount,
            severeLosses
        }
    };
}

async function testUltraFilter() {
    console.log('\n╔════════════════════════════════════════════════════════════╗');
    console.log('║        Ultra Human Sniper Filter 测试                       ║');
    console.log('╚════════════════════════════════════════════════════════════╝\n');

    const sessionPath = './config/gmgn_session.json';
    const sessionData = JSON.parse(fs.readFileSync(sessionPath, 'utf8'));
    const cookies = sessionData?.cookies || [];
    const cookieStr = cookies
        .filter(c => c.domain && c.domain.includes('gmgn'))
        .map(c => `${c.name}=${c.value}`)
        .join('; ');

    const params = new URLSearchParams({
        device_id: DEVICE_ID,
        client_id: CLIENT_ID,
        from_app: 'gmgn',
        app_ver: '20260101',
        tz_name: 'Australia/Brisbane',
        app_lang: 'en',
        os: 'web'
    });

    const headers = {
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0',
        'Cookie': cookieStr,
        'Origin': 'https://gmgn.ai'
    };

    // 获取100个钱包数据
    console.log('📊 获取牛人榜数据...\n');
    const url = `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/7d?tag=smart_degen&limit=100&${params}`;
    const data = await fetchApi(url, headers);

    if (data.error) {
        console.log('❌ API 错误:', data.error);
        return;
    }

    const wallets = data.data?.rank || [];
    console.log(`📥 获取到 ${wallets.length} 个钱包\n`);

    // 运行过滤
    const passed = [];
    const rejected = {};

    for (const wallet of wallets) {
        const result = ultraHumanSniperFilter(wallet);

        if (result.pass) {
            passed.push(result);
        } else {
            rejected[result.reason] = rejected[result.reason] || [];
            rejected[result.reason].push(result);
        }
    }

    // 按分数排序
    passed.sort((a, b) => b.score - a.score);

    // 显示结果
    console.log('═══════════════════════════════════════════════════════════════');
    console.log('📋 过滤结果统计');
    console.log('═══════════════════════════════════════════════════════════════\n');

    console.log(`✅ 通过: ${passed.length} 个`);
    console.log(`❌ 剔除: ${wallets.length - passed.length} 个\n`);

    console.log('📊 剔除原因统计:\n');
    const reasons = {
        high_frequency: '高频机器人 (TXs>50)',
        fake_winrate: '假胜率 (100%+多交易)',
        negative_pnl: '今日亏损',
        bot_pattern: '刷单嫌疑 (买卖比异常)',
        blacklist_tag: '黑名单标签',
        low_profit_per_trade: '每笔利润太低 (<$20)',
        abnormal_winrate: '胜率异常 (<45% 或 >95%)',
        negative_7d_pnl: '7天亏损',
        short_hold_time: '持仓太短 (<5分钟)'
    };

    for (const [reason, desc] of Object.entries(reasons)) {
        const count = rejected[reason]?.length || 0;
        if (count > 0) {
            console.log(`   ${desc}: ${count} 个`);
        }
    }

    // 显示通过的猎人
    console.log('\n═══════════════════════════════════════════════════════════════');
    console.log('🎯 通过筛选的猎人 (按分数排序)');
    console.log('═══════════════════════════════════════════════════════════════\n');

    console.log('排名 | 钱包             | 分数  | 每笔利润 | 金狗 | 胜率   | 持仓时间');
    console.log('─'.repeat(75));

    for (let i = 0; i < Math.min(passed.length, 15); i++) {
        const p = passed[i];
        const m = p.metrics;
        const rank = String(i + 1).padStart(2);
        const name = p.name.slice(0, 14).padEnd(14);
        const score = p.score.toFixed(0).padStart(5);
        const ppt = ('$' + m.profitPerTrade.toFixed(0)).padStart(8);
        const dogs = String(m.goldenDogs).padStart(4);
        const wr = ((m.winrate1d * 100).toFixed(0) + '%').padStart(5);
        const hold = m.avgHoldTime > 0 ? (m.avgHoldTime / 60).toFixed(0) + '分' : 'N/A';

        console.log(`#${rank} | ${name} | ${score} | ${ppt} | ${dogs} | ${wr} | ${hold}`);
    }

    // 显示被剔除的高PnL钱包 (可能是误杀)
    console.log('\n═══════════════════════════════════════════════════════════════');
    console.log('⚠️ 被剔除的高PnL钱包 (检查是否误杀)');
    console.log('═══════════════════════════════════════════════════════════════\n');

    const highPnlRejected = [];
    for (const [reason, items] of Object.entries(rejected)) {
        for (const item of items) {
            // 从原始数据找PnL
            const original = wallets.find(w =>
                (w.wallet_address || w.address).startsWith(item.name.slice(0, 8))
            );
            if (original) {
                const pnl = parseFloat(original.realized_profit_1d) || 0;
                if (pnl > 5000) {
                    highPnlRejected.push({ ...item, reason, pnl });
                }
            }
        }
    }

    highPnlRejected.sort((a, b) => b.pnl - a.pnl);

    for (const r of highPnlRejected.slice(0, 10)) {
        console.log(`   ${r.name} | PnL: $${r.pnl.toFixed(0)} | 原因: ${reasons[r.reason]}`);
        if (r.txs) console.log(`      TXs: ${r.txs}`);
        if (r.profitPerTrade !== undefined) console.log(`      每笔: $${r.profitPerTrade.toFixed(2)}`);
    }

    console.log('\n');
}

testUltraFilter().catch(console.error);
