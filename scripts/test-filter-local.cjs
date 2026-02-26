const fs = require('fs');

// 读取已保存的雷达数据
const dataPath = './logs/radar-data.json';
if (!fs.existsSync(dataPath)) {
    console.log('请先运行采集器获取数据');
    process.exit(1);
}

const data = JSON.parse(fs.readFileSync(dataPath, 'utf8'));
const wallets = data.wallets || [];

console.log('\n╔════════════════════════════════════════════════════════════╗');
console.log('║        Ultra Human Sniper Filter 测试 (本地数据)            ║');
console.log('╚════════════════════════════════════════════════════════════╝\n');
console.log('📥 加载 ' + wallets.length + ' 个钱包数据\n');

// 过滤配置
const config = {
    maxDailyTxs: 50,
    minProfitPerTrade: 20,
    minWinRate: 0.45,
    maxWinRate: 0.95,
    minAvgHoldTime: 5
};

// 过滤逻辑
function filter(w) {
    const txs1d = w.txs_1d || 0;
    const buy1d = w.buy_1d || 0;
    const sell1d = w.sell_1d || 0;
    const winrate1d = w.winrate_1d || 0;
    const pnl1d = parseFloat(w.realized_profit_1d) || 0;
    const pnl7d = parseFloat(w.realized_profit_7d) || 0;
    const avgHoldTime = w.avg_holding_period_1d || 0;
    const goldenDogs = w.pnl_gt_5x_num_7d || 0;
    const name = w.twitter_name || w.name || (w.wallet_address || '').slice(0, 8);

    const profitPerTrade = txs1d > 0 ? pnl1d / txs1d : 0;
    const buySellRatio = sell1d > 0 ? buy1d / sell1d : buy1d;

    // 硬性剔除
    if (txs1d > config.maxDailyTxs) return { pass: false, reason: 'high_frequency', name, txs: txs1d, pnl1d };
    if (winrate1d >= 0.99 && txs1d > 10) return { pass: false, reason: 'fake_winrate', name, txs: txs1d, pnl1d };
    if (pnl1d <= 0) return { pass: false, reason: 'negative_pnl', name, pnl1d };
    if (buySellRatio > 0.85 && buySellRatio < 1.15 && txs1d > 20) return { pass: false, reason: 'bot_pattern', name, txs: txs1d, pnl1d };

    // 质量筛选
    if (profitPerTrade < config.minProfitPerTrade) return { pass: false, reason: 'low_profit_per_trade', name, profitPerTrade, pnl1d };
    if (winrate1d < config.minWinRate || winrate1d > config.maxWinRate) return { pass: false, reason: 'abnormal_winrate', name, winrate: winrate1d, pnl1d };
    if (pnl7d <= 0) return { pass: false, reason: 'negative_7d_pnl', name, pnl7d, pnl1d };

    // 评分
    let score = 50;
    score += Math.min(profitPerTrade / 4, 25);
    score += Math.min(goldenDogs * 8, 40);
    if (winrate1d >= 0.60 && winrate1d <= 0.85) score += 15;
    if (avgHoldTime >= 30) score += 15;

    return { pass: true, name, score, profitPerTrade, goldenDogs, winrate1d, pnl1d, txs1d, avgHoldTime };
}

// 运行过滤
const passed = [];
const rejected = {};

for (const w of wallets) {
    const result = filter(w);
    if (result.pass) {
        passed.push(result);
    } else {
        rejected[result.reason] = rejected[result.reason] || [];
        rejected[result.reason].push(result);
    }
}

passed.sort((a, b) => b.score - a.score);

// 显示结果
console.log('═══════════════════════════════════════════════════════════════');
console.log('📋 过滤结果统计');
console.log('═══════════════════════════════════════════════════════════════\n');
console.log('✅ 通过: ' + passed.length + ' 个');
console.log('❌ 剔除: ' + (wallets.length - passed.length) + ' 个\n');

console.log('📊 剔除原因统计:\n');
const reasons = {
    high_frequency: '高频机器人 (TXs>50)',
    fake_winrate: '假胜率 (100%+多交易)',
    negative_pnl: '今日亏损',
    bot_pattern: '刷单嫌疑',
    low_profit_per_trade: '每笔利润太低 (<$20)',
    abnormal_winrate: '胜率异常 (<45% 或 >95%)',
    negative_7d_pnl: '7天亏损'
};

for (const reason of Object.keys(reasons)) {
    const count = rejected[reason] ? rejected[reason].length : 0;
    if (count > 0) {
        console.log('   ' + reasons[reason] + ': ' + count + ' 个');
    }
}

// 显示通过的猎人
console.log('\n═══════════════════════════════════════════════════════════════');
console.log('🎯 通过筛选的猎人 (Top 15)');
console.log('═══════════════════════════════════════════════════════════════\n');
console.log('排名 | 钱包             | 分数  | 每笔利润  | 金狗 | 胜率   | TXs');
console.log('───────────────────────────────────────────────────────────────────────────');

const limit = Math.min(passed.length, 15);
for (let i = 0; i < limit; i++) {
    const p = passed[i];
    const rank = String(i + 1).padStart(2);
    const name = p.name.slice(0, 14).padEnd(14);
    const score = p.score.toFixed(0).padStart(5);
    const ppt = ('$' + p.profitPerTrade.toFixed(0)).padStart(9);
    const dogs = String(p.goldenDogs).padStart(4);
    const wr = ((p.winrate1d * 100).toFixed(0) + '%').padStart(5);
    const txs = String(p.txs1d).padStart(4);

    console.log('#' + rank + ' | ' + name + ' | ' + score + ' | ' + ppt + ' | ' + dogs + ' | ' + wr + ' | ' + txs);
}

// 显示被剔除的高PnL钱包
console.log('\n═══════════════════════════════════════════════════════════════');
console.log('⚠️ 被剔除的高PnL钱包 (检查是否误杀)');
console.log('═══════════════════════════════════════════════════════════════\n');

const highPnlRejected = [];
for (const reason of Object.keys(rejected)) {
    const items = rejected[reason] || [];
    for (const item of items) {
        if (item.pnl1d > 5000) {
            highPnlRejected.push({ name: item.name, pnl1d: item.pnl1d, reason: reason, txs: item.txs, profitPerTrade: item.profitPerTrade });
        }
    }
}
highPnlRejected.sort((a, b) => b.pnl1d - a.pnl1d);

const highLimit = Math.min(highPnlRejected.length, 10);
for (let i = 0; i < highLimit; i++) {
    const r = highPnlRejected[i];
    const namePad = r.name.slice(0, 14).padEnd(14);
    const pnlPad = r.pnl1d.toFixed(0).padStart(8);
    console.log('   ' + namePad + ' | PnL: $' + pnlPad + ' | 原因: ' + reasons[r.reason]);
    if (r.txs) console.log('      详情: TXs=' + r.txs);
    if (r.profitPerTrade !== undefined) console.log('      详情: 每笔=$' + r.profitPerTrade.toFixed(2));
}

console.log('\n');
