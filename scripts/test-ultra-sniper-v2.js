/**
 * 测试 Ultra Human Sniper v2.0 系统
 *
 * 测试内容:
 * 1. 猎人分类算法
 * 2. 动态评分系统
 * 3. 本地数据验证
 */

import fs from 'fs';
import {
    classifyHunterType,
    calculateDynamicScore,
    HUNTER_TYPES
} from '../src/inputs/ultra-human-sniper-v2.js';

// ═══════════════════════════════════════════════════════════════
// 测试配置
// ═══════════════════════════════════════════════════════════════

const CONFIG = {
    dataPath: './logs/radar-data.json',
    minScore: 55,
    suitableTypes: ['FOX', 'TURTLE', 'WOLF']
};

// ═══════════════════════════════════════════════════════════════
// 主测试函数
// ═══════════════════════════════════════════════════════════════

async function testUltraSniperV2() {
    console.log('\n');
    console.log('╔══════════════════════════════════════════════════════════════════╗');
    console.log('║           Ultra Human Sniper v2.0 测试                            ║');
    console.log('╚══════════════════════════════════════════════════════════════════╝\n');

    // 1. 加载本地数据
    if (!fs.existsSync(CONFIG.dataPath)) {
        console.log('❌ 请先运行采集器获取数据: node src/inputs/gmgn-radar-collector.js');
        process.exit(1);
    }

    const data = JSON.parse(fs.readFileSync(CONFIG.dataPath, 'utf8'));
    const wallets = data.wallets || [];
    console.log(`📥 加载 ${wallets.length} 个钱包数据\n`);

    // ═══════════════════════════════════════════════════════════
    // 2. 猎人分类统计
    // ═══════════════════════════════════════════════════════════

    console.log('═══════════════════════════════════════════════════════════════');
    console.log('📊 第一阶段: 猎人分类');
    console.log('═══════════════════════════════════════════════════════════════\n');

    const classified = {
        EAGLE: [],
        FOX: [],
        TURTLE: [],
        WOLF: [],
        BOT: [],
        NORMAL: []
    };

    for (const wallet of wallets) {
        const profile = classifyHunterType(wallet);
        const address = wallet.wallet_address || wallet.address;
        const name = wallet.twitter_name || wallet.name || address.slice(0, 8);

        classified[profile.type].push({
            address,
            name,
            profile,
            wallet
        });
    }

    // 打印分类结果
    console.log('猎人类型分布:');
    console.log('─'.repeat(60));

    const typeOrder = ['EAGLE', 'FOX', 'TURTLE', 'WOLF', 'BOT', 'NORMAL'];
    for (const type of typeOrder) {
        const info = HUNTER_TYPES[type];
        const count = classified[type].length;
        const percent = ((count / wallets.length) * 100).toFixed(1);
        const bar = '█'.repeat(Math.ceil(count / 2));

        console.log(`${info.emoji} ${type.padEnd(8)} | ${String(count).padStart(3)} (${percent.padStart(5)}%) ${bar}`);
    }

    // ═══════════════════════════════════════════════════════════
    // 3. 详细展示各类型猎人
    // ═══════════════════════════════════════════════════════════

    console.log('\n═══════════════════════════════════════════════════════════════');
    console.log('🦊 FOX 型猎人 (金狗猎手) - 最适合 v6.9!');
    console.log('═══════════════════════════════════════════════════════════════\n');

    if (classified.FOX.length === 0) {
        console.log('   暂无 FOX 型猎人\n');
    } else {
        console.log('排名 | 猎人名称         | 金狗 | 每笔利润  | 胜率   | TXs  | 分类原因');
        console.log('─'.repeat(80));

        for (let i = 0; i < Math.min(classified.FOX.length, 10); i++) {
            const h = classified.FOX[i];
            const w = h.wallet;
            const pnl1d = parseFloat(w.realized_profit_1d) || 0;
            const txs1d = w.txs_1d || 0;
            const profitPerTrade = txs1d > 0 ? pnl1d / txs1d : 0;
            const goldenDogs = w.pnl_gt_5x_num_7d || 0;
            const winrate = ((w.winrate_1d || 0) * 100).toFixed(0);

            const rank = String(i + 1).padStart(2);
            const name = h.name.slice(0, 14).padEnd(14);
            const dogs = String(goldenDogs).padStart(4);
            const ppt = ('$' + profitPerTrade.toFixed(0)).padStart(9);
            const wr = (winrate + '%').padStart(5);
            const txs = String(txs1d).padStart(4);
            const reason = h.profile.reason || '';

            console.log(`#${rank} | ${name} | ${dogs} | ${ppt} | ${wr} | ${txs} | ${reason}`);
        }
    }

    console.log('\n═══════════════════════════════════════════════════════════════');
    console.log('🐢 TURTLE 型猎人 (波段猎手)');
    console.log('═══════════════════════════════════════════════════════════════\n');

    if (classified.TURTLE.length === 0) {
        console.log('   暂无 TURTLE 型猎人\n');
    } else {
        console.log('排名 | 猎人名称         | 持仓时间   | 胜率   | 每笔利润  | TXs');
        console.log('─'.repeat(75));

        for (let i = 0; i < Math.min(classified.TURTLE.length, 10); i++) {
            const h = classified.TURTLE[i];
            const w = h.wallet;
            const pnl1d = parseFloat(w.realized_profit_1d) || 0;
            const txs1d = w.txs_1d || 0;
            const profitPerTrade = txs1d > 0 ? pnl1d / txs1d : 0;
            const avgHoldTime = ((w.avg_holding_period_1d || 0) / 60).toFixed(0);
            const winrate = ((w.winrate_1d || 0) * 100).toFixed(0);

            const rank = String(i + 1).padStart(2);
            const name = h.name.slice(0, 14).padEnd(14);
            const hold = (avgHoldTime + '分钟').padStart(10);
            const wr = (winrate + '%').padStart(5);
            const ppt = ('$' + profitPerTrade.toFixed(0)).padStart(9);
            const txs = String(txs1d).padStart(4);

            console.log(`#${rank} | ${name} | ${hold} | ${wr} | ${ppt} | ${txs}`);
        }
    }

    console.log('\n═══════════════════════════════════════════════════════════════');
    console.log('🐺 WOLF 型猎人 (稳定猎手)');
    console.log('═══════════════════════════════════════════════════════════════\n');

    if (classified.WOLF.length === 0) {
        console.log('   暂无 WOLF 型猎人\n');
    } else {
        console.log('排名 | 猎人名称         | 胜率   | 每笔利润  | 严重亏损 | TXs');
        console.log('─'.repeat(70));

        for (let i = 0; i < Math.min(classified.WOLF.length, 10); i++) {
            const h = classified.WOLF[i];
            const w = h.wallet;
            const pnl1d = parseFloat(w.realized_profit_1d) || 0;
            const txs1d = w.txs_1d || 0;
            const profitPerTrade = txs1d > 0 ? pnl1d / txs1d : 0;
            const winrate = ((w.winrate_1d || 0) * 100).toFixed(0);
            const severeLosses = w.pnl_lt_minus_dot5_num_7d || 0;

            const rank = String(i + 1).padStart(2);
            const name = h.name.slice(0, 14).padEnd(14);
            const wr = (winrate + '%').padStart(5);
            const ppt = ('$' + profitPerTrade.toFixed(0)).padStart(9);
            const losses = String(severeLosses).padStart(6);
            const txs = String(txs1d).padStart(4);

            console.log(`#${rank} | ${name} | ${wr} | ${ppt} | ${losses} | ${txs}`);
        }
    }

    // ═══════════════════════════════════════════════════════════
    // 4. 动态评分系统测试
    // ═══════════════════════════════════════════════════════════

    console.log('\n═══════════════════════════════════════════════════════════════');
    console.log('📊 第二阶段: 动态评分');
    console.log('═══════════════════════════════════════════════════════════════\n');

    // 对适合v6.9的猎人进行评分
    const scoredHunters = [];

    for (const type of CONFIG.suitableTypes) {
        for (const h of classified[type]) {
            const scoreResult = calculateDynamicScore(h.wallet, h.profile);
            scoredHunters.push({
                ...h,
                score: scoreResult.totalScore,
                scoreBreakdown: scoreResult.breakdown,
                weights: scoreResult.weights
            });
        }
    }

    // 按分数排序
    scoredHunters.sort((a, b) => b.score - a.score);

    // 筛选达标的
    const qualified = scoredHunters.filter(h => h.score >= CONFIG.minScore);

    console.log(`适合v6.9的猎人: ${scoredHunters.length} 个`);
    console.log(`达标猎人 (≥${CONFIG.minScore}分): ${qualified.length} 个\n`);

    console.log('═══════════════════════════════════════════════════════════════');
    console.log('🏆 最终猎人排行榜 (Top 15)');
    console.log('═══════════════════════════════════════════════════════════════\n');

    console.log('排名 | 类型 | 猎人名称         | 总分  | 效率  | 金狗  | 稳定  | 匹配  | 控损  | 社区');
    console.log('─'.repeat(95));

    for (let i = 0; i < Math.min(qualified.length, 15); i++) {
        const h = qualified[i];
        const b = h.scoreBreakdown;

        const rank = String(i + 1).padStart(2);
        const emoji = h.profile.emoji;
        const name = h.name.slice(0, 14).padEnd(14);
        const total = h.score.toFixed(0).padStart(5);
        const eff = (b.profitEfficiency || 0).toFixed(0).padStart(5);
        const dog = (b.goldenDogScore || 0).toFixed(0).padStart(5);
        const stab = (b.winrateStability || 0).toFixed(0).padStart(5);
        const match = (b.holdingMatch || 0).toFixed(0).padStart(5);
        const loss = (b.lossControl || 0).toFixed(0).padStart(5);
        const comm = (b.communityScore || 0).toFixed(0).padStart(5);

        console.log(`#${rank} | ${emoji}  | ${name} | ${total} | ${eff} | ${dog} | ${stab} | ${match} | ${loss} | ${comm}`);
    }

    // ═══════════════════════════════════════════════════════════
    // 5. 被排除的高 PnL 钱包分析
    // ═══════════════════════════════════════════════════════════

    console.log('\n═══════════════════════════════════════════════════════════════');
    console.log('⚠️ 被排除的高 PnL 钱包 (检查是否误杀)');
    console.log('═══════════════════════════════════════════════════════════════\n');

    // 收集被排除的
    const excluded = [];
    for (const type of ['BOT', 'NORMAL']) {
        for (const h of classified[type]) {
            const pnl1d = parseFloat(h.wallet.realized_profit_1d) || 0;
            if (pnl1d > 5000) {
                excluded.push({
                    name: h.name,
                    type: h.profile.type,
                    reason: h.profile.reason,
                    pnl1d,
                    txs: h.wallet.txs_1d || 0,
                    winrate: (h.wallet.winrate_1d || 0) * 100
                });
            }
        }
    }

    excluded.sort((a, b) => b.pnl1d - a.pnl1d);

    if (excluded.length === 0) {
        console.log('   无高 PnL 钱包被排除\n');
    } else {
        console.log('猎人名称         | 类型     | 1D PnL       | TXs  | 胜率   | 排除原因');
        console.log('─'.repeat(85));

        for (const e of excluded.slice(0, 10)) {
            const name = e.name.slice(0, 14).padEnd(14);
            const type = e.type.padEnd(8);
            const pnl = ('$' + e.pnl1d.toFixed(0)).padStart(12);
            const txs = String(e.txs).padStart(4);
            const wr = (e.winrate.toFixed(0) + '%').padStart(5);
            const reason = e.reason || '';

            console.log(`${name} | ${type} | ${pnl} | ${txs} | ${wr} | ${reason}`);
        }
    }

    // ═══════════════════════════════════════════════════════════
    // 6. 总结
    // ═══════════════════════════════════════════════════════════

    console.log('\n═══════════════════════════════════════════════════════════════');
    console.log('📈 测试总结');
    console.log('═══════════════════════════════════════════════════════════════\n');

    console.log(`输入钱包数: ${wallets.length}`);
    console.log(`猎人分类:`);
    console.log(`   🦅 Eagle (精准狙击): ${classified.EAGLE.length} - 不适合60秒轮询`);
    console.log(`   🦊 Fox (金狗猎手): ${classified.FOX.length} - ⭐ 最适合v6.9!`);
    console.log(`   🐢 Turtle (波段猎手): ${classified.TURTLE.length} - 适合跟单`);
    console.log(`   🐺 Wolf (稳定猎手): ${classified.WOLF.length} - 适合跟单`);
    console.log(`   🤖 Bot (机器人): ${classified.BOT.length} - 已排除`);
    console.log(`   👤 Normal (普通): ${classified.NORMAL.length} - 已排除`);
    console.log(`\n最终追踪: ${qualified.length} 个猎人 (≥${CONFIG.minScore}分)`);

    if (qualified.length > 0) {
        const avgScore = qualified.reduce((sum, h) => sum + h.score, 0) / qualified.length;
        console.log(`平均分数: ${avgScore.toFixed(1)}`);

        const typeCount = { FOX: 0, TURTLE: 0, WOLF: 0 };
        for (const h of qualified) {
            typeCount[h.profile.type]++;
        }
        console.log(`类型分布: 🦊 ${typeCount.FOX} | 🐢 ${typeCount.TURTLE} | 🐺 ${typeCount.WOLF}`);
    }

    console.log('\n');
}

// 运行测试
testUltraSniperV2().catch(console.error);
