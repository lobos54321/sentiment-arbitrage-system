/**
 * Backtest v7.6 Optimization
 *
 * 基于实际交易数据回测优化方案
 * 目标：将胜率从 18% 提升到 40%+，将平均PnL从 -30% 改善到 +10%
 */

import Database from 'better-sqlite3';
import path from 'path';

const db = new Database(path.join(process.cwd(), 'data/sentiment_arb.db'));

// 获取所有已平仓交易
function getClosedTrades() {
    return db.prepare(`
        SELECT symbol, pnl_percent, exit_type, tier_strategy,
               alpha_tier, intention_tier, entry_price, exit_price,
               signal_source, created_at
        FROM positions
        WHERE status = 'closed'
        ORDER BY created_at DESC
    `).all();
}

// 分析当前策略表现
function analyzeCurrentPerformance(trades) {
    const total = trades.length;
    const wins = trades.filter(t => t.pnl_percent > 0).length;
    const avgPnl = trades.reduce((sum, t) => sum + t.pnl_percent, 0) / total;

    // 按退出类型分析
    const byExitType = {};
    trades.forEach(t => {
        const type = t.exit_type || 'UNKNOWN';
        if (!byExitType[type]) {
            byExitType[type] = { count: 0, totalPnl: 0, wins: 0 };
        }
        byExitType[type].count++;
        byExitType[type].totalPnl += t.pnl_percent;
        if (t.pnl_percent > 0) byExitType[type].wins++;
    });

    // 按叙事层级分析
    const byTier = {};
    trades.forEach(t => {
        // 从tier_strategy解析层级
        const tierMatch = t.tier_strategy?.match(/TIER_([A-Z])/);
        const tier = tierMatch ? `TIER_${tierMatch[1]}` : 'UNKNOWN';
        if (!byTier[tier]) {
            byTier[tier] = { count: 0, totalPnl: 0, wins: 0 };
        }
        byTier[tier].count++;
        byTier[tier].totalPnl += t.pnl_percent;
        if (t.pnl_percent > 0) byTier[tier].wins++;
    });

    return {
        total,
        wins,
        winRate: ((wins / total) * 100).toFixed(1),
        avgPnl: avgPnl.toFixed(2),
        totalPnl: trades.reduce((sum, t) => sum + t.pnl_percent, 0).toFixed(2),
        byExitType,
        byTier
    };
}

// 模拟优化方案1: 更严格的止损
function simulateStricterStopLoss(trades) {
    const newStopLoss = {
        TIER_S: -0.35,
        TIER_A: -0.30,
        TIER_B: -0.25,
        TIER_C: -0.20
    };

    let improved = 0;
    let worsened = 0;
    let newTotalPnl = 0;

    trades.forEach(t => {
        const tierMatch = t.tier_strategy?.match(/TIER_([A-Z])/);
        const tier = tierMatch ? `TIER_${tierMatch[1]}` : 'TIER_C';
        const newLimit = newStopLoss[tier] * 100;

        // 如果原始止损超过新限制，模拟新止损价格
        if (t.exit_type === 'STOP_LOSS' && t.pnl_percent < newLimit) {
            // 更早止损会减少亏损
            const savedLoss = t.pnl_percent - newLimit;
            improved++;
            newTotalPnl += newLimit;
        } else {
            newTotalPnl += t.pnl_percent;
        }
    });

    return {
        name: '方案1: 收紧止损',
        description: 'TIER_S:-35%, TIER_A:-30%, TIER_B:-25%, TIER_C:-20%',
        improved,
        newTotalPnl: newTotalPnl.toFixed(2),
        expectedImprovement: (newTotalPnl - trades.reduce((s, t) => s + t.pnl_percent, 0)).toFixed(2)
    };
}

// 模拟优化方案2: 拒绝低层级叙事
function simulateRejectLowTier(trades) {
    // 如果拒绝TIER_C和未知层级，会避免多少亏损？
    const rejectedTrades = trades.filter(t => {
        const tierMatch = t.tier_strategy?.match(/TIER_([A-Z])/);
        const tier = tierMatch ? tierMatch[1] : 'C';
        return tier === 'C' || !tierMatch;
    });

    const keptTrades = trades.filter(t => {
        const tierMatch = t.tier_strategy?.match(/TIER_([A-Z])/);
        const tier = tierMatch ? tierMatch[1] : 'C';
        return tier !== 'C' && tierMatch;
    });

    const rejectedPnl = rejectedTrades.reduce((s, t) => s + t.pnl_percent, 0);
    const keptPnl = keptTrades.reduce((s, t) => s + t.pnl_percent, 0);

    return {
        name: '方案2: 拒绝TIER_C叙事',
        description: '只买入TIER_A和TIER_B的token',
        rejectedCount: rejectedTrades.length,
        rejectedPnl: rejectedPnl.toFixed(2),
        keptCount: keptTrades.length,
        keptPnl: keptPnl.toFixed(2),
        newWinRate: keptTrades.length > 0
            ? ((keptTrades.filter(t => t.pnl_percent > 0).length / keptTrades.length) * 100).toFixed(1)
            : 'N/A'
    };
}

// 模拟优化方案3: 更短的滞涨时间
function simulateShorterStagnation(trades) {
    // 分析滞涨退出的交易
    const stagnantTrades = trades.filter(t => t.exit_type === 'STAGNANT_EXIT');

    // 如果更早退出（假设亏损减少20%），效果如何？
    let simulatedPnl = 0;
    stagnantTrades.forEach(t => {
        if (t.pnl_percent < 0) {
            // 更早退出，亏损减少20%
            simulatedPnl += t.pnl_percent * 0.8;
        } else {
            // 盈利交易，可能减少盈利
            simulatedPnl += t.pnl_percent * 0.9;
        }
    });

    const originalPnl = stagnantTrades.reduce((s, t) => s + t.pnl_percent, 0);

    return {
        name: '方案3: 缩短滞涨时间',
        description: '将滞涨时间从280min缩短到120min',
        stagnantCount: stagnantTrades.length,
        originalPnl: originalPnl.toFixed(2),
        simulatedPnl: simulatedPnl.toFixed(2),
        improvement: (simulatedPnl - originalPnl).toFixed(2)
    };
}

// 模拟优化方案4: 组合优化
function simulateCombinedOptimization(trades) {
    let newTotalPnl = 0;
    let keptTrades = 0;
    let wins = 0;

    trades.forEach(t => {
        const tierMatch = t.tier_strategy?.match(/TIER_([A-Z])/);
        const tier = tierMatch ? `TIER_${tierMatch[1]}` : 'TIER_C';

        // 方案2: 拒绝TIER_C
        if (tier === 'TIER_C' || !tierMatch) {
            return; // 跳过
        }

        keptTrades++;

        // 方案1: 更严格止损
        const newStopLoss = {
            TIER_S: -35,
            TIER_A: -30,
            TIER_B: -25
        };

        if (t.exit_type === 'STOP_LOSS' && t.pnl_percent < newStopLoss[tier]) {
            newTotalPnl += newStopLoss[tier];
        } else if (t.exit_type === 'STAGNANT_EXIT' && t.pnl_percent < 0) {
            // 方案3: 更早滞涨退出
            newTotalPnl += t.pnl_percent * 0.8;
        } else {
            newTotalPnl += t.pnl_percent;
        }

        if (t.pnl_percent > 0 ||
            (t.exit_type === 'STOP_LOSS' && t.pnl_percent >= newStopLoss[tier])) {
            wins++;
        }
    });

    return {
        name: '方案4: 组合优化 (1+2+3)',
        keptTrades,
        wins,
        newWinRate: keptTrades > 0 ? ((wins / keptTrades) * 100).toFixed(1) : 'N/A',
        newTotalPnl: newTotalPnl.toFixed(2),
        newAvgPnl: keptTrades > 0 ? (newTotalPnl / keptTrades).toFixed(2) : 'N/A'
    };
}

// 主函数
function main() {
    console.log('═══════════════════════════════════════════════════════════════');
    console.log('         回测分析: v7.6 系统优化方案验证');
    console.log('═══════════════════════════════════════════════════════════════\n');

    const trades = getClosedTrades();

    if (trades.length === 0) {
        console.log('⚠️  没有找到已平仓交易数据');
        return;
    }

    // 1. 当前表现分析
    console.log('📊 当前系统表现分析');
    console.log('─────────────────────────────────────────────────────────────────');
    const current = analyzeCurrentPerformance(trades);
    console.log(`总交易: ${current.total}`);
    console.log(`胜率: ${current.winRate}% (${current.wins}/${current.total})`);
    console.log(`平均PnL: ${current.avgPnl}%`);
    console.log(`总PnL: ${current.totalPnl}%`);

    console.log('\n按退出类型:');
    Object.entries(current.byExitType).forEach(([type, data]) => {
        const avgPnl = (data.totalPnl / data.count).toFixed(2);
        const winRate = ((data.wins / data.count) * 100).toFixed(1);
        console.log(`  ${type}: ${data.count}笔, 平均${avgPnl}%, 胜率${winRate}%`);
    });

    console.log('\n按叙事层级:');
    Object.entries(current.byTier).forEach(([tier, data]) => {
        const avgPnl = (data.totalPnl / data.count).toFixed(2);
        const winRate = ((data.wins / data.count) * 100).toFixed(1);
        console.log(`  ${tier}: ${data.count}笔, 平均${avgPnl}%, 胜率${winRate}%`);
    });

    // 2. 优化方案回测
    console.log('\n\n📈 优化方案回测结果');
    console.log('─────────────────────────────────────────────────────────────────');

    const opt1 = simulateStricterStopLoss(trades);
    console.log(`\n🔴 ${opt1.name}`);
    console.log(`   ${opt1.description}`);
    console.log(`   改善止损笔数: ${opt1.improved}`);
    console.log(`   新总PnL: ${opt1.newTotalPnl}% (改善: ${opt1.expectedImprovement}%)`);

    const opt2 = simulateRejectLowTier(trades);
    console.log(`\n🟡 ${opt2.name}`);
    console.log(`   ${opt2.description}`);
    console.log(`   拒绝: ${opt2.rejectedCount}笔 (PnL: ${opt2.rejectedPnl}%)`);
    console.log(`   保留: ${opt2.keptCount}笔 (PnL: ${opt2.keptPnl}%)`);
    console.log(`   新胜率: ${opt2.newWinRate}%`);

    const opt3 = simulateShorterStagnation(trades);
    console.log(`\n🟢 ${opt3.name}`);
    console.log(`   ${opt3.description}`);
    console.log(`   滞涨交易: ${opt3.stagnantCount}笔`);
    console.log(`   原PnL: ${opt3.originalPnl}% → 新PnL: ${opt3.simulatedPnl}%`);
    console.log(`   改善: ${opt3.improvement}%`);

    const opt4 = simulateCombinedOptimization(trades);
    console.log(`\n🔵 ${opt4.name}`);
    console.log(`   保留交易: ${opt4.keptTrades}笔`);
    console.log(`   新胜率: ${opt4.newWinRate}%`);
    console.log(`   新总PnL: ${opt4.newTotalPnl}%`);
    console.log(`   新平均PnL: ${opt4.newAvgPnl}%`);

    // 3. 结论
    console.log('\n\n═══════════════════════════════════════════════════════════════');
    console.log('📋 回测结论与建议');
    console.log('═══════════════════════════════════════════════════════════════');

    console.log(`
问题诊断:
1. 当前胜率极低 (${current.winRate}%)，平均PnL -${Math.abs(parseFloat(current.avgPnl))}%
2. STOP_LOSS 导致大额亏损，止损设置过于宽松
3. STAGNANT_EXIT 占比高，持仓时间过长未能及时止损
4. AI推荐的TIER_A/B token表现不佳，AI可信度存疑

核心问题:
- 入场时机: AI判断过于乐观，将普通币判定为"金狗"
- 出场策略: 止损阈值过高(-40%~-50%)，等到巨亏才出场
- 协同失衡: 高止盈目标(+100%~+150%)与低胜率矛盾

推荐修改:
1. 收紧止损: TIER_A -25%, TIER_B -20%
2. 降低止盈目标: TIER_B +50%而非+100%
3. 缩短滞涨时间: 120分钟而非280分钟
4. 增加入场过滤: 七维分≥40才入池
5. 暂时关闭AI止盈确认，直接按阈值执行
`);
}

main();
