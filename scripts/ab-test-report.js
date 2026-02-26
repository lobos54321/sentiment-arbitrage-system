#!/usr/bin/env node
/**
 * v7.3 A/B 测试报告
 *
 * 功能：
 * 1. 对比 control 组和 treatment 组的表现
 * 2. 计算统计显著性（t-test 简化版）
 * 3. 输出详细对比分析
 * 4. 生成决策建议
 *
 * 用法: node scripts/ab-test-report.js [窗口天数] [测试名称]
 * 示例: node scripts/ab-test-report.js 14
 *       node scripts/ab-test-report.js 7 ai_narrative
 */

import Database from 'better-sqlite3';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const dbPath = path.join(__dirname, '..', 'data', 'sentiment_arb.db');

let db;
try {
    db = new Database(dbPath);
} catch (e) {
    console.error(`无法打开数据库: ${dbPath}`);
    console.error(e.message);
    process.exit(1);
}

/**
 * 生成 A/B 测试基础报告（按 experiment_group 分组）
 */
function generateBasicABReport(windowDays = 14) {
    const cutoff = new Date(Date.now() - windowDays * 24 * 60 * 60 * 1000).toISOString();

    // 按 experiment_group 分组统计
    const groups = db.prepare(`
        SELECT
            experiment_group,
            COUNT(*) as trades,
            AVG(exit_pnl_percent) as avg_pnl,
            SUM(CASE WHEN exit_pnl_percent >= 50 THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN exit_pnl_percent >= 50 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as win_rate,
            SUM(exit_pnl_percent) as total_pnl,
            MAX(exit_pnl_percent) as best,
            MIN(exit_pnl_percent) as worst
        FROM positions
        WHERE status = 'closed'
        AND exit_time >= ?
        AND experiment_group IS NOT NULL
        GROUP BY experiment_group
    `).all(cutoff);

    console.log(`\n${'═'.repeat(70)}`);
    console.log(`🔬 A/B 测试报告 (过去 ${windowDays} 天)`);
    console.log(`${'═'.repeat(70)}\n`);

    if (groups.length === 0) {
        console.log('⚠️ 无 A/B 测试数据');
        console.log('请确保已设置 AB_TEST_ENABLED=true 环境变量\n');
        return null;
    }

    if (groups.length < 2) {
        console.log('⚠️ 数据不足，需要 control 和 treatment 两组数据');
        return null;
    }

    const control = groups.find(g => g.experiment_group === 'control');
    const treatment = groups.find(g => g.experiment_group === 'treatment');

    if (!control || !treatment) {
        console.log('⚠️ 缺少 control 或 treatment 组数据');
        return null;
    }

    // 打印基础统计
    console.log('📊 基础统计:');
    console.log('-'.repeat(70));
    console.log(`Control 组:   ${control.trades} 笔, 胜率 ${control.win_rate?.toFixed(1)}%, 平均 ${control.avg_pnl?.toFixed(1)}%, 总PnL ${control.total_pnl?.toFixed(0)}%`);
    console.log(`Treatment 组: ${treatment.trades} 笔, 胜率 ${treatment.win_rate?.toFixed(1)}%, 平均 ${treatment.avg_pnl?.toFixed(1)}%, 总PnL ${treatment.total_pnl?.toFixed(0)}%`);

    // 计算差异
    const pnlDiff = (treatment.avg_pnl || 0) - (control.avg_pnl || 0);
    const winRateDiff = (treatment.win_rate || 0) - (control.win_rate || 0);

    console.log(`\n📈 差异分析:`);
    console.log('-'.repeat(70));
    console.log(`平均收益差异: ${pnlDiff > 0 ? '+' : ''}${pnlDiff.toFixed(1)}% ${pnlDiff > 0 ? '✅ Treatment 更好' : pnlDiff < 0 ? '❌ Control 更好' : '⚠️ 持平'}`);
    console.log(`胜率差异: ${winRateDiff > 0 ? '+' : ''}${winRateDiff.toFixed(1)}% ${winRateDiff > 0 ? '✅ Treatment 更好' : winRateDiff < 0 ? '❌ Control 更好' : '⚠️ 持平'}`);

    // 统计显著性检验
    const significance = calculateSignificance(control, treatment, cutoff);
    console.log(`\n📐 统计显著性:`);
    console.log('-'.repeat(70));
    console.log(`样本量检验: ${significance.sampleSufficient ? '✅ 足够' : '⚠️ 不足（建议每组至少20笔）'}`);
    console.log(`T值: ${significance.tValue?.toFixed(2) || 'N/A'}`);
    console.log(`统计显著 (p<0.05): ${significance.isSignificant ? '✅ 是' : '❌ 否'}`);

    // 决策建议
    console.log(`\n💡 决策建议:`);
    console.log('-'.repeat(70));

    if (!significance.sampleSufficient) {
        console.log('⏳ 样本量不足，建议继续收集数据 1-2 周后再评估');
    } else if (!significance.isSignificant) {
        console.log('📊 差异不显著，可能原因:');
        console.log('   - Treatment 效果微弱');
        console.log('   - 需要更多样本');
        console.log('   - 市场波动掩盖了效果');
        console.log('建议: 继续运行测试或调整 Treatment 策略');
    } else if (pnlDiff > 0) {
        console.log('✅ Treatment 组显著优于 Control 组');
        console.log(`   预期提升: +${pnlDiff.toFixed(1)}% 平均收益`);
        console.log('建议: 考虑全量上线 Treatment 策略');
    } else {
        console.log('❌ Treatment 组显著差于 Control 组');
        console.log(`   预期损失: ${pnlDiff.toFixed(1)}% 平均收益`);
        console.log('建议: 回滚 Treatment 策略，分析失败原因');
    }

    return { control, treatment, pnlDiff, winRateDiff, significance };
}

/**
 * 按入场来源的 A/B 对比
 */
function generateSourceABReport(windowDays = 14) {
    const cutoff = new Date(Date.now() - windowDays * 24 * 60 * 60 * 1000).toISOString();

    const bySource = db.prepare(`
        SELECT
            entry_source,
            experiment_group,
            COUNT(*) as trades,
            AVG(exit_pnl_percent) as avg_pnl,
            SUM(CASE WHEN exit_pnl_percent >= 50 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as win_rate
        FROM positions
        WHERE status = 'closed'
        AND exit_time >= ?
        AND experiment_group IS NOT NULL
        AND entry_source IS NOT NULL
        GROUP BY entry_source, experiment_group
        HAVING COUNT(*) >= 5
        ORDER BY entry_source, experiment_group
    `).all(cutoff);

    if (bySource.length === 0) {
        return;
    }

    console.log(`\n${'═'.repeat(70)}`);
    console.log(`📊 按入场来源的 A/B 对比`);
    console.log(`${'═'.repeat(70)}\n`);

    // 按 entry_source 分组
    const sourceMap = {};
    for (const row of bySource) {
        if (!sourceMap[row.entry_source]) {
            sourceMap[row.entry_source] = {};
        }
        sourceMap[row.entry_source][row.experiment_group] = row;
    }

    for (const [source, groups] of Object.entries(sourceMap)) {
        const ctrl = groups.control;
        const treat = groups.treatment;

        if (!ctrl || !treat) continue;

        const diff = (treat.avg_pnl || 0) - (ctrl.avg_pnl || 0);
        const emoji = diff > 5 ? '✅' : diff < -5 ? '❌' : '⚠️';

        console.log(`${emoji} ${source}`);
        console.log(`   Control:   ${ctrl.trades} 笔, 胜率 ${ctrl.win_rate?.toFixed(1)}%, 平均 ${ctrl.avg_pnl?.toFixed(1)}%`);
        console.log(`   Treatment: ${treat.trades} 笔, 胜率 ${treat.win_rate?.toFixed(1)}%, 平均 ${treat.avg_pnl?.toFixed(1)}%`);
        console.log(`   差异: ${diff > 0 ? '+' : ''}${diff.toFixed(1)}%\n`);
    }
}

/**
 * AI 叙事的 A/B 对比（有AI评分 vs 无AI评分）
 */
function generateNarrativeABReport(windowDays = 14) {
    const cutoff = new Date(Date.now() - windowDays * 24 * 60 * 60 * 1000).toISOString();

    const withAI = db.prepare(`
        SELECT
            COUNT(*) as trades,
            AVG(exit_pnl_percent) as avg_pnl,
            SUM(CASE WHEN exit_pnl_percent >= 50 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as win_rate
        FROM positions
        WHERE status = 'closed'
        AND exit_time >= ?
        AND ai_narrative_score IS NOT NULL
        AND ai_narrative_score > 0
    `).get(cutoff);

    const withoutAI = db.prepare(`
        SELECT
            COUNT(*) as trades,
            AVG(exit_pnl_percent) as avg_pnl,
            SUM(CASE WHEN exit_pnl_percent >= 50 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as win_rate
        FROM positions
        WHERE status = 'closed'
        AND exit_time >= ?
        AND (ai_narrative_score IS NULL OR ai_narrative_score = 0)
    `).get(cutoff);

    console.log(`\n${'═'.repeat(70)}`);
    console.log(`🎭 AI 叙事评分 A/B 对比`);
    console.log(`${'═'.repeat(70)}\n`);

    if ((withAI.trades || 0) + (withoutAI.trades || 0) < 10) {
        console.log('⚠️ 数据不足');
        return;
    }

    console.log(`有 AI 叙事评分: ${withAI.trades || 0} 笔, 胜率 ${withAI.win_rate?.toFixed(1) || 0}%, 平均 ${withAI.avg_pnl?.toFixed(1) || 0}%`);
    console.log(`无 AI 叙事评分: ${withoutAI.trades || 0} 笔, 胜率 ${withoutAI.win_rate?.toFixed(1) || 0}%, 平均 ${withoutAI.avg_pnl?.toFixed(1) || 0}%`);

    if (withAI.trades >= 10 && withoutAI.trades >= 10) {
        const diff = (withAI.avg_pnl || 0) - (withoutAI.avg_pnl || 0);
        const emoji = diff > 5 ? '✅' : diff < -5 ? '❌' : '⚠️';
        console.log(`\n${emoji} AI 叙事效果: ${diff > 0 ? '+' : ''}${diff.toFixed(1)}%`);

        if (diff > 5) {
            console.log('💡 AI 叙事评分对收益有正向贡献');
        } else if (diff < -5) {
            console.log('💡 AI 叙事评分可能无效或有负面影响，建议检查评分逻辑');
        } else {
            console.log('💡 AI 叙事评分效果不明显，需要更多数据验证');
        }
    }
}

/**
 * 计算统计显著性（简化的 t-test）
 */
function calculateSignificance(control, treatment, cutoff) {
    // 获取详细数据用于计算标准差
    const controlData = db.prepare(`
        SELECT exit_pnl_percent FROM positions
        WHERE status = 'closed' AND exit_time >= ? AND experiment_group = 'control'
    `).all(cutoff).map(r => r.exit_pnl_percent || 0);

    const treatmentData = db.prepare(`
        SELECT exit_pnl_percent FROM positions
        WHERE status = 'closed' AND exit_time >= ? AND experiment_group = 'treatment'
    `).all(cutoff).map(r => r.exit_pnl_percent || 0);

    const n1 = controlData.length;
    const n2 = treatmentData.length;

    // 样本量检验
    const sampleSufficient = n1 >= 20 && n2 >= 20;

    if (n1 < 5 || n2 < 5) {
        return { sampleSufficient: false, tValue: null, isSignificant: false };
    }

    // 计算均值
    const mean1 = controlData.reduce((a, b) => a + b, 0) / n1;
    const mean2 = treatmentData.reduce((a, b) => a + b, 0) / n2;

    // 计算标准差
    const var1 = controlData.reduce((a, b) => a + Math.pow(b - mean1, 2), 0) / (n1 - 1);
    const var2 = treatmentData.reduce((a, b) => a + Math.pow(b - mean2, 2), 0) / (n2 - 1);

    const std1 = Math.sqrt(var1);
    const std2 = Math.sqrt(var2);

    // 计算 t 值（Welch's t-test）
    const se = Math.sqrt(var1/n1 + var2/n2);
    const tValue = se > 0 ? (mean2 - mean1) / se : 0;

    // 简化判断：|t| > 2 约等于 p < 0.05
    const isSignificant = Math.abs(tValue) > 2;

    return {
        sampleSufficient,
        tValue,
        isSignificant,
        controlStats: { n: n1, mean: mean1, std: std1 },
        treatmentStats: { n: n2, mean: mean2, std: std2 }
    };
}

/**
 * 时间趋势分析
 */
function analyzeTrend(windowDays = 14) {
    const cutoff = new Date(Date.now() - windowDays * 24 * 60 * 60 * 1000).toISOString();

    const dailyData = db.prepare(`
        SELECT
            date(exit_time) as day,
            experiment_group,
            COUNT(*) as trades,
            AVG(exit_pnl_percent) as avg_pnl
        FROM positions
        WHERE status = 'closed'
        AND exit_time >= ?
        AND experiment_group IS NOT NULL
        GROUP BY date(exit_time), experiment_group
        ORDER BY day
    `).all(cutoff);

    if (dailyData.length === 0) return;

    console.log(`\n${'═'.repeat(70)}`);
    console.log(`📈 时间趋势分析`);
    console.log(`${'═'.repeat(70)}\n`);

    // 按日期分组
    const dateMap = {};
    for (const row of dailyData) {
        if (!dateMap[row.day]) {
            dateMap[row.day] = {};
        }
        dateMap[row.day][row.experiment_group] = row;
    }

    console.log('日期       | Control         | Treatment       | 差异');
    console.log('-'.repeat(70));

    for (const [day, groups] of Object.entries(dateMap)) {
        const ctrl = groups.control;
        const treat = groups.treatment;

        const ctrlStr = ctrl ? `${ctrl.trades}笔 ${ctrl.avg_pnl?.toFixed(1) || 0}%` : '-';
        const treatStr = treat ? `${treat.trades}笔 ${treat.avg_pnl?.toFixed(1) || 0}%` : '-';

        let diff = '-';
        if (ctrl && treat) {
            const d = (treat.avg_pnl || 0) - (ctrl.avg_pnl || 0);
            diff = `${d > 0 ? '+' : ''}${d.toFixed(1)}%`;
        }

        console.log(`${day} | ${ctrlStr.padEnd(15)} | ${treatStr.padEnd(15)} | ${diff}`);
    }
}

/**
 * 主函数
 */
async function main() {
    const windowDays = parseInt(process.argv[2]) || 14;

    console.log(`\n${'█'.repeat(70)}`);
    console.log(`█  v7.3 A/B 测试分析报告`);
    console.log(`█  分析窗口: ${windowDays} 天`);
    console.log(`█  生成时间: ${new Date().toISOString()}`);
    console.log(`${'█'.repeat(70)}`);

    // 基础 A/B 报告
    const result = generateBasicABReport(windowDays);

    if (result) {
        // 按来源的 A/B 对比
        generateSourceABReport(windowDays);

        // AI 叙事 A/B 对比
        generateNarrativeABReport(windowDays);

        // 时间趋势
        analyzeTrend(windowDays);
    }

    console.log(`\n${'═'.repeat(70)}`);
    console.log(`报告完成`);
    console.log(`${'═'.repeat(70)}\n`);
}

main().catch(err => {
    console.error('A/B 测试报告生成失败:', err.message);
    process.exit(1);
});
