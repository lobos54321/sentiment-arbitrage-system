#!/usr/bin/env node
/**
 * v7.3 模块绩效分析脚本
 *
 * 功能：
 * 1. 按 entry_source 分组计算绩效
 * 2. 按 ai_narrative_tier 分组计算绩效
 * 3. 分析拒绝信号的防守价值
 * 4. 计算各模块相对于基准的表现
 * 5. 输出报告和建议
 *
 * 用法: node scripts/analyze-module-performance.js [窗口天数]
 * 示例: node scripts/analyze-module-performance.js 7
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
 * 按入场来源分析绩效
 */
function analyzeBySource(windowDays = 7) {
    const cutoff = new Date(Date.now() - windowDays * 24 * 60 * 60 * 1000).toISOString();

    // 整体基准
    const baseline = db.prepare(`
        SELECT
            COUNT(*) as total,
            AVG(exit_pnl_percent) as avg_pnl,
            SUM(CASE WHEN exit_pnl_percent >= 50 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as win_rate,
            SUM(exit_pnl_percent) as total_pnl
        FROM positions
        WHERE status = 'closed' AND exit_time >= ?
    `).get(cutoff);

    // 按 entry_source 分组
    const bySource = db.prepare(`
        SELECT
            entry_source,
            COUNT(*) as trades,
            AVG(exit_pnl_percent) as avg_pnl,
            SUM(CASE WHEN exit_pnl_percent >= 50 THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN exit_pnl_percent >= 50 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as win_rate,
            SUM(exit_pnl_percent) as total_pnl,
            MAX(exit_pnl_percent) as best,
            MIN(exit_pnl_percent) as worst,
            AVG(CASE WHEN exit_pnl_percent >= 50 THEN exit_pnl_percent END) as avg_win,
            AVG(CASE WHEN exit_pnl_percent < 50 THEN exit_pnl_percent END) as avg_loss
        FROM positions
        WHERE status = 'closed' AND exit_time >= ?
        GROUP BY entry_source
        ORDER BY avg_pnl DESC
    `).all(cutoff);

    console.log(`\n${'═'.repeat(70)}`);
    console.log(`📊 模块绩效分析 (过去 ${windowDays} 天)`);
    console.log(`${'═'.repeat(70)}`);

    if (baseline.total === 0) {
        console.log('\n⚠️ 无交易数据');
        return { baseline: { total: 0 }, bySource: [] };
    }

    console.log(`\n基准: ${baseline.total} 笔交易, 胜率 ${baseline.win_rate?.toFixed(1) || 0}%, 平均收益 ${baseline.avg_pnl?.toFixed(1) || 0}%\n`);

    console.log('按入场来源分组:');
    console.log('-'.repeat(70));

    for (const row of bySource) {
        const relPerf = baseline.avg_pnl && baseline.avg_pnl !== 0 ?
            ((row.avg_pnl - baseline.avg_pnl) / Math.abs(baseline.avg_pnl) * 100).toFixed(0) : '0';

        const emoji = row.avg_pnl > (baseline.avg_pnl || 0) ? '✅' :
                     row.avg_pnl < 0 ? '❌' : '⚠️';

        const profitFactor = row.avg_loss && row.avg_loss !== 0 ?
            Math.abs(row.avg_win || 0) / Math.abs(row.avg_loss) : 0;

        console.log(`${emoji} ${row.entry_source || 'unknown'}`);
        console.log(`   交易: ${row.trades} | 胜率: ${row.win_rate?.toFixed(1) || 0}% | 平均: ${row.avg_pnl?.toFixed(1) || 0}% | 相对基准: ${relPerf}%`);
        console.log(`   最佳: +${row.best?.toFixed(0) || 0}% | 最差: ${row.worst?.toFixed(0) || 0}% | 盈亏比: ${profitFactor.toFixed(2)}`);
    }

    return { baseline, bySource };
}

/**
 * 按 AI 叙事等级分析绩效
 */
function analyzeByNarrativeTier(windowDays = 7) {
    const cutoff = new Date(Date.now() - windowDays * 24 * 60 * 60 * 1000).toISOString();

    // 使用 intention_tier 字段（实际字段名）
    const byTier = db.prepare(`
        SELECT
            intention_tier as ai_narrative_tier,
            COUNT(*) as trades,
            AVG(exit_pnl_percent) as avg_pnl,
            SUM(CASE WHEN exit_pnl_percent >= 50 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as win_rate,
            AVG(ai_narrative_score) as avg_score
        FROM positions
        WHERE status = 'closed' AND exit_time >= ? AND intention_tier IS NOT NULL
        GROUP BY intention_tier
        ORDER BY
            CASE intention_tier
                WHEN 'TIER_S' THEN 1
                WHEN 'TIER_A' THEN 2
                WHEN 'TIER_B' THEN 3
                WHEN 'TIER_C' THEN 4
                ELSE 5
            END
    `).all(cutoff);

    console.log(`\n${'═'.repeat(70)}`);
    console.log(`🎭 AI 叙事等级绩效分析`);
    console.log(`${'═'.repeat(70)}\n`);

    if (byTier.length === 0) {
        console.log('无足够数据（需要 ai_narrative_tier 字段）');
        return { byTier: [], monotonic: null, correlation: null };
    }

    for (const row of byTier) {
        const emoji = row.ai_narrative_tier === 'TIER_S' ? '🏆' :
                     row.ai_narrative_tier === 'TIER_A' ? '🥇' :
                     row.ai_narrative_tier === 'TIER_B' ? '🥈' : '🥉';

        console.log(`${emoji} ${row.ai_narrative_tier}: ${row.trades} 笔, 胜率 ${row.win_rate?.toFixed(1) || 0}%, 平均 ${row.avg_pnl?.toFixed(1) || 0}%, AI分 ${row.avg_score?.toFixed(0) || 'N/A'}`);
    }

    // 计算叙事等级与收益的单调性和相关性
    const tierOrder = ['TIER_S', 'TIER_A', 'TIER_B', 'TIER_C'];
    const orderedData = tierOrder
        .map(t => byTier.find(r => r.ai_narrative_tier === t))
        .filter(Boolean);

    const orderedPnl = orderedData.map(d => d.avg_pnl || 0);

    // 检查单调性：TIER_S > TIER_A > TIER_B > TIER_C
    let monotonic = true;
    for (let i = 1; i < orderedPnl.length; i++) {
        if (orderedPnl[i] > orderedPnl[i-1]) {
            monotonic = false;
            break;
        }
    }

    // 计算相关系数
    const tierScore = { 'TIER_S': 4, 'TIER_A': 3, 'TIER_B': 2, 'TIER_C': 1 };
    const points = orderedData.map(d => ({
        x: tierScore[d.ai_narrative_tier],
        y: d.avg_pnl || 0
    }));
    const correlation = calculateCorrelation(points);

    console.log(`\n叙事等级与收益单调性: ${monotonic ? '✅ 符合预期 (高等级 = 高收益)' : '❌ 不符合预期'}`);
    console.log(`叙事等级与收益相关性: ${correlation.toFixed(2)} ${correlation > 0.3 ? '✅ 正相关' : correlation < -0.3 ? '❌ 负相关' : '⚠️ 弱相关'}`);

    return { byTier, monotonic, correlation };
}

/**
 * 分析拒绝信号的防守价值
 */
function analyzeRejectedSignals(windowDays = 7) {
    const cutoff = new Date(Date.now() - windowDays * 24 * 60 * 60 * 1000).toISOString();

    // 检查表是否存在
    const tableExists = db.prepare(`
        SELECT name FROM sqlite_master WHERE type='table' AND name='rejected_signals'
    `).get();

    if (!tableExists) {
        console.log(`\n${'═'.repeat(70)}`);
        console.log(`🛡️ 拒绝信号追踪分析 (防守价值)`);
        console.log(`${'═'.repeat(70)}\n`);
        console.log('rejected_signals 表不存在，请先运行迁移脚本');
        return null;
    }

    const byStage = db.prepare(`
        SELECT
            rejection_stage,
            COUNT(*) as total,
            AVG(would_have_profit) as avg_avoided_pnl,
            SUM(CASE WHEN would_have_profit < -20 THEN 1 ELSE 0 END) as dodged_bullets,
            SUM(CASE WHEN would_have_profit > 50 THEN 1 ELSE 0 END) as missed_gains,
            SUM(CASE WHEN would_have_profit < 0 THEN 1 ELSE 0 END) as correct_rejections
        FROM rejected_signals
        WHERE created_at >= ? AND tracking_completed = 1
        GROUP BY rejection_stage
    `).all(cutoff);

    console.log(`\n${'═'.repeat(70)}`);
    console.log(`🛡️ 拒绝信号追踪分析 (防守价值)`);
    console.log(`${'═'.repeat(70)}\n`);

    if (byStage.length === 0) {
        console.log('无追踪数据（需要运行 track-rejected-signals.js 追踪）');
        return null;
    }

    for (const row of byStage) {
        const accuracy = row.total > 0 ? (row.correct_rejections / row.total * 100) : 0;
        const emoji = accuracy >= 60 ? '✅' : accuracy >= 40 ? '⚠️' : '❌';

        console.log(`${emoji} ${row.rejection_stage}`);
        console.log(`   拒绝: ${row.total} 笔 | 拒绝准确率: ${accuracy.toFixed(1)}%`);
        console.log(`   平均假设收益: ${row.avg_avoided_pnl?.toFixed(1) || 0}%`);
        console.log(`   🛡️ 躲过大跌(>20%): ${row.dodged_bullets} | ❌ 错过大涨(>50%): ${row.missed_gains}`);
    }

    return byStage;
}

/**
 * 分析 AI 退出使用情况
 */
function analyzeAIExitUsage(windowDays = 7) {
    const cutoff = new Date(Date.now() - windowDays * 24 * 60 * 60 * 1000).toISOString();

    const withAI = db.prepare(`
        SELECT
            COUNT(*) as trades,
            AVG(exit_pnl_percent) as avg_pnl,
            SUM(CASE WHEN exit_pnl_percent >= 50 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as win_rate
        FROM positions
        WHERE status = 'closed' AND exit_time >= ? AND ai_exit_used = 1
    `).get(cutoff);

    const withoutAI = db.prepare(`
        SELECT
            COUNT(*) as trades,
            AVG(exit_pnl_percent) as avg_pnl,
            SUM(CASE WHEN exit_pnl_percent >= 50 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as win_rate
        FROM positions
        WHERE status = 'closed' AND exit_time >= ? AND (ai_exit_used = 0 OR ai_exit_used IS NULL)
    `).get(cutoff);

    console.log(`\n${'═'.repeat(70)}`);
    console.log(`🤖 AI 退出使用分析`);
    console.log(`${'═'.repeat(70)}\n`);

    if ((withAI.trades || 0) + (withoutAI.trades || 0) === 0) {
        console.log('无足够数据');
        return null;
    }

    console.log(`使用 AI 退出: ${withAI.trades || 0} 笔, 胜率 ${withAI.win_rate?.toFixed(1) || 0}%, 平均 ${withAI.avg_pnl?.toFixed(1) || 0}%`);
    console.log(`未用 AI 退出: ${withoutAI.trades || 0} 笔, 胜率 ${withoutAI.win_rate?.toFixed(1) || 0}%, 平均 ${withoutAI.avg_pnl?.toFixed(1) || 0}%`);

    if (withAI.trades >= 5 && withoutAI.trades >= 5) {
        const diff = (withAI.avg_pnl || 0) - (withoutAI.avg_pnl || 0);
        const emoji = diff > 5 ? '✅' : diff < -5 ? '❌' : '⚠️';
        console.log(`\n${emoji} AI 退出效果差异: ${diff > 0 ? '+' : ''}${diff.toFixed(1)}%`);
    }

    return { withAI, withoutAI };
}

/**
 * 生成优化建议
 */
function generateRecommendations(sourceAnalysis, narrativeAnalysis, rejectedAnalysis, aiExitAnalysis) {
    const recommendations = [];

    // 基于来源分析
    if (sourceAnalysis && sourceAnalysis.bySource) {
        for (const source of sourceAnalysis.bySource) {
            if (source.trades < 10) continue; // 样本不足

            if (source.avg_pnl < -10 && source.win_rate < 30) {
                recommendations.push({
                    priority: 'HIGH',
                    module: source.entry_source || 'unknown',
                    action: 'DISABLE_OR_RESTRICT',
                    reason: `胜率 ${source.win_rate?.toFixed(1)}%, 平均亏损 ${source.avg_pnl?.toFixed(1)}%`
                });
            } else if (source.avg_pnl > (sourceAnalysis.baseline.avg_pnl || 0) * 1.5 && source.avg_pnl > 10) {
                recommendations.push({
                    priority: 'INFO',
                    module: source.entry_source || 'unknown',
                    action: 'INCREASE_WEIGHT',
                    reason: `表现优于基准 50%+, 平均收益 ${source.avg_pnl?.toFixed(1)}%`
                });
            }
        }
    }

    // 基于叙事分析
    if (narrativeAnalysis) {
        if (narrativeAnalysis.monotonic === false && narrativeAnalysis.correlation !== null && narrativeAnalysis.correlation < 0.2) {
            recommendations.push({
                priority: 'MEDIUM',
                module: 'ai_narrative',
                action: 'REVIEW_SCORING',
                reason: `叙事等级与收益不呈正相关 (r=${narrativeAnalysis.correlation?.toFixed(2)})，评分逻辑可能无效`
            });
        } else if (narrativeAnalysis.monotonic === true && narrativeAnalysis.correlation > 0.5) {
            recommendations.push({
                priority: 'INFO',
                module: 'ai_narrative',
                action: 'INCREASE_WEIGHT',
                reason: `叙事评分有效 (r=${narrativeAnalysis.correlation?.toFixed(2)})，可考虑提高权重`
            });
        }
    }

    // 基于 AI 退出分析
    if (aiExitAnalysis && aiExitAnalysis.withAI && aiExitAnalysis.withoutAI) {
        const diff = (aiExitAnalysis.withAI.avg_pnl || 0) - (aiExitAnalysis.withoutAI.avg_pnl || 0);
        if (aiExitAnalysis.withAI.trades >= 10 && diff < -10) {
            recommendations.push({
                priority: 'MEDIUM',
                module: 'smart_exit_ai',
                action: 'REVIEW_OR_DISABLE',
                reason: `AI 退出表现比非 AI 差 ${Math.abs(diff).toFixed(1)}%`
            });
        }
    }

    console.log(`\n${'═'.repeat(70)}`);
    console.log(`💡 优化建议`);
    console.log(`${'═'.repeat(70)}\n`);

    if (recommendations.length === 0) {
        console.log('目前无明确建议，请累积更多数据');
    } else {
        for (const rec of recommendations) {
            const emoji = rec.priority === 'HIGH' ? '🔴' : rec.priority === 'MEDIUM' ? '🟡' : '🟢';
            console.log(`${emoji} [${rec.priority}] ${rec.module}`);
            console.log(`   动作: ${rec.action}`);
            console.log(`   原因: ${rec.reason}\n`);
        }
    }

    return recommendations;
}

/**
 * 保存绩效快照到 module_performance 表
 */
function savePerformanceSnapshot(analysis, windowDays) {
    // 检查表是否存在
    const tableExists = db.prepare(`
        SELECT name FROM sqlite_master WHERE type='table' AND name='module_performance'
    `).get();

    if (!tableExists) {
        console.log('\n⚠️ module_performance 表不存在，跳过快照保存');
        return;
    }

    const snapshotDate = new Date().toISOString().split('T')[0];
    let savedCount = 0;

    for (const source of analysis.bySource) {
        try {
            db.prepare(`
                INSERT OR REPLACE INTO module_performance (
                    snapshot_date, window_days, module_name,
                    total_trades, win_count, win_rate,
                    total_pnl, avg_pnl, avg_win, avg_loss, profit_factor,
                    baseline_win_rate, baseline_avg_pnl, relative_performance,
                    sample_size_sufficient
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `).run(
                snapshotDate,
                windowDays,
                source.entry_source || 'unknown',
                source.trades,
                source.wins,
                source.win_rate,
                source.total_pnl,
                source.avg_pnl,
                source.avg_win || 0,
                source.avg_loss || 0,
                source.avg_loss && source.avg_loss !== 0 ?
                    Math.abs(source.avg_win || 0) / Math.abs(source.avg_loss) : 0,
                analysis.baseline.win_rate,
                analysis.baseline.avg_pnl,
                analysis.baseline.avg_pnl && analysis.baseline.avg_pnl !== 0 ?
                    (source.avg_pnl - analysis.baseline.avg_pnl) / Math.abs(analysis.baseline.avg_pnl) * 100 : 0,
                source.trades >= 10 ? 1 : 0
            );
            savedCount++;
        } catch (e) {
            // 忽略错误
        }
    }

    console.log(`\n📁 已保存 ${savedCount} 条绩效快照到 module_performance 表`);
}

/**
 * 计算相关系数
 */
function calculateCorrelation(points) {
    if (points.length < 2) return 0;

    const n = points.length;
    const sumX = points.reduce((a, p) => a + p.x, 0);
    const sumY = points.reduce((a, p) => a + p.y, 0);
    const sumXY = points.reduce((a, p) => a + p.x * p.y, 0);
    const sumX2 = points.reduce((a, p) => a + p.x * p.x, 0);
    const sumY2 = points.reduce((a, p) => a + p.y * p.y, 0);

    const numerator = n * sumXY - sumX * sumY;
    const denominator = Math.sqrt((n * sumX2 - sumX * sumX) * (n * sumY2 - sumY * sumY));

    return denominator === 0 ? 0 : numerator / denominator;
}

/**
 * 主函数
 */
async function main() {
    const windowDays = parseInt(process.argv[2]) || 7;

    console.log(`\n${'█'.repeat(70)}`);
    console.log(`█  v7.3 模块绩效分析报告`);
    console.log(`█  分析窗口: ${windowDays} 天`);
    console.log(`█  生成时间: ${new Date().toISOString()}`);
    console.log(`${'█'.repeat(70)}`);

    const sourceAnalysis = analyzeBySource(windowDays);
    const narrativeAnalysis = analyzeByNarrativeTier(windowDays);
    const rejectedAnalysis = analyzeRejectedSignals(windowDays);
    const aiExitAnalysis = analyzeAIExitUsage(windowDays);

    generateRecommendations(sourceAnalysis, narrativeAnalysis, rejectedAnalysis, aiExitAnalysis);

    // 保存快照
    if (sourceAnalysis.bySource.length > 0) {
        savePerformanceSnapshot(sourceAnalysis, windowDays);
    }

    console.log(`\n${'═'.repeat(70)}`);
    console.log(`报告完成`);
    console.log(`${'═'.repeat(70)}\n`);
}

main().catch(err => {
    console.error('分析脚本执行失败:', err.message);
    process.exit(1);
});
