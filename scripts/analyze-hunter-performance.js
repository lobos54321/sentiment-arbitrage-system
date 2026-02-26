#!/usr/bin/env node
/**
 * Hunter Performance Analysis Script v7.4.1
 *
 * 分析各猎人类型的历史表现，验证策略假设
 *
 * 运行方式:
 * node scripts/analyze-hunter-performance.js
 *
 * 分析内容:
 * 1. 各猎人类型 (FOX/TURTLE/WOLF) 的胜率和平均收益
 * 2. 各信号源 (ultra_sniper_v2/shadow_v2/telegram) 的表现
 * 3. 仓位倍数建议验证
 * 4. 最佳入场时机分析
 */

import Database from 'better-sqlite3';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DB_PATH = process.env.DB_PATH || path.join(__dirname, '../data/trading.db');

// ═══════════════════════════════════════════════════════════════
// 主分析函数
// ═══════════════════════════════════════════════════════════════

async function analyzeHunterPerformance() {
    console.log('═══════════════════════════════════════════════════════════════');
    console.log('🎯 Hunter Performance Analysis v7.4.1');
    console.log('═══════════════════════════════════════════════════════════════\n');

    const db = new Database(DB_PATH, { readonly: true });

    try {
        // 1. 总体统计
        analyzeOverall(db);

        // 2. 按猎人类型分析
        analyzeByHunterType(db);

        // 3. 按信号源分析
        analyzeBySignalSource(db);

        // 4. 按信号路由分析
        analyzeBySignalRoute(db);

        // 5. 按数据置信度分析
        analyzeByConfidence(db);

        // 6. FOX 猎人深度分析
        analyzeFoxHunters(db);

        // 7. 仓位倍数验证
        analyzePositionMultipliers(db);

        // 8. 入场时机分析
        analyzeEntryTiming(db);

        // 9. 生成优化建议
        generateRecommendations(db);

    } finally {
        db.close();
    }
}

// ═══════════════════════════════════════════════════════════════
// 分析函数
// ═══════════════════════════════════════════════════════════════

function analyzeOverall(db) {
    console.log('📊 1. 总体统计');
    console.log('─────────────────────────────────────────────────────────');

    const overall = db.prepare(`
        SELECT
            COUNT(*) as total_trades,
            SUM(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN pnl_percent <= 0 THEN 1 ELSE 0 END) as losses,
            AVG(pnl_percent) as avg_pnl,
            SUM(pnl_percent) as total_pnl,
            MAX(pnl_percent) as best_trade,
            MIN(pnl_percent) as worst_trade,
            COUNT(DISTINCT signal_hunter_type) as hunter_types_used,
            COUNT(DISTINCT signal_source) as sources_used
        FROM positions
        WHERE status = 'closed'
    `).get();

    const winRate = overall.total_trades > 0
        ? ((overall.wins / overall.total_trades) * 100).toFixed(1)
        : 0;

    console.log(`   总交易数: ${overall.total_trades}`);
    console.log(`   胜率: ${winRate}% (${overall.wins} 胜 / ${overall.losses} 负)`);
    console.log(`   平均收益: ${overall.avg_pnl?.toFixed(2) || 0}%`);
    console.log(`   累计收益: ${overall.total_pnl?.toFixed(2) || 0}%`);
    console.log(`   最佳交易: +${overall.best_trade?.toFixed(2) || 0}%`);
    console.log(`   最差交易: ${overall.worst_trade?.toFixed(2) || 0}%`);
    console.log(`   使用猎人类型: ${overall.hunter_types_used} 种`);
    console.log(`   使用信号源: ${overall.sources_used} 个\n`);
}

function analyzeByHunterType(db) {
    console.log('🦊 2. 按猎人类型分析');
    console.log('─────────────────────────────────────────────────────────');

    const hunterStats = db.prepare(`
        SELECT
            COALESCE(signal_hunter_type, 'UNKNOWN') as hunter_type,
            COUNT(*) as trades,
            SUM(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END) as wins,
            AVG(pnl_percent) as avg_pnl,
            SUM(pnl_percent) as total_pnl,
            MAX(pnl_percent) as best_trade,
            MIN(pnl_percent) as worst_trade,
            AVG(CASE WHEN pnl_percent > 0 THEN pnl_percent END) as avg_win,
            AVG(CASE WHEN pnl_percent <= 0 THEN pnl_percent END) as avg_loss
        FROM positions
        WHERE status = 'closed'
        GROUP BY signal_hunter_type
        ORDER BY total_pnl DESC
    `).all();

    const typeEmojis = {
        'FOX': '🦊',
        'TURTLE': '🐢',
        'WOLF': '🐺',
        'EAGLE': '🦅',
        'BOT': '🤖',
        'NORMAL': '👤',
        'UNKNOWN': '❓'
    };

    for (const row of hunterStats) {
        const emoji = typeEmojis[row.hunter_type] || '❓';
        const winRate = row.trades > 0 ? ((row.wins / row.trades) * 100).toFixed(1) : 0;

        console.log(`\n   ${emoji} ${row.hunter_type}:`);
        console.log(`      交易数: ${row.trades} | 胜率: ${winRate}%`);
        console.log(`      平均收益: ${row.avg_pnl?.toFixed(2) || 0}% | 累计: ${row.total_pnl?.toFixed(2) || 0}%`);
        console.log(`      平均盈利: +${row.avg_win?.toFixed(2) || 0}% | 平均亏损: ${row.avg_loss?.toFixed(2) || 0}%`);
        console.log(`      最佳: +${row.best_trade?.toFixed(2) || 0}% | 最差: ${row.worst_trade?.toFixed(2) || 0}%`);
    }
    console.log();
}

function analyzeBySignalSource(db) {
    console.log('📡 3. 按信号源分析');
    console.log('─────────────────────────────────────────────────────────');

    const sourceStats = db.prepare(`
        SELECT
            COALESCE(signal_source, 'unknown') as source,
            COUNT(*) as trades,
            SUM(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END) as wins,
            AVG(pnl_percent) as avg_pnl,
            SUM(pnl_percent) as total_pnl
        FROM positions
        WHERE status = 'closed'
        GROUP BY signal_source
        ORDER BY total_pnl DESC
    `).all();

    for (const row of sourceStats) {
        const winRate = row.trades > 0 ? ((row.wins / row.trades) * 100).toFixed(1) : 0;

        console.log(`\n   ${row.source}:`);
        console.log(`      交易数: ${row.trades} | 胜率: ${winRate}%`);
        console.log(`      平均收益: ${row.avg_pnl?.toFixed(2) || 0}% | 累计: ${row.total_pnl?.toFixed(2) || 0}%`);
    }
    console.log();
}

function analyzeBySignalRoute(db) {
    console.log('🛤️  4. 按信号路由分析');
    console.log('─────────────────────────────────────────────────────────');

    const routeStats = db.prepare(`
        SELECT
            COALESCE(signal_route, 'unknown') as route,
            COUNT(*) as trades,
            SUM(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END) as wins,
            AVG(pnl_percent) as avg_pnl,
            SUM(pnl_percent) as total_pnl
        FROM positions
        WHERE status = 'closed'
        GROUP BY signal_route
        ORDER BY total_pnl DESC
    `).all();

    for (const row of routeStats) {
        const winRate = row.trades > 0 ? ((row.wins / row.trades) * 100).toFixed(1) : 0;

        console.log(`\n   ${row.route}:`);
        console.log(`      交易数: ${row.trades} | 胜率: ${winRate}%`);
        console.log(`      平均收益: ${row.avg_pnl?.toFixed(2) || 0}% | 累计: ${row.total_pnl?.toFixed(2) || 0}%`);
    }
    console.log();
}

function analyzeByConfidence(db) {
    console.log('🎯 5. 按数据置信度分析');
    console.log('─────────────────────────────────────────────────────────');

    const confStats = db.prepare(`
        SELECT
            COALESCE(signal_confidence, 'unknown') as confidence,
            COUNT(*) as trades,
            SUM(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END) as wins,
            AVG(pnl_percent) as avg_pnl
        FROM positions
        WHERE status = 'closed'
        GROUP BY signal_confidence
        ORDER BY trades DESC
    `).all();

    for (const row of confStats) {
        const winRate = row.trades > 0 ? ((row.wins / row.trades) * 100).toFixed(1) : 0;
        const emoji = row.confidence === 'direct' ? '✅' : row.confidence === 'inferred' ? '🔍' : '❓';

        console.log(`   ${emoji} ${row.confidence}: ${row.trades} 交易 | 胜率 ${winRate}% | 平均 ${row.avg_pnl?.toFixed(2) || 0}%`);
    }
    console.log();
}

function analyzeFoxHunters(db) {
    console.log('🦊 6. FOX 猎人深度分析');
    console.log('─────────────────────────────────────────────────────────');

    // FOX 猎人的详细表现
    const foxStats = db.prepare(`
        SELECT
            signal_hunter_addr as hunter_addr,
            COUNT(*) as trades,
            SUM(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END) as wins,
            AVG(pnl_percent) as avg_pnl,
            SUM(pnl_percent) as total_pnl,
            AVG(signal_hunter_score) as avg_score
        FROM positions
        WHERE status = 'closed'
        AND signal_hunter_type = 'FOX'
        AND signal_hunter_addr IS NOT NULL
        GROUP BY signal_hunter_addr
        HAVING COUNT(*) >= 2
        ORDER BY total_pnl DESC
        LIMIT 10
    `).all();

    if (foxStats.length === 0) {
        console.log('   暂无足够的 FOX 猎人数据\n');
        return;
    }

    console.log('   Top 10 FOX 猎人表现:\n');

    for (let i = 0; i < foxStats.length; i++) {
        const row = foxStats[i];
        const winRate = row.trades > 0 ? ((row.wins / row.trades) * 100).toFixed(1) : 0;
        const addrShort = row.hunter_addr?.slice(0, 8) + '...' || 'Unknown';

        console.log(`   ${i + 1}. ${addrShort}`);
        console.log(`      交易: ${row.trades} | 胜率: ${winRate}% | 平均: ${row.avg_pnl?.toFixed(2) || 0}% | 累计: ${row.total_pnl?.toFixed(2) || 0}%`);
        console.log(`      平均评分: ${row.avg_score?.toFixed(0) || 'N/A'}`);
    }
    console.log();
}

function analyzePositionMultipliers(db) {
    console.log('💰 7. 仓位倍数验证');
    console.log('─────────────────────────────────────────────────────────');

    // 理论: FOX 1.2x, TURTLE 1.5x, WOLF 1.0x
    // 验证: 如果 FOX 的风险调整收益最高，1.2x 合理

    const riskAdjusted = db.prepare(`
        SELECT
            signal_hunter_type as type,
            AVG(pnl_percent) as avg_pnl,
            COUNT(*) as trades,
            -- 计算夏普比率近似值 (avg / stddev)
            AVG(pnl_percent) / (
                CASE
                    WHEN COUNT(*) > 1 THEN
                        SQRT(SUM((pnl_percent - (SELECT AVG(pnl_percent) FROM positions WHERE status='closed' AND signal_hunter_type = p.signal_hunter_type)) *
                             (pnl_percent - (SELECT AVG(pnl_percent) FROM positions WHERE status='closed' AND signal_hunter_type = p.signal_hunter_type))) / (COUNT(*) - 1))
                    ELSE 1
                END
            ) as sharpe_approx
        FROM positions p
        WHERE status = 'closed'
        AND signal_hunter_type IS NOT NULL
        GROUP BY signal_hunter_type
        HAVING COUNT(*) >= 5
        ORDER BY sharpe_approx DESC
    `).all();

    const multipliers = {
        'FOX': 1.2,
        'TURTLE': 1.5,
        'WOLF': 1.0,
        'EAGLE': 0.5,
        'NORMAL': 0.3
    };

    console.log('   理论仓位倍数 vs 实际风险调整收益:\n');

    for (const row of riskAdjusted) {
        const mult = multipliers[row.type] || 1.0;
        const adjPnl = (row.avg_pnl || 0) * mult;

        console.log(`   ${row.type}:`);
        console.log(`      理论倍数: ${mult}x`);
        console.log(`      原始平均收益: ${row.avg_pnl?.toFixed(2) || 0}%`);
        console.log(`      倍数调整后: ${adjPnl.toFixed(2)}%`);
        console.log(`      夏普近似: ${row.sharpe_approx?.toFixed(3) || 'N/A'}`);
    }
    console.log();
}

function analyzeEntryTiming(db) {
    console.log('⏰ 8. 入场时机分析');
    console.log('─────────────────────────────────────────────────────────');

    // 按入场时的评分分析表现
    const timingStats = db.prepare(`
        SELECT
            CASE
                WHEN signal_hunter_score >= 80 THEN 'score_80+'
                WHEN signal_hunter_score >= 70 THEN 'score_70-79'
                WHEN signal_hunter_score >= 60 THEN 'score_60-69'
                WHEN signal_hunter_score >= 50 THEN 'score_50-59'
                ELSE 'score_<50'
            END as score_bucket,
            COUNT(*) as trades,
            SUM(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END) as wins,
            AVG(pnl_percent) as avg_pnl
        FROM positions
        WHERE status = 'closed'
        AND signal_hunter_score IS NOT NULL
        GROUP BY score_bucket
        ORDER BY score_bucket DESC
    `).all();

    console.log('   按猎人评分入场表现:\n');

    for (const row of timingStats) {
        const winRate = row.trades > 0 ? ((row.wins / row.trades) * 100).toFixed(1) : 0;

        console.log(`   ${row.score_bucket}: ${row.trades} 交易 | 胜率 ${winRate}% | 平均 ${row.avg_pnl?.toFixed(2) || 0}%`);
    }
    console.log();
}

function generateRecommendations(db) {
    console.log('═══════════════════════════════════════════════════════════════');
    console.log('📋 优化建议');
    console.log('═══════════════════════════════════════════════════════════════\n');

    // 获取各类型表现
    const typePerformance = db.prepare(`
        SELECT
            signal_hunter_type as type,
            COUNT(*) as trades,
            AVG(pnl_percent) as avg_pnl,
            SUM(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as win_rate
        FROM positions
        WHERE status = 'closed'
        AND signal_hunter_type IS NOT NULL
        GROUP BY signal_hunter_type
        HAVING COUNT(*) >= 3
    `).all();

    const recommendations = [];

    for (const row of typePerformance) {
        if (row.avg_pnl > 10 && row.win_rate > 55) {
            recommendations.push(`✅ ${row.type} 表现优秀 (平均 +${row.avg_pnl.toFixed(1)}%, 胜率 ${row.win_rate.toFixed(0)}%)，建议保持当前仓位倍数`);
        } else if (row.avg_pnl < -5) {
            recommendations.push(`⚠️ ${row.type} 表现不佳 (平均 ${row.avg_pnl.toFixed(1)}%)，建议降低仓位倍数或排除`);
        } else if (row.win_rate < 40) {
            recommendations.push(`⚠️ ${row.type} 胜率偏低 (${row.win_rate.toFixed(0)}%)，建议提高入场门槛`);
        }
    }

    // 数据量建议
    const totalWithLineage = db.prepare(`
        SELECT COUNT(*) as count FROM positions WHERE signal_source IS NOT NULL AND status = 'closed'
    `).get();

    const totalClosed = db.prepare(`
        SELECT COUNT(*) as count FROM positions WHERE status = 'closed'
    `).get();

    const lineageRate = totalClosed.count > 0
        ? (totalWithLineage.count / totalClosed.count * 100).toFixed(1)
        : 0;

    if (lineageRate < 50) {
        recommendations.push(`📊 信号血统覆盖率: ${lineageRate}%，建议运行 repair-signal-lineage.js 修复历史数据`);
    } else {
        recommendations.push(`📊 信号血统覆盖率: ${lineageRate}%，数据质量良好`);
    }

    if (recommendations.length === 0) {
        console.log('   数据量不足，需要更多交易记录才能生成有效建议\n');
    } else {
        for (const rec of recommendations) {
            console.log(`   ${rec}`);
        }
        console.log();
    }

    console.log('═══════════════════════════════════════════════════════════════');
    console.log('分析完成');
    console.log('═══════════════════════════════════════════════════════════════\n');
}

// ═══════════════════════════════════════════════════════════════
// 运行
// ═══════════════════════════════════════════════════════════════

analyzeHunterPerformance().catch(err => {
    console.error('分析失败:', err.message);
    process.exit(1);
});
