/**
 * Self-Iteration Manager v7.3
 *
 * 系统自迭代管理器 - 自动分析、优化和调整系统参数
 *
 * 核心功能：
 * 1. 滚动窗口胜率追踪 (7天/30天)
 * 2. 信号源表现排名与权重调整
 * 3. 猎人类型表现与权重调整
 * 4. 阈值自动优化
 * 5. 周期性报告生成
 * 6. v7.3 模块考核与自动调权
 * 7. v7.3 AI 叙事有效性评估
 * 8. v7.3 模块自动禁用机制
 */

import { EventEmitter } from 'events';
import Database from 'better-sqlite3';

export class SelfIterationManager extends EventEmitter {
    constructor(db, options = {}) {
        super();

        this.db = typeof db === 'string' ? new Database(db) : db;

        this.config = {
            // 滚动窗口配置
            windows: {
                short: 7,    // 7天短期窗口
                long: 30     // 30天长期窗口
            },

            // 最小样本量
            minSamples: {
                forAdjustment: 10,    // 至少10笔交易才开始调整
                forConclusion: 30     // 至少30笔才下结论
            },

            // 权重调整参数
            weightAdjustment: {
                minWeight: 0.3,       // 最低权重
                maxWeight: 1.8,       // 最高权重
                stepUp: 0.1,          // 上调步长
                stepDown: 0.15,       // 下调步长 (惩罚更重)
                winRateThresholdUp: 0.50,    // 胜率>50% 上调
                winRateThresholdDown: 0.30   // 胜率<30% 下调
            },

            // 迭代周期
            iterationIntervalHours: options.iterationIntervalHours || 6,

            // 报告配置
            generateReports: options.generateReports !== false
        };

        // 动态权重存储
        this.dynamicWeights = {
            // 信号源权重
            sources: {
                debot: 1.0,
                shadow_v2: 1.0,
                ultra_sniper: 1.0,
                flash_scout: 0.9,
                telegram: 0.7,
                alpha_monitor: 0.8
            },

            // 猎人类型权重
            hunters: {
                FOX: 1.0,
                TURTLE: 1.0,
                WOLF: 0.9,
                EAGLE: 0.8
            },

            // 狗类型权重
            dogTypes: {
                GOLDEN: 1.2,
                SILVER: 1.0,
                BRONZE: 0.8,
                IRON: 0.6
            }
        };

        this.initDatabase();

        // v7.3 模块考核配置
        this.moduleEvaluationConfig = {
            // 最低贡献线
            minContribution: {
                ultra_fast_track: { minTrades: 10, minWinRate: 35, minAvgPnl: -5 },
                flash_scout: { minTrades: 5, minWinRate: 30, minAvgPnl: -10 },
                waiting_room: { minTrades: 20, minWinRate: 30, minAvgPnl: -5 },
                shadow_v2: { minTrades: 5, minWinRate: 35, minAvgPnl: -5 },
                debot: { minTrades: 5, minWinRate: 30, minAvgPnl: -10 },
                alpha_monitor: { minTrades: 5, minWinRate: 30, minAvgPnl: -10 }
            },

            // 达标奖励阈值
            bonusThreshold: { winRate: 50, avgPnl: 20 },

            // 调整幅度
            penaltyStep: 0.2,  // 未达标降权20%
            bonusStep: 0.1,    // 超标升权10%

            // 危机阈值
            criticalWeight: 0.3,  // 权重低于此值触发危机
            criticalCountForDisable: 3  // 连续3次危机自动禁用
        };

        // v7.3 模块危机计数器
        this.moduleCriticalCount = {};

        console.log('[SelfIteration] 🔄 Self-Iteration Manager v7.3 initialized');
        console.log(`   迭代周期: 每 ${this.config.iterationIntervalHours} 小时`);
        console.log(`   窗口: 短期 ${this.config.windows.short}天, 长期 ${this.config.windows.long}天`);
    }

    /**
     * 初始化自迭代相关数据库表
     */
    initDatabase() {
        this.db.exec(`
            -- 迭代历史记录表
            CREATE TABLE IF NOT EXISTS iteration_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                iteration_type TEXT NOT NULL,
                target TEXT NOT NULL,
                old_value REAL,
                new_value REAL,
                reason TEXT,
                metrics_before TEXT,
                metrics_after TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );

            -- 周期性快照表
            CREATE TABLE IF NOT EXISTS performance_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_date TEXT NOT NULL,
                window_days INTEGER NOT NULL,

                -- 总体表现
                total_trades INTEGER,
                win_count INTEGER,
                loss_count INTEGER,
                win_rate REAL,
                total_pnl REAL,
                avg_pnl REAL,

                -- 分类表现 (JSON)
                source_performance TEXT,
                hunter_performance TEXT,
                dog_type_performance TEXT,

                -- 最佳/最差
                best_trade TEXT,
                worst_trade TEXT,

                created_at TEXT DEFAULT (datetime('now')),
                UNIQUE(snapshot_date, window_days)
            );

            CREATE INDEX IF NOT EXISTS idx_iteration_history_type ON iteration_history(iteration_type);
            CREATE INDEX IF NOT EXISTS idx_perf_snapshots_date ON performance_snapshots(snapshot_date);
        `);
    }

    /**
     * 计算滚动窗口表现
     * @param {number} windowDays - 窗口天数
     * @returns {Object} 表现统计
     */
    calculateWindowPerformance(windowDays) {
        const cutoffDate = new Date(Date.now() - windowDays * 24 * 60 * 60 * 1000).toISOString();

        // 总体表现
        const overall = this.db.prepare(`
            SELECT
                COUNT(*) as total_trades,
                SUM(CASE WHEN exit_pnl_percent >= 50 THEN 1 ELSE 0 END) as win_count,
                SUM(CASE WHEN exit_pnl_percent < 0 THEN 1 ELSE 0 END) as loss_count,
                AVG(exit_pnl_percent) as avg_pnl,
                SUM(exit_pnl_percent) as total_pnl,
                MAX(exit_pnl_percent) as best_pnl,
                MIN(exit_pnl_percent) as worst_pnl
            FROM positions
            WHERE status = 'closed'
            AND exit_time >= ?
        `).get(cutoffDate);

        overall.win_rate = overall.total_trades > 0
            ? (overall.win_count / overall.total_trades * 100).toFixed(1)
            : 0;

        // 按信号源统计
        const sourcePerformance = {};
        try {
            // v7.4 使用新的 signal_source 字段，而非基于 symbol 推断
            const sourceStats = this.db.prepare(`
                SELECT
                    COALESCE(
                        signal_source,
                        CASE
                            WHEN fast_track_type IS NOT NULL THEN 'flash_scout'
                            WHEN entry_source = 'ultra_sniper' OR entry_source = 'ultra_fast_track' THEN 'ultra_sniper_v2'
                            WHEN entry_source = 'shadow_v2' OR entry_source = 'shadow_protocol' THEN 'shadow_v2'
                            WHEN entry_source = 'debot' OR entry_source = 'debot_scout' THEN 'debot'
                            WHEN signal_id IS NOT NULL THEN 'telegram'
                            ELSE 'tiered_observer'
                        END
                    ) as source,
                    COUNT(*) as trades,
                    SUM(CASE WHEN pnl_percent >= 50 THEN 1 ELSE 0 END) as wins,
                    AVG(pnl_percent) as avg_pnl,
                    -- v7.4 额外统计
                    signal_hunter_type,
                    signal_route,
                    signal_confidence
                FROM positions
                WHERE status = 'closed'
                AND exit_time >= ?
                GROUP BY source
            `).all(cutoffDate);

            for (const row of sourceStats) {
                sourcePerformance[row.source] = {
                    trades: row.trades,
                    wins: row.wins,
                    winRate: row.trades > 0 ? (row.wins / row.trades * 100).toFixed(1) : 0,
                    avgPnl: row.avg_pnl?.toFixed(1) || 0
                };
            }
        } catch (e) {
            console.log(`[SelfIteration] Source stats query failed: ${e.message}`);
        }

        // 按猎人类型统计 (v7.4 使用新的 signal_hunter_type 字段)
        const hunterPerformance = {};
        try {
            // 首先尝试从 positions 表的新字段获取
            const hunterStats = this.db.prepare(`
                SELECT
                    signal_hunter_type as hunter_type,
                    COUNT(*) as trades,
                    SUM(CASE WHEN pnl_percent >= 50 THEN 1 ELSE 0 END) as wins,
                    AVG(pnl_percent) as avg_pnl
                FROM positions
                WHERE status = 'closed'
                AND exit_time >= ?
                AND signal_hunter_type IS NOT NULL
                GROUP BY signal_hunter_type
            `).all(cutoffDate);

            for (const row of hunterStats) {
                if (row.hunter_type) {
                    hunterPerformance[row.hunter_type] = {
                        trades: row.trades,
                        wins: row.wins,
                        winRate: row.trades > 0 ? (row.wins / row.trades * 100).toFixed(1) : 0,
                        avgPnl: row.avg_pnl?.toFixed(1) || 0
                    };
                }
            }

            // 如果没有数据，回退到 factor_performance 表
            if (Object.keys(hunterPerformance).length === 0) {
                const fallbackStats = this.db.prepare(`
                    SELECT
                        dog_type as hunter_type,
                        COUNT(*) as trades,
                        SUM(is_winner) as wins,
                        AVG(pnl_percent) as avg_pnl
                    FROM factor_performance
                    WHERE closed_at IS NOT NULL
                    AND closed_at >= ?
                    GROUP BY dog_type
                `).all(cutoffDate);

                for (const row of fallbackStats) {
                    if (row.hunter_type) {
                        hunterPerformance[row.hunter_type] = {
                            trades: row.trades,
                            wins: row.wins,
                            winRate: row.trades > 0 ? (row.wins / row.trades * 100).toFixed(1) : 0,
                            avgPnl: row.avg_pnl?.toFixed(1) || 0
                        };
                    }
                }
            }
        } catch (e) {
            // factor_performance 表可能不存在
        }

        return {
            windowDays,
            overall,
            sourcePerformance,
            hunterPerformance,
            timestamp: new Date().toISOString()
        };
    }

    /**
     * 执行自迭代优化
     * @returns {Object} 迭代结果
     */
    async iterate() {
        console.log('\n[SelfIteration] 🔄 开始自迭代优化周期...');

        const results = {
            timestamp: new Date().toISOString(),
            adjustments: [],
            recommendations: [],
            performance: {}
        };

        // 1. 计算短期和长期表现
        const shortTermPerf = this.calculateWindowPerformance(this.config.windows.short);
        const longTermPerf = this.calculateWindowPerformance(this.config.windows.long);

        results.performance = {
            shortTerm: shortTermPerf,
            longTerm: longTermPerf
        };

        console.log(`\n📊 [${this.config.windows.short}天表现]`);
        console.log(`   总交易: ${shortTermPerf.overall.total_trades}`);
        console.log(`   胜率: ${shortTermPerf.overall.win_rate}%`);
        console.log(`   平均PnL: ${shortTermPerf.overall.avg_pnl?.toFixed(1) || 0}%`);

        // 2. 如果样本量足够，执行权重调整
        if (shortTermPerf.overall.total_trades >= this.config.minSamples.forAdjustment) {

            // 调整信号源权重
            const sourceAdj = this.adjustSourceWeights(shortTermPerf.sourcePerformance);
            results.adjustments.push(...sourceAdj);

            // 调整猎人类型权重
            const hunterAdj = this.adjustHunterWeights(shortTermPerf.hunterPerformance);
            results.adjustments.push(...hunterAdj);
        } else {
            console.log(`   ⚠️ 样本不足 (${shortTermPerf.overall.total_trades}/${this.config.minSamples.forAdjustment})，跳过权重调整`);
        }

        // 3. 生成优化建议
        results.recommendations = this.generateRecommendations(shortTermPerf, longTermPerf);

        // 4. 保存快照
        this.savePerformanceSnapshot(shortTermPerf);

        // 5. v7.3 模块考核
        console.log('\n📋 [v7.3 ModuleEval] 执行模块考核...');
        const moduleEvalResults = this.evaluateModules();
        results.moduleEvaluation = moduleEvalResults;

        // 6. v7.3 AI 叙事有效性评估
        console.log('\n🎭 [v7.3 NarrativeEval] 评估 AI 叙事有效性...');
        const narrativeEval = this.evaluateNarrativeEffectiveness();
        results.narrativeEvaluation = narrativeEval;
        if (narrativeEval.effective !== null) {
            console.log(`   结果: ${narrativeEval.effective ? '✅ 有效' : '❌ 无效'}`);
            console.log(`   相关性: ${narrativeEval.correlation}`);
            if (narrativeEval.recommendation) {
                console.log(`   建议: ${narrativeEval.recommendation}`);
            }
        } else {
            console.log(`   结果: 数据不足`);
        }

        // 7. 发出迭代完成事件
        this.emit('iteration-complete', results);

        // 8. 打印优化建议
        if (results.recommendations.length > 0) {
            console.log('\n💡 优化建议:');
            results.recommendations.forEach((r, i) => {
                console.log(`   ${i + 1}. [${r.priority}] ${r.message}`);
            });
        }

        console.log('\n[SelfIteration] ✅ 自迭代周期完成\n');

        return results;
    }

    /**
     * 调整信号源权重
     */
    adjustSourceWeights(sourcePerf) {
        const adjustments = [];
        const cfg = this.config.weightAdjustment;

        for (const [source, stats] of Object.entries(sourcePerf)) {
            if (!this.dynamicWeights.sources[source]) continue;
            if (stats.trades < 5) continue; // 至少5笔交易

            const currentWeight = this.dynamicWeights.sources[source];
            const winRate = parseFloat(stats.winRate) / 100;
            let newWeight = currentWeight;
            let reason = null;

            if (winRate >= cfg.winRateThresholdUp && stats.trades >= 10) {
                // 表现好，上调权重
                newWeight = Math.min(cfg.maxWeight, currentWeight + cfg.stepUp);
                reason = `胜率 ${stats.winRate}% 超过 ${cfg.winRateThresholdUp * 100}%`;
            } else if (winRate <= cfg.winRateThresholdDown && stats.trades >= 10) {
                // 表现差，下调权重
                newWeight = Math.max(cfg.minWeight, currentWeight - cfg.stepDown);
                reason = `胜率 ${stats.winRate}% 低于 ${cfg.winRateThresholdDown * 100}%`;
            }

            if (newWeight !== currentWeight) {
                this.dynamicWeights.sources[source] = newWeight;

                const adj = {
                    type: 'source_weight',
                    target: source,
                    oldValue: currentWeight,
                    newValue: newWeight,
                    reason
                };

                adjustments.push(adj);
                this.recordAdjustment(adj);

                console.log(`   �� ${source} 权重: ${currentWeight.toFixed(2)} → ${newWeight.toFixed(2)} (${reason})`);
            }
        }

        return adjustments;
    }

    /**
     * 调整猎人类型权重
     */
    adjustHunterWeights(hunterPerf) {
        const adjustments = [];
        const cfg = this.config.weightAdjustment;

        for (const [hunterType, stats] of Object.entries(hunterPerf)) {
            if (!this.dynamicWeights.hunters[hunterType]) continue;
            if (stats.trades < 5) continue;

            const currentWeight = this.dynamicWeights.hunters[hunterType];
            const winRate = parseFloat(stats.winRate) / 100;
            let newWeight = currentWeight;
            let reason = null;

            if (winRate >= cfg.winRateThresholdUp) {
                newWeight = Math.min(cfg.maxWeight, currentWeight + cfg.stepUp);
                reason = `${hunterType} 胜率 ${stats.winRate}%`;
            } else if (winRate <= cfg.winRateThresholdDown) {
                newWeight = Math.max(cfg.minWeight, currentWeight - cfg.stepDown);
                reason = `${hunterType} 胜率 ${stats.winRate}% 过低`;
            }

            if (newWeight !== currentWeight) {
                this.dynamicWeights.hunters[hunterType] = newWeight;

                const adj = {
                    type: 'hunter_weight',
                    target: hunterType,
                    oldValue: currentWeight,
                    newValue: newWeight,
                    reason
                };

                adjustments.push(adj);
                this.recordAdjustment(adj);

                console.log(`   🦊 ${hunterType} 权重: ${currentWeight.toFixed(2)} → ${newWeight.toFixed(2)} (${reason})`);
            }
        }

        return adjustments;
    }

    /**
     * 生成优化建议
     */
    generateRecommendations(shortTerm, longTerm) {
        const recommendations = [];

        // 1. 检查整体胜率趋势
        if (shortTerm.overall.win_rate && longTerm.overall.win_rate) {
            const shortRate = parseFloat(shortTerm.overall.win_rate);
            const longRate = parseFloat(longTerm.overall.win_rate);

            if (shortRate < longRate - 10) {
                recommendations.push({
                    priority: 'HIGH',
                    type: 'WIN_RATE_DECLINE',
                    message: `短期胜率(${shortRate}%)低于长期(${longRate}%)，建议检查近期信号源质量`
                });
            }
        }

        // 2. 检查信号源异常
        for (const [source, stats] of Object.entries(shortTerm.sourcePerformance)) {
            const winRate = parseFloat(stats.winRate);

            if (stats.trades >= 10 && winRate < 20) {
                recommendations.push({
                    priority: 'CRITICAL',
                    type: 'SOURCE_UNDERPERFORMING',
                    message: `${source} 信号源胜率仅 ${winRate}%，建议暂停或降低权重`
                });
            }

            if (stats.trades >= 10 && winRate > 60) {
                recommendations.push({
                    priority: 'INFO',
                    type: 'SOURCE_EXCELLING',
                    message: `${source} 信号源表现优异(${winRate}%)，考虑增加权重`
                });
            }
        }

        // 3. 检查资金效率
        if (shortTerm.overall.total_trades > 0 && shortTerm.overall.avg_pnl < 0) {
            recommendations.push({
                priority: 'HIGH',
                type: 'NEGATIVE_EV',
                message: `短期平均收益为负(${shortTerm.overall.avg_pnl?.toFixed(1)}%)，建议收紧入场条件`
            });
        }

        // 4. 检查最大回撤
        if (shortTerm.overall.worst_pnl < -40) {
            recommendations.push({
                priority: 'MEDIUM',
                type: 'LARGE_DRAWDOWN',
                message: `存在大额亏损(${shortTerm.overall.worst_pnl?.toFixed(1)}%)，建议检查止损策略`
            });
        }

        return recommendations;
    }

    /**
     * 记录调整历史
     */
    recordAdjustment(adjustment) {
        try {
            this.db.prepare(`
                INSERT INTO iteration_history (
                    iteration_type, target, old_value, new_value, reason
                ) VALUES (?, ?, ?, ?, ?)
            `).run(
                adjustment.type,
                adjustment.target,
                adjustment.oldValue,
                adjustment.newValue,
                adjustment.reason
            );
        } catch (e) {
            console.error(`[SelfIteration] 记录调整失败: ${e.message}`);
        }
    }

    /**
     * 保存表现快照
     */
    savePerformanceSnapshot(perf) {
        try {
            const snapshotDate = new Date().toISOString().split('T')[0];

            this.db.prepare(`
                INSERT OR REPLACE INTO performance_snapshots (
                    snapshot_date, window_days,
                    total_trades, win_count, loss_count, win_rate,
                    total_pnl, avg_pnl,
                    source_performance, hunter_performance
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `).run(
                snapshotDate,
                perf.windowDays,
                perf.overall.total_trades,
                perf.overall.win_count,
                perf.overall.loss_count,
                parseFloat(perf.overall.win_rate) || 0,
                perf.overall.total_pnl || 0,
                perf.overall.avg_pnl || 0,
                JSON.stringify(perf.sourcePerformance),
                JSON.stringify(perf.hunterPerformance)
            );
        } catch (e) {
            // 忽略重复键错误
        }
    }

    /**
     * 获取信号源权重
     */
    getSourceWeight(source) {
        return this.dynamicWeights.sources[source] || 1.0;
    }

    /**
     * 获取猎人类型权重
     */
    getHunterWeight(hunterType) {
        return this.dynamicWeights.hunters[hunterType] || 1.0;
    }

    /**
     * 获取所有动态权重
     */
    getWeights() {
        return { ...this.dynamicWeights };
    }

    /**
     * 获取迭代历史
     */
    getIterationHistory(limit = 50) {
        return this.db.prepare(`
            SELECT * FROM iteration_history
            ORDER BY created_at DESC
            LIMIT ?
        `).all(limit);
    }

    /**
     * 获取表现趋势
     */
    getPerformanceTrend(days = 30) {
        return this.db.prepare(`
            SELECT * FROM performance_snapshots
            WHERE snapshot_date >= date('now', '-' || ? || ' days')
            ORDER BY snapshot_date ASC
        `).all(days);
    }

    /**
     * 启动自动迭代循环
     */
    start() {
        console.log(`[SelfIteration] 🚀 启动自动迭代 (每 ${this.config.iterationIntervalHours} 小时)`);

        // 立即执行一次
        this.iterate().catch(e => console.error(`[SelfIteration] 迭代失败: ${e.message}`));

        // 定期执行
        this.iterationInterval = setInterval(() => {
            this.iterate().catch(e => console.error(`[SelfIteration] 迭代失败: ${e.message}`));
        }, this.config.iterationIntervalHours * 60 * 60 * 1000);
    }

    /**
     * 停止自动迭代
     */
    stop() {
        if (this.iterationInterval) {
            clearInterval(this.iterationInterval);
            this.iterationInterval = null;
        }
        console.log('[SelfIteration] ⏹️ 自动迭代已停止');
    }

    // ==================== v7.3 模块考核方法 ====================

    /**
     * v7.3 模块考核与自动调权
     * 基于 module_performance 表的数据
     * @returns {Array} 考核结果列表
     */
    evaluateModules() {
        const results = [];
        const cfg = this.moduleEvaluationConfig;

        // 尝试从 module_performance 表获取最新数据
        let recentPerf = [];
        try {
            recentPerf = this.db.prepare(`
                SELECT * FROM module_performance
                WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM module_performance)
                AND window_days = 7
                AND sample_size_sufficient = 1
            `).all();
        } catch (e) {
            // module_performance 表可能不存在
            console.log('   ⚠️ module_performance 表不存在或为空');
        }

        // 如果没有数据，尝试使用 positions 表直接计算
        if (recentPerf.length === 0) {
            const cutoff = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
            try {
                recentPerf = this.db.prepare(`
                    SELECT
                        entry_source as module_name,
                        COUNT(*) as total_trades,
                        SUM(CASE WHEN exit_pnl_percent >= 50 THEN 1 ELSE 0 END) as win_count,
                        SUM(CASE WHEN exit_pnl_percent >= 50 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as win_rate,
                        AVG(exit_pnl_percent) as avg_pnl,
                        1 as sample_size_sufficient
                    FROM positions
                    WHERE status = 'closed'
                    AND exit_time >= ?
                    AND entry_source IS NOT NULL
                    GROUP BY entry_source
                    HAVING COUNT(*) >= 5
                `).all(cutoff);
            } catch (e) {
                console.log('   ⚠️ 无法从 positions 表获取模块数据');
                return results;
            }
        }

        if (recentPerf.length === 0) {
            console.log('   ⚠️ 无足够的模块绩效数据进行考核');
            return results;
        }

        for (const perf of recentPerf) {
            const moduleName = perf.module_name;
            const minReq = cfg.minContribution[moduleName];

            if (!minReq) continue;

            let action = 'MAINTAIN';
            let adjustment = 0;
            let reason = '';

            // 检查是否达到最低贡献线
            const meetsMinTrades = !minReq.minTrades || perf.total_trades >= minReq.minTrades;
            const meetsMinWinRate = !minReq.minWinRate || (perf.win_rate || 0) >= minReq.minWinRate;
            const meetsMinAvgPnl = !minReq.minAvgPnl || (perf.avg_pnl || 0) >= minReq.minAvgPnl;
            const meetsMin = meetsMinTrades && meetsMinWinRate && meetsMinAvgPnl;

            if (!meetsMin) {
                action = 'PENALIZE';
                adjustment = -cfg.penaltyStep;
                reason = `未达最低贡献线: 交易${perf.total_trades}, 胜率${(perf.win_rate || 0).toFixed(1)}%, 平均${(perf.avg_pnl || 0).toFixed(1)}%`;
            } else if ((perf.win_rate || 0) >= cfg.bonusThreshold.winRate ||
                       (perf.avg_pnl || 0) >= cfg.bonusThreshold.avgPnl) {
                action = 'BONUS';
                adjustment = cfg.bonusStep;
                reason = `超额表现: 胜率${(perf.win_rate || 0).toFixed(1)}%, 平均${(perf.avg_pnl || 0).toFixed(1)}%`;
            }

            results.push({ moduleName, action, adjustment, reason, perf });

            // 执行调整
            if (adjustment !== 0) {
                this.adjustModuleWeight(moduleName, adjustment, reason);
            }
        }

        return results;
    }

    /**
     * v7.3 调整模块权重
     * @param {string} moduleName - 模块名称
     * @param {number} adjustment - 调整幅度
     * @param {string} reason - 调整原因
     */
    adjustModuleWeight(moduleName, adjustment, reason) {
        const currentWeight = this.dynamicWeights.sources[moduleName] || 1.0;
        const newWeight = Math.max(0.1, Math.min(2.0, currentWeight + adjustment));

        this.dynamicWeights.sources[moduleName] = newWeight;

        // 记录调整
        this.recordAdjustment({
            type: 'module_evaluation',
            target: moduleName,
            oldValue: currentWeight,
            newValue: newWeight,
            reason: reason || '自动考核调整'
        });

        const emoji = adjustment > 0 ? '📈' : '📉';
        console.log(`   ${emoji} [ModuleEval] ${moduleName}: ${currentWeight.toFixed(2)} → ${newWeight.toFixed(2)} (${reason})`);

        // 如果权重降到危机阈值以下，发出警告
        if (newWeight <= this.moduleEvaluationConfig.criticalWeight) {
            this.handleModuleCritical(moduleName, newWeight);
        } else {
            // 权重恢复，重置危机计数
            if (this.moduleCriticalCount[moduleName]) {
                this.moduleCriticalCount[moduleName] = 0;
            }
        }
    }

    /**
     * v7.3 处理模块危机
     * @param {string} moduleName - 模块名称
     * @param {number} weight - 当前权重
     */
    handleModuleCritical(moduleName, weight) {
        this.moduleCriticalCount[moduleName] = (this.moduleCriticalCount[moduleName] || 0) + 1;

        console.log(`   ⚠️ [ModuleCritical] ${moduleName} 权重过低 (${weight.toFixed(2)})，危机次数: ${this.moduleCriticalCount[moduleName]}`);

        // 发出模块危机事件
        this.emit('module-critical', { moduleName, weight, criticalCount: this.moduleCriticalCount[moduleName] });

        // 检查是否达到自动禁用阈值
        if (this.moduleCriticalCount[moduleName] >= this.moduleEvaluationConfig.criticalCountForDisable) {
            console.log(`   🔴 [ModuleDisable] ${moduleName} 连续 ${this.moduleCriticalCount[moduleName]} 次危机，触发自动禁用事件`);
            this.emit('module-disable-triggered', { moduleName, weight, reason: '连续未达最低贡献线' });
        }
    }

    /**
     * v7.3 评估 AI 叙事模块的有效性
     * 基于叙事等级与收益的相关性
     * @returns {Object} 评估结果
     */
    evaluateNarrativeEffectiveness() {
        let data = [];
        try {
            // 使用 intention_tier 字段（实际字段名）
            data = this.db.prepare(`
                SELECT
                    intention_tier as ai_narrative_tier,
                    AVG(exit_pnl_percent) as avg_pnl,
                    COUNT(*) as trades
                FROM positions
                WHERE status = 'closed'
                AND intention_tier IS NOT NULL
                AND exit_time >= datetime('now', '-7 days')
                GROUP BY intention_tier
                HAVING COUNT(*) >= 5
            `).all();
        } catch (e) {
            return { effective: null, reason: '查询失败' };
        }

        if (data.length < 2) {
            return { effective: null, reason: '数据不足' };
        }

        // 检查单调性：TIER_S > TIER_A > TIER_B > TIER_C
        const tierOrder = ['TIER_S', 'TIER_A', 'TIER_B', 'TIER_C'];
        const orderedData = tierOrder
            .map(t => data.find(d => d.ai_narrative_tier === t))
            .filter(Boolean);

        const orderedPnl = orderedData.map(d => d.avg_pnl || 0);

        let monotonic = true;
        for (let i = 1; i < orderedPnl.length; i++) {
            if (orderedPnl[i] >= orderedPnl[i-1]) {
                monotonic = false;
                break;
            }
        }

        // 计算简化的相关系数
        const tierScore = { 'TIER_S': 4, 'TIER_A': 3, 'TIER_B': 2, 'TIER_C': 1 };
        const points = orderedData.map(d => ({
            x: tierScore[d.ai_narrative_tier],
            y: d.avg_pnl || 0
        }));
        const correlation = this.calculateCorrelation(points);

        const effective = monotonic && correlation > 0.3;

        return {
            effective,
            monotonic,
            correlation: correlation.toFixed(2),
            data,
            recommendation: effective ?
                '叙事评分有效，保持使用' :
                '叙事评分效果不明显，考虑简化或调整权重'
        };
    }

    /**
     * v7.3 计算相关系数
     * @param {Array} points - 数据点 [{x, y}, ...]
     * @returns {number} 相关系数
     */
    calculateCorrelation(points) {
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
     * v7.3 获取模块健康状态
     * @returns {Object} 模块健康状态
     */
    getModuleHealthStatus() {
        const status = {};

        for (const [moduleName, weight] of Object.entries(this.dynamicWeights.sources)) {
            const criticalCount = this.moduleCriticalCount[moduleName] || 0;

            status[moduleName] = {
                weight,
                criticalCount,
                status: weight <= 0.3 ? 'CRITICAL' :
                       weight <= 0.5 ? 'WARNING' :
                       weight >= 1.5 ? 'EXCELLENT' : 'HEALTHY'
            };
        }

        return status;
    }

    /**
     * v7.3 重置模块危机计数
     * @param {string} moduleName - 模块名称
     */
    resetModuleCritical(moduleName) {
        if (this.moduleCriticalCount[moduleName]) {
            this.moduleCriticalCount[moduleName] = 0;
            console.log(`[SelfIteration] 🔄 ${moduleName} 危机计数已重置`);
        }
    }

    /**
     * v7.3 手动调整模块权重
     * @param {string} moduleName - 模块名称
     * @param {number} weight - 新权重
     */
    setModuleWeight(moduleName, weight) {
        const oldWeight = this.dynamicWeights.sources[moduleName] || 1.0;
        const newWeight = Math.max(0.1, Math.min(2.0, weight));

        this.dynamicWeights.sources[moduleName] = newWeight;

        this.recordAdjustment({
            type: 'manual_adjustment',
            target: moduleName,
            oldValue: oldWeight,
            newValue: newWeight,
            reason: '手动调整'
        });

        console.log(`[SelfIteration] 📝 手动调整 ${moduleName}: ${oldWeight.toFixed(2)} → ${newWeight.toFixed(2)}`);

        // 手动调整后重置危机计数
        this.resetModuleCritical(moduleName);

        return newWeight;
    }
}

export default SelfIterationManager;
