/**
 * 自动调参模块 v1.0
 * 
 * 功能：
 * 1. 记录每笔交易的因子值
 * 2. 计算因子与结果的相关性
 * 3. 自动调整阈值以优化表现
 * 4. A/B 测试框架
 */

import Database from 'better-sqlite3';
import { EventEmitter } from 'events';

class AutoTuner extends EventEmitter {
    constructor(dbPath = './data/sentiment_arb.db') {
        super();
        this.db = new Database(dbPath);
        this.initDatabase();

        // 当前阈值配置 (可被自动调整)
        this.thresholds = {
            smDensity: { high: 5, mid: 1 },
            hypeDivergence: { stealth: 1.0, momentum: 0.2 },
            narrativeHealth: { strong: 10, mid: 5 }
        };

        // 因子性能统计
        this.factorStats = {
            smDensity: { highWinRate: 0, midWinRate: 0, lowWinRate: 0 },
            hypeDivergence: { stealthWinRate: 0, momentumWinRate: 0, trapWinRate: 0 },
            narrativeHealth: { strongWinRate: 0, midWinRate: 0, weakWinRate: 0 }
        };
    }

    /**
     * 初始化数据库表
     */
    initDatabase() {
        this.db.exec(`
      CREATE TABLE IF NOT EXISTS factor_performance (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        token_ca TEXT NOT NULL,
        chain TEXT NOT NULL,
        symbol TEXT,
        
        -- 动态因子值
        sm_density REAL,
        sm_density_rating TEXT,
        hype_divergence REAL,
        hype_divergence_rating TEXT,
        narrative_health REAL,
        narrative_health_rating TEXT,
        
        -- 分类结果
        dog_type TEXT,
        dog_score REAL,
        decision TEXT,
        position_size REAL,
        
        -- 交易结果
        entry_price REAL,
        exit_price REAL,
        pnl_percent REAL,
        max_gain REAL,
        is_winner INTEGER DEFAULT 0,
        exit_reason TEXT,
        
        -- 时间
        created_at TEXT DEFAULT (datetime('now')),
        closed_at TEXT,
        
        UNIQUE(token_ca, chain)
      );
      
      CREATE TABLE IF NOT EXISTS threshold_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        factor_name TEXT NOT NULL,
        old_value REAL,
        new_value REAL,
        reason TEXT,
        win_rate_before REAL,
        win_rate_after REAL,
        changed_at TEXT DEFAULT (datetime('now'))
      );
      
      CREATE INDEX IF NOT EXISTS idx_factor_perf_dog ON factor_performance(dog_type);
      CREATE INDEX IF NOT EXISTS idx_factor_perf_winner ON factor_performance(is_winner);
    `);

        console.log('[AutoTuner] 数据库表初始化完成');
    }

    /**
     * 记录交易的因子表现
     */
    recordTrade(token, dynamicFactors, decision, entryPrice) {
        try {
            const stmt = this.db.prepare(`
        INSERT OR REPLACE INTO factor_performance (
          token_ca, chain, symbol,
          sm_density, sm_density_rating,
          hype_divergence, hype_divergence_rating,
          narrative_health, narrative_health_rating,
          dog_type, dog_score, decision, position_size,
          entry_price, created_at
        ) VALUES (
          @token_ca, @chain, @symbol,
          @sm_density, @sm_density_rating,
          @hype_divergence, @hype_divergence_rating,
          @narrative_health, @narrative_health_rating,
          @dog_type, @dog_score, @decision, @position_size,
          @entry_price, datetime('now')
        )
      `);

            stmt.run({
                token_ca: token.tokenAddress || token.address,
                chain: token.chain,
                symbol: token.symbol || 'Unknown',
                sm_density: dynamicFactors.smDensity,
                sm_density_rating: dynamicFactors.smDensityRating,
                hype_divergence: dynamicFactors.hypeDivergence,
                hype_divergence_rating: dynamicFactors.hypeDivergenceRating,
                narrative_health: dynamicFactors.narrativeHealth,
                narrative_health_rating: dynamicFactors.narrativeHealthRating,
                dog_type: dynamicFactors.dogType,
                dog_score: dynamicFactors.dogScore,
                decision: decision.action,
                position_size: decision.position || 0,
                entry_price: entryPrice || 0
            });

            console.log(`[AutoTuner] 📝 记录因子: ${token.symbol} (${dynamicFactors.dogType})`);
            return true;
        } catch (error) {
            console.error(`[AutoTuner] 记录失败: ${error.message}`);
            return false;
        }
    }

    /**
     * 更新交易结果 (平仓时调用)
     */
    updateTradeResult(tokenCA, chain, exitPrice, pnlPercent, maxGain, exitReason) {
        try {
            const isWinner = pnlPercent >= 100 ? 1 : 0; // 翻倍算赢

            this.db.prepare(`
        UPDATE factor_performance SET
          exit_price = ?,
          pnl_percent = ?,
          max_gain = ?,
          is_winner = ?,
          exit_reason = ?,
          closed_at = datetime('now')
        WHERE token_ca = ? AND chain = ?
      `).run(exitPrice, pnlPercent, maxGain, isWinner, exitReason, tokenCA, chain);

            console.log(`[AutoTuner] 📊 更新结果: ${tokenCA.substring(0, 8)} (PnL: ${pnlPercent.toFixed(1)}%)`);
        } catch (error) {
            console.error(`[AutoTuner] 更新失败: ${error.message}`);
        }
    }

    /**
     * 计算各因子的胜率
     */
    calculateFactorWinRates() {
        const results = {
            smDensity: {},
            hypeDivergence: {},
            narrativeHealth: {},
            dogType: {},
            recommendations: []
        };

        try {
            // 需要至少20条已平仓数据
            const totalClosed = this.db.prepare(`
        SELECT COUNT(*) as count FROM factor_performance WHERE closed_at IS NOT NULL
      `).get();

            if (totalClosed.count < 20) {
                console.log(`[AutoTuner] 数据不足 (${totalClosed.count}/20), 暂不优化`);
                return results;
            }

            // SM Density 胜率
            const smDensityStats = this.db.prepare(`
        SELECT 
          sm_density_rating as rating,
          COUNT(*) as total,
          SUM(is_winner) as winners,
          AVG(pnl_percent) as avg_pnl,
          AVG(max_gain) as avg_max_gain
        FROM factor_performance 
        WHERE closed_at IS NOT NULL
        GROUP BY sm_density_rating
      `).all();

            for (const row of smDensityStats) {
                results.smDensity[row.rating] = {
                    total: row.total,
                    winners: row.winners,
                    winRate: (row.winners / row.total * 100).toFixed(1),
                    avgPnl: row.avg_pnl?.toFixed(1) || 0,
                    avgMaxGain: row.avg_max_gain?.toFixed(1) || 0
                };
            }

            // Hype Divergence 胜率
            const hypeDivStats = this.db.prepare(`
        SELECT 
          hype_divergence_rating as rating,
          COUNT(*) as total,
          SUM(is_winner) as winners,
          AVG(pnl_percent) as avg_pnl
        FROM factor_performance 
        WHERE closed_at IS NOT NULL
        GROUP BY hype_divergence_rating
      `).all();

            for (const row of hypeDivStats) {
                results.hypeDivergence[row.rating] = {
                    total: row.total,
                    winners: row.winners,
                    winRate: (row.winners / row.total * 100).toFixed(1),
                    avgPnl: row.avg_pnl?.toFixed(1) || 0
                };
            }

            // Dog Type 胜率
            const dogTypeStats = this.db.prepare(`
        SELECT 
          dog_type,
          COUNT(*) as total,
          SUM(is_winner) as winners,
          AVG(pnl_percent) as avg_pnl,
          MAX(max_gain) as best_gain
        FROM factor_performance 
        WHERE closed_at IS NOT NULL AND decision = 'BUY'
        GROUP BY dog_type
      `).all();

            for (const row of dogTypeStats) {
                results.dogType[row.dog_type] = {
                    total: row.total,
                    winners: row.winners,
                    winRate: (row.winners / row.total * 100).toFixed(1),
                    avgPnl: row.avg_pnl?.toFixed(1) || 0,
                    bestGain: row.best_gain?.toFixed(0) || 0
                };
            }

            // 生成优化建议
            this.generateOptimizationSuggestions(results);

            console.log('\n📊 ========== 因子胜率分析 ==========');
            console.log('SM Density:', results.smDensity);
            console.log('Hype Divergence:', results.hypeDivergence);
            console.log('Dog Type:', results.dogType);
            console.log('=====================================\n');

        } catch (error) {
            console.error(`[AutoTuner] 计算胜率失败: ${error.message}`);
        }

        return results;
    }

    /**
     * 生成优化建议
     */
    generateOptimizationSuggestions(stats) {
        const suggestions = [];

        // 检查 STEALTH 是否真的比 MOMENTUM 好
        const stealth = stats.hypeDivergence?.STEALTH;
        const momentum = stats.hypeDivergence?.MOMENTUM;

        if (stealth && momentum) {
            if (parseFloat(stealth.winRate) > parseFloat(momentum.winRate) * 1.5) {
                suggestions.push({
                    type: 'INCREASE_STEALTH_BIAS',
                    message: `STEALTH胜率(${stealth.winRate}%)远高于MOMENTUM(${momentum.winRate}%), 建议只买STEALTH阶段`
                });
            }
        }

        // 检查 GOLDEN vs SILVER 实际表现
        const golden = stats.dogType?.GOLDEN;
        const silver = stats.dogType?.SILVER;

        if (golden && silver) {
            if (parseFloat(golden.avgPnl) < parseFloat(silver.avgPnl)) {
                suggestions.push({
                    type: 'REVIEW_GOLDEN_CRITERIA',
                    message: `SILVER平均收益(${silver.avgPnl}%)高于GOLDEN(${golden.avgPnl}%), 可能GOLDEN阈值太松`
                });
            }
        }

        // 输出建议
        if (suggestions.length > 0) {
            console.log('\n💡 优化建议:');
            suggestions.forEach((s, i) => {
                console.log(`   ${i + 1}. [${s.type}] ${s.message}`);
            });
        }

        return suggestions;
    }

    /**
     * 自动调整阈值 (基于历史数据)
     */
    autoAdjustThresholds() {
        console.log('[AutoTuner] 🔄 开始自动调参...');

        const stats = this.calculateFactorWinRates();
        const adjustments = [];

        try {
            // 示例：如果 TRAP 也有一些赢家，可能阈值太严
            const trap = stats.hypeDivergence?.TRAP;
            if (trap && parseFloat(trap.winRate) > 30) {
                // 放宽 TRAP 阈值
                const oldValue = this.thresholds.hypeDivergence.momentum;
                const newValue = oldValue * 0.8; // 降低 20%

                this.thresholds.hypeDivergence.momentum = newValue;

                adjustments.push({
                    factor: 'hypeDivergence.momentum',
                    old: oldValue,
                    new: newValue,
                    reason: `TRAP胜率${trap.winRate}%过高，放宽阈值`
                });

                // 记录变更历史
                this.db.prepare(`
          INSERT INTO threshold_history (factor_name, old_value, new_value, reason, win_rate_before)
          VALUES (?, ?, ?, ?, ?)
        `).run('hypeDivergence.momentum', oldValue, newValue, `TRAP胜率${trap.winRate}%`, parseFloat(trap.winRate));
            }

            if (adjustments.length > 0) {
                console.log('[AutoTuner] ✅ 阈值已调整:');
                adjustments.forEach(a => {
                    console.log(`   ${a.factor}: ${a.old.toFixed(2)} → ${a.new.toFixed(2)} (${a.reason})`);
                });

                this.emit('thresholds-adjusted', { adjustments, newThresholds: this.thresholds });
            } else {
                console.log('[AutoTuner] ℹ️ 当前阈值表现良好，无需调整');
            }

        } catch (error) {
            console.error(`[AutoTuner] 自动调参失败: ${error.message}`);
        }

        return adjustments;
    }

    /**
     * 获取当前阈值配置
     */
    getThresholds() {
        return this.thresholds;
    }

    /**
     * 获取统计摘要
     */
    getSummary() {
        const summary = this.db.prepare(`
      SELECT 
        COUNT(*) as total_trades,
        SUM(closed_at IS NOT NULL) as closed_trades,
        SUM(is_winner) as winners,
        AVG(CASE WHEN closed_at IS NOT NULL THEN pnl_percent END) as avg_pnl,
        MAX(max_gain) as best_trade,
        MIN(pnl_percent) as worst_trade
      FROM factor_performance
    `).get();

        summary.winRate = summary.closed_trades > 0
            ? (summary.winners / summary.closed_trades * 100).toFixed(1)
            : 0;

        return summary;
    }
}

export default AutoTuner;
