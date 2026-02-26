/**
 * 金狗特征追踪模块 v1.0
 * 
 * 功能：
 * 1. 捕获信号入场时的所有动态参数
 * 2. 24小时后追踪结果
 * 3. 计算参数与成功的相关性
 * 4. 识别金狗/银狗特征模式
 */

import Database from 'better-sqlite3';
import { EventEmitter } from 'events';

class GoldDogTracker extends EventEmitter {
    constructor(dbPath = './data/sentiment_arb.db') {
        super();
        this.db = new Database(dbPath);
        this.initDatabase();
    }

    /**
     * 初始化数据库表
     */
    initDatabase() {
        this.db.exec(`
      CREATE TABLE IF NOT EXISTS signal_features (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        token_ca TEXT NOT NULL,
        chain TEXT NOT NULL,
        symbol TEXT,
        
        -- 入场时捕获的特征
        ai_score REAL,
        narrative_type TEXT,
        signal_count INTEGER,
        signal_velocity REAL,
        sm_count INTEGER,
        sm_trend TEXT,
        sm_avg_buy REAL,
        token_age_minutes REAL,
        market_cap REAL,
        liquidity REAL,
        holders INTEGER,
        entry_price REAL,
        price_change_1h REAL,
        top10_percent REAL,
        kol_count INTEGER,
        whale_buys INTEGER,
        x_mentions INTEGER,
        
        -- 综合评分明细
        total_score REAL,
        score_breakdown TEXT,
        decision TEXT,
        position_size REAL,
        
        -- 24小时后追踪结果
        max_gain_24h REAL,
        min_loss_24h REAL,
        final_price_24h REAL,
        is_gold_dog INTEGER DEFAULT 0,
        is_silver_dog INTEGER DEFAULT 0,
        is_winner INTEGER DEFAULT 0,
        
        -- 实际交易结果（如果买入了）
        did_buy INTEGER DEFAULT 0,
        actual_pnl REAL,
        actual_exit_reason TEXT,
        
        -- 状态
        captured_at TEXT DEFAULT (datetime('now')),
        tracked_at TEXT,
        
        UNIQUE(token_ca, chain)
      );
      
      CREATE INDEX IF NOT EXISTS idx_signal_features_captured ON signal_features(captured_at);
      CREATE INDEX IF NOT EXISTS idx_signal_features_tracked ON signal_features(tracked_at);
      CREATE INDEX IF NOT EXISTS idx_signal_features_gold ON signal_features(is_gold_dog);
    `);

        console.log('[GoldDogTracker] 数据库表初始化完成');
    }

    /**
     * 捕获信号入场时的所有特征
     */
    captureFeatures(token, score, decision, aiReport = null, tokenState = null) {
        try {
            const features = {
                token_ca: token.tokenAddress || token.address,
                chain: token.chain,
                symbol: token.symbol || 'Unknown',

                // AI 相关
                ai_score: aiReport?.rating?.score || token.aiScore || 0,
                narrative_type: aiReport?.project_type || 'unknown',

                // 信号相关
                signal_count: tokenState?.history?.length || token.signalCount || 0,
                signal_velocity: this.calculateVelocity(tokenState),

                // 聪明钱相关
                sm_count: token.smartWalletOnline || 0,
                sm_trend: score?.trends?.smartMoney || 'UNKNOWN',
                sm_avg_buy: token.avgBuyAmount || 0,

                // 时机相关
                token_age_minutes: this.calculateTokenAge(token),

                // 市场相关
                market_cap: token.marketCap || 0,
                liquidity: token.liquidity || 0,
                holders: token.holders || 0,
                entry_price: token.currentPrice || 0,
                price_change_1h: token.priceChange1h || 0,
                top10_percent: token.top10Percent || 0,

                // 社交相关
                kol_count: aiReport?.distribution?.kol_interactions?.length || 0,
                whale_buys: token.whaleBuys || 0,
                x_mentions: aiReport?.twitter_24h?.current_count || 0,

                // 评分相关
                total_score: score?.total || 0,
                score_breakdown: JSON.stringify(score?.breakdown || {}),
                decision: decision?.action || 'UNKNOWN',
                position_size: decision?.position || 0,

                // 是否买入
                did_buy: decision?.action === 'BUY' || decision?.action === 'ADD' ? 1 : 0
            };

            // 使用 REPLACE 来更新或插入
            const stmt = this.db.prepare(`
        INSERT OR REPLACE INTO signal_features (
          token_ca, chain, symbol,
          ai_score, narrative_type,
          signal_count, signal_velocity,
          sm_count, sm_trend, sm_avg_buy,
          token_age_minutes,
          market_cap, liquidity, holders,
          entry_price, price_change_1h, top10_percent,
          kol_count, whale_buys, x_mentions,
          total_score, score_breakdown, decision, position_size,
          did_buy, captured_at
        ) VALUES (
          @token_ca, @chain, @symbol,
          @ai_score, @narrative_type,
          @signal_count, @signal_velocity,
          @sm_count, @sm_trend, @sm_avg_buy,
          @token_age_minutes,
          @market_cap, @liquidity, @holders,
          @entry_price, @price_change_1h, @top10_percent,
          @kol_count, @whale_buys, @x_mentions,
          @total_score, @score_breakdown, @decision, @position_size,
          @did_buy, datetime('now')
        )
      `);

            stmt.run(features);
            console.log(`[GoldDogTracker] 📸 捕获特征: ${features.symbol} (SM:${features.sm_count}, Score:${features.total_score})`);

            return features;
        } catch (error) {
            console.error(`[GoldDogTracker] 捕获特征失败: ${error.message}`);
            return null;
        }
    }

    /**
     * 24小时后追踪结果
     */
    async trackOutcomes(snapshotService) {
        try {
            // 获取 24 小时前的未追踪信号
            const signals = this.db.prepare(`
        SELECT * FROM signal_features 
        WHERE tracked_at IS NULL 
        AND captured_at < datetime('now', '-24 hours')
        LIMIT 50
      `).all();

            console.log(`[GoldDogTracker] 📊 追踪 ${signals.length} 个信号的 24h 结果...`);

            for (const signal of signals) {
                try {
                    const snapshot = await snapshotService.getSnapshot(signal.token_ca);
                    if (!snapshot) continue;

                    const currentPrice = snapshot.current_price || 0;
                    const entryPrice = signal.entry_price || 0;

                    if (entryPrice === 0) continue;

                    const gain = ((currentPrice - entryPrice) / entryPrice) * 100;

                    // 判断类型
                    const isGoldDog = gain >= 1000 ? 1 : 0;  // 10x
                    const isSilverDog = gain >= 500 ? 1 : 0;   // 5x
                    const isWinner = gain >= 100 ? 1 : 0;      // 2x

                    this.db.prepare(`
            UPDATE signal_features SET
              max_gain_24h = MAX(COALESCE(max_gain_24h, 0), ?),
              final_price_24h = ?,
              is_gold_dog = ?,
              is_silver_dog = ?,
              is_winner = ?,
              tracked_at = datetime('now')
            WHERE id = ?
          `).run(gain, currentPrice, isGoldDog, isSilverDog, isWinner, signal.id);

                    if (isGoldDog) {
                        console.log(`🏆 [GoldDog] ${signal.symbol}: +${gain.toFixed(0)}%`);
                    } else if (isSilverDog) {
                        console.log(`🥈 [SilverDog] ${signal.symbol}: +${gain.toFixed(0)}%`);
                    }

                } catch (e) {
                    // 跳过失败的币
                }
            }

        } catch (error) {
            console.error(`[GoldDogTracker] 追踪失败: ${error.message}`);
        }
    }

    /**
     * 计算参数与成功的相关性
     */
    calculateCorrelations() {
        const results = {
            parameters: {},
            goldDogSignature: {},
            recommendations: []
        };

        try {
            // 获取所有已追踪的信号
            const signals = this.db.prepare(`
        SELECT * FROM signal_features WHERE tracked_at IS NOT NULL
      `).all();

            if (signals.length < 10) {
                console.log('[GoldDogTracker] 数据不足，需要至少 10 条追踪数据');
                return results;
            }

            // 分析每个参数
            const parameters = [
                'sm_count', 'ai_score', 'signal_count', 'liquidity',
                'holders', 'token_age_minutes', 'market_cap', 'kol_count'
            ];

            for (const param of parameters) {
                const winners = signals.filter(s => s.is_winner === 1);
                const goldDogs = signals.filter(s => s.is_gold_dog === 1);

                const avgAll = this.average(signals.map(s => s[param] || 0));
                const avgWinners = this.average(winners.map(s => s[param] || 0));
                const avgGoldDogs = this.average(goldDogs.map(s => s[param] || 0));

                results.parameters[param] = {
                    avgAll,
                    avgWinners,
                    avgGoldDogs,
                    winnerRatio: avgWinners / (avgAll || 1),
                    goldDogRatio: avgGoldDogs / (avgAll || 1)
                };

                // 如果金狗的参数明显高于平均，记录为特征
                if (avgGoldDogs > avgAll * 1.5) {
                    results.goldDogSignature[param] = `>= ${avgGoldDogs.toFixed(1)}`;
                }
            }

            // 分析聪明钱趋势
            const trendAnalysis = {
                increasing: signals.filter(s => s.sm_trend === 'INCREASING'),
                stable: signals.filter(s => s.sm_trend === 'STABLE'),
                decreasing: signals.filter(s => s.sm_trend === 'DECREASING')
            };

            results.trendWinRate = {
                increasing: this.winRate(trendAnalysis.increasing),
                stable: this.winRate(trendAnalysis.stable),
                decreasing: this.winRate(trendAnalysis.decreasing)
            };

            // 生成优化建议
            if (results.trendWinRate.increasing > results.trendWinRate.stable * 1.5) {
                results.recommendations.push('聪明钱趋势 INCREASING 时胜率显著更高，建议加大权重');
            }

            console.log('[GoldDogTracker] 📊 相关性分析完成');
            console.log('金狗特征:', results.goldDogSignature);
            console.log('趋势胜率:', results.trendWinRate);

        } catch (error) {
            console.error(`[GoldDogTracker] 相关性分析失败: ${error.message}`);
        }

        return results;
    }

    /**
     * 生成每日复盘报告
     */
    generateDailyReport() {
        const report = {
            date: new Date().toISOString().split('T')[0],
            summary: {},
            missedGoldDogs: [],
            overlyEarlyExits: [],
            correlations: {},
            optimizationSuggestions: []
        };

        try {
            // 今日统计
            const today = this.db.prepare(`
        SELECT 
          COUNT(*) as total_signals,
          SUM(did_buy) as bought,
          SUM(is_winner) as winners,
          SUM(is_gold_dog) as gold_dogs,
          SUM(is_silver_dog) as silver_dogs
        FROM signal_features 
        WHERE DATE(captured_at) = DATE('now')
      `).get();

            report.summary = today;

            // 错过的金狗
            report.missedGoldDogs = this.db.prepare(`
        SELECT symbol, token_ca, chain, sm_count, ai_score, total_score, decision, max_gain_24h
        FROM signal_features 
        WHERE is_gold_dog = 1 AND did_buy = 0
        ORDER BY max_gain_24h DESC
        LIMIT 10
      `).all();

            // 相关性
            report.correlations = this.calculateCorrelations();

            console.log('\n📊 ========== 每日复盘报告 ==========');
            console.log(`日期: ${report.date}`);
            console.log(`今日信号: ${today.total_signals} | 买入: ${today.bought} | 涨2x+: ${today.winners} | 金狗: ${today.gold_dogs}`);

            if (report.missedGoldDogs.length > 0) {
                console.log('\n❌ 错过的金狗:');
                report.missedGoldDogs.forEach(dog => {
                    console.log(`   ${dog.symbol}: ${dog.max_gain_24h?.toFixed(0)}% (分数${dog.total_score}, 决策${dog.decision})`);
                });
            }

            console.log('=====================================\n');

        } catch (error) {
            console.error(`[GoldDogTracker] 生成报告失败: ${error.message}`);
        }

        return report;
    }

    // 辅助方法
    calculateVelocity(tokenState) {
        if (!tokenState?.history || tokenState.history.length < 2) return 0;
        const recent = tokenState.history.slice(-5);
        if (recent.length < 2) return 0;
        const timeDiff = (recent[recent.length - 1].time - recent[0].time) / 60000; // 分钟
        return timeDiff > 0 ? recent.length / timeDiff : 0;
    }

    calculateTokenAge(token) {
        if (token.createdAt) {
            return (Date.now() - new Date(token.createdAt).getTime()) / 60000;
        }
        return token.tokenAge || 0;
    }

    average(arr) {
        if (arr.length === 0) return 0;
        return arr.reduce((a, b) => a + b, 0) / arr.length;
    }

    winRate(signals) {
        if (signals.length === 0) return 0;
        return signals.filter(s => s.is_winner === 1).length / signals.length;
    }

    /**
     * 获取统计数据
     */
    getStats() {
        return this.db.prepare(`
      SELECT 
        COUNT(*) as total,
        SUM(tracked_at IS NOT NULL) as tracked,
        SUM(is_gold_dog) as gold_dogs,
        SUM(is_silver_dog) as silver_dogs,
        SUM(is_winner) as winners,
        SUM(did_buy) as bought
      FROM signal_features
    `).get();
    }
}

export default GoldDogTracker;
