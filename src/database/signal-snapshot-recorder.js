/**
 * Signal Snapshot Recorder v7.4
 *
 * 记录所有被硬门槛检查的信号快照，用于后续回测分析
 * 包括通过和被拒绝的信号
 */

import Database from 'better-sqlite3';
import path from 'path';

class SignalSnapshotRecorder {
    constructor() {
        this.db = null;
        this.initialized = false;
    }

    /**
     * 初始化数据库连接
     */
    init() {
        if (this.initialized) return true;

        try {
            const dbPath = path.join(process.cwd(), 'data', 'sentiment_arb.db');
            this.db = new Database(dbPath);
            this.initialized = true;
            console.log('[SnapshotRecorder] ✅ 信号快照记录器已初始化');
            return true;
        } catch (e) {
            console.error('[SnapshotRecorder] ❌ 初始化失败:', e.message);
            return false;
        }
    }

    /**
     * 记录被拒绝的信号
     *
     * @param {Object} token - 代币数据
     * @param {string} gateType - 门槛类型 (sm_flow/price_rise/price_drop)
     * @param {string} reason - 拒绝原因
     * @param {Object} thresholdUsed - 使用的阈值配置
     */
    recordRejection(token, gateType, reason, thresholdUsed = {}) {
        if (!this.initialized && !this.init()) return;

        try {
            // v9.4: 确保 rejected_signals 表存在
            this.db.exec(`
                CREATE TABLE IF NOT EXISTS rejected_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    token_ca TEXT NOT NULL,
                    chain TEXT,
                    symbol TEXT,
                    signal_source TEXT,
                    rejection_stage TEXT,
                    rejection_reason TEXT,
                    rejection_factors TEXT,
                    price_at_rejection REAL,
                    mcap_at_rejection REAL,
                    liquidity_at_rejection REAL,
                    sm_count_at_rejection INTEGER,
                    price_change REAL,
                    intention_tier TEXT,
                    signal_trend_type TEXT,
                    base_score REAL,
                    safety_score REAL,
                    decision_source TEXT,
                    gate_type TEXT,
                    threshold_used TEXT,
                    market_cap REAL,
                    age_minutes REAL,
                    created_at TEXT DEFAULT (datetime('now')),
                    -- v9.4: 后续追踪字段
                    tracked INTEGER DEFAULT 0,
                    track_time TEXT,
                    price_after_24h REAL,
                    mcap_after_24h REAL,
                    hypothetical_pnl REAL,
                    was_gold INTEGER DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS idx_rejected_signals_token ON rejected_signals(token_ca);
                CREATE INDEX IF NOT EXISTS idx_rejected_signals_created ON rejected_signals(created_at);
                CREATE INDEX IF NOT EXISTS idx_rejected_signals_tracked ON rejected_signals(tracked);
            `);

            const stmt = this.db.prepare(`
                INSERT INTO rejected_signals (
                    token_ca, chain, symbol, signal_source,
                    rejection_stage, rejection_reason, rejection_factors,
                    price_at_rejection, mcap_at_rejection, liquidity_at_rejection,
                    sm_count_at_rejection,
                    price_change, intention_tier, signal_trend_type,
                    base_score, safety_score,
                    decision_source, gate_type, threshold_used,
                    market_cap, age_minutes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `);

            stmt.run(
                token.tokenAddress || token.address || token.token_ca || '',
                token.chain || 'SOL',
                token.symbol || '',
                token.signalSource || 'batch_ai_advisor',
                'hard_gate_price',
                reason,
                JSON.stringify({ reason, gateType }),
                token.currentPrice || token.price || 0,
                token.marketCap || token.mcap || 0,
                token.liquidity || 0,
                token.smartMoney || token.smCurrent || 0,
                // 新字段
                parseFloat(token.priceChange || 0),
                token.intentionTier || 'TIER_C',
                token.signalTrendType || token.trendType || 'STABLE',
                token.baseScore || 0,
                token.safetyScore || 0,
                'batch_ai_advisor',
                gateType,
                JSON.stringify(thresholdUsed),
                token.marketCap || token.mcap || 0,
                token.ageMinutes || 0
            );

            console.log(`[SnapshotRecorder] 📝 记录拒绝: ${token.symbol} (${gateType})`);
        } catch (e) {
            // 静默处理错误，不影响主流程
            // console.error('[SnapshotRecorder] 记录失败:', e.message);
        }
    }

    /**
     * 记录通过的信号 (存入单独的表)
     */
    recordPassed(token, thresholdUsed = {}) {
        if (!this.initialized && !this.init()) return;

        try {
            // 检查 passed_signals 表是否存在，如果不存在则创建
            this.db.exec(`
                CREATE TABLE IF NOT EXISTS passed_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    token_ca TEXT NOT NULL,
                    chain TEXT,
                    symbol TEXT,
                    price_change REAL,
                    intention_tier TEXT,
                    signal_trend_type TEXT,
                    sm_count INTEGER,
                    base_score REAL,
                    market_cap REAL,
                    threshold_used TEXT,
                    created_at TEXT DEFAULT (datetime('now')),
                    -- 后续追踪
                    final_pnl REAL,
                    is_gold INTEGER DEFAULT 0,
                    tracked INTEGER DEFAULT 0
                )
            `);

            const stmt = this.db.prepare(`
                INSERT INTO passed_signals (
                    token_ca, chain, symbol,
                    price_change, intention_tier, signal_trend_type,
                    sm_count, base_score, market_cap, threshold_used
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `);

            stmt.run(
                token.tokenAddress || token.address || '',
                token.chain || 'SOL',
                token.symbol || '',
                parseFloat(token.priceChange || 0),
                token.intentionTier || 'TIER_C',
                token.signalTrendType || 'STABLE',
                token.smartMoney || token.smCurrent || 0,
                token.baseScore || 0,
                token.marketCap || 0,
                JSON.stringify(thresholdUsed)
            );

            console.log(`[SnapshotRecorder] 📝 记录通过: ${token.symbol}`);
        } catch (e) {
            // 静默处理
        }
    }

    /**
     * v7.4: 记录 WATCH 决策 (用于回测分析)
     *
     * @param {Object} token - 代币数据
     * @param {Object} context - 上下文信息
     */
    recordWatch(token, context = {}) {
        if (!this.initialized && !this.init()) return;

        try {
            // 确保 watch_signals 表存在
            this.db.exec(`
                CREATE TABLE IF NOT EXISTS watch_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    token_ca TEXT NOT NULL,
                    chain TEXT,
                    symbol TEXT,
                    price_change REAL,
                    intention_tier TEXT,
                    signal_trend_type TEXT,
                    sm_count INTEGER,
                    base_score REAL,
                    market_cap REAL,
                    watch_reason TEXT,
                    context TEXT,
                    created_at TEXT DEFAULT (datetime('now')),
                    -- 后续追踪
                    final_decision TEXT,
                    final_pnl REAL,
                    tracked INTEGER DEFAULT 0
                )
            `);

            const stmt = this.db.prepare(`
                INSERT INTO watch_signals (
                    token_ca, chain, symbol,
                    price_change, intention_tier, signal_trend_type,
                    sm_count, base_score, market_cap, watch_reason, context
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `);

            stmt.run(
                token.tokenAddress || token.address || '',
                token.chain || 'SOL',
                token.symbol || '',
                parseFloat(token.priceChange || 0),
                token.intentionTier || 'TIER_C',
                token.signalTrendType || 'STABLE',
                token.smartMoney || token.smCurrent || 0,
                token.baseScore || 0,
                token.marketCap || 0,
                context.reason || 'AI建议观察',
                JSON.stringify(context)
            );

            // 静默记录，不打印日志避免噪音
        } catch (e) {
            // 静默处理
        }
    }

    /**
     * v9.4: 记录AI决策 (BUY/DISCARD/WATCH)
     * 用于追踪AI决策准确率
     *
     * @param {Object} token - 代币数据
     * @param {Object} decision - AI决策结果
     * @param {Object} context - 上下文信息
     */
    recordAIDecision(token, decision, context = {}) {
        if (!this.initialized && !this.init()) return;

        try {
            // 确保 ai_decisions 表存在
            this.db.exec(`
                CREATE TABLE IF NOT EXISTS ai_decisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    token_ca TEXT NOT NULL,
                    chain TEXT,
                    symbol TEXT,

                    -- AI决策
                    decision_action TEXT NOT NULL,  -- BUY, DISCARD, WATCH, SKIP
                    decision_reason TEXT,
                    narrative_tier TEXT,            -- TIER_S/A/B/C
                    narrative_reason TEXT,
                    target_mcap REAL,
                    confidence INTEGER,

                    -- 决策时市场数据
                    price_at_decision REAL,
                    mcap_at_decision REAL,
                    liquidity_at_decision REAL,
                    sm_count_at_decision INTEGER,

                    -- 信号源
                    signal_source TEXT,

                    -- 时间戳
                    created_at TEXT DEFAULT (datetime('now')),

                    -- v9.4: 后续追踪字段
                    tracked INTEGER DEFAULT 0,
                    track_time TEXT,
                    price_1h_later REAL,
                    price_24h_later REAL,
                    max_price_24h REAL,
                    actual_pnl_1h REAL,
                    actual_pnl_24h REAL,
                    was_correct INTEGER,  -- 1=正确决策, 0=错误决策

                    -- 如果执行了买入
                    position_id INTEGER,
                    final_pnl REAL
                );
                CREATE INDEX IF NOT EXISTS idx_ai_decisions_token ON ai_decisions(token_ca);
                CREATE INDEX IF NOT EXISTS idx_ai_decisions_action ON ai_decisions(decision_action);
                CREATE INDEX IF NOT EXISTS idx_ai_decisions_created ON ai_decisions(created_at);
                CREATE INDEX IF NOT EXISTS idx_ai_decisions_tracked ON ai_decisions(tracked);
                CREATE INDEX IF NOT EXISTS idx_ai_decisions_correct ON ai_decisions(was_correct);
            `);

            const stmt = this.db.prepare(`
                INSERT INTO ai_decisions (
                    token_ca, chain, symbol,
                    decision_action, decision_reason, narrative_tier, narrative_reason,
                    target_mcap, confidence,
                    price_at_decision, mcap_at_decision, liquidity_at_decision, sm_count_at_decision,
                    signal_source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `);

            stmt.run(
                token.tokenAddress || token.address || token.token_ca || '',
                token.chain || 'SOL',
                token.symbol || '',
                decision.action || 'UNKNOWN',
                decision.reason || '',
                decision.narrativeTier || decision.intention_tier || 'TIER_C',
                decision.narrativeReason || decision.intention_reason || '',
                decision.targetMcap || decision.target_mcap || null,
                decision.confidence || 50,
                token.currentPrice || token.price || 0,
                token.marketCap || token.mcap || 0,
                token.liquidity || 0,
                token.smartMoney || token.smCurrent || 0,
                context.signalSource || token.signalSource || 'unknown'
            );

            console.log(`[SnapshotRecorder] 🤖 记录AI决策: ${token.symbol} → ${decision.action}`);
        } catch (e) {
            // 静默处理，不影响主流程
            // console.error('[SnapshotRecorder] AI决策记录失败:', e.message);
        }
    }

    /**
     * 获取统计信息
     */
    getStats() {
        if (!this.initialized) return null;

        try {
            const rejected = this.db.prepare(`
                SELECT
                    gate_type,
                    COUNT(*) as count,
                    AVG(price_change) as avg_price_change,
                    COUNT(CASE WHEN signal_trend_type = 'ACCELERATING' THEN 1 END) as accel_count
                FROM rejected_signals
                WHERE decision_source = 'batch_ai_advisor'
                AND created_at > datetime('now', '-24 hours')
                GROUP BY gate_type
            `).all();

            const passed = this.db.prepare(`
                SELECT COUNT(*) as count
                FROM passed_signals
                WHERE created_at > datetime('now', '-24 hours')
            `).get();

            return { rejected, passed: passed?.count || 0 };
        } catch (e) {
            return null;
        }
    }

    close() {
        if (this.db) {
            this.db.close();
            this.initialized = false;
        }
    }
}

// 单例模式
const recorder = new SignalSnapshotRecorder();

export default recorder;
export { SignalSnapshotRecorder };
