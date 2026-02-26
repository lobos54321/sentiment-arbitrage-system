/**
 * Signal Merger v1.0
 * 
 * 交集验证系统 - 多信号源交叉验证
 * 
 * 核心功能:
 * 1. 检测 DeBot + GMGN 猎人同时买入同一代币
 * 2. 根据交集类型计算信号增强 (boost)
 * 3. 时间衰减：同时性越强，信号越强
 * 
 * 交集等级:
 * - 🏆 黄金交集: DeBot + FOX → ×2.0, 观察期 30%
 * - 🥇 银色交集: DeBot + WOLF → ×1.5, 观察期 50%
 * - 🥈 影子交集: DeBot + Shadow → ×1.3, 观察期 70%
 * - 普通信号: 单一来源 → ×1.0, 观察期 100%
 */

import { EventEmitter } from 'events';

// 交集配置
const INTERSECTION_CONFIG = {
    // 时间窗口 (毫秒)
    TIME_WINDOW: 5 * 60 * 1000,  // 5分钟

    // 交集等级定义
    LEVELS: {
        GOLDEN_CROSS: {
            name: '黄金交集',
            emoji: '🏆',
            requiredHunterTypes: ['FOX', 'EAGLE'],
            positionMultiplier: 2.0,
            observeTimeMultiplier: 0.3  // 观察时间缩短到30%
        },
        SILVER_CROSS: {
            name: '银色交集',
            emoji: '🥇',
            requiredHunterTypes: ['WOLF'],
            positionMultiplier: 1.5,
            observeTimeMultiplier: 0.5
        },
        SHADOW_CROSS: {
            name: '影子交集',
            emoji: '🥈',
            requiredHunterTypes: ['SHADOW'],  // 来自 ShadowProtocolV2 的共识
            positionMultiplier: 1.3,
            observeTimeMultiplier: 0.7
        },
        TURTLE_CROSS: {
            name: '波段交集',
            emoji: '🐢',
            requiredHunterTypes: ['TURTLE'],
            positionMultiplier: 1.2,
            observeTimeMultiplier: 0.8
        }
    },

    // 时间衰减
    TIME_DECAY: {
        VERY_FAST: { maxMs: 60000, bonus: 1.2 },    // < 1分钟，+20%
        FAST: { maxMs: 180000, bonus: 1.0 },        // < 3分钟，正常
        SLOW: { maxMs: 300000, bonus: 0.8 }         // > 3分钟，-20%
    }
};

export class SignalMerger extends EventEmitter {
    constructor(config = {}) {
        super();

        this.config = {
            timeWindow: config.timeWindow || INTERSECTION_CONFIG.TIME_WINDOW,
            debug: config.debug !== false
        };

        // 存储最近的信号
        // Map<tokenCA, SignalRecord>
        this.debotSignals = new Map();      // DeBot 来源
        this.hunterSignals = new Map();     // GMGN 猎人来源
        this.shadowSignals = new Map();     // Shadow 共识来源

        // 已处理的交集 (防止重复)
        this.processedIntersections = new Set();

        // 统计
        this.stats = {
            debotReceived: 0,
            hunterReceived: 0,
            shadowReceived: 0,
            goldenCross: 0,
            silverCross: 0,
            shadowCross: 0,
            turtleCross: 0,
            signalsEmitted: 0
        };

        // 清理定时器
        this.cleanupTimer = null;

        console.log('[SignalMerger] 🔀 交集验证系统初始化');
        console.log(`   - 时间窗口: ${this.config.timeWindow / 1000}秒`);
    }

    /**
     * 启动清理定时器
     */
    start() {
        // 每分钟清理过期信号
        this.cleanupTimer = setInterval(() => {
            this.cleanupExpiredSignals();
        }, 60 * 1000);

        console.log('[SignalMerger] ✅ 已启动');
    }

    /**
     * 停止
     */
    stop() {
        if (this.cleanupTimer) {
            clearInterval(this.cleanupTimer);
            this.cleanupTimer = null;
        }
        console.log('[SignalMerger] ⏹️ 已停止');
        this.printStats();
    }

    // ═══════════════════════════════════════════════════════════
    // 信号接收入口
    // ═══════════════════════════════════════════════════════════

    /**
     * 接收 DeBot 信号
     */
    onDebotSignal(signal) {
        const tokenCA = signal.token_ca || signal.tokenAddress;
        if (!tokenCA) return null;

        this.stats.debotReceived++;

        const record = {
            tokenCA,
            symbol: signal.symbol,
            chain: signal.chain || 'sol',
            source: 'debot',
            score: signal.score || signal.ai_score || 50,
            marketCap: signal.market_cap || signal.marketCap,
            liquidity: signal.liquidity,
            timestamp: Date.now(),
            originalSignal: signal
        };

        this.debotSignals.set(tokenCA, record);

        if (this.config.debug) {
            console.log(`[SignalMerger] 📥 DeBot信号: ${signal.symbol} (${tokenCA.slice(0, 8)}...)`);
        }

        // 检查是否有交集
        return this.checkIntersection(tokenCA, 'debot');
    }

    /**
     * 接收猎人信号 (来自 UltraSniperV2)
     */
    onHunterSignal(signal) {
        const tokenCA = signal.token_ca || signal.tokenAddress;
        if (!tokenCA) return null;

        this.stats.hunterReceived++;

        const hunterType = signal.hunter?.type ||
            signal.signalLineage?.hunterType ||
            'UNKNOWN';

        const record = {
            tokenCA,
            symbol: signal.symbol,
            chain: signal.chain || 'sol',
            source: 'hunter',
            hunterType: hunterType,
            hunterAddress: signal.hunter?.address || signal.shadow_wallet,
            hunterScore: signal.hunter?.score || signal.shadow_score || 50,
            timestamp: Date.now(),
            originalSignal: signal
        };

        this.hunterSignals.set(tokenCA, record);

        if (this.config.debug) {
            console.log(`[SignalMerger] 📥 猎人信号: ${hunterType} → ${signal.symbol}`);
        }

        // 检查是否有交集
        return this.checkIntersection(tokenCA, 'hunter');
    }

    /**
     * 接收影子共识信号 (来自 ShadowProtocolV2)
     */
    onShadowSignal(signal) {
        const tokenCA = signal.token_ca || signal.tokenAddress;
        if (!tokenCA) return null;

        this.stats.shadowReceived++;

        const record = {
            tokenCA,
            symbol: signal.symbol,
            chain: signal.chain || 'sol',
            source: 'shadow',
            consensusType: signal.consensus?.type || 'normal',
            shadowCount: signal.consensus?.shadowCount || 0,
            timestamp: Date.now(),
            originalSignal: signal
        };

        this.shadowSignals.set(tokenCA, record);

        if (this.config.debug) {
            console.log(`[SignalMerger] 📥 影子共识: ${signal.symbol} (${record.shadowCount}个影子)`);
        }

        // 检查是否有交集
        return this.checkIntersection(tokenCA, 'shadow');
    }

    // ═══════════════════════════════════════════════════════════
    // 交集检测核心逻辑
    // ═══════════════════════════════════════════════════════════

    /**
     * 检查交集
     * @param {string} tokenCA - 代币地址
     * @param {string} triggerSource - 触发检测的来源
     * @returns {Object|null} 交集结果
     */
    checkIntersection(tokenCA, triggerSource) {
        const now = Date.now();

        // 获取各来源的信号
        const debotRecord = this.debotSignals.get(tokenCA);
        const hunterRecord = this.hunterSignals.get(tokenCA);
        const shadowRecord = this.shadowSignals.get(tokenCA);

        // 检查是否已处理过
        const intersectionKey = `${tokenCA}_${now - (now % 60000)}`;  // 按分钟去重
        if (this.processedIntersections.has(intersectionKey)) {
            return null;
        }

        // 必须有 DeBot 信号作为基础
        if (!debotRecord) {
            return null;
        }

        // 检查 DeBot 信号是否在时间窗口内
        if (now - debotRecord.timestamp > this.config.timeWindow) {
            return null;
        }

        // 确定交集类型
        let intersectionLevel = null;
        let hunterType = null;
        let timeDiff = 0;

        // 优先级: FOX/EAGLE > WOLF > TURTLE > Shadow
        if (hunterRecord && (now - hunterRecord.timestamp <= this.config.timeWindow)) {
            hunterType = hunterRecord.hunterType;
            timeDiff = Math.abs(debotRecord.timestamp - hunterRecord.timestamp);

            if (['FOX', 'EAGLE'].includes(hunterType)) {
                intersectionLevel = INTERSECTION_CONFIG.LEVELS.GOLDEN_CROSS;
                this.stats.goldenCross++;
            } else if (hunterType === 'WOLF') {
                intersectionLevel = INTERSECTION_CONFIG.LEVELS.SILVER_CROSS;
                this.stats.silverCross++;
            } else if (hunterType === 'TURTLE') {
                intersectionLevel = INTERSECTION_CONFIG.LEVELS.TURTLE_CROSS;
                this.stats.turtleCross++;
            }
        }

        // 如果没有猎人交集，检查影子交集
        if (!intersectionLevel && shadowRecord &&
            (now - shadowRecord.timestamp <= this.config.timeWindow)) {
            intersectionLevel = INTERSECTION_CONFIG.LEVELS.SHADOW_CROSS;
            timeDiff = Math.abs(debotRecord.timestamp - shadowRecord.timestamp);
            this.stats.shadowCross++;
        }

        // 没有交集
        if (!intersectionLevel) {
            return null;
        }

        // 计算时间衰减
        const timeBonus = this.calculateTimeBonus(timeDiff);

        // 构建交集结果
        const result = {
            tokenCA,
            symbol: debotRecord.symbol,
            chain: debotRecord.chain,

            // 交集信息
            intersection: {
                level: intersectionLevel.name,
                emoji: intersectionLevel.emoji,
                hunterType: hunterType,
                timeDiffMs: timeDiff,
                timeBonus: timeBonus
            },

            // 增强参数
            positionMultiplier: intersectionLevel.positionMultiplier * timeBonus,
            observeTimeMultiplier: intersectionLevel.observeTimeMultiplier,

            // 来源信号
            sources: {
                debot: debotRecord,
                hunter: hunterRecord,
                shadow: shadowRecord
            },

            timestamp: now
        };

        // 标记为已处理
        this.processedIntersections.add(intersectionKey);

        // 发出交集信号
        this.emitIntersectionSignal(result);

        return result;
    }

    /**
     * 计算时间衰减 bonus
     */
    calculateTimeBonus(timeDiffMs) {
        const decay = INTERSECTION_CONFIG.TIME_DECAY;

        if (timeDiffMs < decay.VERY_FAST.maxMs) {
            return decay.VERY_FAST.bonus;  // +20%
        } else if (timeDiffMs < decay.FAST.maxMs) {
            return decay.FAST.bonus;       // 正常
        } else {
            return decay.SLOW.bonus;       // -20%
        }
    }

    /**
     * 发出交集信号
     */
    emitIntersectionSignal(result) {
        this.stats.signalsEmitted++;

        console.log(`\n[SignalMerger] ════════════════════════════════════════════`);
        console.log(`[SignalMerger] ${result.intersection.emoji} 检测到 ${result.intersection.level}!`);
        console.log(`   代币: ${result.symbol} (${result.tokenCA.slice(0, 12)}...)`);
        console.log(`   猎人: ${result.intersection.hunterType || 'Shadow共识'}`);
        console.log(`   时间差: ${(result.intersection.timeDiffMs / 1000).toFixed(1)}秒`);
        console.log(`   时间加成: ${result.intersection.timeBonus}x`);
        console.log(`   最终仓位: ${result.positionMultiplier.toFixed(2)}x`);
        console.log(`   观察时间: ${(result.observeTimeMultiplier * 100).toFixed(0)}%`);
        console.log(`[SignalMerger] ════════════════════════════════════════════\n`);

        this.emit('intersection', result);
    }

    // ═══════════════════════════════════════════════════════════
    // 辅助方法
    // ═══════════════════════════════════════════════════════════

    /**
     * 清理过期信号
     */
    cleanupExpiredSignals() {
        const now = Date.now();
        const expireTime = this.config.timeWindow * 2;  // 双倍窗口后清理

        let cleaned = 0;

        for (const [tokenCA, record] of this.debotSignals.entries()) {
            if (now - record.timestamp > expireTime) {
                this.debotSignals.delete(tokenCA);
                cleaned++;
            }
        }

        for (const [tokenCA, record] of this.hunterSignals.entries()) {
            if (now - record.timestamp > expireTime) {
                this.hunterSignals.delete(tokenCA);
                cleaned++;
            }
        }

        for (const [tokenCA, record] of this.shadowSignals.entries()) {
            if (now - record.timestamp > expireTime) {
                this.shadowSignals.delete(tokenCA);
                cleaned++;
            }
        }

        // 清理已处理的交集记录 (保留最近30分钟)
        const oldKeys = [];
        for (const key of this.processedIntersections) {
            const timestamp = parseInt(key.split('_')[1]) || 0;
            if (now - timestamp > 30 * 60 * 1000) {
                oldKeys.push(key);
            }
        }
        oldKeys.forEach(k => this.processedIntersections.delete(k));

        if (cleaned > 0 && this.config.debug) {
            console.log(`[SignalMerger] 🧹 清理 ${cleaned} 个过期信号`);
        }
    }

    /**
     * 手动查询某代币是否有交集
     */
    queryIntersection(tokenCA) {
        const debot = this.debotSignals.get(tokenCA);
        const hunter = this.hunterSignals.get(tokenCA);
        const shadow = this.shadowSignals.get(tokenCA);

        return {
            hasDebot: !!debot,
            hasHunter: !!hunter,
            hasShadow: !!shadow,
            debot,
            hunter,
            shadow
        };
    }

    /**
     * 获取状态
     */
    getStatus() {
        return {
            debotSignals: this.debotSignals.size,
            hunterSignals: this.hunterSignals.size,
            shadowSignals: this.shadowSignals.size,
            processedIntersections: this.processedIntersections.size,
            stats: this.stats
        };
    }

    /**
     * 打印统计
     */
    printStats() {
        console.log(`[SignalMerger] 📊 统计:`);
        console.log(`   - DeBot接收: ${this.stats.debotReceived}`);
        console.log(`   - 猎人接收: ${this.stats.hunterReceived}`);
        console.log(`   - 影子接收: ${this.stats.shadowReceived}`);
        console.log(`   - 黄金交集: ${this.stats.goldenCross}`);
        console.log(`   - 银色交集: ${this.stats.silverCross}`);
        console.log(`   - 影子交集: ${this.stats.shadowCross}`);
        console.log(`   - 信号发出: ${this.stats.signalsEmitted}`);
    }
}

export default SignalMerger;
