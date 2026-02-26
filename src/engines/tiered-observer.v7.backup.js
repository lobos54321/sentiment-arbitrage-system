/**
 * Tiered Observer v1.0
 * 
 * 三级动态观察室 - "优胜劣汰的晋级赛机制"
 * 
 * 🥇 金池 (GOLD): 核心区，高频监控 + AI 直通车
 * 🥈 银池 (SILVER): 缓冲区，中频监控
 * 🥉 铜池 (BRONZE): 海选区，低频监控
 * 
 * 核心机制：
 * 1. 挤压替换：高分币挤掉低分币
 * 2. 动态晋级：根据聪明钱变化和价格涨幅晋级
 * 3. 差异化监控：金池优先调用 AI
 */

import { EventEmitter } from 'events';
import fs from 'fs';
import path from 'path';
import dynamicCalculator from '../utils/dynamic-calculator.js';
import { buildEntryPrompt, buildSimplifiedPrompt } from '../utils/prompt-builder.js';
import aiAnalyst from '../utils/ai-analyst.js';
import STRATEGY from '../config/strategy.js';
import { AINarrativeSystem } from '../scoring/ai-narrative-system.js';

// 三级池策略配置 v8.0 (四维评分系统：叙事+SM+市值+趋势)
const TIERED_POOL = {
    // 1. 池子容量 (总共 150 个)
    CAPACITY: {
        GOLD: 10,     // 核心区: 最多10个
        SILVER: 40,   // 缓冲区: 最多40个
        BRONZE: 100   // 海选区: 最多100个
    },

    // v6.7 标签系统 (逻辑标签，非物理数组)
    TAGS: {
        DEAD_DOG: 'DEAD_DOG',           // 死狗集中营 (不买，只监控复活)
        PVP_SURVIVOR: 'PVP_SURVIVOR',   // PVP 幸存者 (重点关注二波)
        DAILY_ALPHA: 'DAILY_ALPHA',     // 每日金狗 (重仓)
        PHOENIX: 'PHOENIX',             // 凤凰涅槃 (死而复生)
        TIER_S: 'TIER_S',               // 大叙事 S 级 (可死拿10x+)
        TIER_A: 'TIER_A',               // 大叙事 A 级 (可持3x+)
        TIER_B: 'TIER_B',               // 普通叙事 (赚50%就跑)
        STAGNANT: 'STAGNANT'            // 滞涨中 (横盘观察)
    },

    // v6.7 凤凰检测配置 (死而不僵)
    PHOENIX_CONFIG: {
        MIN_STABLE_MINUTES: 30,          // 至少横盘30分钟
        MAX_PRICE_DROP_FROM_ENTRY: -0.30, // 从入场跌幅不超过30%
        MIN_SM_HOLD: 1,                  // 至少还有1个聪明钱
        MCAP_FLOOR: 50000,               // 市值底线 $50k (死而不僵)
        PRICE_VOLATILITY_MAX: 0.05       // 价格波动<5%视为稳定
    },

    // ═══════════════════════════════════════════════════════════════
    // v8.0 四维评分系统 (叙事 + SM + 市值 + 趋势)
    // ═══════════════════════════════════════════════════════════════

    // 2. 池级门槛 (基于四维分数)
    TIER_THRESHOLD: {
        GOLD: 85,     // ≥85 进金池
        SILVER: 65,   // ≥65 进银池
        BRONZE: 50    // ≥50 进铜池，<50 不入池
    },

    // 3. 叙事分 (DeBot 5分制 × 2 = 10分制)
    NARRATIVE_SCORE: {
        LEVEL_5: { min: 10, points: 30 },  // DeBot=5 (×2=10) → +30
        LEVEL_4: { min: 8, points: 25 },   // DeBot=4 (×2=8) → +25
        LEVEL_3: { min: 6, points: 20 },   // DeBot=3 (×2=6) → +20
        LEVEL_2: { min: 4, points: 10 },   // DeBot=2 (×2=4) → +10
        LEVEL_1: { min: 0, points: 5 }     // DeBot=1 (×2=2) → +5
    },

    // 4. SM分
    SM_SCORE: {
        HIGH: { min: 5, points: 25 },
        MID: { min: 3, points: 20 },
        LOW: { min: 2, points: 10 },
        MIN: { min: 1, points: 5 }
    },

    // 5. 市值分 (结合年龄调整)
    MCAP_SCORE: {
        RANGES: [
            { maxMcap: 30000, basePoints: 25 },   // <$30K
            { maxMcap: 100000, basePoints: 20 },  // <$100K
            { maxMcap: 300000, basePoints: 10 },  // <$300K
            { maxMcap: Infinity, basePoints: 0 }  // ≥$300K
        ],
        AGE_MULTIPLIER: {
            FRESH: { maxMinutes: 30, multiplier: 1.0 },   // ≤30min: 满分
            YOUNG: { maxMinutes: 120, multiplier: 0.8 },  // ≤2h: 8折
            OLD: { maxMinutes: Infinity, multiplier: 0.6 } // >2h: 6折
        }
    },

    // 6. 趋势调整 (5分钟观察窗口)
    TREND_ADJUSTMENT: {
        OBSERVE_WINDOW_MS: 5 * 60 * 1000,  // 5分钟窗口
        BULLISH: { priceChange: 0.10, smDelta: 0, points: 10 },   // 涨>10% 且 SM不跌 → +10
        BEARISH: { priceChange: -0.20, smDelta: -1, points: -15 } // 跌>20% 且 SM跑 → -15
    },

    // 7. 高SM保护 (防止优质币被降级)
    HIGH_SM_PROTECTION: 5,  // SM≥5 的币不会被降到铜池

    // 8. 最小观察时间 (统一5分钟用于趋势计算)
    MIN_OBSERVE_TIME: {
        ALL: 5 * 60 * 1000  // 所有池子统一5分钟观察期
    },

    // 9. 🎓 毕业答辩标准 (v6.6 收紧!)
    GRADUATION_REQ: {
        GOLD: {
            MIN_SM_DELTA: 0,
            MAX_PRICE_DROP: -0.05,
            REQUIRE_STABLE_TREND: true
        }
    }
};


class TieredObserver extends EventEmitter {
    constructor(config = {}) {
        super();
        this.config = config;
        this.tokens = new Map();
        this.params = TIERED_POOL;
        this.checkInterval = null;
        this.solService = null;
        this.bscService = null;

        // v7.4.5 AI调用优化: 拒绝冒却期
        this.rejectedTokens = new Map();  // tokenCA → { timestamp, reason, cooldown }
        this.COOLDOWN_BY_REASON = {
            '市值过大': 4 * 60 * 60 * 1000,    // 4小时
            '流动性不足': 1 * 60 * 60 * 1000,  // 1小时
            '叙事衰退': 6 * 60 * 60 * 1000,    // 6小时
            'SM流出': 30 * 60 * 1000,          // 30分钟
            '价格急跌': 1 * 60 * 60 * 1000,    // 1小时
            'default': 2 * 60 * 60 * 1000        // 默认2小时
        };
        this.REJECTED_TOKENS_PATH = 'logs/rejected-tokens.json';

        // v7.4.6: 毕业中的代币锁 (防止并发重复毕业)
        this.graduatingTokens = new Set();

        // 启动时加载拒绝记录
        this.loadRejectedTokens();

        console.log(`🏟️  [TieredObserver v8.0] 四维评分观察室初始化完成`);
        console.log(`   金池门槛: ≥${this.params.TIER_THRESHOLD.GOLD}分 | 容量: ${this.params.CAPACITY.GOLD}`);
        console.log(`   银池门槛: ≥${this.params.TIER_THRESHOLD.SILVER}分 | 容量: ${this.params.CAPACITY.SILVER}`);
        console.log(`   铜池门槛: ≥${this.params.TIER_THRESHOLD.BRONZE}分 | 容量: ${this.params.CAPACITY.BRONZE}`);
        console.log(`   📊 评分维度: 叙事 + SM + 市值(年龄调整) + 趋势(5min窗口)`);
        console.log(`   🛡️ v7.4.5 拒绝冒却期: 已加载 ${this.rejectedTokens.size} 条记录`);
    }

    /**
     * 绑定快照服务
     */
    bindServices(solService, bscService) {
        this.solService = solService;
        this.bscService = bscService;
    }

    /**
     * v6.9: 绑定 AI 叙事系统 (毕业答辩叙事验证)
     * @param {AINarrativeSystem} narrativeSystem - AI 叙事系统实例
     */
    bindNarrativeSystem(narrativeSystem) {
        this.narrativeSystem = narrativeSystem;
        console.log('🎭 [TieredObserver] AI 叙事系统已绑定 (v6.9 毕业答辩增强)');
    }

    /**
     * v6.5: 绑定容量检查器 (由 index.js 注入 CrossValidator)
     */
    bindCapacityChecker(checker) {
        this.capacityChecker = checker;
        console.log('🏦 [TieredObserver] 容量检查器已绑定 (v6.5)');
    }

    // ═══════════════════════════════════════════════════════════════
    // v8.0 四维评分方法
    // ═══════════════════════════════════════════════════════════════

    /**
     * 计算四维池级分数 (叙事 + SM + 市值 + 趋势)
     * @param {Object} record - 代币记录
     * @returns {number} 总分 (0-100)
     */
    calculateTierScore(record) {
        const narrativeScore = this.getNarrativeScore(record);
        const smScore = this.getSmScore(record);
        const mcapScore = this.getMcapScore(record);
        const trendAdjustment = this.getTrendAdjustment(record);

        const totalScore = narrativeScore + smScore + mcapScore + trendAdjustment;

        // 存储分数明细供调试
        record.tierScoreBreakdown = {
            narrative: narrativeScore,
            sm: smScore,
            mcap: mcapScore,
            trend: trendAdjustment,
            total: totalScore
        };

        return totalScore;
    }

    /**
     * 获取叙事分 (DeBot 5分制 × 2)
     */
    getNarrativeScore(record) {
        // 从 aiReport 获取 DeBot 叙事评分 (1-5分制)
        const debotScore = record.aiReport?.score || record.analysis?.aiReport?.score || 3;
        const normalizedScore = debotScore * 2; // 转为10分制

        const config = this.params.NARRATIVE_SCORE;
        if (normalizedScore >= config.LEVEL_5.min) return config.LEVEL_5.points;
        if (normalizedScore >= config.LEVEL_4.min) return config.LEVEL_4.points;
        if (normalizedScore >= config.LEVEL_3.min) return config.LEVEL_3.points;
        if (normalizedScore >= config.LEVEL_2.min) return config.LEVEL_2.points;
        return config.LEVEL_1.points;
    }

    /**
     * 获取SM分
     */
    getSmScore(record) {
        const sm = record.current?.smartMoney || record.initial?.smartMoney || 0;
        const config = this.params.SM_SCORE;

        if (sm >= config.HIGH.min) return config.HIGH.points;
        if (sm >= config.MID.min) return config.MID.points;
        if (sm >= config.LOW.min) return config.LOW.points;
        if (sm >= config.MIN.min) return config.MIN.points;
        return 0;
    }

    /**
     * 获取市值分 (结合年龄调整)
     */
    getMcapScore(record) {
        const mcap = record.current?.marketCap || record.initial?.marketCap || 0;
        const ageMinutes = (Date.now() - record.startTime) / 60000;

        // 查找市值档位
        const config = this.params.MCAP_SCORE;
        let basePoints = 0;
        for (const range of config.RANGES) {
            if (mcap < range.maxMcap) {
                basePoints = range.basePoints;
                break;
            }
        }

        // 应用年龄乘数
        let multiplier = config.AGE_MULTIPLIER.OLD.multiplier;
        if (ageMinutes <= config.AGE_MULTIPLIER.FRESH.maxMinutes) {
            multiplier = config.AGE_MULTIPLIER.FRESH.multiplier;
        } else if (ageMinutes <= config.AGE_MULTIPLIER.YOUNG.maxMinutes) {
            multiplier = config.AGE_MULTIPLIER.YOUNG.multiplier;
        }

        return Math.round(basePoints * multiplier);
    }

    /**
     * 获取趋势调整分 (5分钟窗口)
     */
    getTrendAdjustment(record) {
        const config = this.params.TREND_ADJUSTMENT;
        const observeTime = Date.now() - record.startTime;

        // 观察不足5分钟，趋势分为0
        if (observeTime < config.OBSERVE_WINDOW_MS) {
            return 0;
        }

        // 获取5分钟内的价格变化
        const recentSnapshots = record.priceSnapshots?.filter(
            s => Date.now() - s.time < config.OBSERVE_WINDOW_MS
        ) || [];

        if (recentSnapshots.length < 2) return 0;

        const oldPrice = recentSnapshots[0].price;
        const newPrice = record.current?.price || recentSnapshots[recentSnapshots.length - 1].price;

        if (!oldPrice || oldPrice === 0) return 0;

        const priceChange = (newPrice - oldPrice) / oldPrice;
        const smDelta = (record.current?.smartMoney || 0) - (record.initial?.smartMoney || 0);

        // 5min涨>10% 且 SM不跌 → +10
        if (priceChange >= config.BULLISH.priceChange && smDelta >= config.BULLISH.smDelta) {
            return config.BULLISH.points;
        }

        // 5min跌>20% 且 SM流出 → -15
        if (priceChange <= config.BEARISH.priceChange && smDelta <= config.BEARISH.smDelta) {
            return config.BEARISH.points;
        }

        return 0;
    }

    /**
     * 根据四维分数确定池级
     */
    getTierFromScore(score) {
        const threshold = this.params.TIER_THRESHOLD;
        if (score >= threshold.GOLD) return 'GOLD';
        if (score >= threshold.SILVER) return 'SILVER';
        if (score >= threshold.BRONZE) return 'BRONZE';
        return null; // 不入池
    }

    /**
     * 📥 新币入场: 自动分级 + 挤压替换
     * v7.4.3: 改为 async 以支持入池时获取完整快照
     */
    async addToken(signal, analysis) {
        const tokenCA = signal.token_ca;

        // v7.4.5: 检查是否在拒绝冒却期
        if (this.rejectedTokens.has(tokenCA)) {
            const rejection = this.rejectedTokens.get(tokenCA);
            const elapsed = Date.now() - rejection.timestamp;
            const cooldown = rejection.cooldown || this.COOLDOWN_BY_REASON.default;

            if (elapsed < cooldown) {
                const remaining = Math.ceil((cooldown - elapsed) / 60000);
                console.log(`   ⏭️ [AI冒却] ${signal.symbol} 拒绝过 (${rejection.reason})，${remaining}分钟后可重试`);
                return;  // 不入池
            } else {
                // 冒却期结束，清除记录
                this.rejectedTokens.delete(tokenCA);
            }
        }

        if (this.tokens.has(tokenCA)) {
            // 已存在，重新计算分数检查晋级
            const existing = this.tokens.get(tokenCA);
            this.checkTierChange(existing);
            return;
        }

        // 提取初始快照数据
        const initialSnapshot = {
            price: signal.price || analysis?.token?.price || 0,
            smartMoney: signal.smart_wallet_online || analysis?.token?.smartWalletOnline || 0,
            liquidity: signal.liquidity || analysis?.token?.liquidity || 0,
            holders: signal.holders || analysis?.token?.holders || 0,
            marketCap: signal.marketCap || analysis?.token?.marketCap || 0
        };

        // 创建临时记录用于计算初始分数
        const tempRecord = {
            startTime: Date.now(),
            initial: initialSnapshot,
            current: { ...initialSnapshot },
            priceSnapshots: [],
            aiReport: analysis?.aiReport || signal.aiReport || null,
            analysis: analysis
        };

        // v8.0 使用四维评分计算初始池级
        const tierScore = this.calculateTierScore(tempRecord);
        const tier = this.getTierFromScore(tierScore);

        // 分数不够，不入池
        if (!tier) {
            console.log(`   ⏭️ [Filter] ${signal.symbol} 四维分 ${tierScore} < ${this.params.TIER_THRESHOLD.BRONZE}，不入池`);
            return;
        }

        // 检查容量 & 执行挤压
        const canEnter = this.ensureCapacity(tier, tierScore);
        if (!canEnter) {
            console.log(`   ⏭️ [Filter] ${signal.symbol} 分数不足，无法入池`);
            return;
        }

        // 3. 记录入池快照
        const initialSnapshot = {
            price: signal.price || analysis?.token?.price || 0,
            smartMoney: signal.smart_wallet_online || analysis?.token?.smartWalletOnline || 0,
            liquidity: signal.liquidity || analysis?.token?.liquidity || 0,
            holders: signal.holders || analysis?.token?.holders || 0,
            marketCap: signal.marketCap || analysis?.token?.marketCap || 0
        };

        // v7.4.3: 如果快照被推迟 (snapshotDeferred)，在入池时获取完整链上数据
        const wasDeferred = analysis?.token?.snapshotDeferred || signal.snapshotDeferred;
        if (wasDeferred && (this.solService || this.bscService)) {
            console.log(`   📡 [v7.4.3] 获取推迟的完整快照...`);
            try {
                const service = signal.chain === 'SOL' ? this.solService : this.bscService;
                if (service) {
                    const snapshot = await service.getSnapshot(tokenCA);
                    if (snapshot) {
                        initialSnapshot.liquidity = snapshot.liquidity_usd || initialSnapshot.liquidity;
                        initialSnapshot.price = snapshot.current_price || initialSnapshot.price;
                        initialSnapshot.marketCap = snapshot.market_cap || initialSnapshot.marketCap;
                        console.log(`   ✅ 完整快照: 流动性=$${(initialSnapshot.liquidity / 1000).toFixed(1)}K | MC=$${(initialSnapshot.marketCap / 1000).toFixed(1)}K`);
                    }
                }
            } catch (e) {
                console.log(`   ⚠️ 完整快照获取失败: ${e.message}`);
            }
        }

        // 4. 正式记录
        const tierEmoji = tier === 'GOLD' ? '🥇' : tier === 'SILVER' ? '🥈' : '🥉';
        console.log(`📥 [TieredObserver] ${tierEmoji} ${signal.symbol} -> ${tier}池 (${score}分)`);
        console.log(`   📊 快照: SM=${initialSnapshot.smartMoney} | 流动性=$${(initialSnapshot.liquidity / 1000).toFixed(1)}K`);

        this.tokens.set(tokenCA, {
            signal,
            analysis,
            symbol: signal.symbol,
            address: tokenCA,
            chain: signal.chain,
            tier: tier,
            score: score,
            tag: factors.tag || 'NORMAL',
            // v6.7 标签系统
            tags: new Set(),                      // 多标签集合
            intentionTier: null,                  // 叙事等级: S/A/B/C
            startTime: Date.now(),
            lastUpdateTime: Date.now(),
            initial: initialSnapshot,
            current: { ...initialSnapshot },
            priceSnapshots: [],
            klineData: signal.klineData || [],
            aiReport: analysis?.aiReport || signal.aiReport || null,
            // v6.7 凤凰检测
            stableStartTime: null,                // 开始横盘的时间
            highWaterMark: initialSnapshot.price, // 历史最高价
            wasInDeadDog: false,                  // 是否曾经进入死狗池
            // v7.4.5 交集信号增强
            observeTimeMultiplier: analysis?.token?.intersectionBoost?.observeTimeMultiplier ||
                signal.intersectionBoost?.observeTimeMultiplier || 1.0,
            intersectionBoost: analysis?.token?.intersectionBoost || signal.intersectionBoost || null
        });
    }

    /**
     * 🌊 挤压逻辑 (核心算法)
     * 如果目标池满了，踢掉该池分数最低的，把它降级到下一层
     * @returns {boolean} 是否可以入池
     */
    ensureCapacity(targetTier, incomingScore) {
        const pool = this.getTokensByTier(targetTier);

        if (pool.length < this.params.CAPACITY[targetTier]) {
            return true; // 没满，直接进
        }

        // 1. 找到当前池最弱的币
        pool.sort((a, b) => a.score - b.score);
        const weakest = pool[0];

        // 2. PK: 如果新来的比最弱的还弱，尝试降级入池
        if (incomingScore <= weakest.score) {
            const nextTier = this.getNextTierDown(targetTier);
            if (nextTier) {
                return this.ensureCapacity(nextTier, incomingScore);
            }
            return false; // 铜池也满了，且分数不够
        }

        // 3. 替换: 把最弱的踢下去
        const nextTier = this.getNextTierDown(targetTier);
        if (nextTier) {
            console.log(`📉 [降级] ${weakest.symbol} 被挤出 ${targetTier} -> ${nextTier}`);
            weakest.tier = nextTier;
            this.ensureCapacity(nextTier, weakest.score);
        } else {
            console.log(`👋 [淘汰] ${weakest.symbol} 被挤出系统`);
            this.tokens.delete(weakest.address);
        }

        return true;
    }

    /**
     * 启动定时检查
     */
    start() {
        if (this.checkInterval) return;

        // 差异化检查频率
        this.checkInterval = setInterval(() => {
            this.checkAllSignals();
        }, 15 * 1000); // 每 15 秒检查一次

        console.log(`▶️  TieredObserver started`);
    }

    stop() {
        if (this.checkInterval) {
            clearInterval(this.checkInterval);
            this.checkInterval = null;
        }
        console.log(`⏹️  TieredObserver stopped`);
    }

    /**
     * 🔄 检查所有信号
     */
    async checkAllSignals() {
        const now = Date.now();
        const tokens = Array.from(this.tokens.values());

        // 按层级优先级处理：金 > 银 > 铜
        const goldPool = tokens.filter(t => t.tier === 'GOLD');
        const silverPool = tokens.filter(t => t.tier === 'SILVER');
        const bronzePool = tokens.filter(t => t.tier === 'BRONZE');

        // 金池：每次都检查
        for (const record of goldPool) {
            await this.updateAndCheck(record, now);
        }

        // 银池：每 2 轮检查一次
        if (Math.floor(now / 30000) % 2 === 0) {
            for (const record of silverPool) {
                await this.updateAndCheck(record, now);
            }
        }

        // 铜池：每 4 轮检查一次
        if (Math.floor(now / 60000) % 4 === 0) {
            for (const record of bronzePool) {
                await this.updateAndCheck(record, now);
            }
        }

        // 保存状态到 JSON 文件供 Dashboard 读取 (每 30 秒保存一次)
        if (Math.floor(now / 30000) % 2 === 0) {
            this.savePoolStatus();
        }
    }

    /**
     * 保存池子状态到文件 (供 Dashboard 和 BatchAIAdvisor 读取)
     */
    savePoolStatus() {
        try {
            const tokens = Array.from(this.tokens.values()).map(t => ({
                symbol: t.symbol,
                address: t.address,
                chain: t.chain,
                tier: t.tier,
                score: t.score,
                observeMinutes: Math.floor((Date.now() - t.startTime) / 60000),
                smInitial: t.initial.smartMoney,
                smCurrent: t.current.smartMoney,
                smartMoney: t.current.smartMoney,  // v8.0 alias for BatchAIAdvisor
                priceChange: t.initial.price > 0
                    ? ((t.current.price - t.initial.price) / t.initial.price * 100).toFixed(1)
                    : '0.0',
                // v8.0 添加 MCAP 和 liquidity 供 AI 分析
                marketCap: t.current.marketCap || 0,
                liquidity: t.current.liquidity || 0,
                tag: t.analysis?.dynamicFactors?.tag || 'NORMAL'
            }));

            // 按分数降序排列
            tokens.sort((a, b) => b.score - a.score);

            const poolData = {
                counts: {
                    total: tokens.length,
                    gold: tokens.filter(t => t.tier === 'GOLD').length,
                    silver: tokens.filter(t => t.tier === 'SILVER').length,
                    bronze: tokens.filter(t => t.tier === 'BRONZE').length
                },
                tokens: tokens,
                lastUpdate: new Date().toISOString()
            };

            const filePath = path.join(process.cwd(), 'data', 'observation_pool.json');
            fs.writeFileSync(filePath, JSON.stringify(poolData, null, 2));
        } catch (e) {
            console.error('❌ Failed to save pool status:', e.message);
        }
    }

    /**
     * 更新并检查单个代币
     */
    async updateAndCheck(record, now) {
        const observeTime = now - record.startTime;
        // v7.4.5: 应用交集信号的观察时间倍率
        const observeTimeMultiplier = record.observeTimeMultiplier || 1.0;
        const minTime = this.params.MIN_OBSERVE_TIME[record.tier] * observeTimeMultiplier;

        // 1. 获取最新数据
        try {
            const service = record.chain === 'SOL' ? this.solService : this.bscService;
            if (service) {
                const snapshot = await service.getSnapshot(record.address);
                if (snapshot) {
                    record.current = {
                        price: snapshot.current_price || snapshot.price || record.current.price,
                        smartMoney: snapshot.smart_wallet_count || snapshot.smartWalletOnline || record.current.smartMoney,
                        liquidity: snapshot.liquidity_usd || snapshot.liquidity || record.current.liquidity,
                        holders: snapshot.holder_count || snapshot.holders || record.initial.holders,
                        marketCap: snapshot.market_cap || snapshot.marketCap || record.current.marketCap
                    };
                    record.lastUpdateTime = now;
                }
            }
        } catch (e) {
            // 使用 initial 作为后备
        }

        // 2. 计算变化量
        const smDelta = (record.current.smartMoney || 0) - (record.initial.smartMoney || 0);
        const priceDelta = record.initial.price > 0
            ? ((record.current.price - record.initial.price) / record.initial.price * 100)
            : 0;

        // 3. 晋级检查
        this.checkPromotion(record, smDelta, priceDelta);

        // 4. 降级/淘汰检查 (v6.2: 传递 priceDelta)
        this.checkEviction(record, smDelta, priceDelta, observeTime);

        // ═══════════════════════════════════════════════════════════════
        // v6.7 标签系统检查 (势利眼 + 拾荒者)
        // ═══════════════════════════════════════════════════════════════

        // 5a. 死狗检测 (进入死狗池)
        const isDeadDog = this.checkDeadDog(record, smDelta, priceDelta, observeTime);

        // 5b. 凤凰检测 (死而复生) - 只检查死狗池中的币
        if (isDeadDog || record.wasInDeadDog) {
            const isPhoenix = this.checkPhoenix(record, smDelta, priceDelta, observeTime);
            if (isPhoenix) {
                // v7.4.6: 禁用旧毕业流程，现在由 BatchAIAdvisor 批量决策
                // await this.checkGraduation(record, smDelta, priceDelta, observeTime);
                console.log(`   🐦 [凤凰] ${record.symbol} 复活信号，等待 BatchAIAdvisor 决策`);
                return;
            }
        }

        // 5c. 滞涨检测 (横盘标记)
        this.checkStagnant(record, priceDelta, observeTime);

        // 5d. 记录价格快照 (供凤凰检测用)
        record.priceSnapshots.push({
            time: now,
            price: record.current.price,
            sm: record.current.smartMoney
        });
        // 只保留最近20个快照
        if (record.priceSnapshots.length > 20) {
            record.priceSnapshots.shift();
        }

        // 5e. 更新历史最高价
        if (record.current.price > (record.highWaterMark || 0)) {
            record.highWaterMark = record.current.price;
        }

        // 6. v7.4.6: 禁用旧毕业流程，现在由 BatchAIAdvisor 每10分钟批量决策
        // const TAGS = this.params.TAGS;
        // const inDeadDog = this.hasTag(record, TAGS.DEAD_DOG);
        // if (observeTime >= minTime && !inDeadDog) {
        //     await this.checkGraduation(record, smDelta, priceDelta, observeTime);
        // }
    }

    /**
     * 🚀 晋级检查
     */
    checkPromotion(record, smDelta = 0, priceDelta = 0) {
        if (record.tier === 'BRONZE') {
            if (smDelta >= this.params.PROMOTION.TO_SILVER.smDelta ||
                priceDelta >= this.params.PROMOTION.TO_SILVER.priceDelta) {
                this.promote(record, 'SILVER', `SM+${smDelta}/涨${priceDelta.toFixed(1)}%`);
            }
        } else if (record.tier === 'SILVER') {
            if (smDelta >= this.params.PROMOTION.TO_GOLD.smDelta ||
                priceDelta >= this.params.PROMOTION.TO_GOLD.priceDelta) {
                this.promote(record, 'GOLD', `SM+${smDelta}/涨${priceDelta.toFixed(1)}%`);
            }
        }
    }

    promote(record, newTier, reason) {
        const canPromote = this.ensureCapacity(newTier, record.score + 10);
        if (canPromote) {
            const oldTier = record.tier;
            record.tier = newTier;
            record.score += 10;
            console.log(`🚀 [晋级] ${record.symbol} ${oldTier} -> ${newTier} (${reason})`);
        }
    }

    /**
     * 📉 降级/淘汰检查 (v6.2 快速反应)
     */
    checkEviction(record, smDelta, priceDelta, observeTime) {
        const eviction = this.params.EVICTION;

        // 🥇 金池降级规则: SM-2 或 价格跌15%
        if (record.tier === 'GOLD') {
            if (smDelta <= eviction.DROP_GOLD.smDelta || priceDelta <= eviction.DROP_GOLD.priceDelta) {
                console.log(`📉 [降级] ${record.symbol} GOLD -> SILVER (SM${smDelta}/价格${priceDelta.toFixed(1)}%)`);
                record.tier = 'SILVER';
                return;
            }
        }

        // 🥈 银池降级规则: SM-1 且 价格跌10%
        if (record.tier === 'SILVER') {
            if (smDelta <= eviction.DROP_SILVER.smDelta && priceDelta <= eviction.DROP_SILVER.priceDelta) {
                console.log(`📉 [降级] ${record.symbol} SILVER -> BRONZE (SM${smDelta}/价格${priceDelta.toFixed(1)}%)`);
                record.tier = 'BRONZE';
                return;
            }
        }

        // 🥉 铜池超时淘汰
        if (record.tier === 'BRONZE' && observeTime > eviction.MAX_IDLE_TIME) {
            if (smDelta <= 0) {
                console.log(`👋 [超时] ${record.symbol} 铜池超时淘汰`);
                this.tokens.delete(record.address);
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // v6.7 标签管理系统 (势利眼 + 拾荒者)
    // ═══════════════════════════════════════════════════════════════

    /**
     * 🏷️ 添加标签
     */
    addTag(record, tag) {
        if (!record.tags) record.tags = new Set();
        record.tags.add(tag);
        console.log(`   🏷️ [Tag] ${record.symbol} +${tag}`);
    }

    /**
     * 🏷️ 移除标签
     */
    removeTag(record, tag) {
        if (record.tags) {
            record.tags.delete(tag);
            console.log(`   🏷️ [Tag] ${record.symbol} -${tag}`);
        }
    }

    /**
     * 🏷️ 检查是否有标签
     */
    hasTag(record, tag) {
        return record.tags && record.tags.has(tag);
    }

    /**
     * 🐕 v6.7 死狗检测 - 进入死狗池的条件
     * 条件：跌幅 > 50% 且 SM = 0 且 观察超过15分钟
     */
    checkDeadDog(record, smDelta, priceDelta, observeTime) {
        const TAGS = this.params.TAGS;
        const isSignificantLoss = priceDelta < -50;
        const noSmartMoney = (record.current.smartMoney || 0) === 0;
        const observedEnough = observeTime > 15 * 60 * 1000;

        if (isSignificantLoss && noSmartMoney && observedEnough) {
            if (!this.hasTag(record, TAGS.DEAD_DOG)) {
                this.addTag(record, TAGS.DEAD_DOG);
                record.wasInDeadDog = true;
                console.log(`   💀 [死狗池] ${record.symbol} 进入死狗集中营 (跌${priceDelta.toFixed(1)}%, SM=0)`);
            }
            return true;
        }
        return false;
    }

    /**
     * 🔥 v6.7 凤凰检测 - 死而不僵，PVP幸存者
     * 条件：曾经进入死狗池 → 现在稳定横盘 + SM回流 → 标记为 PHOENIX
     */
    checkPhoenix(record, smDelta, priceDelta, observeTime) {
        const TAGS = this.params.TAGS;
        const config = this.params.PHOENIX_CONFIG;
        const now = Date.now();

        // 只检查曾经进入死狗池的币
        if (!record.wasInDeadDog && !this.hasTag(record, TAGS.DEAD_DOG)) {
            return false;
        }

        const currentSM = record.current.smartMoney || 0;
        const currentMcap = record.current.marketCap || 0;
        const currentPrice = record.current.price || 0;

        // 1️⃣ 检查是否达到凤凰条件
        const hasSM = currentSM >= config.MIN_SM_HOLD;
        const mcapAboveFloor = currentMcap >= config.MCAP_FLOOR;

        // 2️⃣ 检查价格稳定性 (横盘检测)
        let isStable = false;
        if (record.priceSnapshots && record.priceSnapshots.length >= 3) {
            const recentPrices = record.priceSnapshots.slice(-5).map(s => s.price).filter(p => p > 0);
            if (recentPrices.length >= 3) {
                const avgPrice = recentPrices.reduce((a, b) => a + b, 0) / recentPrices.length;
                const maxDeviation = Math.max(...recentPrices.map(p => Math.abs(p - avgPrice) / avgPrice));
                isStable = maxDeviation < config.PRICE_VOLATILITY_MAX;

                // 记录稳定开始时间
                if (isStable && !record.stableStartTime) {
                    record.stableStartTime = now;
                } else if (!isStable) {
                    record.stableStartTime = null;
                }
            }
        }

        // 3️⃣ 检查稳定时长
        const stableMinutes = record.stableStartTime
            ? (now - record.stableStartTime) / 60000
            : 0;
        const stableEnough = stableMinutes >= config.MIN_STABLE_MINUTES;

        // 4️⃣ 凤凰涅槃！
        if (hasSM && mcapAboveFloor && isStable && stableEnough) {
            // 从死狗池移除，标记为凤凰
            this.removeTag(record, TAGS.DEAD_DOG);
            this.addTag(record, TAGS.PHOENIX);
            this.addTag(record, TAGS.PVP_SURVIVOR);

            console.log(`\n🔥🔥🔥 [凤凰涅槃] ${record.symbol} 死而复生！`);
            console.log(`   📊 MCAP: $${(currentMcap / 1000).toFixed(1)}K | SM: ${currentSM} | 横盘: ${stableMinutes.toFixed(0)}min`);

            // 提升到金池
            if (record.tier !== 'GOLD') {
                record.tier = 'GOLD';
                record.score += 20; // 凤凰加分
                console.log(`   🚀 [晋级] ${record.symbol} -> GOLD池 (凤凰加成 +20分)`);
            }

            return true;
        }

        return false;
    }

    /**
     * 📈 v6.7 滞涨检测 - 横盘但不死
     * 用于标记那些"不死不活"的币，后续可能变凤凰
     */
    checkStagnant(record, priceDelta, observeTime) {
        const TAGS = this.params.TAGS;
        const observeMinutes = observeTime / 60000;

        // 横盘定义：价格变化在 -10% ~ +10% 之间，且观察超过20分钟
        const isStagnant = priceDelta > -10 && priceDelta < 10 && observeMinutes > 20;

        if (isStagnant && !this.hasTag(record, TAGS.STAGNANT)) {
            this.addTag(record, TAGS.STAGNANT);
        } else if (!isStagnant && this.hasTag(record, TAGS.STAGNANT)) {
            this.removeTag(record, TAGS.STAGNANT);
        }
    }

    /**
     * 🎓 毕业检查 (v6.2 毕业答辩机制)
     * 不再是"自动毕业"，而是要通过"答辩"
     */
    async checkGraduation(record, smDelta, priceDelta, observeTime) {
        // v7.4.6: 防止并发重复毕业 - 检查是否已在毕业进行中
        if (this.graduatingTokens.has(record.address)) {
            console.log(`   🔒 [锁定] ${record.symbol} 毕业进行中，跳过重复检查`);
            return;
        }

        const observeMinutes = (observeTime / 60000).toFixed(1);
        const gradReq = this.params.GRADUATION_REQ[record.tier];

        // 计算动态因子 (提前计算，供多处使用)
        const factors = dynamicCalculator.calculateFactors({
            smartMoney: record.current.smartMoney,
            signalCount: record.analysis?.alertCount || 1,
            liquidity: record.current.liquidity,
            marketCap: record.current.marketCap,
            avgBuyAmount: record.signal?.avgBuyAmount || 0.5
        });

        // ═══════════════════════════════════════════════════════════════
        // 🚑 银池直通车 (Silver Emergency Exit)
        // 条件：GOLDEN标签 + SM激增(+4) + 价格暴涨(+20%) = 直接毕业
        // 用于捕捉"马斯克发推"式的突发爆发行情
        // ═══════════════════════════════════════════════════════════════
        const emergencyConfig = STRATEGY.SILVER_EMERGENCY || {};
        if (record.tier === 'SILVER' && emergencyConfig.ENABLED !== false) {
            const requiredTag = emergencyConfig.REQUIRE_TAG || 'GOLDEN';
            const minSmDelta = emergencyConfig.MIN_SM_DELTA || 4;
            const minPriceGain = emergencyConfig.MIN_PRICE_GAIN || 20;

            const isGoldenTag = factors.tag === requiredTag;
            const isSMExplosion = smDelta >= minSmDelta;
            const isPriceExplosion = priceDelta >= minPriceGain;

            if (isGoldenTag && isSMExplosion && isPriceExplosion) {
                console.log(`\n🚨🚨🚨 [银池直通车] ${record.symbol} 触发紧急通道！`);
                console.log(`   💎 条件: ${requiredTag}标签 ✓ | SM+${smDelta}≥${minSmDelta} ✓ | 涨幅${priceDelta.toFixed(1)}%≥${minPriceGain}% ✓`);
                console.log(`   ⚡ 资金抢筹+价格暴涨双重确认，破格录取！`);

                // 直接走毕业流程，不等晋级
                await this.executeGraduation(record, factors, observeMinutes, 'SILVER_EMERGENCY');
                return;
            }
        }

        // ═══════════════════════════════════════════════════════════════
        // 🎓 常规毕业答辩 (Final Exam) - 仅限金池
        // ═══════════════════════════════════════════════════════════════

        // 铜池和普通银池不能直接毕业
        if (!gradReq) return;

        // 1️⃣ 资金门槛 (SM Check)
        if (smDelta < gradReq.MIN_SM_DELTA) {
            // console.log(`   🛑 [延毕] ${record.symbol} 聪明钱流出过大 (${smDelta} < ${gradReq.MIN_SM_DELTA})`);
            return;
        }

        // 2️⃣ 价格门槛 (Price Check)
        const pricePct = priceDelta / 100;
        if (record.tier === 'GOLD' && pricePct < gradReq.MAX_PRICE_DROP) {
            console.log(`   🛑 [延毕] ${record.symbol} 价格走势太弱 (${priceDelta.toFixed(1)}% < ${gradReq.MAX_PRICE_DROP * 100}%)`);
            return;
        }

        // 3️⃣ 趋势稳定检查 (防止正在急跌)
        if (gradReq.REQUIRE_STABLE_TREND && record.priceSnapshots && record.priceSnapshots.length >= 2) {
            const lastPrice = record.priceSnapshots[record.priceSnapshots.length - 1];
            const prevPrice = record.priceSnapshots[record.priceSnapshots.length - 2];
            if (lastPrice && prevPrice && lastPrice.price < prevPrice.price * 0.98) {
                console.log(`   🛑 [延毕] ${record.symbol} 正在急跌中，暂缓毕业`);
                return;
            }
        }

        // 4️⃣ v6.6 回撤检查 - 拒绝接飞刀
        // 如果当前价比观察期内的最高价回撤超过 8%，暂缓毕业
        if (record.priceSnapshots && record.priceSnapshots.length >= 3) {
            const prices = record.priceSnapshots.map(s => s.price || 0).filter(p => p > 0);
            if (prices.length >= 3) {
                const recentHigh = Math.max(...prices);
                const currentPrice = record.current.price || prices[prices.length - 1];
                const drawdown = (recentHigh - currentPrice) / recentHigh;

                if (drawdown > 0.08) {
                    console.log(`   📉 [延毕] ${record.symbol} 正在回调中 (回撤 ${(drawdown * 100).toFixed(1)}% 从高点 $${recentHigh.toFixed(10)})，等待企稳...`);
                    return;
                }
            }
        }

        // ═══════════════════════════════════════════════════════════════
        // ✅ 通过毕业答辩，进入 AI 面试环节
        // ═══════════════════════════════════════════════════════════════
        await this.executeGraduation(record, factors, observeMinutes, 'GOLD_STANDARD');
    }

    /**
     * 🎓 执行毕业流程 (AI 面试 + 发出毕业事件)
     * 供 金池常规毕业 和 银池越级毕业 共用
     */
    async executeGraduation(record, factors, observeMinutes, graduationType) {
        const smDelta = record.current.smartMoney - record.initial.smartMoney;
        const priceDelta = record.initial.price > 0
            ? ((record.current.price - record.initial.price) / record.initial.price * 100)
            : 0;

        console.log(`\n🎓 [TieredObserver] ${record.symbol} 通过毕业答辩，准备面试...`);
        console.log(`   🏅 层级: ${record.tier} | 类型: ${graduationType} | 观察: ${observeMinutes}min`);
        console.log(`   📊 SM: ${record.initial.smartMoney} → ${record.current.smartMoney} (${smDelta >= 0 ? '+' : ''}${smDelta})`);
        console.log(`   📈 涨幅: ${priceDelta.toFixed(1)}%`);
        console.log(`   🧮 特征: [${factors.tag}] ${factors.reason}`);

        // ═══════════════════════════════════════════════════════════════
        // 🏦 v6.5 容量预审 (Capacity Pre-check)
        // 在调用 AI 之前先检查是否有座位，省 AI 调用费用！
        // 普通席位 (1-6): 任何标签都可以
        // VIP 席位 (7-8): 必须是 GOLDEN 标签
        // ═══════════════════════════════════════════════════════════════
        if (this.capacityChecker) {
            const capacityResult = this.capacityChecker.checkCapacity(factors, record.chain);

            if (!capacityResult.allow) {
                console.log(`⛔ [拒绝面试] ${record.symbol} 毕业但无仓位: ${capacityResult.reason}`);
                console.log(`   📋 类型: ${capacityResult.type} | 标签: ${factors.tag}`);

                // v7.4.6: 仓位满时设置冷却期并删除，防止无限循环尝试毕业
                const capacityCooldown = 10 * 60 * 1000;  // 10分钟冷却期
                this.rejectedTokens.set(record.address, {
                    timestamp: Date.now(),
                    reason: `仓位满 (${capacityResult.reason})`,
                    cooldown: capacityCooldown,
                    symbol: record.symbol
                });
                this.saveRejectedTokens();
                console.log(`   🧊 [冷却] 设置${Math.ceil(capacityCooldown / 60000)}分钟冷却期，等待仓位释放`);

                this.tokens.delete(record.address);  // 删除后不会无限循环
                return;
            }

            console.log(`🎟️ [面试资格] ${record.symbol} 获得 ${capacityResult.type} 席位！`);
        } else {
            console.log(`⚠️ [容量检查器未绑定] 跳过容量预审`);
        }

        // ═══════════════════════════════════════════════════════════════
        // 🎭 v6.9 AI 叙事验证 (Narrative Verification)
        // 在 AI 决策前，先验证叙事是否处于健康阶段
        // 拒绝衰退期叙事，为上升期叙事加分
        // v7.4.5: 如果已有 DeBot aiReport，跳过额外的 AI 调用
        // ═══════════════════════════════════════════════════════════════
        let narrativeVerification = null;

        // v7.4.5: 检查是否已有 DeBot 叙事信息，避免重复调用 Grok API
        const hasDebotNarrative = record.aiReport?.narrative_type || record.signal?.aiNarrativeType;

        if (hasDebotNarrative) {
            // 使用 DeBot 已获取的叙事信息，不再调用 Grok API
            console.log(`   🎭 [叙事] 使用 DeBot aiReport (跳过额外API调用)`);
            narrativeVerification = {
                narrative: record.aiReport?.narrative_type || record.signal?.aiNarrativeType || 'Unknown',
                confidence: 0.8,
                source: 'debot_aiReport',
                reasoning: record.aiReport?.background?.origin?.text || record.signal?.aiNarrative || ''
            };
        } else if (this.narrativeSystem) {
            try {
                console.log(`   🎭 [叙事验证] 正在验证 ${record.symbol} 的叙事生命周期...`);

                // 调用 AI 叙事系统进行 Twitter 搜索验证
                narrativeVerification = await this.narrativeSystem.identifyTokenNarrative(
                    record.symbol,
                    record.signal?.name || record.symbol,
                    null  // 让系统自动搜索 Twitter
                );

                if (narrativeVerification) {
                    console.log(`   🎭 叙事: ${narrativeVerification.narrative || 'Unknown'}`);
                    console.log(`   📊 置信度: ${(narrativeVerification.confidence * 100).toFixed(0)}%`);
                    console.log(`   💡 来源: ${narrativeVerification.source || 'N/A'}`);

                    // 获取叙事详情
                    const narrativeDetails = this.narrativeSystem.narrativesCache?.get(narrativeVerification.narrative);

                    if (narrativeDetails) {
                        const lifecycle = narrativeDetails.lifecycle_stage;
                        const heat = narrativeDetails.market_heat;

                        console.log(`   🔥 热度: ${heat}/10 | 阶段: ${lifecycle}`);

                        // ⛔ 衰退期叙事拒绝 (decline stage = death spiral)
                        if (lifecycle === 'decline') {
                            console.log(`   ❌ [叙事拒绝] ${record.symbol} 叙事处于衰退期 (${narrativeVerification.narrative})，拒绝毕业！`);
                            console.log(`   💀 原因: 衰退期叙事 = 资金流出 = 接盘风险`);
                            this.tokens.delete(record.address);
                            return;
                        }

                        // ⚠️ 热度过低警告 (heat < 3 = dead narrative)
                        if (heat < 3) {
                            console.log(`   ⚠️ [低热度警告] ${record.symbol} 叙事热度过低 (${heat}/10)，需要更强的链上信号`);
                            // 不直接拒绝，但会影响后续 AI 决策
                        }

                        // 🚀 上升期叙事加分 (early_explosion / early_growth = bonus)
                        if (lifecycle === 'early_explosion' || lifecycle === 'early_growth') {
                            console.log(`   🚀 [叙事加成] ${narrativeVerification.narrative} 处于上升期，增加权重！`);
                            // 这个信息会传递给 AI 做决策
                        }
                    }
                }
            } catch (error) {
                console.log(`   ⚠️ [叙事验证失败] ${error.message}，继续使用普通验证`);
            }
        }

        // 构造 currentData (与 WaitingRoom 兼容)
        const currentData = {
            price: record.current.price,
            smartWalletOnline: record.current.smartMoney,
            liquidity: record.current.liquidity,
            holders: record.current.holders,
            marketCap: record.current.marketCap,
            isMintAbandoned: record.signal?.isMintAbandoned,
            top10Percent: record.signal?.top10Percent
        };

        // 🤖 AI 决策
        let decision = null;
        const useAI = STRATEGY.WAITING_ROOM?.USE_AI_DECISION ?? true;

        if (useAI) {
            try {
                // v6.9: 构建叙事上下文信息
                let narrativeContext = null;
                if (narrativeVerification && narrativeVerification.narrative !== 'Unknown') {
                    const narrativeDetails = this.narrativeSystem?.narrativesCache?.get(narrativeVerification.narrative);
                    narrativeContext = {
                        narrative: narrativeVerification.narrative,
                        confidence: narrativeVerification.confidence,
                        lifecycle_stage: narrativeDetails?.lifecycle_stage || 'unknown',
                        market_heat: narrativeDetails?.market_heat || 5,
                        sustainability: narrativeDetails?.sustainability || 5,
                        is_new_narrative: narrativeVerification.is_new_narrative || false,
                        reasoning: narrativeVerification.reasoning
                    };
                }

                let prompt;

                // v7.4.5: 信号交集优化
                // 如果是黄金交集（DeBot + 猎人），已经双重验证，使用简化版 Prompt 进行快速风控
                if (record.intersectionBoost?.level === '黄金交集' || record.intersectionBoost?.level === '银色交集') {
                    console.log(`   ⚡ [FastTrack] 使用简化版信审 (交集等级: ${record.intersectionBoost.level})`);
                    prompt = buildSimplifiedPrompt({
                        record: {
                            symbol: record.symbol,
                            address: record.address,
                            chain: record.chain,
                            startTime: record.startTime,
                            initial: record.initial
                        },
                        current: currentData,
                        factors,
                        klines: record.klineData,
                        intersectionBoost: record.intersectionBoost
                    });
                } else {
                    // 普通信号，使用完整版 Prompt
                    prompt = buildEntryPrompt({
                        record: {
                            symbol: record.symbol,
                            address: record.address,
                            chain: record.chain,
                            startTime: record.startTime,
                            initial: record.initial
                        },
                        current: currentData,
                        factors,
                        aiReport: record.aiReport,
                        klines: record.klineData,
                        narrativeContext  // v6.9: 传递叙事上下文
                    });
                }

                decision = await aiAnalyst.analyze(prompt);
                console.log(`   🤖 AI 决策: ${decision.action} (${decision.position})`);
                console.log(`   💡 理由: ${decision.reason}`);

                // ═══════════════════════════════════════════════════════════════
                // v6.7 叙事立意等级处理 (Grand Narrative)
                // ═══════════════════════════════════════════════════════════════
                if (decision.intention_tier) {
                    record.intentionTier = decision.intention_tier;
                    const TAGS = this.params.TAGS;

                    // 添加叙事等级标签
                    if (decision.intention_tier === 'TIER_S') {
                        this.addTag(record, TAGS.TIER_S);
                        console.log(`   🏆 [Grand Narrative] S级大叙事: ${decision.intention_reason || '顶级热点'}`);
                    } else if (decision.intention_tier === 'TIER_A') {
                        this.addTag(record, TAGS.TIER_A);
                        console.log(`   🥇 [Grand Narrative] A级优质叙事: ${decision.intention_reason || '有潜力'}`);
                    } else if (decision.intention_tier === 'TIER_B') {
                        this.addTag(record, TAGS.TIER_B);
                        console.log(`   🥈 [Grand Narrative] B级普通叙事: ${decision.intention_reason || '一般meme'}`);
                    } else {
                        console.log(`   ⚠️ [Grand Narrative] C级垃圾叙事: ${decision.intention_reason || '谨慎'}`);
                    }
                }

                if (decision.action === 'SKIP') {
                    console.log(`   ❌ AI 拒绝，不买入`);

                    // v7.4.5: 记录拒绝，设置冷却期
                    const cooldown = this.getCooldownByReason(decision.reason);
                    this.rejectedTokens.set(record.address, {
                        timestamp: Date.now(),
                        reason: decision.reason,
                        cooldown: cooldown,
                        symbol: record.symbol
                    });
                    this.saveRejectedTokens();  // 持久化
                    console.log(`   🛡️ [冷却期] ${Math.ceil(cooldown / 60000)}分钟内不再尝试`);

                    this.tokens.delete(record.address);
                    return;  // AI 说 SKIP，不毕业
                }
            } catch (e) {
                console.log(`   ⚠️ AI 分析失败: ${e.message}，使用默认决策`);
                // AI 失败时，如果是 GOLDEN 标签就买入
                if (factors.tag !== 'GOLDEN') {
                    this.tokens.delete(record.address);
                    return;
                }
                decision = { action: 'BUY', position: 'small', reason: 'AI超时，金狗保底买入' };
            }
        } else {
            // 不使用 AI，直接判断
            if (factors.tag === 'TRAP' || factors.tag === 'WEAK') {
                console.log(`   ❌ 特征不佳 (${factors.tag})，跳过`);
                this.tokens.delete(record.address);
                return;
            }
            decision = { action: 'BUY', position: 'normal', reason: `${factors.tag}标签通过` };
        }

        // v7.4.6: 加锁防止重复毕业 + 先删除后发送
        this.graduatingTokens.add(record.address);

        // 先移出观察池，防止下一个周期再次触发毕业
        this.tokens.delete(record.address);

        // 发出毕业事件
        console.log(`🚀 [GRADUATE] ${record.symbol} 毕业了!`);
        this.emit('graduate', {
            signal: record.signal,
            analysis: record.analysis,
            factors,
            decision,  // ⬅️ 现在传递 AI 决策了！
            currentData,
            reason: `${record.tier}池毕业 (${observeMinutes}min) [${graduationType}]`,
            record: {
                symbol: record.symbol,
                address: record.address,
                chain: record.chain,
                tier: record.tier,
                startTime: record.startTime,
                initial: record.initial,
                // v6.7 叙事等级
                intentionTier: record.intentionTier || decision?.intention_tier || null,
                tags: record.tags ? Array.from(record.tags) : []
            },
            klines: record.klineData,
            aiReport: record.aiReport
        });

        // v7.4.6: 解锁
        this.graduatingTokens.delete(record.address);
    }

    /**
     * 辅助方法
     */
    getTokensByTier(tier) {
        return Array.from(this.tokens.values()).filter(t => t.tier === tier);
    }

    getNextTierDown(tier) {
        if (tier === 'GOLD') return 'SILVER';
        if (tier === 'SILVER') return 'BRONZE';
        return null;
    }

    getStatus() {
        return {
            gold: this.getTokensByTier('GOLD').map(t => ({ symbol: t.symbol, score: t.score })),
            silver: this.getTokensByTier('SILVER').map(t => ({ symbol: t.symbol, score: t.score })),
            bronze: this.getTokensByTier('BRONZE').map(t => ({ symbol: t.symbol, score: t.score })),
            total: this.tokens.size
        };
    }

    has(tokenCA) {
        return this.tokens.has(tokenCA);
    }

    // 兼容 WaitingRoom 接口
    async addSignal(signal, analysis) {
        return this.addToken(signal, analysis);
    }

    // ═══════════════════════════════════════════════════════════════
    // v7.4.5 AI 调用优化: 拒绝冷却期辅助方法
    // ═══════════════════════════════════════════════════════════════

    /**
     * 根据拒绝原因获取冷却时间
     */
    getCooldownByReason(reason) {
        if (!reason) return this.COOLDOWN_BY_REASON.default;

        for (const [keyword, cooldown] of Object.entries(this.COOLDOWN_BY_REASON)) {
            if (keyword !== 'default' && reason.includes(keyword)) {
                return cooldown;
            }
        }
        return this.COOLDOWN_BY_REASON.default;
    }

    /**
     * 保存拒绝记录到文件
     */
    saveRejectedTokens() {
        try {
            const data = Array.from(this.rejectedTokens.entries());
            fs.writeFileSync(this.REJECTED_TOKENS_PATH, JSON.stringify(data, null, 2));
        } catch (e) {
            console.log(`[TieredObserver] ⚠️ 保存拒绝记录失败: ${e.message}`);
        }
    }

    /**
     * 启动时加载拒绝记录
     */
    loadRejectedTokens() {
        try {
            if (fs.existsSync(this.REJECTED_TOKENS_PATH)) {
                const data = JSON.parse(fs.readFileSync(this.REJECTED_TOKENS_PATH, 'utf8'));

                // 过滤掉已过期的记录
                const now = Date.now();
                const validRecords = data.filter(([tokenCA, record]) => {
                    const elapsed = now - record.timestamp;
                    const cooldown = record.cooldown || this.COOLDOWN_BY_REASON.default;
                    return elapsed < cooldown;
                });

                this.rejectedTokens = new Map(validRecords);
            }
        } catch (e) {
            console.log(`[TieredObserver] ⚠️ 加载拒绝记录失败: ${e.message}`);
            this.rejectedTokens = new Map();
        }
    }

    /**
     * 获取拒绝记录统计
     */
    getRejectedStats() {
        return {
            count: this.rejectedTokens.size,
            tokens: Array.from(this.rejectedTokens.entries()).map(([ca, r]) => ({
                ca: ca.slice(0, 8) + '...',
                symbol: r.symbol,
                reason: r.reason?.slice(0, 30),
                remainingMin: Math.ceil((r.cooldown - (Date.now() - r.timestamp)) / 60000)
            }))
        };
    }
}

export default TieredObserver;
