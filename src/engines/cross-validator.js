/**
 * Cross Validator - 交叉验证引擎 v2.0
 * 
 * 核心逻辑：DeBot 为主（事实层），Telegram 为辅（情绪层），LLM 做二次验证
 * 
 * 漏斗流程：
 * 1. Activity Rank (3-5秒轮询) → 发现新信号
 * 2. 第一层本地过滤 → 聪明钱/流动性/安全性 (Hard Gates)
 * 3. 第二层API调用 → DeBot AI Report 叙事评分
 * 4. 第三层LLM分析 → Grok 二次验证叙事质量 (可选)
 * 5. 第四层交叉验证 → Telegram DB 热度查询
 * 6. 综合评分决策 → Watch / Buy / Ignore
 * 
 * 评分公式 v4.0 (满分100)：
 * - 聪明钱行为 30%: 不只看数量，还看他们是买还是卖
 * - AI叙事 20%: DeBot×2 (10) + LLM×0.1 (10)
 * - 入场时机 15%: 发币时间、已涨幅度、离ATH距离
 * - 关键人物 10%: 交易所/巨鲸/KOL互动
 * - 社交热度 10%: X 提及数 + engagement
 * - 报警动量 5%: 黄金区间
 * - 安全性 10%: Mint + 流动性 + 持仓集中度
 */

import { EventEmitter } from 'events';
// import debotScout from '../inputs/debot-scout.js'; // REMOVED: 使用注入的实例
import signalDatabase from '../database/signal-database.js';
import snapshotRecorder from '../database/signal-snapshot-recorder.js';
import aiAnalyst from '../utils/ai-analyst.js';
import GrokTwitterClient from '../social/grok-twitter-client.js';
import { SolanaSnapshotService } from '../inputs/chain-snapshot-sol.js';
import { BSCSnapshotService } from '../inputs/chain-snapshot-bsc.js';
import { KeyInfluencerScorer } from '../scoring/key-influencer-scorer.js';
import { StageDetector } from '../scoring/stage-detector.js';
import { EntryTimingScorer } from '../scoring/entry-timing-scorer.js';
import { SocialHeatScorer } from '../scoring/social-heat-scorer.js';
import GoldDogTracker from '../analytics/gold-dog-tracker.js';
import AutoTuner from '../analytics/auto-tuner.js';
import thresholdConfig from '../utils/threshold-config.js';
// v6.7 死狗学习池
import deadDogPool from '../risk/dead-dog-pool.js';
// v6.9 新鲜度过滤器
import { FreshnessFilter } from '../gates/freshness-filter.js';
// v8.0 统一准入检查器
import { TokenGatekeeper } from '../gates/token-gatekeeper.js';
// v7.6 优化过滤参数 (基于参数网格搜索)
import { applyFilter, getActiveFilterParams, printFilterConfig } from '../config/filter-params.js';

// 链上服务实例
const solService = new SolanaSnapshotService({});
const bscService = new BSCSnapshotService({});

class CrossValidator extends EventEmitter {
    constructor() {
        super();
        this.debotScout = null; // 将在 init 中注入
        this.db = null; // v7.3 數據庫引用
        this.gatekeeper = null; // v8.0 统一准入检查器

        // v8.1 待审队列: 当普通席位满且非 GOLDEN 时，放入此队列等待下次 AI 分析
        this.pendingQueue = new Map();  // address -> { token, factors, queuedAt }

        // ═══════════════════════════════════════════════════════════════
        // 评分配置 v6.0 - 动态追踪，聪明钱为王
        // ═══════════════════════════════════════════════════════════════
        this.scoringConfig = {
            // 权重 (总计100分)
            weights: {
                smartMoneyCount: 25,     // 聪明钱数量 25分
                smartMoneyTrend: 20,     // 聪明钱趋势 20分 (核心!)
                signalDensity: 15,       // 信号密度 15分
                signalTrend: 15,         // 信号趋势 15分
                safety: 15,              // 安全性 15分
                debotAI: 10              // DeBot AI评分 10分
            },

            // 阈值 v6.0 - 更早入场
            thresholds: {
                trial: 45,               // 试探仓（AI未出时）
                buyScout: 55,            // Scout买入线
                buyNormal: 65,           // 确认仓买入线
                buyPremium: 75           // 重仓买入线
            },

            // 仓位配置 (SOL) - 分层进场
            positions: {
                trial: 0.05,             // 试探仓: 0.05 SOL
                scout: 0.10,             // Scout级: 0.10 SOL
                normal: 0.15,            // 确认仓: 0.15 SOL
                premium: 0.20            // 重仓: 0.20 SOL
            },

            // ═══════════════════════════════════════════════════════════════
            // 🛑 v6.8 BSC 限制模式 (压力测试)
            // BSC 胜率太低，限制仓位和每日交易次数
            // ═══════════════════════════════════════════════════════════════
            BSC_LIMITS: {
                ENABLED: true,
                MAX_POSITION_BNB: 0.02,   // BSC 最大仓位 0.02 BNB (~$15)
                DAILY_TRADE_LIMIT: 5,      // 每日最多 5 单
                REQUIRE_TIER_A_PLUS: false // 是否要求 TIER_A 或以上
            },

            // 单币最大累计仓位
            maxPositionPerToken: {
                trial: 0.05,
                scout: 0.15,
                normal: 0.30,
                premium: 0.50
            },

            // 信号密度评分规则
            signalDensityRules: [
                { window: 1, count: 3, score: 15 },   // 1分钟3次=15分
                { window: 1, count: 2, score: 10 },   // 1分钟2次=10分
                { window: 2, count: 6, score: 15 },   // 2分钟6次=15分
                { window: 5, count: 8, score: 15 },   // 5分钟8次=15分
            ],

            // 聪明钱数量评分
            smartMoneyRules: {
                2: 10,   // 2个=10分
                3: 15,   // 3个=15分
                5: 20,   // 5个=20分
                8: 25    // 8个+=25分
            }
        };

        // Hard Gates 配置 v6.7 (势利眼: 动态MCAP门槛)
        this.hardGates = {
            minSmartWalletOnline: 1,       // 最少聪明钱 >= 1 (捕捉从1变N的起爆瞬间)
            minLiquidity: 5000,             // 最低流动性 $5K (抓更早期)
            minMarketCap: 5000,             // 最低市值 $5K (绝对底线)
            maxMarketCap: 5000000,          // 最高市值 $5M (不看大盘股)
            minHolders: 30,                 // 最少持有人 30
            maxTop10Percent: 80,            // TOP10 不超过 80%
            minTokenAgeMinutes: 1,          // 代币至少 1 分钟
            maxTokenAgeMinutes: 360,        // 代币不超过 6 小时
            minAIScore: 0,                  // 不限制 AI 评分（交给观察室）
            requireMintAbandoned: true,     // SOL 必须丢弃权限（铁律）
            bannedKeywords: ['scam', 'rug', 'honeypot', 'fake', 'fraud', 'test', 'airdrop', '欺诈', '骗局']
        };

        // ═══════════════════════════════════════════════════════════════
        // v6.7 动态MCAP门槛 (势利眼系统)
        // 核心逻辑：越小的盘子，需要越多的聪明钱来背书
        // ═══════════════════════════════════════════════════════════════
        this.mcapGate = {
            // 极小盘 (< $50k): 必须有 3+ 聪明钱才能入场
            MICRO: { maxMcap: 50000, minSM: 3 },
            // 小盘 ($50k - $100k): 必须有 2+ 聪明钱
            SMALL: { maxMcap: 100000, minSM: 2 },
            // 正常盘 (>= $100k): 无限制，或有 5+ 聪明钱可无视市值
            SM_OVERRIDE: 5  // 有 5 个聪明钱可以跳过 MCAP 检查
        };


        // 可选：X/Twitter 边界复核（默认关闭）
        this.twitterEdgeEnabled = process.env.TWITTER_EDGE_CHECK_ENABLED === 'true';
        this.twitterEdgeTimeoutMs = parseInt(process.env.TWITTER_EDGE_CHECK_TIMEOUT_MS || '2500', 10);
        this.grokTwitterClient = null;

        // 状态
        this.isRunning = false;
        this.pendingValidation = new Map();
        this.validatedTokens = new Map();

        // ═══════════════════════════════════════════════════════════════
        // v6.0 动态状态追踪系统
        // ═══════════════════════════════════════════════════════════════
        this.tokenStateMap = new Map(); // 每个币的历史状态

        // 当前持仓追踪（按币地址）
        this.tokenPositions = new Map(); // address -> { amount, tier, entryPrice, entryTime }

        // 当前持仓计数（按级别）
        this.currentPositions = {
            trial: 0,    // 试探仓数
            scout: 0,    // Scout 级持仓数
            normal: 0,   // 确认仓持仓数
            premium: 0   // 重仓持仓数
        };

        // Tier 1 频道列表（需要配置）
        this.tier1Channels = new Set([
            // 添加 Tier 1 频道ID
        ]);

        // 关键人物评分器
        this.keyInfluencerScorer = new KeyInfluencerScorer({}, null);

        // 阶段检测器
        this.stageDetector = new StageDetector({});

        // 入场时机评分器 (保留但降权)
        this.entryTimingScorer = new EntryTimingScorer();

        // 社交热度评分器 (保留为加分项)
        this.socialHeatScorer = new SocialHeatScorer();

        // v7.0 金狗特征追踪器
        this.goldDogTracker = new GoldDogTracker();

        // v7.1 自动调参模块
        this.autoTuner = new AutoTuner();

        // v6.9 新鲜度过滤器
        this.freshnessFilter = new FreshnessFilter();
    }

    // ═══════════════════════════════════════════════════════════════
    // v6.0 Token 状态追踪方法
    // ═══════════════════════════════════════════════════════════════

    /**
     * 更新代币状态并计算趋势
     */
    updateTokenState(tokenAddress, smartMoneyCount, signalCount) {
        const now = Date.now();

        if (!this.tokenStateMap.has(tokenAddress)) {
            // 首次发现
            this.tokenStateMap.set(tokenAddress, {
                address: tokenAddress,
                history: [],
                firstSeen: now,
                lastSeen: now,
                buyExecuted: false,
                totalPosition: 0
            });
        }

        const state = this.tokenStateMap.get(tokenAddress);
        state.lastSeen = now;

        // 记录历史
        state.history.push({
            time: now,
            smartMoney: smartMoneyCount,
            signals: signalCount
        });

        // 只保留最近10条记录
        if (state.history.length > 10) {
            state.history.shift();
        }

        return state;
    }

    /**
     * 计算聪明钱趋势
     */
    getSmartMoneyTrend(state) {
        if (state.history.length < 2) return 'STABLE';

        const latest = state.history[state.history.length - 1];
        const previous = state.history[state.history.length - 2];

        if (latest.smartMoney > previous.smartMoney + 1) return 'INCREASING';
        if (latest.smartMoney < previous.smartMoney - 1) return 'DECREASING';
        return 'STABLE';
    }

    /**
     * 计算信号趋势
     */
    getSignalTrend(state) {
        if (state.history.length < 2) return 'STABLE';

        const latest = state.history[state.history.length - 1];
        const previous = state.history[state.history.length - 2];

        // v6.4: 提升敏感度 - 引入绝对增量判断
        const delta = latest.signals - previous.signals;
        const ratio = previous.signals > 0 ? latest.signals / previous.signals : 2;

        // 🚀 极速爆发: 增加 >= 2 个信号，或信号翻倍
        if (delta >= 2 || ratio >= 1.5) return 'ACCELERATING';

        // 📉 衰退: 减少或占比下降
        if (delta < 0 || ratio < 0.8) return 'DECAYING';

        return 'STABLE';
    }

    // ═══════════════════════════════════════════════════════════════
    // 🛑 v6.8 BSC Daily Trade Counter (Stress Test)
    // ═══════════════════════════════════════════════════════════════

    /**
     * 获取今日 BSC 交易次数
     * @returns {number} 今日 BSC 交易数量
     */
    getTodayBscTradeCount() {
        try {
            if (!signalDatabase.ensureConnection()) {
                console.log('[BSC Limit] 数据库连接失败，默认放行');
                return 0;
            }

            // 计算今日 0 点的 Unix 时间戳
            const today = new Date();
            today.setHours(0, 0, 0, 0);
            const todayStart = Math.floor(today.getTime() / 1000);

            const result = signalDatabase.db.prepare(`
                SELECT COUNT(*) as count
                FROM trades
                WHERE chain = 'BSC'
                AND timestamp >= ?
                AND status != 'CANCELLED'
            `).get(todayStart);

            return result?.count || 0;
        } catch (error) {
            console.log(`[BSC Limit] 查询失败: ${error.message}，默认放行`);
            return 0;
        }
    }

    /**
     * 计算信号密度得分
     */
    calculateSignalDensityScore(state) {
        const now = Date.now();
        const oneMinuteAgo = now - 60000;
        const twoMinutesAgo = now - 120000;
        const fiveMinutesAgo = now - 300000;

        // 统计各时间窗口的信号数
        const signalsIn1Min = state.history.filter(h => h.time > oneMinuteAgo).length;
        const signalsIn2Min = state.history.filter(h => h.time > twoMinutesAgo).length;
        const signalsIn5Min = state.history.filter(h => h.time > fiveMinutesAgo).length;

        // 按规则返回分数
        if (signalsIn1Min >= 3) return 15;
        if (signalsIn1Min >= 2) return 10;
        if (signalsIn2Min >= 6) return 15;
        if (signalsIn5Min >= 8) return 15;
        return 5;
    }

    /**
     * v6.0 动态评分计算
     * 评分公式 (满分100):
     * - 聪明钱数量: 25分
     * - 聪明钱趋势: 20分 (核心!)
     * - 信号密度: 15分
     * - 信号趋势: 15分
     * - 安全性: 15分
     * - DeBot AI: 10分
     */
    calculateScoreV6(token, tokenState, aiReport = null, allowTrial = false) {
        const w = this.scoringConfig.weights;
        const details = [];
        let bonusPoints = 0;

        // 1. 聪明钱数量分数 (25分) - v7.4.6: 倒置评分，早期发现更高分
        const smartMoneyCount = token.smartWalletOnline || 0;
        let smartMoneyScore = 0;
        // 早期发现=高分，晚期跟进=低分
        if (smartMoneyCount >= 8) smartMoneyScore = 5;       // 晚期：很多人知道了
        else if (smartMoneyCount >= 5) smartMoneyScore = 10;  // 中期：开始拥挤
        else if (smartMoneyCount >= 2) smartMoneyScore = 20;  // 早期：信息优势大
        else if (smartMoneyCount >= 1) smartMoneyScore = 15;  // 极早期：仅1人，需验证
        details.push(`聪明钱: ${smartMoneyScore}/25 (${smartMoneyCount}个, 早期发现更高分)`);

        // 2. 聪明钱趋势分数 (20分)
        const smTrend = this.getSmartMoneyTrend(tokenState);
        let smTrendScore = 0;
        if (smTrend === 'INCREASING') smTrendScore = 20;
        else if (smTrend === 'STABLE') smTrendScore = 10;
        else smTrendScore = 0; // DECREASING
        details.push(`趋势: ${smTrendScore}/${w.smartMoneyTrend} (${smTrend})`);

        // 3. 信号密度分数 (15分)
        const signalDensityScore = this.calculateSignalDensityScore(tokenState);
        details.push(`密度: ${signalDensityScore}/${w.signalDensity}`);

        // 4. 信号趋势分数 (15分 -> 20分 for ACCELERATING)
        const signalTrend = this.getSignalTrend(tokenState);
        let signalTrendScore = 0;
        if (signalTrend === 'ACCELERATING') signalTrendScore = 20;  // v6.4: 15 -> 20
        else if (signalTrend === 'STABLE') signalTrendScore = 8;
        else signalTrendScore = 3; // DECAYING
        details.push(`信号趋势: ${signalTrendScore}/${w.signalTrend} (${signalTrend})`);

        // 5. 安全性分数 (15分)
        let safetyScore = 0;
        const safetyDetails = [];

        if (token.isMintAbandoned === true) {
            safetyScore += 5;
            safetyDetails.push('Mint✓');
        }

        const liquidity = token.liquidity || 0;
        if (liquidity >= 50000) {
            safetyScore += 5;
            safetyDetails.push('Liq✓');
        } else if (liquidity >= 30000) {
            safetyScore += 3;
            safetyDetails.push('Liq~');
        }

        const holders = token.holders || 0;
        if (holders >= 200) {
            safetyScore += 5;
            safetyDetails.push('Holders✓');
        } else if (holders >= 100) {
            safetyScore += 3;
            safetyDetails.push('Holders~');
        }

        safetyScore = Math.min(safetyScore, w.safety);
        details.push(`安全: ${safetyScore}/${w.safety} [${safetyDetails.join(',')}]`);

        // 6. DeBot AI 分数 (10分)
        let aiScore = 0;
        const debotScore = aiReport?.rating?.score || token.aiScore || token.debotScore || 0;
        if (debotScore >= 5) aiScore = 10;
        else if (debotScore >= 4) aiScore = 8;
        else if (debotScore >= 3) aiScore = 6;
        else if (allowTrial) aiScore = 5; // 试探仓给默认分
        else aiScore = 0;
        details.push(`AI: ${aiScore}/${w.debotAI} (DeBot=${debotScore})`);

        // 计算基础总分
        const baseTotal = smartMoneyScore + smTrendScore + signalDensityScore + signalTrendScore + safetyScore + aiScore;

        // === 加分项 ===
        // 🐣 v6.4 早鸟奖励 (Early Bird Bonus)
        // 市值极低 (<$50k) 且有聪明钱 -> 强行提分，保送进铜池
        const marketCap = token.marketCap || 0;
        if (marketCap > 5000 && marketCap < 50000 && smartMoneyCount >= 1) {
            bonusPoints += 15;
            details.push(`+15 早鸟🐣`);
            console.log(`   🐣 [早鸟奖励] ${token.symbol} 市值$${(marketCap / 1000).toFixed(0)}K+SM${smartMoneyCount} -> +15分`);
        }

        // KOL 互动加分
        if (aiReport?.distribution?.kol_interactions?.length > 0) {
            bonusPoints += 5;
            details.push(`+5 KOL互动`);
        }

        // 热门推文加分
        const topTweet = aiReport?.distribution?.top_tweets?.[0];
        if (topTweet?.views > 50000) {
            bonusPoints += 5;
            details.push(`+5 热门推文`);
        }

        const total = baseTotal + bonusPoints;

        // 确定买入层级
        const t = this.scoringConfig.thresholds;
        let tier = null;
        if (allowTrial && smTrend === 'INCREASING' && baseTotal >= t.trial) {
            tier = 'trial';
        } else if (total >= t.buyPremium) {
            tier = 'premium';
        } else if (total >= t.buyNormal) {
            tier = 'normal';
        } else if (total >= t.buyScout) {
            tier = 'scout';
        }

        return {
            total,
            breakdown: {
                smartMoneyCount: smartMoneyScore,
                smartMoneyTrend: smTrendScore,
                signalDensity: signalDensityScore,
                signalTrend: signalTrendScore,
                safety: safetyScore,
                debotAI: aiScore,
                bonus: bonusPoints
            },
            details,
            tier,
            allowTrial,
            trends: { smartMoney: smTrend, signal: signalTrend }
        };
    }

    // ═══════════════════════════════════════════════════════════════
    // v7.1 多因子量化模型 - 寻找参数间的关系
    // ═══════════════════════════════════════════════════════════════

    /**
     * 计算三大动态因子
     * @returns {Object} { smDensity, hypeDivergence, narrativeHealth, dogType, factors }
     */
    calculateDynamicFactors(token, tokenState, aiReport = null, score = null) {
        const factors = {};

        // 从 token 获取参数
        const smCount = token.smartWalletOnline || 0;
        const avgBuy = token.avgBuyAmount || 0.5; // 默认 0.5 SOL
        const marketCap = token.marketCap || 50000;
        const liquidity = token.liquidity || 0;
        const signalCount = tokenState?.history?.length || 1;
        const aiScore = aiReport?.rating?.score || token.aiScore || 0;

        // ════════════════════════════════════════════════════════════
        // 因子1: 聪明钱密度 (Smart Money Density)
        // 公式: (SM数量 × 平均买入SOL) / (市值 / 1000)
        // 高值意味着：大佬重仓买小市值币 → 金狗信号
        // ════════════════════════════════════════════════════════════
        const marketCapK = marketCap / 1000;
        factors.smDensity = marketCapK > 0
            ? (smCount * avgBuy) / marketCapK
            : 0;

        // 评级 (使用动态阈值)
        const smDensityHighThreshold = thresholdConfig.get('sm_density_high', 5);
        const smDensityMidThreshold = thresholdConfig.get('sm_density_mid', 1);
        let smDensityRating = 'LOW';
        if (factors.smDensity >= smDensityHighThreshold) smDensityRating = 'HIGH'; // 🥇
        else if (factors.smDensity >= smDensityMidThreshold) smDensityRating = 'MID';

        // ════════════════════════════════════════════════════════════
        // 因子2: 舆论-资金背离度 (Hype-Money Divergence)
        // 公式: SM数量 / (信号次数 + 1)
        // 高值 (>1): 钱多话少 → 潜伏期 (买入最佳时机)
        // 低值 (<0.2): 话多钱少 → 出货期 (快跑)
        // ════════════════════════════════════════════════════════════
        factors.hypeDivergence = smCount / (signalCount + 1);

        // 评级 (使用动态阈值)
        const hypeDivergenceStealthThreshold = thresholdConfig.get('hype_divergence_stealth', 1.0);
        const hypeDivergenceMomentumThreshold = thresholdConfig.get('hype_divergence_momentum', 0.2);
        let hypeDivergenceRating = 'TRAP';
        if (factors.hypeDivergence >= hypeDivergenceStealthThreshold) hypeDivergenceRating = 'STEALTH'; // 潜伏期
        else if (factors.hypeDivergence >= hypeDivergenceMomentumThreshold) hypeDivergenceRating = 'MOMENTUM'; // 共振期
        // < 0.2 = 出货期 (TRAP)

        // ════════════════════════════════════════════════════════════
        // 因子3: 叙事健康度 (Narrative Health)
        // 公式: AI评分 × (流动性 / 市值 × 10)
        // 好故事 + 厚池子 = 可持续
        // ════════════════════════════════════════════════════════════
        const liqMcRatio = marketCap > 0 ? (liquidity / marketCap) * 10 : 0;
        factors.narrativeHealth = aiScore * liqMcRatio;

        // 评级 (使用动态阈值)
        const narrativeHealthStrongThreshold = thresholdConfig.get('narrative_health_strong', 10);
        const narrativeHealthMidThreshold = thresholdConfig.get('narrative_health_mid', 5);
        let narrativeHealthRating = 'WEAK';
        if (factors.narrativeHealth >= narrativeHealthStrongThreshold) narrativeHealthRating = 'STRONG';
        else if (factors.narrativeHealth >= narrativeHealthMidThreshold) narrativeHealthRating = 'MID';

        // ════════════════════════════════════════════════════════════
        // 因子5: 精英占比 (Elite Ratio) - v7.2 New
        // 公式: 聪明钱数量 / (持币人总数 + 1) * 100
        // 如果持币人少但全是聪明钱 (占比 > 5%) → 精英局 (+20分)
        // ════════════════════════════════════════════════════════════
        const holders = token.holders || 100;
        factors.eliteRatio = (smCount / holders) * 100;

        let eliteRatioScore = 0;
        let eliteRatioRating = 'NORMAL';

        if (factors.eliteRatio >= 5) {
            eliteRatioRating = 'ELITE'; // 精英局
            eliteRatioScore = 20;
        } else if (factors.eliteRatio >= 2) {
            eliteRatioRating = 'HIGH'; // 高质量
            eliteRatioScore = 10;
        }

        // ════════════════════════════════════════════════════════════
        // 因子6: 接盘惩罚 (Late Follower Penalty) - v7.2 New
        // 如果市值很大 (> $500K) 但聪明钱很少 (< 5个) → 可能是跟单狗
        // ════════════════════════════════════════════════════════════
        let lateFollowerPenalty = 0;
        if (marketCap > 500000 && smCount < 5) {
            lateFollowerPenalty = -30;
            console.log(`   ⚠️ [接盘警告] 市值大($${(marketCap / 1000).toFixed(0)}K)但大哥少(${smCount}) - 扣30分`);
        }

        // ════════════════════════════════════════════════════════════
        // 综合判定: 金狗/银狗/土狗
        // ════════════════════════════════════════════════════════════
        let dogType = 'TRAP'; // 默认土狗
        let dogScore = 0;

        // 计算总分 (基础分 + 动态因子分)
        // 这里的 dogScore 只是一个参考标签分，不直接影响最终 Total
        // 实际影响通过下面的 entryTimingScore + eliteRatioScore + lateFollowerPenalty 返回给主函数加分

        // 金狗条件: 三个因子都达标
        if (smDensityRating === 'HIGH' && hypeDivergenceRating === 'STEALTH') {
            dogType = 'GOLDEN';
            dogScore = 90;
        } else if (
            (smDensityRating === 'HIGH' || smDensityRating === 'MID') &&
            hypeDivergenceRating !== 'TRAP'
        ) {
            dogType = 'SILVER';
            dogScore = 70;
        } else if (hypeDivergenceRating === 'TRAP') {
            dogType = 'TRAP';
            dogScore = 30;
        } else {
            dogType = 'UNKNOWN';
            dogScore = 50;
        }

        // 如果是精英局，直接晋升
        if (eliteRatioRating === 'ELITE' && dogType !== 'GOLDEN') {
            dogType = 'GOLDEN'; // 强行拉升
            console.log(`   🚀 [精英局] 聪明钱占比 ${factors.eliteRatio.toFixed(1)}% - 晋升为金狗`);
        }

        // ════════════════════════════════════════════════════════════
        // 因子4: 入场时机 (Entry Timing Position) - v6.3 修复版
        // 对低市值代币增加"波动容忍度"，区分"正常换手"和"恐慌出货"
        // ════════════════════════════════════════════════════════════
        const smTrendForTiming = score?.trends?.smartMoney || tokenState?.trend || 'STABLE';

        let entryTimingRating = 'MID';
        let entryTimingScore = 0;

        // 市值阶段判断
        const mcK = marketCap / 1000; // 转为 K

        // ========================================
        // v6.3 新增：计算聪明钱变化量，区分"恐慌性出货"和"正常换手"
        // ========================================
        const smDelta = tokenState?.smDelta || 0;  // 从历史记录获取
        const prevSM = smCount - smDelta;

        // 🚨 恐慌性出货 (Panic Dump): 流出 >= 2 个，或占比 > 20%
        const isPanicDump = smDelta <= -2 || (prevSM > 0 && smDelta / prevSM < -0.2);

        // 📉 微量流出 (Minor Dip): 只流出 1 个，正常换手
        const isMinorDip = smDelta === -1 && !isPanicDump;

        // ========================================
        // 🟢 EARLY: 市值 $10K-$80K (放宽上限)
        // 修正：允许微量流出，只要不是崩盘
        // ========================================
        if (mcK >= 10 && mcK <= 80 && !isPanicDump) {
            entryTimingRating = 'EARLY';
            entryTimingScore = 25;
            if (isMinorDip) {
                console.log(`   🎯 [早期回调] 市值$${mcK.toFixed(0)}K, SM仅-1，视为上车机会`);
            }
        }
        // ========================================
        // 🔵 PRIME: 市值 $80K-$250K (主升浪区域)
        // 修正：允许稳定或微量流出
        // ========================================
        else if (mcK > 80 && mcK <= 250 && (smTrendForTiming !== 'DECREASING' || isMinorDip)) {
            entryTimingRating = 'PRIME';
            entryTimingScore = 20;
        }
        // ========================================
        // 🟡 MID: 市值 $250K-$500K (中后期)
        // 要求：聪明钱必须稳定或增加
        // ========================================
        else if (mcK > 250 && mcK <= 500 && smTrendForTiming !== 'DECREASING') {
            entryTimingRating = 'MID';
            entryTimingScore = 5;
        }
        // ========================================
        // 🟡 LATE: 市值 $500K-$1M
        // 晚期但如果 SM 还在增加，可以参与
        // ========================================
        else if (mcK > 500 && mcK <= 1000 && smTrendForTiming === 'INCREASING') {
            entryTimingRating = 'LATE';
            entryTimingScore = 0;
        }
        // ========================================
        // 🔴 RISKY: 真正的风险区
        // 修正：只有大市值出货，或任何市值崩盘，才算 RISKY
        // ========================================
        else if (
            mcK > 1000 ||                                    // 市值太大 (> $1M)
            isPanicDump ||                                   // 任何市值，大崩盘就是风险
            (mcK > 150 && smTrendForTiming === 'DECREASING' && !isMinorDip) || // 中大市值不允许流出
            lateFollowerPenalty < 0                          // 接盘局
        ) {
            entryTimingRating = 'RISKY';
            entryTimingScore = -15 + lateFollowerPenalty;
        }
        // ========================================
        // 其他情况
        // ========================================
        else {
            entryTimingRating = 'NORMAL';
            entryTimingScore = 10;
        }

        // 叠加精英分
        entryTimingScore += eliteRatioScore;

        factors.entryTiming = entryTimingScore;

        // 如果时机不对，降级处理
        if (entryTimingRating === 'RISKY') {
            if (dogType === 'GOLDEN') dogType = 'SILVER';
            else if (dogType === 'SILVER') dogType = 'UNKNOWN';
            console.log(`   ⚠️ [时机警告] ${entryTimingRating} (市值$${mcK.toFixed(0)}K) - 降级处理`);
        }

        const result = {
            smDensity: factors.smDensity,
            smDensityRating,
            hypeDivergence: factors.hypeDivergence,
            hypeDivergenceRating,
            narrativeHealth: factors.narrativeHealth,
            narrativeHealthRating,
            eliteRatio: factors.eliteRatio, // v7.2
            eliteRatioRating, // v7.2
            entryTiming: entryTimingScore,
            entryTimingRating,
            dogType,
            dogScore,
            // 原始数据用于调试
            raw: { smCount, avgBuy, marketCap, liquidity, signalCount, aiScore, mcK, smTrendForTiming, holders }
        };

        // 输出关键因子
        console.log(`   📊 动态因子: 密度=${factors.smDensity.toFixed(2)}(${smDensityRating}) | 背离=${factors.hypeDivergence.toFixed(2)}(${hypeDivergenceRating}) | 健康=${factors.narrativeHealth.toFixed(1)}(${narrativeHealthRating}) | 时机=${entryTimingRating}`);

        console.log(`   🐕 类型判定: ${dogType === 'GOLDEN' ? '🥇金狗' : dogType === 'SILVER' ? '🥈银狗' : dogType === 'TRAP' ? '☠️土狗' : '❓未知'}`);

        return result;
    }


    /**
     * 获取信号级别
     */
    getSignalTier(score) {
        const t = this.scoringConfig.thresholds;
        if (score >= t.buyPremium) return 'premium';
        if (score >= t.buyNormal) return 'normal';
        if (score >= t.buyScout) return 'scout';
        return null; // 不够买入
    }


    // ═══════════════════════════════════════════════════════════════
    // 🏦 v6.5 仓位容量管理 (Position Capacity Management)
    // 从数据库实时查询，只有 OPEN 状态计入限制
    // PARTIAL (已出本) 是零风险 Moon Bag，不占用额度
    // ═══════════════════════════════════════════════════════════════

    /**
     * 设置数据库引用（由 index.js 在启动时注入）
     */
    setDatabase(db) {
        this.db = db;
    }

    /**
     * 🔍 获取当前活跃仓位统计 (从数据库实时查询)
     * 只统计 status = 'open' 的仓位，partial 不占用额度！
     */
    getActivePositionStats() {
        if (!this.db) {
            console.warn('⚠️ [CrossValidator] 数据库未初始化，返回空统计');
            return { total: 0, byChain: { SOL: 0, BSC: 0 } };
        }

        try {
            // 总数统计 (只统计 OPEN)
            const totalRow = this.db.prepare(`
                SELECT COUNT(*) as count 
                FROM positions 
                WHERE status = 'open'
            `).get();

            // 按链统计
            const chainRows = this.db.prepare(`
                SELECT chain, COUNT(*) as count 
                FROM positions 
                WHERE status = 'open'
                GROUP BY chain
            `).all();

            const byChain = { SOL: 0, BSC: 0 };
            for (const row of chainRows) {
                byChain[row.chain] = row.count;
            }

            return {
                total: totalRow?.count || 0,
                byChain
            };
        } catch (e) {
            console.error('❌ [CrossValidator] 查询仓位统计失败:', e.message);
            return { total: 0, byChain: { SOL: 0, BSC: 0 } };
        }
    }

    /**
     * 🛡️ 入场资格预审 (Capacity Pre-check)
     * 在调用 AI 之前先检查是否有座位，省 AI 调用费用！
     * 
     * @param {Object} factors - 动态因子 (包含 tag)
     * @param {string} chain - 链类型 (SOL/BSC)
     * @returns {Object} { allow: boolean, type: string, reason?: string }
     */
    checkCapacity(factors, chain = 'SOL') {
        const stats = this.getActivePositionStats();

        // 导入配置
        const config = this.scoringConfig?.maxPositions || {
            NORMAL: 6,
            VIP: 2,
            TOTAL: 8,
            PER_CHAIN: { SOL: 5, BSC: 4 }
        };

        const isGolden = factors?.dogType === 'GOLDEN';  // v8.1 修复: 使用 dogType 而不是 tag
        const chainLimit = config.PER_CHAIN?.[chain] || 5;

        console.log(`🏦 [容量检查] 当前: ${stats.total}/${config.TOTAL} (${chain}: ${stats.byChain[chain]}/${chainLimit})`);

        // 1. 检查链上限
        if (stats.byChain[chain] >= chainLimit) {
            return {
                allow: false,
                type: 'CHAIN_FULL',
                reason: `${chain}链仓位已满 (${stats.byChain[chain]}/${chainLimit})`
            };
        }

        // 2. 普通席位有空 (< 6)
        if (stats.total < config.NORMAL) {
            return { allow: true, type: 'NORMAL_SLOT' };
        }

        // 3. VIP 席位检查 (6 <= count < 8)
        if (stats.total < config.TOTAL) {
            if (isGolden) {
                return { allow: true, type: 'VIP_SLOT' };
            } else {
                // v8.1 待审队列: 不拒绝，而是加入等待队列
                return {
                    allow: false,
                    type: 'PENDING',
                    reason: `普通席位已满 (${stats.total}/${config.NORMAL})，已加入待审队列`
                };
            }
        }

        // 4. 彻底满了 (>= 8)
        // 未来可扩展 SWAP 逻辑：如果是超级金狗 (score >= 85)，可以置换最弱仓位
        return {
            allow: false,
            type: 'FULL',
            reason: `仓位已满 (${stats.total}/${config.TOTAL})，无法入场`
        };
    }

    /**
     * 旧版兼容：检查是否有可用槽位
     * @deprecated 请使用 checkCapacity() 替代
     */
    hasAvailableSlot(tier) {
        const stats = this.getActivePositionStats();
        const config = this.scoringConfig?.maxPositions || { TOTAL: 8 };
        return stats.total < config.TOTAL;
    }

    /**
     * 旧版兼容：占用槽位 (现在是空操作，由数据库自动管理)
     * @deprecated 仓位现在由数据库自动统计
     */
    occupySlot(tier) {
        // v6.5: 仓位由数据库自动管理，不再手动计数
        const stats = this.getActivePositionStats();
        console.log(`[Position] 当前活跃仓位: ${stats.total}/8 (OPEN only)`);
    }

    /**
     * 旧版兼容：释放槽位 (现在是空操作，由数据库自动管理)
     * @deprecated 仓位现在由数据库自动统计
     */
    releaseSlot(tier) {
        // v6.5: 仓位由数据库自动管理，不再手动计数
        const stats = this.getActivePositionStats();
        console.log(`[Position] 当前活跃仓位: ${stats.total}/10 (OPEN only)`);
    }

    // ═══════════════════════════════════════════════════════════════
    // v8.1 待审队列管理 (Pending Queue for blocked non-GOLDEN tokens)
    // ═══════════════════════════════════════════════════════════════

    /**
     * 将代币加入待审队列
     */
    addToPendingQueue(token, factors = {}) {
        const address = token.address || token.tokenAddress;
        if (!this.pendingQueue.has(address)) {
            this.pendingQueue.set(address, {
                token,
                factors,
                queuedAt: Date.now()
            });
            console.log(`📋 [待审队列] ${token.symbol || address.slice(0, 8)} 已入队，等待下次 AI 分析`);
        }
    }

    /**
     * 获取待审队列中的代币 (供 BatchAIAdvisor 使用)
     * 返回后清空队列
     */
    getPendingTokens() {
        if (this.pendingQueue.size === 0) return [];

        const tokens = Array.from(this.pendingQueue.values()).map(item => item.token);
        const count = tokens.length;
        this.pendingQueue.clear();

        console.log(`📋 [待审队列] 取出 ${count} 个待审代币，队列已清空`);
        return tokens;
    }

    /**
     * 获取待审队列大小
     */
    getPendingQueueSize() {
        return this.pendingQueue.size;
    }

    /**
     * 初始化并绑定 DeBot Scout 事件
     * @param {Object} scoutInstance - 可选，指定要使用的 Scout 实例 (如 Playwright Scout)
     */
    init(scoutInstance = null) {
        // 如果提供了外部实例，则优先使用
        if (scoutInstance) {
            this.debotScout = scoutInstance;
            console.log('[CrossValidator] 已绑定外部注入的 DeBot Scout 实例');
        } else {
            this.debotScout = debotScout; // 回退到默认导入
            console.log('[CrossValidator] 使用默认导入的 DeBot Scout 实例');
        }

        // 监听 DeBot 热门代币事件
        this.debotScout.on('hot-token', async (token) => {
            await this.onNewToken(token);
        });

        // 监听 DeBot 信号事件 (移除了重复的 hunter-signal 监听)

        console.log('[CrossValidator] 初始化完成，已绑定 DeBot Scout 事件');
    }

    /**
     * v7.3 設置數據庫引用
     * @param {Database} db - better-sqlite3 數據庫實例
     */
    setDatabase(db) {
        this.db = db;
    }

    /**
     * v8.0 設置 TokenGatekeeper
     * @param {TokenGatekeeper} gatekeeper - 统一准入检查器
     */
    setGatekeeper(gatekeeper) {
        this.gatekeeper = gatekeeper;
        console.log('[CrossValidator] 🚪 TokenGatekeeper 已绑定');
    }

    /**
     * v7.3 記錄被拒絕/忽略的信號
     * @param {Object} token - 代幣數據
     * @param {Object} snapshot - 鏈上快照
     * @param {string} stage - 拒絕階段 (cross_validator)
     * @param {string} reason - 拒絕原因
     * @param {Object} factors - 決策因子
     */
    recordRejection(token, snapshot, stage, reason, factors = {}) {
        if (!this.db) return;

        try {
            this.db.prepare(`
                INSERT INTO rejected_signals (
                    token_ca, chain, symbol, signal_source,
                    rejection_stage, rejection_reason, rejection_factors,
                    price_at_rejection, mcap_at_rejection, liquidity_at_rejection,
                    sm_count_at_rejection
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `).run(
                token.address || token.tokenAddress || token.token_ca,
                token.chain || snapshot?.chain || 'SOL',
                token.symbol || (token.address || '').substring(0, 8),
                factors.source || 'cross_validator',
                stage,
                reason,
                JSON.stringify(factors),
                snapshot?.current_price || 0,
                snapshot?.market_cap || 0,
                snapshot?.liquidity_usd || 0,
                snapshot?.smart_wallet_online || factors.sm_count || 0
            );
        } catch (e) {
            // 忽略錯誤，不影響主流程
        }
    }

    /**
     * 处理新代币 v6.0（来自 Activity Rank 或 Alpha Monitor）
     * 使用动态状态追踪和新评分系统
     */
    async onNewToken(token) {
        const tokenAddress = token.tokenAddress;

        // 🔥 v6.6 防重复验证 (Fix FRAUDCOIN duplicate entry bug)
        if (this.pendingValidation.has(tokenAddress)) {
            console.log(`[Validator] ⏭️ ${token.symbol || tokenAddress.slice(0, 8)} 正在验证中，跳过`);
            return;
        }

        // 标记为正在验证
        this.pendingValidation.set(tokenAddress, Date.now());

        try {
            const isTelegramSignal = typeof token.source === 'string' && token.source.startsWith('TG:');

            // ═══════════════════════════════════════════════════════════════
            // v8.0 统一准入检查 (P0 - 最优先级)
            // 持仓/冷却期/AI拒绝/观察池/死狗池 一次性检查
            // ═══════════════════════════════════════════════════════════════
            if (this.gatekeeper) {
                const gateCheck = this.gatekeeper.canEnter(tokenAddress, token.chain || 'SOL', {
                    symbol: token.symbol,
                    name: token.name,
                    creator: token.creator || token.deployer,
                    narrative: token.narrative || token.category
                });
                if (!gateCheck.allowed) {
                    console.log(`[Gatekeeper] 🚫 ${token.symbol || tokenAddress.slice(0, 8)}: ${gateCheck.reason}`);
                    return;
                }
            } else {
                // 回退到旧逻辑：死狗池检查
                const blacklistCheck = deadDogPool.isBlacklisted({
                    symbol: token.symbol,
                    name: token.name || token.symbol,
                    tokenCA: token.tokenAddress,
                    creator: token.creator || token.deployer,
                    narrative: token.narrative || token.category
                });
                if (blacklistCheck.blocked) {
                    console.log(`[DeadDog] 🚫 ${token.symbol || tokenAddress.slice(0, 8)}: ${blacklistCheck.reason}`);
                    return;
                }
            }

            // ═══════════════════════════════════════════════════════════════
            // v6.9 新鲜度过滤 (P1 - 高优先级)
            // 过滤过时信号、重复信号、冷却期信号
            // ═══════════════════════════════════════════════════════════════
            const freshnessResult = this.freshnessFilter.evaluate(token, {
                score: 0, // 尚未评分
                smartMoneyCount: token.smartWalletOnline || 0
            });
            if (!freshnessResult.pass) {
                console.log(`[Freshness] 🧊 ${token.symbol || tokenAddress.slice(0, 8)}: ${freshnessResult.reasons[0]} (filter: ${freshnessResult.filter})`);
                return;
            }
            // 记录新鲜度得分供后续参考
            token.freshnessScore = freshnessResult.score;

            // 🔥 === Alpha 快速通道检测 ===
            if (token.source?.startsWith('alpha:') && token.fastTrackType) {
                return await this.handleAlphaFastTrack(token);
            }

            // === 第一层：本地 Hard Gates 过滤 ===
            const gateResult = this.checkHardGates(token);
            if (!gateResult.passed) {
                console.log(`[Gate] ❌ ${token.symbol || token.tokenAddress.slice(0, 8)}: ${gateResult.reason}`);
                // v7.4: 记录被拒绝的信号用于回测
                snapshotRecorder.recordRejection({
                    tokenAddress: token.tokenAddress,
                    chain: token.chain,
                    symbol: token.symbol,
                    signalSource: token.source || 'debot',
                    currentPrice: token.currentPrice || token.price || 0,
                    marketCap: token.marketCap || token.market_cap || 0,
                    liquidity: token.liquidity || 0,
                    smartMoney: token.smartWalletOnline || 0,
                    priceChange: token.priceChange24h || 0,
                    intentionTier: token.narrativeTier || 'TIER_C'
                }, 'cross_validator_gate', gateResult.reason, { source: 'CrossValidator.checkHardGates' });
                return;
            }

            const allowTrial = gateResult.allowTrial || false;
            if (allowTrial) {
                console.log(`[Gate] ⚠️ ${token.symbol}: ${gateResult.reason}`);
            }

            // === v6.0 动态状态追踪 ===
            const smartMoneyCount = token.smartWalletOnline || 0;
            const signalCount = token.signalCount || 0;
            const tokenState = this.updateTokenState(token.tokenAddress, smartMoneyCount, signalCount);

            console.log(`\n[Validator] 🔍 验证中: ${token.symbol} (${token.tokenAddress.slice(0, 8)}...)`);
            console.log(`   📊 聪明钱: ${smartMoneyCount} | 信号次数: ${tokenState.history.length}`);

            // === 第二层：获取 DeBot AI Report ===
            let aiReport = token.aiReport;
            if (!aiReport && this.debotScout && !isTelegramSignal) {
                console.log(`[Validator] 🌐 补全 AI Report...`);
                aiReport = await this.debotScout.fetchAIReport(token.tokenAddress);
                if (aiReport && !aiReport.rating) {
                    aiReport = { rating: { score: typeof aiReport.score === 'number' ? aiReport.score : parseInt(aiReport.rating?.score || 0) }, ...aiReport };
                }
            }

            // TG 信号处理
            if (isTelegramSignal) {
                console.log(`[Gate] ℹ️ ${token.symbol}: TG信号存入DB，不触发交易`);
                return;
            }

            // 检查负面标记
            if (aiReport?.distribution?.negativeIncidents) {
                const negative = aiReport.distribution.negativeIncidents.toLowerCase();
                for (const keyword of this.hardGates.bannedKeywords) {
                    if (negative.includes(keyword)) {
                        console.log(`[Gate] ❌ ${token.symbol}: 有负面标记 (${keyword})`);
                        // v7.4: 记录被拒绝的信号
                        snapshotRecorder.recordRejection({
                            tokenAddress: token.tokenAddress,
                            chain: token.chain,
                            symbol: token.symbol,
                            signalSource: token.source || 'debot',
                            currentPrice: token.currentPrice || token.price || 0,
                            marketCap: token.marketCap || token.market_cap || 0,
                            liquidity: token.liquidity || 0,
                            smartMoney: token.smartWalletOnline || 0
                        }, 'negative_keyword', `负面标记: ${keyword}`, { keyword, source: 'AI Report' });
                        return;
                    }
                }
            }

            // === v6.0 评分 ===
            const score = this.calculateScoreV6(token, tokenState, aiReport, allowTrial);

            // 打印评分明细
            console.log(`📊 评分明细 [${score.total}分]: ${score.details.join(' | ')}`);
            console.log(`   趋势: 聪明钱=${score.trends.smartMoney}, 信号=${score.trends.signal}`);

            // === v7.1 多因子分析 ===
            const dynamicFactors = this.calculateDynamicFactors(token, tokenState, aiReport, score);

            // ═══════════════════════════════════════════════════════════════
            // v7.6 优化过滤 (基于参数网格搜索)
            // 目标: 提高金狗命中率 + 降低噪音
            // ═══════════════════════════════════════════════════════════════
            const filterParams = getActiveFilterParams();
            const filterData = {
                baseScore: score.total,
                smartWalletOnline: token.smartWalletOnline || 0,
                trendType: score.trends.smartMoney,
                signalTrendType: score.trends.signal,
                marketCap: token.marketCap || 0,
                lateFollower: dynamicFactors.lateFollower || false
            };
            const filterResult = applyFilter(filterData);

            if (!filterResult.pass) {
                console.log(`   ⏭️ [Filter v7.6] ${token.symbol} 未通过优化过滤: ${filterResult.reason}`);
                console.log(`   📊 七维特征: 基础分=${score.total} SM=${filterData.smartWalletOnline} 趋势=${filterData.trendType} 信号=${filterData.signalTrendType} MC=$${(filterData.marketCap/1000).toFixed(0)}K`);
                return;
            }

            // 土狗检测 - 如果背离度太低，强制降级或拒绝
            if (dynamicFactors.dogType === 'TRAP') {
                console.log(`⚠️ [TRAP检测] 舆论-资金背离严重 (${dynamicFactors.hypeDivergence.toFixed(2)}), 降级处理`);
                // 土狗情况下不买入
                const decision = { action: 'IGNORE', reason: `土狗信号: 话多钱少 (背离度${dynamicFactors.hypeDivergence.toFixed(2)})`, tier: null };
                this.emit('trap-detected', { token, score, dynamicFactors });
                return;
            }

            // 金狗检测 - 如果是金狗特征，提升仓位
            let tierBoost = null;
            if (dynamicFactors.dogType === 'GOLDEN') {
                console.log(`🥇 [GOLDEN检测] 资金强+话少+健康 → 提升仓位`);
                tierBoost = 'premium';
            } else if (dynamicFactors.dogType === 'SILVER') {
                console.log(`🥈 [SILVER检测] 资金与舆论共振 → 正常仓位`);
            }

            // === 做出决策 ===
            const decision = this.makeDecisionV6(token, score, tokenState, tierBoost);


            // === v7.0 金狗特征捕获 ===
            // 记录所有动态参数，用于后续相关性分析
            try {
                this.goldDogTracker.captureFeatures(token, score, decision, aiReport, tokenState);
            } catch (e) {
                console.error(`[GoldDogTracker] 捕获特征失败: ${e.message}`);
            }

            // === v7.1 自动调参 - 记录因子表现 ===
            if (decision.action === 'BUY' || decision.action === 'ADD') {
                try {
                    this.autoTuner.recordTrade(token, dynamicFactors, decision, token.currentPrice || 0);
                } catch (e) {
                    console.error(`[AutoTuner] 记录失败: ${e.message}`);
                }
            }


            // 记录验证结果
            this.validatedTokens.set(token.tokenAddress, {
                token,
                aiReport,
                score,
                decision,
                tokenState,
                timestamp: Date.now()
            });


            // 打印决策
            console.log(`🎯 [CrossValidator] 决策: ${decision.action}`);
            if (decision.action === 'BUY') {
                console.log(`   💰 仓位: ${decision.position} SOL (${decision.tier})`);
                console.log(`   📝 理由: ${decision.reason}`);
            }

            // 发射决策事件
            if (decision.action !== 'IGNORE') {
                this.emit('validated-signal', {
                    token,
                    aiReport,
                    score,
                    decision,
                    tokenState
                });
            }

        } catch (error) {
            console.error(`[Validator] 验证错误: ${error.message}`);
        } finally {
            // 🔥 v6.6 清理验证锁
            this.pendingValidation.delete(tokenAddress);
        }
    }

    /**
     * v6.0 决策逻辑 (v7.1 支持 tierBoost 参数)
     */
    makeDecisionV6(token, score, tokenState, tierBoost = null) {
        const t = this.scoringConfig.thresholds;
        const positions = this.scoringConfig.positions;

        // 检查是否已持仓
        const existingPosition = this.tokenPositions.get(token.tokenAddress);
        const hasPosition = !!existingPosition;

        // 如果已经达到该币最大仓位，不再买入
        if (hasPosition) {
            const maxPosition = this.scoringConfig.maxPositionPerToken[score.tier || 'scout'];
            if (existingPosition.amount >= maxPosition) {
                return { action: 'WATCH', reason: '已达单币最大仓位', tier: null };
            }
        }

        // v7.1: 如果是金狗，强制升级到 premium tier
        let effectiveTier = score.tier;
        if (tierBoost && tierBoost === 'premium') {
            effectiveTier = 'premium';
            console.log(`   🥇 [TierBoost] 金狗升级: ${score.tier} → premium`);
        }

        // 买入决策
        if (effectiveTier) {
            const positionSize = positions[effectiveTier];

            // 检查是否有可用槽位
            // (暂时跳过槽位检查，允许买入)

            let reason = '';
            if (tierBoost === 'premium') {
                reason = `🥇 金狗信号: 资金密集+话少+健康，重仓买入`;
            } else if (score.allowTrial) {
                reason = `试探仓: 聪明钱${token.smartWalletOnline}个且趋势${score.trends.smartMoney}`;
            } else if (score.trends.smartMoney === 'INCREASING') {
                reason = `聪明钱增加中 (${token.smartWalletOnline}个), 评分${score.total}`;
            } else {
                reason = `评分${score.total}达到${effectiveTier}级阈值`;
            }


            // 如果已持仓，尝试加仓
            if (hasPosition && score.trends.smartMoney === 'INCREASING') {
                return {
                    action: 'ADD',
                    tier: score.tier,
                    position: positionSize * 0.5, // 加仓一半
                    reason: `加仓: 聪明钱从${tokenState.history[tokenState.history.length - 2]?.smartMoney || 0}增加到${token.smartWalletOnline}`,
                    stopLoss: -30,
                    takeProfit: [100, 300]
                };
            }

            return {
                action: 'BUY',
                tier: score.tier,
                position: positionSize,
                reason,
                stopLoss: -30,
                takeProfit: [100, 300]
            };
        }

        // 不够分数
        if (score.total >= 45) {
            return { action: 'WATCH', reason: `评分${score.total}，继续观察`, tier: null };
        }

        return { action: 'IGNORE', reason: `评分${score.total}太低`, tier: null };
    }

    /**
     * 处理新信号（来自 Heatmap）

                const xCheck = await this.runTwitterEdgeCheck(token);
                score.xRisk = xCheck.risk;
                score.xSummary = xCheck.summary;
                score.xMentions = xCheck.mentions;
                if (xCheck.risk === 'HIGH') {
                    console.log(`⚠️ X边界复核: HIGH - ${xCheck.summary}`);
                } else {
                    console.log(`✅ X边界复核: OK - ${xCheck.summary}`);
                }
            }

            // === 做出决策 ===
            const decision = this.makeDecision(token, aiReport, score);

            // 记录验证结果
            this.validatedTokens.set(token.tokenAddress, {
                token,
                aiReport,
                tgHeat,
                llmResult,
                score,
                decision,
                timestamp: Date.now()
            });

            // 打印结果
            this.printValidationResult(token, aiReport, tgHeat, score, decision, llmResult);

            // 发射决策事件
            if (decision.action !== 'IGNORE') {
                this.emit('validated-signal', {
                    token,
                    aiReport,
                    tgHeat,
                    llmResult,
                    score,
                    decision
                });
            }

        } catch (error) {
            console.error(`[Validator] 验证错误: ${error.message}`);
        }
    }

    /**
     * 处理新信号（来自 Heatmap）
     */
    async onNewSignal(signal) {
        // 信号转换为统一格式后验证
        const token = {
            tokenAddress: signal.tokenAddress,
            chain: signal.chain,
            symbol: signal.tokenAddress.slice(0, 8),
            signalCount: signal.signalCount,
            maxPriceGain: signal.maxPriceGain,
            tokenLevel: signal.tokenLevel,
            smartWalletOnline: signal.signalCount || 0, // 用信号次数近似
            liquidity: 0,
            isMintAbandoned: true, // 假设安全
            aiReport: signal.aiReport
        };

        // 尝试从 DeBot Token Metrics 获取数据
        let metrics = null;
        if (this.debotScout) {
            metrics = await this.debotScout.fetchTokenMetrics(signal.tokenAddress,
                signal.chain === 'SOL' ? 'solana' : 'bsc');
        }

        if (metrics && metrics.liquidity > 0) {
            token.liquidity = metrics.liquidity || 0;
            token.price = metrics.price || 0;
            token.marketCap = metrics.mkt_cap || 0;
            token.holders = metrics.holders || 0;
        } else {
            // v7.4.3 优化: DeBot Metrics 失败时，不立即调用 getSnapshot
            // 使用保守默认值，进入观察池后才做完整链上验证
            // 这样可以大幅减少 API 调用，避免 429 限流
            console.log(`[Validator] ⚠️ DeBot Metrics 不可用，使用默认值 (完整快照将在入池后获取)`);
            token.liquidity = token.liquidity || 10000;  // 默认 $10K (保守)
            token.price = token.price || 0;
            token.marketCap = token.marketCap || 50000;  // 默认 $50K
            token.snapshotDeferred = true;  // 标记需要在入池后获取完整快照
        }

        // 进入验证流程
        await this.onNewToken(token);
    }

    /**
     * 第一层：Hard Gates 检查 v6.0
     * 返回 { passed: boolean, reason?: string, allowTrial?: boolean }
     */
    checkHardGates(token) {
        const isTelegramSignal = typeof token.source === 'string' && token.source.startsWith('TG:');

        // 检查聪明钱数量（TG 信号缺该字段，跳过）
        const smartMoney = token.smartWalletOnline || 0;
        if (!isTelegramSignal && smartMoney < this.hardGates.minSmartWalletOnline) {
            return {
                passed: false,
                reason: `聪明钱不足 (${smartMoney}/${this.hardGates.minSmartWalletOnline})`
            };
        }

        // 检查流动性
        const liquidity = token.liquidity || 0;
        if (liquidity < this.hardGates.minLiquidity) {
            return {
                passed: false,
                reason: `流动性不足 ($${liquidity.toFixed(0)}/$${this.hardGates.minLiquidity})`
            };
        }

        // ═══════════════════════════════════════════════════════════════
        // 🛑 v6.8 防穿仓：流动性=0 或 Liq/MCAP < 8% 拒绝
        // 教训：LC(-100%), SOL(-70.5%) 都是因为池子太薄
        // ═══════════════════════════════════════════════════════════════
        if (liquidity === 0 || liquidity < 100) {
            return {
                passed: false,
                reason: `🛑 流动性异常 ($${liquidity.toFixed(0)})，可能是 rug`
            };
        }

        const marketCapForLiqCheck = token.marketCap || token.market_cap || 0;
        if (marketCapForLiqCheck > 0) {
            const liqRatio = liquidity / marketCapForLiqCheck;
            if (liqRatio < 0.08) {
                return {
                    passed: false,
                    reason: `🛑 池子太薄 (Liq/MCAP=${(liqRatio * 100).toFixed(1)}% < 8%)，一砸就穿`
                };
            }
        }

        // ═══════════════════════════════════════════════════════════════
        // 🛑 v6.8 防接盘：24h涨幅 > 300% 不买 (除非 TIER_S)
        // 教训：买已经涨过10倍的币 = 接盘侠
        // ═══════════════════════════════════════════════════════════════
        const priceChange24h = token.priceChange24h || token.price_change_24h || 0;
        const narrativeTier = token.narrativeTier || token.narrative_tier || 'TIER_C';

        if (priceChange24h > 300 && narrativeTier !== 'TIER_S') {
            return {
                passed: false,
                reason: `🛑 拒绝追高 (24h涨幅 ${priceChange24h.toFixed(0)}% > 300%，非TIER_S)`
            };
        }

        // 检查市值范围 (v6.0 基础检查)
        const marketCap = token.marketCap || token.market_cap || 0;
        if (marketCap > 0) {
            if (marketCap < this.hardGates.minMarketCap) {
                return {
                    passed: false,
                    reason: `市值太低 ($${(marketCap / 1000).toFixed(0)}K < $${this.hardGates.minMarketCap / 1000}K)`
                };
            }
            if (marketCap > this.hardGates.maxMarketCap) {
                return {
                    passed: false,
                    reason: `市值太高 ($${(marketCap / 1000).toFixed(0)}K > $${this.hardGates.maxMarketCap / 1000}K)`
                };
            }
        }

        // ═══════════════════════════════════════════════════════════════
        // v6.7 动态MCAP门槛 (势利眼系统)
        // 核心逻辑：越小的盘子，需要越多的聪明钱来背书
        // ═══════════════════════════════════════════════════════════════
        const mcapGateResult = this.checkMcapGate(marketCap, smartMoney);
        if (!mcapGateResult.valid) {
            return {
                passed: false,
                reason: mcapGateResult.reason
            };
        }

        // 检查持有人数 (v6.0 新增)
        const holders = token.holders || token.holder_count || 0;
        if (holders > 0 && holders < this.hardGates.minHolders) {
            return {
                passed: false,
                reason: `持有人不足 (${holders}/${this.hardGates.minHolders})`
            };
        }

        // 检查TOP10持仓 (v6.0 新增)
        const top10 = token.top10Percent || token.top10_percent || 0;
        if (top10 > 0 && top10 > this.hardGates.maxTop10Percent) {
            return {
                passed: false,
                reason: `TOP10控盘 (${top10.toFixed(0)}% > ${this.hardGates.maxTop10Percent}%)`
            };
        }

        // 检查权限（SOL 链）
        if (this.hardGates.requireMintAbandoned &&
            token.chain === 'SOL' &&
            token.isMintAbandoned === false) {
            return {
                passed: false,
                reason: '未丢弃 Mint 权限'
            };
        }

        // 检查 AI 评分 (v6.0: 区分试探仓和正式买入)
        const aiScore = token.aiScore || token.debotScore || token.aiReport?.rating?.score || 0;
        if (aiScore < this.hardGates.minAIScore) {
            // 如果聪明钱趋势好，允许试探仓
            if (smartMoney >= 3) {
                return {
                    passed: true,
                    allowTrial: true,  // 标记为试探仓
                    reason: `AI评分暂无，但聪明钱有${smartMoney}个，允许试探`
                };
            }
            return {
                passed: false,
                reason: `AI评分不足 (${aiScore}/${this.hardGates.minAIScore})`
            };
        }

        return { passed: true, allowTrial: false };
    }

    /**
     * 🎩 v6.7 动态MCAP门槛检查 (势利眼系统)
     *
     * 核心逻辑：越小的盘子，需要越多的聪明钱来背书
     * - 极小盘 (< $50k): 必须有 3+ 聪明钱才能入场
     * - 小盘 ($50k - $100k): 必须有 2+ 聪明钱
     * - 正常盘 (>= $100k): 无限制
     * - 特例: 有 5+ 聪明钱可以跳过 MCAP 检查
     *
     * @returns {{ valid: boolean, reason?: string }}
     */
    checkMcapGate(marketCap, smartMoney) {
        const gate = this.mcapGate;

        // 🐋 特例: 聪明钱足够多，可以跳过 MCAP 检查
        if (smartMoney >= gate.SM_OVERRIDE) {
            console.log(`   🐋 [势利眼] SM=${smartMoney} >= ${gate.SM_OVERRIDE}，跳过MCAP门槛检查`);
            return { valid: true };
        }

        // 如果没有市值数据，暂时放行 (让后续流程处理)
        if (marketCap <= 0) {
            return { valid: true };
        }

        // 🔴 极小盘过滤 (Micro Cap < $50k)
        if (marketCap < gate.MICRO.maxMcap) {
            if (smartMoney < gate.MICRO.minSM) {
                return {
                    valid: false,
                    reason: `🎩极小盘($${(marketCap / 1000).toFixed(1)}k<$50k)需SM≥${gate.MICRO.minSM}(当前${smartMoney})`
                };
            }
            console.log(`   🐣 [势利眼] 极小盘但SM=${smartMoney}≥${gate.MICRO.minSM}，放行`);
            return { valid: true };
        }

        // 🟡 小盘过滤 (Small Cap $50k - $100k)
        if (marketCap < gate.SMALL.maxMcap) {
            if (smartMoney < gate.SMALL.minSM) {
                return {
                    valid: false,
                    reason: `🎩小盘($${(marketCap / 1000).toFixed(1)}k<$100k)需SM≥${gate.SMALL.minSM}(当前${smartMoney})`
                };
            }
            return { valid: true };
        }

        // ✅ 正常盘 (>= $100k) - 无限制
        return { valid: true };
    }

    /**
     * 第三层：获取 Telegram 热度
     */
    async getTelegramHeat(tokenAddress) {
        try {
            // 严格 1 小时时间窗口 - 减少存储压力并捕捉最热趋势
            const timeWindowMinutes = 60;
            const since = Date.now() - (timeWindowMinutes * 60 * 1000);

            // 从数据库聚合查询唯一频道数
            const channelCount = await signalDatabase.getUniqueChannelCount(tokenAddress, timeWindowMinutes);
            const mentions = await signalDatabase.getTokenMentions(tokenAddress, since);

            if (!mentions || mentions.length === 0) {
                return {
                    mentionCount: 0,
                    channelCount: 0,
                    tier1Count: 0,
                    channels: []
                };
            }

            // 统计频道数
            const channels = new Set();
            let tier1Count = 0;

            for (const mention of mentions) {
                channels.add(mention.channel_id);
                if (this.tier1Channels.has(mention.channel_id)) {
                    tier1Count++;
                }
            }

            return {
                mentionCount: mentions.length,
                channelCount: channelCount || channels.size,
                tier1Count,
                channels: Array.from(channels)
            };

        } catch (error) {
            // 数据库查询失败时返回空数据
            return {
                mentionCount: 0,
                channelCount: 0,
                tier1Count: 0,
                channels: []
            };
        }
    }

    /**
     * X/Twitter 边界复核：只在接近阈值时调用一次（省钱+提速）
     */
    async runTwitterEdgeCheck(token) {
        try {
            if (!process.env.XAI_API_KEY) {
                return { risk: 'UNKNOWN', summary: 'XAI_API_KEY未配置', mentions: 0 };
            }

            if (!this.grokTwitterClient) {
                this.grokTwitterClient = new GrokTwitterClient();
            }

            const symbol = token.symbol || token.tokenAddress.slice(0, 8);
            const ca = token.tokenAddress;

            const result = await Promise.race([
                this.grokTwitterClient.searchToken(symbol, ca, 30),
                new Promise((_, reject) => setTimeout(() => reject(new Error('X edge check timeout')), this.twitterEdgeTimeoutMs))
            ]);

            const mentions = result?.mention_count || 0;
            const origin = result?.origin_source;
            const riskFlags = Array.isArray(result?.risk_flags) ? result.risk_flags : [];

            // 简单规则：低提及 + 不真实/风险标记 → HIGH
            if (mentions < 2) {
                return { risk: 'HIGH', summary: `提及过少(${mentions})`, mentions };
            }
            if (origin && origin.is_authentic === false) {
                return { risk: 'HIGH', summary: `源头可疑(${origin.type || 'unknown'})`, mentions };
            }
            if (riskFlags.length > 0) {
                return { risk: 'HIGH', summary: `风险标记:${riskFlags.slice(0, 2).join(',')}`, mentions };
            }

            return { risk: 'LOW', summary: `提及${mentions}，未见明显风险`, mentions };

        } catch (e) {
            return { risk: 'UNKNOWN', summary: `X复核失败:${e.message}`, mentions: 0 };
        }
    }

    /**
     * 🔥 Alpha 快速通道处理
     * 
     * Tier 1 (@lookonchain, @spotonchain): 直接买入，只检查安全性
     * Tier 2 (@Ansem, @MustStopMurad): 降低门槛到 50 分
     * Tier 3 (其他): 正常评估 + 入场加成
     */
    async handleAlphaFastTrack(token) {
        const source = token.alphaSource || 'unknown';
        const tier = token.tier || 'tier3';
        const fastTrackType = token.fastTrackType;

        console.log(`\n🔥 [Alpha Fast Track] ${tier.toUpperCase()} 信号来自 @${source}`);
        console.log(`   代币: ${token.symbol} (${token.chain})`);
        console.log(`   地址: ${token.tokenAddress.slice(0, 12)}...`);

        // === Tier 1: 直接买入通道 ===
        if (fastTrackType === 'DIRECT_BUY') {
            return await this.handleTier1DirectBuy(token);
        }

        // === Tier 2: 降低门槛通道 ===
        if (fastTrackType === 'REDUCED_THRESHOLD') {
            return await this.handleTier2ReducedThreshold(token);
        }

        // === Tier 3: 正常评估 + 加成 ===
        // 走正常流程，但 alphaBonus 会在入场时机评分中生效
        console.log(`   通道: 正常评估 + ${token.entryBonus} 入场加成`);

        // 重新调用正常流程（去掉 fastTrackType 避免递归）
        const normalToken = { ...token, fastTrackType: null };
        return await this.onNewToken(normalToken);
    }

    /**
     * Tier 1: 直接买入通道
     * 只检查基础安全性，跳过评分
     */
    async handleTier1DirectBuy(token) {
        const source = token.alphaSource;

        console.log(`   🚀 TIER 1 直接买入通道`);

        // 最小安全检查
        const safetyCheck = await this.checkMinimalSafety(token);
        if (!safetyCheck.passed) {
            console.log(`   ❌ 安全检查失败: ${safetyCheck.reason}`);
            return;
        }

        console.log(`   ✅ 安全检查通过`);

        // 直接发出买入决策
        const decision = {
            action: 'BUY',
            tier: 'scout',           // 使用最小仓位
            position: 0.08,          // 0.08 SOL (Tier 1 专用)
            reason: `🚀 Tier1 Alpha: @${source} 直接买入`,
            stopLoss: -30,           // 更紧的止损
            isAlphaFastTrack: true
        };

        console.log(`\n✅ [Tier 1 决策] BUY ${decision.position} SOL`);
        console.log(`   理由: ${decision.reason}`);
        console.log(`   止损: ${decision.stopLoss}%`);

        // 发射验证通过信号
        this.emit('validated-signal', {
            token,
            score: { total: 'N/A (Tier1 Fast Track)', breakdown: {} },
            decision,
            aiReport: null,
            llmResult: null,
            isAlphaFastTrack: true,
            alphaTier: 'tier1'
        });
    }

    /**
     * Tier 2: 降低门槛通道
     * 完整评分但门槛降到 50 分，缺失数据用默认值
     */
    async handleTier2ReducedThreshold(token) {
        const source = token.alphaSource;
        const REDUCED_THRESHOLD = 50; // 降低的门槛

        console.log(`   🚀 TIER 2 降门槛通道 (门槛: ${REDUCED_THRESHOLD})`);

        // 尝试获取链上数据
        let snapshot = null;
        try {
            const service = token.chain === 'SOL'
                ? (await import('../inputs/chain-snapshot-sol.js')).SolanaSnapshotService
                : (await import('../inputs/chain-snapshot-bsc.js')).BSCSnapshotService;
            const svc = new service({});
            snapshot = await svc.getSnapshot(token.tokenAddress);
        } catch (e) {
            console.log(`   ⚠️ 链上数据获取失败: ${e.message}`);
        }

        // 合并数据
        const enrichedToken = {
            ...token,
            liquidity: snapshot?.liquidity_usd || token.liquidity || 10000,
            marketCap: snapshot?.market_cap || token.marketCap || 50000,
            price: snapshot?.current_price || token.price || 0,
            // 如果没有聪明钱数据，使用保守默认值
            smartWalletOnline: token.smartWalletOnline || 2,
            // 强制添加 Alpha 加成
            alphaBonus: token.entryBonus,
            alphaSource: source
        };

        // 尝试获取 AI 报告 (可选)
        let aiReport = null;
        try {
            if (this.debotScout) {
                const story = await this.debotScout.fetchAIReport(token.tokenAddress);
                if (story) {
                    // 如果是原始 story 格式，转换为 Validator 期望的格式
                    aiReport = {
                        projectName: story.project_name,
                        narrativeType: story.narrative_type,
                        distribution: {
                            negativeIncidents: story.distribution?.negative_incidents?.text || ''
                        },
                        rating: {
                            score: parseInt(story.rating?.score) || 0,
                            reason: story.rating?.reason || ''
                        }
                    };
                }
            }
        } catch (e) {
            // 没有 AI 报告，使用默认值
        }

        // 如果没有 AI 报告，使用默认叙事分
        const defaultAIReport = aiReport || {
            rating: { score: 3 },  // 默认 3/5
            distribution: {}
        };

        // 计算评分
        const score = await this.calculateScore(
            enrichedToken,
            defaultAIReport,
            { mentionCount: 0 },      // TG heat 默认
            { score: 50, risk_level: 'MEDIUM' }, // LLM 默认中等
            {}                         // X data 默认
        );

        console.log(`   📊 评分: ${score.total}/100 (门槛: ${REDUCED_THRESHOLD})`);

        // 降低门槛判断
        if (score.total < REDUCED_THRESHOLD) {
            console.log(`   ❌ 未达到降低门槛 (${score.total} < ${REDUCED_THRESHOLD})`);
            return;
        }

        // 决策
        const decision = this.makeDecision(score, enrichedToken, { score: 50 });
        decision.reason = `🚀 Tier2 Alpha: @${source} | ${decision.reason}`;
        decision.isAlphaFastTrack = true;

        console.log(`\n✅ [Tier 2 决策] ${decision.action} ${decision.position || ''} SOL`);
        console.log(`   评分: ${score.total}/100`);
        console.log(`   理由: ${decision.reason}`);

        if (decision.action === 'BUY') {
            this.emit('validated-signal', {
                token: enrichedToken,
                score,
                decision,
                aiReport: defaultAIReport,
                llmResult: { score: 50, risk_level: 'MEDIUM' },
                isAlphaFastTrack: true,
                alphaTier: 'tier2'
            });
        }
    }

    /**
     * 最小安全检查 (用于 Tier 1 直接买入)
     */
    async checkMinimalSafety(token) {
        const issues = [];

        // 检查流动性
        if (token.liquidity && token.liquidity < 5000) {
            issues.push(`流动性太低 ($${token.liquidity})`);
        }

        // 对于 SOL: 尝试检查 Mint 权限
        if (token.chain === 'SOL') {
            try {
                const { SolanaSnapshotService } = await import('../inputs/chain-snapshot-sol.js');
                const svc = new SolanaSnapshotService({});
                const snapshot = await svc.getSnapshot(token.tokenAddress);

                if (snapshot) {
                    // 检查 mint authority
                    if (snapshot.mint_authority !== null && snapshot.mint_authority !== '') {
                        issues.push('Mint权限未丢弃');
                    }
                    // 更新流动性
                    if (snapshot.liquidity_usd && snapshot.liquidity_usd < 5000) {
                        issues.push(`流动性太低 ($${snapshot.liquidity_usd.toFixed(0)})`);
                    }
                }
            } catch (e) {
                // 无法获取快照，继续
            }
        }

        // 如果有严重问题，不通过
        if (issues.length > 0) {
            return { passed: false, reason: issues.join(', ') };
        }

        return { passed: true, reason: null };
    }

    /**
     * 计算综合评分 v4.0 - 时机为王
     * 
     * 评分公式 (满分100):
     * - 聪明钱: 30% (smartWallet × 5, 需6个满分)
     * - AI叙事: 20% (DeBot 10 + LLM 10)
     * - 入场时机: 15% (发币时间 + 涨幅 + ATH距离 + 动量) 🔥
     * - 关键人物: 10% (交易所/巨鲸/KOL互动)
     * - 社交热度: 10% (X提及数 + engagement) 🔥
     * - 报警动量: 5% (signalCount 黄金区间)
     * - 安全性: 10% (Mint权限 + 流动性 + 持仓集中)
     */
    async calculateScore(token, aiReport, tgHeat, llmResult = null, xData = null) {
        const w = this.scoringConfig.weights;
        const momentum = this.scoringConfig.signalMomentum;
        const fallbacks = this.scoringConfig.fallbacks;
        const isTelegramSignal = typeof token.source === 'string' && token.source.startsWith('TG:');
        let details = [];

        // 1. 聪明钱分数 (30%) - 支持从 enrichment 数据中获取真实统计
        let smartMoneyScore = 0;
        const swCount = token.smartWalletOnline || aiReport?.smart_money?.online_count || 0;

        if (swCount > 0) {
            smartMoneyScore = Math.min(swCount * 5, w.smartMoney);
        } else if (isTelegramSignal) {
            smartMoneyScore = Math.min(fallbacks.telegram.smartMoney, w.smartMoney);
        }
        details.push(`聪明钱: ${smartMoneyScore}/${w.smartMoney} (${swCount}个在线)`);

        // 2. AI叙事分数 (20%) = DeBot基础(10) + LLM(10)
        let narrativeScore = 0;
        let narrativeDetail = '';

        // DeBot 基础分：0-5分 × 2 = 最高10分
        const debotScore = aiReport?.rating?.score || 0;
        const debotBase = Math.min(debotScore * 2, 10);

        // LLM 分数：0-100 映射到 0-10 (调整比例)
        let llmBase = 0;
        if (llmResult && typeof llmResult.score === 'number') {
            llmBase = Math.min(llmResult.score * 0.1, 10);
        }

        // 如果是 TG 信号且完全没有获取到数据，直接用极其保守的兜底分
        if (isTelegramSignal && !aiReport && !llmResult) {
            narrativeScore = Math.min(fallbacks.telegram.narrative, w.narrative);
            narrativeDetail = 'fallback';
        } else {
            narrativeScore = Math.max(0, Math.min(debotBase + llmBase, w.narrative));
            narrativeDetail = `DeBot${debotBase}+LLM${llmBase.toFixed(1)}`;
        }
        details.push(`叙事: ${narrativeScore.toFixed(1)}/${w.narrative} (${narrativeDetail})`);

        // 3. 🔥 入场时机分数 (15%) - v4.0 新增!
        let entryTimingScore = 0;
        let entryTimingDetail = '';
        let entryWarnings = [];
        let isAlphaSignal = false;

        try {
            // 检查是否有 Alpha 信号加成
            const alphaOptions = {};
            if (token.alphaBonus && token.alphaBonus > 0) {
                alphaOptions.alphaBonus = token.alphaBonus;
                alphaOptions.alphaSource = token.alphaSource || 'unknown';
            }

            const timingResult = this.entryTimingScorer.score(token, alphaOptions);
            entryTimingScore = timingResult.score;
            entryTimingDetail = timingResult.recommendation;
            entryWarnings = timingResult.warnings || [];
            isAlphaSignal = timingResult.isAlphaSignal || false;

            if (entryWarnings.length > 0) {
                console.log(`⏰ [入场时机] ${entryWarnings.join(', ')}`);
            }

            // Alpha 信号特殊标记
            if (isAlphaSignal) {
                entryTimingDetail = `ALPHA:${entryTimingDetail}`;
            }
        } catch (e) {
            entryTimingScore = 5; // 默认中等分
            entryTimingDetail = '未知';
        }
        details.push(`入场时机: ${entryTimingScore.toFixed(1)}/${w.entryTiming} (${entryTimingDetail})`);

        // 4. 关键人物分数 (10%) - 交易所/巨鲸/KOL 互动
        let keyInfluencerScore = 0;
        let keyInfluencerDetail = '';
        let superSignal = null;

        try {
            const influencerResult = await this.keyInfluencerScorer.score(
                token.symbol || token.tokenAddress?.slice(0, 8),
                xData || llmResult?.xData || {}
            );
            keyInfluencerScore = Math.min(influencerResult.score, w.keyInfluencer);
            keyInfluencerDetail = influencerResult.details.slice(0, 2).join(', ') || '无关键人物';
            superSignal = influencerResult.superSignal;

            if (superSignal) {
                console.log(`🚨 [超级信号] ${superSignal.message}`);
            }
        } catch (e) {
            keyInfluencerDetail = 'error';
        }
        details.push(`关键人物: ${keyInfluencerScore.toFixed(1)}/${w.keyInfluencer} (${keyInfluencerDetail})`);

        // 5. 🔥 社交热度分数 (10%) - v4.0 联动 TG 实战数据
        let socialHeatScore = 0;
        let socialHeatDetail = '';

        try {
            // 基础分数来自 XData
            const heatResult = this.socialHeatScorer.score(
                xData || llmResult?.xData || {},
                null
            );
            socialHeatScore = heatResult.score;

            // 🔥 Telegram 频道数加分 (用户指定: 3+频道加3分, 10+频道加5分)
            const tgChannelCount = tgHeat?.channelCount || 0;
            let tgBonus = 0;
            if (tgChannelCount >= 10) {
                tgBonus = 5;
            } else if (tgChannelCount >= 3) {
                tgBonus = 3;
            }

            socialHeatScore = Math.min(socialHeatScore + tgBonus, w.socialHeat);
            socialHeatDetail = `${heatResult.heatLevel}${tgBonus > 0 ? ` (+${tgBonus} TG)` : ''}`;

            if (tgBonus > 0 || heatResult.heatLevel === 'ON_FIRE') {
                console.log(`🔥 [社交热度] ${socialHeatDetail}: ${tgChannelCount} TG 频道讨论`);
            }
        } catch (e) {
            socialHeatDetail = '未知';
        }
        details.push(`社交热度: ${socialHeatScore.toFixed(1)}/${w.socialHeat} (${socialHeatDetail})`);

        // 6. 报警动量分数 - v7.4.6 简化版: 每次推送+2分，封顶30分
        const signalCount = isTelegramSignal ? 0 : (token.signalCount || 0);
        let signalBonus = 0;
        let signalStatus = '';

        if (isTelegramSignal) {
            signalBonus = Math.min(fallbacks.telegram.signalMomentum, w.signalMomentum);
            signalStatus = 'SKIPPED(TG)';
        } else {
            // 简单加分：每次推送 +2 分，封顶 30 分
            signalBonus = Math.min(signalCount * 2, 30);
            if (signalCount >= 15) signalStatus = '🔥高热度';
            else if (signalCount >= 5) signalStatus = '📈中热度';
            else signalStatus = '🌱启动中';
        }
        details.push(`动量: ${signalBonus}/30 [${signalStatus}, ${signalCount}次]`);

        // 7. 安全性分数 (10%) - 包含持仓集中度检测
        let safetyScore = 0;
        let safetyDetails = [];

        if (token.isMintAbandoned === true) {
            safetyScore += 3;
            safetyDetails.push('Mint✓');
        } else if (token.freeze_authority === null && token.mint_authority === null) {
            safetyScore += 3;
            safetyDetails.push('Auth✓');
        } else {
            safetyDetails.push('Auth?');
        }

        // 流动性健康度
        const liquidity = token.liquidity || 0;
        const marketCap = token.marketCap || 0;
        if (liquidity >= this.hardGates.minLiquidity) {
            if (marketCap > 0 && (liquidity / marketCap) >= 0.15) {
                safetyScore += 4; // 流动性占比 >= 15%
                safetyDetails.push(`Liq✓(${(liquidity / marketCap * 100).toFixed(0)}%)`);
            } else if (marketCap > 0 && (liquidity / marketCap) >= 0.08) {
                safetyScore += 2; // 流动性占比 8-15%
                safetyDetails.push(`Liq~(${(liquidity / marketCap * 100).toFixed(0)}%)`);
            } else {
                safetyDetails.push(`Liq低`);
            }
        } else {
            safetyDetails.push(`Liq✗`);
        }

        // 持仓集中度检测 (使用 DeBot 数据)
        const top10Holding = aiReport?.distribution?.topHolderPercent || 0;
        if (top10Holding > 0) {
            if (top10Holding < 30) {
                safetyScore += 3;
                safetyDetails.push(`Top10:${top10Holding}%✓`);
            } else if (top10Holding < 50) {
                safetyScore += 1;
                safetyDetails.push(`Top10:${top10Holding}%~`);
            } else {
                safetyDetails.push(`Top10:${top10Holding}%⚠️`);
            }
        }

        safetyScore = Math.min(safetyScore, w.safety);
        details.push(`安全: ${safetyScore}/${w.safety} [${safetyDetails.join(',')}]`);

        // 总分
        const totalScore = smartMoneyScore + narrativeScore + entryTimingScore +
            keyInfluencerScore + socialHeatScore + signalBonus + safetyScore;

        console.log(`📊 评分明细 [${Math.round(totalScore)}分]: ${details.join(' | ')}`);

        return {
            total: Math.round(totalScore),
            breakdown: {
                smartMoney: smartMoneyScore,
                narrative: narrativeScore,
                entryTiming: entryTimingScore,       // 🔥 新增
                keyInfluencer: keyInfluencerScore,
                socialHeat: socialHeatScore,         // 🔥 新增
                signalMomentum: signalBonus,
                safety: safetyScore
            },
            signalCount: signalCount,
            superSignal: superSignal,
            llmRisk: llmResult?.risk_level || 'UNKNOWN',
            entryRecommendation: entryTimingDetail,  // 🔥 新增
            entryWarnings: entryWarnings,            // 🔥 新增
            xRisk: 'SKIPPED',
            xSummary: null,
            xMentions: null
        };
    }


    /**
     * 做出决策 v3.0
     * 
     * 决策矩阵:
     * - 超级信号 (Tier S 互动): 直接 BUY_PREMIUM
     * - < 50分: IGNORE
     * - 50-59分: WATCH
     * - 60-69分: BUY_SCOUT (0.10 SOL)
     * - 70-79分: BUY_NORMAL (0.15 SOL)
     * - 80+分: BUY_PREMIUM (0.25 SOL)
     * 
     * 强制降级规则:
     * - signalCount > 50: 强制 WATCH
     * - LLM risk_level = HIGH: 最高 WATCH (除非有超级信号)
     */
    makeDecision(token, aiReport, score) {
        const thresholds = this.scoringConfig.thresholds;
        const positions = this.scoringConfig.positions;
        const momentum = this.scoringConfig.signalMomentum;
        const bscLimits = this.scoringConfig.BSC_LIMITS;  // v6.8 BSC限制

        // === 🔥 超级信号优先处理 ===
        // Tier S 账号互动 = 直接买入，绕过正常评分
        if (score.superSignal?.type === 'TIER_S_INTERACTION') {
            console.log(`🔥 [超级信号] ${score.superSignal.message}`);

            // 检查是否有仓位
            if (this.hasAvailableSlot('premium')) {
                this.occupySlot('premium');
                return {
                    action: 'BUY',
                    tier: 'PREMIUM',
                    reason: `🔥 超级信号: ${score.superSignal.account} 互动 - 直接买入`,
                    position: positions.premium,
                    superSignal: score.superSignal
                };
            }
        }

        // === 强制降级规则 ===

        // 规则1: 信号过热 (>50次) → 强制 WATCH
        if (score.signalCount > momentum.overheat) {
            return {
                action: 'WATCH',
                tier: null,
                reason: `🔴 信号过热 (${score.signalCount}次 > ${momentum.overheat})，强制观望`,
                position: 0
            };
        }

        // 规则2: LLM 识别高风险 → 强制 WATCH (除非有 Tier A+ 信号)
        if (score.llmRisk === 'HIGH') {
            // 如果有 Tier A 以上的关键人物互动，允许继续评估
            if (score.superSignal?.type === 'MULTI_EXCHANGE_INTEREST') {
                console.log(`⚠️ [高风险但有交易所关注] 继续评估...`);
            } else {
                return {
                    action: 'WATCH',
                    tier: null,
                    reason: `⚠️ AI识别高风险，强制观望`,
                    position: 0
                };
            }
        }

        // 规则3: X 边界复核高风险 → 强制 WATCH
        if (score.xRisk === 'HIGH') {
            return {
                action: 'WATCH',
                tier: null,
                reason: `⚠️ X边界复核高风险: ${score.xSummary || 'unknown'}`,
                position: 0
            };
        }

        // ═══════════════════════════════════════════════════════════════
        // 🛑 v6.8 BSC 每日交易限制 (Stress Test)
        // BSC 胜率低，限制每日交易次数用于收集数据
        // ═══════════════════════════════════════════════════════════════
        if (bscLimits?.ENABLED && token.chain === 'BSC') {
            const todayBscCount = this.getTodayBscTradeCount();
            if (todayBscCount >= bscLimits.DAILY_TRADE_LIMIT) {
                console.log(`🛑 [BSC Limit] 今日已交易 ${todayBscCount} 次，达到限制 ${bscLimits.DAILY_TRADE_LIMIT}`);
                return {
                    action: 'WATCH',
                    tier: null,
                    reason: `🛑 BSC 每日限额已满 (${todayBscCount}/${bscLimits.DAILY_TRADE_LIMIT})`,
                    position: 0
                };
            }
            console.log(`📊 [BSC Limit] 今日 BSC 交易: ${todayBscCount}/${bscLimits.DAILY_TRADE_LIMIT}`);
        }

        // === 正常决策流程 ===

        // 低于忽略线 → IGNORE
        if (score.total < thresholds.ignore) {
            return {
                action: 'IGNORE',
                tier: null,
                reason: `❌ 评分不足 (${score.total}分 < ${thresholds.ignore})`,
                position: 0
            };
        }

        // 观察区间 [50, 60)
        if (score.total < thresholds.buyScout) {
            return {
                action: 'WATCH',
                tier: null,
                reason: `👀 观察中 (${score.total}分)`,
                position: 0
            };
        }

        // === 分级仓位管理 ===
        // 确定信号级别
        const signalTier = this.getSignalTier(score.total);

        if (!signalTier) {
            return {
                action: 'WATCH',
                tier: null,
                reason: `👀 评分不足买入线 (${score.total}分)`,
                position: 0
            };
        }

        // 检查该级别是否有空仓位
        if (!this.hasAvailableSlot(signalTier)) {
            return {
                action: 'WATCH',
                tier: signalTier,
                reason: `⏸️ ${signalTier}级仓位已满 (${this.currentPositions[signalTier]}/${this.scoringConfig.maxPositions[signalTier]})，等待空位`,
                position: 0
            };
        }

        // 确定仓位大小和标签
        let position, emoji;

        if (signalTier === 'premium') {
            // 精选级: 80+ 分
            position = positions.premium;
            emoji = '🚀';
        } else if (signalTier === 'normal') {
            // 普通级: 70-79 分
            position = positions.normal;
            emoji = '✅';
        } else {
            // Scout级: 60-69 分
            position = positions.scout;
            emoji = '🐦';
        }

        // ═══════════════════════════════════════════════════════════════
        // 🛑 v6.8 BSC 仓位上限 (Stress Test)
        // BSC 强制使用小仓位 0.02 BNB，不管评分多高
        // ═══════════════════════════════════════════════════════════════
        let positionNote = '';
        if (bscLimits?.ENABLED && token.chain === 'BSC') {
            const originalPosition = position;
            position = bscLimits.MAX_POSITION_BNB;  // 强制 0.02 BNB
            if (originalPosition !== position) {
                positionNote = ` [BSC限仓:${originalPosition}→${position}]`;
                console.log(`🛑 [BSC Limit] 仓位限制: ${originalPosition} → ${position} BNB`);
            }
        }

        // 占用仓位
        this.occupySlot(signalTier);

        return {
            action: 'BUY',
            tier: signalTier.toUpperCase(),
            reason: `${emoji} ${signalTier.toUpperCase()}级 (${score.total}分) - ${this.getDecisionReason(token, aiReport, score)}${positionNote}`,
            position
        };
    }

    /**
     * 生成决策理由 v3.0 (移除 TG 依赖)
     */
    getDecisionReason(token, aiReport, score) {
        const reasons = [];

        // 聪明钱
        const smartWallets = token.smartWalletOnline || 0;
        if (smartWallets >= 5) {
            reasons.push(`🐋${smartWallets}个聪明钱`);
        } else if (smartWallets >= 3) {
            reasons.push(`${smartWallets}个聪明钱`);
        }

        // DeBot AI 评分
        const debotScore = aiReport?.rating?.score || 0;
        if (debotScore >= 4) {
            reasons.push(`AI${debotScore}/5`);
        }

        // 关键人物
        if (score.breakdown?.keyInfluencer > 0) {
            reasons.push(`关键人物+${score.breakdown.keyInfluencer.toFixed(0)}`);
        }

        // 超级信号
        if (score.superSignal) {
            reasons.push(`🔥超级信号`);
        }

        return reasons.join(' + ') || `综合评分${score.total}分`;
    }

    /**
     * 打印验证结果 v2.0
     */
    printValidationResult(token, aiReport, tgHeat, score, decision, llmResult = null) {
        const symbol = token.symbol || token.tokenAddress.slice(0, 8);

        console.log(`\n${'='.repeat(60)}`);
        console.log(`📊 [CrossValidator] 验证结果: ${symbol}`);
        console.log(`${'='.repeat(60)}`);

        // 基础信息
        console.log(`📍 地址: ${token.tokenAddress}`);
        console.log(`⛓️  链: ${token.chain}`);
        console.log(`💰 流动性: $${(token.liquidity || 0).toLocaleString()}`);
        console.log(`📢 报警次数: ${token.signalCount || 0}`);

        // 分数明细 (v4.0)
        console.log(`\n📈 评分明细 (总分: ${score.total}/100):`);
        console.log(`   聪明钱:   ${score.breakdown.smartMoney}/30 (${token.smartWalletOnline || 0}个在线)`);
        console.log(`   AI叙事:   ${score.breakdown.narrative.toFixed(1)}/20 (DeBot ${aiReport?.rating?.score || 0}/5${llmResult ? `, LLM ${llmResult.score}分` : ''})`);
        console.log(`   入场时机: ${(score.breakdown.entryTiming || 0).toFixed(1)}/15 (${score.entryRecommendation || '未知'})`);
        console.log(`   关键人物: ${(score.breakdown.keyInfluencer || 0).toFixed(1)}/10`);
        console.log(`   社交热度: ${(score.breakdown.socialHeat || 0).toFixed(1)}/10`);
        console.log(`   报警动量: ${score.breakdown.signalMomentum}/5`);
        console.log(`   安全性:   ${score.breakdown.safety}/10`);

        // LLM 分析结果
        if (llmResult) {
            console.log(`\n🧠 LLM分析:`);
            console.log(`   评分: ${llmResult.score}/100`);
            console.log(`   判断: ${llmResult.reason}`);
            console.log(`   风险: ${llmResult.risk_level}`);
        }

        // X 边界复核
        if (score.xRisk && score.xRisk !== 'SKIPPED') {
            console.log(`\n🐦 X边界复核:`);
            console.log(`   风险: ${score.xRisk}`);
            if (score.xMentions !== null) console.log(`   提及: ${score.xMentions}`);
            if (score.xSummary) console.log(`   备注: ${score.xSummary}`);
        }

        // 决策
        const actionEmoji = {
            'BUY': '🟢',
            'WATCH': '🟡',
            'IGNORE': '⚫'
        };

        console.log(`\n🎯 决策: ${actionEmoji[decision.action]} ${decision.action}`);
        if (decision.tier) {
            console.log(`   等级: ${decision.tier}`);
        }
        console.log(`   理由: ${decision.reason}`);
        if (decision.position > 0) {
            console.log(`   仓位: ${decision.position} SOL`);
        }

        console.log(`${'='.repeat(60)}\n`);
    }

    /**
     * 启动验证器 v2.0
     * @param {Object} scoutInstance - 要使用的 DeBot Scout 实例
     */
    start(scoutInstance = null) {
        if (this.isRunning) {
            console.log('[CrossValidator] 已在运行中');
            return;
        }

        this.isRunning = true;
        this.init(scoutInstance);

        const t = this.scoringConfig.thresholds;
        const w = this.scoringConfig.weights;
        const p = this.scoringConfig.positions;

        console.log('\n🔄 [CrossValidator v6.0] 动态评分引擎启动');
        console.log(`   Hard Gates:`);
        console.log(`     - 最少聪明钱: ${this.hardGates.minSmartWalletOnline}`);
        console.log(`     - 流动性: $${this.hardGates.minLiquidity}-$${this.hardGates.maxMarketCap}`);
        console.log(`     - 市值: $${this.hardGates.minMarketCap / 1000}K-$${this.hardGates.maxMarketCap / 1000}K`);
        console.log(`     - 最低DeBot评分: ${this.hardGates.minAIScore}`);
        console.log(`   v6.0评分权重:`);
        console.log(`     - 聪明钱: ${w.smartMoneyCount}+${w.smartMoneyTrend}(趋势) | 信号: ${w.signalDensity}+${w.signalTrend}(趋势) | 安全: ${w.safety} | AI: ${w.debotAI}`);
        console.log(`   分层仓位:`);
        console.log(`     - 试探仓 (${t.trial}分+趋势): ${p.trial} SOL`);
        console.log(`     - Scout级 (${t.buyScout}分): ${p.scout} SOL`);
        console.log(`     - 确认仓 (${t.buyNormal}分): ${p.normal} SOL`);
        console.log(`     - 重仓 (${t.buyPremium}分): ${p.premium} SOL`);
        console.log(`   LLM分析: ❌ 已禁用 (使用DeBot数据)`);

        // v7.6 打印优化过滤配置
        printFilterConfig();
    }


    /**
     * 获取信号数据库实例 (用于定期清理)
     */
    getSignalDatabase() {
        return signalDatabase;
    }

    /**
     * 停止验证器
     */
    stop() {
        this.isRunning = false;
        console.log('[CrossValidator] 已停止');
    }

    /**
     * 获取验证统计
     */
    getStats() {
        const validated = Array.from(this.validatedTokens.values());

        return {
            totalValidated: validated.length,
            buySignals: validated.filter(v => v.decision.action === 'BUY').length,
            watchSignals: validated.filter(v => v.decision.action === 'WATCH').length,
            ignoredSignals: validated.filter(v => v.decision.action === 'IGNORE').length,
            avgScore: validated.length > 0
                ? validated.reduce((sum, v) => sum + v.score.total, 0) / validated.length
                : 0
        };
    }
}

// 单例导出
const crossValidator = new CrossValidator();

export default crossValidator;
export { CrossValidator };
