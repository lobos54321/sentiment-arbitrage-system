/**
 * 🎯 Ultra Human Sniper Filter v2.0
 *
 * 核心设计哲学:
 * - 从"剔除坏的"→"发现好的"
 * - 从"静态阈值"→"动态自适应"
 * - 从"单一评分"→"多维度画像"
 *
 * 猎人类型:
 * - 🦅 鹰型 (Eagle)  - 精准出手, 单笔高盈利 (不适合60秒轮询)
 * - 🦊 狐型 (Fox)    - 金狗猎手, 善于抄底 (最适合v6.9!)
 * - 🐢 龟型 (Turtle) - 长持仓, 波段操作 (适合跟单)
 * - 🐺 狼型 (Wolf)   - 中频交易, 稳定盈利 (适合跟单)
 *
 * v6.9 系统特性优化:
 * - 60秒轮询延迟 → 排除鹰型，优选狐型/龟型
 * - 单笔高赔率 → 筛选金狗猎手
 * - 非高频 → 排除机器人
 */

import { EventEmitter } from 'events';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { gmgnGateway, getLeaderboard, getWalletActivity, getWalletTokens, getTokenInfo } from '../utils/gmgn-api-gateway.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ═══════════════════════════════════════════════════════════════
// 猎人类型定义
// ═══════════════════════════════════════════════════════════════

const HUNTER_TYPES = {
    EAGLE: {
        type: 'EAGLE',
        emoji: '🦅',
        description: '精准狙击手 - 单笔高盈利',
        followStrategy: 'WARNING',      // 警告,不自动跟 (太快)
        positionMultiplier: 0.5,
        suitableForV69: false,
        color: '#FFD700'  // 金色
    },
    FOX: {
        type: 'FOX',
        emoji: '🦊',
        description: '金狗猎手 - 善于发现早期机会',
        followStrategy: 'IMMEDIATE',    // 立即跟单
        positionMultiplier: 1.2,
        suitableForV69: true,           // 最适合!
        color: '#FF6B35'  // 橙色
    },
    TURTLE: {
        type: 'TURTLE',
        emoji: '🐢',
        description: '波段猎手 - 长持仓稳定盈利',
        followStrategy: 'DELAYED',      // 延迟30分钟观察
        delayMinutes: 30,
        positionMultiplier: 1.5,
        suitableForV69: true,
        color: '#2ECC71'  // 绿色
    },
    WOLF: {
        type: 'WOLF',
        emoji: '🐺',
        description: '稳定猎手 - 中频稳定盈利',
        followStrategy: 'IMMEDIATE',
        positionMultiplier: 1.0,
        suitableForV69: true,
        color: '#3498DB'  // 蓝色
    },
    BOT: {
        type: 'BOT',
        emoji: '🤖',
        description: '疑似机器人 - 排除',
        followStrategy: 'REJECT',
        positionMultiplier: 0,
        suitableForV69: false,
        color: '#95A5A6'  // 灰色
    },
    NORMAL: {
        type: 'NORMAL',
        emoji: '👤',
        description: '普通交易者',
        followStrategy: 'WATCH_ONLY',
        positionMultiplier: 0.3,
        suitableForV69: false,
        color: '#BDC3C7'  // 浅灰
    }
};

// ═══════════════════════════════════════════════════════════════
// 猎人画像分类算法
// ═══════════════════════════════════════════════════════════════

function classifyHunterType(wallet) {
    const w = wallet;

    // 关键指标
    const txs1d = w.txs_1d || 0;
    const buy1d = w.buy_1d || 0;
    const sell1d = w.sell_1d || 0;
    const pnl1d = parseFloat(w.realized_profit_1d) || 0;
    const pnl7d = parseFloat(w.realized_profit_7d) || 0;
    const winrate1d = w.winrate_1d || 0;
    const winrate7d = w.winrate_7d || 0;
    const goldenDogs = w.pnl_gt_5x_num_7d || 0;
    const severeLosses = w.pnl_lt_minus_dot5_num_7d || 0;
    const avgHoldTime = w.avg_holding_period_1d || 0;
    const avgHoldTimeMinutes = avgHoldTime / 60;
    const profitPerTrade = txs1d > 0 ? pnl1d / txs1d : 0;
    const buySellRatio = sell1d > 0 ? buy1d / sell1d : buy1d;

    // ═══════════════════════════════════════════════════════════
    // 🌟 v7.4.3 VIP通道 - 金狗猎手直通
    // 有金狗记录 + 7天盈利 = 跳过BOT检测
    // ═══════════════════════════════════════════════════════════
    const hasGoldenRecord = goldenDogs >= 1 && pnl7d > 0;

    // ═══════════════════════════════════════════════════════════
    // 第一层: 排除机器人 (v7.4.3 放宽条件)
    // ═══════════════════════════════════════════════════════════

    if (!hasGoldenRecord) {  // 金狗猎手跳过BOT检测
        // 高频机器人 - 300笔以上 (原100)
        if (txs1d > 300) {
            return { ...HUNTER_TYPES.BOT, reason: 'high_frequency_300' };
        }

        // 假胜率 - 完美100%胜率 + 超过50笔 + 无金狗
        if (winrate1d >= 1.0 && txs1d > 50 && goldenDogs === 0) {
            return { ...HUNTER_TYPES.BOT, reason: 'fake_winrate' };
        }

        // 刷单机器人 - 买卖相等 + 超高频 + 无盈利优势
        if (buySellRatio > 0.95 && buySellRatio < 1.05 && txs1d > 80 && winrate1d < 0.55) {
            return { ...HUNTER_TYPES.BOT, reason: 'wash_trading' };
        }
    }

    // ═══════════════════════════════════════════════════════════
    // 第二层: 基本盈利要求 (v7.4.3 放宽条件)
    // ═══════════════════════════════════════════════════════════

    // 7天+1天都亏损才排除 (原：只看1天)
    if (pnl7d <= 0 && pnl1d <= 0) {
        return { ...HUNTER_TYPES.NORMAL, reason: 'both_period_loss' };
    }

    // 每笔利润太低 (降低到$5，原$10)
    // 但高频高胜率猎人例外
    if (profitPerTrade < 5 && !(txs1d > 50 && winrate7d >= 0.55)) {
        return { ...HUNTER_TYPES.NORMAL, reason: 'low_profit_per_trade' };
    }

    // ═══════════════════════════════════════════════════════════
    // 🆕 v7.4.3 高胜率优先通道
    // ═══════════════════════════════════════════════════════════

    // 7天胜率60%+ 且 7天盈利 = 至少是WOLF
    if (winrate7d >= 0.60 && pnl7d > 0) {
        // 有金狗记录 → FOX
        if (goldenDogs >= 1) {
            return {
                ...HUNTER_TYPES.FOX,
                reason: 'high_winrate_golden',
                metrics: { winrate7d, pnl7d, goldenDogs, txs1d }
            };
        }
        // 无金狗但稳定盈利 → WOLF
        return {
            ...HUNTER_TYPES.WOLF,
            reason: 'high_winrate_stable',
            metrics: { winrate7d, pnl7d, txs1d }
        };
    }

    // ═══════════════════════════════════════════════════════════
    // 第三层: 原有分类逻辑 (v7.4.3 条件放宽)
    // ═══════════════════════════════════════════════════════════

    // 🦅 Eagle - 精准狙击 (txs1d <= 20)
    // v7.4.4: 增加胜率门槛 ≥45%
    if (txs1d <= 20 && profitPerTrade >= 200 && avgHoldTimeMinutes < 30 && winrate1d >= 0.45) {
        return {
            ...HUNTER_TYPES.EAGLE,
            reason: 'precision_sniper',
            metrics: { profitPerTrade, avgHoldTimeMinutes, txs1d, winrate1d }
        };
    }

    // 🦊 Fox - 金狗猎手 (放宽到 txs1d <= 80，原40)
    // v7.4.4: 增加胜率门槛 ≥50%；若亏损多于金狗1.5倍则要求 ≥55%
    if (goldenDogs >= 1 && profitPerTrade >= 30 && txs1d <= 80) {
        // 动态胜率门槛：亏损多时要求更高胜率
        const foxWinrateThreshold = severeLosses > goldenDogs * 1.5 ? 0.55 : 0.50;
        if (winrate1d < foxWinrateThreshold) {
            // 胜率不达标，跳过 FOX 分类
        } else {
            const foxScore = goldenDogs * 30 + profitPerTrade / 10;
            return {
                ...HUNTER_TYPES.FOX,
                reason: 'golden_dog_hunter',
                foxScore,
                metrics: { goldenDogs, profitPerTrade, txs1d, winrate1d, severeLosses }
            };
        }
    }

    // 🐢 Turtle - 波段操作 (放宽到 avgHoldTimeMinutes >= 45，原60)
    if (avgHoldTimeMinutes >= 45 && winrate1d >= 0.45 && winrate1d <= 0.85 && txs1d <= 50) {
        return {
            ...HUNTER_TYPES.TURTLE,
            reason: 'swing_trader',
            metrics: { avgHoldTimeMinutes, winrate1d, txs1d }
        };
    }

    // 🐺 Wolf - 稳定交易 (放宽到 txs1d <= 100，原50)
    // v7.4.4: 提高胜率门槛从 40% 到 55%
    if (txs1d >= 10 && txs1d <= 100 &&
        winrate1d >= 0.55 &&
        profitPerTrade >= 10 &&
        severeLosses <= 5) {
        return {
            ...HUNTER_TYPES.WOLF,
            reason: 'consistent_trader',
            metrics: { txs1d, winrate1d, profitPerTrade, severeLosses }
        };
    }

    // 默认: 普通交易者
    return {
        ...HUNTER_TYPES.NORMAL,
        reason: 'unclassified',
        metrics: { txs1d, winrate1d, profitPerTrade }
    };
}

// ═══════════════════════════════════════════════════════════════
// 动态评分系统
// ═══════════════════════════════════════════════════════════════

function calculateDynamicScore(wallet, hunterProfile) {
    const w = wallet;

    // 基础数据
    const txs1d = w.txs_1d || 0;
    const pnl1d = parseFloat(w.realized_profit_1d) || 0;
    const pnl7d = parseFloat(w.realized_profit_7d) || 0;
    const winrate1d = w.winrate_1d || 0;
    const winrate7d = w.winrate_7d || 0;
    const goldenDogs = w.pnl_gt_5x_num_7d || 0;
    const severeLosses = w.pnl_lt_minus_dot5_num_7d || 0;
    const avgHoldTime = (w.avg_holding_period_1d || 0) / 60;  // 转为分钟
    const followCount = w.follow_count || 0;

    const profitPerTrade = txs1d > 0 ? pnl1d / txs1d : 0;

    // ═══════════════════════════════════════════════════════════
    // 评分维度 (0-100)
    // ═══════════════════════════════════════════════════════════

    const scores = {
        // 1. 每笔利润效率 (核心指标!)
        // $500+ = 100分, $100 = 50分, $20 = 20分
        profitEfficiency: Math.min(100, Math.max(0,
            profitPerTrade >= 500 ? 100 :
                profitPerTrade >= 100 ? 50 + (profitPerTrade - 100) / 8 :
                    profitPerTrade * 0.5
        )),

        // 2. 金狗命中率 (v6.9 核心!)
        // 4个+ = 100分, 2个 = 50分, 1个 = 25分
        goldenDogScore: Math.min(100, goldenDogs * 25),

        // 3. 胜率稳定性
        // 1D和7D胜率接近 = 稳定 (差值小于5%为满分)
        winrateStability: Math.max(0, 100 - Math.abs(winrate1d - winrate7d) * 400),

        // 4. 持仓匹配度 (针对60秒轮询)
        // 最佳: 30-180 分钟
        holdingMatch: avgHoldTime >= 30 && avgHoldTime <= 180
            ? 100
            : avgHoldTime > 180
                ? Math.max(50, 100 - (avgHoldTime - 180) / 10)
                : Math.max(0, avgHoldTime * 3),

        // 5. 亏损控制
        // 0次严重亏损 = 100分, 每次扣20分
        lossControl: Math.max(0, 100 - severeLosses * 20),

        // 6. 社区认可度
        // 最佳: 50-200 被追踪
        communityScore: followCount >= 50 && followCount <= 200
            ? 100
            : followCount < 50
                ? Math.min(100, followCount * 2)
                : Math.max(0, 100 - (followCount - 200) / 10)
    };

    // ═══════════════════════════════════════════════════════════
    // 动态权重 (根据猎人类型调整)
    // ═══════════════════════════════════════════════════════════

    let weights = {
        profitEfficiency: 0.25,
        goldenDogScore: 0.20,
        winrateStability: 0.15,
        holdingMatch: 0.15,
        lossControl: 0.15,
        communityScore: 0.10
    };

    // 根据猎人类型调整权重
    if (hunterProfile.type === 'FOX') {
        // 狐型猎手: 金狗命中更重要
        weights.goldenDogScore = 0.35;
        weights.profitEfficiency = 0.20;
        weights.holdingMatch = 0.10;
    } else if (hunterProfile.type === 'TURTLE') {
        // 龟型猎手: 持仓匹配和稳定性更重要
        weights.holdingMatch = 0.25;
        weights.winrateStability = 0.20;
        weights.goldenDogScore = 0.10;
    } else if (hunterProfile.type === 'EAGLE') {
        // 鹰型猎手: 每笔利润最重要
        weights.profitEfficiency = 0.40;
        weights.goldenDogScore = 0.15;
        weights.holdingMatch = 0.05;
    }

    // ═══════════════════════════════════════════════════════════
    // 计算总分
    // ═══════════════════════════════════════════════════════════

    let totalScore = 0;
    for (const [key, weight] of Object.entries(weights)) {
        totalScore += (scores[key] || 0) * weight;
    }

    // ═══════════════════════════════════════════════════════════
    // 加成/惩罚
    // ═══════════════════════════════════════════════════════════

    // 7天盈利稳定加成 (7天盈利 >= 3倍 1天盈利)
    if (pnl7d >= pnl1d * 3) {
        totalScore *= 1.15;
    }

    // 胜率在最优区间加成 (55%-75%)
    if (winrate1d >= 0.55 && winrate1d <= 0.75) {
        totalScore *= 1.10;
    }

    // 适合v6.9系统加成
    if (hunterProfile.suitableForV69) {
        totalScore *= 1.05;
    }

    // 今日亏损大惩罚 (理论上已经被分类排除,但双保险)
    if (pnl1d <= 0) {
        totalScore *= 0.2;
    }

    return {
        totalScore: Math.min(100, Math.max(0, totalScore)),
        breakdown: scores,
        weights,
        hunterType: hunterProfile.type
    };
}

// ═══════════════════════════════════════════════════════════════
// UltraHumanSniperV2 主类
// ═══════════════════════════════════════════════════════════════

export class UltraHumanSniperV2 extends EventEmitter {
    constructor(config = {}) {
        super();

        this.config = {
            // 路径配置
            sessionPath: config.sessionPath || path.join(__dirname, '../../config/gmgn_session.json'),
            cachePath: config.cachePath || path.join(__dirname, '../../logs/ultra-sniper-cache.json'),

            // 刷新间隔
            leaderboardRefreshInterval: config.leaderboardRefreshInterval || 10 * 60 * 1000,  // 10分钟
            activityPollInterval: config.activityPollInterval || 30 * 1000,  // 30秒

            // 猎人筛选
            topHuntersCount: config.topHuntersCount || 15,
            huntersToCheckPerPoll: config.huntersToCheckPerPoll || 5,
            minScore: config.minScore || 55,

            // 适合v6.9的猎人类型
            suitableTypes: config.suitableTypes || ['FOX', 'TURTLE', 'WOLF'],

            // 信号冷却
            signalCooldown: config.signalCooldown || 10 * 60 * 1000,  // 10分钟

            // 代币过滤
            maxMarketCap: config.maxMarketCap || 500000,  // $500K
            minLiquidity: config.minLiquidity || 5000,    // $5K
            maxTokenAge: config.maxTokenAge || 24,        // 24小时

            // 调试
            debug: config.debug !== false
        };

        // 状态
        this.isRunning = false;
        this.sessionData = null;
        this.topHunters = [];           // 筛选后的顶级猎人
        this.hunterProfiles = new Map(); // 猎人画像缓存
        this.signalHistory = new Map();  // 信号历史 (防重复)
        this.delayedQueue = [];          // 延迟跟单队列 (龟型)

        // 计时器
        this.leaderboardTimer = null;
        this.activityTimer = null;
        this.delayedTimer = null;

        // 统计
        this.stats = {
            leaderboardUpdates: 0,
            walletsScanned: 0,
            huntersFound: { EAGLE: 0, FOX: 0, TURTLE: 0, WOLF: 0, BOT: 0, NORMAL: 0 },
            signalsEmitted: 0,
            signalsDelayed: 0
        };

        console.log('[UltraSniperV2] 🎯 Ultra Human Sniper v2.0 初始化');
        console.log(`[UltraSniperV2] 📊 配置:`);
        console.log(`   - 适合类型: ${this.config.suitableTypes.join(', ')}`);
        console.log(`   - 最低评分: ${this.config.minScore}`);
        console.log(`   - 追踪猎人: ${this.config.topHuntersCount} 个`);
    }

    // ═══════════════════════════════════════════════════════════
    // Session 管理 (v7.4 由 GMGNApiGateway 统一管理)
    // ═══════════════════════════════════════════════════════════

    loadSession() {
        // Session 现在由 GMGNApiGateway 统一管理
        console.log(`[UltraSniperV2] ✅ Session 由 GMGNApiGateway 统一管理`);
    }

    // ═══════════════════════════════════════════════════════════
    // 获取牛人榜 + 猎人分类 + 评分
    // ═══════════════════════════════════════════════════════════

    async fetchAndClassifyHunters() {
        console.log('[UltraSniperV2] 📊 获取牛人榜数据...');

        try {
            // v7.4 使用 GMGNApiGateway
            const data = await getLeaderboard('sol', '7d', 'smart_degen', 100, {
                priority: 'normal',
                source: 'UltraSniperV2:Leaderboard'
            });

            if (data.error) {
                console.log(`[UltraSniperV2] ❌ API 错误: ${data.error}`);
                return [];
            }

            const wallets = data.data?.rank || [];
            console.log(`[UltraSniperV2] 📥 获取到 ${wallets.length} 个钱包，开始分类...`);

            // 重置统计
            this.stats.walletsScanned = wallets.length;
            this.stats.huntersFound = { EAGLE: 0, FOX: 0, TURTLE: 0, WOLF: 0, BOT: 0, NORMAL: 0 };

            // 分类和评分
            const hunters = [];

            for (const wallet of wallets) {
                // 1. 分类猎人类型
                const profile = classifyHunterType(wallet);
                this.stats.huntersFound[profile.type]++;

                // 2. 排除不适合的类型
                if (!this.config.suitableTypes.includes(profile.type)) {
                    continue;
                }

                // 3. 计算评分
                const scoreResult = calculateDynamicScore(wallet, profile);

                // 4. 筛选达标的猎人
                if (scoreResult.totalScore >= this.config.minScore) {
                    const address = wallet.wallet_address || wallet.address;
                    const name = wallet.twitter_name || wallet.name || address.slice(0, 8);

                    hunters.push({
                        address,
                        name,
                        twitter: wallet.twitter_username,
                        avatar: wallet.avatar,
                        profile,
                        score: scoreResult.totalScore,
                        scoreBreakdown: scoreResult.breakdown,
                        metrics: {
                            pnl1d: parseFloat(wallet.realized_profit_1d) || 0,
                            pnl7d: parseFloat(wallet.realized_profit_7d) || 0,
                            winrate1d: wallet.winrate_1d || 0,
                            txs1d: wallet.txs_1d || 0,
                            goldenDogs: wallet.pnl_gt_5x_num_7d || 0,
                            profitPerTrade: wallet.txs_1d > 0
                                ? (parseFloat(wallet.realized_profit_1d) || 0) / wallet.txs_1d
                                : 0,
                            avgHoldTimeMinutes: (wallet.avg_holding_period_1d || 0) / 60,
                            followCount: wallet.follow_count || 0
                        },
                        raw: wallet
                    });

                    // 缓存猎人画像
                    this.hunterProfiles.set(address, { profile, score: scoreResult });
                }
            }

            // 按评分排序，取前 N 个
            hunters.sort((a, b) => b.score - a.score);
            this.topHunters = hunters.slice(0, this.config.topHuntersCount);

            this.stats.leaderboardUpdates++;

            // 打印结果
            this.printHunterSummary();

            // 保存缓存
            this.saveCache();

            return this.topHunters;

        } catch (e) {
            console.error('[UltraSniperV2] ❌ 获取牛人榜失败:', e.message);
            return [];
        }
    }

    printHunterSummary() {
        console.log(`\n[UltraSniperV2] ═══════════════════════════════════════════════════════`);
        console.log(`[UltraSniperV2] 🎯 猎人分类结果:`);
        console.log(`   扫描: ${this.stats.walletsScanned} 个钱包`);
        console.log(`   🦅 Eagle: ${this.stats.huntersFound.EAGLE} | 🦊 Fox: ${this.stats.huntersFound.FOX}`);
        console.log(`   🐢 Turtle: ${this.stats.huntersFound.TURTLE} | 🐺 Wolf: ${this.stats.huntersFound.WOLF}`);
        console.log(`   🤖 Bot: ${this.stats.huntersFound.BOT} | 👤 Normal: ${this.stats.huntersFound.NORMAL}`);
        console.log(`[UltraSniperV2] ✅ 最终追踪 ${this.topHunters.length} 个猎人:`);

        for (let i = 0; i < Math.min(this.topHunters.length, 10); i++) {
            const h = this.topHunters[i];
            const m = h.metrics;
            console.log(`   ${i + 1}. ${h.profile.emoji} ${h.name} (${h.profile.type})`);
            console.log(`      分数: ${h.score.toFixed(0)} | 每笔$${m.profitPerTrade.toFixed(0)} | 金狗${m.goldenDogs} | 胜率${(m.winrate1d * 100).toFixed(0)}%`);
        }
        console.log(`[UltraSniperV2] ═══════════════════════════════════════════════════════\n`);
    }

    // ═══════════════════════════════════════════════════════════
    // 轮询猎人活动
    // ═══════════════════════════════════════════════════════════

    async pollHunterActivities() {
        if (!this.isRunning || this.topHunters.length === 0) return;

        const now = Date.now();

        // 检查前 N 个猎人
        const huntersToCheck = this.topHunters.slice(0, this.config.huntersToCheckPerPoll);

        for (const hunter of huntersToCheck) {
            try {
                // v7.4 使用 GMGNApiGateway
                const data = await getWalletActivity('sol', hunter.address, 'buy', 5, {
                    priority: 'high',
                    source: 'UltraSniperV2:Activity'
                });

                if (data.error) continue;

                const activities = data.data?.activities || [];

                for (const activity of activities) {
                    // 检查是否是最近 90 秒内的买入
                    const activityTime = (activity.timestamp || 0) * 1000;
                    if (now - activityTime > 90000) continue;

                    const tokenAddress = activity.token_address;
                    if (!tokenAddress) continue;

                    // 检查冷却期
                    const cooldownKey = `${hunter.address}-${tokenAddress}`;
                    const lastSignal = this.signalHistory.get(cooldownKey);
                    if (lastSignal && now - lastSignal < this.config.signalCooldown) continue;

                    // 检查代币条件
                    const marketCap = activity.market_cap || 0;
                    const liquidity = activity.liquidity || 0;

                    if (marketCap > this.config.maxMarketCap) continue;
                    if (liquidity < this.config.minLiquidity) continue;

                    // 根据猎人类型决定策略
                    await this.processHunterBuy(hunter, activity, now);

                    this.signalHistory.set(cooldownKey, now);
                }

                // 请求间隔
                await new Promise(r => setTimeout(r, 200));

            } catch (e) {
                // 忽略单个猎人的错误
            }
        }
    }

    async processHunterBuy(hunter, activity, now) {
        const tokenAddress = activity.token_address;
        const symbol = activity.token_symbol || activity.symbol || tokenAddress.slice(0, 8);

        console.log(`\n[UltraSniperV2] ═══════════════════════════════════════════════════════`);
        console.log(`[UltraSniperV2] 🎯 检测到猎人买入!`);
        console.log(`   ${hunter.profile.emoji} 猎人: ${hunter.name} (${hunter.profile.type})`);
        console.log(`   📊 评分: ${hunter.score.toFixed(0)} | ${hunter.profile.description}`);
        console.log(`   🪙 代币: ${symbol} (${tokenAddress.slice(0, 12)}...)`);
        console.log(`   💰 市值: $${((activity.market_cap || 0) / 1000).toFixed(1)}K`);

        // 根据猎人类型决定策略
        const strategy = hunter.profile.followStrategy;

        if (strategy === 'IMMEDIATE') {
            // 🦊 Fox / 🐺 Wolf - 立即发出信号
            console.log(`   ⚡ 策略: 立即跟单 (${hunter.profile.type})`);
            this.emitSignal(hunter, activity, now);

        } else if (strategy === 'DELAYED') {
            // 🐢 Turtle - 加入延迟队列
            const delayMinutes = hunter.profile.delayMinutes || 30;
            console.log(`   ⏳ 策略: 延迟 ${delayMinutes} 分钟观察`);

            this.delayedQueue.push({
                hunter,
                activity,
                addedAt: now,
                executeAt: now + delayMinutes * 60 * 1000
            });
            this.stats.signalsDelayed++;

        } else if (strategy === 'WARNING') {
            // 🦅 Eagle - 只发警告，不自动跟单
            console.log(`   ⚠️ 策略: 仅警告 (Eagle 太快，60秒轮询跟不上)`);
            this.emit('warning', {
                type: 'eagle_buy',
                hunter,
                token: {
                    ca: tokenAddress,
                    symbol,
                    marketCap: activity.market_cap,
                    liquidity: activity.liquidity
                },
                timestamp: now
            });
        }

        console.log(`[UltraSniperV2] ═══════════════════════════════════════════════════════\n`);
    }

    emitSignal(hunter, activity, now) {
        const tokenAddress = activity.token_address;
        const symbol = activity.token_symbol || activity.symbol || tokenAddress.slice(0, 8);

        // 计算建议仓位
        const basePosition = 0.1;  // 基础 0.1 SOL
        const positionSize = basePosition * hunter.profile.positionMultiplier;

        const signal = {
            source: 'ultra_sniper_v2',
            type: 'hunter_buy',
            chain: 'sol',

            // 代币信息
            token_ca: tokenAddress,
            symbol: symbol,
            market_cap: activity.market_cap || 0,
            liquidity: activity.liquidity || 0,
            price: activity.price || 0,

            // 猎人信息
            hunter: {
                address: hunter.address,
                name: hunter.name,
                type: hunter.profile.type,
                emoji: hunter.profile.emoji,
                description: hunter.profile.description,
                score: hunter.score,
                metrics: hunter.metrics
            },

            // 交易策略
            strategy: {
                followType: hunter.profile.followStrategy,
                positionSize: positionSize,
                positionMultiplier: hunter.profile.positionMultiplier,
                // 根据猎人类型设置止损止盈
                stopLoss: hunter.profile.type === 'FOX' ? -0.30 :    // 金狗需要时间
                    hunter.profile.type === 'TURTLE' ? -0.20 : // 龟型严格
                        -0.25,                                      // 默认
                takeProfit: hunter.profile.type === 'FOX' ? 2.00 :   // 金狗期待高回报
                    hunter.profile.type === 'TURTLE' ? 1.50 :
                        1.00
            },

            // 元数据
            urgency: hunter.profile.type === 'FOX' ? 'high' : 'normal',
            timestamp: now,
            buy_amount_usd: activity.amount_usd || activity.amount || 0,

            // v7.4 信号血统追踪 (Signal Lineage)
            signalLineage: {
                source: 'ultra_sniper_v2',
                hunterType: hunter.profile.type,
                hunterAddr: hunter.address,
                hunterScore: hunter.score,
                route: 'flash_scout',
                entryReason: hunter.profile.reason || `${hunter.profile.type.toLowerCase()}_hunter`,
                confidence: 'direct'
            }
        };

        this.stats.signalsEmitted++;
        this.emit('signal', signal);

        console.log(`   📤 信号已发出 | 仓位: ${positionSize.toFixed(2)} SOL | 止损: ${(signal.strategy.stopLoss * 100).toFixed(0)}% | 止盈: ${(signal.strategy.takeProfit * 100).toFixed(0)}%`);
    }

    // ═══════════════════════════════════════════════════════════
    // 处理延迟队列 (龟型猎人)
    // ═══════════════════════════════════════════════════════════

    async processDelayedQueue() {
        const now = Date.now();
        const readyItems = this.delayedQueue.filter(item => now >= item.executeAt);

        for (const item of readyItems) {
            // 从队列移除
            const index = this.delayedQueue.indexOf(item);
            if (index > -1) {
                this.delayedQueue.splice(index, 1);
            }

            const tokenCA = item.activity.token_address;
            const symbol = item.activity.token_symbol || item.activity.symbol || tokenCA.slice(0, 8);
            const hunterAddress = item.hunter.address;

            console.log(`\n[UltraSniperV2] 🐢 延迟信号到期: ${item.hunter.name} → ${symbol}`);

            // P0-1 修复: 检查猎人是否还持有该代币
            const stillHolding = await this.checkHunterHolding(hunterAddress, tokenCA);

            if (!stillHolding.holds) {
                // 猎人已卖出，取消跟单
                console.log(`   ❌ 猎人已清仓 (${stillHolding.reason})，取消跟单`);
                this.stats.signalsCancelled = (this.stats.signalsCancelled || 0) + 1;
                continue;
            }

            // 检查价格是否暴涨 (超过买入价的 3x，不再跟单)
            const currentPrice = await this.getTokenPrice(tokenCA);
            const buyPrice = item.activity.price || 0;

            if (buyPrice > 0 && currentPrice > 0) {
                const priceChange = currentPrice / buyPrice;
                if (priceChange > 3.0) {
                    console.log(`   ⚠️ 价格已涨 ${((priceChange - 1) * 100).toFixed(0)}%，放弃跟单`);
                    this.stats.signalsCancelled = (this.stats.signalsCancelled || 0) + 1;
                    continue;
                }
                console.log(`   📈 价格变化: ${((priceChange - 1) * 100).toFixed(1)}%`);
            }

            console.log(`   ✅ 猎人仍持仓，执行跟单信号`);

            // 更新activity中的当前价格
            const updatedActivity = {
                ...item.activity,
                price: currentPrice || item.activity.price,
                delayed_minutes: ((now - item.addedAt) / 60000).toFixed(0)
            };

            this.emitSignal(item.hunter, updatedActivity, now);
        }
    }

    /**
     * 检查猎人是否还持有某代币
     * @param {string} walletAddress - 猎人钱包地址
     * @param {string} tokenCA - 代币合约地址
     * @returns {Promise<{holds: boolean, reason: string, balance?: number}>}
     */
    async checkHunterHolding(walletAddress, tokenCA) {
        try {
            // v7.4 使用 GMGNApiGateway
            const data = await getWalletTokens('sol', walletAddress, {
                priority: 'normal',
                source: 'UltraSniperV2:HoldingCheck'
            });

            if (data.error) {
                // v7.4.2 API 失败时严格处理，放弃跟单
                // 原因: 无法确认猎人是否仍持仓，跟单风险高
                console.log(`   ⚠️ 无法验证持仓 (${data.error})，放弃跟单`);
                return { holds: false, reason: 'api_error_strict' };
            }

            const tokens = data.data?.tokens || data.tokens || [];

            // 查找特定代币
            const holding = tokens.find(t =>
                t.token_address?.toLowerCase() === tokenCA.toLowerCase() ||
                t.address?.toLowerCase() === tokenCA.toLowerCase()
            );

            if (holding) {
                const balance = parseFloat(holding.balance) || parseFloat(holding.amount) || 0;
                const valueUsd = parseFloat(holding.value_usd) || parseFloat(holding.usd_value) || 0;

                if (balance > 0) {
                    return {
                        holds: true,
                        reason: 'holding',
                        balance: balance,
                        valueUsd: valueUsd
                    };
                }
            }

            return { holds: false, reason: 'sold_or_transferred' };

        } catch (e) {
            // v7.4.2 异常时严格处理，放弃跟单
            console.log(`   ⚠️ 持仓检查异常: ${e.message}，放弃跟单`);
            return { holds: false, reason: 'check_failed_strict' };
        }
    }

    /**
     * 获取代币当前价格
     * @param {string} tokenCA - 代币合约地址
     * @returns {Promise<number|null>}
     */
    async getTokenPrice(tokenCA) {
        try {
            // v7.4 使用 GMGNApiGateway
            const data = await getTokenInfo('sol', tokenCA, {
                priority: 'high',
                source: 'UltraSniperV2:TokenPrice'
            });

            if (data.error) return null;

            const token = data.data?.token || data.token || data.data;
            return parseFloat(token?.price) || parseFloat(token?.current_price) || null;

        } catch (e) {
            return null;
        }
    }

    // ═══════════════════════════════════════════════════════════
    // 缓存管理
    // ═══════════════════════════════════════════════════════════

    saveCache() {
        try {
            const cacheData = {
                hunters: this.topHunters.map(h => ({
                    address: h.address,
                    name: h.name,
                    type: h.profile.type,
                    score: h.score,
                    metrics: h.metrics
                })),
                stats: this.stats,
                savedAt: new Date().toISOString()
            };

            fs.mkdirSync(path.dirname(this.config.cachePath), { recursive: true });
            fs.writeFileSync(this.config.cachePath, JSON.stringify(cacheData, null, 2));
        } catch (e) {
            // 忽略缓存错误
        }
    }

    loadCache() {
        try {
            if (fs.existsSync(this.config.cachePath)) {
                const data = JSON.parse(fs.readFileSync(this.config.cachePath, 'utf8'));
                // 缓存 30 分钟有效
                const cacheAge = Date.now() - new Date(data.savedAt).getTime();
                if (cacheAge < 30 * 60 * 1000) {
                    console.log(`[UltraSniperV2] 📂 加载缓存 (${(cacheAge / 60000).toFixed(0)}分钟前)`);
                    return data;
                }
            }
        } catch (e) {
            // 忽略
        }
        return null;
    }

    // ═══════════════════════════════════════════════════════════
    // 启动/停止
    // ═══════════════════════════════════════════════════════════

    async start() {
        if (this.isRunning) {
            console.log('[UltraSniperV2] 已经在运行中');
            return;
        }

        console.log('[UltraSniperV2] 🚀 启动 Ultra Human Sniper v2.0...');

        try {
            // 加载 session
            this.loadSession();

            // 尝试加载缓存
            const cache = this.loadCache();

            // 获取牛人榜 + 分类
            await this.fetchAndClassifyHunters();

            if (this.topHunters.length === 0) {
                console.log('[UltraSniperV2] ⚠️ 没有找到合适的猎人，继续运行等待...');
            }

            this.isRunning = true;

            // 启动活动轮询
            this.activityTimer = setInterval(async () => {
                await this.pollHunterActivities();
            }, this.config.activityPollInterval);

            // 首次立即执行
            await this.pollHunterActivities();

            // 定时刷新牛人榜
            this.leaderboardTimer = setInterval(async () => {
                console.log('[UltraSniperV2] 🔄 刷新猎人列表...');
                await this.fetchAndClassifyHunters();
            }, this.config.leaderboardRefreshInterval);

            // 处理延迟队列
            this.delayedTimer = setInterval(async () => {
                await this.processDelayedQueue();
            }, 60000);  // 每分钟检查

            // v7.4.1 启动内存清理定时器 (每30分钟清理过期数据)
            this.cleanupTimer = setInterval(() => {
                this.cleanupStaleData();
            }, 30 * 60 * 1000);

            console.log('[UltraSniperV2] ✅ Ultra Human Sniper v2.0 已启动');
            console.log(`[UltraSniperV2] - 追踪 ${this.topHunters.length} 个猎人`);
            console.log(`[UltraSniperV2] - 活动轮询: 每 ${this.config.activityPollInterval / 1000} 秒`);
            console.log(`[UltraSniperV2] - 猎人刷新: 每 ${this.config.leaderboardRefreshInterval / 60000} 分钟`);
            console.log(`[UltraSniperV2] - 内存清理: 每 30 分钟`);

        } catch (error) {
            console.error('[UltraSniperV2] 启动失败:', error.message);
            throw error;
        }
    }

    async stop() {
        console.log('[UltraSniperV2] ⏹️ 正在停止...');

        this.isRunning = false;

        if (this.leaderboardTimer) {
            clearInterval(this.leaderboardTimer);
            this.leaderboardTimer = null;
        }

        if (this.activityTimer) {
            clearInterval(this.activityTimer);
            this.activityTimer = null;
        }

        if (this.delayedTimer) {
            clearInterval(this.delayedTimer);
            this.delayedTimer = null;
        }

        // v7.4.1 清理内存清理定时器
        if (this.cleanupTimer) {
            clearInterval(this.cleanupTimer);
            this.cleanupTimer = null;
        }

        // 保存最终缓存
        this.saveCache();

        console.log('[UltraSniperV2] ⏹️ 已停止');
        console.log(`[UltraSniperV2] 📊 运行统计:`);
        console.log(`   - 牛人榜更新: ${this.stats.leaderboardUpdates} 次`);
        console.log(`   - 信号发出: ${this.stats.signalsEmitted} 个`);
        console.log(`   - 延迟信号: ${this.stats.signalsDelayed} 个`);
    }

    /**
     * v7.4.1 清理过期数据，防止内存泄漏
     */
    cleanupStaleData() {
        const now = Date.now();
        const maxAge = 60 * 60 * 1000;  // 1小时

        // 清理过期的信号历史
        let cleanedSignals = 0;
        for (const [key, timestamp] of this.signalHistory.entries()) {
            if (now - timestamp > maxAge) {
                this.signalHistory.delete(key);
                cleanedSignals++;
            }
        }

        // 清理过期的猎人画像缓存
        let cleanedProfiles = 0;
        for (const [address, data] of this.hunterProfiles.entries()) {
            // 保留活跃猎人（在 topHunters 中）
            const isActiveHunter = this.topHunters.some(h => h.address === address);
            if (!isActiveHunter && data.score && data.score.timestamp && now - data.score.timestamp > maxAge) {
                this.hunterProfiles.delete(address);
                cleanedProfiles++;
            }
        }

        if (cleanedSignals > 0 || cleanedProfiles > 0) {
            console.log(`[UltraSniperV2] 🧹 清理了 ${cleanedSignals} 条信号历史, ${cleanedProfiles} 个猎人画像`);
        }
    }

    // ═══════════════════════════════════════════════════════════
    // 状态查询
    // ═══════════════════════════════════════════════════════════

    getStatus() {
        return {
            isRunning: this.isRunning,
            huntersTracking: this.topHunters.length,
            topHunters: this.topHunters.map(h => ({
                name: h.name,
                type: h.profile.type,
                emoji: h.profile.emoji,
                score: h.score,
                profitPerTrade: h.metrics.profitPerTrade,
                goldenDogs: h.metrics.goldenDogs,
                winrate: h.metrics.winrate1d
            })),
            delayedQueue: this.delayedQueue.length,
            stats: this.stats
        };
    }

    // ═══════════════════════════════════════════════════════════
    // 工具方法
    // ═══════════════════════════════════════════════════════════

    getHunterByAddress(address) {
        return this.topHunters.find(h => h.address === address);
    }

    getHuntersByType(type) {
        return this.topHunters.filter(h => h.profile.type === type);
    }
}

// ═══════════════════════════════════════════════════════════════
// 导出工具函数
// ═══════════════════════════════════════════════════════════════

export {
    classifyHunterType,
    calculateDynamicScore,
    HUNTER_TYPES
};

export default UltraHumanSniperV2;
