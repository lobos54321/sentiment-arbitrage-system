/**
 * Shadow Protocol v2.1 - Shadow Wallet Consensus Detector
 *
 * v7.4 重新定位: 从"牛人榜扫描"改为"影子钱包共识检测"
 *
 * 核心设计哲学:
 * - Ultra Sniper V2 负责: 发现顶级猎人 (FOX/TURTLE/WOLF)
 * - Shadow Protocol V2 负责: 检测"跟单共识"
 *
 * 检测逻辑:
 * 1. 接收 UltraSniperV2 的猎人买入信号
 * 2. 监控该代币的后续买入者
 * 3. 如果短时间内有多个"影子钱包"也买入 → 共识信号
 * 4. 共识强度 = 影子钱包数量 + 影子钱包质量
 *
 * 影子钱包识别:
 * - 经常在顶级猎人买入后买入同一代币
 * - 盈利能力不如顶级猎人但稳定正收益
 * - 跟单时间窗口 < 5 分钟
 */

import { EventEmitter } from 'events';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { gmgnGateway, getTokenInfo } from '../utils/gmgn-api-gateway.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ═══════════════════════════════════════════════════════════════
// Shadow Protocol V2.1 主类
// ═══════════════════════════════════════════════════════════════

export class ShadowProtocolV2 extends EventEmitter {
    constructor(config = {}) {
        super();

        this.config = {
            // 共识检测配置
            consensusTimeWindow: config.consensusTimeWindow || 5 * 60 * 1000,  // 5分钟窗口
            minShadowBuyers: config.minShadowBuyers || 3,  // 最少影子买家数
            maxConsensusWait: config.maxConsensusWait || 10 * 60 * 1000,  // 最长等待10分钟

            // 影子钱包评分阈值
            minShadowScore: config.minShadowScore || 30,  // 影子钱包最低评分

            // 信号增强
            consensusBoostThreshold: config.consensusBoostThreshold || 5,  // 5个影子 = 强共识
            strongConsensusMultiplier: config.strongConsensusMultiplier || 1.5,

            // 代币过滤
            maxMarketCap: config.maxMarketCap || 500000,  // $500K
            minLiquidity: config.minLiquidity || 5000,    // $5K

            // 缓存路径
            cachePath: config.cachePath || path.join(__dirname, '../../logs/shadow-consensus-cache.json'),

            // 调试
            debug: config.debug !== false
        };

        // 状态
        this.isRunning = false;

        // 待检测队列: 等待共识的猎人买入
        // Map<tokenCA, PendingConsensus>
        this.pendingConsensus = new Map();

        // 已知影子钱包数据库
        // Map<walletAddress, ShadowProfile>
        this.shadowWallets = new Map();

        // 代币买入者追踪
        // Map<tokenCA, Set<walletAddress>>
        this.tokenBuyers = new Map();

        // 计时器
        this.consensusCheckTimer = null;

        // 统计
        this.stats = {
            hunterBuysReceived: 0,
            consensusDetected: 0,
            weakConsensus: 0,
            strongConsensus: 0,
            signalsEmitted: 0,
            shadowWalletsTracked: 0
        };

        console.log('[ShadowV2] 🥷 Shadow Protocol v2.1 初始化 (Shadow Wallet Consensus Detector)');
        console.log(`[ShadowV2] 📊 配置:`);
        console.log(`   - 共识窗口: ${this.config.consensusTimeWindow / 1000}秒`);
        console.log(`   - 最少影子买家: ${this.config.minShadowBuyers} 个`);
        console.log(`   - 强共识阈值: ${this.config.consensusBoostThreshold} 个`);
    }

    // ═══════════════════════════════════════════════════════════
    // 接收猎人买入信号 (来自 UltraSniperV2)
    // ═══════════════════════════════════════════════════════════

    /**
     * 处理猎人买入信号
     * 这是 Shadow Protocol V2 的主要入口
     * @param {Object} hunterSignal - 来自 UltraSniperV2 的信号
     */
    async onHunterBuy(hunterSignal) {
        if (!this.isRunning) return;

        const tokenCA = hunterSignal.token_ca;
        const hunterAddr = hunterSignal.hunter?.address || hunterSignal.shadow_wallet;
        const hunterType = hunterSignal.hunter?.type || hunterSignal.signalLineage?.hunterType;
        const symbol = hunterSignal.symbol;

        this.stats.hunterBuysReceived++;

        console.log(`[ShadowV2] 📥 收到猎人买入: ${hunterType || 'UNKNOWN'} → ${symbol}`);

        // 如果该代币已在监控中，更新信息
        if (this.pendingConsensus.has(tokenCA)) {
            const existing = this.pendingConsensus.get(tokenCA);
            existing.hunters.push({
                address: hunterAddr,
                type: hunterType,
                timestamp: Date.now()
            });
            console.log(`[ShadowV2] 📍 代币已在监控 (${existing.hunters.length} 个猎人买入)`);
            return;
        }

        // 创建新的待检测记录
        const pendingRecord = {
            tokenCA,
            symbol,
            chain: hunterSignal.chain || 'sol',
            marketCap: hunterSignal.market_cap || 0,
            liquidity: hunterSignal.liquidity || 0,
            price: hunterSignal.price || 0,
            hunters: [{
                address: hunterAddr,
                type: hunterType,
                score: hunterSignal.hunter?.score || hunterSignal.shadow_score || 0,
                timestamp: Date.now()
            }],
            shadowBuyers: [],
            startTime: Date.now(),
            originalSignal: hunterSignal
        };

        this.pendingConsensus.set(tokenCA, pendingRecord);

        console.log(`[ShadowV2] 🔍 开始监控共识: ${symbol} (等待 ${this.config.maxConsensusWait / 60000} 分钟)`);

        // 初始化代币买入者追踪
        this.tokenBuyers.set(tokenCA, new Set([hunterAddr]));

        // 开始监控该代币的后续买入
        this.startTokenMonitoring(tokenCA);
    }

    // ═══════════════════════════════════════════════════════════
    // 代币买入监控 (通过 GMGN API)
    // ═══════════════════════════════════════════════════════════

    /**
     * 开始监控代币的后续买入
     */
    async startTokenMonitoring(tokenCA) {
        // 实际实现中，我们可以通过以下方式监控:
        // 1. GMGN WebSocket (如果有)
        // 2. 定期轮询代币的最近交易
        // 3. 监听其他模块的买入信号

        // 目前使用定期轮询方式
        // 监控会在 checkConsensus 中进行
    }

    /**
     * 检查代币的最近买入者
     */
    async fetchRecentBuyers(tokenCA) {
        try {
            // 使用 GMGNApiGateway 获取代币的最近交易
            const result = await gmgnGateway.request(
                `/defi/quotation/v1/tokens/sol/${tokenCA}/trades?limit=50`,
                { priority: 'normal', source: 'ShadowV2:RecentBuyers' }
            );

            if (result.error) return [];

            const trades = result.data?.trades || result.data || [];

            // 筛选买入交易
            const buyTrades = trades.filter(t =>
                t.event === 'Buy' || t.type === 'buy' || t.side === 'buy'
            );

            return buyTrades.map(t => ({
                address: t.maker || t.wallet || t.from,
                amount: t.amount_usd || t.amount || 0,
                timestamp: (t.timestamp || 0) * 1000,
                txHash: t.tx_hash || t.hash
            }));

        } catch (e) {
            return [];
        }
    }

    // ═══════════════════════════════════════════════════════════
    // 共识检测引擎
    // ═══════════════════════════════════════════════════════════

    /**
     * 定期检查所有待检测代币的共识状态
     */
    async checkConsensus() {
        if (!this.isRunning) return;

        const now = Date.now();
        const toRemove = [];

        for (const [tokenCA, pending] of this.pendingConsensus.entries()) {
            // 检查是否超时
            if (now - pending.startTime > this.config.maxConsensusWait) {
                console.log(`[ShadowV2] ⏰ 共识超时: ${pending.symbol} (${pending.shadowBuyers.length} 影子买家)`);
                toRemove.push(tokenCA);

                // 即使超时，如果有一些影子买家，也发出弱信号
                if (pending.shadowBuyers.length >= 2) {
                    this.emitConsensusSignal(pending, 'weak', 'timeout');
                }
                continue;
            }

            // 获取最近买入者
            const recentBuyers = await this.fetchRecentBuyers(tokenCA);

            // 筛选在共识窗口内的新买家
            const existingBuyers = this.tokenBuyers.get(tokenCA) || new Set();
            const hunterAddrs = new Set(pending.hunters.map(h => h.address.toLowerCase()));

            for (const buyer of recentBuyers) {
                if (!buyer.address) continue;

                const buyerAddr = buyer.address.toLowerCase();

                // 跳过已知买家和猎人本身
                if (existingBuyers.has(buyerAddr)) continue;
                if (hunterAddrs.has(buyerAddr)) continue;

                // 检查时间窗口
                const timeSinceHunterBuy = buyer.timestamp - pending.startTime;
                if (timeSinceHunterBuy < 0 || timeSinceHunterBuy > this.config.consensusTimeWindow) {
                    continue;
                }

                // 评估影子钱包质量
                const shadowScore = await this.evaluateShadowWallet(buyer.address);

                if (shadowScore >= this.config.minShadowScore) {
                    pending.shadowBuyers.push({
                        address: buyer.address,
                        score: shadowScore,
                        amount: buyer.amount,
                        timestamp: buyer.timestamp,
                        delay: timeSinceHunterBuy
                    });

                    existingBuyers.add(buyerAddr);

                    console.log(`[ShadowV2] 👤 检测到影子买家: ${buyer.address.slice(0, 8)} (延迟 ${(timeSinceHunterBuy / 1000).toFixed(0)}秒, 评分 ${shadowScore})`);
                }
            }

            this.tokenBuyers.set(tokenCA, existingBuyers);

            // 检查是否达到共识阈值
            if (pending.shadowBuyers.length >= this.config.minShadowBuyers) {
                const consensusType = pending.shadowBuyers.length >= this.config.consensusBoostThreshold
                    ? 'strong'
                    : 'normal';

                console.log(`[ShadowV2] ✅ 共识达成! ${pending.symbol} (${pending.shadowBuyers.length} 影子买家)`);

                this.emitConsensusSignal(pending, consensusType, 'threshold');
                toRemove.push(tokenCA);
            }
        }

        // 清理已处理的记录
        for (const ca of toRemove) {
            this.pendingConsensus.delete(ca);
            this.tokenBuyers.delete(ca);
        }
    }

    /**
     * 评估钱包是否是高质量影子钱包
     */
    async evaluateShadowWallet(walletAddress) {
        // 检查缓存
        if (this.shadowWallets.has(walletAddress)) {
            return this.shadowWallets.get(walletAddress).score;
        }

        // 简化评分逻辑 (实际可以扩展为调用 GMGN API 获取钱包数据)
        // 目前给新钱包一个基础分数，后续根据表现调整
        const baseScore = 50;

        this.shadowWallets.set(walletAddress, {
            address: walletAddress,
            score: baseScore,
            firstSeen: Date.now(),
            followCount: 0,
            successRate: null
        });

        this.stats.shadowWalletsTracked++;

        return baseScore;
    }

    // ═══════════════════════════════════════════════════════════
    // 信号发射
    // ═══════════════════════════════════════════════════════════

    /**
     * 发出共识信号
     */
    emitConsensusSignal(pending, consensusType, trigger) {
        const now = Date.now();

        // 计算共识强度
        const shadowCount = pending.shadowBuyers.length;
        const avgShadowScore = shadowCount > 0
            ? pending.shadowBuyers.reduce((sum, s) => sum + s.score, 0) / shadowCount
            : 0;
        const avgDelay = shadowCount > 0
            ? pending.shadowBuyers.reduce((sum, s) => sum + s.delay, 0) / shadowCount
            : 0;

        // 计算共识分数
        let consensusScore = 50;  // 基础分
        consensusScore += shadowCount * 10;  // 每个影子钱包 +10
        consensusScore += avgShadowScore * 0.3;  // 影子钱包质量
        consensusScore -= avgDelay / 10000;  // 延迟越长扣分
        consensusScore = Math.max(0, Math.min(100, consensusScore));

        // 更新统计
        this.stats.consensusDetected++;
        if (consensusType === 'strong') {
            this.stats.strongConsensus++;
        } else if (consensusType === 'weak') {
            this.stats.weakConsensus++;
        }

        const signal = {
            source: 'shadow_v2_consensus',
            type: 'shadow_consensus',
            chain: pending.chain,

            // 代币信息
            token_ca: pending.tokenCA,
            symbol: pending.symbol,
            market_cap: pending.marketCap,
            liquidity: pending.liquidity,
            price: pending.price,

            // 共识信息
            consensus: {
                type: consensusType,
                trigger: trigger,
                score: consensusScore,
                shadowCount: shadowCount,
                avgShadowScore: avgShadowScore,
                avgDelayMs: avgDelay,
                hunterCount: pending.hunters.length,
                hunters: pending.hunters.map(h => ({
                    address: h.address,
                    type: h.type,
                    score: h.score
                })),
                shadowBuyers: pending.shadowBuyers.map(s => ({
                    address: s.address.slice(0, 12) + '...',
                    score: s.score,
                    delayMs: s.delay
                }))
            },

            // 交易建议
            urgency: consensusType === 'strong' ? 'high' : 'normal',
            positionMultiplier: consensusType === 'strong'
                ? this.config.strongConsensusMultiplier
                : 1.0,

            timestamp: now,

            // v7.4 信号血统追踪 (Signal Lineage)
            signalLineage: {
                source: 'shadow_v2',
                hunterType: pending.hunters[0]?.type || null,
                hunterAddr: pending.hunters[0]?.address || null,
                hunterScore: consensusScore,
                route: 'cross_validator',
                entryReason: `shadow_consensus_${consensusType}`,
                confidence: 'direct'
            }
        };

        this.stats.signalsEmitted++;
        this.emit('signal', signal);

        console.log(`[ShadowV2] ════════════════════════════════════════════════════════`);
        console.log(`[ShadowV2] 📤 共识信号已发出!`);
        console.log(`   代币: ${pending.symbol} (${pending.tokenCA.slice(0, 12)}...)`);
        console.log(`   类型: ${consensusType.toUpperCase()} (${trigger})`);
        console.log(`   共识分数: ${consensusScore.toFixed(0)}`);
        console.log(`   猎人: ${pending.hunters.length} | 影子: ${shadowCount}`);
        console.log(`   平均延迟: ${(avgDelay / 1000).toFixed(1)}秒`);
        console.log(`[ShadowV2] ════════════════════════════════════════════════════════`);
    }

    // ═══════════════════════════════════════════════════════════
    // 启动/停止
    // ═══════════════════════════════════════════════════════════

    async start() {
        if (this.isRunning) {
            console.log('[ShadowV2] 已经在运行中');
            return;
        }

        console.log('[ShadowV2] 🚀 启动 Shadow Protocol v2.1 (Consensus Detector)...');

        this.isRunning = true;

        // 加载缓存
        this.loadCache();

        // 启动共识检查定时器
        this.consensusCheckTimer = setInterval(async () => {
            await this.checkConsensus();
        }, 10 * 1000);  // 每10秒检查一次

        // v7.4.1 启动内存清理定时器 (每30分钟清理过期数据)
        this.cleanupTimer = setInterval(() => {
            this.cleanupStaleData();
        }, 30 * 60 * 1000);

        console.log('[ShadowV2] ✅ Shadow Protocol v2.1 已启动');
        console.log(`[ShadowV2] - 共识检查: 每 10 秒`);
        console.log(`[ShadowV2] - 内存清理: 每 30 分钟`);
        console.log(`[ShadowV2] - 等待 UltraSniperV2 猎人信号...`);
    }

    /**
     * v7.4.1 清理过期数据，防止内存泄漏
     */
    cleanupStaleData() {
        const now = Date.now();
        const maxAge = 24 * 60 * 60 * 1000;  // 24小时

        // 清理过期的影子钱包数据
        let cleanedWallets = 0;
        for (const [address, profile] of this.shadowWallets.entries()) {
            if (now - profile.firstSeen > maxAge && profile.followCount < 3) {
                // 超过24小时且跟单次数少于3次的钱包，删除
                this.shadowWallets.delete(address);
                cleanedWallets++;
            }
        }

        if (cleanedWallets > 0) {
            console.log(`[ShadowV2] 🧹 清理了 ${cleanedWallets} 个过期影子钱包数据`);
        }
    }

    async stop() {
        console.log('[ShadowV2] ⏹️ 正在停止...');

        this.isRunning = false;

        if (this.consensusCheckTimer) {
            clearInterval(this.consensusCheckTimer);
            this.consensusCheckTimer = null;
        }

        // v7.4.1 清理内存清理定时器
        if (this.cleanupTimer) {
            clearInterval(this.cleanupTimer);
            this.cleanupTimer = null;
        }

        // 保存缓存
        this.saveCache();

        console.log('[ShadowV2] ⏹️ 已停止');
        console.log(`[ShadowV2] 📊 运行统计:`);
        console.log(`   - 猎人信号接收: ${this.stats.hunterBuysReceived}`);
        console.log(`   - 共识检测: ${this.stats.consensusDetected}`);
        console.log(`   - 强共识: ${this.stats.strongConsensus} | 弱共识: ${this.stats.weakConsensus}`);
        console.log(`   - 信号发出: ${this.stats.signalsEmitted}`);
        console.log(`   - 影子钱包追踪: ${this.stats.shadowWalletsTracked}`);
    }

    // ═══════════════════════════════════════════════════════════
    // 缓存管理
    // ═══════════════════════════════════════════════════════════

    saveCache() {
        try {
            const cacheData = {
                shadowWallets: Array.from(this.shadowWallets.entries()),
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

                // 恢复影子钱包数据
                if (data.shadowWallets) {
                    this.shadowWallets = new Map(data.shadowWallets);
                    console.log(`[ShadowV2] 📂 加载 ${this.shadowWallets.size} 个影子钱包数据`);
                }
            }
        } catch (e) {
            // 忽略
        }
    }

    // ═══════════════════════════════════════════════════════════
    // 状态查询
    // ═══════════════════════════════════════════════════════════

    getStatus() {
        return {
            isRunning: this.isRunning,
            pendingConsensus: this.pendingConsensus.size,
            shadowWalletsKnown: this.shadowWallets.size,
            pending: Array.from(this.pendingConsensus.values()).map(p => ({
                symbol: p.symbol,
                hunters: p.hunters.length,
                shadowBuyers: p.shadowBuyers.length,
                waitingMs: Date.now() - p.startTime
            })),
            stats: this.stats
        };
    }
}

export default ShadowProtocolV2;
