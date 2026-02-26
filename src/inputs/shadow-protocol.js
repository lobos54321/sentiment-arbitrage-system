/**
 * Shadow Protocol v6.9
 *
 * 核心策略: 不监控 Twitter，监控那些监控 Twitter 的聪明钱包
 *
 * 原理:
 * - 顶级 Sniper 有自己的情报系统（监控 KOL、Telegram、Discord）
 * - 我们直接"借用"他们的情报 —— 当他们买入时，跟单
 * - 比自己建立情报系统便宜 100x，效果可能更好
 *
 * 数据源:
 * - Nansen Top 10 Memecoin Wallets 2025
 * - Kolscan Real-time KOL Tracker
 * - GMGN Smart Money Detection
 */

import axios from 'axios';
import { EventEmitter } from 'events';

export class ShadowProtocol extends EventEmitter {
    constructor(config = {}) {
        super();

        // 🔥 Tier S: 顶级狙击手 - 千万级盈利
        // 🟠 Tier A: 高手 - 百万级盈利
        // 🟢 Tier B: 优秀 - 近期活跃的实力派

        this.shadowWallets = config.shadowWallets || [
            // ===== TIER S: 顶级狙击手 =====
            {
                address: 'H2ikJvq8or5MyjvFowD7CDY6fG3Sc2yi4mxTnfovXy3K',
                name: 'shatter.sol',
                tier: 'S',
                stats: '1,053% ROI on TRUMP, $3M→$35M',
                weight: 3.0  // 权重最高
            },
            {
                address: '5CP6zv8a17mz91v6rMruVH6ziC5qAL8GFaJzwrX9Fvup',
                name: 'naseem',
                tier: 'S',
                stats: '$8M SHROOM, $3.9M ENRON, $1M HAWK',
                weight: 3.0
            },
            {
                address: 'EdCNh8EzETJLFphW8yvdY7rDd8zBiyweiz8DU5gUUUka',
                name: 'cifwifhatday.sol',
                tier: 'S',
                stats: '579% ROI on WIF, $6M→$23.4M',
                weight: 3.0
            },

            // ===== TIER A: 实力高手 =====
            {
                address: '4EtAJ1p8RjqccEVhEhaYnEgQ6kA4JHR8oYqyLFwARUj6',
                name: 'TRUMP Whale',
                tier: 'A',
                stats: '97% avg ROI, $260K ARC, $229K MELANIA',
                weight: 2.0
            },
            {
                address: '8zFZHuSRuDpuAR7J6FzwyF3vKNx4CVW3DFHJerQhc7Zd',
                name: 'traderpow',
                tier: 'A',
                stats: '75% ROI, $14.8M on TRUMP',
                weight: 2.0
            },
            {
                address: '8mZYBV8aPvPCo34CyCmt6fWkZRFviAUoBZr1Bn993gro',
                name: 'popchad.sol',
                tier: 'A',
                stats: '$7.24M total, 538% ROI on WIF',
                weight: 2.0
            },
            {
                address: '4DPxYoJ5DgjvXPUtZdT3CYUZ3EEbSPj4zMNEVFJTd1Ts',
                name: 'Sigil Fund',
                tier: 'A',
                stats: '$6.09M from 820 trades',
                weight: 2.0
            },

            // ===== TIER B: 活跃追踪 =====
            {
                address: '2h7s3FpSvc6v2oHke6Uqg191B5fPCeFTmMGnh5oPWhX7',
                name: 'tonka.sol',
                tier: 'B',
                stats: '196% ROI, $7.3M→$21.8M',
                weight: 1.5
            },
            {
                address: 'HWdeCUjBvPP1HJ5oCJt7aNsvMWpWoDgiejUWvfFX6T7R',
                name: 'HWdeC (Anonymous)',
                tier: 'B',
                stats: '$9.65M realized, 287% ROI FARTCOIN',
                weight: 1.5
            },
            {
                address: 'Hwz4BDgtDRDBTScpEKDawshdKatZJh6z1SJYmRUxTxKE',
                name: '0x5a8...a81',
                tier: 'B',
                stats: 'Insane gains on SPX and TRUMP',
                weight: 1.5
            },

            // ===== KOLSCAN 活跃钱包 (实时追踪) =====
            {
                address: 'GNrmKZCxYyNiSUsjduwwPJzhed3LATjciiKVuSGrsHEC',
                name: 'Giann (Kolscan)',
                tier: 'B',
                stats: 'Real-time active trader',
                weight: 1.0
            },
            {
                address: 'FpD6n8gfoZNxyAN6QqNH4TFQdV9vZEgcv5W4H2YL8k4X',
                name: 'Hesi (Kolscan)',
                tier: 'B',
                stats: 'Real-time active trader',
                weight: 1.0
            },
            {
                address: 'BTf4A2exGK9BCVDNzy65b9dUzXgMqB4weVkvTMFQsadd',
                name: 'Kev (Kolscan)',
                tier: 'B',
                stats: 'Real-time active trader',
                weight: 1.0
            }
        ];

        this.config = {
            // GMGN API
            gmgnBaseUrl: 'https://gmgn.ai/defi/quotation/v1',

            // Helius RPC (免费层)
            heliusRpc: config.heliusRpc || process.env.HELIUS_RPC || 'https://api.mainnet-beta.solana.com',

            // 轮询间隔
            pollInterval: config.pollInterval || 30000, // 30秒检查一次

            // 最小买入金额 (SOL) - 过滤小额测试交易
            minBuyAmount: config.minBuyAmount || 0.5,

            // 代币最大年龄 (小时) - 只关注新币
            maxTokenAge: config.maxTokenAge || 24,

            // 最大市值 (USD) - 早期入场
            maxMarketCap: config.maxMarketCap || 500000,

            // 最少影子钱包数量触发 (多个大佬买才跟)
            minShadowCount: config.minShadowCount || 1,

            // 用户代理
            userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        };

        this.isRunning = false;
        this.pollTimer = null;

        // 缓存: 已见过的代币（防止重复信号）
        this.seenTokens = new Map();

        // 缓存: 钱包上次交易
        this.walletLastTx = new Map();

        console.log(`[Shadow] 🥷 Shadow Protocol 初始化`);
        console.log(`[Shadow] 📊 追踪 ${this.shadowWallets.length} 个影子钱包`);
        console.log(`[Shadow]   - Tier S: ${this.shadowWallets.filter(w => w.tier === 'S').length} 个`);
        console.log(`[Shadow]   - Tier A: ${this.shadowWallets.filter(w => w.tier === 'A').length} 个`);
        console.log(`[Shadow]   - Tier B: ${this.shadowWallets.filter(w => w.tier === 'B').length} 个`);
    }

    /**
     * 获取钱包最近交易 (通过 GMGN)
     */
    async getWalletRecentTrades(walletAddress) {
        try {
            const url = `${this.config.gmgnBaseUrl}/wallet_activity/sol`;
            const response = await axios.get(url, {
                headers: {
                    'accept': 'application/json',
                    'user-agent': this.config.userAgent,
                    'origin': 'https://gmgn.ai',
                    'referer': 'https://gmgn.ai/'
                },
                params: {
                    type: 'buy',
                    wallet: walletAddress,
                    limit: 10
                },
                timeout: 10000
            });

            if (response.data?.data?.activities) {
                return response.data.data.activities;
            }
            return [];
        } catch (error) {
            // GMGN API 可能需要 cookie，尝试备用方案
            return this.getWalletTradesFallback(walletAddress);
        }
    }

    /**
     * 备用: 通过 GMGN 钱包页面 API
     */
    async getWalletTradesFallback(walletAddress) {
        try {
            const url = `${this.config.gmgnBaseUrl}/smartmoney/sol/walletNew/${walletAddress}`;
            const response = await axios.get(url, {
                headers: {
                    'accept': 'application/json',
                    'user-agent': this.config.userAgent
                },
                params: {
                    period: '1d',
                    orderby: 'last_active_timestamp',
                    direction: 'desc'
                },
                timeout: 10000
            });

            if (response.data?.data?.holdings) {
                // 转换格式
                return response.data.data.holdings.map(h => ({
                    token_address: h.token_address,
                    symbol: h.symbol,
                    amount_usd: h.usd_value,
                    timestamp: h.last_active_timestamp,
                    type: 'buy'
                }));
            }
            return [];
        } catch (error) {
            console.debug(`[Shadow] ${walletAddress.slice(0,8)} API fallback failed`);
            return [];
        }
    }

    /**
     * 扫描所有影子钱包
     */
    async scanShadowWallets() {
        const newBuys = new Map(); // token_ca -> { wallets: [], totalWeight: 0 }

        for (const wallet of this.shadowWallets) {
            try {
                const trades = await this.getWalletRecentTrades(wallet.address);

                for (const trade of trades) {
                    // 只关注买入
                    if (trade.type !== 'buy') continue;

                    const tokenCA = trade.token_address;
                    if (!tokenCA) continue;

                    // 检查是否已经发过这个信号
                    const cacheKey = `${wallet.address}:${tokenCA}`;
                    if (this.seenTokens.has(cacheKey)) continue;

                    // 检查买入金额
                    const buyAmount = parseFloat(trade.amount_usd || 0);
                    if (buyAmount < this.config.minBuyAmount * 150) continue; // 约 $75 USD

                    // 记录新买入
                    if (!newBuys.has(tokenCA)) {
                        newBuys.set(tokenCA, {
                            token_ca: tokenCA,
                            symbol: trade.symbol || 'Unknown',
                            wallets: [],
                            totalWeight: 0,
                            buyAmountUsd: 0
                        });
                    }

                    const buyInfo = newBuys.get(tokenCA);
                    buyInfo.wallets.push({
                        address: wallet.address,
                        name: wallet.name,
                        tier: wallet.tier,
                        weight: wallet.weight,
                        buyAmount: buyAmount
                    });
                    buyInfo.totalWeight += wallet.weight;
                    buyInfo.buyAmountUsd += buyAmount;

                    // 标记为已见
                    this.seenTokens.set(cacheKey, Date.now());
                }

                // 避免请求过快
                await this.sleep(500);

            } catch (error) {
                console.debug(`[Shadow] ${wallet.name} 扫描失败: ${error.message}`);
            }
        }

        return newBuys;
    }

    /**
     * 验证代币是否符合条件
     */
    async validateToken(tokenCA) {
        try {
            const url = `${this.config.gmgnBaseUrl}/tokens/sol/${tokenCA}`;
            const response = await axios.get(url, {
                headers: {
                    'accept': 'application/json',
                    'user-agent': this.config.userAgent
                },
                timeout: 10000
            });

            if (!response.data?.data) return null;

            const data = response.data.data;

            // 检查市值
            const marketCap = data.market_cap || 0;
            if (marketCap > this.config.maxMarketCap) {
                console.log(`[Shadow] ⏭️ ${data.symbol} 市值 $${(marketCap/1000).toFixed(0)}K > ${this.config.maxMarketCap/1000}K，跳过`);
                return null;
            }

            // 检查代币年龄
            const openTime = data.open_timestamp ? data.open_timestamp * 1000 : 0;
            const ageHours = (Date.now() - openTime) / (1000 * 60 * 60);
            if (ageHours > this.config.maxTokenAge) {
                console.log(`[Shadow] ⏭️ ${data.symbol} 已上线 ${ageHours.toFixed(1)}h > ${this.config.maxTokenAge}h，跳过`);
                return null;
            }

            // 检查是否是蜜罐
            if (data.is_honeypot) {
                console.log(`[Shadow] ⚠️ ${data.symbol} 检测到蜜罐，跳过`);
                return null;
            }

            return {
                token_ca: tokenCA,
                symbol: data.symbol,
                name: data.name,
                price: data.price,
                market_cap: marketCap,
                liquidity: data.liquidity,
                volume_24h: data.volume_24h,
                holder_count: data.holder_count,
                smart_money_count: data.smart_money_count || 0,
                open_timestamp: data.open_timestamp,
                age_hours: ageHours
            };
        } catch (error) {
            return null;
        }
    }

    /**
     * 生成 Shadow 信号
     */
    createShadowSignal(buyInfo, tokenData) {
        // 计算影子分数
        const tierSCount = buyInfo.wallets.filter(w => w.tier === 'S').length;
        const tierACount = buyInfo.wallets.filter(w => w.tier === 'A').length;
        const tierBCount = buyInfo.wallets.filter(w => w.tier === 'B').length;

        // 影子分数 = Tier S * 30 + Tier A * 20 + Tier B * 10
        const shadowScore = tierSCount * 30 + tierACount * 20 + tierBCount * 10;

        // 紧急度 (基于 Tier S 数量)
        let urgency = 'normal';
        if (tierSCount >= 2) urgency = 'critical';
        else if (tierSCount >= 1 || tierACount >= 2) urgency = 'high';

        const walletNames = buyInfo.wallets.map(w => `${w.name}[${w.tier}]`).join(', ');

        return {
            // 基本信息
            token_ca: buyInfo.token_ca,
            chain: 'SOL',
            symbol: tokenData?.symbol || buyInfo.symbol,
            name: tokenData?.name || buyInfo.symbol,

            // Shadow 专属数据
            signal_type: 'shadow_protocol',
            emoji: '🥷',
            shadow_score: shadowScore,
            shadow_wallets: buyInfo.wallets.length,
            shadow_weight: buyInfo.totalWeight,
            tier_breakdown: { S: tierSCount, A: tierACount, B: tierBCount },
            wallet_names: walletNames,
            total_buy_usd: buyInfo.buyAmountUsd,
            urgency: urgency,

            // 代币数据
            price: tokenData?.price || 0,
            market_cap: tokenData?.market_cap || 0,
            liquidity: tokenData?.liquidity || 0,
            holder_count: tokenData?.holder_count || 0,
            smart_money_count: tokenData?.smart_money_count || 0,
            age_hours: tokenData?.age_hours || 0,

            // 元数据
            source: 'shadow_protocol',
            timestamp: Date.now(),

            // 推荐仓位 (基于紧急度)
            recommended_position: urgency === 'critical' ? 0.15 :
                                  urgency === 'high' ? 0.10 : 0.05
        };
    }

    /**
     * 主扫描循环
     */
    async scan() {
        console.log(`[Shadow] 🔍 扫描 ${this.shadowWallets.length} 个影子钱包...`);

        try {
            const newBuys = await this.scanShadowWallets();

            if (newBuys.size === 0) {
                console.log(`[Shadow] 📭 本轮无新买入信号`);
                return [];
            }

            const signals = [];

            for (const [tokenCA, buyInfo] of newBuys) {
                // 检查是否达到最小影子钱包数量
                if (buyInfo.wallets.length < this.config.minShadowCount) {
                    continue;
                }

                // 验证代币
                const tokenData = await this.validateToken(tokenCA);

                // 即使验证失败，如果有 Tier S 买入，也发信号
                const hasTierS = buyInfo.wallets.some(w => w.tier === 'S');
                if (!tokenData && !hasTierS) {
                    continue;
                }

                // 创建信号
                const signal = this.createShadowSignal(buyInfo, tokenData);
                signals.push(signal);

                // 打印信号
                const tierStr = `S:${signal.tier_breakdown.S} A:${signal.tier_breakdown.A} B:${signal.tier_breakdown.B}`;
                console.log(`[Shadow] 🥷 ${signal.urgency.toUpperCase()}: ${signal.symbol}`);
                console.log(`         📊 Shadow Score: ${signal.shadow_score} | Tiers: ${tierStr}`);
                console.log(`         💰 总买入: $${signal.total_buy_usd.toFixed(0)} | 钱包: ${signal.wallet_names}`);
                console.log(`         📈 市值: $${(signal.market_cap/1000).toFixed(0)}K | 年龄: ${signal.age_hours.toFixed(1)}h`);

                // 发送事件
                this.emit('signal', signal);
            }

            return signals;

        } catch (error) {
            console.error(`[Shadow] 扫描错误: ${error.message}`);
            return [];
        }
    }

    /**
     * 启动 Shadow Protocol
     */
    async start() {
        if (this.isRunning) {
            console.log('[Shadow] 已经在运行中');
            return;
        }

        this.isRunning = true;
        console.log('[Shadow] 🥷 Shadow Protocol 启动...');

        // 立即执行一次扫描
        await this.scan();

        // 设置定时扫描
        this.pollTimer = setInterval(async () => {
            if (!this.isRunning) return;
            await this.scan();
        }, this.config.pollInterval);

        console.log(`[Shadow] ✅ 每 ${this.config.pollInterval/1000}s 扫描一次`);
    }

    /**
     * 停止 Shadow Protocol
     */
    async stop() {
        this.isRunning = false;

        if (this.pollTimer) {
            clearInterval(this.pollTimer);
            this.pollTimer = null;
        }

        console.log('[Shadow] ⏹️ Shadow Protocol 已停止');
    }

    /**
     * 添加新的影子钱包
     */
    addShadowWallet(wallet) {
        const exists = this.shadowWallets.find(w => w.address === wallet.address);
        if (exists) {
            console.log(`[Shadow] ⚠️ 钱包 ${wallet.name} 已存在`);
            return false;
        }

        this.shadowWallets.push(wallet);
        console.log(`[Shadow] ✅ 添加钱包: ${wallet.name} [${wallet.tier}]`);
        return true;
    }

    /**
     * 移除影子钱包
     */
    removeShadowWallet(address) {
        const index = this.shadowWallets.findIndex(w => w.address === address);
        if (index === -1) {
            console.log(`[Shadow] ⚠️ 钱包不存在`);
            return false;
        }

        const removed = this.shadowWallets.splice(index, 1)[0];
        console.log(`[Shadow] ✅ 移除钱包: ${removed.name}`);
        return true;
    }

    /**
     * 获取状态
     */
    getStatus() {
        return {
            isRunning: this.isRunning,
            walletCount: this.shadowWallets.length,
            tiers: {
                S: this.shadowWallets.filter(w => w.tier === 'S').length,
                A: this.shadowWallets.filter(w => w.tier === 'A').length,
                B: this.shadowWallets.filter(w => w.tier === 'B').length
            },
            pollInterval: this.config.pollInterval,
            minShadowCount: this.config.minShadowCount,
            maxMarketCap: this.config.maxMarketCap,
            seenTokens: this.seenTokens.size
        };
    }

    /**
     * 清理过期缓存
     */
    cleanupCache() {
        const now = Date.now();
        const maxAge = 6 * 60 * 60 * 1000; // 6小时

        for (const [key, timestamp] of this.seenTokens) {
            if (now - timestamp > maxAge) {
                this.seenTokens.delete(key);
            }
        }
    }

    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

export default ShadowProtocol;
