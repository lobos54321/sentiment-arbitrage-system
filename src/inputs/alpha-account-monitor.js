/**
 * Alpha Account Monitor v1.0
 * 
 * 主动监控高胜率交易员和 Alpha 信息源的推文
 * 发现他们提到的代币地址，作为早期信号注入系统
 * 
 * 核心策略：
 * - 跟踪高手的信息来源（而不是等 DeBot 发现）
 * - 给来自可信源的信号加入场时机加成
 * - 绕过正常发现流程，直接发送到 CrossValidator
 * 
 * 监控层级：
 * - Tier 1 (每 5 分钟): @lookonchain, @spotonchain - 鲸鱼追踪
 * - Tier 2 (每 10 分钟): @Ansem, @MustStopMurad - 顶级 KOL
 * - Tier 3 (每 15 分钟): 其他 Alpha 源
 */

import { EventEmitter } from 'events';
import GrokTwitterClient from '../social/grok-twitter-client.js';
import apiCache from '../utils/api-cache.js';

export class AlphaAccountMonitor extends EventEmitter {
    constructor(config = {}) {
        super();

        this.grokClient = new GrokTwitterClient();
        this.isRunning = false;
        this.intervalIds = [];

        // 已发现的代币缓存（避免重复发送）
        this.discoveredTokens = new Map(); // tokenAddress -> timestamp
        this.tokenCacheTTL = 30 * 60 * 1000; // 30 分钟后可以再次发送

        // 监控账号配置 (分层) - 🔥 降低频率节省 API 成本
        this.monitorTiers = {
            // Tier 1: 鲸鱼追踪 (最优先，每 15 分钟) - 原 5 分钟
            tier1: {
                accounts: [
                    { handle: 'lookonchain', name: 'Lookonchain', type: 'onchain', weight: 10 },
                    { handle: 'spotonchain', name: 'Spot On Chain', type: 'onchain', weight: 9 },
                ],
                intervalMs: 15 * 60 * 1000, // 🔥 15 分钟 (省 API)
                entryBonus: 12,  // 入场时机加成
            },

            // Tier 2: 顶级 KOL (每 30 分钟) - 原 10 分钟
            tier2: {
                accounts: [
                    { handle: 'blknoiz06', name: 'Ansem', type: 'kol', weight: 9.5 },
                    { handle: 'MustStopMurad', name: 'Murad', type: 'kol', weight: 9 },
                    { handle: 'aaalyonbtc', name: 'Aaalyonbtc', type: 'alpha_trader', weight: 8.5 },
                ],
                intervalMs: 30 * 60 * 1000, // 🔥 30 分钟 (省 API)
                entryBonus: 8,
            },

            // Tier 3: 其他 Alpha 源 (每 60 分钟) - 原 15 分钟
            tier3: {
                accounts: [
                    { handle: 'zachxbt', name: 'ZachXBT', type: 'analyst', weight: 9 },
                    { handle: 'theunipcs', name: 'The Unipcs', type: 'kol', weight: 8.5 },
                    { handle: 'wizardofsoho', name: 'Wizard of Soho', type: 'kol', weight: 8 },
                    { handle: 'defi_mochi', name: 'Defi Mochi', type: 'kol', weight: 8 },
                ],
                intervalMs: 60 * 60 * 1000, // 🔥 60 分钟 (省 API)
                entryBonus: 5,
            }
        };

        // 配置覆盖
        this.config = {
            enabled: config.enabled !== false,
            maxRequestsPerMinute: config.maxRequestsPerMinute || 6,
            lookbackMinutes: config.lookbackMinutes || 15, // 查看最近 15 分钟的推文
            ...config
        };

        // 请求限制
        this.lastRequestTime = 0;
        this.minRequestInterval = 60000 / this.config.maxRequestsPerMinute;
    }

    /**
     * 启动监控
     */
    async start() {
        if (!this.config.enabled) {
            console.log(`⏸️ [Alpha Monitor] 已禁用`);
            return;
        }

        if (this.isRunning) {
            console.log(`⚠️ [Alpha Monitor] 已在运行中`);
            return;
        }

        this.isRunning = true;
        console.log(`🔍 [Alpha Monitor] 启动中...`);

        // 统计账号数量
        const totalAccounts = Object.values(this.monitorTiers)
            .reduce((sum, tier) => sum + tier.accounts.length, 0);
        console.log(`   监控 ${totalAccounts} 个 Alpha 账号`);

        // 为每个层级设置定时器
        for (const [tierName, tier] of Object.entries(this.monitorTiers)) {
            console.log(`   ${tierName}: ${tier.accounts.length} 个账号, 每 ${tier.intervalMs / 60000} 分钟检查`);

            // 立即执行一次
            this.checkTier(tierName, tier);

            // 设置定时器
            const id = setInterval(() => {
                this.checkTier(tierName, tier);
            }, tier.intervalMs);

            this.intervalIds.push(id);
        }

        console.log(`✅ [Alpha Monitor] 已启动`);
    }

    /**
     * 更新账号层级
     */
    updateAccountTier(handle, newTierName) {
        if (!this.monitorTiers[newTierName]) {
            console.error(`❌ [Alpha Monitor] 无效的层级: ${newTierName}`);
            return false;
        }

        let found = false;
        let oldTierName = null;
        let accountData = null;

        // 查找账号当前所在层级
        for (const [name, tier] of Object.entries(this.monitorTiers)) {
            const index = tier.accounts.findIndex(a => a.handle === handle);
            if (index !== -1) {
                oldTierName = name;
                accountData = tier.accounts.splice(index, 1)[0];
                found = true;
                break;
            }
        }

        if (!found) {
            console.error(`❌ [Alpha Monitor] 未找到账号: @${handle}`);
            return false;
        }

        // 添加到新层级
        this.monitorTiers[newTierName].accounts.push(accountData);
        console.log(`✅ [Alpha Monitor] @${handle} 已从 ${oldTierName} 移动到 ${newTierName}`);

        return true;
    }

    /**
     * 停止监控
     */
    stop() {
        this.isRunning = false;

        for (const id of this.intervalIds) {
            clearInterval(id);
        }
        this.intervalIds = [];

        console.log(`⏹️ [Alpha Monitor] 已停止`);
    }

    /**
     * 检查某个层级的账号
     */
    async checkTier(tierName, tier) {
        if (!this.isRunning) return;

        for (const account of tier.accounts) {
            try {
                await this.checkAccount(account, tier.entryBonus, tierName);
            } catch (error) {
                console.error(`❌ [Alpha Monitor] 检查 @${account.handle} 失败:`, error.message);
            }

            // 请求间隔控制
            await this.throttle();
        }
    }

    /**
     * 检查单个账号
     */
    async checkAccount(account, entryBonus, tierName) {
        const prompt = `
Search for the latest tweets from @${account.handle} in the last ${this.config.lookbackMinutes} minutes.

Focus on tweets that mention:
1. Solana token contract addresses (base58, ~44 characters)
2. BSC/ETH token addresses (0x... format)
3. Token tickers with $ prefix (e.g., $PEPE, $WIF)
4. Pump.fun or Four.meme launches

For each token found, extract:
- Contract address (if available)
- Token symbol
- Chain (SOL/BSC/ETH)
- Tweet summary
- Timestamp

Return JSON:
{
  "account": "@${account.handle}",
  "checked_at": "<ISO timestamp>",
  "tokens_found": [
    {
      "contract_address": "<address or null>",
      "symbol": "<token symbol>",
      "chain": "SOL/BSC/ETH",
      "tweet_summary": "<brief summary>",
      "tweet_time": "<ISO timestamp or relative time>"
    }
  ],
  "total_tweets_scanned": <number>
}

Return ONLY JSON. If no tokens found, return empty array for tokens_found.
`;

        try {
            // 🔥 检查缓存
            const cached = apiCache.getAlphaCache(account.handle);
            if (cached) {
                console.log(`📦 [Alpha Monitor] 缓存命中: @${account.handle}`);
                // 处理缓存的代币
                if (cached.tokens_found && cached.tokens_found.length > 0) {
                    for (const token of cached.tokens_found) {
                        await this.processDiscoveredToken(token, account, entryBonus, tierName);
                    }
                }
                return;
            }

            const result = await this.grokClient._callGrokAPI(prompt);
            let data;

            try {
                let content = result.choices[0].message.content;

                // 提取 JSON
                const jsonBlockMatch = content.match(/```json\n?([\s\S]*?)\n?```/);
                if (jsonBlockMatch) {
                    content = jsonBlockMatch[1];
                } else {
                    const jsonMatch = content.match(/\{[\s\S]*\}/);
                    if (jsonMatch) {
                        content = jsonMatch[0];
                    }
                }

                data = JSON.parse(content);
            } catch (parseError) {
                // 解析失败，跳过
                return;
            }

            // 🔥 缓存结果
            apiCache.setAlphaCache(account.handle, data);

            // 处理发现的代币
            if (data.tokens_found && data.tokens_found.length > 0) {
                console.log(`🎯 [Alpha Monitor] @${account.handle} 发现 ${data.tokens_found.length} 个代币`);

                for (const token of data.tokens_found) {
                    await this.processDiscoveredToken(token, account, entryBonus, tierName);
                }
            }

        } catch (error) {
            // Grok API 错误，静默处理
        }
    }

    /**
     * 处理发现的代币
     */
    async processDiscoveredToken(token, account, entryBonus, tierName) {
        // 需要有合约地址
        if (!token.contract_address) {
            return;
        }

        const address = token.contract_address;

        // 检查是否最近已发送过
        if (this.discoveredTokens.has(address)) {
            const lastSeen = this.discoveredTokens.get(address);
            if (Date.now() - lastSeen < this.tokenCacheTTL) {
                return; // 跳过重复
            }
        }

        // 记录发现
        this.discoveredTokens.set(address, Date.now());

        // 确定链
        let chain = 'SOL';
        if (address.startsWith('0x')) {
            chain = token.chain?.toUpperCase() || 'BSC';
        }

        // 🔥 确定快速通道类型
        let fastTrackType = null;
        if (tierName === 'tier1') {
            fastTrackType = 'DIRECT_BUY';      // Tier 1: 直接买入
        } else if (tierName === 'tier2') {
            fastTrackType = 'REDUCED_THRESHOLD'; // Tier 2: 降低门槛
        } else {
            fastTrackType = 'NORMAL_WITH_BONUS'; // Tier 3: 正常+加成
        }

        // 发送信号
        const alphaSignal = {
            type: 'ALPHA_SIGNAL',
            source: `alpha:@${account.handle}`,
            tokenAddress: address,
            symbol: token.symbol || 'UNKNOWN',
            chain: chain,
            tier: tierName,                    // 🔥 新增: tier1/tier2/tier3
            fastTrackType: fastTrackType,      // 🔥 新增: 快速通道类型
            accountInfo: {
                handle: account.handle,
                name: account.name,
                type: account.type,
                weight: account.weight
            },
            entryBonus: entryBonus,
            tweetSummary: token.tweet_summary,
            discoveredAt: new Date().toISOString()
        };

        const tierEmoji = tierName === 'tier1' ? '🚀🚀🚀' : tierName === 'tier2' ? '🚀🚀' : '🚀';
        console.log(`${tierEmoji} [Alpha Signal] @${account.handle} → $${token.symbol} (${chain})`);
        console.log(`   地址: ${address.slice(0, 12)}...`);
        console.log(`   层级: ${tierName.toUpperCase()} | 通道: ${fastTrackType}`);

        // 发射事件
        this.emit('alpha-signal', alphaSignal);
    }

    /**
     * 请求节流
     */
    async throttle() {
        const now = Date.now();
        const elapsed = now - this.lastRequestTime;

        if (elapsed < this.minRequestInterval) {
            await new Promise(resolve =>
                setTimeout(resolve, this.minRequestInterval - elapsed)
            );
        }

        this.lastRequestTime = Date.now();
    }

    /**
     * 清理过期缓存
     */
    cleanupCache() {
        const now = Date.now();
        for (const [address, timestamp] of this.discoveredTokens) {
            if (now - timestamp > this.tokenCacheTTL) {
                this.discoveredTokens.delete(address);
            }
        }
    }

    /**
     * 获取监控状态
     */
    getStatus() {
        return {
            isRunning: this.isRunning,
            discoveredTokensCount: this.discoveredTokens.size,
            tiers: Object.entries(this.monitorTiers).map(([name, tier]) => ({
                name,
                accountCount: tier.accounts.length,
                intervalMinutes: tier.intervalMs / 60000,
                entryBonus: tier.entryBonus
            }))
        };
    }
}

export default AlphaAccountMonitor;
