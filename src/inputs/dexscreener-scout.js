/**
 * DexScreener 信号源 - 真正免费无需 Cookie
 * 
 * 免费 API 端点:
 * - /token-boosts/latest/v1 - 最新付费推广代币
 * - /token-boosts/top/v1 - 热门付费推广代币
 * - /token-profiles/latest/v1 - 最新更新资料的代币
 * - /latest/dex/tokens/{address} - 代币详情
 * 
 * 完全免费，无 Cloudflare 限制！
 */

import axios from 'axios';
import { EventEmitter } from 'events';

export class DexScreenerScout extends EventEmitter {
    constructor(config = {}) {
        super();
        
        this.config = {
            baseUrl: 'https://api.dexscreener.com',
            
            // 轮询间隔（毫秒）
            pollInterval: config.pollInterval || 60000, // 1分钟
            
            // 支持的链
            chains: config.chains || ['solana', 'bsc'],
            
            // 启用的信号类型
            enabledSignals: config.enabledSignals || {
                boosts: true,      // 付费推广
                topBoosts: true,   // 热门付费
                profiles: true     // 新资料更新
            },
            
            // 最小流动性过滤
            minLiquidity: config.minLiquidity || 10000, // $10k
            
            userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        };
        
        this.isRunning = false;
        this.lastSeenTokens = new Map();
        this.pollTimer = null;
        
        console.log('[DexScreener] ✅ 免费信号源初始化完成 - 无需 Cookie！');
    }
    
    getHeaders() {
        return {
            'accept': 'application/json',
            'user-agent': this.config.userAgent
        };
    }
    
    // ==========================================
    // 1. 付费推广代币 (Boosts)
    // ==========================================
    async getLatestBoosts() {
        try {
            const response = await axios.get(`${this.config.baseUrl}/token-boosts/latest/v1`, {
                headers: this.getHeaders(),
                timeout: 15000
            });
            
            if (Array.isArray(response.data)) {
                // 过滤指定链
                return response.data.filter(t => 
                    this.config.chains.includes(t.chainId)
                );
            }
            return [];
        } catch (error) {
            console.error(`[DexScreener] Boosts API错误: ${error.message}`);
            return [];
        }
    }
    
    // ==========================================
    // 2. 热门付费推广 (Top Boosts)
    // ==========================================
    async getTopBoosts() {
        try {
            const response = await axios.get(`${this.config.baseUrl}/token-boosts/top/v1`, {
                headers: this.getHeaders(),
                timeout: 15000
            });
            
            if (Array.isArray(response.data)) {
                return response.data.filter(t => 
                    this.config.chains.includes(t.chainId)
                );
            }
            return [];
        } catch (error) {
            console.error(`[DexScreener] Top Boosts API错误: ${error.message}`);
            return [];
        }
    }
    
    // ==========================================
    // 3. 新资料更新 (Profiles)
    // ==========================================
    async getLatestProfiles() {
        try {
            const response = await axios.get(`${this.config.baseUrl}/token-profiles/latest/v1`, {
                headers: this.getHeaders(),
                timeout: 15000
            });
            
            if (Array.isArray(response.data)) {
                return response.data.filter(t => 
                    this.config.chains.includes(t.chainId)
                );
            }
            return [];
        } catch (error) {
            console.error(`[DexScreener] Profiles API错误: ${error.message}`);
            return [];
        }
    }
    
    // ==========================================
    // 4. 获取代币详情
    // ==========================================
    async getTokenDetails(tokenAddress) {
        try {
            const response = await axios.get(
                `${this.config.baseUrl}/latest/dex/tokens/${tokenAddress}`,
                {
                    headers: this.getHeaders(),
                    timeout: 10000
                }
            );
            
            if (response.data?.pairs?.length > 0) {
                const pair = response.data.pairs[0];
                return {
                    token_ca: tokenAddress,
                    chain: pair.chainId === 'solana' ? 'SOL' : pair.chainId.toUpperCase(),
                    symbol: pair.baseToken?.symbol || 'Unknown',
                    name: pair.baseToken?.name || 'Unknown',
                    price: parseFloat(pair.priceUsd) || 0,
                    liquidity: pair.liquidity?.usd || 0,
                    volume_24h: pair.volume?.h24 || 0,
                    price_change_5m: pair.priceChange?.m5 || 0,
                    price_change_1h: pair.priceChange?.h1 || 0,
                    price_change_24h: pair.priceChange?.h24 || 0,
                    txns_24h: (pair.txns?.h24?.buys || 0) + (pair.txns?.h24?.sells || 0),
                    market_cap: pair.marketCap || 0,
                    fdv: pair.fdv || 0,
                    pair_address: pair.pairAddress
                };
            }
            return null;
        } catch (error) {
            return null;
        }
    }
    
    // ==========================================
    // 5. 综合扫描
    // ==========================================
    async scanAll() {
        const signals = [];
        const { enabledSignals } = this.config;
        
        // 并行获取所有信号
        const [boosts, topBoosts, profiles] = await Promise.all([
            enabledSignals.boosts ? this.getLatestBoosts() : [],
            enabledSignals.topBoosts ? this.getTopBoosts() : [],
            enabledSignals.profiles ? this.getLatestProfiles() : []
        ]);
        
        console.log(`[DexScreener] 扫描: Boosts=${boosts.length}, TopBoosts=${topBoosts.length}, Profiles=${profiles.length}`);
        
        // 处理付费推广
        for (const token of boosts.slice(0, 10)) {
            const signal = await this.createSignal(token, 'boost', '💎');
            if (signal && this.isNewSignal(signal)) {
                signals.push(signal);
            }
        }
        
        // 处理热门付费
        for (const token of topBoosts.slice(0, 5)) {
            const signal = await this.createSignal(token, 'top_boost', '🔥');
            if (signal && this.isNewSignal(signal)) {
                signals.push(signal);
            }
        }
        
        // 处理新资料
        for (const token of profiles.slice(0, 5)) {
            const signal = await this.createSignal(token, 'profile', '📋');
            if (signal && this.isNewSignal(signal)) {
                signals.push(signal);
            }
        }
        
        return signals;
    }
    
    async createSignal(token, signalType, emoji) {
        const tokenCA = token.tokenAddress;
        if (!tokenCA) return null;
        
        // 获取代币详情
        const details = await this.getTokenDetails(tokenCA);
        
        // 过滤低流动性代币
        if (details && details.liquidity < this.config.minLiquidity) {
            return null;
        }
        
        const chainMap = {
            'solana': 'SOL',
            'bsc': 'BSC',
            'ethereum': 'ETH',
            'base': 'BASE'
        };
        
        return {
            token_ca: tokenCA,
            chain: chainMap[token.chainId] || token.chainId.toUpperCase(),
            symbol: details?.symbol || 'Unknown',
            name: details?.name || 'Unknown',
            signal_type: signalType,
            emoji: emoji,
            description: token.description?.substring(0, 100) || '',
            price: details?.price || 0,
            liquidity: details?.liquidity || 0,
            volume_24h: details?.volume_24h || 0,
            price_change_5m: details?.price_change_5m || 0,
            price_change_1h: details?.price_change_1h || 0,
            market_cap: details?.market_cap || 0,
            has_twitter: token.links?.some(l => l.type === 'twitter') || false,
            has_telegram: token.links?.some(l => l.type === 'telegram') || false,
            has_website: token.links?.some(l => l.url && !l.type) || false,
            source: `dexscreener_${signalType}`,
            timestamp: Date.now()
        };
    }
    
    isNewSignal(signal) {
        const cacheKey = `${signal.chain}:${signal.token_ca}:${signal.signal_type}`;
        if (this.lastSeenTokens.has(cacheKey)) {
            const lastSeen = this.lastSeenTokens.get(cacheKey);
            if (Date.now() - lastSeen < 60 * 60 * 1000) { // 1小时内不重复
                return false;
            }
        }
        this.lastSeenTokens.set(cacheKey, Date.now());
        return true;
    }
    
    // ==========================================
    // 启动/停止
    // ==========================================
    async start() {
        if (this.isRunning) {
            console.log('[DexScreener] 已经在运行中');
            return;
        }
        
        this.isRunning = true;
        console.log('[DexScreener] 🚀 启动信号监控...');
        
        // 立即执行一次
        await this.pollOnce();
        
        // 设置定时轮询
        this.pollTimer = setInterval(async () => {
            if (!this.isRunning) return;
            await this.pollOnce();
        }, this.config.pollInterval);
        
        console.log('[DexScreener] ✅ 信号监控已启动');
    }
    
    async pollOnce() {
        try {
            const signals = await this.scanAll();
            
            for (const signal of signals) {
                console.log(`[DexScreener] ${signal.emoji} ${signal.signal_type.toUpperCase()}: ${signal.symbol} (${signal.chain}) - $${signal.liquidity?.toFixed(0)} liq`);
                this.emit('signal', signal);
            }
            
        } catch (error) {
            console.error(`[DexScreener] 扫描错误:`, error.message);
        }
    }
    
    stop() {
        this.isRunning = false;
        
        if (this.pollTimer) {
            clearInterval(this.pollTimer);
            this.pollTimer = null;
        }
        
        console.log('[DexScreener] ⏹️ 信号监控已停止');
    }
    
    getStatus() {
        return {
            isRunning: this.isRunning,
            chains: this.config.chains,
            enabledSignals: this.config.enabledSignals,
            pollInterval: this.config.pollInterval,
            cachedTokens: this.lastSeenTokens.size
        };
    }
}

export default DexScreenerScout;
