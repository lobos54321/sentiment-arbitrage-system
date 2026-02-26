/**
 * API Cache Module
 * 
 * 缓存 Grok/LLM API 调用结果，减少重复请求和成本
 * 
 * 功能：
 * 1. LLM 分析结果缓存 (30 分钟)
 * 2. X 搜索结果缓存 (15 分钟)
 * 3. Alpha 源信息缓存 (1 小时)
 * 4. 调用统计和成本追踪
 */

class APICache {
    constructor() {
        // 缓存存储
        this.llmCache = new Map();        // tokenAddress -> { result, timestamp }
        this.xSearchCache = new Map();     // query -> { result, timestamp }
        this.alphaCache = new Map();       // accountHandle -> { result, timestamp }

        // 缓存 TTL (毫秒)
        this.ttl = {
            llm: 30 * 60 * 1000,        // LLM 分析: 30 分钟
            xSearch: 15 * 60 * 1000,    // X 搜索: 15 分钟
            alpha: 60 * 60 * 1000       // Alpha 源: 1 小时
        };

        // 调用统计
        this.stats = {
            llm: { calls: 0, cached: 0, saved: 0 },
            xSearch: { calls: 0, cached: 0, saved: 0 },
            alpha: { calls: 0, cached: 0, saved: 0 }
        };

        // 预估成本 (USD per call)
        this.costPerCall = {
            llm: 0.003,       // ~$0.003 per LLM call
            xSearch: 0.002,   // ~$0.002 per X search
            alpha: 0.002      // ~$0.002 per Alpha check
        };

        // 定期清理过期缓存
        setInterval(() => this.cleanup(), 5 * 60 * 1000); // 每 5 分钟清理
    }

    /**
     * 获取 LLM 分析缓存
     */
    getLLMCache(tokenAddress) {
        const key = tokenAddress.toLowerCase();
        const cached = this.llmCache.get(key);

        if (cached && Date.now() - cached.timestamp < this.ttl.llm) {
            this.stats.llm.cached++;
            this.stats.llm.saved += this.costPerCall.llm;
            console.log(`📦 [Cache] LLM 命中: ${tokenAddress.slice(0, 8)}...`);
            return cached.result;
        }
        return null;
    }

    /**
     * 设置 LLM 分析缓存
     */
    setLLMCache(tokenAddress, result) {
        const key = tokenAddress.toLowerCase();
        this.llmCache.set(key, {
            result,
            timestamp: Date.now()
        });
        this.stats.llm.calls++;
    }

    /**
     * 获取 X 搜索缓存
     */
    getXSearchCache(symbol, tokenAddress) {
        const key = `${symbol}:${tokenAddress}`.toLowerCase();
        const cached = this.xSearchCache.get(key);

        if (cached && Date.now() - cached.timestamp < this.ttl.xSearch) {
            this.stats.xSearch.cached++;
            this.stats.xSearch.saved += this.costPerCall.xSearch;
            console.log(`📦 [Cache] X搜索 命中: $${symbol}`);
            return cached.result;
        }
        return null;
    }

    /**
     * 设置 X 搜索缓存
     */
    setXSearchCache(symbol, tokenAddress, result) {
        const key = `${symbol}:${tokenAddress}`.toLowerCase();
        this.xSearchCache.set(key, {
            result,
            timestamp: Date.now()
        });
        this.stats.xSearch.calls++;
    }

    /**
     * 获取 Alpha 源缓存
     */
    getAlphaCache(accountHandle) {
        const key = accountHandle.toLowerCase();
        const cached = this.alphaCache.get(key);

        if (cached && Date.now() - cached.timestamp < this.ttl.alpha) {
            this.stats.alpha.cached++;
            this.stats.alpha.saved += this.costPerCall.alpha;
            return cached.result;
        }
        return null;
    }

    /**
     * 设置 Alpha 源缓存
     */
    setAlphaCache(accountHandle, result) {
        const key = accountHandle.toLowerCase();
        this.alphaCache.set(key, {
            result,
            timestamp: Date.now()
        });
        this.stats.alpha.calls++;
    }

    /**
     * 清理过期缓存
     */
    cleanup() {
        const now = Date.now();
        let cleaned = 0;

        // 清理 LLM 缓存
        for (const [key, value] of this.llmCache) {
            if (now - value.timestamp > this.ttl.llm) {
                this.llmCache.delete(key);
                cleaned++;
            }
        }

        // 清理 X 搜索缓存
        for (const [key, value] of this.xSearchCache) {
            if (now - value.timestamp > this.ttl.xSearch) {
                this.xSearchCache.delete(key);
                cleaned++;
            }
        }

        // 清理 Alpha 缓存
        for (const [key, value] of this.alphaCache) {
            if (now - value.timestamp > this.ttl.alpha) {
                this.alphaCache.delete(key);
                cleaned++;
            }
        }

        if (cleaned > 0) {
            console.log(`🧹 [Cache] 清理了 ${cleaned} 条过期缓存`);
        }
    }

    /**
     * 获取统计信息
     */
    getStats() {
        return {
            llm: {
                ...this.stats.llm,
                cacheSize: this.llmCache.size,
                hitRate: this.stats.llm.calls > 0
                    ? ((this.stats.llm.cached / (this.stats.llm.calls + this.stats.llm.cached)) * 100).toFixed(1) + '%'
                    : 'N/A'
            },
            xSearch: {
                ...this.stats.xSearch,
                cacheSize: this.xSearchCache.size,
                hitRate: this.stats.xSearch.calls > 0
                    ? ((this.stats.xSearch.cached / (this.stats.xSearch.calls + this.stats.xSearch.cached)) * 100).toFixed(1) + '%'
                    : 'N/A'
            },
            alpha: {
                ...this.stats.alpha,
                cacheSize: this.alphaCache.size,
                hitRate: this.stats.alpha.calls > 0
                    ? ((this.stats.alpha.cached / (this.stats.alpha.calls + this.stats.alpha.cached)) * 100).toFixed(1) + '%'
                    : 'N/A'
            },
            totalSaved: `$${(this.stats.llm.saved + this.stats.xSearch.saved + this.stats.alpha.saved).toFixed(3)}`
        };
    }

    /**
     * 打印统计信息
     */
    printStats() {
        const stats = this.getStats();
        console.log('\n📊 [API Cache] 统计信息:');
        console.log(`   LLM:      ${stats.llm.calls} 调用, ${stats.llm.cached} 缓存命中, 命中率 ${stats.llm.hitRate}`);
        console.log(`   X搜索:    ${stats.xSearch.calls} 调用, ${stats.xSearch.cached} 缓存命中, 命中率 ${stats.xSearch.hitRate}`);
        console.log(`   Alpha:    ${stats.alpha.calls} 调用, ${stats.alpha.cached} 缓存命中, 命中率 ${stats.alpha.hitRate}`);
        console.log(`   预估节省: ${stats.totalSaved}`);
    }

    /**
     * 重置统计
     */
    resetStats() {
        this.stats = {
            llm: { calls: 0, cached: 0, saved: 0 },
            xSearch: { calls: 0, cached: 0, saved: 0 },
            alpha: { calls: 0, cached: 0, saved: 0 }
        };
    }
}

// 导出全局单例
const apiCache = new APICache();
export { apiCache, APICache };
export default apiCache;
