/**
 * GMGN API Gateway v7.4
 *
 * 中心化的 GMGN API 网关，解决多模块同时调用导致的限流问题
 *
 * 核心功能:
 * - 单例模式：全局只有一个实例
 * - 令牌桶限流：控制请求频率
 * - 请求队列：排队等待处理
 * - 优先级支持：高优先级请求优先处理
 * - 会话管理：统一管理 GMGN Session
 *
 * 使用示例:
 * import { gmgnGateway } from '../utils/gmgn-api-gateway.js';
 * const data = await gmgnGateway.request('/defi/quotation/v1/rank/sol/wallets/7d', { priority: 'normal' });
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { fetchWithRetry } from './fetch-with-retry.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ═══════════════════════════════════════════════════════════════
// 配置
// ═══════════════════════════════════════════════════════════════

const GATEWAY_CONFIG = {
    // 令牌桶配置
    bucketCapacity: 10,          // 桶容量（最大并发令牌数）
    refillRate: 2,               // 每秒补充的令牌数
    refillInterval: 1000,        // 补充间隔 (ms)

    // 请求队列配置
    maxQueueSize: 100,           // 最大队列长度
    requestTimeout: 30000,       // 单个请求超时 (ms)
    queueTimeout: 60000,         // 队列等待超时 (ms)

    // 请求间隔
    minRequestInterval: 300,     // 最小请求间隔 (ms)

    // 优先级权重
    priorities: {
        critical: 3,   // 关键请求 (快速通道)
        high: 2,       // 高优先级 (信号触发)
        normal: 1,     // 普通 (轮询)
        low: 0         // 低优先级 (后台任务)
    },

    // GMGN API 配置
    baseUrl: 'https://gmgn.ai',
    deviceId: '1d29f750-687f-42e1-851d-59a43e5d2ffa',
    clientId: 'gmgn_web_v2',

    // Session 路径
    sessionPath: path.join(__dirname, '../../config/gmgn_session.json')
};

// ═══════════════════════════════════════════════════════════════
// GMGNApiGateway 类
// ═══════════════════════════════════════════════════════════════

class GMGNApiGateway {
    constructor() {
        // 令牌桶
        this.tokens = GATEWAY_CONFIG.bucketCapacity;
        this.lastRefill = Date.now();

        // 请求队列
        this.queue = [];
        this.isProcessing = false;

        // Session 数据
        this.sessionData = null;
        this.headers = null;

        // v7.4.1 熔断器 (Circuit Breaker)
        this.circuitBreaker = {
            failures: 0,           // 连续失败次数
            lastFailure: 0,        // 最后失败时间
            isOpen: false,         // 熔断状态
            threshold: 5,          // 连续失败阈值触发熔断
            cooldownMs: 60000,     // 熔断冷却时间 60秒
            halfOpenAttempts: 0,   // 半开状态尝试次数
            maxHalfOpenAttempts: 2 // 半开状态最大尝试次数
        };

        // 统计
        this.stats = {
            totalRequests: 0,
            successfulRequests: 0,
            failedRequests: 0,
            rateLimited: 0,
            queueOverflows: 0,
            avgWaitTime: 0,
            totalWaitTime: 0,
            circuitBreakerTrips: 0  // 熔断触发次数
        };

        // 最后请求时间（防止过快发送）
        this.lastRequestTime = 0;

        // 启动令牌补充定时器
        this.refillTimer = setInterval(() => this.refillTokens(), GATEWAY_CONFIG.refillInterval);

        // v7.4.2 定期持久化状态供 Dashboard 读取 (每30秒)
        this.statsPath = path.join(__dirname, '../../data/gmgn_gateway_stats.json');
        this.persistTimer = setInterval(() => this.persistStats(), 30000);

        console.log('[GMGNGateway] 🌐 GMGN API Gateway v7.4 初始化');
        console.log(`[GMGNGateway] 配置: 桶容量=${GATEWAY_CONFIG.bucketCapacity}, 补充速率=${GATEWAY_CONFIG.refillRate}/s`);
    }

    /**
     * v7.4.2 持久化统计数据供 Dashboard 读取
     */
    persistStats() {
        try {
            const statsData = {
                timestamp: new Date().toISOString(),
                circuitBreaker: this.circuitBreaker.isOpen,
                requestsToday: this.stats.totalRequests,
                successfulRequests: this.stats.successfulRequests,
                failedRequests: this.stats.failedRequests,
                rateLimited: this.stats.rateLimited,
                circuitBreakerTrips: this.stats.circuitBreakerTrips,
                queueLength: this.queue.length,
                availableTokens: this.tokens
            };
            fs.writeFileSync(this.statsPath, JSON.stringify(statsData, null, 2));
        } catch (e) {
            // 静默失败，不影响主流程
        }
    }

    /**
     * 加载 Session
     */
    loadSession() {
        if (this.sessionData) return true;

        try {
            if (!fs.existsSync(GATEWAY_CONFIG.sessionPath)) {
                console.error('[GMGNGateway] ❌ Session 文件不存在');
                return false;
            }

            this.sessionData = JSON.parse(fs.readFileSync(GATEWAY_CONFIG.sessionPath, 'utf8'));

            // 构建 headers
            const cookies = this.sessionData?.cookies || [];
            const cookieStr = cookies
                .filter(c => c.domain && c.domain.includes('gmgn'))
                .map(c => `${c.name}=${c.value}`)
                .join('; ');

            this.headers = {
                'Accept': 'application/json',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                'Cookie': cookieStr,
                'Origin': 'https://gmgn.ai',
                'Referer': 'https://gmgn.ai/'
            };

            console.log(`[GMGNGateway] ✅ Session 已加载 (${cookies.length} cookies)`);
            return true;

        } catch (error) {
            console.error(`[GMGNGateway] ❌ Session 加载失败: ${error.message}`);
            return false;
        }
    }

    /**
     * 补充令牌
     */
    refillTokens() {
        const now = Date.now();
        const elapsed = now - this.lastRefill;
        const tokensToAdd = Math.floor(elapsed / GATEWAY_CONFIG.refillInterval) * GATEWAY_CONFIG.refillRate;

        if (tokensToAdd > 0) {
            this.tokens = Math.min(this.tokens + tokensToAdd, GATEWAY_CONFIG.bucketCapacity);
            this.lastRefill = now;
        }
    }

    /**
     * 尝试获取令牌
     */
    tryAcquireToken() {
        this.refillTokens();

        if (this.tokens > 0) {
            this.tokens--;
            return true;
        }

        return false;
    }

    /**
     * 构建完整 URL
     */
    buildUrl(endpoint) {
        const params = new URLSearchParams({
            device_id: GATEWAY_CONFIG.deviceId,
            client_id: GATEWAY_CONFIG.clientId,
            from_app: 'gmgn',
            app_ver: '20260107',
            tz_name: 'Australia/Brisbane',
            app_lang: 'en',
            os: 'web'
        });

        const baseEndpoint = endpoint.startsWith('/') ? endpoint : `/${endpoint}`;
        const sep = baseEndpoint.includes('?') ? '&' : '?';
        return `${GATEWAY_CONFIG.baseUrl}${baseEndpoint}${sep}${params}`;
    }

    /**
     * 主请求方法
     * @param {string} endpoint - API 端点
     * @param {Object} options - 选项
     * @param {string} options.priority - 优先级 (critical/high/normal/low)
     * @param {string} options.source - 请求来源标识
     * @param {boolean} options.skipQueue - 跳过队列直接执行
     */
    async request(endpoint, options = {}) {
        const priority = options.priority || 'normal';
        const source = options.source || 'Unknown';
        const skipQueue = options.skipQueue || false;

        this.stats.totalRequests++;

        // 确保 Session 已加载
        if (!this.loadSession()) {
            return { error: 'Session not loaded' };
        }

        // 如果是跳过队列的请求，直接执行
        if (skipQueue) {
            return this.executeRequest(endpoint, source);
        }

        // 否则，加入队列
        return new Promise((resolve, reject) => {
            const queueItem = {
                endpoint,
                source,
                priority: GATEWAY_CONFIG.priorities[priority] || 1,
                resolve,
                reject,
                addedAt: Date.now(),
                timeout: setTimeout(() => {
                    // 移除超时的请求
                    const index = this.queue.indexOf(queueItem);
                    if (index > -1) {
                        this.queue.splice(index, 1);
                        resolve({ error: 'Queue timeout' });
                    }
                }, GATEWAY_CONFIG.queueTimeout)
            };

            // 检查队列大小
            if (this.queue.length >= GATEWAY_CONFIG.maxQueueSize) {
                this.stats.queueOverflows++;
                clearTimeout(queueItem.timeout);
                resolve({ error: 'Queue overflow' });
                return;
            }

            // 按优先级插入队列
            let inserted = false;
            for (let i = 0; i < this.queue.length; i++) {
                if (this.queue[i].priority < queueItem.priority) {
                    this.queue.splice(i, 0, queueItem);
                    inserted = true;
                    break;
                }
            }
            if (!inserted) {
                this.queue.push(queueItem);
            }

            // 触发队列处理
            this.processQueue();
        });
    }

    /**
     * 处理队列
     */
    async processQueue() {
        if (this.isProcessing || this.queue.length === 0) return;

        this.isProcessing = true;

        while (this.queue.length > 0) {
            // 等待令牌
            while (!this.tryAcquireToken()) {
                await this.sleep(100);
            }

            // 确保请求间隔
            const timeSinceLastRequest = Date.now() - this.lastRequestTime;
            if (timeSinceLastRequest < GATEWAY_CONFIG.minRequestInterval) {
                await this.sleep(GATEWAY_CONFIG.minRequestInterval - timeSinceLastRequest);
            }

            // 取出队列头部
            const item = this.queue.shift();
            if (!item) continue;

            clearTimeout(item.timeout);

            // 计算等待时间
            const waitTime = Date.now() - item.addedAt;
            this.stats.totalWaitTime += waitTime;
            this.stats.avgWaitTime = this.stats.totalWaitTime / this.stats.totalRequests;

            // 执行请求
            try {
                const result = await this.executeRequest(item.endpoint, item.source);
                item.resolve(result);
            } catch (error) {
                item.resolve({ error: error.message });
            }
        }

        this.isProcessing = false;
    }

    /**
     * 执行实际请求
     */
    async executeRequest(endpoint, source) {
        // v7.4.1 熔断器检查
        if (this.circuitBreaker.isOpen) {
            const timeSinceLastFailure = Date.now() - this.circuitBreaker.lastFailure;

            if (timeSinceLastFailure < this.circuitBreaker.cooldownMs) {
                // 熔断中，拒绝请求
                return { error: 'Circuit breaker open', rateLimited: true, circuitBreakerOpen: true };
            }

            // 冷却期结束，进入半开状态
            this.circuitBreaker.halfOpenAttempts++;
            console.log(`[GMGNGateway] 🔄 熔断器半开状态 (尝试 ${this.circuitBreaker.halfOpenAttempts}/${this.circuitBreaker.maxHalfOpenAttempts})`);

            if (this.circuitBreaker.halfOpenAttempts > this.circuitBreaker.maxHalfOpenAttempts) {
                // 重置熔断器
                this.circuitBreaker.isOpen = false;
                this.circuitBreaker.failures = 0;
                this.circuitBreaker.halfOpenAttempts = 0;
                console.log(`[GMGNGateway] ✅ 熔断器已关闭，恢复正常服务`);
            }
        }

        this.lastRequestTime = Date.now();

        const url = this.buildUrl(endpoint);

        try {
            const result = await fetchWithRetry(url, {
                headers: this.headers,
                source: `GMGN:${source}`,
                timeout: GATEWAY_CONFIG.requestTimeout,
                silent: true
            });

            if (result.error) {
                this.stats.failedRequests++;

                // 检查是否是限流
                if (result.status === 403 || result.status === 429) {
                    this.stats.rateLimited++;
                }

                // v7.4.1 更新熔断器失败计数
                this.recordFailure();
            } else {
                this.stats.successfulRequests++;

                // v7.4.1 成功请求重置失败计数
                this.recordSuccess();
            }

            return result;

        } catch (error) {
            this.stats.failedRequests++;

            // v7.4.1 更新熔断器失败计数
            this.recordFailure();

            return { error: error.message };
        }
    }

    /**
     * v7.4.1 记录成功请求
     */
    recordSuccess() {
        this.circuitBreaker.failures = 0;
        this.circuitBreaker.halfOpenAttempts = 0;

        // 如果之前是半开状态，现在关闭熔断器
        if (this.circuitBreaker.isOpen) {
            this.circuitBreaker.isOpen = false;
            console.log(`[GMGNGateway] ✅ 熔断器已关闭，服务恢复`);
        }
    }

    /**
     * v7.4.1 记录失败请求
     */
    recordFailure() {
        this.circuitBreaker.failures++;
        this.circuitBreaker.lastFailure = Date.now();

        // 检查是否需要触发熔断
        if (this.circuitBreaker.failures >= this.circuitBreaker.threshold && !this.circuitBreaker.isOpen) {
            this.circuitBreaker.isOpen = true;
            this.stats.circuitBreakerTrips++;
            console.log(`[GMGNGateway] 🔴 熔断器触发! 连续失败 ${this.circuitBreaker.failures} 次，暂停 ${this.circuitBreaker.cooldownMs / 1000} 秒`);
        }
    }

    /**
     * 批量请求 (优化多个请求)
     */
    async batchRequest(endpoints, options = {}) {
        const priority = options.priority || 'normal';
        const source = options.source || 'Batch';

        const results = [];

        for (const endpoint of endpoints) {
            const result = await this.request(endpoint, { priority, source });
            results.push(result);
        }

        return results;
    }

    /**
     * 获取统计信息
     */
    getStats() {
        return {
            ...this.stats,
            queueLength: this.queue.length,
            availableTokens: this.tokens,
            successRate: this.stats.totalRequests > 0
                ? ((this.stats.successfulRequests / this.stats.totalRequests) * 100).toFixed(1) + '%'
                : 'N/A',
            // v7.4.1 熔断器状态
            circuitBreaker: {
                isOpen: this.circuitBreaker.isOpen,
                failures: this.circuitBreaker.failures,
                trips: this.stats.circuitBreakerTrips
            }
        };
    }

    /**
     * 重置统计
     */
    resetStats() {
        this.stats = {
            totalRequests: 0,
            successfulRequests: 0,
            failedRequests: 0,
            rateLimited: 0,
            queueOverflows: 0,
            avgWaitTime: 0,
            totalWaitTime: 0
        };
    }

    /**
     * 停止网关
     */
    stop() {
        if (this.refillTimer) {
            clearInterval(this.refillTimer);
            this.refillTimer = null;
        }

        // v7.4.2 清除持久化定时器
        if (this.persistTimer) {
            clearInterval(this.persistTimer);
            this.persistTimer = null;
        }

        // 清理队列
        for (const item of this.queue) {
            clearTimeout(item.timeout);
            item.resolve({ error: 'Gateway stopped' });
        }
        this.queue = [];

        console.log('[GMGNGateway] ⏹️ Gateway 已停止');
        console.log(`[GMGNGateway] 统计: ${JSON.stringify(this.getStats())}`);
    }

    /**
     * 工具方法
     */
    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

// ═══════════════════════════════════════════════════════════════
// 单例实例
// ═══════════════════════════════════════════════════════════════

const gmgnGateway = new GMGNApiGateway();

// ═══════════════════════════════════════════════════════════════
// 便捷方法
// ═══════════════════════════════════════════════════════════════

/**
 * 获取牛人榜数据
 */
async function getLeaderboard(chain = 'sol', period = '7d', tag = 'smart_degen', limit = 100, options = {}) {
    return gmgnGateway.request(
        `/defi/quotation/v1/rank/${chain}/wallets/${period}?tag=${tag}&limit=${limit}`,
        { priority: 'normal', source: 'Leaderboard', ...options }
    );
}

/**
 * 获取钱包活动
 */
async function getWalletActivity(chain = 'sol', walletAddress, type = 'buy', limit = 5, options = {}) {
    return gmgnGateway.request(
        `/defi/quotation/v1/wallet_activity/${chain}?type=${type}&wallet=${walletAddress}&limit=${limit}`,
        { priority: 'high', source: 'WalletActivity', ...options }
    );
}

/**
 * 获取钱包持仓
 */
async function getWalletTokens(chain = 'sol', walletAddress, options = {}) {
    return gmgnGateway.request(
        `/defi/quotation/v1/wallet/${chain}/tokens/${walletAddress}`,
        { priority: 'normal', source: 'WalletTokens', ...options }
    );
}

/**
 * 获取代币信息
 */
async function getTokenInfo(chain = 'sol', tokenAddress, options = {}) {
    return gmgnGateway.request(
        `/defi/quotation/v1/tokens/${chain}/${tokenAddress}`,
        { priority: 'high', source: 'TokenInfo', ...options }
    );
}

// ═══════════════════════════════════════════════════════════════
// 导出
// ═══════════════════════════════════════════════════════════════

export {
    gmgnGateway,
    GMGNApiGateway,
    GATEWAY_CONFIG,
    getLeaderboard,
    getWalletActivity,
    getWalletTokens,
    getTokenInfo
};

export default gmgnGateway;
