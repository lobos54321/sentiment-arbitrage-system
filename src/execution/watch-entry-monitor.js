import { EventEmitter } from 'events';
import fs from 'fs';
import path from 'path';

/**
 * WatchEntryMonitor - 自动盯盘买入模块
 * v1.0
 * 
 * 职责:
 * 1. 管理 WATCH_ENTRY 状态的待买入代币清单
 * 2. 周期性获取实时点位数据 (DexScreener/Birdeye)
 * 3. 价格回踩到目标位时，自动触发买入执行
 */
export class WatchEntryMonitor extends EventEmitter {
    constructor(config = {}, services = {}) {
        super();
        this.config = config;
        this.solService = services.sol;
        this.bscService = services.bsc;
        this.db = services.db;

        this.poolPath = path.join(process.cwd(), 'data', 'watch_entry_pool.json');
        this.tokens = new Map();
        this.triggeringTokens = new Set(); // 正在触发买入的代币，防止重复触发

        // 轮询间隔 (默认 60 秒)
        this.checkIntervalMs = config.watchIntervalMs || 60000;
        this.timer = null;

        this.loadPool();
        console.log(`🎯 [WatchEntry] Monitor initialized. Currently watching ${this.tokens.size} tokens.`);

        // 如果启动时池不为空，自动开始监控
        if (this.tokens.size > 0) {
            this.start();
        }
    }

    /**
     * 加载持久化的观察清单
     */
    loadPool() {
        try {
            if (fs.existsSync(this.poolPath)) {
                const data = JSON.parse(fs.readFileSync(this.poolPath, 'utf8'));
                if (Array.isArray(data)) {
                    data.forEach(item => {
                        this.tokens.set(item.address, item);
                    });
                }
            }
        } catch (error) {
            console.error('❌ [WatchEntry] Failed to load pool:', error.message);
        }
    }

    /**
     * 持久化观察清单
     */
    savePool() {
        try {
            const data = Array.from(this.tokens.values());
            fs.writeFileSync(this.poolPath, JSON.stringify(data, null, 2));
        } catch (error) {
            console.error('❌ [WatchEntry] Failed to save pool:', error.message);
        }
    }

    /**
     * 添加代币到观察队列
     * @param {Object} token - 代币信息
     * @param {number} targetPrice - AI 建议的目标价位
     */
    addToken(token, targetPrice) {
        if (!token?.address || !targetPrice) return;

        const record = {
            address: token.address,
            symbol: token.symbol,
            chain: token.chain,
            targetPrice: parseFloat(targetPrice),
            entryTimestamp: Date.now(),
            intentionTier: token.intentionTier,
            targetMarketCap: token.targetMarketCap,
            initialPrice: token.initial?.price || token.current?.price || 0,
            token: token // 原始数据备份
        };

        this.tokens.set(token.address, record);
        this.savePool();

        console.log(`✅ [WatchEntry] Added ${token.symbol} to monitor (Target: $${targetPrice})`);

        // 如果是第一个代币，启动定时器
        if (this.tokens.size === 1 && !this.timer) {
            this.start();
        }
    }

    /**
     * 移除代币
     */
    removeToken(address) {
        if (this.tokens.has(address)) {
            this.tokens.delete(address);
            this.savePool();
            console.log(`🗑️ [WatchEntry] Removed ${address} from monitor.`);
        }
    }

    /**
     * 启动监控
     */
    start() {
        if (this.timer) return;

        console.log(`🚀 [WatchEntry] Starting price monitoring... (Interval: ${this.checkIntervalMs}ms)`);
        this.timer = setInterval(() => this.checkPrices(), this.checkIntervalMs);

        // 立即执行一次初始检查
        this.checkPrices();
    }

    /**
     * 停止监控
     */
    stop() {
        if (this.timer) {
            clearInterval(this.timer);
            this.timer = null;
        }
    }

    /**
     * 核心逻辑: 检查价格是否跌破或触及目标位
     */
    async checkPrices() {
        if (this.tokens.size === 0) {
            this.stop();
            return;
        }

        console.log(`🔍 [WatchEntry] Checking prices for ${this.tokens.size} tokens...`);

        for (const [address, record] of this.tokens.entries()) {
            try {
                const service = record.chain === 'SOL' ? this.solService : this.bscService;
                if (!service) continue;

                // 获取实时快照
                const snapshot = await service.getSnapshot(address);
                const currentPrice = snapshot?.current_price;

                if (!currentPrice) {
                    console.log(`   ⚠️ [WatchEntry] Could not get price for ${record.symbol}`);
                    continue;
                }

                const targetPrice = record.targetPrice;
                const dropPercent = ((record.initialPrice - currentPrice) / record.initialPrice * 100).toFixed(1);

                console.log(`   🔸 [${record.symbol}] Current: $${currentPrice.toFixed(8)} | Target: $${targetPrice.toFixed(8)} (Drop: ${dropPercent}%)`);

                // 🎯 价格触发买入条件 (当前价 <= 目标价)
                if (currentPrice <= targetPrice) {
                    this.triggerBuy(record, snapshot);
                }
            } catch (error) {
                console.error(`❌ [WatchEntry] Check error for ${record.symbol}:`, error.message);
            }
        }
    }

    /**
     * 触发买入信号
     */
    triggerBuy(record, snapshot) {
        // 防止重复触发（在买入确认前不允许再次触发同一代币）
        if (this.triggeringTokens.has(record.address)) {
            console.log(`⏭️  [WatchEntry] ${record.symbol} 正在触发买入，跳过重复触发`);
            return;
        }
        this.triggeringTokens.add(record.address);

        console.log(`🎯 [WatchEntry] TRIGGERED! ${record.symbol} reached target price $${record.targetPrice}`);

        // 发出 trigger 事件，供 index.js 捕获并推送到交易流程
        // 买入成功后，调用方应调用 removeToken(address) 从监控池移除
        this.emit('trigger', {
            token: {
                ...record.token,
                current: snapshot,
                isFromWatchMonitor: true,
                intentionTier: record.intentionTier, // 显式传递建议等级
                targetPrice: record.targetPrice,
                entryReason: `WatchEntry reached target price: $${record.targetPrice}`
            },
            snapshot: snapshot
        });

        // 不立即删除 token — 由调用方在买入成功后调用 confirmTrigger(address) 移除
        // triggeringTokens 防重入保护会阻止重复触发，直到 confirmTrigger 或 cancelTrigger 被调用
    }

    /**
     * 买入成功后确认触发 — 移除 token 并清理防重入标记
     */
    confirmTrigger(address) {
        this.removeToken(address);
        this.triggeringTokens.delete(address);
    }

    /**
     * 买入失败后取消触发 — 保留 token 继续监控，清理防重入标记
     */
    cancelTrigger(address) {
        this.triggeringTokens.delete(address);
        console.log(`🔄 [WatchEntry] ${address} 买入失败，恢复监控`);
    }
}

export default WatchEntryMonitor;
