/**
 * Flash Scout v6.9
 *
 * 核心理念: 早期入场 > 高分确认
 *
 * 数据分析结论:
 * - 高分 (80+) = 接盘 (-10.6% 亏损)
 * - 早期 (SM=1-2) = 最佳入场点
 *
 * Flash Scout 策略:
 * - 当检测到 SM=1-2 的新币 (< 30分钟)
 * - 只检查 Hard Gate (安全性)
 * - 立即以小仓位买入
 * - 后台继续验证，通过则加仓
 *
 * 速度: 3-5秒 vs 原流程 5-8分钟
 */

import { EventEmitter } from 'events';

export class FlashScout extends EventEmitter {
    constructor(config = {}) {
        super();

        this.config = {
            // Flash 触发条件
            maxTokenAge: config.maxTokenAge || 30,           // 最大代币年龄 (分钟)
            minSmartMoney: config.minSmartMoney || 1,        // 最少聪明钱数量
            maxSmartMoney: config.maxSmartMoney || 3,        // 最大聪明钱 (>3 可能已经晚了)
            maxMarketCap: config.maxMarketCap || 100000,     // 最大市值 $100K
            minLiquidity: config.minLiquidity || 5000,       // 最低流动性 $5K

            // Flash 仓位
            flashPositionSOL: config.flashPositionSOL || 0.05,  // SOL 试探仓
            flashPositionBNB: config.flashPositionBNB || 0.015, // BNB 试探仓

            // 加仓条件 (后台验证通过)
            addPositionMultiplier: config.addPositionMultiplier || 2, // 加仓倍数

            // 冷却时间 (同一代币)
            cooldownMs: config.cooldownMs || 5 * 60 * 1000,  // 5分钟

            // 启用的链
            enabledChains: config.enabledChains || ['SOL', 'BSC']
        };

        // 缓存: 已处理的代币
        this.processedTokens = new Map();

        // 缓存: Flash 买入记录 (用于后续加仓判断)
        this.flashBuys = new Map();

        // 统计
        this.stats = {
            totalSignals: 0,
            flashTriggers: 0,
            hardGatePassed: 0,
            hardGateRejected: 0,
            executed: 0
        };

        // 依赖注入
        this.hardGateFilter = null;
        this.executor = null;
        this.crossValidator = null;  // v7.4.1 添加去重支持

        console.log('[FlashScout] ⚡ Flash Scout v6.9 初始化');
        console.log(`[FlashScout] 触发条件: Age<${this.config.maxTokenAge}min, SM=${this.config.minSmartMoney}-${this.config.maxSmartMoney}, MCAP<$${this.config.maxMarketCap/1000}K`);
    }

    /**
     * 注入依赖
     */
    bindServices(hardGateFilter, executor, crossValidator = null) {
        this.hardGateFilter = hardGateFilter;
        this.executor = executor;
        this.crossValidator = crossValidator;  // v7.4.1
        console.log('[FlashScout] ✅ 服务绑定完成');
    }

    /**
     * 检查是否满足 Flash 条件
     */
    isFlashCandidate(signal) {
        // 检查链
        const chain = (signal.chain || 'SOL').toUpperCase();
        if (!this.config.enabledChains.includes(chain)) {
            return { eligible: false, reason: `链 ${chain} 未启用` };
        }

        // 检查代币年龄
        let ageMinutes = null;
        if (signal.open_timestamp) {
            ageMinutes = (Date.now() - signal.open_timestamp * 1000) / (1000 * 60);
        } else if (signal.age_hours !== undefined) {
            ageMinutes = signal.age_hours * 60;
        } else if (signal.created_at) {
            ageMinutes = (Date.now() - new Date(signal.created_at).getTime()) / (1000 * 60);
        }

        if (ageMinutes === null) {
            return { eligible: false, reason: '无法确定代币年龄' };
        }

        if (ageMinutes > this.config.maxTokenAge) {
            return { eligible: false, reason: `代币年龄 ${ageMinutes.toFixed(0)}min > ${this.config.maxTokenAge}min` };
        }

        // 检查聪明钱数量
        const smCount = signal.smart_money_count ||
                        signal.smartMoneyCount ||
                        signal.smart_wallet_online ||
                        signal.smartWalletOnline || 0;

        if (smCount < this.config.minSmartMoney) {
            return { eligible: false, reason: `SM=${smCount} < ${this.config.minSmartMoney}` };
        }

        if (smCount > this.config.maxSmartMoney) {
            return { eligible: false, reason: `SM=${smCount} > ${this.config.maxSmartMoney} (可能已晚)` };
        }

        // 检查市值
        const marketCap = signal.market_cap || signal.marketCap || 0;
        if (marketCap > this.config.maxMarketCap) {
            return { eligible: false, reason: `市值 $${(marketCap/1000).toFixed(0)}K > $${this.config.maxMarketCap/1000}K` };
        }

        // 检查流动性
        const liquidity = signal.liquidity || 0;
        if (liquidity < this.config.minLiquidity) {
            return { eligible: false, reason: `流动性 $${liquidity.toFixed(0)} < $${this.config.minLiquidity}` };
        }

        // 检查冷却
        const tokenCA = signal.token_ca || signal.tokenAddress;
        if (this.processedTokens.has(tokenCA)) {
            const lastTime = this.processedTokens.get(tokenCA);
            if (Date.now() - lastTime < this.config.cooldownMs) {
                return { eligible: false, reason: '冷却中' };
            }
        }

        return {
            eligible: true,
            reason: `✅ Flash 候选: Age=${ageMinutes.toFixed(0)}min, SM=${smCount}, MCAP=$${(marketCap/1000).toFixed(0)}K`,
            data: { ageMinutes, smCount, marketCap, liquidity }
        };
    }

    /**
     * 快速 Hard Gate 检查 (只检查关键安全项)
     */
    async quickHardGateCheck(signal, chainSnapshot) {
        if (!this.hardGateFilter) {
            console.warn('[FlashScout] ⚠️ Hard Gate Filter 未绑定');
            return { pass: false, reason: 'Hard Gate 未配置' };
        }

        try {
            // 构造 snapshot 数据
            const snapshot = chainSnapshot || {
                token_ca: signal.token_ca || signal.tokenAddress,
                chain: (signal.chain || 'SOL').toUpperCase(),
                // SOL 安全检查
                freeze_authority: signal.freeze_authority || 'Unknown',
                mint_authority: signal.mint_authority || 'Unknown',
                lp_status: signal.lp_status || 'Unknown',
                top1_percent: signal.top1_percent || signal.top10Percent / 10 || null,
                // BSC 安全检查
                honeypot: signal.honeypot || signal.is_honeypot ? 'Fail' : 'Pass',
                tax_buy: signal.tax_buy || 0,
                tax_sell: signal.tax_sell || 0
            };

            // 调用 Hard Gate
            const result = this.hardGateFilter.evaluate(snapshot);

            if (result.status === 'REJECT') {
                return { pass: false, reason: result.reasons.join(', ') };
            }

            // PASS 或 GREYLIST 都允许 Flash 买入
            return { pass: true, reason: result.reasons.join(', '), status: result.status };

        } catch (error) {
            console.error(`[FlashScout] Hard Gate 检查失败: ${error.message}`);
            return { pass: false, reason: `检查失败: ${error.message}` };
        }
    }

    /**
     * 处理信号 - Flash Scout 主入口
     */
    async processSignal(signal, chainSnapshot = null) {
        this.stats.totalSignals++;

        const tokenCA = signal.token_ca || signal.tokenAddress;
        const symbol = signal.symbol || tokenCA?.slice(0, 8) || 'Unknown';
        const chain = (signal.chain || 'SOL').toUpperCase();

        // v7.4.1 全局去重检查 (通过 CrossValidator 统一入口)
        if (this.crossValidator && tokenCA) {
            const isDuplicate = this.crossValidator.isDuplicated ?
                this.crossValidator.isDuplicated(tokenCA) :
                this.crossValidator.recentDecisions?.has(tokenCA);

            if (isDuplicate) {
                return {
                    flash: false,
                    reason: '全局去重: 代币已在其他通道处理',
                    action: 'SKIP_DUPLICATE'
                };
            }
        }

        // Step 1: 检查是否是 Flash 候选
        const flashCheck = this.isFlashCandidate(signal);

        if (!flashCheck.eligible) {
            // 不是 Flash 候选，返回让正常流程处理
            return {
                flash: false,
                reason: flashCheck.reason,
                action: 'NORMAL_FLOW'
            };
        }

        this.stats.flashTriggers++;
        console.log(`\n⚡ [FlashScout] ${symbol} (${chain})`);
        console.log(`   ${flashCheck.reason}`);

        // Step 2: 快速 Hard Gate 检查
        const hardGateResult = await this.quickHardGateCheck(signal, chainSnapshot);

        if (!hardGateResult.pass) {
            this.stats.hardGateRejected++;
            console.log(`   ❌ Hard Gate 拒绝: ${hardGateResult.reason}`);
            return {
                flash: true,
                triggered: true,
                executed: false,
                reason: `Hard Gate 拒绝: ${hardGateResult.reason}`,
                action: 'REJECT'
            };
        }

        this.stats.hardGatePassed++;
        console.log(`   ✅ Hard Gate 通过 (${hardGateResult.status})`);

        // Step 3: 计算 Flash 仓位
        const positionSize = chain === 'BSC'
            ? this.config.flashPositionBNB
            : this.config.flashPositionSOL;

        // Step 4: 发射 Flash 买入事件
        const flashBuy = {
            token_ca: tokenCA,
            symbol: symbol,
            chain: chain,
            position_size: positionSize,
            flash_data: flashCheck.data,
            hard_gate_status: hardGateResult.status,
            signal_source: signal.source || signal.signal_type || 'unknown',
            timestamp: Date.now(),

            // v7.4 信号血统追踪 (Signal Lineage)
            signalLineage: {
                source: signal.signalLineage?.source || signal.source || 'flash_scout',
                hunterType: signal.signalLineage?.hunterType || signal.hunter?.type || null,
                hunterAddr: signal.signalLineage?.hunterAddr || signal.hunter?.address || signal.shadow_wallet || null,
                hunterScore: signal.signalLineage?.hunterScore || signal.hunter?.score || signal.shadow_score || null,
                route: 'flash_scout',  // Flash Scout 是快速通道路由
                entryReason: signal.signalLineage?.entryReason || 'early_entry_flash',
                confidence: signal.signalLineage?.confidence || 'direct'
            }
        };

        // 记录已处理
        this.processedTokens.set(tokenCA, Date.now());
        this.flashBuys.set(tokenCA, flashBuy);

        console.log(`   🚀 Flash 买入: ${positionSize} ${chain === 'BSC' ? 'BNB' : 'SOL'}`);

        // 发射事件让主系统执行
        this.emit('flash-buy', flashBuy);

        this.stats.executed++;

        return {
            flash: true,
            triggered: true,
            executed: true,
            reason: `Flash 买入触发`,
            action: 'FLASH_BUY',
            data: flashBuy
        };
    }

    /**
     * 后台验证通过后的加仓处理
     */
    async handleValidationPassed(tokenCA, validationResult) {
        if (!this.flashBuys.has(tokenCA)) {
            return null;
        }

        const flashBuy = this.flashBuys.get(tokenCA);
        const chain = flashBuy.chain;

        // 计算加仓金额
        const addPosition = flashBuy.position_size * this.config.addPositionMultiplier;

        console.log(`\n⚡ [FlashScout] ${flashBuy.symbol} 验证通过，建议加仓`);
        console.log(`   原始仓位: ${flashBuy.position_size} ${chain === 'BSC' ? 'BNB' : 'SOL'}`);
        console.log(`   建议加仓: ${addPosition} ${chain === 'BSC' ? 'BNB' : 'SOL'}`);

        const addBuy = {
            token_ca: tokenCA,
            symbol: flashBuy.symbol,
            chain: chain,
            position_size: addPosition,
            action: 'ADD_POSITION',
            original_flash: flashBuy,
            validation: validationResult,
            timestamp: Date.now()
        };

        this.emit('flash-add', addBuy);

        return addBuy;
    }

    /**
     * 获取统计信息
     */
    getStats() {
        return {
            ...this.stats,
            conversionRate: this.stats.flashTriggers > 0
                ? (this.stats.executed / this.stats.flashTriggers * 100).toFixed(1) + '%'
                : '0%',
            hardGatePassRate: this.stats.flashTriggers > 0
                ? (this.stats.hardGatePassed / this.stats.flashTriggers * 100).toFixed(1) + '%'
                : '0%',
            activeFlashBuys: this.flashBuys.size
        };
    }

    /**
     * 清理过期缓存
     */
    cleanup() {
        const now = Date.now();
        const maxAge = 2 * 60 * 60 * 1000; // 2小时

        for (const [key, timestamp] of this.processedTokens) {
            if (now - timestamp > maxAge) {
                this.processedTokens.delete(key);
            }
        }

        for (const [key, data] of this.flashBuys) {
            if (now - data.timestamp > maxAge) {
                this.flashBuys.delete(key);
            }
        }
    }
}

export default FlashScout;
