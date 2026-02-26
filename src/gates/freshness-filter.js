/**
 * Freshness Filter v6.9
 *
 * 新鲜度过滤器 - 确保只处理新鲜、可操作的信号
 *
 * 核心理念:
 * - 信号有时效性，越新越有价值
 * - 代币有最佳入场窗口，错过了就是接盘
 * - 信息传播有延迟，需要估算真实发生时间
 *
 * 过滤维度:
 * 1. 信号年龄 - 信号产生到接收的时间差
 * 2. 代币年龄 - 代币创建/上市到现在的时间
 * 3. 价格走势 - 已经涨了多少 (判断是否错过)
 * 4. 信息源延迟 - 不同信息源的平均延迟校正
 */

export class FreshnessFilter {
    constructor(config = {}) {
        this.config = {
            // ═══════════════════════════════════════════════════════════════
            // 1. 信号新鲜度 (Signal Freshness)
            // 信号从产生到我们接收的时间
            // ═══════════════════════════════════════════════════════════════
            signal: {
                // 理想时间窗口
                idealMaxAgeMinutes: 5,        // 最佳: 5分钟内
                acceptableMaxAgeMinutes: 15,  // 可接受: 15分钟内
                staleAgeMinutes: 30,          // 过时: 30分钟以上

                // 分数衰减 (指数衰减)
                decayRate: 0.1,  // 每分钟衰减 10%

                // 信号源延迟校正 (毫秒)
                sourceLatency: {
                    telegram: 0,           // TG 实时
                    debot: 5000,           // DeBot 约 5秒延迟
                    gmgn: 10000,           // GMGN 约 10秒延迟
                    dexscreener: 30000,    // DEXScreener 约 30秒延迟
                    shadow: 15000,         // Shadow 约 15秒延迟
                    alpha: 60000,          // Alpha 账号 约 1分钟延迟
                    twitter: 120000,       // Twitter 约 2分钟延迟
                }
            },

            // ═══════════════════════════════════════════════════════════════
            // 2. 代币新鲜度 (Token Freshness)
            // 代币从创建/上市到现在的时间
            // ═══════════════════════════════════════════════════════════════
            token: {
                // 最佳入场窗口
                goldenWindowMinutes: 30,      // 黄金窗口: 30分钟内
                silverWindowMinutes: 60,      // 白银窗口: 1小时内
                bronzeWindowMinutes: 120,     // 青铜窗口: 2小时内

                // 超过这个时间不再考虑
                maxAgeMinutes: 360,           // 最大: 6小时

                // 例外: 如果聪明钱刚进场，可以放宽
                smartMoneyException: {
                    enabled: true,
                    maxAgeMinutes: 720,       // 有聪明钱可延长到 12小时
                    minSmartMoney: 3          // 需要至少 3 个聪明钱
                }
            },

            // ═══════════════════════════════════════════════════════════════
            // 3. 价格走势检查 (Price Move Check)
            // 如果价格已经涨太多，可能是接盘
            // ═══════════════════════════════════════════════════════════════
            priceMove: {
                enabled: true,

                // 相对于信号产生时的价格
                maxGainFromSignal: 0.50,     // 信号后涨 50% 以上 = 可能晚了

                // 相对于代币初始价格
                maxGainFromLaunch: 3.0,      // 从发射涨 300% 以上 = 可能接盘

                // 例外: 高聪明钱 + 高评分可以放宽
                exception: {
                    enabled: true,
                    minScore: 75,
                    minSmartMoney: 5,
                    maxGainFromLaunch: 5.0   // 高质量信号可放宽到 500%
                }
            },

            // ═══════════════════════════════════════════════════════════════
            // 4. 重复信号过滤 (Duplicate Check)
            // 同一代币短时间内多次出现，只处理第一次
            // ═══════════════════════════════════════════════════════════════
            duplicate: {
                enabled: true,
                windowMinutes: 15,            // 15分钟内视为重复
                allowUpgrade: true,           // 允许升级 (更高优先级信号覆盖)
            },

            // ═══════════════════════════════════════════════════════════════
            // 5. 冷却期检查 (Cooldown Check)
            // 之前处理过但失败的信号需要冷却
            // ═══════════════════════════════════════════════════════════════
            cooldown: {
                enabled: true,
                // 不同失败原因的冷却时间
                durations: {
                    hard_gate_fail: 30,       // Hard Gate 失败: 30分钟
                    low_score: 15,            // 评分低: 15分钟
                    execution_fail: 60,       // 执行失败: 1小时
                    stop_loss: 120,           // 止损卖出: 2小时
                    blacklist: 'permanent'    // 黑名单: 永久
                }
            }
        };

        // 缓存
        this.processedSignals = new Map();  // tokenCA -> { timestamp, source, score }
        this.cooldownTokens = new Map();    // tokenCA -> { reason, until }
        this.processingLocks = new Map();   // v7.4.2 竞态条件修复: tokenCA -> timestamp

        console.log('[Freshness] 🧊 Freshness Filter v6.9 初始化');
        console.log(`[Freshness] 信号窗口: ${this.config.signal.acceptableMaxAgeMinutes}分钟`);
        console.log(`[Freshness] 代币窗口: ${this.config.token.goldenWindowMinutes}-${this.config.token.maxAgeMinutes}分钟`);
    }

    /**
     * 计算信号新鲜度得分
     * @param {Object} signal - 信号数据
     * @returns {Object} { score, ageMinutes, status, reason }
     */
    calculateSignalFreshness(signal) {
        const source = signal.source || signal.signal_type || 'unknown';
        const latency = this.config.signal.sourceLatency[source] || 0;

        // 计算信号年龄 (考虑信号源延迟)
        let signalTime = signal.timestamp || signal.created_at;
        if (typeof signalTime === 'string') {
            signalTime = new Date(signalTime).getTime();
        } else if (typeof signalTime === 'number' && signalTime < 10000000000) {
            signalTime = signalTime * 1000; // Unix 秒转毫秒
        }

        const actualSignalTime = signalTime - latency;
        const ageMs = Date.now() - actualSignalTime;
        const ageMinutes = ageMs / (1000 * 60);

        // 计算得分 (指数衰减)
        const decayRate = this.config.signal.decayRate;
        const idealMax = this.config.signal.idealMaxAgeMinutes;
        const acceptableMax = this.config.signal.acceptableMaxAgeMinutes;
        const staleAge = this.config.signal.staleAgeMinutes;

        let score, status;

        if (ageMinutes <= idealMax) {
            score = 1.0;
            status = 'FRESH';
        } else if (ageMinutes <= acceptableMax) {
            score = 1.0 - (ageMinutes - idealMax) * decayRate;
            status = 'OK';
        } else if (ageMinutes <= staleAge) {
            score = Math.max(0.3, 1.0 - (ageMinutes - idealMax) * decayRate);
            status = 'AGING';
        } else {
            score = 0.1;
            status = 'STALE';
        }

        return {
            score: Math.max(0, Math.min(1, score)),
            ageMinutes,
            status,
            reason: `信号年龄 ${ageMinutes.toFixed(1)}min (${source} +${(latency/1000).toFixed(0)}s延迟)`
        };
    }

    /**
     * 计算代币新鲜度得分
     * @param {Object} signal - 信号数据
     * @returns {Object} { score, ageMinutes, window, reason }
     */
    calculateTokenFreshness(signal) {
        // 获取代币创建时间
        let tokenCreatedAt = signal.open_timestamp ||
                            signal.token_created_at ||
                            signal.created_timestamp;

        if (!tokenCreatedAt) {
            // 无法确定代币年龄，使用信号时间估算
            return {
                score: 0.8,
                ageMinutes: null,
                window: 'UNKNOWN',
                reason: '无法确定代币年龄'
            };
        }

        if (typeof tokenCreatedAt === 'number' && tokenCreatedAt < 10000000000) {
            tokenCreatedAt = tokenCreatedAt * 1000;
        }

        const ageMs = Date.now() - tokenCreatedAt;
        const ageMinutes = ageMs / (1000 * 60);

        const goldenWindow = this.config.token.goldenWindowMinutes;
        const silverWindow = this.config.token.silverWindowMinutes;
        const bronzeWindow = this.config.token.bronzeWindowMinutes;
        const maxAge = this.config.token.maxAgeMinutes;

        // 检查聪明钱例外
        const smartMoneyCount = signal.smart_money_count ||
                               signal.smartMoneyCount ||
                               signal.smart_wallet_online || 0;

        const smException = this.config.token.smartMoneyException;
        if (smException.enabled && smartMoneyCount >= smException.minSmartMoney) {
            if (ageMinutes <= smException.maxAgeMinutes) {
                return {
                    score: 0.8,
                    ageMinutes,
                    window: 'SM_EXCEPTION',
                    reason: `代币${(ageMinutes/60).toFixed(1)}h但有${smartMoneyCount}个聪明钱`
                };
            }
        }

        let score, window;

        if (ageMinutes <= goldenWindow) {
            score = 1.0;
            window = 'GOLDEN';
        } else if (ageMinutes <= silverWindow) {
            score = 0.85;
            window = 'SILVER';
        } else if (ageMinutes <= bronzeWindow) {
            score = 0.70;
            window = 'BRONZE';
        } else if (ageMinutes <= maxAge) {
            score = 0.50;
            window = 'LATE';
        } else {
            score = 0.1;
            window = 'EXPIRED';
        }

        return {
            score,
            ageMinutes,
            window,
            reason: `代币年龄 ${ageMinutes.toFixed(0)}min (${window}窗口)`
        };
    }

    /**
     * 检查价格走势
     * @param {Object} signal - 信号数据
     * @param {Object} options - 额外选项 { score, smartMoneyCount }
     * @returns {Object} { pass, reason, gainFromLaunch }
     */
    checkPriceMove(signal, options = {}) {
        if (!this.config.priceMove.enabled) {
            return { pass: true, reason: '价格检查已禁用' };
        }

        const currentPrice = signal.current_price || signal.price || 0;
        const launchPrice = signal.launch_price || signal.initial_price || 0;
        const signalPrice = signal.signal_price || 0;

        // 无法比较
        if (!currentPrice || (!launchPrice && !signalPrice)) {
            return { pass: true, reason: '无价格数据' };
        }

        // 计算涨幅
        const gainFromLaunch = launchPrice > 0
            ? (currentPrice - launchPrice) / launchPrice
            : null;

        const gainFromSignal = signalPrice > 0
            ? (currentPrice - signalPrice) / signalPrice
            : null;

        // 检查例外条件
        const exception = this.config.priceMove.exception;
        if (exception.enabled) {
            const score = options.score || 0;
            const smartMoney = options.smartMoneyCount || signal.smart_money_count || 0;

            if (score >= exception.minScore && smartMoney >= exception.minSmartMoney) {
                // 高质量信号，放宽限制
                if (gainFromLaunch !== null && gainFromLaunch > exception.maxGainFromLaunch) {
                    return {
                        pass: false,
                        reason: `从发射已涨 ${(gainFromLaunch * 100).toFixed(0)}% (高质量限制 ${(exception.maxGainFromLaunch * 100).toFixed(0)}%)`,
                        gainFromLaunch
                    };
                }
                return { pass: true, reason: '高质量信号例外', gainFromLaunch };
            }
        }

        // 标准检查
        if (gainFromSignal !== null && gainFromSignal > this.config.priceMove.maxGainFromSignal) {
            return {
                pass: false,
                reason: `信号后已涨 ${(gainFromSignal * 100).toFixed(0)}% (限制 ${(this.config.priceMove.maxGainFromSignal * 100).toFixed(0)}%)`,
                gainFromLaunch,
                gainFromSignal
            };
        }

        if (gainFromLaunch !== null && gainFromLaunch > this.config.priceMove.maxGainFromLaunch) {
            return {
                pass: false,
                reason: `从发射已涨 ${(gainFromLaunch * 100).toFixed(0)}% (限制 ${(this.config.priceMove.maxGainFromLaunch * 100).toFixed(0)}%)`,
                gainFromLaunch
            };
        }

        return {
            pass: true,
            reason: '价格涨幅合理',
            gainFromLaunch,
            gainFromSignal
        };
    }

    /**
     * 检查是否重复信号
     * @param {string} tokenCA - 代币地址
     * @param {Object} signal - 信号数据
     * @returns {Object} { isDuplicate, reason, previousSignal }
     */
    checkDuplicate(tokenCA, signal) {
        if (!this.config.duplicate.enabled) {
            return { isDuplicate: false };
        }

        const previous = this.processedSignals.get(tokenCA);
        if (!previous) {
            return { isDuplicate: false };
        }

        const ageMs = Date.now() - previous.timestamp;
        const ageMinutes = ageMs / (1000 * 60);

        if (ageMinutes > this.config.duplicate.windowMinutes) {
            // 超过窗口，不算重复
            return { isDuplicate: false };
        }

        // 检查是否允许升级
        if (this.config.duplicate.allowUpgrade) {
            const currentPriority = this.getSourcePriority(signal.source || 'unknown');
            const previousPriority = this.getSourcePriority(previous.source);

            if (currentPriority > previousPriority) {
                return {
                    isDuplicate: false,
                    isUpgrade: true,
                    reason: `升级信号: ${previous.source} → ${signal.source}`,
                    previousSignal: previous
                };
            }
        }

        return {
            isDuplicate: true,
            reason: `${ageMinutes.toFixed(1)}分钟前已处理 (来源: ${previous.source})`,
            previousSignal: previous
        };
    }

    /**
     * 获取信号源优先级
     */
    getSourcePriority(source) {
        const priorities = {
            shadow: 100,      // Shadow Protocol 最高
            debot: 90,        // DeBot 次之
            gmgn: 80,         // GMGN
            alpha: 70,        // Alpha 账号
            dexscreener: 60,  // DEXScreener
            telegram: 50,     // Telegram
            twitter: 40,      // Twitter
            unknown: 10       // 未知来源
        };
        return priorities[source] || priorities.unknown;
    }

    /**
     * 检查冷却期
     * @param {string} tokenCA - 代币地址
     * @returns {Object} { inCooldown, reason, remainingMinutes }
     */
    checkCooldown(tokenCA) {
        if (!this.config.cooldown.enabled) {
            return { inCooldown: false };
        }

        const cooldown = this.cooldownTokens.get(tokenCA);
        if (!cooldown) {
            return { inCooldown: false };
        }

        if (cooldown.until === 'permanent') {
            return {
                inCooldown: true,
                reason: `永久冷却 (${cooldown.reason})`,
                remainingMinutes: Infinity
            };
        }

        const now = Date.now();
        if (now >= cooldown.until) {
            this.cooldownTokens.delete(tokenCA);
            return { inCooldown: false };
        }

        const remainingMinutes = (cooldown.until - now) / (1000 * 60);
        return {
            inCooldown: true,
            reason: `冷却中 (${cooldown.reason})`,
            remainingMinutes
        };
    }

    /**
     * 添加到冷却列表
     * @param {string} tokenCA - 代币地址
     * @param {string} reason - 冷却原因
     */
    addToCooldown(tokenCA, reason) {
        const duration = this.config.cooldown.durations[reason] ||
                        this.config.cooldown.durations.low_score;

        const until = duration === 'permanent'
            ? 'permanent'
            : Date.now() + duration * 60 * 1000;

        this.cooldownTokens.set(tokenCA, { reason, until });
    }

    /**
     * 记录已处理信号
     * @param {string} tokenCA - 代币地址
     * @param {Object} signal - 信号数据
     */
    recordProcessed(tokenCA, signal) {
        this.processedSignals.set(tokenCA, {
            timestamp: Date.now(),
            source: signal.source || signal.signal_type || 'unknown',
            score: signal.score || 0
        });
    }

    /**
     * 综合评估信号新鲜度
     * @param {Object} signal - 信号数据
     * @param {Object} options - 额外选项
     * @returns {Object} { pass, score, reasons, checks }
     */
    evaluate(signal, options = {}) {
        const tokenCA = signal.token_ca || signal.tokenAddress;
        const reasons = [];
        const checks = {};

        // 1. 冷却期检查
        checks.cooldown = this.checkCooldown(tokenCA);
        if (checks.cooldown.inCooldown) {
            return {
                pass: false,
                score: 0,
                reasons: [checks.cooldown.reason],
                checks,
                filter: 'cooldown'
            };
        }

        // 2. 重复信号检查
        checks.duplicate = this.checkDuplicate(tokenCA, signal);
        if (checks.duplicate.isDuplicate) {
            return {
                pass: false,
                score: 0,
                reasons: [`重复信号: ${checks.duplicate.reason}`],
                checks,
                filter: 'duplicate'
            };
        }

        // v7.4.2 竞态条件修复: 检查处理锁
        const lockTime = this.processingLocks.get(tokenCA);
        if (lockTime && (Date.now() - lockTime) < 30000) { // 30秒锁定窗口
            return {
                pass: false,
                score: 0,
                reasons: ['正在处理中 (并发锁)'],
                checks,
                filter: 'concurrent_lock'
            };
        }
        // 立即设置锁,防止并发
        this.processingLocks.set(tokenCA, Date.now());

        // 3. 信号新鲜度
        checks.signalFreshness = this.calculateSignalFreshness(signal);
        if (checks.signalFreshness.status === 'STALE') {
            reasons.push(`信号过时: ${checks.signalFreshness.reason}`);
        }

        // 4. 代币新鲜度
        checks.tokenFreshness = this.calculateTokenFreshness(signal);
        if (checks.tokenFreshness.window === 'EXPIRED') {
            return {
                pass: false,
                score: checks.tokenFreshness.score,
                reasons: [`代币过期: ${checks.tokenFreshness.reason}`],
                checks,
                filter: 'token_age'
            };
        }

        // 5. 价格走势检查
        checks.priceMove = this.checkPriceMove(signal, options);
        if (!checks.priceMove.pass) {
            return {
                pass: false,
                score: 0.3,
                reasons: [`价格不合适: ${checks.priceMove.reason}`],
                checks,
                filter: 'price_move'
            };
        }

        // 计算综合得分
        const signalWeight = 0.4;
        const tokenWeight = 0.6;
        const compositeScore =
            checks.signalFreshness.score * signalWeight +
            checks.tokenFreshness.score * tokenWeight;

        // 收集原因
        if (checks.signalFreshness.status !== 'FRESH') {
            reasons.push(checks.signalFreshness.reason);
        }
        if (checks.tokenFreshness.window !== 'GOLDEN') {
            reasons.push(checks.tokenFreshness.reason);
        }
        if (checks.duplicate.isUpgrade) {
            reasons.push(checks.duplicate.reason);
        }

        // 记录已处理
        this.recordProcessed(tokenCA, signal);

        return {
            pass: compositeScore >= 0.3, // 最低30%通过
            score: compositeScore,
            reasons: reasons.length > 0 ? reasons : ['新鲜度检查通过'],
            checks,
            filter: null
        };
    }

    /**
     * 清理过期缓存
     */
    cleanup() {
        const now = Date.now();

        // 清理已处理信号缓存 (1小时)
        const signalMaxAge = 60 * 60 * 1000;
        for (const [key, data] of this.processedSignals) {
            if (now - data.timestamp > signalMaxAge) {
                this.processedSignals.delete(key);
            }
        }

        // 清理过期冷却
        for (const [key, data] of this.cooldownTokens) {
            if (data.until !== 'permanent' && now >= data.until) {
                this.cooldownTokens.delete(key);
            }
        }

        // v7.4.2 清理过期处理锁 (30秒)
        const lockMaxAge = 30 * 1000;
        for (const [key, timestamp] of this.processingLocks) {
            if (now - timestamp > lockMaxAge) {
                this.processingLocks.delete(key);
            }
        }
    }

    /**
     * 获取统计信息
     */
    getStats() {
        return {
            processedCount: this.processedSignals.size,
            cooldownCount: this.cooldownTokens.size,
            config: {
                signalWindow: `${this.config.signal.acceptableMaxAgeMinutes}min`,
                tokenWindow: `${this.config.token.goldenWindowMinutes}-${this.config.token.maxAgeMinutes}min`
            }
        };
    }
}

export default FreshnessFilter;
