/**
 * Token Gatekeeper v1.0
 * 
 * 统一的代币准入检查器
 * 在信号进入系统的最早阶段进行过滤，节省后续资源
 * 
 * 检查顺序 (越前越省资源):
 * 1. 已持仓检查 - 防止重复买入
 * 2. 冷却期检查 - 刚卖出的不买回
 * 3. AI拒绝缓存 - 30分钟内被AI拒绝的不重复评估
 * 4. 死狗池检查 - 已知失败模式
 */

import deadDogPool from '../risk/dead-dog-pool.js';

// 拒绝码定义
export const REJECTION_CODES = {
    HAS_POSITION: 'has_position',       // 已持仓
    IN_COOLDOWN: 'in_cooldown',         // 冷却期
    AI_REJECTED: 'ai_rejected',         // AI 拒绝
    DEAD_DOG: 'dead_dog',               // 死狗池
    IN_OBSERVER: 'in_observer'          // 已在观察池
};

export class TokenGatekeeper {
    constructor(db, exitCooldownService) {
        this.db = db;
        this.exitCooldown = exitCooldownService;

        // AI 拒绝缓存 (内存)
        // key: `${chain}:${tokenCA}`, value: { reason, time, stage }
        this.aiRejections = new Map();

        // 观察池追踪 (由外部设置)
        this.observerPool = null;

        // 配置
        this.config = {
            aiRejectionTTL: 30 * 60 * 1000,     // AI 拒绝缓存 30 分钟
            cleanupInterval: 5 * 60 * 1000       // 每 5 分钟清理
        };

        // 启动定期清理
        this.cleanupTimer = setInterval(() => this.cleanup(), this.config.cleanupInterval);

        console.log('[Gatekeeper] 🚪 Token Gatekeeper v1.0 初始化');
    }

    /**
     * 设置观察池引用 (用于检查是否已在观察池)
     */
    setObserverPool(observerPool) {
        this.observerPool = observerPool;
    }

    /**
     * 统一准入检查
     * 在 CrossValidator.onNewToken() 最开始调用
     * 
     * @param {string} tokenCA - 代币合约地址
     * @param {string} chain - 链 (SOL/BSC)
     * @param {Object} options - 可选参数
     * @returns {Object} { allowed: boolean, reason?: string, code?: string }
     */
    canEnter(tokenCA, chain, options = {}) {
        const symbol = options.symbol || tokenCA.substring(0, 8);

        // ═══════════════════════════════════════════════════════════════
        // 1. 已持仓检查 (防止重复买入)
        // ═══════════════════════════════════════════════════════════════
        const positionCheck = this.checkPosition(tokenCA, chain);
        if (!positionCheck.allowed) {
            return positionCheck;
        }

        // ═══════════════════════════════════════════════════════════════
        // 2. 冷却期检查 (刚卖出的不买回)
        // ═══════════════════════════════════════════════════════════════
        const cooldownCheck = this.checkCooldown(tokenCA, chain);
        if (!cooldownCheck.allowed) {
            return cooldownCheck;
        }

        // ═══════════════════════════════════════════════════════════════
        // 3. AI 拒绝缓存检查 (30分钟内被拒的不重复评估)
        // ═══════════════════════════════════════════════════════════════
        const aiCheck = this.checkAIRejection(tokenCA, chain);
        if (!aiCheck.allowed) {
            return aiCheck;
        }

        // ═══════════════════════════════════════════════════════════════
        // 4. 观察池检查 (已在观察池的不重复入池)
        // ═══════════════════════════════════════════════════════════════
        if (this.observerPool) {
            const inObserver = this.observerPool.hasToken(tokenCA);
            if (inObserver) {
                return {
                    allowed: false,
                    reason: '已在观察池中',
                    code: REJECTION_CODES.IN_OBSERVER
                };
            }
        }

        // ═══════════════════════════════════════════════════════════════
        // 5. 死狗池检查 (已知失败模式)
        // ═══════════════════════════════════════════════════════════════
        const deadDogCheck = this.checkDeadDog(tokenCA, options);
        if (!deadDogCheck.allowed) {
            return deadDogCheck;
        }

        return { allowed: true };
    }

    /**
     * 检查是否已持仓
     */
    checkPosition(tokenCA, chain) {
        if (!this.db) {
            return { allowed: true };
        }

        try {
            const existing = this.db.prepare(`
        SELECT id, symbol, position_size_native, status
        FROM positions 
        WHERE token_ca = ? AND chain = ? 
        AND status IN ('open', 'partial')
      `).get(tokenCA, chain);

            if (existing) {
                return {
                    allowed: false,
                    reason: `已持仓 ${existing.symbol || tokenCA.substring(0, 8)} (${existing.position_size_native} ${chain === 'SOL' ? 'SOL' : 'BNB'}, ${existing.status})`,
                    code: REJECTION_CODES.HAS_POSITION,
                    existingPosition: existing
                };
            }

            return { allowed: true };
        } catch (e) {
            console.error('[Gatekeeper] 持仓检查错误:', e.message);
            return { allowed: true }; // 出错时放行，不阻断流程
        }
    }

    /**
     * 检查冷却期
     */
    checkCooldown(tokenCA, chain) {
        if (!this.exitCooldown) {
            return { allowed: true };
        }

        try {
            const cooldownInfo = this.exitCooldown.isInCooldown(tokenCA, chain);

            if (cooldownInfo) {
                return {
                    allowed: false,
                    reason: `冷却中 ${cooldownInfo.remaining_minutes.toFixed(0)}min (${cooldownInfo.exit_reason})`,
                    code: REJECTION_CODES.IN_COOLDOWN,
                    cooldownInfo
                };
            }

            return { allowed: true };
        } catch (e) {
            console.error('[Gatekeeper] 冷却检查错误:', e.message);
            return { allowed: true };
        }
    }

    /**
     * 检查 AI 拒绝缓存
     */
    checkAIRejection(tokenCA, chain) {
        const key = `${chain}:${tokenCA}`;
        const rejection = this.aiRejections.get(key);

        if (!rejection) {
            return { allowed: true };
        }

        // 检查是否过期
        const age = Date.now() - rejection.time;
        if (age > this.config.aiRejectionTTL) {
            this.aiRejections.delete(key);
            return { allowed: true };
        }

        const remainingMinutes = Math.ceil((this.config.aiRejectionTTL - age) / 60000);

        return {
            allowed: false,
            reason: `AI已拒绝: ${rejection.reason} (${remainingMinutes}min后重试)`,
            code: REJECTION_CODES.AI_REJECTED,
            rejectionInfo: rejection
        };
    }

    /**
     * 检查死狗池
     */
    checkDeadDog(tokenCA, options = {}) {
        try {
            const blacklistCheck = deadDogPool.isBlacklisted({
                symbol: options.symbol,
                name: options.name || options.symbol,
                tokenCA: tokenCA,
                creator: options.creator || options.deployer,
                narrative: options.narrative || options.category
            });

            if (blacklistCheck.blocked) {
                return {
                    allowed: false,
                    reason: blacklistCheck.reason,
                    code: REJECTION_CODES.DEAD_DOG,
                    blacklistInfo: blacklistCheck
                };
            }

            return { allowed: true };
        } catch (e) {
            console.error('[Gatekeeper] 死狗池检查错误:', e.message);
            return { allowed: true };
        }
    }

    /**
     * 记录 AI 拒绝
     * 当 AI 决策返回 SKIP 时调用
     * 
     * @param {string} tokenCA - 代币地址
     * @param {string} chain - 链
     * @param {string} reason - 拒绝原因
     * @param {string} stage - 拒绝阶段 (ai_decision, score_low, trap_detected 等)
     */
    recordAIRejection(tokenCA, chain, reason, stage = 'ai_decision') {
        const key = `${chain}:${tokenCA}`;

        this.aiRejections.set(key, {
            reason,
            stage,
            time: Date.now()
        });

        // 同时写入数据库 (可选，用于分析)
        if (this.db) {
            try {
                this.db.prepare(`
          INSERT OR REPLACE INTO rejected_signals (
            token_ca, chain, symbol, signal_source,
            rejection_stage, rejection_reason, rejection_factors,
            created_at
          ) VALUES (?, ?, ?, 'gatekeeper', ?, ?, '{}', datetime('now'))
        `).run(
                    tokenCA,
                    chain,
                    tokenCA.substring(0, 8),
                    stage,
                    reason
                );
            } catch (e) {
                // 忽略 DB 错误，不影响主流程
            }
        }
    }

    /**
     * 清除 AI 拒绝记录
     * 当代币状态改变时可能需要重新评估
     */
    clearAIRejection(tokenCA, chain) {
        const key = `${chain}:${tokenCA}`;
        this.aiRejections.delete(key);
    }

    /**
     * 清理过期缓存
     */
    cleanup() {
        const now = Date.now();
        let cleaned = 0;

        for (const [key, rejection] of this.aiRejections) {
            if (now - rejection.time > this.config.aiRejectionTTL) {
                this.aiRejections.delete(key);
                cleaned++;
            }
        }

        if (cleaned > 0) {
            console.log(`[Gatekeeper] 🧹 清理了 ${cleaned} 条过期 AI 拒绝记录`);
        }
    }

    /**
     * 获取统计信息
     */
    getStats() {
        return {
            aiRejectionsCount: this.aiRejections.size,
            config: {
                aiRejectionTTL: `${this.config.aiRejectionTTL / 60000} min`
            }
        };
    }

    /**
     * 停止服务
     */
    stop() {
        if (this.cleanupTimer) {
            clearInterval(this.cleanupTimer);
            this.cleanupTimer = null;
        }
        console.log('[Gatekeeper] 🛑 Token Gatekeeper 已停止');
    }
}

export default TokenGatekeeper;
