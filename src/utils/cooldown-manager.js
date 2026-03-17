/**
 * Cooldown Manager v9.3
 *
 * 统一管理系统中所有冷却期逻辑，避免冲突和重复
 */

import fs from 'fs';
import path from 'path';
import { atomicWriteJSON } from './atomic-write.js';

class CooldownManager {
    constructor() {
        // 统一的冷却期配置 (毫秒)
        this.COOLDOWNS = {
            // 拒绝冷却期
            rejection: {
                '市值过大': 4 * 60 * 60 * 1000,        // 4小时
                '流动性不足': 1 * 60 * 60 * 1000,      // 1小时
                '叙事衰退': 6 * 60 * 60 * 1000,        // 6小时
                'SM流出': 30 * 60 * 1000,              // 30分钟
                '价格急跌': 1 * 60 * 60 * 1000,        // 1小时
                '安全检查失败': 2 * 60 * 60 * 1000,    // 2小时
                'default': 2 * 60 * 60 * 1000          // 默认2小时
            },

            // 硬门槛拒绝冷却期
            hardgate: {
                'price_rise': 15 * 60 * 1000,          // 涨幅过高：15分钟
                'price_drop': 20 * 60 * 1000,          // 跌幅过大：20分钟
                'sm_flow': 10 * 60 * 1000,             // SM流出：10分钟
                'security': 2 * 60 * 60 * 1000,        // 安全检查：2小时
                'default': 15 * 60 * 1000              // 默认15分钟
            },

            // AI决策冷却期
            ai: {
                'discard': 30 * 60 * 1000,             // AI丢弃：30分钟
                'analysis': 10 * 60 * 1000,            // 分析冷却：10分钟
                'default': 15 * 60 * 1000
            },

            // 退出冷却期 (防止同币反复交易)
            exit: {
                'stop_loss': 30 * 60 * 1000,           // 止损后：30分钟
                'take_profit': 60 * 60 * 1000,         // 止盈后：60分钟
                'time_stop': 15 * 60 * 1000,           // 时间止损：15分钟
                'default': 30 * 60 * 1000
            },

            // 仓位冷却期
            position: {
                'same_token': 30 * 60 * 1000,          // 同币买入：30分钟
                'capacity_full': 10 * 60 * 1000,       // 仓位满：10分钟
                'default': 30 * 60 * 1000
            }
        };

        // 冷却记录存储
        this.records = new Map();  // key: `${type}:${tokenCA}` -> { timestamp, reason, cooldown }

        // 持久化路径
        this.persistPath = path.join(process.cwd(), 'data', 'cooldowns.json');

        // 加载持久化数据
        this.loadRecords();

        console.log('[CooldownManager] ✅ 冷却期管理器初始化完成');
    }

    /**
     * 获取冷却时间
     * @param {string} category - 类别 (rejection/hardgate/ai/exit/position)
     * @param {string} reason - 原因
     * @returns {number} 冷却时间(毫秒)
     */
    getCooldownTime(category, reason) {
        const categoryConfig = this.COOLDOWNS[category];
        if (!categoryConfig) return this.COOLDOWNS.rejection.default;

        // 尝试精确匹配
        if (categoryConfig[reason]) return categoryConfig[reason];

        // 尝试模糊匹配
        for (const [key, value] of Object.entries(categoryConfig)) {
            if (reason.includes(key) || key.includes(reason)) {
                return value;
            }
        }

        return categoryConfig.default || this.COOLDOWNS.rejection.default;
    }

    /**
     * 添加冷却记录
     * @param {string} category - 类别
     * @param {string} tokenCA - 代币地址
     * @param {string} reason - 原因
     * @param {object} extra - 额外信息
     */
    addCooldown(category, tokenCA, reason, extra = {}) {
        const cooldown = this.getCooldownTime(category, reason);
        const key = `${category}:${tokenCA}`;

        this.records.set(key, {
            timestamp: Date.now(),
            reason,
            cooldown,
            category,
            ...extra
        });

        // 异步保存
        this.saveRecords();

        return cooldown;
    }

    /**
     * 检查是否在冷却期
     * @param {string} category - 类别
     * @param {string} tokenCA - 代币地址
     * @returns {object|null} 如果在冷却期返回记录，否则返回 null
     */
    isInCooldown(category, tokenCA) {
        const key = `${category}:${tokenCA}`;
        const record = this.records.get(key);

        if (!record) return null;

        const elapsed = Date.now() - record.timestamp;
        if (elapsed >= record.cooldown) {
            // 冷却期已结束，清除记录
            this.records.delete(key);
            this.saveRecords();
            return null;
        }

        // 返回剩余时间
        return {
            ...record,
            remaining: record.cooldown - elapsed,
            remainingMinutes: Math.ceil((record.cooldown - elapsed) / 60000)
        };
    }

    /**
     * 清除冷却记录
     */
    clearCooldown(category, tokenCA) {
        const key = `${category}:${tokenCA}`;
        this.records.delete(key);
        this.saveRecords();
    }

    /**
     * 批量清理过期记录
     */
    cleanupExpired() {
        const now = Date.now();
        let cleaned = 0;

        for (const [key, record] of this.records.entries()) {
            if (now - record.timestamp >= record.cooldown) {
                this.records.delete(key);
                cleaned++;
            }
        }

        if (cleaned > 0) {
            this.saveRecords();
            console.log(`[CooldownManager] 🧹 清理了 ${cleaned} 条过期记录`);
        }

        return cleaned;
    }

    /**
     * 获取统计信息
     */
    getStats() {
        const stats = {
            total: this.records.size,
            byCategory: {}
        };

        for (const [key, record] of this.records.entries()) {
            const category = record.category || key.split(':')[0];
            stats.byCategory[category] = (stats.byCategory[category] || 0) + 1;
        }

        return stats;
    }

    /**
     * 持久化保存
     */
    saveRecords() {
        try {
            const data = Array.from(this.records.entries());
            atomicWriteJSON(this.persistPath, data).catch(() => {});
        } catch (e) {
            // 静默处理
        }
    }

    /**
     * 加载持久化数据
     */
    loadRecords() {
        try {
            if (fs.existsSync(this.persistPath)) {
                const data = JSON.parse(fs.readFileSync(this.persistPath, 'utf8'));

                // 过滤掉已过期的记录
                const now = Date.now();
                const validRecords = data.filter(([key, record]) => {
                    const elapsed = now - record.timestamp;
                    return elapsed < record.cooldown;
                });

                this.records = new Map(validRecords);
                console.log(`[CooldownManager] 📂 加载了 ${this.records.size} 条有效冷却记录`);
            }
        } catch (e) {
            this.records = new Map();
        }
    }
}

// 单例模式
const cooldownManager = new CooldownManager();

export default cooldownManager;
export { CooldownManager };
