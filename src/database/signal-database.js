/**
 * Signal Database - 信号数据库操作
 * 
 * 提供 Telegram 信号查询功能，用于交叉验证
 */

import Database from 'better-sqlite3';
import path from 'path';

class SignalDatabase {
    constructor() {
        const dbPath = process.env.DB_PATH || './data/sentiment_arb.db';
        this.db = null;
        this.dbPath = dbPath;
    }

    /**
     * 确保数据库连接
     */
    ensureConnection() {
        if (!this.db) {
            try {
                this.db = new Database(this.dbPath);
            } catch (error) {
                console.error('[SignalDB] 数据库连接失败:', error.message);
                return false;
            }
        }
        return true;
    }

    /**
     * 查询代币的 Telegram 提及（用于交叉验证）
     * @param {string} tokenAddress - 代币地址
     * @param {number} since - 起始时间戳（毫秒）
     * @returns {Array} 提及列表
     */
    getTokenMentions(tokenAddress, since) {
        if (!this.ensureConnection()) {
            return [];
        }

        try {
            const sinceSeconds = Math.floor(since / 1000);

            const mentions = this.db.prepare(`
                SELECT 
                    id,
                    channel_name,
                    channel_username,
                    message_text,
                    timestamp,
                    created_at
                FROM telegram_signals
                WHERE token_ca = ? 
                AND created_at >= ?
                ORDER BY created_at DESC
            `).all(tokenAddress, sinceSeconds);

            // 添加 channel_id 字段（用 channel_name 作为 ID）
            return mentions.map(m => ({
                ...m,
                channel_id: m.channel_name
            }));

        } catch (error) {
            console.error('[SignalDB] 查询失败:', error.message);
            return [];
        }
    }

    /**
     * 查询代币的频道分布（用于 TG Spread 评分）
     * @param {string} tokenAddress - 代币地址
     * @param {number} timeWindowMinutes - 时间窗口（分钟）
     * @returns {Object} 频道分布信息
     */
    getChannelSpread(tokenAddress, timeWindowMinutes = 60) {
        if (!this.ensureConnection()) {
            return { channelCount: 0, channels: [] };
        }

        try {
            const since = Math.floor(Date.now() / 1000) - (timeWindowMinutes * 60);

            const result = this.db.prepare(`
                SELECT 
                    channel_name,
                    COUNT(*) as mention_count,
                    MIN(created_at) as first_mention,
                    MAX(created_at) as last_mention
                FROM telegram_signals
                WHERE token_ca = ?
                AND created_at >= ?
                GROUP BY channel_name
                ORDER BY first_mention ASC
            `).all(tokenAddress, since);

            return {
                channelCount: result.length,
                totalMentions: result.reduce((sum, r) => sum + r.mention_count, 0),
                channels: result.map(r => ({
                    name: r.channel_name,
                    mentionCount: r.mention_count,
                    firstMention: new Date(r.first_mention * 1000),
                    lastMention: new Date(r.last_mention * 1000)
                }))
            };

        } catch (error) {
            console.error('[SignalDB] 查询失败:', error.message);
            return { channelCount: 0, channels: [] };
        }
    }

    /**
     * 检查代币是否在指定时间内已被处理
     * @param {string} tokenAddress - 代币地址
     * @param {string} chain - 链
     * @param {number} minutes - 时间窗口（分钟）
     * @returns {boolean} 是否已处理
     */
    isRecentlyProcessed(tokenAddress, chain, minutes = 30) {
        if (!this.ensureConnection()) {
            return false;
        }

        try {
            const since = Math.floor(Date.now() / 1000) - (minutes * 60);

            const result = this.db.prepare(`
                SELECT id FROM telegram_signals
                WHERE token_ca = ?
                AND chain = ?
                AND created_at >= ?
                AND processed = 1
                LIMIT 1
            `).get(tokenAddress, chain, since);

            return !!result;

        } catch (error) {
            return false;
        }
    }

    /**
     * 清理过期信号 (留存 1 小时)
     */
    cleanupOldSignals(hours = 1) {
        if (!this.ensureConnection()) return;
        try {
            const since = Math.floor(Date.now() / 1000) - (hours * 3600);
            const result = this.db.prepare(`
                DELETE FROM telegram_signals 
                WHERE created_at < ?
            `).run(since);
            if (result.changes > 0) {
                console.log(`[SignalDB] 🧹 已清理 ${result.changes} 个过期信号 (> ${hours}h)`);
            }
        } catch (error) {
            console.error('[SignalDB] 清理失败:', error.message);
        }
    }

    /**
     * 高效获取唯一频道数 (热度)
     */
    getUniqueChannelCount(tokenAddress, windowMinutes = 60) {
        if (!this.ensureConnection()) return 0;
        try {
            const since = Math.floor(Date.now() / 1000) - (windowMinutes * 60);
            const result = this.db.prepare(`
                SELECT COUNT(DISTINCT channel_name) as count
                FROM telegram_signals
                WHERE token_ca = ? AND created_at >= ?
            `).get(tokenAddress, since);
            return result?.count || 0;
        } catch (error) {
            return 0;
        }
    }

    /**
     * 获取频道的 Tier 信息
     * @param {string} channelName - 频道名称
     * @returns {string} Tier 等级 (A/B/C)
     */
    getChannelTier(channelName) {
        if (!this.ensureConnection()) {
            return 'C';
        }

        try {
            const result = this.db.prepare(`
                SELECT tier FROM telegram_channels
                WHERE channel_name = ? OR channel_username LIKE ?
                LIMIT 1
            `).get(channelName, `%${channelName}%`);

            return result?.tier || 'C';

        } catch (error) {
            return 'C';
        }
    }

    /**
     * 关闭数据库连接
     */
    close() {
        if (this.db) {
            this.db.close();
            this.db = null;
        }
    }
}

// 单例导出
const signalDatabase = new SignalDatabase();

export default signalDatabase;
export { SignalDatabase };
