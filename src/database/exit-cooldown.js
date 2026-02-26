/**
 * Exit Cooldown Service
 * 
 * 临时冷却机制：当系统因为 TIME_STOP、STOP_LOSS 或其他"逻辑证伪"原因平仓后，
 * 将该 token 加入临时冷却列表，避免在冷却期内重新买入。
 * 
 * 修复 BUG：系统平仓后 7 秒又买入同一个币的问题
 * 
 * 冷却时间：
 * - TIME_STOP (时间止损): 2 小时
 * - STOP_LOSS (价格止损): 4 小时
 * - EMERGENCY_EXIT (紧急撤退): 24 小时
 * - 其他: 1 小时
 */

export class ExitCooldownService {
    constructor(db) {
        this.db = db;
        this.initDatabase();
    }

    /**
     * 初始化 exit_cooldown 表
     */
    initDatabase() {
        try {
            this.db.exec(`
        CREATE TABLE IF NOT EXISTS exit_cooldown (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          token_ca TEXT NOT NULL,
          chain TEXT NOT NULL,
          exit_reason TEXT NOT NULL,
          exit_time TEXT NOT NULL DEFAULT (datetime('now')),
          cooldown_until TEXT NOT NULL,
          pnl_percent REAL,
          position_id INTEGER,
          created_at INTEGER DEFAULT (strftime('%s', 'now')),
          UNIQUE(token_ca, chain)
        )
      `);

            // 创建索引
            this.db.exec(`
        CREATE INDEX IF NOT EXISTS idx_exit_cooldown_lookup 
        ON exit_cooldown(token_ca, chain, cooldown_until)
      `);

            console.log('   ✅ Exit Cooldown table initialized');
        } catch (error) {
            // 表可能已存在
            if (!error.message.includes('already exists')) {
                console.error('❌ Exit Cooldown init error:', error.message);
            }
        }
    }

    /**
     * 获取冷却时长（分钟）
     */
    getCooldownMinutes(exitReason) {
        const cooldowns = {
            'TIME_STOP': 120,        // 2 小时 - 时间止损/逻辑证伪
            'STOP_LOSS': 240,        // 4 小时 - 价格止损
            'EMERGENCY_EXIT': 1440,  // 24 小时 - 紧急撤退（流动性崩溃、Dev出逃等）
            'PROFIT_TAKE': 60,       // 1 小时 - 止盈卖出（可能还会有机会）
            'BREAKEVEN': 30,         // 30 分钟 - 翻倍出本（只是部分卖出）
            'default': 60            // 默认 1 小时
        };

        return cooldowns[exitReason] || cooldowns['default'];
    }

    /**
     * 将 token 加入冷却列表
     */
    addToCooldown(tokenCA, chain, exitReason, pnlPercent = null, positionId = null) {
        try {
            const cooldownMinutes = this.getCooldownMinutes(exitReason);

            // 使用 INSERT OR REPLACE 确保每个 token+chain 只有一条记录
            this.db.prepare(`
        INSERT OR REPLACE INTO exit_cooldown (
          token_ca, chain, exit_reason, exit_time, cooldown_until, pnl_percent, position_id
        ) VALUES (
          ?, ?, ?, datetime('now'), datetime('now', '+${cooldownMinutes} minutes'), ?, ?
        )
      `).run(tokenCA, chain, exitReason, pnlPercent, positionId);

            console.log(`   🧊 [Cooldown] ${tokenCA.substring(0, 8)} 冷却 ${cooldownMinutes} 分钟 (原因: ${exitReason})`);

            return { success: true, cooldown_minutes: cooldownMinutes };
        } catch (error) {
            console.error('❌ Add to cooldown error:', error.message);
            return { success: false, error: error.message };
        }
    }

    /**
     * 检查 token 是否在冷却中
     * 
     * @returns {Object|null} 如果在冷却中返回冷却信息，否则返回 null
     */
    isInCooldown(tokenCA, chain) {
        try {
            const record = this.db.prepare(`
        SELECT 
          exit_reason,
          exit_time,
          cooldown_until,
          pnl_percent,
          ROUND((julianday(cooldown_until) - julianday('now')) * 24 * 60) as remaining_minutes
        FROM exit_cooldown
        WHERE token_ca = ? AND chain = ?
          AND cooldown_until > datetime('now')
      `).get(tokenCA, chain);

            if (record) {
                return {
                    in_cooldown: true,
                    exit_reason: record.exit_reason,
                    exit_time: record.exit_time,
                    cooldown_until: record.cooldown_until,
                    pnl_percent: record.pnl_percent,
                    remaining_minutes: Math.max(0, record.remaining_minutes)
                };
            }

            return null;
        } catch (error) {
            console.error('❌ Check cooldown error:', error.message);
            return null;
        }
    }

    /**
     * 清理过期的冷却记录
     */
    cleanup() {
        try {
            const result = this.db.prepare(`
        DELETE FROM exit_cooldown
        WHERE cooldown_until < datetime('now')
      `).run();

            if (result.changes > 0) {
                console.log(`   🧹 [Cooldown] 清理了 ${result.changes} 条过期冷却记录`);
            }

            return result.changes;
        } catch (error) {
            console.error('❌ Cleanup cooldown error:', error.message);
            return 0;
        }
    }

    /**
     * 获取当前所有冷却中的 token
     */
    getAllCooldowns(chain = null) {
        try {
            let sql = `
        SELECT 
          token_ca, chain, exit_reason, exit_time, cooldown_until, pnl_percent,
          ROUND((julianday(cooldown_until) - julianday('now')) * 24 * 60) as remaining_minutes
        FROM exit_cooldown
        WHERE cooldown_until > datetime('now')
      `;

            if (chain) {
                sql += ` AND chain = ?`;
                return this.db.prepare(sql).all(chain);
            }

            return this.db.prepare(sql).all();
        } catch (error) {
            console.error('❌ Get all cooldowns error:', error.message);
            return [];
        }
    }

    /**
     * 获取冷却统计
     */
    getStats() {
        try {
            const stats = this.db.prepare(`
        SELECT 
          chain,
          exit_reason,
          COUNT(*) as count,
          AVG(pnl_percent) as avg_pnl
        FROM exit_cooldown
        WHERE cooldown_until > datetime('now')
        GROUP BY chain, exit_reason
      `).all();

            const total = this.db.prepare(`
        SELECT COUNT(*) as total
        FROM exit_cooldown
        WHERE cooldown_until > datetime('now')
      `).get();

            return {
                total_in_cooldown: total?.total || 0,
                by_reason: stats
            };
        } catch (error) {
            console.error('❌ Get cooldown stats error:', error.message);
            return { total_in_cooldown: 0, by_reason: [] };
        }
    }
}

export default ExitCooldownService;
