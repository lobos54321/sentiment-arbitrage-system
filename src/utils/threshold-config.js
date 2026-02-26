/**
 * 动态阈值配置管理器 v1.0
 * 
 * 功能：
 * 1. 从数据库加载阈值配置
 * 2. 允许 AI 自动复盘更新阈值
 * 3. 记录所有阈值变更历史
 */

import Database from 'better-sqlite3';

class ThresholdConfig {
    constructor(dbPath = './data/sentiment_arb.db') {
        this.db = new Database(dbPath);
        this.initDatabase();
        this.loadFromDatabase();
    }

    /**
     * 初始化数据库表
     */
    initDatabase() {
        this.db.exec(`
      CREATE TABLE IF NOT EXISTS dynamic_thresholds (
        key TEXT PRIMARY KEY,
        value REAL NOT NULL,
        updated_at TEXT DEFAULT (datetime('now')),
        updated_by TEXT DEFAULT 'system'
      );
    `);

        // 插入默认值（如果不存在）
        const defaults = [
            ['sm_density_high', 5],
            ['sm_density_mid', 1],
            ['hype_divergence_stealth', 1.0],
            ['hype_divergence_momentum', 0.2],
            ['narrative_health_strong', 10],
            ['narrative_health_mid', 5],
            ['stop_loss_percent', -50],
            ['time_stop_sol_minutes', 45],
            ['time_stop_bsc_minutes', 120],
            ['position_size_min', 0.1],
            ['position_size_max', 0.5]
        ];

        const insertStmt = this.db.prepare(`
      INSERT OR IGNORE INTO dynamic_thresholds (key, value) VALUES (?, ?)
    `);

        for (const [key, value] of defaults) {
            insertStmt.run(key, value);
        }
    }

    /**
     * 从数据库加载所有阈值
     */
    loadFromDatabase() {
        const rows = this.db.prepare(`SELECT key, value FROM dynamic_thresholds`).all();
        this.thresholds = {};
        for (const row of rows) {
            this.thresholds[row.key] = row.value;
        }
        console.log(`[ThresholdConfig] 已加载 ${rows.length} 个阈值配置`);
    }

    /**
     * 获取阈值
     */
    get(key, defaultValue = 0) {
        return this.thresholds[key] ?? defaultValue;
    }

    /**
     * 更新阈值
     */
    set(key, value, updatedBy = 'system') {
        const oldValue = this.thresholds[key];
        this.thresholds[key] = value;

        this.db.prepare(`
      INSERT OR REPLACE INTO dynamic_thresholds (key, value, updated_at, updated_by)
      VALUES (?, ?, datetime('now'), ?)
    `).run(key, value, updatedBy);

        console.log(`[ThresholdConfig] ${key}: ${oldValue} → ${value} (by ${updatedBy})`);
        return { key, oldValue, newValue: value };
    }

    /**
     * 批量更新阈值（AI 自动复盘使用）
     */
    applyAISuggestions(suggestions, safetyLimit = 0.3) {
        const applied = [];
        const rejected = [];

        for (const s of suggestions) {
            // 映射因子名到数据库 key
            const keyMap = {
                '聪明钱密度': 'sm_density_high',
                '舆论背离度': 'hype_divergence_stealth',
                '叙事健康度': 'narrative_health_strong',
                '止损': 'stop_loss_percent',
                '仓位': 'position_size_min',
                '时间止损': 'time_stop_sol_minutes'
            };

            const key = keyMap[s.factor];
            if (!key) {
                console.log(`[ThresholdConfig] 跳过未知因子: ${s.factor}`);
                continue;
            }

            const currentValue = this.get(key);
            // 解析数值（处理 ">=4" 或 ">=0.8" 这样的格式）
            let suggestedValue = s.suggested;
            if (typeof suggestedValue === 'string') {
                // 移除 >=, <=, >, < 等前缀，只保留数字
                const numMatch = suggestedValue.match(/[-]?[\d.]+/);
                suggestedValue = numMatch ? parseFloat(numMatch[0]) : suggestedValue;
            }

            // 安全检查：变化幅度不能超过 safetyLimit (30%)
            if (typeof currentValue === 'number' && typeof suggestedValue === 'number') {
                const changePercent = Math.abs(suggestedValue - currentValue) / Math.abs(currentValue);
                if (changePercent > safetyLimit) {
                    console.log(`[ThresholdConfig] ⚠️ 拒绝 ${s.factor}: 变化幅度 ${(changePercent * 100).toFixed(0)}% 超过安全限制 ${safetyLimit * 100}%`);
                    rejected.push({ ...s, reason: `变化幅度超过${safetyLimit * 100}%限制` });
                    continue;
                }
            }

            // 应用更新
            const result = this.set(key, suggestedValue, 'AI_AUTO_REVIEW');
            applied.push({ ...s, ...result });
        }

        return { applied, rejected };
    }

    /**
     * 获取所有阈值（用于显示）
     */
    getAll() {
        return { ...this.thresholds };
    }
}

// 单例导出
const thresholdConfig = new ThresholdConfig();

export default thresholdConfig;
export { ThresholdConfig };
