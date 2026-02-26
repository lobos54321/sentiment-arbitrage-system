/**
 * v7.4 数据库迁移 - 增强 rejected_signals 表
 *
 * 添加回测所需的关键字段:
 * - price_change: 检查时的价格变化
 * - intention_tier: AI 叙事等级
 * - signal_trend_type: 信号趋势类型
 * - base_score: 基础评分
 * - decision_source: 决策来源 (hard_gate_安全检查 vs hard_gate_涨跌幅)
 */

import Database from 'better-sqlite3';
import path from 'path';

const dbPath = path.join(process.cwd(), 'data', 'sentiment_arb.db');
const db = new Database(dbPath);

console.log('═'.repeat(60));
console.log('🔧 v7.4 数据库迁移 - 增强 rejected_signals 表');
console.log('═'.repeat(60));

// 检查当前表结构
const columns = db.prepare("PRAGMA table_info(rejected_signals)").all();
const existingColumns = columns.map(c => c.name);

console.log('\n📋 当前字段:', existingColumns.join(', '));

// 需要添加的新字段
const newColumns = [
    { name: 'price_change', type: 'REAL', description: '检查时的价格变化百分比' },
    { name: 'intention_tier', type: 'TEXT', description: 'AI 叙事等级 (TIER_S/A/B/C)' },
    { name: 'signal_trend_type', type: 'TEXT', description: '信号趋势类型 (ACCELERATING/STABLE/DECAYING)' },
    { name: 'base_score', type: 'REAL', description: '基础评分' },
    { name: 'safety_score', type: 'REAL', description: '安全评分' },
    { name: 'decision_source', type: 'TEXT', description: '决策来源 (batch_ai_advisor/hard_gate/cross_validator)' },
    { name: 'gate_type', type: 'TEXT', description: '门槛类型 (security/price_change/sm_flow)' },
    { name: 'threshold_used', type: 'TEXT', description: '使用的阈值配置 (JSON)' },
    { name: 'market_cap', type: 'REAL', description: '市值' },
    { name: 'age_minutes', type: 'INTEGER', description: '代币年龄(分钟)' }
];

console.log('\n🔄 添加新字段...');

for (const col of newColumns) {
    if (!existingColumns.includes(col.name)) {
        try {
            db.exec(`ALTER TABLE rejected_signals ADD COLUMN ${col.name} ${col.type}`);
            console.log(`   ✅ 添加 ${col.name} (${col.type}) - ${col.description}`);
        } catch (e) {
            if (e.message.includes('duplicate column')) {
                console.log(`   ⏭️ ${col.name} 已存在`);
            } else {
                console.log(`   ❌ 添加 ${col.name} 失败: ${e.message}`);
            }
        }
    } else {
        console.log(`   ⏭️ ${col.name} 已存在`);
    }
}

// 创建新索引
console.log('\n🔄 创建索引...');

const indexes = [
    { name: 'idx_rejected_price_change', columns: 'price_change' },
    { name: 'idx_rejected_intention_tier', columns: 'intention_tier' },
    { name: 'idx_rejected_signal_type', columns: 'signal_trend_type' },
    { name: 'idx_rejected_decision_source', columns: 'decision_source' },
    { name: 'idx_rejected_gate_type', columns: 'gate_type' }
];

for (const idx of indexes) {
    try {
        db.exec(`CREATE INDEX IF NOT EXISTS ${idx.name} ON rejected_signals(${idx.columns})`);
        console.log(`   ✅ 创建索引 ${idx.name}`);
    } catch (e) {
        console.log(`   ⏭️ 索引 ${idx.name} 已存在或创建失败`);
    }
}

// 验证更新后的表结构
console.log('\n📋 更新后的表结构:');
const updatedColumns = db.prepare("PRAGMA table_info(rejected_signals)").all();
updatedColumns.forEach(col => {
    console.log(`   ${col.cid}. ${col.name} (${col.type})`);
});

console.log('\n✅ 迁移完成');

db.close();
