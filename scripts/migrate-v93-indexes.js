/**
 * v9.3 数据库索引优化
 *
 * 为回测查询添加必要的索引
 */

import Database from 'better-sqlite3';
import path from 'path';

const dbPath = path.join(process.cwd(), 'data', 'sentiment_arb.db');
const db = new Database(dbPath);

console.log('═'.repeat(60));
console.log('🔧 v9.3 数据库索引优化');
console.log('═'.repeat(60));

// 需要创建的索引
const indexes = [
    // rejected_signals 表索引
    {
        name: 'idx_rejected_backtest',
        table: 'rejected_signals',
        columns: 'token_ca, created_at',
        description: '回测查询优化'
    },
    {
        name: 'idx_rejected_gate_created',
        table: 'rejected_signals',
        columns: 'gate_type, created_at',
        description: '按门槛类型查询'
    },
    {
        name: 'idx_rejected_tier_type',
        table: 'rejected_signals',
        columns: 'intention_tier, signal_trend_type',
        description: '叙事+信号类型查询'
    },

    // passed_signals 表索引
    {
        name: 'idx_passed_backtest',
        table: 'passed_signals',
        columns: 'token_ca, created_at',
        description: '回测查询优化'
    },
    {
        name: 'idx_passed_tier',
        table: 'passed_signals',
        columns: 'intention_tier, created_at',
        description: '按叙事等级查询'
    },

    // watch_signals 表索引
    {
        name: 'idx_watch_backtest',
        table: 'watch_signals',
        columns: 'token_ca, created_at',
        description: '回测查询优化'
    },
    {
        name: 'idx_watch_tracked',
        table: 'watch_signals',
        columns: 'tracked, created_at',
        description: '追踪状态查询'
    },

    // positions 表索引
    {
        name: 'idx_positions_status',
        table: 'positions',
        columns: 'status, created_at',
        description: '持仓状态查询'
    },
    {
        name: 'idx_positions_token',
        table: 'positions',
        columns: 'token_ca, status',
        description: '代币持仓查询'
    },

    // trades 表索引
    {
        name: 'idx_trades_position',
        table: 'trades',
        columns: 'position_id, trade_type',
        description: '交易记录查询'
    },
    {
        name: 'idx_trades_token_time',
        table: 'trades',
        columns: 'token_ca, created_at',
        description: '代币交易历史'
    }
];

console.log('\n🔄 创建索引...\n');

let created = 0;
let skipped = 0;
let failed = 0;

for (const idx of indexes) {
    try {
        // 先检查表是否存在
        const tableExists = db.prepare(`
            SELECT name FROM sqlite_master
            WHERE type='table' AND name=?
        `).get(idx.table);

        if (!tableExists) {
            console.log(`   ⏭️ 跳过 ${idx.name}: 表 ${idx.table} 不存在`);
            skipped++;
            continue;
        }

        // 检查索引是否已存在
        const indexExists = db.prepare(`
            SELECT name FROM sqlite_master
            WHERE type='index' AND name=?
        `).get(idx.name);

        if (indexExists) {
            console.log(`   ⏭️ ${idx.name} 已存在`);
            skipped++;
            continue;
        }

        // 创建索引
        db.exec(`CREATE INDEX ${idx.name} ON ${idx.table}(${idx.columns})`);
        console.log(`   ✅ 创建 ${idx.name} ON ${idx.table}(${idx.columns})`);
        console.log(`      用途: ${idx.description}`);
        created++;
    } catch (e) {
        console.log(`   ❌ 创建 ${idx.name} 失败: ${e.message}`);
        failed++;
    }
}

console.log('\n' + '─'.repeat(60));
console.log(`📊 结果: 创建 ${created} 个, 跳过 ${skipped} 个, 失败 ${failed} 个`);

// 分析数据库大小
try {
    const pageCount = db.prepare('PRAGMA page_count').get();
    const pageSize = db.prepare('PRAGMA page_size').get();
    const dbSize = (pageCount.page_count * pageSize.page_size / 1024 / 1024).toFixed(2);
    console.log(`💾 数据库大小: ${dbSize} MB`);
} catch (e) {
    // 忽略
}

console.log('\n✅ 索引优化完成');

db.close();
