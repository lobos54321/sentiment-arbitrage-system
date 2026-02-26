#!/usr/bin/env node
/**
 * v7.3 數據庫遷移腳本
 *
 * 功能：
 * 1. 為 positions 表添加歸因字段
 * 2. 創建 rejected_signals 表
 * 3. 創建 module_performance 表
 * 4. 為現有數據填充默認值
 *
 * 運行方式: node scripts/migrate-v73.js
 */

import Database from 'better-sqlite3';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import dotenv from 'dotenv';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const projectRoot = join(__dirname, '..');

const dbPath = process.env.DB_PATH || join(projectRoot, 'data', 'sentiment_arb.db');
const db = new Database(dbPath);

console.log('═'.repeat(60));
console.log('🔄 v7.3 數據庫遷移');
console.log('═'.repeat(60));
console.log(`📍 數據庫: ${dbPath}`);
console.log();

// 開啟事務
db.exec('BEGIN TRANSACTION');

try {
    // ============================================
    // 任務 1.1: 擴展 positions 表
    // ============================================
    console.log('📋 任務 1.1: 擴展 positions 表結構...');

    // 檢查字段是否已存在的輔助函數
    const columnExists = (table, column) => {
        const info = db.prepare(`PRAGMA table_info(${table})`).all();
        return info.some(col => col.name === column);
    };

    // 添加 entry_source 字段
    if (!columnExists('positions', 'entry_source')) {
        db.exec(`ALTER TABLE positions ADD COLUMN entry_source TEXT`);
        console.log('   ✅ 添加 entry_source');
    } else {
        console.log('   ⏭️ entry_source 已存在');
    }

    // 添加 ai_narrative_score 字段
    if (!columnExists('positions', 'ai_narrative_score')) {
        db.exec(`ALTER TABLE positions ADD COLUMN ai_narrative_score REAL`);
        console.log('   ✅ 添加 ai_narrative_score');
    } else {
        console.log('   ⏭️ ai_narrative_score 已存在');
    }

    // 添加 ai_exit_used 字段
    if (!columnExists('positions', 'ai_exit_used')) {
        db.exec(`ALTER TABLE positions ADD COLUMN ai_exit_used INTEGER DEFAULT 0`);
        console.log('   ✅ 添加 ai_exit_used');
    } else {
        console.log('   ⏭️ ai_exit_used 已存在');
    }

    // 添加 decision_factors 字段
    if (!columnExists('positions', 'decision_factors')) {
        db.exec(`ALTER TABLE positions ADD COLUMN decision_factors TEXT`);
        console.log('   ✅ 添加 decision_factors');
    } else {
        console.log('   ⏭️ decision_factors 已存在');
    }

    // 添加 experiment_group 字段
    if (!columnExists('positions', 'experiment_group')) {
        db.exec(`ALTER TABLE positions ADD COLUMN experiment_group TEXT`);
        console.log('   ✅ 添加 experiment_group');
    } else {
        console.log('   ⏭️ experiment_group 已存在');
    }

    // 添加 market_condition 字段
    if (!columnExists('positions', 'market_condition')) {
        db.exec(`ALTER TABLE positions ADD COLUMN market_condition TEXT`);
        console.log('   ✅ 添加 market_condition');
    } else {
        console.log('   ⏭️ market_condition 已存在');
    }

    // 添加 entry_timestamp_ms 字段
    if (!columnExists('positions', 'entry_timestamp_ms')) {
        db.exec(`ALTER TABLE positions ADD COLUMN entry_timestamp_ms INTEGER`);
        console.log('   ✅ 添加 entry_timestamp_ms');
    } else {
        console.log('   ⏭️ entry_timestamp_ms 已存在');
    }

    // 添加 exit_pnl_percent 字段 (如果不存在，供分析腳本使用)
    if (!columnExists('positions', 'exit_pnl_percent')) {
        db.exec(`ALTER TABLE positions ADD COLUMN exit_pnl_percent REAL`);
        console.log('   ✅ 添加 exit_pnl_percent');
    } else {
        console.log('   ⏭️ exit_pnl_percent 已存在');
    }

    // ============================================
    // 任務 1.2: 創建 rejected_signals 表
    // ============================================
    console.log('\n📋 任務 1.2: 創建 rejected_signals 表...');

    db.exec(`
        CREATE TABLE IF NOT EXISTS rejected_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token_ca TEXT NOT NULL,
            chain TEXT NOT NULL,
            symbol TEXT,

            -- 信號來源
            signal_source TEXT,

            -- 拒絕原因
            rejection_stage TEXT,
            rejection_reason TEXT,
            rejection_factors TEXT,

            -- 被拒絕時的市場數據
            price_at_rejection REAL,
            mcap_at_rejection REAL,
            liquidity_at_rejection REAL,
            sm_count_at_rejection INTEGER,

            -- 後續追蹤
            price_1h_later REAL,
            price_24h_later REAL,
            max_price_24h REAL,
            min_price_24h REAL,
            would_have_profit REAL,
            tracking_completed INTEGER DEFAULT 0,

            -- 時間戳
            created_at TEXT DEFAULT (datetime('now')),
            tracked_at TEXT
        )
    `);
    console.log('   ✅ rejected_signals 表創建完成');

    // 創建索引
    db.exec(`
        CREATE INDEX IF NOT EXISTS idx_rejected_stage ON rejected_signals(rejection_stage);
        CREATE INDEX IF NOT EXISTS idx_rejected_tracking ON rejected_signals(tracking_completed);
        CREATE INDEX IF NOT EXISTS idx_rejected_token ON rejected_signals(token_ca);
        CREATE INDEX IF NOT EXISTS idx_rejected_created ON rejected_signals(created_at);
    `);
    console.log('   ✅ rejected_signals 索引創建完成');

    // ============================================
    // 任務 1.3: 創建 module_performance 表
    // ============================================
    console.log('\n📋 任務 1.3: 創建 module_performance 表...');

    db.exec(`
        CREATE TABLE IF NOT EXISTS module_performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_date TEXT NOT NULL,
            window_days INTEGER NOT NULL,

            module_name TEXT NOT NULL,

            -- 績效指標
            total_trades INTEGER,
            win_count INTEGER,
            loss_count INTEGER,
            win_rate REAL,
            total_pnl REAL,
            avg_pnl REAL,
            avg_win REAL,
            avg_loss REAL,
            profit_factor REAL,
            max_drawdown REAL,

            -- 對比基準
            baseline_win_rate REAL,
            baseline_avg_pnl REAL,
            relative_performance REAL,

            -- 統計顯著性
            sample_size_sufficient INTEGER,

            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE(snapshot_date, window_days, module_name)
        )
    `);
    console.log('   ✅ module_performance 表創建完成');

    // 創建索引
    db.exec(`
        CREATE INDEX IF NOT EXISTS idx_module_perf_date ON module_performance(snapshot_date);
        CREATE INDEX IF NOT EXISTS idx_module_perf_name ON module_performance(module_name);
    `);
    console.log('   ✅ module_performance 索引創建完成');

    // ============================================
    // 填充現有數據的默認值
    // ============================================
    console.log('\n📋 填充現有數據默認值...');

    // 為現有 positions 填充 entry_source
    const updateResult = db.prepare(`
        UPDATE positions
        SET entry_source = CASE
            WHEN fast_track_type IS NOT NULL THEN fast_track_type
            WHEN alpha_tier IS NOT NULL THEN 'alpha_monitor'
            ELSE 'waiting_room'
        END
        WHERE entry_source IS NULL
    `).run();
    console.log(`   ✅ 更新了 ${updateResult.changes} 條 positions 記錄的 entry_source`);

    // 填充 exit_pnl_percent (從 pnl_percent 複製)
    const pnlUpdate = db.prepare(`
        UPDATE positions
        SET exit_pnl_percent = pnl_percent
        WHERE exit_pnl_percent IS NULL AND pnl_percent IS NOT NULL
    `).run();
    console.log(`   ✅ 更新了 ${pnlUpdate.changes} 條 positions 記錄的 exit_pnl_percent`);

    // 提交事務
    db.exec('COMMIT');

    console.log('\n' + '═'.repeat(60));
    console.log('✅ v7.3 數據庫遷移完成！');
    console.log('═'.repeat(60));

    // 顯示統計
    const posCount = db.prepare('SELECT COUNT(*) as count FROM positions').get();
    const rejCount = db.prepare('SELECT COUNT(*) as count FROM rejected_signals').get();
    const modCount = db.prepare('SELECT COUNT(*) as count FROM module_performance').get();

    console.log('\n📊 表統計:');
    console.log(`   positions: ${posCount.count} 條記錄`);
    console.log(`   rejected_signals: ${rejCount.count} 條記錄`);
    console.log(`   module_performance: ${modCount.count} 條記錄`);

} catch (error) {
    // 回滾事務
    db.exec('ROLLBACK');
    console.error('\n❌ 遷移失敗，已回滾:', error.message);
    process.exit(1);
} finally {
    db.close();
}
