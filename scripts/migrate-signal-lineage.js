#!/usr/bin/env node
/**
 * Migration Script: Add Signal Lineage Fields to positions table
 *
 * v7.4 信号血统追踪系统
 *
 * 新增字段:
 * - signal_source: 信号源 (ultra_sniper_v2, shadow_v1, shadow_v2, telegram, debot, dexscreener, alpha_monitor)
 * - signal_hunter_type: 猎人类型 (FOX, TURTLE, WOLF, EAGLE, BOT, NORMAL)
 * - signal_hunter_addr: 猎人钱包地址
 * - signal_hunter_score: 猎人评分 (0-100)
 * - signal_route: 路由路径 (flash_scout, cross_validator, tiered_observer, direct)
 * - signal_entry_reason: 入场原因 (golden_dog_hunter, swing_trader, consistent_trader...)
 * - signal_confidence: 数据置信度 (direct, inferred)
 *
 * 运行方式:
 *   node scripts/migrate-signal-lineage.js
 */

import Database from 'better-sqlite3';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import dotenv from 'dotenv';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const projectRoot = join(__dirname, '..');

// Use DB_PATH from env, or fallback to ./data
const dbPath = process.env.DB_PATH || join(projectRoot, 'data', 'sentiment_arb.db');

console.log('═'.repeat(70));
console.log('🔄 Signal Lineage Migration v7.4');
console.log('═'.repeat(70));
console.log(`📍 Database: ${dbPath}`);
console.log('');

const db = new Database(dbPath);

// Helper function to check if column exists
function columnExists(tableName, columnName) {
    const columns = db.prepare(`PRAGMA table_info(${tableName})`).all();
    return columns.some(col => col.name === columnName);
}

// Helper function to add column if not exists
function addColumnIfNotExists(tableName, columnName, columnDef) {
    if (columnExists(tableName, columnName)) {
        console.log(`   ⏭️  ${columnName} already exists, skipping`);
        return false;
    }

    try {
        db.prepare(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${columnDef}`).run();
        console.log(`   ✅ Added ${columnName}`);
        return true;
    } catch (error) {
        console.error(`   ❌ Failed to add ${columnName}: ${error.message}`);
        return false;
    }
}

// Helper function to create index if not exists
function createIndexIfNotExists(indexName, tableName, columnName) {
    try {
        db.prepare(`CREATE INDEX IF NOT EXISTS ${indexName} ON ${tableName}(${columnName})`).run();
        console.log(`   ✅ Index ${indexName} ready`);
        return true;
    } catch (error) {
        console.error(`   ❌ Failed to create index ${indexName}: ${error.message}`);
        return false;
    }
}

console.log('📋 Phase 1: Adding signal lineage columns to positions table...');
console.log('');

// Add new columns
const columnsToAdd = [
    { name: 'signal_source', def: 'TEXT' },
    { name: 'signal_hunter_type', def: 'TEXT' },
    { name: 'signal_hunter_addr', def: 'TEXT' },
    { name: 'signal_hunter_score', def: 'REAL' },
    { name: 'signal_route', def: 'TEXT' },
    { name: 'signal_entry_reason', def: 'TEXT' },
    { name: 'signal_confidence', def: "TEXT DEFAULT 'direct'" }
];

let columnsAdded = 0;
for (const col of columnsToAdd) {
    if (addColumnIfNotExists('positions', col.name, col.def)) {
        columnsAdded++;
    }
}

console.log('');
console.log(`📊 Columns added: ${columnsAdded}/${columnsToAdd.length}`);
console.log('');

console.log('📋 Phase 2: Creating indexes for efficient queries...');
console.log('');

// Create indexes
createIndexIfNotExists('idx_positions_signal_source', 'positions', 'signal_source');
createIndexIfNotExists('idx_positions_signal_hunter_type', 'positions', 'signal_hunter_type');
createIndexIfNotExists('idx_positions_signal_confidence', 'positions', 'signal_confidence');
createIndexIfNotExists('idx_positions_signal_route', 'positions', 'signal_route');

console.log('');
console.log('📋 Phase 3: Updating init-db.js schema (manual step required)...');
console.log('');
console.log('   Please manually add these columns to scripts/init-db.js');
console.log('   in the CREATE TABLE positions section:');
console.log('');
console.log('   -- v7.4 Signal Lineage (信号血统追踪)');
console.log('   signal_source TEXT,           -- 信号源');
console.log('   signal_hunter_type TEXT,      -- 猎人类型');
console.log('   signal_hunter_addr TEXT,      -- 猎人钱包');
console.log('   signal_hunter_score REAL,     -- 猎人评分');
console.log('   signal_route TEXT,            -- 路由路径');
console.log('   signal_entry_reason TEXT,     -- 入场原因');
console.log("   signal_confidence TEXT DEFAULT 'direct', -- 数据置信度");
console.log('');

// Verify migration
console.log('📋 Phase 4: Verification...');
console.log('');

const columns = db.prepare('PRAGMA table_info(positions)').all();
const newColumns = columns.filter(col => col.name.startsWith('signal_'));

console.log('   Signal lineage columns in positions table:');
for (const col of newColumns) {
    console.log(`   - ${col.name} (${col.type || 'TEXT'}${col.dflt_value ? `, default: ${col.dflt_value}` : ''})`);
}

console.log('');

// Show sample query
console.log('📋 Sample query to verify signal lineage data:');
console.log('');
console.log(`   SELECT
     signal_source,
     signal_hunter_type,
     COUNT(*) as trades,
     AVG(pnl_percent) as avg_pnl,
     SUM(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as win_rate
   FROM positions
   WHERE status = 'closed'
   AND signal_source IS NOT NULL
   GROUP BY signal_source, signal_hunter_type
   ORDER BY win_rate DESC;`);

console.log('');
console.log('═'.repeat(70));
console.log('✅ Migration complete!');
console.log('═'.repeat(70));
console.log('');
console.log('Next steps:');
console.log('  1. Modify signal emitters to include lineage data');
console.log('  2. Update index.js graduate event to record lineage');
console.log('  3. Run historical data repair script');
console.log('');

db.close();
