#!/usr/bin/env node
/**
 * v6.7 Database Migration
 * Adds intention_tier and exit_price columns to positions table
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

console.log('🔄 Running v6.7 Migration...');
console.log(`📍 Database: ${dbPath}`);

// Check existing columns
const tableInfo = db.prepare("PRAGMA table_info(positions)").all();
const existingColumns = tableInfo.map(col => col.name);

const migrations = [];

// Add is_shadow if missing
if (!existingColumns.includes('is_shadow')) {
  migrations.push({
    column: 'is_shadow',
    sql: "ALTER TABLE positions ADD COLUMN is_shadow INTEGER DEFAULT 0"
  });
}

// Add alpha_tier if missing
if (!existingColumns.includes('alpha_tier')) {
  migrations.push({
    column: 'alpha_tier',
    sql: "ALTER TABLE positions ADD COLUMN alpha_tier TEXT"
  });
}

// Add fast_track_type if missing
if (!existingColumns.includes('fast_track_type')) {
  migrations.push({
    column: 'fast_track_type',
    sql: "ALTER TABLE positions ADD COLUMN fast_track_type TEXT"
  });
}

// Add intention_tier if missing
if (!existingColumns.includes('intention_tier')) {
  migrations.push({
    column: 'intention_tier',
    sql: "ALTER TABLE positions ADD COLUMN intention_tier TEXT"
  });
}

// Add exit_price if missing
if (!existingColumns.includes('exit_price')) {
  migrations.push({
    column: 'exit_price',
    sql: "ALTER TABLE positions ADD COLUMN exit_price REAL"
  });
}

// Run migrations
if (migrations.length === 0) {
  console.log('✅ No migrations needed - all columns exist');
} else {
  console.log(`📦 Running ${migrations.length} migration(s)...`);

  for (const migration of migrations) {
    try {
      db.exec(migration.sql);
      console.log(`   ✅ Added column: ${migration.column}`);
    } catch (error) {
      if (error.message.includes('duplicate column')) {
        console.log(`   ⏭️  Column already exists: ${migration.column}`);
      } else {
        console.error(`   ❌ Failed to add ${migration.column}:`, error.message);
      }
    }
  }
}

// Create index for intention_tier
try {
  db.exec("CREATE INDEX IF NOT EXISTS idx_positions_intention_tier ON positions(intention_tier)");
  console.log('   ✅ Created index: idx_positions_intention_tier');
} catch (error) {
  console.log('   ⏭️  Index already exists');
}

db.close();

console.log('\n🎉 v6.7 Migration complete!');
console.log('\nNew columns added:');
console.log('  - intention_tier: AI评估的叙事立意等级 (TIER_S/A/B/C)');
console.log('  - exit_price: AI预测的目标退出价格');
console.log('\n止盈策略:');
console.log('  TIER_S: 大叙事 → 可死拿 10x-20x');
console.log('  TIER_A: 优质叙事 → 可持 3x-5x');
console.log('  TIER_B: 普通叙事 → 赚 50%-2x 就跑');
console.log('  TIER_C: 垃圾叙事 → 保守止盈 30%-1x');
