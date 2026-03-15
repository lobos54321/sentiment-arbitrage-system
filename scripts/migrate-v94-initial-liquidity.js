/**
 * v9.4 数据库迁移 — initial_liquidity + deployer 字段
 *
 * 为回溯分析添加以下字段到 gates 表:
 * - initial_liquidity_usd: 信号首次发现时的 USD 流动性
 * - deployer_address: 合约部署者地址
 * - deployer_balance: 部署者当前持有代币余额 (%)
 *
 * 使用场景:
 * - 流动性崩溃检测 (exit 时 liquidity < 50% of initial)
 * - 部署者抛售预警
 * - 回溯分析和风控优化
 */

import Database from 'better-sqlite3';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const dbPath = path.join(__dirname, '..', 'data', 'sentiment_arb.db');

console.log('═'.repeat(60));
console.log('🔧 v9.4 数据库迁移 — initial_liquidity + deployer 字段');
console.log('═'.repeat(60));

let db;
try {
  db = new Database(dbPath);
} catch (error) {
  console.error(`❌ 无法打开数据库: ${dbPath}`);
  console.error(`   错误: ${error.message}`);
  console.log('\n💡 提示: 请先运行 npm run db:init 初始化数据库');
  process.exit(1);
}

const migrations = [
  {
    name: 'gates.initial_liquidity_usd',
    sql: 'ALTER TABLE gates ADD COLUMN initial_liquidity_usd REAL',
    description: '信号首次发现时的 USD 流动性（用于检测流动性崩溃）'
  },
  {
    name: 'gates.deployer_address',
    sql: 'ALTER TABLE gates ADD COLUMN deployer_address TEXT',
    description: '合约部署者钱包地址'
  },
  {
    name: 'gates.deployer_balance_pct',
    sql: 'ALTER TABLE gates ADD COLUMN deployer_balance_pct REAL',
    description: '部署者持有代币百分比（部署者抛售预警用）'
  }
];

let successCount = 0;
let skipCount = 0;
let errorCount = 0;

for (const migration of migrations) {
  try {
    db.exec(migration.sql);
    console.log(`✅ 添加列: ${migration.name}`);
    console.log(`   ${migration.description}`);
    successCount++;
  } catch (error) {
    if (error.message.includes('duplicate column name') || error.message.includes('already exists')) {
      console.log(`⏭️  跳过 (已存在): ${migration.name}`);
      skipCount++;
    } else {
      console.error(`❌ 迁移失败: ${migration.name}`);
      console.error(`   ${error.message}`);
      errorCount++;
    }
  }
}

// 创建索引以优化回溯查询
const indexes = [
  {
    name: 'idx_gates_initial_liquidity',
    sql: 'CREATE INDEX IF NOT EXISTS idx_gates_initial_liquidity ON gates(initial_liquidity_usd)',
    description: '流动性范围查询优化'
  },
  {
    name: 'idx_gates_deployer',
    sql: 'CREATE INDEX IF NOT EXISTS idx_gates_deployer ON gates(deployer_address)',
    description: '部署者地址查询优化'
  }
];

console.log('\n📊 创建索引...');
for (const idx of indexes) {
  try {
    db.exec(idx.sql);
    console.log(`✅ 索引: ${idx.name} — ${idx.description}`);
    successCount++;
  } catch (error) {
    console.error(`❌ 索引创建失败: ${idx.name}: ${error.message}`);
    errorCount++;
  }
}

db.close();

console.log('\n' + '═'.repeat(60));
console.log(`📊 迁移结果:`);
console.log(`   成功: ${successCount}`);
console.log(`   跳过: ${skipCount}`);
console.log(`   失败: ${errorCount}`);

if (errorCount > 0) {
  console.log('\n⚠️  有迁移失败，请检查上面的错误信息');
  process.exit(1);
} else {
  console.log('\n✅ v9.4 迁移完成！');
  console.log('   gates 表现在支持 initial_liquidity_usd + deployer 字段');
  console.log('   系统重启后将自动采集这些数据');
}
