#!/usr/bin/env node
/**
 * Historical Signal Lineage Repair Script v7.4
 *
 * 修复历史仓位数据，推断信号血统信息
 *
 * 推断规则:
 * - 如果有 signal_source 字段，保持不变
 * - 根据现有字段推断 signal_source (如 alpha_tier, fast_track_type 等)
 * - 标记 signal_confidence = 'inferred'
 *
 * 运行方式:
 *   node scripts/repair-signal-lineage.js
 *   node scripts/repair-signal-lineage.js --dry-run  # 只显示将要修改的内容
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

// 解析命令行参数
const isDryRun = process.argv.includes('--dry-run');

console.log('═'.repeat(70));
console.log('🔧 Signal Lineage Historical Repair Script v7.4');
console.log('═'.repeat(70));
console.log(`📍 Database: ${dbPath}`);
console.log(`🏃 Mode: ${isDryRun ? 'DRY RUN (不实际修改)' : 'LIVE (将修改数据库)'}`);
console.log('');

const db = new Database(dbPath);

// ═══════════════════════════════════════════════════════════════
// 推断规则
// ═══════════════════════════════════════════════════════════════

/**
 * 根据现有数据推断 signal_source
 */
function inferSignalSource(position) {
    // 已经有 source 的情况
    if (position.signal_source) {
        return position.signal_source;
    }

    // 根据 alpha_tier 推断
    if (position.alpha_tier) {
        if (position.alpha_tier.includes('SNIPER') || position.alpha_tier.includes('ULTRA')) {
            return 'ultra_sniper_v2';
        }
        if (position.alpha_tier.includes('SHADOW')) {
            return 'shadow_v2';
        }
    }

    // 根据 fast_track_type 推断
    if (position.fast_track_type) {
        return 'flash_scout';
    }

    // 根据 signal_id 是否存在推断 (有 signal_id 说明来自 telegram)
    if (position.signal_id) {
        return 'telegram';
    }

    // 默认认为是通过传统观察室
    return 'tiered_observer';
}

/**
 * 根据现有数据推断 hunter_type
 */
function inferHunterType(position) {
    if (position.signal_hunter_type) {
        return position.signal_hunter_type;
    }

    // 根据 alpha_tier 推断
    if (position.alpha_tier) {
        const tier = position.alpha_tier.toUpperCase();
        if (tier.includes('FOX')) return 'FOX';
        if (tier.includes('TURTLE')) return 'TURTLE';
        if (tier.includes('WOLF')) return 'WOLF';
        if (tier.includes('EAGLE')) return 'EAGLE';
    }

    return null;
}

/**
 * 根据现有数据推断 route
 */
function inferRoute(position) {
    if (position.signal_route) {
        return position.signal_route;
    }

    const source = inferSignalSource(position);

    switch (source) {
        case 'ultra_sniper_v2':
            return 'flash_scout';
        case 'shadow_v2':
            return 'cross_validator';
        case 'flash_scout':
            return 'flash_scout';
        case 'telegram':
            return 'tiered_observer';
        default:
            return 'tiered_observer';
    }
}

/**
 * 根据现有数据推断 entry_reason
 */
function inferEntryReason(position) {
    if (position.signal_entry_reason) {
        return position.signal_entry_reason;
    }

    const hunterType = inferHunterType(position);

    if (hunterType) {
        switch (hunterType) {
            case 'FOX': return 'golden_dog_hunter';
            case 'TURTLE': return 'swing_trader';
            case 'WOLF': return 'consistent_trader';
            case 'EAGLE': return 'precision_sniper';
            default: return 'inferred_entry';
        }
    }

    // 根据 intention_tier 推断
    if (position.intention_tier) {
        switch (position.intention_tier) {
            case 'TIER_S': return 's_grade_consensus';
            case 'TIER_A': return 'dual_validation';
            case 'TIER_B': return 'early_bird';
            default: return 'cross_validated';
        }
    }

    return 'inferred_entry';
}

// ═══════════════════════════════════════════════════════════════
// 主修复逻辑
// ═══════════════════════════════════════════════════════════════

console.log('📋 Phase 1: 查询需要修复的仓位...');
console.log('');

// 查询所有没有完整 signal_lineage 的仓位
const positionsToRepair = db.prepare(`
    SELECT
        id,
        token_ca,
        symbol,
        chain,
        alpha_tier,
        fast_track_type,
        intention_tier,
        signal_id,
        signal_source,
        signal_hunter_type,
        signal_hunter_addr,
        signal_hunter_score,
        signal_route,
        signal_entry_reason,
        signal_confidence,
        status,
        created_at
    FROM positions
    WHERE signal_source IS NULL
       OR signal_confidence IS NULL
       OR signal_confidence != 'direct'
    ORDER BY created_at DESC
`).all();

console.log(`   找到 ${positionsToRepair.length} 个需要修复的仓位`);
console.log('');

if (positionsToRepair.length === 0) {
    console.log('✅ 所有仓位都已有完整的信号血统数据，无需修复');
    db.close();
    process.exit(0);
}

console.log('📋 Phase 2: 推断信号血统数据...');
console.log('');

// 准备更新语句
const updateStmt = db.prepare(`
    UPDATE positions
    SET
        signal_source = ?,
        signal_hunter_type = ?,
        signal_route = ?,
        signal_entry_reason = ?,
        signal_confidence = 'inferred',
        updated_at = strftime('%s', 'now')
    WHERE id = ?
`);

let repairCount = 0;
const repairs = [];

for (const position of positionsToRepair) {
    const inferredSource = inferSignalSource(position);
    const inferredHunterType = inferHunterType(position);
    const inferredRoute = inferRoute(position);
    const inferredEntryReason = inferEntryReason(position);

    // 只有在有变化时才更新
    const hasChanges =
        position.signal_source !== inferredSource ||
        position.signal_hunter_type !== inferredHunterType ||
        position.signal_route !== inferredRoute ||
        position.signal_entry_reason !== inferredEntryReason ||
        position.signal_confidence !== 'inferred';

    if (hasChanges) {
        repairs.push({
            id: position.id,
            symbol: position.symbol,
            chain: position.chain,
            status: position.status,
            original: {
                source: position.signal_source,
                hunterType: position.signal_hunter_type,
                route: position.signal_route,
                entryReason: position.signal_entry_reason,
                confidence: position.signal_confidence
            },
            inferred: {
                source: inferredSource,
                hunterType: inferredHunterType,
                route: inferredRoute,
                entryReason: inferredEntryReason,
                confidence: 'inferred'
            }
        });

        if (!isDryRun) {
            updateStmt.run(
                inferredSource,
                inferredHunterType,
                inferredRoute,
                inferredEntryReason,
                position.id
            );
        }

        repairCount++;
    }
}

// 显示修复详情
console.log(`   推断完成，${repairCount} 个仓位将被修复:`);
console.log('');

// 按 source 分组统计
const sourceStats = {};
for (const repair of repairs) {
    const source = repair.inferred.source;
    sourceStats[source] = (sourceStats[source] || 0) + 1;
}

console.log('   按信号源统计:');
for (const [source, count] of Object.entries(sourceStats)) {
    console.log(`     - ${source}: ${count} 个`);
}
console.log('');

// 显示前 10 个修复示例
console.log('   修复示例 (前 10 个):');
for (let i = 0; i < Math.min(10, repairs.length); i++) {
    const r = repairs[i];
    console.log(`     [${r.id}] ${r.symbol} (${r.chain}): ${r.original.source || 'NULL'} → ${r.inferred.source}`);
}
if (repairs.length > 10) {
    console.log(`     ... 还有 ${repairs.length - 10} 个`);
}
console.log('');

// ═══════════════════════════════════════════════════════════════
// 验证
// ═══════════════════════════════════════════════════════════════

if (!isDryRun) {
    console.log('📋 Phase 3: 验证修复结果...');
    console.log('');

    const verifyResult = db.prepare(`
        SELECT
            signal_source,
            signal_confidence,
            COUNT(*) as count
        FROM positions
        WHERE signal_source IS NOT NULL
        GROUP BY signal_source, signal_confidence
        ORDER BY count DESC
    `).all();

    console.log('   修复后的信号源分布:');
    for (const row of verifyResult) {
        console.log(`     - ${row.signal_source} (${row.signal_confidence}): ${row.count} 个`);
    }
    console.log('');

    // 检查还有多少未修复的
    const remaining = db.prepare(`
        SELECT COUNT(*) as count
        FROM positions
        WHERE signal_source IS NULL
    `).get();

    console.log(`   剩余未修复: ${remaining.count} 个`);
}

console.log('');
console.log('═'.repeat(70));
if (isDryRun) {
    console.log('🏃 DRY RUN 完成 - 以上显示的是将要修改的内容');
    console.log('   运行 `node scripts/repair-signal-lineage.js` (不带 --dry-run) 以实际执行修复');
} else {
    console.log(`✅ 修复完成！共修复 ${repairCount} 个仓位`);
}
console.log('═'.repeat(70));
console.log('');

db.close();
