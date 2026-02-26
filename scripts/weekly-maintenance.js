#!/usr/bin/env node
/**
 * Weekly Maintenance Script - Auto-runs all weekly tasks
 * 
 * This script should be run automatically via cron every week
 * 
 * Tasks:
 * 1. Update narrative weights (AI reassessment)
 * 2. Discover new KOLs and channels
 * 3. Auto-update monitoring list
 * 4. Optimize signal sources (keep TOP 10)
 * 5. Clean up old data
 * 
 * Setup cron (run every Sunday at 00:00):
 *   0 0 * * 0 cd /path/to/sentiment-arbitrage-system && node scripts/weekly-maintenance.js >> logs/weekly.log 2>&1
 */

import dotenv from 'dotenv';
import Database from 'better-sqlite3';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import fs from 'fs';
import { AINarrativeSystem } from '../src/scoring/ai-narrative-system.js';
import { AIInfluencerSystem } from '../src/scoring/ai-influencer-system.js';
import { SignalSourceOptimizer } from '../src/scoring/signal-source-optimizer.js';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const projectRoot = join(__dirname, '..');

// Ensure logs directory exists
const logsDir = join(projectRoot, 'logs');
if (!fs.existsSync(logsDir)) {
  fs.mkdirSync(logsDir, { recursive: true });
}

async function main() {
  const startTime = Date.now();
  console.log('═'.repeat(60));
  console.log(`🔄 WEEKLY MAINTENANCE - ${new Date().toISOString()}`);
  console.log('═'.repeat(60));

  // Connect to database
  const dbPath = process.env.DB_PATH || join(projectRoot, 'data', 'sentiment_arb.db');
  const db = new Database(dbPath);

  try {
    // ========================================
    // TASK 1: Update Narratives
    // ========================================
    console.log('\n📖 [1/3] Updating narrative weights...');
    const aiNarrative = new AINarrativeSystem({}, db);
    
    const narrativesBefore = aiNarrative.getRankedNarratives();
    await aiNarrative.weeklyNarrativeUpdate();
    const narrativesAfter = aiNarrative.getRankedNarratives();
    
    console.log(`   ✅ Narratives updated: ${narrativesAfter.length} total`);
    
    // Show significant changes
    for (const after of narrativesAfter) {
      const before = narrativesBefore.find(b => b.name === after.name);
      if (before) {
        const change = after.weight - before.weight;
        if (Math.abs(change) > 0.5) {
          const arrow = change > 0 ? '📈' : '📉';
          console.log(`   ${arrow} ${after.name}: ${before.weight.toFixed(1)} → ${after.weight.toFixed(1)}`);
        }
      } else {
        console.log(`   ✨ NEW: ${after.name} (${after.weight.toFixed(1)})`);
      }
    }

    // ========================================
    // TASK 2: Discover Influencers
    // ========================================
    console.log('\n👥 [2/4] Discovering new influencers...');
    const aiInfluencer = new AIInfluencerSystem({}, db);
    
    const kolsBefore = aiInfluencer.getRankedKOLs().length;
    const channelsBefore = aiInfluencer.getRankedChannels().length;
    
    await aiInfluencer.weeklyDiscovery();
    
    const kolsAfter = aiInfluencer.getRankedKOLs().length;
    const channelsAfter = aiInfluencer.getRankedChannels().length;
    
    console.log(`   ✅ KOLs: ${kolsBefore} → ${kolsAfter} (+${kolsAfter - kolsBefore})`);
    console.log(`   ✅ Channels: ${channelsBefore} → ${channelsAfter} (+${channelsAfter - channelsBefore})`);

    // ========================================
    // TASK 3: Auto-update monitoring list
    // ========================================
    console.log('\n📱 [3/4] Updating channel monitoring list...');
    
    // Auto-add high quality channels to monitoring
    const addedToMonitoring = db.prepare(`
      UPDATE ai_telegram_channels 
      SET is_monitored = 1 
      WHERE (tier = 'A' OR (tier = 'B' AND influence_score >= 7))
        AND is_monitored = 0
    `).run();
    
    console.log(`   ✅ New channels added to AI monitoring: ${addedToMonitoring.changes}`);
    
    // Sync to telegram_channels table (the actual monitoring table)
    const monitoredChannels = db.prepare(`
      SELECT username, display_name, tier 
      FROM ai_telegram_channels 
      WHERE is_monitored = 1
    `).all();
    
    const insertChannelStmt = db.prepare(`
      INSERT OR IGNORE INTO telegram_channels (channel_name, channel_username, tier, active)
      VALUES (?, ?, ?, 1)
    `);
    
    let syncedCount = 0;
    for (const ch of monitoredChannels) {
      const r = insertChannelStmt.run(
        ch.display_name || ch.username.replace('@', ''), 
        ch.username, 
        ch.tier
      );
      if (r.changes > 0) syncedCount++;
    }
    
    console.log(`   ✅ Synced to monitoring list: ${syncedCount} new channels`);
    
    // Show total monitored
    const totalMonitored = db.prepare('SELECT COUNT(*) as count FROM telegram_channels WHERE active = 1').get();
    console.log(`   📊 Total monitored channels: ${totalMonitored.count}`);

    // ========================================
    // TASK 4: Optimize Signal Sources (CORE)
    // ========================================
    console.log('\n🎯 [4/5] Optimizing signal sources (keep TOP 10)...');
    
    const sourceOptimizer = new SignalSourceOptimizer({ max_active_sources: 10 }, db);
    
    // Run daily evaluation first
    sourceOptimizer.dailyEvaluation();
    
    // Then run weekly optimization
    sourceOptimizer.weeklyOptimization();
    
    // Show stats
    const stats = sourceOptimizer.getStats();
    console.log(`\n   📊 Signal Source Stats:`);
    for (const s of stats.byStatus) {
      console.log(`      ${s.status}: ${s.count} sources, avg quality: ${s.avg_quality?.toFixed(0) || 'N/A'}, win rate: ${((s.avg_win_rate || 0) * 100).toFixed(0)}%`);
    }

    // ========================================
    // TASK 5: Cleanup old data
    // ========================================
    console.log('\n🧹 [5/5] Cleaning up old data...');
    
    const now = Math.floor(Date.now() / 1000);
    
    // 1. Delete telegram_signals older than 7 days
    const sevenDaysAgo = now - 7 * 24 * 60 * 60;
    const deletedSignals = db.prepare(`
      DELETE FROM telegram_signals WHERE created_at < ?
    `).run(sevenDaysAgo);
    console.log(`   ✅ Deleted ${deletedSignals.changes} old signals (>7 days)`);
    
    // 2. Delete shadow_price_tracking older than 30 days
    const thirtyDaysAgo = now - 30 * 24 * 60 * 60;
    try {
      const deletedTracking = db.prepare(`
        DELETE FROM shadow_price_tracking WHERE created_at < ?
      `).run(thirtyDaysAgo);
      console.log(`   ✅ Deleted ${deletedTracking.changes} old tracking records (>30 days)`);
    } catch (e) {
      // Table might not exist
    }
    
    // 3. Delete signal_outcomes older than 30 days (keep aggregated stats)
    try {
      const deletedOutcomes = db.prepare(`
        DELETE FROM signal_outcomes WHERE entry_time < ?
      `).run(thirtyDaysAgo);
      console.log(`   ✅ Deleted ${deletedOutcomes.changes} old signal outcomes (>30 days)`);
    } catch (e) {
      // Table might not exist
    }
    
    // 4. Vacuum database to reclaim space
    db.exec('VACUUM');
    console.log(`   ✅ Database vacuumed (space reclaimed)`);
    
    // 5. Show database size
    const dbPath2 = process.env.DB_PATH || join(projectRoot, 'data', 'sentiment_arb.db');
    const stats2 = fs.statSync(dbPath2);
    const sizeMB = (stats2.size / 1024 / 1024).toFixed(2);
    console.log(`   📊 Database size: ${sizeMB} MB`);

    // ========================================
    // SUMMARY
    // ========================================
    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log('\n' + '═'.repeat(60));
    console.log(`✅ WEEKLY MAINTENANCE COMPLETE - ${duration}s`);
    console.log('═'.repeat(60));

    // Log to file
    const topSources = sourceOptimizer.getTopSources();
    const logEntry = {
      timestamp: new Date().toISOString(),
      duration_seconds: parseFloat(duration),
      narratives: narrativesAfter.length,
      kols: kolsAfter,
      channels: channelsAfter,
      signals_cleaned: deletedSignals.changes,
      active_sources: topSources.length,
      top_sources: topSources.map(s => ({
        id: s.source_id,
        quality: s.quality_score,
        win_rate: s.win_rate
      }))
    };
    
    const logFile = join(logsDir, 'weekly-maintenance.json');
    let logs = [];
    if (fs.existsSync(logFile)) {
      try {
        logs = JSON.parse(fs.readFileSync(logFile, 'utf-8'));
      } catch (e) {
        logs = [];
      }
    }
    logs.push(logEntry);
    // Keep only last 52 weeks
    if (logs.length > 52) logs = logs.slice(-52);
    fs.writeFileSync(logFile, JSON.stringify(logs, null, 2));

  } catch (error) {
    console.error('\n❌ MAINTENANCE FAILED:', error.message);
    console.error(error.stack);
    process.exit(1);
  } finally {
    db.close();
  }
}

main().catch(error => {
  console.error('❌ Fatal error:', error.message);
  process.exit(1);
});
