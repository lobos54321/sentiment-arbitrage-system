#!/usr/bin/env node
/**
 * Weekly Influencer Discovery Script
 * 
 * Run this weekly (via cron) to discover new KOLs and channels
 * 
 * Usage:
 *   node scripts/discover-influencers.js
 *   
 * Cron example (every Sunday at 01:00):
 *   0 1 * * 0 cd /path/to/project && node scripts/discover-influencers.js
 */

import dotenv from 'dotenv';
import Database from 'better-sqlite3';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { AIInfluencerSystem } from '../src/scoring/ai-influencer-system.js';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const projectRoot = join(__dirname, '..');

async function main() {
  console.log('🔍 Starting weekly influencer discovery...\n');

  // Connect to database
  const dbPath = process.env.DB_PATH || join(projectRoot, 'data', 'sentiment_arb.db');
  const db = new Database(dbPath);

  // Initialize AI Influencer System
  const config = {};
  const aiInfluencer = new AIInfluencerSystem(config, db);

  // Show current stats
  console.log('📊 Current influencer database:');
  const kolsBefore = aiInfluencer.getRankedKOLs();
  const channelsBefore = aiInfluencer.getRankedChannels();
  console.log(`   Twitter KOLs: ${kolsBefore.length}`);
  console.log(`   Telegram Channels: ${channelsBefore.length}`);

  // Run weekly discovery
  console.log('\n🤖 Running AI discovery...');
  await aiInfluencer.weeklyDiscovery();

  // Show updated stats
  console.log('\n📊 Updated influencer database:');
  const kolsAfter = aiInfluencer.getRankedKOLs();
  const channelsAfter = aiInfluencer.getRankedChannels();
  console.log(`   Twitter KOLs: ${kolsAfter.length} (${kolsAfter.length - kolsBefore.length > 0 ? '+' : ''}${kolsAfter.length - kolsBefore.length})`);
  console.log(`   Telegram Channels: ${channelsAfter.length} (${channelsAfter.length - channelsBefore.length > 0 ? '+' : ''}${channelsAfter.length - channelsBefore.length})`);

  // Show top KOLs
  console.log('\n🐦 Top 10 Twitter KOLs:');
  console.table(kolsAfter.slice(0, 10).map(k => ({
    Handle: '@' + k.handle,
    Tier: k.tier,
    Influence: k.influence_score.toFixed(1),
    Followers: k.followers?.toLocaleString() || 'N/A'
  })));

  // Show top Channels
  console.log('\n📱 Top 10 Telegram Channels:');
  console.table(channelsAfter.slice(0, 10).map(c => ({
    Channel: c.username,
    Tier: c.tier,
    Influence: c.influence_score.toFixed(1),
    Monitored: c.is_monitored ? '✅' : '❌'
  })));

  db.close();
  console.log('\n✅ Influencer discovery complete!');
}

main().catch(error => {
  console.error('❌ Failed:', error.message);
  process.exit(1);
});
