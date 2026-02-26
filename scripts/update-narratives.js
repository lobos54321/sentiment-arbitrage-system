#!/usr/bin/env node
/**
 * Weekly Narrative Update Script
 * 
 * Run this weekly (via cron) to update narrative weights based on AI analysis
 * 
 * Usage:
 *   node scripts/update-narratives.js
 *   
 * Cron example (every Sunday at 00:00):
 *   0 0 * * 0 cd /path/to/project && node scripts/update-narratives.js
 */

import dotenv from 'dotenv';
import Database from 'better-sqlite3';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { AINarrativeSystem } from '../src/scoring/ai-narrative-system.js';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const projectRoot = join(__dirname, '..');

async function main() {
  console.log('🔄 Starting weekly narrative update...\n');

  // Connect to database
  const dbPath = process.env.DB_PATH || join(projectRoot, 'data', 'sentiment_arb.db');
  const db = new Database(dbPath);

  // Initialize AI Narrative System
  const config = {}; // Minimal config needed
  const aiNarrative = new AINarrativeSystem(config, db);

  // Show current narratives
  console.log('📊 Current narratives before update:');
  const before = aiNarrative.getRankedNarratives();
  console.table(before.map(n => ({
    Name: n.name,
    Weight: n.weight.toFixed(1),
    Heat: n.market_heat,
    Stage: n.lifecycle_stage,
    Updated: n.last_updated.split('T')[0]
  })));

  // Run weekly update
  console.log('\n🤖 Calling Grok AI for narrative analysis...');
  await aiNarrative.weeklyNarrativeUpdate();

  // Show updated narratives
  console.log('\n📊 Narratives after update:');
  const after = aiNarrative.getRankedNarratives();
  console.table(after.map(n => ({
    Name: n.name,
    Weight: n.weight.toFixed(1),
    Heat: n.market_heat,
    Stage: n.lifecycle_stage,
    Updated: n.last_updated.split('T')[0]
  })));

  // Show changes
  console.log('\n📈 Weight changes:');
  for (const afterN of after) {
    const beforeN = before.find(b => b.name === afterN.name);
    if (beforeN) {
      const change = afterN.weight - beforeN.weight;
      if (Math.abs(change) > 0.1) {
        const arrow = change > 0 ? '↑' : '↓';
        console.log(`   ${arrow} ${afterN.name}: ${beforeN.weight.toFixed(1)} → ${afterN.weight.toFixed(1)} (${change > 0 ? '+' : ''}${change.toFixed(1)})`);
      }
    } else {
      console.log(`   ✨ NEW: ${afterN.name} (weight: ${afterN.weight.toFixed(1)})`);
    }
  }

  db.close();
  console.log('\n✅ Weekly narrative update complete!');
}

main().catch(error => {
  console.error('❌ Failed:', error.message);
  process.exit(1);
});
