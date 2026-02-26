/**
 * Backtest Scoring System
 * 
 * Runs historical positions through 15-minute price performance analysis
 * to identify high-quality signal sources and narratives.
 */

import Database from 'better-sqlite3';
import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const dbPath = process.env.DB_PATH || './data/sentiment_arb.db';

const db = new Database(dbPath);

async function runBacktest() {
    console.log('🧪 Starting Historical Performance Backtest...');

    // 1. Get all closed positions or shadow tracks
    const data = db.prepare(`
    SELECT 
      p.token_ca,
      p.chain,
      p.symbol,
      p.alpha_tier,
      p.entry_price,
      p.entry_time,
      p.exit_price,
      p.pnl_percent,
      COALESCE(spt.source_id, ts.channel_name, 'Unknown') as source,
      COALESCE(spt.source_type, 'telegram') as source_type
    FROM positions p
    LEFT JOIN shadow_price_tracking spt ON p.token_ca = spt.token_ca AND p.chain = spt.chain
    LEFT JOIN telegram_signals ts ON p.signal_id = ts.id
    WHERE p.status = 'closed' OR spt.status = 'completed'
    ORDER BY p.entry_time DESC
  `).all();

    console.log(`📊 Found ${data.length} historical trades to analyze.\n`);

    const stats = {
        total: 0,
        wins: 0,
        losses: 0,
        totalPnl: 0,
        sources: {},
        tiers: {
            tier1: { count: 0, pnl: 0, wins: 0 },
            tier2: { count: 0, pnl: 0, wins: 0 },
            tier3: { count: 0, pnl: 0, wins: 0 },
            none: { count: 0, pnl: 0, wins: 0 }
        }
    };

    for (const trade of data) {
        stats.total++;
        const pnl = trade.pnl_percent || 0;
        stats.totalPnl += pnl;

        if (pnl > 0) stats.wins++;
        else stats.losses++;

        // Source stats
        if (!stats.sources[trade.source]) {
            stats.sources[trade.source] = { count: 0, pnl: 0, wins: 0 };
        }
        stats.sources[trade.source].count++;
        stats.sources[trade.source].pnl += pnl;
        if (pnl > 0) stats.sources[trade.source].wins++;

        // Tier stats
        const tier = trade.alpha_tier || 'none';
        if (stats.tiers[tier]) {
            stats.tiers[tier].count++;
            stats.tiers[tier].pnl += pnl;
            if (pnl > 0) stats.tiers[tier].wins++;
        }
    }

    // Print Summary
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log('🏆 OVERALL PERFORMANCE');
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log(`Total Trades: ${stats.total}`);
    console.log(`Win Rate:     ${((stats.wins / stats.total) * 100).toFixed(1)}%`);
    console.log(`Total PnL:    ${stats.totalPnl.toFixed(1)}%`);
    console.log(`Avg PnL:      ${(stats.totalPnl / stats.total).toFixed(2)}%`);

    console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log('🚀 PERFORMANCE BY ALPHA TIER');
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    for (const [tier, tStats] of Object.entries(stats.tiers)) {
        if (tStats.count === 0) continue;
        console.log(`${tier.toUpperCase()}: ${tStats.count} trades | WR: ${((tStats.wins / tStats.count) * 100).toFixed(1)}% | Avg: ${(tStats.pnl / tStats.count).toFixed(2)}%`);
    }

    console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log('🔝 TOP SIGNAL SOURCES');
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    const sortedSources = Object.entries(stats.sources)
        .sort((a, b) => (b[1].pnl / b[1].count) - (a[1].pnl / a[1].count))
        .filter(s => s[1].count >= 2);

    for (let i = 0; i < Math.min(10, sortedSources.length); i++) {
        const [name, sStats] = sortedSources[i];
        console.log(`${i + 1}. ${name.padEnd(20)} | Trades: ${sStats.count} | WR: ${((sStats.wins / sStats.count) * 100).toFixed(1)}% | Avg PnL: ${(sStats.pnl / sStats.count).toFixed(2)}%`);
    }

    console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
}

runBacktest().catch(console.error);
