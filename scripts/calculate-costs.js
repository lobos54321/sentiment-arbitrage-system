/**
 * Calculate XAI API Costs
 * 
 * Estimates costs based on signal volume and API pricing.
 */

import Database from 'better-sqlite3';
import dotenv from 'dotenv';

dotenv.config();
const db = new Database(process.env.DB_PATH || './data/sentiment_arb.db');

async function calculate() {
    console.log('💰 Calculating XAI API Costs (Past 24H)...\n');

    // 1. Alpha Monitor Checks (18 per hour approx)
    const monitorCalls = 18 * 14; // Estimated 14 hours of run time
    const monitorCost = monitorCalls * 0.002;

    // 2. LLM Analysis (From signals that required full evaluation)
    const alphaSignals = db.prepare(`
        SELECT count(*) as c FROM shadow_price_tracking 
        WHERE source_type = 'alpha' 
        AND created_at > strftime('%s', 'now', '-1 day')
    `).get().c;

    // Estimate: Only Tier 2 and Tier 3 trigger LLM if not cached. 
    // Let's assume 50% of alpha signals triggered an LLM call.
    const llmCalls = alphaSignals * 0.5;
    const llmCost = llmCalls * 0.003;

    // 3. X Search (Triggered by LLM)
    const xSearchCalls = llmCalls;
    const xSearchCost = xSearchCalls * 0.002;

    const total = monitorCost + llmCost + xSearchCost;

    console.log('-------------------------------------------');
    console.log(`📡 Alpha Monitoring:  ~${monitorCalls} calls  |  ~$${monitorCost.toFixed(2)}`);
    console.log(`🧠 LLM Analysis:      ~${llmCalls.toFixed(0)} calls  |  ~$${llmCost.toFixed(2)}`);
    console.log(`🔍 X Search:          ~${xSearchCalls.toFixed(0)} calls  |  ~$${xSearchCost.toFixed(2)}`);
    console.log('-------------------------------------------');
    console.log(`💵 TOTAL ESTIMATED:   $${total.toFixed(2)} USD`);
    console.log('-------------------------------------------');

    // Convert to RMB approx
    console.log(`💴 约合人民币:         ¥${(total * 7.2).toFixed(2)} 元`);
}

calculate().catch(console.error);
