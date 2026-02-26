
import Database from 'better-sqlite3';
import dotenv from 'dotenv';
import { GMGNTelegramExecutor } from '../src/execution/gmgn-telegram-executor.js';

dotenv.config();

const dbPath = process.env.DB_PATH || './data/sentiment_arb.db';
const db = new Database(dbPath);

async function closeAllPositions() {
    console.log('🚀 Starting manual liquidation of all positions...');

    const executor = new GMGNTelegramExecutor({}, db);

    // Check if we are in shadow mode
    if (process.env.SHADOW_MODE === 'true') {
        console.log('🎭 SHADOW_MODE is ENABLED. Trades will be simulated.');
    }

    try {
        // Fetch open/partial positions from 'positions' table
        const positions = db.prepare(`
            SELECT * FROM positions 
            WHERE status IN ('open', 'partial')
        `).all();

        if (positions.length === 0) {
            console.log('✅ No open positions found in "positions" table.');
        } else {
            console.log(`🔍 Found ${positions.length} active positions. Liquidating...`);

            for (const pos of positions) {
                const symbol = pos.symbol || pos.token_ca.substring(0, 8);
                console.log(`💸 Liquidating ${symbol} (${pos.token_ca})...`);

                try {
                    // Execute sell 100%
                    const result = await executor.executeSell(pos, 'MANUAL_CLOSE_ALL');

                    if (result.success) {
                        console.log(`   ✅ Successfully sold ${symbol}. TX: ${result.tx_hash || 'Simulated'}`);

                        // Update status in positions table
                        db.prepare(`
                            UPDATE positions 
                            SET status = 'closed',
                                exit_time = datetime('now'),
                                exit_type = 'MANUAL_CLOSE_ALL',
                                last_monitor_time = datetime('now')
                            WHERE id = ?
                        `).run(pos.id);
                    } else {
                        console.error(`   ❌ Failed to sell ${symbol}: ${result.error}`);
                    }
                } catch (err) {
                    console.error(`   ❌ Error liquidating ${symbol}:`, err.message);
                }
            }
        }

        // Also check 'trades' table for any stray 'OPEN' records
        const trades = db.prepare(`
            SELECT * FROM trades 
            WHERE status = 'OPEN'
        `).all();

        if (trades.length > 0) {
            console.log(`🔍 Found ${trades.length} stray 'OPEN' records in "trades" table. Liquidating...`);
            for (const trade of trades) {
                const symbol = trade.symbol || trade.token_ca.substring(0, 8);
                console.log(`💸 Liquidating stray trade ${symbol} (${trade.token_ca})...`);

                try {
                    const result = await executor.executeSell(trade, 'MANUAL_CLOSE_ALL_STRAY');
                    if (result.success) {
                        console.log(`   ✅ Successfully sold stray ${symbol}.`);
                        // trades table is updated within closeTradeRecord inside executor.executeSell
                    } else {
                        console.error(`   ❌ Failed to sell stray ${symbol}: ${result.error}`);
                    }
                } catch (err) {
                    console.error(`   ❌ Error liquidating stray ${symbol}:`, err.message);
                }
            }
        }

        console.log('\n🏁 Liquidation process completed.');

    } catch (error) {
        console.error('❌ Fatal error during liquidation:', error.message);
    } finally {
        await executor.disconnect();
        db.close();
    }
}

closeAllPositions();
