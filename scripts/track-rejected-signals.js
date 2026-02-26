#!/usr/bin/env node
/**
 * v7.3 拒绝信号追踪脚本
 *
 * 功能：
 * 1. 查找需要追踪的拒绝信号（创建超过24小时但未完成追踪）
 * 2. 获取被拒绝代币的当前价格
 * 3. 计算假设收益率（如果当时买入会怎样）
 * 4. 更新 rejected_signals 表的追踪数据
 *
 * 用法: node scripts/track-rejected-signals.js [批次大小]
 * 示例: node scripts/track-rejected-signals.js 50
 *
 * 建议通过 cron 每小时运行一次
 */

import Database from 'better-sqlite3';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const dbPath = path.join(__dirname, '..', 'data', 'sentiment_arb.db');

let db;
try {
    db = new Database(dbPath);
} catch (e) {
    console.error(`无法打开数据库: ${dbPath}`);
    console.error(e.message);
    process.exit(1);
}

// DexScreener API 调用
async function fetchDexScreener(tokenCA) {
    try {
        const response = await fetch(`https://api.dexscreener.com/latest/dex/tokens/${tokenCA}`, {
            headers: { 'Accept': 'application/json' },
            signal: AbortSignal.timeout(10000)
        });

        if (!response.ok) {
            return null;
        }

        const data = await response.json();
        return data;
    } catch (e) {
        return null;
    }
}

/**
 * 获取代币当前价格
 */
async function getCurrentPrice(tokenCA, chain) {
    try {
        const data = await fetchDexScreener(tokenCA);

        if (!data || !data.pairs || data.pairs.length === 0) {
            // 代币可能已经归零或下架
            return { price: 0, symbol: null, status: 'delisted' };
        }

        // 优先匹配指定链
        let pair = data.pairs.find(p =>
            (chain === 'SOL' && p.chainId === 'solana') ||
            (chain === 'BSC' && p.chainId === 'bsc')
        );

        // 如果没找到，使用第一个
        if (!pair) {
            pair = data.pairs[0];
        }

        return {
            price: parseFloat(pair.priceUsd) || 0,
            symbol: pair.baseToken?.symbol || null,
            status: 'active',
            liquidity: parseFloat(pair.liquidity?.usd) || 0,
            volume24h: parseFloat(pair.volume?.h24) || 0,
            priceChange24h: parseFloat(pair.priceChange?.h24) || 0
        };
    } catch (e) {
        console.error(`   获取 ${tokenCA} 价格失败:`, e.message);
        return null;
    }
}

/**
 * 追踪拒绝信号
 */
async function trackRejectedSignals(batchSize = 50) {
    console.log(`\n${'═'.repeat(60)}`);
    console.log(`🛡️ v7.3 拒绝信号追踪`);
    console.log(`${'═'.repeat(60)}`);
    console.log(`时间: ${new Date().toISOString()}`);

    // 检查表是否存在
    const tableExists = db.prepare(`
        SELECT name FROM sqlite_master WHERE type='table' AND name='rejected_signals'
    `).get();

    if (!tableExists) {
        console.log('\n❌ rejected_signals 表不存在，请先运行迁移脚本');
        return { tracked: 0, failed: 0 };
    }

    // 找出需要追踪的信号
    // 条件：创建超过1小时但未完成追踪（用于1小时后追踪）
    // 以及创建超过24小时但未完成24小时追踪
    const toTrack1h = db.prepare(`
        SELECT * FROM rejected_signals
        WHERE tracking_completed = 0
        AND price_1h_later IS NULL
        AND created_at <= datetime('now', '-1 hour')
        ORDER BY created_at ASC
        LIMIT ?
    `).all(batchSize);

    const toTrack24h = db.prepare(`
        SELECT * FROM rejected_signals
        WHERE tracking_completed = 0
        AND price_1h_later IS NOT NULL
        AND price_24h_later IS NULL
        AND created_at <= datetime('now', '-24 hours')
        ORDER BY created_at ASC
        LIMIT ?
    `).all(batchSize);

    console.log(`\n📋 待追踪信号: 1h=${toTrack1h.length}, 24h=${toTrack24h.length}`);

    let tracked = 0;
    let failed = 0;

    // 追踪1小时后价格
    if (toTrack1h.length > 0) {
        console.log(`\n⏰ 追踪 1 小时后价格...`);
        for (const signal of toTrack1h) {
            try {
                const result = await getCurrentPrice(signal.token_ca, signal.chain);

                if (!result) {
                    failed++;
                    continue;
                }

                const price1h = result.price || 0;
                const pnl1h = signal.price_at_rejection > 0 ?
                    (price1h - signal.price_at_rejection) / signal.price_at_rejection * 100 : -100;

                db.prepare(`
                    UPDATE rejected_signals SET
                        price_1h_later = ?
                    WHERE id = ?
                `).run(price1h, signal.id);

                const emoji = pnl1h >= 0 ? '📈' : '📉';
                console.log(`   ${emoji} ${signal.symbol || signal.token_ca.slice(0, 8)}: 1h ${pnl1h >= 0 ? '+' : ''}${pnl1h.toFixed(1)}%`);
                tracked++;

                // 避免 API 限流
                await sleep(200);
            } catch (e) {
                console.error(`   ❌ ${signal.symbol}: ${e.message}`);
                failed++;
            }
        }
    }

    // 追踪24小时后价格（完成追踪）
    if (toTrack24h.length > 0) {
        console.log(`\n⏰ 追踪 24 小时后价格...`);
        for (const signal of toTrack24h) {
            try {
                const result = await getCurrentPrice(signal.token_ca, signal.chain);

                let price24h, wouldHaveProfit, status;

                if (!result || result.status === 'delisted' || result.price === 0) {
                    // 代币已下架或归零
                    price24h = 0;
                    wouldHaveProfit = -100;
                    status = 'delisted';
                } else {
                    price24h = result.price;
                    wouldHaveProfit = signal.price_at_rejection > 0 ?
                        (price24h - signal.price_at_rejection) / signal.price_at_rejection * 100 : -100;
                    status = 'tracked';
                }

                // 计算最大/最小价格（简化：使用当前价格和1小时价格估算）
                const price1h = signal.price_1h_later || 0;
                const maxPrice = Math.max(price1h, price24h);
                const minPrice = Math.min(price1h > 0 ? price1h : price24h, price24h > 0 ? price24h : price1h);

                db.prepare(`
                    UPDATE rejected_signals SET
                        price_24h_later = ?,
                        max_price_24h = ?,
                        min_price_24h = ?,
                        would_have_profit = ?,
                        tracking_completed = 1,
                        tracked_at = datetime('now')
                    WHERE id = ?
                `).run(
                    price24h,
                    maxPrice,
                    minPrice,
                    wouldHaveProfit,
                    signal.id
                );

                const emoji = wouldHaveProfit >= 0 ? '❌' : '✅'; // 反过来：拒绝后涨了是错过机会，跌了是正确决策
                const statusEmoji = status === 'delisted' ? '💀' : '';
                console.log(`   ${emoji} ${signal.symbol || signal.token_ca.slice(0, 8)}: 24h ${wouldHaveProfit >= 0 ? '+' : ''}${wouldHaveProfit.toFixed(1)}% ${statusEmoji}`);
                tracked++;

                // 避免 API 限流
                await sleep(200);
            } catch (e) {
                console.error(`   ❌ ${signal.symbol}: ${e.message}`);
                failed++;
            }
        }
    }

    // 统计汇总
    console.log(`\n${'─'.repeat(60)}`);
    console.log(`📊 追踪完成: ${tracked} 成功, ${failed} 失败`);

    // 输出防守价值统计
    await printDefenseStats();

    return { tracked, failed };
}

/**
 * 打印防守价值统计
 */
async function printDefenseStats() {
    const stats = db.prepare(`
        SELECT
            rejection_stage,
            COUNT(*) as total,
            SUM(CASE WHEN would_have_profit < 0 THEN 1 ELSE 0 END) as correct_rejections,
            SUM(CASE WHEN would_have_profit < -20 THEN 1 ELSE 0 END) as dodged_big_loss,
            SUM(CASE WHEN would_have_profit > 50 THEN 1 ELSE 0 END) as missed_big_gain,
            AVG(would_have_profit) as avg_avoided_pnl
        FROM rejected_signals
        WHERE tracking_completed = 1
        GROUP BY rejection_stage
    `).all();

    if (stats.length > 0) {
        console.log(`\n${'═'.repeat(60)}`);
        console.log(`🛡️ 防守价值汇总`);
        console.log(`${'═'.repeat(60)}\n`);

        for (const s of stats) {
            const accuracy = s.total > 0 ? (s.correct_rejections / s.total * 100) : 0;
            const emoji = accuracy >= 60 ? '✅' : accuracy >= 40 ? '⚠️' : '❌';

            console.log(`${emoji} ${s.rejection_stage}`);
            console.log(`   总拒绝: ${s.total} | 正确率: ${accuracy.toFixed(1)}% | 平均避开: ${s.avg_avoided_pnl?.toFixed(1) || 0}%`);
            console.log(`   🛡️ 躲过大跌: ${s.dodged_big_loss} | ❌ 错过大涨: ${s.missed_big_gain}`);
        }
    }
}

/**
 * 清理过期数据
 */
function cleanupOldData(retentionDays = 30) {
    const cutoff = new Date(Date.now() - retentionDays * 24 * 60 * 60 * 1000).toISOString();

    const result = db.prepare(`
        DELETE FROM rejected_signals
        WHERE created_at < ? AND tracking_completed = 1
    `).run(cutoff);

    if (result.changes > 0) {
        console.log(`\n🧹 清理了 ${result.changes} 条超过 ${retentionDays} 天的旧数据`);
    }
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * 主函数
 */
async function main() {
    const batchSize = parseInt(process.argv[2]) || 50;

    try {
        await trackRejectedSignals(batchSize);

        // 定期清理旧数据
        cleanupOldData(30);

        console.log(`\n追踪完成`);
    } catch (e) {
        console.error('追踪脚本执行失败:', e.message);
        process.exit(1);
    }
}

main();
