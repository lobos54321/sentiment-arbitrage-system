/**
 * 雷达数据展示脚本
 *
 * 展示完整的雷达数据：
 * - 钱包排行（按不同排序）
 * - 命中代币
 * - 命中代币PNL
 * - 1D PNL
 * - 1D 胜率
 */

import fs from 'fs';

function displayRadarData() {
    const dataPath = './logs/radar-data.json';

    if (!fs.existsSync(dataPath)) {
        console.log('❌ 请先运行 node src/inputs/gmgn-radar-collector.js 采集数据');
        return;
    }

    const data = JSON.parse(fs.readFileSync(dataPath, 'utf8'));

    console.log('\n');
    console.log('╔══════════════════════════════════════════════════════════════════════════════╗');
    console.log('║                         GMGN 雷达数据完整展示                                 ║');
    console.log('╚══════════════════════════════════════════════════════════════════════════════╝');
    console.log(`\n数据时间: ${data.savedAt}\n`);

    // ==================== 命中代币 ====================
    console.log('┌──────────────────────────────────────────────────────────────────────────────┐');
    console.log('│  🎯 命中代币列表                                                             │');
    console.log('└──────────────────────────────────────────────────────────────────────────────┘');

    if (data.radarTokens && data.radarTokens.length > 0) {
        console.log('\n代币           | 市值           | 地址');
        console.log('─'.repeat(80));

        for (const token of data.radarTokens) {
            const symbol = (token.symbol || '').slice(0, 14).padEnd(14);
            const mcap = '$' + (parseFloat(token.market_cap || 0) / 1e6).toFixed(2) + 'M';
            const mcapStr = mcap.padStart(12);
            const addr = token.address.slice(0, 20) + '...';

            console.log(`${symbol} | ${mcapStr} | ${addr}`);
        }
    } else {
        console.log('\n  暂无命中代币数据');
    }

    // ==================== 雷达钱包详情（命中代币PNL） ====================
    console.log('\n┌──────────────────────────────────────────────────────────────────────────────┐');
    console.log('│  💰 雷达钱包详情（含命中代币PNL）                                             │');
    console.log('└──────────────────────────────────────────────────────────────────────────────┘');

    if (data.radarWallets && data.radarWallets.length > 0) {
        console.log('\n排名 | 钱包地址         | 命中数 | 命中代币PNL      | 胜率    | 买入 | 卖出');
        console.log('─'.repeat(85));

        // 按命中代币PNL排序
        const sortedWallets = [...data.radarWallets].sort((a, b) =>
            (b.matched_profit || 0) - (a.matched_profit || 0)
        );

        for (let i = 0; i < Math.min(sortedWallets.length, 20); i++) {
            const w = sortedWallets[i];
            const rank = String(i + 1).padStart(2);
            const addr = (w.address || '').slice(0, 14).padEnd(14);
            const matchedCount = String(w.matched_count || 0).padStart(4);
            const matchedProfit = ('$' + (w.matched_profit || 0).toFixed(0)).padStart(14);
            const winrate = ((w.winrate || 0) * 100).toFixed(1).padStart(5) + '%';
            const buys = String(w.total_buy || 0).padStart(4);
            const sells = String(w.total_sell || 0).padStart(4);

            console.log(`#${rank} | ${addr} | ${matchedCount} | ${matchedProfit} | ${winrate} | ${buys} | ${sells}`);
        }

        // 显示命中代币详情
        console.log('\n📋 命中代币详情 (前10个钱包):');
        console.log('─'.repeat(80));

        for (let i = 0; i < Math.min(sortedWallets.length, 10); i++) {
            const w = sortedWallets[i];
            if (w.matched_tokens && w.matched_tokens.length > 0) {
                const addr = (w.address || '').slice(0, 12);
                const tokens = w.matched_tokens.map(t => {
                    // 查找代币名称
                    const tokenInfo = data.radarTokens.find(rt => rt.address === t);
                    return tokenInfo ? tokenInfo.symbol : t.slice(0, 8);
                }).join(', ');

                console.log(`${addr}... | 命中: ${tokens}`);
            }
        }
    } else {
        console.log('\n  暂无雷达钱包详情数据');
    }

    // ==================== 钱包排行榜 ====================
    console.log('\n┌──────────────────────────────────────────────────────────────────────────────┐');
    console.log('│  📊 钱包排行榜（按 1D PNL 排序）                                              │');
    console.log('└──────────────────────────────────────────────────────────────────────────────┘');

    if (data.wallets && data.wallets.length > 0) {
        console.log('\n排名 | 钱包名称             | 1D PNL         | 1D胜率  | 买入  | 卖出  | 标签');
        console.log('─'.repeat(95));

        // 按 1D PNL 排序
        const sortedByPnl = [...data.wallets].sort((a, b) =>
            parseFloat(b.realized_profit_1d || 0) - parseFloat(a.realized_profit_1d || 0)
        );

        for (let i = 0; i < Math.min(sortedByPnl.length, 20); i++) {
            const w = sortedByPnl[i];
            const rank = String(i + 1).padStart(2);
            const name = (w.twitter_name || w.wallet_address?.slice(0, 12) || '').slice(0, 18).padEnd(18);
            const pnl = ('$' + parseFloat(w.realized_profit_1d || 0).toFixed(0)).padStart(13);
            const winrate = ((w.winrate_1d || 0) * 100).toFixed(1).padStart(5) + '%';
            const buys = String(w.buy_1d || 0).padStart(5);
            const sells = String(w.sell_1d || 0).padStart(5);
            const tags = (w.tags || []).slice(0, 2).join(', ').slice(0, 20);

            console.log(`#${rank} | ${name} | ${pnl} | ${winrate} | ${buys} | ${sells} | ${tags}`);
        }
    } else {
        console.log('\n  暂无钱包排行数据');
    }

    // ==================== 高胜率钱包 ====================
    console.log('\n┌──────────────────────────────────────────────────────────────────────────────┐');
    console.log('│  🏆 高胜率钱包排行                                                            │');
    console.log('└──────────────────────────────────────────────────────────────────────────────┘');

    if (data.wallets && data.wallets.length > 0) {
        console.log('\n排名 | 钱包名称             | 1D胜率  | 1D PNL         | 交易次数 | 标签');
        console.log('─'.repeat(90));

        // 按胜率排序
        const sortedByWinrate = [...data.wallets].sort((a, b) =>
            (b.winrate_1d || 0) - (a.winrate_1d || 0)
        );

        for (let i = 0; i < Math.min(sortedByWinrate.length, 15); i++) {
            const w = sortedByWinrate[i];
            const rank = String(i + 1).padStart(2);
            const name = (w.twitter_name || w.wallet_address?.slice(0, 12) || '').slice(0, 18).padEnd(18);
            const winrate = ((w.winrate_1d || 0) * 100).toFixed(1).padStart(5) + '%';
            const pnl = ('$' + parseFloat(w.realized_profit_1d || 0).toFixed(0)).padStart(13);
            const txs = String(w.txs_1d || 0).padStart(6);
            const tags = (w.tags || []).slice(0, 2).join(', ').slice(0, 20);

            console.log(`#${rank} | ${name} | ${winrate} | ${pnl} | ${txs} | ${tags}`);
        }
    }

    // ==================== 总结 ====================
    console.log('\n╔══════════════════════════════════════════════════════════════════════════════╗');
    console.log('║  📈 数据采集能力总结                                                          ║');
    console.log('╚══════════════════════════════════════════════════════════════════════════════╝');
    console.log('\n✅ 成功采集的数据:');
    console.log(`   • 钱包排行榜: ${data.wallets?.length || 0} 个钱包`);
    console.log(`   • 命中代币: ${data.radarTokens?.length || 0} 个代币`);
    console.log(`   • 雷达钱包详情: ${data.radarWallets?.length || 0} 个钱包（含命中代币PNL）`);
    console.log('\n📊 可获取的字段:');
    console.log('   • 钱包排行: 地址, 名称, 1D/7D/30D PNL, 胜率, 买卖次数, 标签');
    console.log('   • 命中代币: 代币地址, 符号, 名称, 市值');
    console.log('   • 命中代币PNL: 每个钱包的命中数, 命中代币列表, 命中利润, 胜率');
    console.log('\n');
}

displayRadarData();
