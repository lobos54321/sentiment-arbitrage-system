/**
 * 测试 GMGN 雷达 - 钱包命中代币 PNL 详情
 */

import fs from 'fs';

const DEVICE_ID = '1d29f750-687f-42e1-851d-59a43e5d2ffa';
const CLIENT_ID = 'gmgn_web_test';

async function testWalletHoldings() {
    console.log('\n========== GMGN 雷达 - 命中代币 PNL 详情 ==========\n');

    const sessionPath = './config/gmgn_session.json';
    const sessionData = JSON.parse(fs.readFileSync(sessionPath, 'utf8'));
    const cookies = sessionData?.cookies || [];
    const cookieStr = cookies
        .filter(c => c.domain && c.domain.includes('gmgn'))
        .map(c => `${c.name}=${c.value}`)
        .join('; ');

    const params = new URLSearchParams({
        device_id: DEVICE_ID,
        client_id: CLIENT_ID,
        from_app: 'gmgn',
        app_ver: '20260101',
        tz_name: 'Australia/Brisbane',
        app_lang: 'en',
        os: 'web'
    });

    const headers = {
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Cookie': cookieStr,
        'Origin': 'https://gmgn.ai',
        'Referer': 'https://gmgn.ai/'
    };

    // 获取前3个钱包
    console.log('📊 获取钱包排行 (买入最多)...\n');
    const rankUrl = `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=buy&limit=3&${params}`;
    const rankResponse = await fetch(rankUrl, { headers });
    const rankData = await rankResponse.json();
    const wallets = rankData.data?.rank || [];

    for (const wallet of wallets.slice(0, 2)) {
        const walletAddress = wallet.wallet_address;
        const walletName = wallet.twitter_name || wallet.name || walletAddress.slice(0, 8);

        console.log('═'.repeat(80));
        console.log(`👤 ${walletName}`);
        console.log(`   地址: ${walletAddress}`);
        console.log(`   1D盈利: $${Math.round(Number(wallet.realized_profit_1d) || 0)} | 胜率: ${((Number(wallet.winrate_1d) || 0) * 100).toFixed(1)}%`);
        console.log('═'.repeat(80));

        // 获取持仓详情
        const holdingsUrl = `https://gmgn.ai/defi/quotation/v1/wallet/sol/holdings/${walletAddress}?${params}`;
        const holdingsResponse = await fetch(holdingsUrl, { headers });
        const holdingsData = await holdingsResponse.json();

        if (holdingsData.data?.holdings) {
            const holdings = holdingsData.data.holdings;
            console.log(`\n📦 持仓代币 (${holdings.length} 个):\n`);

            // 打印所有字段看看有哪些
            if (holdings.length > 0) {
                console.log('字段:', Object.keys(holdings[0]).join(', '));
                console.log('');
            }

            // 格式化显示
            console.log('代币           | 持仓数量          | 当前价值       | 成本          | 已实现PNL     | 未实现PNL    | 买入均价      | 卖出均价');
            console.log('-'.repeat(130));

            holdings.slice(0, 10).forEach((h, i) => {
                const token = h.token || {};
                const symbol = (token.symbol || h.symbol || 'N/A').slice(0, 12).padEnd(12);
                const amount = Number(h.balance || h.amount || 0);
                const amountStr = amount > 1e9 ? `${(amount/1e9).toFixed(2)}B` : amount > 1e6 ? `${(amount/1e6).toFixed(2)}M` : amount > 1e3 ? `${(amount/1e3).toFixed(2)}K` : amount.toFixed(2);

                const usdValue = Number(h.usd_value || h.value || 0);
                const totalCost = Number(h.total_cost || h.cost || 0);
                const realizedPnl = Number(h.realized_pnl || h.realized_profit || 0);
                const unrealizedPnl = Number(h.unrealized_pnl || 0);
                const avgBuyPrice = Number(h.avg_cost || h.avg_buy_price || 0);
                const avgSellPrice = Number(h.avg_sold_price || h.avg_sell_price || 0);

                console.log(
                    `${symbol} | ${amountStr.padStart(15)} | $${usdValue.toFixed(2).padStart(10)} | $${totalCost.toFixed(2).padStart(10)} | $${realizedPnl.toFixed(2).padStart(10)} | $${unrealizedPnl.toFixed(2).padStart(9)} | $${avgBuyPrice.toFixed(8).padStart(12)} | $${avgSellPrice.toFixed(8).padStart(12)}`
                );
            });

            // 显示一个完整示例
            if (holdings.length > 0) {
                console.log('\n📋 完整数据示例 (第1个代币):');
                console.log(JSON.stringify(holdings[0], null, 2));
            }
        } else {
            console.log('\n⚠️ 无持仓数据');
        }

        console.log('\n');
        await new Promise(r => setTimeout(r, 500));
    }
}

testWalletHoldings().catch(console.error);
