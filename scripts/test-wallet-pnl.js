/**
 * 测试 GMGN 雷达 - 钱包持仓/命中代币 API
 *
 * 雷达页面 URL: https://gmgn.ai/trade/ZAxgSuiP?chain=sol
 * ZAxgSuiP 可能是某个钱包的简码
 */

import fs from 'fs';

const DEVICE_ID = '1d29f750-687f-42e1-851d-59a43e5d2ffa';
const CLIENT_ID = 'gmgn_web_test';

async function fetchApi(url, headers) {
    try {
        const response = await fetch(url, { headers });
        if (!response.ok) {
            return { error: response.status };
        }
        return await response.json();
    } catch (e) {
        return { error: e.message };
    }
}

async function testWalletPNL() {
    console.log('\n========== 测试 GMGN 钱包持仓/命中代币 ==========\n');

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
        'Referer': 'https://gmgn.ai/trade/ZAxgSuiP?chain=sol'
    };

    // 先获取钱包排行榜中的钱包地址
    console.log('📊 获取钱包排行榜...\n');
    const rankUrl = `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?limit=5&${params}`;
    const rankData = await fetchApi(rankUrl, headers);

    if (rankData.error) {
        console.log('❌ 获取钱包排行失败:', rankData.error);
        return;
    }

    const wallets = rankData.data?.rank || [];
    console.log(`✅ 获取到 ${wallets.length} 个钱包\n`);

    // 对前3个钱包查询持仓和 PNL
    for (let i = 0; i < Math.min(3, wallets.length); i++) {
        const wallet = wallets[i];
        const address = wallet.wallet_address || wallet.address;
        const name = wallet.twitter_name || wallet.name || address?.slice(0, 8);

        console.log(`\n${'═'.repeat(60)}`);
        console.log(`👤 钱包 ${i + 1}: ${name}`);
        console.log(`   地址: ${address}`);
        console.log(`   1D 胜率: ${((wallet.winrate_1d || 0) * 100).toFixed(0)}%`);
        console.log(`   1D 盈利: $${Math.round(wallet.realized_profit_1d || 0)}`);
        console.log(`${'═'.repeat(60)}`);

        // 1. 获取钱包持仓
        console.log('\n📦 持仓代币 (Holdings):');
        const holdingsUrl = `https://gmgn.ai/defi/quotation/v1/wallet_holdings/sol/${address}?${params}`;
        const holdingsData = await fetchApi(holdingsUrl, headers);

        if (!holdingsData.error && holdingsData.data) {
            const holdings = holdingsData.data.holdings || holdingsData.data || [];
            if (Array.isArray(holdings) && holdings.length > 0) {
                console.log(`   共 ${holdings.length} 个持仓:\n`);
                holdings.slice(0, 5).forEach((h, idx) => {
                    const symbol = h.token?.symbol || h.symbol || 'N/A';
                    const value = h.usd_value || h.value || 0;
                    const pnl = h.realized_pnl || h.pnl || h.profit || 0;
                    const unrealizedPnl = h.unrealized_pnl || h.unrealized_profit || 0;
                    const cost = h.cost || h.buy_cost || 0;
                    console.log(`   ${idx + 1}. ${symbol.padEnd(12)} | 持仓: $${Math.round(value).toString().padStart(8)} | 成本: $${Math.round(cost).toString().padStart(6)} | 已实现PNL: $${Math.round(pnl).toString().padStart(6)} | 未实现: $${Math.round(unrealizedPnl).toString().padStart(6)}`);
                });
            } else {
                console.log('   暂无持仓数据');
            }
        } else {
            console.log('   ❌ 获取失败:', holdingsData.error || holdingsData.code);
        }

        // 2. 获取钱包活动 (买入)
        console.log('\n📈 最近买入:');
        const buyUrl = `https://gmgn.ai/defi/quotation/v1/wallet_activity/sol?type=buy&wallet=${address}&limit=5&${params}`;
        const buyData = await fetchApi(buyUrl, headers);

        if (!buyData.error && buyData.data) {
            const activities = buyData.data.activities || buyData.data || [];
            if (Array.isArray(activities) && activities.length > 0) {
                activities.slice(0, 5).forEach((a, idx) => {
                    const symbol = a.token_symbol || a.symbol || 'N/A';
                    const amount = a.amount_usd || a.usd_value || 0;
                    const price = a.price || 0;
                    const time = a.timestamp ? new Date(a.timestamp * 1000).toLocaleString() : 'N/A';
                    console.log(`   ${idx + 1}. ${symbol.padEnd(12)} | 买入: $${Math.round(amount).toString().padStart(6)} | 价格: $${Number(price).toFixed(8)} | 时间: ${time}`);
                });
            } else {
                console.log('   暂无买入记录');
            }
        } else {
            console.log('   ❌ 获取失败:', buyData.error || buyData.code);
        }

        // 3. 获取钱包统计/PNL
        console.log('\n📊 钱包统计:');
        const statsUrl = `https://gmgn.ai/defi/quotation/v1/wallet_stat/sol/${address}?${params}`;
        const statsData = await fetchApi(statsUrl, headers);

        if (!statsData.error && statsData.data) {
            const stats = statsData.data;
            console.log(`   总交易数: ${stats.total_trades || stats.txs || 'N/A'}`);
            console.log(`   总买入: ${stats.total_buys || stats.buy_count || 'N/A'}`);
            console.log(`   总卖出: ${stats.total_sells || stats.sell_count || 'N/A'}`);
            console.log(`   总盈亏: $${Math.round(stats.total_pnl || stats.realized_profit || 0)}`);
            console.log(`   平均胜率: ${((stats.winrate || stats.win_rate || 0) * 100).toFixed(1)}%`);
        } else {
            console.log('   ❌ 获取失败:', statsData.error || statsData.code);
        }

        // 延迟避免限流
        await new Promise(r => setTimeout(r, 500));
    }

    console.log('\n' + '═'.repeat(60));
    console.log('✅ 测试完成');
}

testWalletPNL().catch(console.error);
