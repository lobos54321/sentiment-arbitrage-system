/**
 * 测试 GMGN 雷达 - 寻找命中代币详情
 *
 * 综合测试已知有效的 API 端点
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

async function testHitTokens() {
    console.log('\n========== GMGN 雷达 - 寻找命中代币详情 ==========\n');

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

    // 获取有交易活动的钱包
    console.log('📊 获取交易活跃的钱包 (按买入数量排序)...\n');

    const rankUrl = `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=buy&limit=10&${params}`;
    const rankData = await fetchApi(rankUrl, headers);
    const wallets = rankData.data?.rank || [];

    console.log(`获取到 ${wallets.length} 个钱包\n`);

    // 对每个钱包获取已知有效的端点数据
    for (let i = 0; i < Math.min(3, wallets.length); i++) {
        const w = wallets[i];
        const name = w.twitter_name || w.name || w.wallet_address?.slice(0, 8);
        const addr = w.wallet_address;

        console.log('='.repeat(100));
        console.log(`钱包 ${i+1}: ${name}`);
        console.log(`地址: ${addr}`);
        console.log(`1D盈利: $${Math.round(Number(w.realized_profit_1d) || 0)} | 胜率: ${((Number(w.winrate_1d) || 0) * 100).toFixed(1)}%`);
        console.log(`买入: ${w.buy_1d || 0} | 卖出: ${w.sell_1d || 0}`);
        console.log('='.repeat(100));

        // 1. 获取持仓 (wallet/sol/holdings)
        console.log('\n📦 持仓代币:');
        const holdingsUrl = `https://gmgn.ai/defi/quotation/v1/wallet/sol/holdings/${addr}?${params}`;
        const holdingsData = await fetchApi(holdingsUrl, headers);

        if (!holdingsData.error && holdingsData.data?.holdings) {
            const holdings = holdingsData.data.holdings;
            console.log(`   共 ${holdings.length} 个持仓`);

            if (holdings.length > 0) {
                console.log('   代币           | 持仓价值     | 成本         | 已实现PNL    | 未实现PNL');
                console.log('   ' + '-'.repeat(85));

                holdings.slice(0, 8).forEach(h => {
                    const token = h.token || {};
                    const symbol = (token.symbol || 'N/A').slice(0, 12).padEnd(12);
                    const value = Number(h.usd_value || 0);
                    const cost = Number(h.total_cost || 0);
                    const realizedPnl = Number(h.realized_pnl || 0);
                    const unrealizedPnl = Number(h.unrealized_pnl || 0);

                    console.log(`   ${symbol} | $${value.toFixed(2).padStart(10)} | $${cost.toFixed(2).padStart(10)} | $${realizedPnl.toFixed(2).padStart(10)} | $${unrealizedPnl.toFixed(2).padStart(10)}`);
                });

                // 完整示例
                console.log('\n   完整数据示例:');
                console.log('   ' + JSON.stringify(holdings[0], null, 2).split('\n').join('\n   '));
            }
        } else {
            console.log(`   获取失败或无数据`);
        }

        // 2. 获取活动记录 (wallet_activity)
        console.log('\n📈 交易活动:');
        const actUrl = `https://gmgn.ai/defi/quotation/v1/wallet_activity/sol?wallet=${addr}&limit=20&${params}`;
        const actData = await fetchApi(actUrl, headers);

        if (!actData.error && actData.data?.activities) {
            const activities = actData.data.activities;
            console.log(`   共 ${activities.length} 条活动`);

            if (activities.length > 0) {
                console.log('   类型   | 代币           | 金额 (USD)   | 价格             | 市值         | PNL');
                console.log('   ' + '-'.repeat(90));

                activities.slice(0, 10).forEach(a => {
                    const type = (a.event_type || 'N/A').padEnd(6);
                    const symbol = (a.token?.symbol || 'N/A').slice(0, 12).padEnd(12);
                    const usd = Number(a.amount_usd || 0);
                    const price = Number(a.price || 0);
                    const mcap = Number(a.market_cap || 0);
                    const pnl = a.realized_profit ? `$${Number(a.realized_profit).toFixed(2)}` : '-';

                    console.log(`   ${type} | ${symbol} | $${usd.toFixed(2).padStart(10)} | $${price.toFixed(10).padStart(14)} | $${(mcap/1e6).toFixed(1)}M`.padEnd(80) + ` | ${pnl}`);
                });

                // 完整示例
                console.log('\n   完整活动数据示例:');
                console.log('   ' + JSON.stringify(activities[0], null, 2).split('\n').join('\n   '));
            }
        } else {
            console.log(`   获取失败或无数据`);
        }

        console.log('\n');
        await new Promise(r => setTimeout(r, 800));
    }

    // 3. 测试 swaps 排行榜中的代币信息
    console.log('\n\n========== SWAPS 排行榜 (交易记录) ==========\n');
    const swapsUrl = `https://gmgn.ai/defi/quotation/v1/rank/sol/swaps/5m?limit=20&${params}`;
    const swapsData = await fetchApi(swapsUrl, headers);

    if (!swapsData.error && swapsData.data?.rank) {
        const swaps = swapsData.data.rank;
        console.log(`获取到 ${swaps.length} 条交易\n`);

        if (swaps.length > 0) {
            console.log('字段: ' + Object.keys(swaps[0]).join(', '));
            console.log('\n钱包           | 代币           | 类型   | 金额 USD     | PNL');
            console.log('-'.repeat(80));

            swaps.slice(0, 15).forEach(s => {
                const wallet = (s.twitter_name || s.wallet_address?.slice(0, 8) || 'N/A').slice(0, 12).padEnd(12);
                const symbol = (s.token?.symbol || s.symbol || 'N/A').slice(0, 12).padEnd(12);
                const type = (s.event_type || s.type || 'N/A').padEnd(6);
                const usd = Number(s.amount_usd || s.usd || 0);
                const pnl = s.realized_profit || s.pnl || s.profit || '-';
                const pnlStr = pnl !== '-' ? `$${Number(pnl).toFixed(2)}` : '-';

                console.log(`${wallet} | ${symbol} | ${type} | $${usd.toFixed(2).padStart(10)} | ${pnlStr}`);
            });

            console.log('\n完整交易数据示例:');
            console.log(JSON.stringify(swaps[0], null, 2));
        }
    }

    console.log('\n' + '='.repeat(100));
    console.log('测试完成');
}

testHitTokens().catch(console.error);
