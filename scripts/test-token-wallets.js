/**
 * 测试 GMGN 雷达 - 按代币筛选钱包
 *
 * 根据截图分析：
 * - 顶部有代币标签过滤器 (Yaranaika, 114514, fish, AMEEN, NANJ)
 * - 这些可能是热门代币，点击后可以筛选买了这些代币的钱包
 * - "命中代币" 列可能显示的是钱包买入的这些热门代币
 */

import fs from 'fs';

const DEVICE_ID = '1d29f750-687f-42e1-851d-59a43e5d2ffa';
const CLIENT_ID = 'gmgn_web_test';

async function fetchApi(url, headers) {
    try {
        const response = await fetch(url, { headers });
        if (!response.ok) return { error: response.status };
        return await response.json();
    } catch (e) {
        return { error: e.message };
    }
}

async function testTokenWallets() {
    console.log('\n========== GMGN 雷达 - 按代币筛选钱包 ==========\n');

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

    // 先获取热门代币地址
    console.log('📊 获取热门代币...\n');

    const swapsUrl = `https://gmgn.ai/defi/quotation/v1/rank/sol/swaps/5m?limit=10&${params}`;
    const swapsData = await fetchApi(swapsUrl, headers);
    const tokens = swapsData.data?.rank || [];

    console.log('热门代币:');
    tokens.slice(0, 5).forEach((t, i) => {
        console.log(`  ${i+1}. ${t.symbol} (${t.address?.slice(0, 8)}...) - MC: $${(t.market_cap/1e6).toFixed(2)}M`);
    });

    if (tokens.length === 0) {
        console.log('无代币数据');
        return;
    }

    // 使用第一个代币测试按代币筛选钱包的 API
    const tokenAddr = tokens[0].address;
    const tokenSymbol = tokens[0].symbol;

    console.log(`\n\n========== 测试按代币(${tokenSymbol})筛选钱包 ==========\n`);

    const tokenEndpoints = [
        // 代币持有者/交易者
        { name: 'token/holders', url: `https://gmgn.ai/defi/quotation/v1/tokens/sol/holders/${tokenAddr}?limit=20&${params}` },
        { name: 'token/top_traders', url: `https://gmgn.ai/defi/quotation/v1/tokens/sol/top_traders/${tokenAddr}?period=1d&${params}` },
        { name: 'token/traders', url: `https://gmgn.ai/defi/quotation/v1/tokens/sol/traders/${tokenAddr}?period=1d&${params}` },

        // 代币交易活动
        { name: 'token/trades', url: `https://gmgn.ai/defi/quotation/v1/tokens/sol/trades/${tokenAddr}?limit=30&${params}` },
        { name: 'token/tx', url: `https://gmgn.ai/defi/quotation/v1/tokens/sol/tx/${tokenAddr}?limit=30&${params}` },

        // 智能钱包持有该代币
        { name: 'token/smart_wallets', url: `https://gmgn.ai/defi/quotation/v1/tokens/sol/smart_wallets/${tokenAddr}?${params}` },
        { name: 'token/whale_wallets', url: `https://gmgn.ai/defi/quotation/v1/tokens/sol/whale_wallets/${tokenAddr}?${params}` },

        // 钱包排行按代币筛选
        { name: 'rank/wallets + token', url: `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=buy&limit=10&token=${tokenAddr}&${params}` },
        { name: 'rank/wallets + token_address', url: `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=buy&limit=10&token_address=${tokenAddr}&${params}` },

        // 代币详情
        { name: 'tokens/top_buyers', url: `https://gmgn.ai/defi/quotation/v1/tokens/top_buyers/sol/${tokenAddr}?${params}` },
        { name: 'tokens/top_sellers', url: `https://gmgn.ai/defi/quotation/v1/tokens/top_sellers/sol/${tokenAddr}?${params}` },
    ];

    for (const ep of tokenEndpoints) {
        console.log(`📡 ${ep.name}:`);
        const data = await fetchApi(ep.url, headers);

        if (data.error) {
            console.log(`   ❌ ${data.error}`);
        } else if (data.code && data.code !== 0) {
            console.log(`   ⚠️ code: ${data.code}`);
        } else {
            console.log('   ✅ 成功!');

            const result = data.data;
            if (result) {
                // 查找钱包列表
                let wallets = null;
                if (Array.isArray(result)) {
                    wallets = result;
                } else if (result.holders) {
                    wallets = result.holders;
                } else if (result.traders) {
                    wallets = result.traders;
                } else if (result.wallets) {
                    wallets = result.wallets;
                } else if (result.trades) {
                    wallets = result.trades;
                } else if (result.rank) {
                    wallets = result.rank;
                }

                if (wallets && wallets.length > 0) {
                    console.log(`   数据量: ${wallets.length}`);
                    console.log(`   字段: ${Object.keys(wallets[0]).slice(0, 12).join(', ')}`);

                    // 显示前3个
                    wallets.slice(0, 3).forEach((w, i) => {
                        const addr = w.wallet_address || w.address || w.maker || 'N/A';
                        const name = w.twitter_name || w.name || addr?.slice(0, 8);
                        const pnl = w.realized_profit || w.pnl || w.profit || w.realized_pnl || 0;
                        console.log(`   ${i+1}. ${name?.slice(0, 15)} | PNL: $${Number(pnl).toFixed(2)}`);
                    });
                } else {
                    console.log(`   返回对象字段: ${Object.keys(result).join(', ')}`);
                }
            }
        }

        await new Promise(r => setTimeout(r, 300));
    }

    // 测试通用的 "wallet traded token X" 概念
    console.log('\n\n========== 测试钱包交易记录(带代币筛选) ==========\n');

    // 获取一个钱包地址
    const walletRankUrl = `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=buy&limit=1&${params}`;
    const walletRank = await fetchApi(walletRankUrl, headers);
    const walletAddr = walletRank.data?.rank?.[0]?.wallet_address;

    if (walletAddr) {
        console.log(`钱包: ${walletAddr.slice(0, 8)}...\n`);

        const walletTokenEndpoints = [
            `https://gmgn.ai/defi/quotation/v1/wallet_activity/sol?wallet=${walletAddr}&token=${tokenAddr}&limit=20&${params}`,
            `https://gmgn.ai/defi/quotation/v1/wallet/sol/holdings/${walletAddr}?token=${tokenAddr}&${params}`,
            `https://gmgn.ai/defi/quotation/v1/wallet/sol/pnl/${walletAddr}?token=${tokenAddr}&${params}`,
        ];

        for (const url of walletTokenEndpoints) {
            const name = url.split('?')[0].split('/').slice(-2).join('/');
            console.log(`📡 ${name}:`);

            const data = await fetchApi(url, headers);
            if (data.error) {
                console.log(`   ❌ ${data.error}`);
            } else if (data.code && data.code !== 0) {
                console.log(`   ⚠️ code: ${data.code}`);
            } else {
                console.log('   ✅ 成功!');
                if (data.data) {
                    console.log(`   数据: ${JSON.stringify(data.data).slice(0, 200)}`);
                }
            }

            await new Promise(r => setTimeout(r, 200));
        }
    }
}

testTokenWallets().catch(console.error);
