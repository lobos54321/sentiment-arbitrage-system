/**
 * 测试按代币筛选钱包排行 - 命中代币 PNL
 *
 * 假设：命中代币 = 钱包在选定代币上的交易
 * 命中代币 PNL = 钱包在这些代币上的 PNL
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

async function testTokenFilter() {
    console.log('\n========== 测试按代币筛选钱包排行 ==========\n');

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

    // 获取一个热门代币
    const swapsUrl = `https://gmgn.ai/defi/quotation/v1/rank/sol/swaps/1h?limit=5&${params}`;
    const swapsData = await fetchApi(swapsUrl, headers);
    const hotToken = swapsData.data?.rank?.[0];

    if (!hotToken) {
        console.log('无法获取热门代币');
        return;
    }

    console.log(`热门代币: ${hotToken.symbol} (${hotToken.address})\n`);

    // 测试按代币筛选的钱包排行 API
    const filterEndpoints = [
        // 直接在钱包排行中添加代币筛选
        {
            name: 'wallets + token filter',
            url: `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=buy&limit=10&token=${hotToken.address}&${params}`
        },
        {
            name: 'wallets + tokens array',
            url: `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=buy&limit=10&tokens=${hotToken.address}&${params}`
        },
        {
            name: 'wallets + token_addresses',
            url: `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=buy&limit=10&token_addresses=${hotToken.address}&${params}`
        },

        // 单独的 API
        {
            name: 'token/wallets',
            url: `https://gmgn.ai/defi/quotation/v1/token/${hotToken.address}/wallets?chain=sol&limit=20&${params}`
        },
        {
            name: 'token/sol/wallets',
            url: `https://gmgn.ai/defi/quotation/v1/token/sol/${hotToken.address}/wallets?limit=20&${params}`
        },
        {
            name: 'tokens/wallets',
            url: `https://gmgn.ai/defi/quotation/v1/tokens/${hotToken.address}/wallets?chain=sol&limit=20&${params}`
        },
        {
            name: 'tokens/sol/wallets',
            url: `https://gmgn.ai/defi/quotation/v1/tokens/sol/${hotToken.address}/wallets?limit=20&${params}`
        },

        // Smartmoney 相关
        {
            name: 'smartmoney/token/wallets',
            url: `https://gmgn.ai/defi/quotation/v1/smartmoney/sol/token/${hotToken.address}/wallets?${params}`
        },
    ];

    for (const ep of filterEndpoints) {
        console.log(`📡 ${ep.name}:`);

        const data = await fetchApi(ep.url, headers);

        if (data.error) {
            console.log(`   ❌ ${data.error}`);
        } else if (data.code && data.code !== 0) {
            console.log(`   ⚠️ code: ${data.code}`);
        } else {
            console.log('   ✅ 成功!');

            // 显示数据结构
            if (data.data) {
                let wallets = null;
                if (Array.isArray(data.data)) {
                    wallets = data.data;
                } else if (data.data.rank) {
                    wallets = data.data.rank;
                } else if (data.data.wallets) {
                    wallets = data.data.wallets;
                }

                if (wallets && wallets.length > 0) {
                    console.log(`   钱包数: ${wallets.length}`);
                    console.log(`   字段: ${Object.keys(wallets[0]).slice(0, 15).join(', ')}`);

                    // 检查是否有代币相关的 PNL 字段
                    const w = wallets[0];
                    const pnlFields = Object.keys(w).filter(k =>
                        k.includes('pnl') || k.includes('profit') || k.includes('token')
                    );
                    if (pnlFields.length > 0) {
                        console.log(`   PNL/代币字段: ${pnlFields.join(', ')}`);
                    }

                    // 显示前3个钱包
                    wallets.slice(0, 3).forEach((wallet, i) => {
                        const addr = wallet.wallet_address || wallet.address || 'N/A';
                        const name = wallet.twitter_name || wallet.name || addr?.slice(0, 8);
                        const pnl = wallet.realized_profit_1d || wallet.realized_profit || wallet.pnl || 0;
                        console.log(`   ${i+1}. ${name?.slice(0, 15)} | PNL: $${Number(pnl).toFixed(2)}`);
                    });
                } else {
                    console.log(`   数据结构: ${JSON.stringify(data.data).slice(0, 200)}`);
                }
            }
        }

        await new Promise(r => setTimeout(r, 300));
    }

    // 测试 top_buyers 是否有 PNL 字段
    console.log('\n\n========== 测试 top_buyers 完整字段 ==========\n');

    const topBuyersUrl = `https://gmgn.ai/defi/quotation/v1/tokens/top_buyers/sol/${hotToken.address}?${params}`;
    const topBuyersData = await fetchApi(topBuyersUrl, headers);

    if (topBuyersData.data?.holders?.holderInfo?.[0]) {
        const holder = topBuyersData.data.holders.holderInfo[0];
        console.log('Holder 完整字段:');
        console.log(JSON.stringify(holder, null, 2));
    }
}

testTokenFilter().catch(console.error);
