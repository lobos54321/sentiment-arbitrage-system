/**
 * 测试钱包在特定代币上的 PNL
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

async function testWalletTokenPnl() {
    console.log('\n========== 测试钱包在特定代币上的 PNL ==========\n');

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

    // 获取钱包和代币
    const walletUrl = `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=buy&limit=1&${params}`;
    const walletData = await fetchApi(walletUrl, headers);
    const wallet = walletData.data?.rank?.[0];

    const swapsUrl = `https://gmgn.ai/defi/quotation/v1/rank/sol/swaps/1h?limit=3&${params}`;
    const swapsData = await fetchApi(swapsUrl, headers);
    const tokens = swapsData.data?.rank || [];

    if (!wallet || tokens.length === 0) {
        console.log('无法获取数据');
        return;
    }

    const walletAddr = wallet.wallet_address;
    const tokenAddr = tokens[0].address;

    console.log(`钱包: ${wallet.twitter_name || wallet.name} (${walletAddr.slice(0, 8)}...)`);
    console.log(`代币: ${tokens[0].symbol} (${tokenAddr.slice(0, 8)}...)\n`);

    // 测试各种可能的 API
    const endpoints = [
        // 钱包在特定代币上的 PNL
        `https://gmgn.ai/defi/quotation/v1/wallet/sol/token_pnl/${walletAddr}/${tokenAddr}?${params}`,
        `https://gmgn.ai/defi/quotation/v1/wallet/sol/token/${tokenAddr}/pnl/${walletAddr}?${params}`,
        `https://gmgn.ai/defi/quotation/v1/wallet/sol/pnl/${walletAddr}/${tokenAddr}?${params}`,

        // 代币持有者的 PNL
        `https://gmgn.ai/defi/quotation/v1/token/sol/${tokenAddr}/holder/${walletAddr}?${params}`,
        `https://gmgn.ai/defi/quotation/v1/tokens/sol/${tokenAddr}/holder/${walletAddr}?${params}`,

        // 钱包活动按代币筛选
        `https://gmgn.ai/defi/quotation/v1/wallet_activity/sol?wallet=${walletAddr}&token=${tokenAddr}&limit=30&${params}`,

        // 钱包持仓按代币筛选
        `https://gmgn.ai/defi/quotation/v1/wallet/sol/holdings/${walletAddr}?token=${tokenAddr}&${params}`,
    ];

    for (const url of endpoints) {
        const path = url.split('?')[0].replace('https://gmgn.ai/defi/quotation/', '').slice(0, 70);
        console.log(`📡 ${path}:`);

        const data = await fetchApi(url, headers);

        if (data.error) {
            console.log(`   ❌ ${data.error}`);
        } else if (data.code && data.code !== 0) {
            console.log(`   ⚠️ code: ${data.code}`);
        } else {
            console.log('   ✅ 成功!');
            console.log(`   数据: ${JSON.stringify(data.data).slice(0, 300)}`);
        }

        await new Promise(r => setTimeout(r, 300));
    }

    // 测试 top_buyers 带额外参数
    console.log('\n\n========== 测试 top_buyers 额外参数 ==========\n');

    const buyerEndpoints = [
        `https://gmgn.ai/defi/quotation/v1/tokens/top_buyers/sol/${tokenAddr}?with_pnl=true&${params}`,
        `https://gmgn.ai/defi/quotation/v1/tokens/top_buyers/sol/${tokenAddr}?include_pnl=1&${params}`,
        `https://gmgn.ai/defi/quotation/v1/tokens/top_buyers/sol/${tokenAddr}?expand=pnl&${params}`,
        `https://gmgn.ai/defi/quotation/v1/tokens/top_buyers/sol/${tokenAddr}?detail=true&${params}`,
    ];

    for (const url of buyerEndpoints) {
        const p = url.split('?')[1]?.split('&')[0];
        console.log(`📡 ${p}:`);

        const data = await fetchApi(url, headers);

        if (data.data?.holders?.holderInfo?.[0]) {
            const holder = data.data.holders.holderInfo[0];
            const fields = Object.keys(holder);

            // 检查是否有 PNL 字段
            const pnlFields = fields.filter(f =>
                f.includes('pnl') || f.includes('profit') || f.includes('cost') || f.includes('value')
            );

            if (pnlFields.length > 0) {
                console.log(`   ✅ PNL 字段: ${pnlFields.join(', ')}`);
            } else {
                console.log(`   字段: ${fields.join(', ')}`);
            }
        } else {
            console.log(`   无数据`);
        }

        await new Promise(r => setTimeout(r, 200));
    }
}

testWalletTokenPnl().catch(console.error);
