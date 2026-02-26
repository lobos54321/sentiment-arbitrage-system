/**
 * 综合测试 - 关联钱包和命中代币
 *
 * 思路：
 * 1. 获取热门代币列表
 * 2. 对每个热门代币获取 top_buyers
 * 3. 找到每个钱包命中了哪些热门代币
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

async function findHitTokens() {
    console.log('\n========== 关联钱包和命中代币 ==========\n');

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

    // 1. 获取热门代币
    console.log('📊 获取热门代币...');
    const swapsUrl = `https://gmgn.ai/defi/quotation/v1/rank/sol/swaps/1h?limit=10&${params}`;
    const swapsData = await fetchApi(swapsUrl, headers);
    const hotTokens = swapsData.data?.rank || [];

    console.log(`找到 ${hotTokens.length} 个热门代币:`);
    hotTokens.forEach((t, i) => console.log(`  ${i+1}. ${t.symbol} (${t.address.slice(0, 8)}...)`));

    // 2. 获取钱包排行
    console.log('\n📊 获取钱包排行...');
    const walletUrl = `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=buy&limit=10&${params}`;
    const walletData = await fetchApi(walletUrl, headers);
    const wallets = walletData.data?.rank || [];

    console.log(`找到 ${wallets.length} 个钱包`);

    // 创建钱包地址到名称的映射
    const walletNames = {};
    wallets.forEach(w => {
        walletNames[w.wallet_address] = w.twitter_name || w.name || w.wallet_address.slice(0, 8);
    });

    // 3. 对每个热门代币获取买家，建立钱包-代币关联
    console.log('\n📊 建立钱包-代币关联...\n');

    const walletTokens = {}; // 钱包地址 -> [代币信息]

    for (const token of hotTokens.slice(0, 8)) {
        const buyersUrl = `https://gmgn.ai/defi/quotation/v1/tokens/top_buyers/sol/${token.address}?${params}`;
        const buyersData = await fetchApi(buyersUrl, headers);

        if (buyersData.data?.holders?.holderInfo) {
            const holders = buyersData.data.holders.holderInfo;

            // 查找在钱包排行榜中的买家
            for (const holder of holders) {
                if (walletNames[holder.wallet_address]) {
                    if (!walletTokens[holder.wallet_address]) {
                        walletTokens[holder.wallet_address] = [];
                    }
                    walletTokens[holder.wallet_address].push({
                        symbol: token.symbol,
                        logo: token.logo,
                        status: holder.status || 'hold',
                        tags: holder.maker_token_tags || [],
                        marketCap: token.market_cap
                    });
                }
            }
        }

        await new Promise(r => setTimeout(r, 300));
    }

    // 4. 显示结果
    console.log('========== 钱包命中代币结果 ==========\n');

    for (const w of wallets) {
        const addr = w.wallet_address;
        const name = walletNames[addr];
        const hitTokens = walletTokens[addr] || [];

        console.log(`👤 ${name}`);
        console.log(`   地址: ${addr.slice(0, 12)}...`);
        console.log(`   1D PNL: $${Math.round(Number(w.realized_profit_1d) || 0)} | 胜率: ${((w.winrate_1d || 0) * 100).toFixed(1)}%`);

        if (hitTokens.length > 0) {
            console.log(`   命中代币 (${hitTokens.length}个):`);
            hitTokens.forEach(t => {
                const statusIcon = t.status === 'sold' ? '💰' : '📦';
                console.log(`     ${statusIcon} ${t.symbol} (MC: $${(t.marketCap/1e6).toFixed(2)}M) - ${t.status}`);
            });
        } else {
            console.log('   命中代币: 无(未在当前热门代币中)');
        }

        console.log('');
    }

    // 额外：测试是否有更直接的 API
    console.log('\n========== 测试直接的命中代币 API ==========\n');

    const testWallet = wallets[0]?.wallet_address;
    if (testWallet) {
        const directEndpoints = [
            `https://gmgn.ai/defi/quotation/v1/wallet/sol/hit_tokens/${testWallet}?${params}`,
            `https://gmgn.ai/defi/quotation/v1/wallet/sol/traded/${testWallet}?period=1d&${params}`,
            `https://gmgn.ai/defi/quotation/v1/wallet/sol/traded_tokens/${testWallet}?${params}`,
            `https://gmgn.ai/defi/quotation/v1/smartmoney/sol/walletNew/${testWallet}/tokens?${params}`,
        ];

        for (const url of directEndpoints) {
            const path = url.split('?')[0].split('/').slice(-3).join('/');
            console.log(`📡 ${path}:`);

            const data = await fetchApi(url, headers);
            if (data.error) {
                console.log(`   ❌ ${data.error}`);
            } else if (data.code && data.code !== 0) {
                console.log(`   ⚠️ code: ${data.code}`);
            } else {
                console.log('   ✅ 成功!');
                console.log(`   数据: ${JSON.stringify(data.data).slice(0, 200)}`);
            }

            await new Promise(r => setTimeout(r, 200));
        }
    }
}

findHitTokens().catch(console.error);
