/**
 * 测试 GMGN SmartMoney Cards API - 命中代币数据
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

async function testSmartMoneyCards() {
    console.log('\n========== 测试 SmartMoney Cards API ==========\n');

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

    // 测试可能的 SmartMoney Cards 端点
    const endpoints = [
        // 主要候选
        `https://gmgn.ai/defi/quotation/v1/smartmoney/sol/cards?${params}`,
        `https://gmgn.ai/defi/quotation/v1/smartmoney/sol/cards?limit=20&${params}`,
        `https://gmgn.ai/defi/quotation/v1/smartmoney/sol/wallets/cards?${params}`,

        // Monitor 相关
        `https://gmgn.ai/defi/quotation/v1/monitor/sol/tokens?${params}`,
        `https://gmgn.ai/defi/quotation/v1/monitor/sol/cards?${params}`,
        `https://gmgn.ai/defi/quotation/v1/monitor_token_wallets/sol?${params}`,

        // Radar/Trade 页面
        `https://gmgn.ai/defi/quotation/v1/trade/sol/cards?${params}`,
        `https://gmgn.ai/defi/quotation/v1/radar/sol/cards?${params}`,

        // 新代币相关
        `https://gmgn.ai/defi/quotation/v1/tokens/sol/new?${params}`,
        `https://gmgn.ai/defi/quotation/v1/tokens/sol/hot?${params}`,

        // Signals
        `https://gmgn.ai/defi/quotation/v1/signals/sol/tokens?${params}`,
        `https://gmgn.ai/defi/quotation/v1/signals/sol/cards?${params}`,
    ];

    for (const url of endpoints) {
        const path = url.split('?')[0].replace('https://gmgn.ai/defi/quotation/', '');
        console.log(`📡 ${path}:`);

        const data = await fetchApi(url, headers);

        if (data.error) {
            console.log(`   ❌ ${data.error}`);
        } else if (data.code && data.code !== 0) {
            console.log(`   ⚠️ code: ${data.code}`);
        } else {
            console.log('   ✅ 成功!');

            // 检查是否有 cards 数据
            if (data.data?.cards) {
                console.log(`   cards 数量: ${data.data.cards.length}`);

                // 检查第一个 card 是否有 wallets
                const firstCard = data.data.cards[0];
                if (firstCard) {
                    console.log(`   首个代币: ${firstCard.symbol}`);
                    if (firstCard.wallets && firstCard.wallets.length > 0) {
                        console.log(`   🎯 包含 wallets 数组! (${firstCard.wallets.length} 个钱包)`);
                        const w = firstCard.wallets[0];
                        console.log(`      钱包示例: ${w.twitter_name || w.wallet_address?.slice(0, 8)}`);
                        console.log(`      net_inflow: ${w.net_inflow}`);
                        console.log(`      buys/sells: ${w.buys}/${w.sells}`);
                    }
                }
            } else if (Array.isArray(data.data)) {
                console.log(`   数组长度: ${data.data.length}`);
            } else if (data.data) {
                console.log(`   字段: ${Object.keys(data.data).slice(0, 10).join(', ')}`);
            }
        }

        await new Promise(r => setTimeout(r, 300));
    }
}

testSmartMoneyCards().catch(console.error);
