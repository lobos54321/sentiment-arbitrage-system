/**
 * 测试 Monitor Token Wallets API - 基于CSS类名线索
 *
 * CSS: MonitorTokenWallets_table__qd6Hk
 * JSON结构: { data: { cards: [{ address, symbol, wallets: [...] }] } }
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

async function testMonitorApi() {
    console.log('\n========== 测试 Monitor Token Wallets API ==========\n');

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
        'Referer': 'https://gmgn.ai/sol/radar'
    };

    // 基于 MonitorTokenWallets 类名的各种可能端点
    const endpoints = [
        // monitor_token_wallets 相关
        `https://gmgn.ai/defi/quotation/v1/monitor_token_wallets?chain=sol&${params}`,
        `https://gmgn.ai/defi/quotation/v1/monitor_token_wallets/sol/list?${params}`,
        `https://gmgn.ai/defi/quotation/v1/monitor_token_wallets/sol/cards?${params}`,
        `https://gmgn.ai/defi/quotation/v1/monitorTokenWallets?chain=sol&${params}`,

        // radar 页面相关
        `https://gmgn.ai/defi/quotation/v1/radar?chain=sol&${params}`,
        `https://gmgn.ai/defi/quotation/v1/radar/list?chain=sol&${params}`,
        `https://gmgn.ai/defi/quotation/v1/radar/cards?chain=sol&${params}`,
        `https://gmgn.ai/defi/quotation/v1/radar/sol/list?${params}`,

        // smartmoney 相关 (带 list/monitor)
        `https://gmgn.ai/defi/quotation/v1/smartmoney/sol/monitor?${params}`,
        `https://gmgn.ai/defi/quotation/v1/smartmoney/sol/monitor/tokens?${params}`,
        `https://gmgn.ai/defi/quotation/v1/smartmoney/sol/list?${params}`,
        `https://gmgn.ai/defi/quotation/v1/smartmoney/sol/tokens?${params}`,
        `https://gmgn.ai/defi/quotation/v1/smartmoney/monitor/sol?${params}`,

        // 可能的 WebSocket 初始化 API
        `https://gmgn.ai/defi/quotation/v1/ws/monitor/sol?${params}`,

        // 带 type 参数
        `https://gmgn.ai/defi/quotation/v1/smartmoney/sol/wallets/1d?type=monitor&${params}`,
        `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?type=radar&${params}`,
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
                console.log(`   🎯 找到 cards! 数量: ${data.data.cards.length}`);
                const firstCard = data.data.cards[0];
                if (firstCard) {
                    console.log(`   代币: ${firstCard.symbol}`);
                    if (firstCard.wallets?.length > 0) {
                        console.log(`   🎯🎯🎯 包含 wallets 数组!!! (${firstCard.wallets.length})`);
                        const w = firstCard.wallets[0];
                        console.log(`   钱包字段: ${Object.keys(w).join(', ')}`);
                    }
                }
            } else if (data.data) {
                console.log(`   字段: ${Object.keys(data.data).slice(0, 10).join(', ')}`);
            }
        }

        await new Promise(r => setTimeout(r, 300));
    }

    // 测试可能需要特殊参数的端点
    console.log('\n\n========== 测试带特殊参数的端点 ==========\n');

    const specialEndpoints = [
        // orderby 参数变体
        `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=smart_buy&limit=20&${params}`,
        `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=monitor&limit=20&${params}`,

        // tag 筛选
        `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=buy&tags=smart_degen&limit=20&${params}`,

        // view 参数
        `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?view=radar&${params}`,
        `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?view=monitor&${params}`,
    ];

    for (const url of specialEndpoints) {
        const params_part = url.split('?')[1]?.split('&').slice(0, 2).join('&');
        console.log(`📡 ${params_part}:`);

        const data = await fetchApi(url, headers);

        if (data.error) {
            console.log(`   ❌ ${data.error}`);
        } else if (data.code && data.code !== 0) {
            console.log(`   ⚠️ code: ${data.code}`);
        } else if (data.data?.rank?.[0]) {
            console.log('   ✅ 成功!');
            const w = data.data.rank[0];
            // 检查是否有命中代币相关字段
            const hitFields = Object.keys(w).filter(k =>
                k.includes('token') || k.includes('hit') || k.includes('trade')
            );
            if (hitFields.length > 0) {
                console.log(`   命中相关字段: ${hitFields.join(', ')}`);
            }
            console.log(`   全部字段: ${Object.keys(w).join(', ')}`);
        }

        await new Promise(r => setTimeout(r, 200));
    }
}

testMonitorApi().catch(console.error);
