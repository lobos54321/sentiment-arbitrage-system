/**
 * 测试 GMGN WebSocket 和 GraphQL API
 *
 * 基于 CSS: MonitorTokenWallets_table__qd6Hk
 * 可能是通过 WebSocket 推送或 GraphQL 查询获取的数据
 */

import fs from 'fs';

const DEVICE_ID = '1d29f750-687f-42e1-851d-59a43e5d2ffa';
const CLIENT_ID = 'gmgn_web_test';

async function fetchApi(url, headers, method = 'GET', body = null) {
    try {
        const options = { method, headers };
        if (body) options.body = JSON.stringify(body);
        const response = await fetch(url, options);
        if (!response.ok) return { error: response.status };
        const text = await response.text();
        try {
            return JSON.parse(text);
        } catch {
            return { raw: text.slice(0, 500) };
        }
    } catch (e) {
        return { error: e.message };
    }
}

async function testAdvancedApi() {
    console.log('\n========== 测试高级 API 端点 ==========\n');

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
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Cookie': cookieStr,
        'Origin': 'https://gmgn.ai',
        'Referer': 'https://gmgn.ai/sol/radar'
    };

    // 测试不同的 API 版本和路径
    const endpoints = [
        // v2 API
        `https://gmgn.ai/defi/quotation/v2/smartmoney/sol/cards?${params}`,
        `https://gmgn.ai/defi/quotation/v2/monitor/sol/tokens?${params}`,

        // API 前缀变体
        `https://gmgn.ai/api/v1/smartmoney/sol/cards?${params}`,
        `https://gmgn.ai/api/v1/monitor_token_wallets/sol?${params}`,

        // defi 路径变体
        `https://gmgn.ai/defi/v1/smartmoney/sol/cards?${params}`,
        `https://gmgn.ai/defi/smartmoney/sol/cards?${params}`,

        // 用户相关端点 (可能需要登录)
        `https://gmgn.ai/defi/quotation/v1/user/monitor/tokens?chain=sol&${params}`,
        `https://gmgn.ai/defi/quotation/v1/user/smartmoney/cards?chain=sol&${params}`,
        `https://gmgn.ai/defi/quotation/v1/user/radar/cards?chain=sol&${params}`,

        // follow/watch 相关
        `https://gmgn.ai/defi/quotation/v1/follow/sol/tokens?${params}`,
        `https://gmgn.ai/defi/quotation/v1/watch/sol/tokens?${params}`,

        // panel/dashboard
        `https://gmgn.ai/defi/quotation/v1/panel/sol/tokens?${params}`,
        `https://gmgn.ai/defi/quotation/v1/dashboard/sol/cards?${params}`,

        // 最近交易
        `https://gmgn.ai/defi/quotation/v1/recent/sol/tokens?${params}`,
        `https://gmgn.ai/defi/quotation/v1/latest/sol/cards?${params}`,

        // feed
        `https://gmgn.ai/defi/quotation/v1/feed/sol/tokens?${params}`,
        `https://gmgn.ai/defi/quotation/v1/feed/sol/cards?${params}`,
    ];

    for (const url of endpoints) {
        const path = url.split('?')[0].replace('https://gmgn.ai/', '');
        console.log(`📡 ${path}:`);

        const data = await fetchApi(url, headers);

        if (data.error) {
            console.log(`   ❌ ${data.error}`);
        } else if (data.raw) {
            console.log(`   📄 ${data.raw.slice(0, 100)}`);
        } else if (data.code && data.code !== 0) {
            console.log(`   ⚠️ code: ${data.code} msg: ${data.msg || ''}`);
        } else {
            console.log('   ✅ 成功!');
            if (data.data?.cards) {
                console.log(`   🎯 找到 cards: ${data.data.cards.length}`);
            } else if (data.data) {
                console.log(`   字段: ${JSON.stringify(data.data).slice(0, 200)}`);
            }
        }

        await new Promise(r => setTimeout(r, 200));
    }

    // 测试 WebSocket 相关的 HTTP 端点
    console.log('\n\n========== 测试 WebSocket 初始化端点 ==========\n');

    const wsEndpoints = [
        `https://gmgn.ai/socket.io/?EIO=4&transport=polling&${params}`,
        `https://gmgn.ai/ws/init?chain=sol&${params}`,
        `https://gmgn.ai/defi/ws/init?chain=sol&${params}`,
    ];

    for (const url of wsEndpoints) {
        const path = url.split('?')[0].replace('https://gmgn.ai/', '');
        console.log(`📡 ${path}:`);

        const data = await fetchApi(url, headers);
        if (data.error) {
            console.log(`   ❌ ${data.error}`);
        } else if (data.raw) {
            console.log(`   📄 ${data.raw.slice(0, 150)}`);
        } else {
            console.log(`   数据: ${JSON.stringify(data).slice(0, 200)}`);
        }

        await new Promise(r => setTimeout(r, 200));
    }

    // 测试带身份验证的端点
    console.log('\n\n========== 测试需要登录的端点 ==========\n');

    // 检查是否有登录token
    const authCookies = cookies.filter(c => c.name.includes('token') || c.name.includes('auth') || c.name.includes('session'));
    console.log(`Auth cookies: ${authCookies.map(c => c.name).join(', ') || 'none'}`);

    // 尝试获取用户信息
    const userUrl = `https://gmgn.ai/defi/quotation/v1/user/info?${params}`;
    console.log(`\n📡 用户信息:`);
    const userData = await fetchApi(userUrl, headers);
    if (userData.error) {
        console.log(`   ❌ ${userData.error}`);
    } else if (userData.code && userData.code !== 0) {
        console.log(`   ⚠️ ${userData.msg || 'not logged in'}`);
    } else {
        console.log(`   ✅ 已登录: ${JSON.stringify(userData.data).slice(0, 200)}`);
    }
}

testAdvancedApi().catch(console.error);
