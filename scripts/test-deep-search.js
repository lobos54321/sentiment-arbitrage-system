/**
 * 深度测试 - 寻找命中代币数据
 *
 * 根据截图：
 * - 命中代币列显示多个代币图标
 * - 这可能是钱包排行 API 的额外参数，或者是单独的 API
 */

import fs from 'fs';

const DEVICE_ID = '1d29f750-687f-42e1-851d-59a43e5d2ffa';
const CLIENT_ID = 'gmgn_web_test';

async function fetchApi(url, headers) {
    try {
        const response = await fetch(url, { headers });
        const text = await response.text();
        try {
            return JSON.parse(text);
        } catch {
            return { error: 'parse_error', text: text.slice(0, 200) };
        }
    } catch (e) {
        return { error: e.message };
    }
}

async function deepSearch() {
    console.log('\n========== 深度搜索命中代币 API ==========\n');

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

    // 测试各种可能包含命中代币的端点
    const endpoints = [
        // 排行榜变体
        `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=buy&limit=5&show_hit_tokens=true&${params}`,
        `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=buy&limit=5&include_tokens=true&${params}`,
        `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=buy&limit=5&with_hit_tokens=1&${params}`,
        `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=buy&limit=5&expand=tokens&${params}`,

        // trade 页面相关 (雷达URL是 /trade/ZAxgSuiP)
        `https://gmgn.ai/defi/quotation/v1/trade/sol/wallets?period=1d&orderby=buy&limit=5&${params}`,
        `https://gmgn.ai/defi/quotation/v1/trade/sol/rank?period=1d&orderby=buy&limit=5&${params}`,

        // 雷达 / 监控
        `https://gmgn.ai/defi/quotation/v1/monitor/sol/wallets?period=1d&${params}`,
        `https://gmgn.ai/defi/quotation/v1/watch/sol/wallets?period=1d&${params}`,

        // 聪明钱带代币
        `https://gmgn.ai/defi/quotation/v1/smartmoney/sol/rank?period=1d&orderby=buy&limit=5&${params}`,
        `https://gmgn.ai/defi/quotation/v1/smartmoney/sol/leaderboard?period=1d&${params}`,

        // 获取单个钱包的命中代币
        `https://gmgn.ai/defi/quotation/v1/wallet/sol/hit_tokens/CyaE1VxvBrahnPWkqm5VsdCvyS2QmNht2UFrKJHga54o?period=1d&${params}`,
        `https://gmgn.ai/defi/quotation/v1/wallet/sol/traded_tokens/CyaE1VxvBrahnPWkqm5VsdCvyS2QmNht2UFrKJHga54o?period=1d&${params}`,
        `https://gmgn.ai/defi/quotation/v1/wallet/sol/tokens/CyaE1VxvBrahnPWkqm5VsdCvyS2QmNht2UFrKJHga54o?period=1d&${params}`,
    ];

    for (const url of endpoints) {
        const shortUrl = url.split('?')[0].replace('https://gmgn.ai/defi/quotation/', '');
        console.log(`📡 ${shortUrl.slice(0, 70)}:`);

        const data = await fetchApi(url, headers);

        if (data.error) {
            console.log(`   ❌ ${data.error}`);
        } else if (data.code && data.code !== 0) {
            console.log(`   ⚠️ code: ${data.code}`);
        } else if (data.data) {
            console.log('   ✅ 成功!');

            // 递归搜索包含 "token" 的字段
            const searchTokens = (obj, path = '') => {
                if (!obj || typeof obj !== 'object') return;

                for (const key of Object.keys(obj)) {
                    const newPath = path ? `${path}.${key}` : key;
                    const value = obj[key];

                    if (key.toLowerCase().includes('token') ||
                        key.toLowerCase().includes('hit') ||
                        key.toLowerCase().includes('symbol')) {

                        if (Array.isArray(value) && value.length > 0) {
                            console.log(`   🎯 ${newPath}: Array[${value.length}]`);
                            if (typeof value[0] === 'object') {
                                console.log(`      首项字段: ${Object.keys(value[0]).join(', ')}`);
                            } else {
                                console.log(`      首项: ${value[0]}`);
                            }
                        } else if (value && typeof value === 'object') {
                            console.log(`   🎯 ${newPath}: Object`);
                            console.log(`      字段: ${Object.keys(value).join(', ')}`);
                        } else if (value !== null && value !== undefined) {
                            console.log(`   🎯 ${newPath}: ${JSON.stringify(value).slice(0, 50)}`);
                        }
                    }

                    // 继续深入搜索
                    if (Array.isArray(value)) {
                        if (value.length > 0 && typeof value[0] === 'object') {
                            searchTokens(value[0], `${newPath}[0]`);
                        }
                    } else if (value && typeof value === 'object') {
                        searchTokens(value, newPath);
                    }
                }
            };

            searchTokens(data.data);
        }

        await new Promise(r => setTimeout(r, 200));
    }

    // 测试顶部热门代币 API
    console.log('\n\n========== 测试热门代币 API ==========\n');

    const hotEndpoints = [
        `https://gmgn.ai/defi/quotation/v1/tokens/sol/hot?period=1d&${params}`,
        `https://gmgn.ai/defi/quotation/v1/tokens/sol/trending?period=1d&${params}`,
        `https://gmgn.ai/defi/quotation/v1/tokens/sol/top?period=1d&${params}`,
        `https://gmgn.ai/defi/quotation/v1/rank/sol/tokens/1d?limit=10&${params}`,
        `https://gmgn.ai/defi/quotation/v1/rank/sol/tokens/1h?limit=10&${params}`,
    ];

    for (const url of hotEndpoints) {
        const shortUrl = url.split('?')[0].replace('https://gmgn.ai/defi/quotation/', '');
        console.log(`📡 ${shortUrl}:`);

        const data = await fetchApi(url, headers);

        if (data.error) {
            console.log(`   ❌ ${data.error}`);
        } else if (data.code && data.code !== 0) {
            console.log(`   ⚠️ code: ${data.code}`);
        } else if (data.data) {
            console.log('   ✅ 成功!');
            if (Array.isArray(data.data)) {
                console.log(`   数组 [${data.data.length}]`);
                if (data.data.length > 0) {
                    console.log(`   字段: ${Object.keys(data.data[0]).slice(0, 10).join(', ')}`);
                    // 显示前3个代币
                    data.data.slice(0, 3).forEach((t, i) => {
                        console.log(`   ${i+1}. ${t.symbol || t.name || 'N/A'} - $${(t.market_cap/1e6 || 0).toFixed(2)}M`);
                    });
                }
            } else if (data.data.rank) {
                console.log(`   rank [${data.data.rank.length}]`);
            }
        }

        await new Promise(r => setTimeout(r, 200));
    }
}

deepSearch().catch(console.error);
