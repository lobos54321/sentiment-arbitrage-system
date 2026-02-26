/**
 * 深度测试 - 寻找命中代币字段
 *
 * 根据截图：雷达页面 URL 是 https://gmgn.ai/trade/ZAxgSuiP?chain=sol
 * ZAxgSuiP 可能是某种标识符
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

async function deepTest() {
    console.log('\n========== 深度测试命中代币 API ==========\n');

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
        'Referer': 'https://gmgn.ai/trade/ZAxgSuiP?chain=sol'  // 使用雷达页面的 URL
    };

    // 测试雷达相关的 API 路径
    console.log('测试不同的 API 路径...\n');

    const endpoints = [
        // 使用 trade 页面的标识符
        `https://gmgn.ai/defi/quotation/v1/trade/ZAxgSuiP?chain=sol&${params}`,
        `https://gmgn.ai/defi/quotation/v1/trade/sol/ZAxgSuiP?${params}`,

        // 雷达 API
        `https://gmgn.ai/defi/quotation/v1/radar?chain=sol&${params}`,
        `https://gmgn.ai/defi/quotation/v1/radar/sol?${params}`,

        // 钱包排行 - 不同版本
        `https://gmgn.ai/defi/quotation/v2/rank/sol/wallets/1d?orderby=buy&limit=5&${params}`,

        // 带 show_tokens 参数
        `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=buy&limit=5&show_tokens=1&${params}`,
        `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=buy&limit=5&with_tokens=1&${params}`,
        `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=buy&limit=5&include_hit_tokens=1&${params}`,
        `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=buy&limit=5&expand=hit_tokens&${params}`,

        // 热门代币相关
        `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d/tokens?limit=10&${params}`,
        `https://gmgn.ai/defi/quotation/v1/rank/sol/hot_tokens?period=1d&${params}`,
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

            // 深度搜索包含 token/tokens/hit 的字段
            const searchFields = (obj, prefix = '') => {
                if (!obj || typeof obj !== 'object') return;

                for (const [key, value] of Object.entries(obj)) {
                    const fullKey = prefix ? `${prefix}.${key}` : key;

                    // 检查键名是否包含 token 或 hit
                    if (key.toLowerCase().includes('token') || key.toLowerCase().includes('hit')) {
                        if (Array.isArray(value) && value.length > 0) {
                            console.log(`   🎯 ${fullKey}: Array[${value.length}]`);
                            if (typeof value[0] === 'object') {
                                console.log(`      字段: ${Object.keys(value[0]).slice(0, 8).join(', ')}`);
                            }
                        } else if (value !== null && value !== undefined) {
                            console.log(`   🎯 ${fullKey}: ${JSON.stringify(value).slice(0, 80)}`);
                        }
                    }

                    // 继续搜索
                    if (Array.isArray(value) && value.length > 0 && typeof value[0] === 'object') {
                        searchFields(value[0], `${fullKey}[0]`);
                    } else if (value && typeof value === 'object' && !Array.isArray(value)) {
                        searchFields(value, fullKey);
                    }
                }
            };

            searchFields(data);

            // 显示顶级字段
            if (data.data) {
                if (Array.isArray(data.data)) {
                    console.log(`   数据: Array[${data.data.length}]`);
                } else if (data.data.rank) {
                    console.log(`   数据: rank[${data.data.rank.length}]`);
                } else {
                    console.log(`   数据字段: ${Object.keys(data.data).slice(0, 10).join(', ')}`);
                }
            }
        }

        await new Promise(r => setTimeout(r, 300));
    }

    // 测试是否有单独的热门代币 API
    console.log('\n\n========== 测试热门代币列表 API ==========\n');

    const hotTokenEndpoints = [
        `https://gmgn.ai/defi/quotation/v1/rank/sol/swaps/1d?limit=20&${params}`,
        `https://gmgn.ai/defi/quotation/v1/rank/sol/swaps/1h?limit=20&${params}`,
    ];

    for (const url of hotTokenEndpoints) {
        const path = url.split('?')[0].replace('https://gmgn.ai/defi/quotation/', '');
        console.log(`\n📡 ${path}:`);

        const data = await fetchApi(url, headers);

        if (data.data?.rank) {
            console.log(`   获取 ${data.data.rank.length} 个代币`);
            console.log('\n   热门代币 (前10):');
            data.data.rank.slice(0, 10).forEach((t, i) => {
                console.log(`   ${i+1}. ${t.symbol.padEnd(15)} | MC: $${(t.market_cap/1e6).toFixed(2).padStart(8)}M | Vol: $${(t.volume/1e3).toFixed(1).padStart(8)}K`);
            });
        }
    }
}

deepTest().catch(console.error);
