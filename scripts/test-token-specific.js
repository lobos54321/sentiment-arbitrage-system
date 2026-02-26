/**
 * 基于用户提供的JSON数据结构分析
 *
 * JSON 显示:
 * - address: "CS9Fs7DUZXC8PtjVPRsfaW8QEkLzxdX2etd1a5trpump"
 * - symbol: "runner"
 * - wallets 数组包含: net_inflow, buys, sells, side, is_open_or_close, tags
 *
 * 这看起来像是 "智能钱包监控" 的实时数据
 * 让我们测试一些可能的组合
 */

import fs from 'fs';

const DEVICE_ID = '1d29f750-687f-42e1-851d-59a43e5d2ffa';
const CLIENT_ID = 'gmgn_web_test';

async function fetchApi(url, headers) {
    try {
        const response = await fetch(url, { headers });
        if (!response.ok) return { error: response.status, headers: Object.fromEntries(response.headers) };
        return await response.json();
    } catch (e) {
        return { error: e.message };
    }
}

async function testTokenSpecific() {
    console.log('\n========== 基于代币地址测试 API ==========\n');

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

    // 用户提供的代币地址
    const tokenAddr = 'CS9Fs7DUZXC8PtjVPRsfaW8QEkLzxdX2etd1a5trpump';

    // 针对这个特定代币测试各种端点
    const endpoints = [
        // 代币 + 智能钱包
        `https://gmgn.ai/defi/quotation/v1/tokens/sol/${tokenAddr}/smart_wallets?${params}`,
        `https://gmgn.ai/defi/quotation/v1/token/sol/${tokenAddr}/smart_wallets?${params}`,
        `https://gmgn.ai/defi/quotation/v1/smartmoney/sol/token/${tokenAddr}/wallets?${params}`,

        // 代币 + 活跃钱包
        `https://gmgn.ai/defi/quotation/v1/tokens/sol/${tokenAddr}/active_wallets?${params}`,
        `https://gmgn.ai/defi/quotation/v1/tokens/sol/${tokenAddr}/traders?${params}`,

        // 代币详情
        `https://gmgn.ai/defi/quotation/v1/tokens/sol/${tokenAddr}?${params}`,
        `https://gmgn.ai/defi/quotation/v1/token/sol/${tokenAddr}?${params}`,

        // 代币 + 买卖信息
        `https://gmgn.ai/defi/quotation/v1/tokens/sol/${tokenAddr}/trades?${params}`,
        `https://gmgn.ai/defi/quotation/v1/tokens/sol/${tokenAddr}/buys?${params}`,

        // top holders
        `https://gmgn.ai/defi/quotation/v1/tokens/top_holders/sol/${tokenAddr}?${params}`,
        `https://gmgn.ai/defi/quotation/v1/tokens/sol/${tokenAddr}/holders?${params}`,

        // 监控相关
        `https://gmgn.ai/defi/quotation/v1/token/sol/${tokenAddr}/monitor?${params}`,
        `https://gmgn.ai/defi/quotation/v1/tokens/sol/${tokenAddr}/monitor_wallets?${params}`,
    ];

    for (const url of endpoints) {
        const path = url.split('?')[0].replace('https://gmgn.ai/defi/quotation/', '').replace(tokenAddr, '{TOKEN}');
        console.log(`📡 ${path}:`);

        const data = await fetchApi(url, headers);

        if (data.error) {
            console.log(`   ❌ ${data.error}`);
        } else if (data.code && data.code !== 0) {
            console.log(`   ⚠️ code: ${data.code}`);
        } else {
            console.log('   ✅ 成功!');

            // 检查返回数据结构
            if (data.data) {
                if (data.data.wallets) {
                    console.log(`   🎯 找到 wallets: ${data.data.wallets.length}`);
                    if (data.data.wallets[0]) {
                        console.log(`   字段: ${Object.keys(data.data.wallets[0]).join(', ')}`);
                    }
                } else if (data.data.holders) {
                    console.log(`   持有者数据: ${JSON.stringify(data.data).slice(0, 200)}`);
                } else if (Array.isArray(data.data)) {
                    console.log(`   数组长度: ${data.data.length}`);
                    if (data.data[0]) {
                        console.log(`   第一项字段: ${Object.keys(data.data[0]).slice(0, 10).join(', ')}`);
                    }
                } else {
                    console.log(`   顶级字段: ${Object.keys(data.data).slice(0, 15).join(', ')}`);
                }
            }
        }

        await new Promise(r => setTimeout(r, 250));
    }

    // 获取一个当前热门代币来测试
    console.log('\n\n========== 使用当前热门代币测试 ==========\n');

    const swapsUrl = `https://gmgn.ai/defi/quotation/v1/rank/sol/swaps/1h?limit=3&${params}`;
    const swapsData = await fetchApi(swapsUrl, headers);
    const hotTokens = swapsData.data?.rank || [];

    if (hotTokens.length > 0) {
        const hotToken = hotTokens[0];
        console.log(`热门代币: ${hotToken.symbol} (${hotToken.address.slice(0, 12)}...)\n`);

        // 测试 top_buyers 完整数据
        const buyersUrl = `https://gmgn.ai/defi/quotation/v1/tokens/top_buyers/sol/${hotToken.address}?${params}`;
        console.log('📡 top_buyers 完整响应:');

        const buyersData = await fetchApi(buyersUrl, headers);
        if (buyersData.data) {
            console.log(`   顶级字段: ${Object.keys(buyersData.data).join(', ')}`);

            if (buyersData.data.holders?.holderInfo?.[0]) {
                const holder = buyersData.data.holders.holderInfo[0];
                console.log(`\n   第一个 holder 完整字段:`);
                for (const [k, v] of Object.entries(holder)) {
                    const val = typeof v === 'object' ? JSON.stringify(v).slice(0, 50) : String(v).slice(0, 50);
                    console.log(`     ${k}: ${val}`);
                }
            }
        }
    }
}

testTokenSpecific().catch(console.error);
