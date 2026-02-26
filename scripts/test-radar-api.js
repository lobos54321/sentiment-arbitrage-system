/**
 * 测试新发现的雷达 API
 *
 * /vas/api/v1/radar/list
 * /vas/api/v1/radar/detail
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

async function testRadarApi() {
    console.log('\n========== 测试雷达 API ==========\n');

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

    // 测试雷达列表 API
    console.log('📡 测试 /vas/api/v1/radar/list\n');

    const radarListUrl = `https://gmgn.ai/vas/api/v1/radar/list?${params}`;
    const radarListData = await fetchApi(radarListUrl, headers);

    if (radarListData.error) {
        console.log(`❌ 错误: ${radarListData.error}`);
    } else if (radarListData.code && radarListData.code !== 0) {
        console.log(`⚠️ code: ${radarListData.code} msg: ${radarListData.msg}`);
    } else {
        console.log('✅ 成功!');
        console.log(`数据: ${JSON.stringify(radarListData.data || radarListData).slice(0, 500)}`);

        // 如果有列表数据
        if (Array.isArray(radarListData.data)) {
            console.log(`\n列表数量: ${radarListData.data.length}`);
            for (const item of radarListData.data.slice(0, 3)) {
                console.log(`  - ${JSON.stringify(item).slice(0, 150)}`);
            }
        }
    }

    // 测试雷达详情 API
    console.log('\n\n📡 测试 /vas/api/v1/radar/detail\n');

    // 尝试不同的 code 参数
    const testCodes = ['ZAxgSuiP', 'default', 'sol'];

    for (const code of testCodes) {
        const detailUrl = `https://gmgn.ai/vas/api/v1/radar/detail?code=${code}&${params}`;
        console.log(`\n尝试 code=${code}:`);

        const detailData = await fetchApi(detailUrl, headers);

        if (detailData.error) {
            console.log(`   ❌ 错误: ${detailData.error}`);
        } else if (detailData.code && detailData.code !== 0) {
            console.log(`   ⚠️ code: ${detailData.code}`);
        } else {
            console.log('   ✅ 成功!');

            if (detailData.data) {
                console.log(`   顶级字段: ${Object.keys(detailData.data).join(', ')}`);

                // 检查是否有 wallets 或 cards
                if (detailData.data.wallets) {
                    console.log(`   🎯 wallets: ${detailData.data.wallets.length}`);
                }
                if (detailData.data.cards) {
                    console.log(`   🎯 cards: ${detailData.data.cards.length}`);
                }
                if (detailData.data.tokens) {
                    console.log(`   🎯 tokens: ${detailData.data.tokens.length}`);
                }

                console.log(`   数据预览: ${JSON.stringify(detailData.data).slice(0, 300)}`);
            }
        }

        await new Promise(r => setTimeout(r, 300));
    }

    // 测试更多变体
    console.log('\n\n📡 测试更多雷达 API 变体\n');

    const variants = [
        `https://gmgn.ai/vas/api/v1/radar/wallets?${params}`,
        `https://gmgn.ai/vas/api/v1/radar/tokens?${params}`,
        `https://gmgn.ai/vas/api/v1/radar/cards?${params}`,
        `https://gmgn.ai/vas/api/v1/radar/detail?code=ZAxgSuiP&chain=sol&${params}`,
        `https://gmgn.ai/vas/api/v1/radar/ZAxgSuiP?chain=sol&${params}`,
    ];

    for (const url of variants) {
        const path = url.split('?')[0].replace('https://gmgn.ai', '');
        console.log(`\n📡 ${path}:`);

        const data = await fetchApi(url, headers);
        if (data.error) {
            console.log(`   ❌ ${data.error}`);
        } else if (data.code && data.code !== 0) {
            console.log(`   ⚠️ code: ${data.code}`);
        } else {
            console.log('   ✅ 成功!');
            if (data.data) {
                const fields = Object.keys(data.data);
                console.log(`   字段: ${fields.join(', ')}`);
            }
        }

        await new Promise(r => setTimeout(r, 200));
    }
}

testRadarApi().catch(console.error);
