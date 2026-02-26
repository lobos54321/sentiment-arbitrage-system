/**
 * 测试 GMGN 雷达 - 命中代币
 *
 * 根据截图分析：
 * - 顶部有热门代币标签 (Yaranaika, 114514, fish, AMEEN, NANJ)
 * - "命中代币" 列显示钱包交易过的代币图标
 * - "命中代币PNL" 是这些代币的总 PNL
 *
 * 需要找到：
 * 1. 热门代币列表 API
 * 2. 钱包命中代币列表 API
 */

import fs from 'fs';

const DEVICE_ID = '1d29f750-687f-42e1-851d-59a43e5d2ffa';
const CLIENT_ID = 'gmgn_web_test';

async function fetchApi(url, headers) {
    try {
        const response = await fetch(url, { headers });
        if (!response.ok) {
            return { error: response.status };
        }
        return await response.json();
    } catch (e) {
        return { error: e.message };
    }
}

async function testHitTokens() {
    console.log('\n========== GMGN 雷达 - 命中代币分析 ==========\n');

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

    // 测试雷达页面可能用的 API 端点
    console.log('📡 测试雷达相关 API 端点...\n');

    const radarEndpoints = [
        // 雷达热门代币
        { name: 'radar/hot_tokens', url: `https://gmgn.ai/defi/quotation/v1/radar/sol/hot_tokens?period=1d&${params}` },
        { name: 'radar/tokens', url: `https://gmgn.ai/defi/quotation/v1/radar/sol/tokens?period=1d&${params}` },
        { name: 'radar/trending', url: `https://gmgn.ai/defi/quotation/v1/radar/sol/trending?period=1d&${params}` },

        // 雷达钱包排行 (带命中代币)
        { name: 'radar/wallets', url: `https://gmgn.ai/defi/quotation/v1/radar/sol/wallets?period=1d&orderby=buy&limit=20&${params}` },
        { name: 'radar/wallets/1d', url: `https://gmgn.ai/defi/quotation/v1/radar/sol/wallets/1d?orderby=buy&limit=20&${params}` },

        // 聪明钱雷达
        { name: 'smartmoney/radar', url: `https://gmgn.ai/defi/quotation/v1/smartmoney/sol/radar?period=1d&${params}` },
        { name: 'smartmoney/wallets', url: `https://gmgn.ai/defi/quotation/v1/smartmoney/sol/wallets?period=1d&orderby=buy&limit=20&${params}` },

        // 信号/热点
        { name: 'signals/sol', url: `https://gmgn.ai/defi/quotation/v1/signals/sol?period=1d&${params}` },
        { name: 'hot/sol', url: `https://gmgn.ai/defi/quotation/v1/hot/sol?period=1d&${params}` },

        // 排行榜带代币信息
        { name: 'rank/wallets (带 tokens)', url: `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=buy&limit=10&with_tokens=true&${params}` },
        { name: 'rank/wallets (带 hit_tokens)', url: `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=buy&limit=10&hit_tokens=true&${params}` },
    ];

    for (const ep of radarEndpoints) {
        console.log(`📡 ${ep.name}:`);
        const data = await fetchApi(ep.url, headers);

        if (data.error) {
            console.log(`   ❌ ${data.error}`);
        } else if (data.code && data.code !== 0) {
            console.log(`   ⚠️ code: ${data.code}`);
        } else {
            console.log('   ✅ 成功!');

            if (data.data) {
                if (Array.isArray(data.data)) {
                    console.log(`   数组 [${data.data.length}]`);
                    if (data.data.length > 0) {
                        const fields = Object.keys(data.data[0]);
                        console.log(`   字段: ${fields.slice(0, 12).join(', ')}`);

                        // 查找代币相关字段
                        const tokenFields = fields.filter(f =>
                            f.includes('token') || f.includes('hit') || f.includes('symbol')
                        );
                        if (tokenFields.length > 0) {
                            console.log(`   代币字段: ${tokenFields.join(', ')}`);
                        }
                    }
                } else if (data.data.rank) {
                    console.log(`   rank 数组 [${data.data.rank.length}]`);
                    if (data.data.rank.length > 0) {
                        const fields = Object.keys(data.data.rank[0]);
                        console.log(`   字段: ${fields.slice(0, 12).join(', ')}`);

                        // 查找代币相关字段
                        const tokenFields = fields.filter(f =>
                            f.includes('token') || f.includes('hit') || f.includes('symbol')
                        );
                        if (tokenFields.length > 0) {
                            console.log(`   代币字段: ${tokenFields.join(', ')}`);
                            // 显示代币数据
                            const w = data.data.rank[0];
                            tokenFields.forEach(f => {
                                console.log(`     ${f}: ${JSON.stringify(w[f])?.slice(0, 100)}`);
                            });
                        }
                    }
                } else {
                    console.log(`   对象字段: ${Object.keys(data.data).slice(0, 10).join(', ')}`);
                }
            }
        }

        await new Promise(r => setTimeout(r, 300));
    }

    // 再次检查标准钱包排行的完整字段
    console.log('\n\n========== 钱包排行完整字段分析 ==========\n');

    const rankUrl = `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=buy&limit=5&${params}`;
    const rankData = await fetchApi(rankUrl, headers);

    if (rankData.data?.rank?.length > 0) {
        const wallet = rankData.data.rank[0];
        console.log('钱包数据所有字段:\n');

        const allFields = Object.keys(wallet);
        allFields.forEach(field => {
            const value = wallet[field];
            const valueStr = JSON.stringify(value);
            console.log(`  ${field}: ${valueStr?.slice(0, 80)}${valueStr?.length > 80 ? '...' : ''}`);
        });
    }
}

testHitTokens().catch(console.error);
