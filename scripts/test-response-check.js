/**
 * 分析 parse_error 的响应内容
 */

import fs from 'fs';

const DEVICE_ID = '1d29f750-687f-42e1-851d-59a43e5d2ffa';
const CLIENT_ID = 'gmgn_web_test';

async function checkResponse() {
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

    // 测试几个返回 parse_error 的端点
    const testUrls = [
        `https://gmgn.ai/defi/quotation/v1/trade/sol/wallets?period=1d&orderby=buy&limit=5&${params}`,
        `https://gmgn.ai/defi/quotation/v1/rank/sol/tokens/1h?limit=10&${params}`,
    ];

    for (const url of testUrls) {
        console.log(`\n📡 ${url.split('?')[0].replace('https://gmgn.ai/defi/quotation/', '')}`);
        const res = await fetch(url, { headers });
        console.log(`   Status: ${res.status}`);
        console.log(`   Content-Type: ${res.headers.get('content-type')}`);
        const text = await res.text();
        console.log(`   Response (first 300 chars): ${text.slice(0, 300)}`);
    }

    // 尝试 swaps 排行榜（之前成功过）- 看看是否有钱包和代币关联数据
    console.log('\n\n========== 检查 swaps 排行榜中的钱包数据 ==========\n');

    const swapsUrl = `https://gmgn.ai/defi/quotation/v1/rank/sol/swaps/5m?limit=30&${params}`;
    const swapsRes = await fetch(swapsUrl, { headers });
    const swapsData = await swapsRes.json();

    if (swapsData.data?.rank) {
        console.log(`获取到 ${swapsData.data.rank.length} 条交易\n`);

        // 这些是代币数据，不是钱包数据
        console.log('字段:', Object.keys(swapsData.data.rank[0]).join(', '));
        console.log('\n前5个代币:');
        swapsData.data.rank.slice(0, 5).forEach((t, i) => {
            console.log(`${i+1}. ${t.symbol} - MC: $${(t.market_cap/1e6).toFixed(2)}M - Vol: $${(t.volume/1e3).toFixed(1)}K`);
        });
    }

    // 测试获取钱包排行的不同参数组合
    console.log('\n\n========== 测试钱包排行参数组合 ==========\n');

    const walletParams = [
        'type=smart_money',
        'type=whale',
        'type=degen',
        'filter=hit_tokens',
        'data=full',
        'detail=true',
    ];

    for (const p of walletParams) {
        const url = `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=buy&limit=3&${p}&${params}`;
        console.log(`测试参数: ${p}`);

        const res = await fetch(url, { headers });
        if (res.ok) {
            const data = await res.json();
            if (data.data?.rank?.[0]) {
                const w = data.data.rank[0];
                // 检查是否有新字段
                const fields = Object.keys(w);
                const tokenFields = fields.filter(f =>
                    f.includes('token') || f.includes('hit') || f.includes('traded')
                );
                if (tokenFields.length > 0) {
                    console.log(`   ✅ 找到代币字段: ${tokenFields.join(', ')}`);
                } else {
                    console.log(`   标准字段 (${fields.length}个)`);
                }
            }
        } else {
            console.log(`   ❌ ${res.status}`);
        }

        await new Promise(r => setTimeout(r, 200));
    }
}

checkResponse().catch(console.error);
