/**
 * 测试代币交易者 API - 寻找带 PNL 的数据
 */

import fs from 'fs';

const DEVICE_ID = '1d29f750-687f-42e1-851d-59a43e5d2ffa';
const CLIENT_ID = 'gmgn_web_test';

async function testTokenTraders() {
    console.log('\n========== 测试代币交易者 API ==========\n');

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

    const tokenAddr = 'EE3hEc8HTvVAwMeRxmqvmJpWGXx6p5YSx8DMKCTEpump';

    const endpoints = [
        `https://gmgn.ai/defi/quotation/v1/tokens/top_traders/sol/${tokenAddr}?${params}`,
        `https://gmgn.ai/defi/quotation/v1/tokens/traders/sol/${tokenAddr}?${params}`,
        `https://gmgn.ai/defi/quotation/v1/token/top_traders/sol/${tokenAddr}?${params}`,
        `https://gmgn.ai/defi/quotation/v1/token/traders/sol/${tokenAddr}?${params}`,
        `https://gmgn.ai/defi/quotation/v1/token/holders/sol/${tokenAddr}?${params}`,
        `https://gmgn.ai/defi/quotation/v1/tokens/sol/${tokenAddr}/traders?${params}`,
        `https://gmgn.ai/defi/quotation/v1/tokens/sol/${tokenAddr}/top_traders?${params}`,
        // 带更多参数
        `https://gmgn.ai/defi/quotation/v1/tokens/top_buyers/sol/${tokenAddr}?orderby=profit&${params}`,
        `https://gmgn.ai/defi/quotation/v1/tokens/top_buyers/sol/${tokenAddr}?type=smart&${params}`,
    ];

    for (const url of endpoints) {
        const path = url.split('?')[0].replace('https://gmgn.ai/defi/quotation/', '');
        console.log(`\n📡 ${path}:`);

        try {
            const res = await fetch(url, { headers });
            if (!res.ok) {
                console.log(`   ❌ ${res.status}`);
                continue;
            }

            const data = await res.json();
            if (data.code && data.code !== 0) {
                console.log(`   ⚠️ code: ${data.code}`);
                continue;
            }

            console.log('   ✅ 成功!');
            console.log('   数据结构:', JSON.stringify(data.data).slice(0, 400));
        } catch (e) {
            console.log(`   ❌ ${e.message}`);
        }
    }
}

testTokenTraders().catch(console.error);
