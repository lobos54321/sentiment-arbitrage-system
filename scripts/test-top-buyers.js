/**
 * 测试 tokens/top_buyers - 找到命中代币的钱包
 */

import fs from 'fs';

const DEVICE_ID = '1d29f750-687f-42e1-851d-59a43e5d2ffa';
const CLIENT_ID = 'gmgn_web_test';

async function testTopBuyers() {
    console.log('\n========== GMGN tokens/top_buyers 详细测试 ==========\n');

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

    // 获取热门代币
    const swapsUrl = `https://gmgn.ai/defi/quotation/v1/rank/sol/swaps/5m?limit=5&${params}`;
    const swapsRes = await fetch(swapsUrl, { headers });
    const swapsData = await swapsRes.json();
    const tokens = swapsData.data?.rank || [];

    console.log('热门代币:');
    tokens.forEach((t, i) => {
        console.log(`  ${i+1}. ${t.symbol} - ${t.address}`);
    });

    // 测试每个代币的 top_buyers
    for (const token of tokens.slice(0, 3)) {
        console.log(`\n${'='.repeat(80)}`);
        console.log(`代币: ${token.symbol} (${token.address.slice(0, 8)}...)`);
        console.log(`${'='.repeat(80)}`);

        const url = `https://gmgn.ai/defi/quotation/v1/tokens/top_buyers/sol/${token.address}?${params}`;
        const res = await fetch(url, { headers });
        const data = await res.json();

        if (data.data?.holders) {
            const holders = data.data.holders;
            console.log(`\n找到 ${holders.length} 个买家\n`);

            if (holders.length > 0) {
                // 显示字段
                console.log('字段:', Object.keys(holders[0]).join(', '));

                // 显示买家
                console.log('\n钱包              | 标签        | 成本          | 已实现PNL      | 未实现PNL     | 持有数量');
                console.log('-'.repeat(100));

                holders.slice(0, 10).forEach((h, i) => {
                    const addr = (h.address || '').slice(0, 8);
                    const tags = (h.tags || []).slice(0, 2).join(',') || '-';
                    const cost = Number(h.total_cost || h.cost || 0);
                    const realizedPnl = Number(h.realized_profit || h.realized_pnl || 0);
                    const unrealizedPnl = Number(h.unrealized_profit || h.unrealized_pnl || 0);
                    const balance = Number(h.balance || h.amount || 0);

                    console.log(
                        `${addr.padEnd(16)} | ${tags.slice(0, 10).padEnd(10)} | $${cost.toFixed(2).padStart(10)} | $${realizedPnl.toFixed(2).padStart(10)} | $${unrealizedPnl.toFixed(2).padStart(10)} | ${balance.toFixed(0)}`
                    );
                });

                // 显示第一个完整数据
                console.log('\n完整数据示例:');
                console.log(JSON.stringify(holders[0], null, 2));
            }
        } else {
            console.log('无数据或错误:', data.code || data.error);
        }

        await new Promise(r => setTimeout(r, 500));
    }

    // 测试相关端点
    console.log('\n\n========== 测试相关端点 ==========\n');

    const tokenAddr = tokens[0]?.address;
    if (tokenAddr) {
        const relatedEndpoints = [
            `https://gmgn.ai/defi/quotation/v1/tokens/top_buyers/sol/${tokenAddr}?orderby=profit&${params}`,
            `https://gmgn.ai/defi/quotation/v1/tokens/top_buyers/sol/${tokenAddr}?orderby=cost&${params}`,
            `https://gmgn.ai/defi/quotation/v1/tokens/top_buyers/sol/${tokenAddr}?type=smart&${params}`,
            `https://gmgn.ai/defi/quotation/v1/tokens/holders/sol/${tokenAddr}?${params}`,
        ];

        for (const url of relatedEndpoints) {
            const p = url.split('?')[1]?.split('&')[0] || 'default';
            console.log(`📡 ${p}:`);

            const res = await fetch(url, { headers });
            if (!res.ok) {
                console.log(`   ❌ ${res.status}`);
                continue;
            }

            const data = await res.json();
            if (data.data?.holders) {
                console.log(`   ✅ ${data.data.holders.length} 条记录`);
            } else {
                console.log(`   ⚠️ ${data.code || 'no holders'}`);
            }

            await new Promise(r => setTimeout(r, 200));
        }
    }
}

testTopBuyers().catch(console.error);
