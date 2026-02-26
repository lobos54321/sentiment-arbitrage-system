/**
 * 测试 GMGN - 钱包持仓和 PNL
 *
 * 尝试找到正确的端点格式
 */

import fs from 'fs';

const DEVICE_ID = '1d29f750-687f-42e1-851d-59a43e5d2ffa';
const CLIENT_ID = 'gmgn_web_test';

async function testWalletPnlEndpoints() {
    console.log('\n========== GMGN 钱包持仓和 PNL 端点测试 ==========\n');

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

    // 获取钱包
    const rankUrl = `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=pnl&limit=5&${params}`;
    const rankRes = await fetch(rankUrl, { headers });
    const rankData = await rankRes.json();
    const wallets = rankData.data?.rank || [];

    console.log(`获取到 ${wallets.length} 个钱包\n`);

    // 测试不同的端点 - 使用已知工作的格式
    for (let i = 0; i < Math.min(2, wallets.length); i++) {
        const w = wallets[i];
        const addr = w.wallet_address;
        const name = w.twitter_name || w.name || addr.slice(0, 8);

        console.log('='.repeat(100));
        console.log(`钱包 ${i+1}: ${name}`);
        console.log(`地址: ${addr}`);
        console.log('='.repeat(100));

        // 测试已知的两个端点格式
        const endpointFormats = [
            // 原来成功的格式
            { name: 'wallet/sol/holdings', url: `https://gmgn.ai/defi/quotation/v1/wallet/sol/holdings/${addr}?${params}` },

            // 带更多参数
            { name: 'wallet/sol/holdings + orderby', url: `https://gmgn.ai/defi/quotation/v1/wallet/sol/holdings/${addr}?orderby=last_active_timestamp&direction=desc&showsmall=true&sellout=true&${params}` },

            // wallet_activity 带类型
            { name: 'wallet_activity all', url: `https://gmgn.ai/defi/quotation/v1/wallet_activity/sol?wallet=${addr}&limit=50&${params}` },
            { name: 'wallet_activity buy', url: `https://gmgn.ai/defi/quotation/v1/wallet_activity/sol?type=buy&wallet=${addr}&limit=30&${params}` },
            { name: 'wallet_activity sell', url: `https://gmgn.ai/defi/quotation/v1/wallet_activity/sol?type=sell&wallet=${addr}&limit=30&${params}` },

            // 不带查询参数
            { name: 'holdings bare', url: `https://gmgn.ai/defi/quotation/v1/wallet/sol/holdings/${addr}` },
        ];

        for (const ep of endpointFormats) {
            console.log(`\n📡 ${ep.name}:`);

            try {
                const res = await fetch(ep.url, { headers });

                if (!res.ok) {
                    console.log(`   ❌ HTTP ${res.status}`);
                    continue;
                }

                const data = await res.json();

                if (data.code && data.code !== 0) {
                    console.log(`   ⚠️ API Error: ${data.code} - ${data.msg || ''}`);
                    continue;
                }

                console.log('   ✅ 成功!');

                // 分析返回的数据
                if (data.data) {
                    if (data.data.holdings && Array.isArray(data.data.holdings)) {
                        const h = data.data.holdings;
                        console.log(`   持仓数: ${h.length}`);
                        if (h.length > 0) {
                            console.log(`   字段: ${Object.keys(h[0]).join(', ')}`);
                            h.slice(0, 3).forEach((item, idx) => {
                                const sym = item.token?.symbol || 'N/A';
                                const pnl = item.realized_pnl || item.pnl || 0;
                                const cost = item.total_cost || item.cost || 0;
                                console.log(`   ${idx+1}. ${sym.padEnd(12)} | PNL: $${Number(pnl).toFixed(2).padStart(10)} | Cost: $${Number(cost).toFixed(2)}`);
                            });
                        }
                    } else if (data.data.activities && Array.isArray(data.data.activities)) {
                        const a = data.data.activities;
                        console.log(`   活动数: ${a.length}`);
                        if (a.length > 0) {
                            console.log(`   字段: ${Object.keys(a[0]).join(', ')}`);
                            a.slice(0, 5).forEach((item, idx) => {
                                const sym = item.token?.symbol || item.symbol || 'N/A';
                                const type = item.event_type || item.type || 'N/A';
                                const usd = item.amount_usd || item.usd_value || 0;
                                const pnl = item.realized_profit || item.pnl || '';
                                console.log(`   ${idx+1}. ${type.padEnd(6)} ${sym.padEnd(12)} | USD: $${Number(usd).toFixed(2).padStart(8)} | PNL: ${pnl ? '$' + Number(pnl).toFixed(2) : '-'}`);
                            });
                        }
                    } else if (Array.isArray(data.data)) {
                        console.log(`   数组长度: ${data.data.length}`);
                    } else {
                        console.log(`   对象字段: ${Object.keys(data.data).join(', ')}`);
                    }
                }
            } catch (e) {
                console.log(`   ❌ Error: ${e.message}`);
            }

            await new Promise(r => setTimeout(r, 300));
        }

        console.log('\n');
    }
}

testWalletPnlEndpoints().catch(console.error);
