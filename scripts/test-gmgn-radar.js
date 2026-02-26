/**
 * 测试 GMGN 雷达 API (买入最多/利润最高/热门等)
 */

import fs from 'fs';

const DEVICE_ID = '1d29f750-687f-42e1-851d-59a43e5d2ffa';
const CLIENT_ID = 'gmgn_web_test';

async function testGmgnRadar() {
    console.log('\n========== 测试 GMGN 雷达 API ==========\n');

    // 1. 加载 Session
    const sessionPath = './config/gmgn_session.json';
    if (!fs.existsSync(sessionPath)) {
        console.log('❌ Session 文件不存在');
        return false;
    }

    const sessionData = JSON.parse(fs.readFileSync(sessionPath, 'utf8'));
    console.log('✅ Session 已加载');
    console.log('   Cookies 数量:', sessionData?.cookies?.length || 0);

    // 构造 Cookie
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

    // 测试雷达相关的 API 端点
    const endpoints = [
        // 买入最多 - 按交易次数排序 (1小时)
        {
            name: '买入最多 (Swaps 1H)',
            url: `https://gmgn.ai/defi/quotation/v1/rank/sol/swaps/1h?orderby=swaps&direction=desc&${params}`
        },
        // 聪明钱买入最多
        {
            name: '聪明钱买入 (Smart Money 1H)',
            url: `https://gmgn.ai/defi/quotation/v1/rank/sol/swaps/1h?orderby=smartmoney&direction=desc&${params}`
        },
        // 5分钟涨幅
        {
            name: '5分钟涨幅 (Change 5M)',
            url: `https://gmgn.ai/defi/quotation/v1/rank/sol/swaps/5m?orderby=change&direction=desc&${params}`
        },
        // 热门成交量
        {
            name: '热门榜 (Volume 1H)',
            url: `https://gmgn.ai/defi/quotation/v1/rank/sol/swaps/1h?orderby=volume&direction=desc&${params}`
        },
        // 钱包排行榜 1D
        {
            name: '钱包排行 (Wallet Rank 1D)',
            url: `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?${params}`
        },
        // 钱包排行榜 7D (牛人榜)
        {
            name: '牛人榜 (Wallet Rank 7D)',
            url: `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/7d?tag=smart_degen&limit=10&${params}`
        }
    ];

    let allSuccess = true;

    for (const ep of endpoints) {
        console.log(`\n📡 测试: ${ep.name}...`);

        try {
            const response = await fetch(ep.url, { headers });

            console.log('   HTTP Status:', response.status);

            if (!response.ok) {
                const text = await response.text();
                console.log('   ❌ 请求失败');
                console.log('   响应:', text.slice(0, 200));
                allSuccess = false;
                continue;
            }

            const data = await response.json();

            if (data.code && data.code !== 0) {
                console.log('   ❌ API 错误码:', data.code, data.msg || '');
                allSuccess = false;
                continue;
            }

            const items = data.data?.rank || data.data || [];
            const count = Array.isArray(items) ? items.length : 0;

            console.log('   ✅ 获取到', count, '条数据');

            // 显示前3条数据
            if (count > 0) {
                console.log('   📊 Top 3:');
                items.slice(0, 3).forEach((item, i) => {
                    // 尝试提取不同格式的数据
                    const symbol = item.symbol || item.token_symbol || 'N/A';
                    const address = (item.address || item.token_address || item.wallet_address || '').slice(0, 8);
                    const price = item.price || item.current_price || 0;
                    const mcap = item.market_cap || item.marketcap || 0;
                    const swaps = item.swaps || item.txs_1d || 0;
                    const sm = item.smart_money_count || item.smartmoney || 0;

                    if (symbol !== 'N/A') {
                        console.log(`      ${i+1}. ${symbol} | 地址: ${address}... | 价格: $${Number(price).toFixed(8)} | 市值: $${Math.round(mcap/1000)}K | 交易: ${swaps} | SM: ${sm}`);
                    } else {
                        // 钱包数据格式
                        const name = item.twitter_name || item.name || address;
                        const winrate = ((item.winrate_1d || 0) * 100).toFixed(0);
                        const pnl = item.realized_profit_1d || 0;
                        console.log(`      ${i+1}. ${name} | 胜率: ${winrate}% | 盈利: $${Math.round(pnl)} | 交易: ${swaps}`);
                    }
                });
            }

        } catch (e) {
            console.log('   ❌ 请求异常:', e.message);
            allSuccess = false;
        }
    }

    return allSuccess;
}

async function testGmgnRadarWS() {
    console.log('\n========== 测试 GMGN 雷达 WebSocket ==========\n');
    console.log('（WebSocket 测试需要更复杂的实现，暂时跳过）');
    console.log('如果 HTTP API 正常，雷达数据应该可以正常获取');
}

async function main() {
    console.log('🔍 GMGN 雷达 API 测试\n');
    console.log('═'.repeat(80));

    const httpOk = await testGmgnRadar();
    await testGmgnRadarWS();

    console.log('\n' + '═'.repeat(80));
    console.log('\n📋 测试结果:');
    console.log('   GMGN 雷达 HTTP API:', httpOk ? '✅ 正常' : '⚠️ 部分异常');
    console.log('');

    if (!httpOk) {
        console.log('💡 提示: 如果 API 返回 403，可能需要重新登录获取 Cookie');
        console.log('   运行: node scripts/gmgn-login-setup.js');
    }
}

main().catch(console.error);
