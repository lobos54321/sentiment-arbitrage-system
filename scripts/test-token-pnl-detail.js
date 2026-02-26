/**
 * 测试 GMGN 雷达 - 命中代币 PNL 详情
 *
 * 目标: 找到显示每个代币具体 PNL 的 API
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

async function testTokenPnlDetail() {
    console.log('\n========== GMGN 雷达 - 命中代币 PNL 详情 ==========\n');

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

    // 获取钱包排行
    console.log('📊 获取钱包排行 (按PNL排序)...\n');

    const rankUrl = `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=pnl&limit=5&${params}`;
    const rankData = await fetchApi(rankUrl, headers);
    const wallets = rankData.data?.rank || [];

    console.log(`获取到 ${wallets.length} 个钱包\n`);

    if (wallets.length === 0) {
        console.log('未获取到钱包数据');
        return;
    }

    const w = wallets[0];
    const name = w.twitter_name || w.name || w.wallet_address?.slice(0, 8);

    console.log('='.repeat(80));
    console.log(`测试钱包: ${name}`);
    console.log(`地址: ${w.wallet_address}`);
    console.log('='.repeat(80));

    // 列出所有与代币/PNL相关的字段
    console.log('\n📋 钱包排行数据中的PNL相关字段:');
    const allFields = Object.keys(w);
    allFields.forEach(field => {
        if (field.includes('pnl') || field.includes('profit') || field.includes('token') ||
            field.includes('hit') || field.includes('win') || field.includes('trade')) {
            console.log(`   ${field}: ${JSON.stringify(w[field])}`);
        }
    });

    // 测试多个可能的 API 端点
    console.log('\n\n🔍 测试不同的 API 端点获取代币级别 PNL...\n');

    const endpoints = [
        { name: 'profit', url: `https://gmgn.ai/defi/quotation/v1/wallet/sol/profit/${w.wallet_address}?period=1d&${params}` },
        { name: 'trades', url: `https://gmgn.ai/defi/quotation/v1/wallet/sol/trades/${w.wallet_address}?period=1d&${params}` },
        { name: 'position', url: `https://gmgn.ai/defi/quotation/v1/wallet/sol/position/${w.wallet_address}?${params}` },
        { name: 'token_summary', url: `https://gmgn.ai/defi/quotation/v1/wallet/sol/token_summary/${w.wallet_address}?period=1d&${params}` },
        { name: 'pnl_list', url: `https://gmgn.ai/defi/quotation/v1/wallet/sol/pnl_list/${w.wallet_address}?period=1d&${params}` },
        { name: 'realized', url: `https://gmgn.ai/defi/quotation/v1/wallet/sol/realized/${w.wallet_address}?period=1d&${params}` },
        { name: 'token_profit', url: `https://gmgn.ai/defi/quotation/v1/wallet/sol/token_profit/${w.wallet_address}?period=1d&${params}` },
        { name: 'trade_history', url: `https://gmgn.ai/defi/quotation/v1/wallet/sol/trade_history/${w.wallet_address}?period=1d&${params}` },
        { name: 'analysis', url: `https://gmgn.ai/defi/quotation/v1/wallet/sol/analysis/${w.wallet_address}?${params}` },
    ];

    for (const ep of endpoints) {
        console.log(`\n📡 ${ep.name}:`);
        const data = await fetchApi(ep.url, headers);

        if (data.error) {
            console.log(`   ❌ 失败: ${data.error}`);
            continue;
        }

        if (data.code && data.code !== 0) {
            console.log(`   ⚠️ API错误: ${data.code}`);
            continue;
        }

        console.log('   ✅ 成功!');

        // 分析返回数据
        const result = data.data;
        if (Array.isArray(result)) {
            console.log(`   数组长度: ${result.length}`);
            if (result.length > 0) {
                console.log(`   字段: ${Object.keys(result[0]).slice(0, 15).join(', ')}`);
                // 显示前2条
                result.slice(0, 2).forEach((item, i) => {
                    const symbol = item.token?.symbol || item.symbol || 'N/A';
                    const pnl = item.realized_profit || item.profit || item.pnl || item.realized_pnl || 0;
                    console.log(`   ${i+1}. ${symbol} | PNL: $${Number(pnl).toFixed(2)}`);
                });
            }
        } else if (result && typeof result === 'object') {
            console.log(`   对象字段: ${Object.keys(result).slice(0, 15).join(', ')}`);
            // 查找可能的列表
            for (const key of Object.keys(result)) {
                if (Array.isArray(result[key]) && result[key].length > 0) {
                    console.log(`   ${key} 列表 (${result[key].length} 条):`);
                    result[key].slice(0, 2).forEach((item, i) => {
                        const symbol = item.token?.symbol || item.symbol || 'N/A';
                        const pnl = item.realized_profit || item.profit || item.pnl || 0;
                        console.log(`     ${i+1}. ${symbol} | PNL: $${Number(pnl).toFixed(2)}`);
                    });
                }
            }
        }

        await new Promise(r => setTimeout(r, 300));
    }

    console.log('\n' + '='.repeat(80));
    console.log('测试完成');
}

testTokenPnlDetail().catch(console.error);
