/**
 * 测试 GMGN 雷达 - 钱包交易代币 PNL
 *
 * 尝试获取钱包过去交易的代币及其 PNL
 */

import fs from 'fs';

const DEVICE_ID = '1d29f750-687f-42e1-851d-59a43e5d2ffa';
const CLIENT_ID = 'gmgn_web_test';

async function fetchApi(url, headers) {
    try {
        const response = await fetch(url, { headers });
        if (!response.ok) {
            return { error: response.status, status: response.status };
        }
        return await response.json();
    } catch (e) {
        return { error: e.message };
    }
}

async function testWalletTrades() {
    console.log('\n========== GMGN 钱包交易代币 PNL ==========\n');

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

    // 获取一个钱包
    const rankUrl = `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=pnl&limit=3&${params}`;
    const rankData = await fetchApi(rankUrl, headers);
    const wallet = rankData.data?.rank?.[0];

    if (!wallet) {
        console.log('获取钱包失败');
        return;
    }

    const addr = wallet.wallet_address;
    const name = wallet.twitter_name || wallet.name || addr.slice(0, 8);

    console.log(`测试钱包: ${name}`);
    console.log(`地址: ${addr}`);
    console.log(`1D盈利: $${Math.round(Number(wallet.realized_profit_1d) || 0)}`);
    console.log(`>5x代币数(7D): ${wallet.pnl_gt_5x_num_7d}`);
    console.log(`2-5x代币数(7D): ${wallet.pnl_2x_5x_num_7d}`);
    console.log(`<2x代币数(7D): ${wallet.pnl_lt_2x_num_7d}`);
    console.log('');

    // 尝试更多端点变体
    const endpoints = [
        // 基础路径变体
        `https://gmgn.ai/defi/quotation/v1/smartmoney/sol/walletNew/${addr}?${params}`,
        `https://gmgn.ai/defi/quotation/v1/smartmoney/sol/wallet/${addr}?${params}`,
        `https://gmgn.ai/defi/quotation/v1/wallet_holdings/sol/${addr}?${params}`,
        `https://gmgn.ai/defi/quotation/v1/wallet_stat/sol/${addr}?${params}`,

        // 带 period 参数
        `https://gmgn.ai/defi/quotation/v1/wallet_holdings/sol/${addr}?period=1d&${params}`,
        `https://gmgn.ai/defi/quotation/v1/wallet_holdings/sol/${addr}?period=7d&${params}`,

        // Radar 特定端点
        `https://gmgn.ai/defi/quotation/v1/radar/sol/wallet/${addr}?${params}`,
        `https://gmgn.ai/defi/quotation/v1/radar/sol/wallet_pnl/${addr}?${params}`,

        // 代币相关
        `https://gmgn.ai/defi/quotation/v1/wallet_token_pnl/sol/${addr}?period=7d&${params}`,
        `https://gmgn.ai/defi/quotation/v1/wallet_tokens/sol/${addr}?period=7d&${params}`,

        // v2 版本
        `https://gmgn.ai/defi/quotation/v2/wallet/sol/holdings/${addr}?${params}`,
        `https://gmgn.ai/defi/quotation/v2/wallet_activity/sol?wallet=${addr}&${params}`,

        // 另一种活动端点格式
        `https://gmgn.ai/defi/quotation/v1/wallet/activity/sol/${addr}?limit=30&${params}`,
        `https://gmgn.ai/defi/quotation/v1/wallet/${addr}/activity/sol?limit=30&${params}`,

        // 交易历史
        `https://gmgn.ai/defi/quotation/v1/tx_history/sol/${addr}?limit=30&${params}`,
        `https://gmgn.ai/defi/quotation/v1/wallet_tx/sol/${addr}?limit=30&${params}`,
    ];

    console.log('测试各种 API 端点...\n');
    console.log('-'.repeat(80));

    for (const url of endpoints) {
        // 提取端点名称
        const urlPath = url.split('?')[0].replace('https://gmgn.ai/defi/quotation/', '');

        const data = await fetchApi(url, headers);

        if (data.error) {
            console.log(`❌ ${urlPath.slice(0, 60).padEnd(60)} | ${data.status || data.error}`);
        } else if (data.code && data.code !== 0) {
            console.log(`⚠️ ${urlPath.slice(0, 60).padEnd(60)} | code: ${data.code}`);
        } else {
            console.log(`✅ ${urlPath.slice(0, 60).padEnd(60)} | 成功!`);

            // 显示数据结构
            const result = data.data;
            if (result) {
                if (Array.isArray(result)) {
                    console.log(`   数组 [${result.length}]`);
                    if (result.length > 0) {
                        console.log(`   字段: ${Object.keys(result[0]).slice(0, 10).join(', ')}`);
                    }
                } else {
                    console.log(`   对象字段: ${Object.keys(result).slice(0, 10).join(', ')}`);
                }
            }
        }

        await new Promise(r => setTimeout(r, 200));
    }

    console.log('\n' + '-'.repeat(80));
    console.log('\n测试完成');
}

testWalletTrades().catch(console.error);
