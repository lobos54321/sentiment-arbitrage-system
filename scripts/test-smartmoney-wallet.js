/**
 * 测试 GMGN SmartMoney 钱包详情 API
 */

import fs from 'fs';

const DEVICE_ID = '1d29f750-687f-42e1-851d-59a43e5d2ffa';
const CLIENT_ID = 'gmgn_web_test';

async function testSmartMoneyWallet() {
    console.log('\n========== GMGN SmartMoney 钱包详情 ==========\n');

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
    const rankUrl = `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=pnl&limit=3&${params}`;
    const rankRes = await fetch(rankUrl, { headers });
    const rankData = await rankRes.json();
    const wallet = rankData.data?.rank?.[0];

    if (!wallet) {
        console.log('获取钱包失败');
        return;
    }

    const addr = wallet.wallet_address;

    console.log(`钱包地址: ${addr}\n`);

    // 获取 smartmoney 详情
    const url = `https://gmgn.ai/defi/quotation/v1/smartmoney/sol/walletNew/${addr}?${params}`;
    const res = await fetch(url, { headers });
    const data = await res.json();

    console.log('SmartMoney Wallet 完整数据:\n');
    console.log(JSON.stringify(data.data, null, 2));

    // 如果有 tags 或 tokens 字段
    if (data.data) {
        const d = data.data;
        console.log('\n\n========== 关键字段分析 ==========');
        console.log('\n用户信息:');
        console.log(`  名称: ${d.twitter_name || d.name}`);
        console.log(`  Twitter: ${d.twitter_username}`);
        console.log(`  粉丝: ${d.twitter_fans_num}`);

        console.log('\n余额:');
        console.log(`  SOL: ${d.sol_balance}`);
        console.log(`  ETH: ${d.eth_balance}`);
        console.log(`  TRX: ${d.trx_balance}`);

        // 查找所有可能包含代币数据的字段
        const tokenFields = Object.keys(d).filter(k =>
            k.includes('token') || k.includes('pnl') || k.includes('profit') ||
            k.includes('trade') || k.includes('holding') || k.includes('position')
        );

        if (tokenFields.length > 0) {
            console.log('\n代币/PNL 相关字段:');
            tokenFields.forEach(field => {
                console.log(`  ${field}: ${JSON.stringify(d[field])}`);
            });
        }

        // 检查 tags
        if (d.tags) {
            console.log('\n标签:', d.tags);
        }
    }

    // 测试更多 smartmoney 相关端点
    console.log('\n\n========== 测试更多 SmartMoney 端点 ==========\n');

    const smEndpoints = [
        `https://gmgn.ai/defi/quotation/v1/smartmoney/sol/wallet/${addr}/tokens?${params}`,
        `https://gmgn.ai/defi/quotation/v1/smartmoney/sol/wallet/${addr}/pnl?${params}`,
        `https://gmgn.ai/defi/quotation/v1/smartmoney/sol/wallet/${addr}/trades?${params}`,
        `https://gmgn.ai/defi/quotation/v1/smartmoney/sol/wallet/${addr}/holdings?${params}`,
        `https://gmgn.ai/defi/quotation/v1/smartmoney/sol/walletNew/${addr}/tokens?${params}`,
        `https://gmgn.ai/defi/quotation/v1/smartmoney/sol/walletNew/${addr}/pnl?period=7d&${params}`,
        `https://gmgn.ai/defi/quotation/v1/smartmoney/sol/walletNew/${addr}/holdings?${params}`,
    ];

    for (const epUrl of smEndpoints) {
        const path = epUrl.split('?')[0].replace('https://gmgn.ai/defi/quotation/v1/smartmoney/sol/', '');
        try {
            const epRes = await fetch(epUrl, { headers });
            if (!epRes.ok) {
                console.log(`❌ ${path.slice(0, 50).padEnd(50)} | ${epRes.status}`);
            } else {
                const epData = await epRes.json();
                if (epData.code && epData.code !== 0) {
                    console.log(`⚠️ ${path.slice(0, 50).padEnd(50)} | code: ${epData.code}`);
                } else {
                    console.log(`✅ ${path.slice(0, 50).padEnd(50)} | 成功!`);
                    if (epData.data) {
                        if (Array.isArray(epData.data)) {
                            console.log(`   数组 [${epData.data.length}]`);
                            if (epData.data.length > 0) {
                                console.log(`   示例: ${JSON.stringify(epData.data[0]).slice(0, 200)}`);
                            }
                        } else {
                            console.log(`   对象: ${JSON.stringify(epData.data).slice(0, 200)}`);
                        }
                    }
                }
            }
        } catch (e) {
            console.log(`❌ ${path.slice(0, 50).padEnd(50)} | ${e.message}`);
        }
        await new Promise(r => setTimeout(r, 200));
    }
}

testSmartMoneyWallet().catch(console.error);
