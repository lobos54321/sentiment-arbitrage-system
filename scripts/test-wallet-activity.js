/**
 * 测试 GMGN 雷达 - 钱包交易活动 + 代币 PNL
 */

import fs from 'fs';

const DEVICE_ID = '1d29f750-687f-42e1-851d-59a43e5d2ffa';
const CLIENT_ID = 'gmgn_web_test';

async function testWalletActivity() {
    console.log('\n========== GMGN 雷达 - 钱包交易活动 ==========\n');

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
    const rankUrl = `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=buy&limit=1&${params}`;
    const rankResponse = await fetch(rankUrl, { headers });
    const rankData = await rankResponse.json();
    const wallet = rankData.data?.rank?.[0];

    if (!wallet) {
        console.log('❌ 获取钱包失败');
        return;
    }

    const walletAddress = wallet.wallet_address;
    const walletName = wallet.twitter_name || wallet.name || walletAddress.slice(0, 8);

    console.log(`👤 钱包: ${walletName}`);
    console.log(`   地址: ${walletAddress}\n`);

    // 获取活动记录
    const activityUrl = `https://gmgn.ai/defi/quotation/v1/wallet_activity/sol?wallet=${walletAddress}&limit=30&${params}`;
    console.log('📡 获取交易活动...\n');

    const actResponse = await fetch(activityUrl, { headers });
    const actData = await actResponse.json();

    if (!actData.data?.activities) {
        console.log('❌ 获取活动失败');
        return;
    }

    const activities = actData.data.activities;
    console.log(`✅ 获取到 ${activities.length} 条活动记录\n`);

    // 打印字段
    if (activities.length > 0) {
        console.log('📋 数据字段:');
        console.log(Object.keys(activities[0]).join(', '));
        console.log('');
    }

    // 显示活动
    console.log('═'.repeat(140));
    console.log('类型   | 代币           | 数量              | 价格            | 金额 (USD)    | 市值          | 时间                | PNL');
    console.log('═'.repeat(140));

    activities.slice(0, 20).forEach((a) => {
        const type = (a.event_type || a.type || 'N/A').padEnd(6);
        const symbol = (a.token?.symbol || a.token_symbol || a.symbol || 'N/A').slice(0, 12).padEnd(12);
        const amount = Number(a.token_amount || a.amount || 0);
        const amountStr = amount > 1e9 ? `${(amount/1e9).toFixed(2)}B` : amount > 1e6 ? `${(amount/1e6).toFixed(2)}M` : amount > 1e3 ? `${(amount/1e3).toFixed(2)}K` : amount.toFixed(2);
        const price = Number(a.price || a.token_price || 0);
        const priceStr = `$${price.toFixed(10)}`.slice(0, 15);
        const usdAmount = Number(a.amount_usd || a.usd_value || a.cost_usd || 0);
        const mcap = Number(a.market_cap || 0);
        const mcapStr = mcap > 1e6 ? `$${(mcap/1e6).toFixed(1)}M` : `$${(mcap/1e3).toFixed(1)}K`;
        const timestamp = a.timestamp ? new Date(a.timestamp * 1000).toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai' }) : 'N/A';
        const pnl = a.realized_profit || a.pnl || a.profit || '';
        const pnlStr = pnl ? `$${Number(pnl).toFixed(2)}` : '-';

        console.log(`${type} | ${symbol} | ${amountStr.padStart(15)} | ${priceStr.padStart(15)} | $${usdAmount.toFixed(2).padStart(10)} | ${mcapStr.padStart(10)} | ${timestamp} | ${pnlStr}`);
    });

    console.log('═'.repeat(140));

    // 完整示例
    console.log('\n📊 完整数据示例:');
    console.log(JSON.stringify(activities[0], null, 2));
}

testWalletActivity().catch(console.error);
