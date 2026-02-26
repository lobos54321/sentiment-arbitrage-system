/**
 * 测试 GMGN 雷达 - 钱包排行 + 命中代币 PNL
 */

import fs from 'fs';

const DEVICE_ID = '1d29f750-687f-42e1-851d-59a43e5d2ffa';
const CLIENT_ID = 'gmgn_web_test';

async function testRadarWallets() {
    console.log('\n========== GMGN 雷达 - 钱包命中代币 PNL ==========\n');

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

    // 雷达页面的钱包排行 - 买入最多 (orderby=buy)
    const url = `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=buy&direction=desc&limit=20&${params}`;

    console.log('📡 获取雷达钱包数据 (买入最多)...\n');

    try {
        const response = await fetch(url, { headers });

        if (!response.ok) {
            console.log('❌ 请求失败:', response.status);
            return;
        }

        const data = await response.json();
        const wallets = data.data?.rank || [];

        console.log(`✅ 获取到 ${wallets.length} 个钱包\n`);

        // 打印第一个钱包的完整字段，看看有哪些数据
        if (wallets.length > 0) {
            console.log('📋 数据字段:');
            console.log(Object.keys(wallets[0]).join(', '));
            console.log('');
        }

        // 格式化显示
        console.log('═'.repeat(140));
        console.log('排名 | 钱包                | SOL余额   | 命中代币PNL      | 1D PnL          | 1D胜率  | 交易数 (买/卖)     | 追踪/备注  | >500%');
        console.log('═'.repeat(140));

        wallets.slice(0, 15).forEach((w, i) => {
            const name = (w.twitter_name || w.name || (w.wallet_address || '').slice(0, 8)).slice(0, 15).padEnd(15);
            const solBalance = Number(w.sol_balance || 0).toFixed(2).padStart(8);

            // 命中代币 PNL (pnl_7d 是命中代币的统计)
            const tokenPnl = Number(w.pnl_7d) || 0;
            const tokenPnlPct = Number(w.pnl_7d) > 0 ? '+' : '';
            const tokenPnlStr = `$${Math.round(tokenPnl).toString().padStart(8)}`.padEnd(16);

            // 1D PNL
            const pnl1d = Number(w.realized_profit_1d) || 0;
            const pnl1dStr = `$${Math.round(pnl1d).toString().padStart(8)}`.padEnd(16);

            // 胜率
            const winrate = ((Number(w.winrate_1d) || 0) * 100).toFixed(1).padStart(5) + '%';

            // 交易数
            const txs = w.txs_1d || 0;
            const buys = w.buy_1d || 0;
            const sells = w.sell_1d || 0;
            const txsStr = `${txs} (${buys}/${sells})`.padEnd(16);

            // 追踪/备注
            const followers = w.follow_count || 0;
            const remarks = w.remark_count || 0;
            const followStr = `${followers}/${remarks}`.padStart(8);

            // >500% 的代币数 (金狗数)
            const gt500 = w.pnl_gt_5x_num_7d || 0;

            console.log(`${String(i + 1).padStart(2)}   | ${name} | ${solBalance} | ${tokenPnlStr} | ${pnl1dStr} | ${winrate} | ${txsStr} | ${followStr} | ${gt500}`);
        });

        console.log('═'.repeat(140));

        // 显示一个完整的钱包数据示例
        console.log('\n📊 完整数据示例 (第1个钱包):');
        console.log(JSON.stringify(wallets[0], null, 2));

    } catch (e) {
        console.log('❌ 异常:', e.message);
    }
}

testRadarWallets().catch(console.error);
