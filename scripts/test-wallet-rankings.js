/**
 * 测试不同排序方式的钱包排行 API
 *
 * 根据截图：
 * - 买入最多 (orderby=buy)
 * - 利润最高 (orderby=pnl 或 profit)
 * - 买入最早 (orderby=first_buy 或 buy_time)
 * - 共同持仓 (orderby=common_holdings)
 */

import fs from 'fs';

const DEVICE_ID = '1d29f750-687f-42e1-851d-59a43e5d2ffa';
const CLIENT_ID = 'gmgn_web_test';

async function fetchApi(url, headers) {
    try {
        const response = await fetch(url, { headers });
        if (!response.ok) return { error: response.status };
        return await response.json();
    } catch (e) {
        return { error: e.message };
    }
}

async function testWalletRankings() {
    console.log('\n========== 测试钱包排行不同排序方式 ==========\n');

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
        'Referer': 'https://gmgn.ai/sol/radar'
    };

    // 测试不同的 orderby 参数
    const orderbyOptions = [
        'buy',           // 买入最多
        'profit',        // 利润最高
        'pnl',           // PNL
        'realized_profit', // 已实现利润
        'realized_profit_1d', // 1D利润
        'first_buy',     // 买入最早
        'buy_time',      // 买入时间
        'early',         // 最早
        'common',        // 共同持仓
        'common_holdings', // 共同持仓
        'winrate',       // 胜率
        'winrate_1d',    // 1D胜率
    ];

    const periods = ['1d', '7d'];

    for (const period of periods) {
        console.log(`\n========== 周期: ${period} ==========\n`);

        for (const orderby of orderbyOptions) {
            const url = `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/${period}?orderby=${orderby}&direction=desc&limit=5&${params}`;

            console.log(`📡 orderby=${orderby}:`);

            const data = await fetchApi(url, headers);

            if (data.error) {
                console.log(`   ❌ ${data.error}`);
            } else if (data.code && data.code !== 0) {
                console.log(`   ⚠️ code: ${data.code}`);
            } else if (data.data?.rank?.[0]) {
                console.log(`   ✅ 成功! 返回 ${data.data.rank.length} 条`);

                // 显示第一个钱包的关键数据
                const w = data.data.rank[0];
                const name = w.twitter_name || w.wallet_address?.slice(0, 12);
                console.log(`   #1: ${name}`);
                console.log(`      1D PNL: $${Number(w.realized_profit_1d || 0).toFixed(2)}`);
                console.log(`      1D 胜率: ${((w.winrate_1d || 0) * 100).toFixed(1)}%`);
                console.log(`      买入: ${w.buy_1d || w.buy || 0}`);

                // 检查是否有命中代币相关字段
                const hitFields = Object.keys(w).filter(k =>
                    k.includes('token') || k.includes('hit') || k.includes('card')
                );
                if (hitFields.length > 0) {
                    console.log(`      🎯 命中相关字段: ${hitFields.join(', ')}`);
                }
            } else {
                console.log(`   ⚠️ 无数据`);
            }

            await new Promise(r => setTimeout(r, 200));
        }
    }

    // 测试返回字段的完整性
    console.log('\n\n========== 检查返回字段完整性 ==========\n');

    const fullUrl = `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=buy&direction=desc&limit=3&${params}`;
    const fullData = await fetchApi(fullUrl, headers);

    if (fullData.data?.rank?.[0]) {
        const wallet = fullData.data.rank[0];
        console.log('钱包数据所有字段:\n');

        // 按类别分组显示
        const categories = {
            '基本信息': ['wallet_address', 'twitter_name', 'name', 'avatar', 'tags'],
            'PNL数据': ['realized_profit_1d', 'realized_profit_7d', 'realized_profit_30d', 'pnl_1d', 'pnl_7d', 'pnl_30d'],
            '胜率数据': ['winrate_1d', 'winrate_7d', 'winrate_30d'],
            '交易数据': ['buy_1d', 'buy_7d', 'buy_30d', 'sell_1d', 'sell_7d', 'sell_30d', 'txs_1d', 'txs_7d', 'txs_30d'],
            '持仓数据': ['balance', 'sol_balance', 'volume_1d', 'volume_7d', 'avg_cost_1d', 'avg_holding_period_1d'],
            'PNL分布': ['pnl_lt_minus_dot5_num_7d', 'pnl_minus_dot5_0x_num_7d', 'pnl_lt_2x_num_7d', 'pnl_2x_5x_num_7d', 'pnl_gt_5x_num_7d'],
            '其他': []
        };

        const allKeys = Object.keys(wallet);
        const usedKeys = new Set();

        for (const [category, keys] of Object.entries(categories)) {
            if (category === '其他') continue;
            console.log(`\n📊 ${category}:`);
            for (const key of keys) {
                if (wallet[key] !== undefined) {
                    usedKeys.add(key);
                    const value = wallet[key];
                    const displayValue = typeof value === 'number'
                        ? (value > 1000 ? value.toFixed(2) : value)
                        : (typeof value === 'string' ? value.slice(0, 50) : JSON.stringify(value).slice(0, 50));
                    console.log(`   ${key}: ${displayValue}`);
                }
            }
        }

        // 显示未分类的字段
        const otherKeys = allKeys.filter(k => !usedKeys.has(k));
        if (otherKeys.length > 0) {
            console.log('\n📊 其他字段:');
            for (const key of otherKeys) {
                const value = wallet[key];
                if (value !== null && value !== undefined && value !== '') {
                    const displayValue = typeof value === 'object'
                        ? JSON.stringify(value).slice(0, 100)
                        : String(value).slice(0, 50);
                    console.log(`   ${key}: ${displayValue}`);
                }
            }
        }
    }

    // 总结
    console.log('\n\n========== API 能力总结 ==========\n');
    console.log('✅ 可以获取的数据:');
    console.log('   - 钱包地址、名称、标签');
    console.log('   - 1D/7D/30D PNL (realized_profit_1d/7d/30d)');
    console.log('   - 1D/7D/30D 胜率 (winrate_1d/7d/30d)');
    console.log('   - 买卖次数 (buy_1d, sell_1d, txs_1d)');
    console.log('   - 交易量 (volume_1d/7d)');
    console.log('   - 平均持仓时间 (avg_holding_period_1d)');
    console.log('   - PNL 分布 (pnl_2x_5x_num_7d 等)');
    console.log('');
    console.log('❓ 命中代币数据:');
    console.log('   - 需要检查 smartmoney/walletNew API');
}

testWalletRankings().catch(console.error);
