/**
 * 测试 GMGN 雷达 - 钱包命中代币详情
 *
 * 找到每个钱包命中的具体代币和各自的 PNL
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

async function testWalletTokens() {
    console.log('\n========== GMGN 雷达 - 钱包命中代币详情 ==========\n');

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

    // 先获取一个钱包地址
    console.log('📊 获取钱包排行...');
    const rankUrl = `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=buy&limit=3&${params}`;
    const rankData = await fetchApi(rankUrl, headers);

    if (rankData.error || !rankData.data?.rank?.length) {
        console.log('❌ 获取钱包排行失败');
        return;
    }

    const wallet = rankData.data.rank[0];
    const walletAddress = wallet.wallet_address;
    const walletName = wallet.twitter_name || wallet.name || walletAddress.slice(0, 8);

    console.log(`\n👤 测试钱包: ${walletName}`);
    console.log(`   地址: ${walletAddress}\n`);

    // 尝试多个可能的 API 端点来获取命中代币
    const endpoints = [
        // 钱包持仓代币
        {
            name: '持仓代币 (holdings)',
            url: `https://gmgn.ai/defi/quotation/v1/wallet/sol/holdings/${walletAddress}?${params}`
        },
        // 钱包代币列表
        {
            name: '代币列表 (tokens)',
            url: `https://gmgn.ai/defi/quotation/v1/wallet/sol/tokens/${walletAddress}?${params}`
        },
        // 钱包 PNL 分析
        {
            name: 'PNL 分析 (pnl_analysis)',
            url: `https://gmgn.ai/defi/quotation/v1/wallet/sol/pnl_analysis/${walletAddress}?${params}`
        },
        // 钱包交易代币
        {
            name: '交易代币 (traded_tokens)',
            url: `https://gmgn.ai/defi/quotation/v1/wallet/sol/traded_tokens/${walletAddress}?period=1d&${params}`
        },
        // 钱包持仓 v2
        {
            name: '持仓 v2 (position)',
            url: `https://gmgn.ai/defi/quotation/v1/wallet_holdings/sol/${walletAddress}?${params}`
        },
        // 钱包代币 PNL
        {
            name: '代币 PNL (token_pnl)',
            url: `https://gmgn.ai/defi/quotation/v1/wallet/sol/token_pnl/${walletAddress}?${params}`
        },
        // 钱包活动 - 所有类型
        {
            name: '活动记录 (activity all)',
            url: `https://gmgn.ai/defi/quotation/v1/wallet_activity/sol?wallet=${walletAddress}&limit=20&${params}`
        },
        // 钱包活动 - 买入
        {
            name: '买入记录 (activity buy)',
            url: `https://gmgn.ai/defi/quotation/v1/wallet_activity/sol?type=buy&wallet=${walletAddress}&limit=10&${params}`
        },
        // 钱包统计
        {
            name: '钱包统计 (stat)',
            url: `https://gmgn.ai/defi/quotation/v1/wallet_stat/sol?wallet=${walletAddress}&${params}`
        },
        // 代币持仓
        {
            name: '代币持仓 (token_holdings)',
            url: `https://gmgn.ai/defi/quotation/v1/token_holdings/sol?wallet=${walletAddress}&${params}`
        }
    ];

    for (const ep of endpoints) {
        console.log(`\n📡 测试: ${ep.name}`);
        console.log(`   URL: ${ep.url.replace(walletAddress, walletAddress.slice(0,8) + '...')}`);

        const data = await fetchApi(ep.url, headers);

        if (data.error) {
            console.log(`   ❌ 失败: ${data.error}`);
            continue;
        }

        if (data.code && data.code !== 0) {
            console.log(`   ⚠️ API错误: ${data.code} ${data.msg || ''}`);
            continue;
        }

        // 分析返回的数据结构
        const result = data.data;
        if (!result) {
            console.log('   ⚠️ 无数据');
            continue;
        }

        // 尝试找到代币列表
        let tokens = null;
        if (Array.isArray(result)) {
            tokens = result;
        } else if (result.holdings) {
            tokens = result.holdings;
        } else if (result.tokens) {
            tokens = result.tokens;
        } else if (result.activities) {
            tokens = result.activities;
        } else if (result.list) {
            tokens = result.list;
        }

        if (tokens && Array.isArray(tokens) && tokens.length > 0) {
            console.log(`   ✅ 成功! 获取到 ${tokens.length} 条数据`);
            console.log(`   📋 字段: ${Object.keys(tokens[0]).slice(0, 10).join(', ')}`);

            // 显示前3条
            console.log('\n   📊 示例数据:');
            tokens.slice(0, 3).forEach((t, i) => {
                const symbol = t.token?.symbol || t.symbol || t.token_symbol || 'N/A';
                const pnl = t.realized_pnl || t.pnl || t.profit || t.realized_profit || 0;
                const cost = t.total_cost || t.cost || t.buy_cost || 0;
                const value = t.usd_value || t.value || t.current_value || 0;
                const amount = t.amount || t.balance || 0;

                console.log(`      ${i+1}. ${String(symbol).padEnd(12)} | PNL: $${Number(pnl).toFixed(2).padStart(10)} | 成本: $${Number(cost).toFixed(2).padStart(8)} | 价值: $${Number(value).toFixed(2).padStart(8)}`);
            });
        } else if (typeof result === 'object') {
            console.log(`   ✅ 成功! 返回对象`);
            console.log(`   📋 字段: ${Object.keys(result).slice(0, 15).join(', ')}`);
        }

        await new Promise(r => setTimeout(r, 300));
    }

    console.log('\n' + '═'.repeat(60));
}

testWalletTokens().catch(console.error);
