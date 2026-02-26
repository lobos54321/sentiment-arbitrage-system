/**
 * 完整测试 - 获取真实的雷达数据
 *
 * 目标：
 * 1. 钱包排行（买入最多、利润最高等不同排序）
 * 2. 命中代币和命中代币 PNL
 * 3. 1D PNL、1D 胜率
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

async function getRadarData() {
    console.log('\n========== GMGN 雷达真实数据测试 ==========\n');

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

    // ========== 1. 获取买入最多排行 ==========
    console.log('📊 【买入最多】排行榜:\n');

    const buyMostUrl = `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=buy&direction=desc&limit=10&${params}`;
    const buyMostData = await fetchApi(buyMostUrl, headers);

    if (buyMostData.data?.rank) {
        const wallets = buyMostData.data.rank;
        console.log('排名 | 钱包                | 1D PNL        | 1D胜率  | 买入次数 | 标签');
        console.log('─'.repeat(85));

        for (let i = 0; i < wallets.length; i++) {
            const w = wallets[i];
            const rank = String(i + 1).padStart(2);
            const name = (w.twitter_name || w.wallet_address?.slice(0, 12)).slice(0, 18).padEnd(18);
            const pnl = ('$' + Number(w.realized_profit_1d || 0).toFixed(0)).padStart(12);
            const winrate = ((w.winrate_1d || 0) * 100).toFixed(1).padStart(5) + '%';
            const buys = String(w.buy_1d || 0).padStart(8);
            const tags = (w.tags || []).slice(0, 2).join(', ');

            console.log(`#${rank} | ${name} | ${pnl} | ${winrate} | ${buys} | ${tags}`);
        }
    }

    // ========== 2. 获取利润最高排行 ==========
    console.log('\n\n📊 【利润最高】排行榜 (按 realized_profit_1d 降序):\n');

    const profitUrl = `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=realized_profit_1d&direction=desc&limit=10&${params}`;
    const profitData = await fetchApi(profitUrl, headers);

    if (profitData.data?.rank) {
        const wallets = profitData.data.rank;
        console.log('排名 | 钱包                | 1D PNL        | 1D胜率  | 卖出次数 | TXs');
        console.log('─'.repeat(80));

        for (let i = 0; i < wallets.length; i++) {
            const w = wallets[i];
            const rank = String(i + 1).padStart(2);
            const name = (w.twitter_name || w.wallet_address?.slice(0, 12)).slice(0, 18).padEnd(18);
            const pnl = ('$' + Number(w.realized_profit_1d || 0).toFixed(0)).padStart(12);
            const winrate = ((w.winrate_1d || 0) * 100).toFixed(1).padStart(5) + '%';
            const sells = String(w.sell_1d || 0).padStart(8);
            const txs = String(w.txs_1d || 0).padStart(6);

            console.log(`#${rank} | ${name} | ${pnl} | ${winrate} | ${sells} | ${txs}`);
        }
    }

    // ========== 3. 获取高胜率排行 ==========
    console.log('\n\n📊 【高胜率】排行榜 (按 winrate_1d 降序):\n');

    const winrateUrl = `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/1d?orderby=winrate_1d&direction=desc&limit=10&${params}`;
    const winrateData = await fetchApi(winrateUrl, headers);

    if (winrateData.data?.rank) {
        const wallets = winrateData.data.rank;
        console.log('排名 | 钱包                | 1D PNL        | 1D胜率  | TXs');
        console.log('─'.repeat(70));

        for (let i = 0; i < wallets.length; i++) {
            const w = wallets[i];
            const rank = String(i + 1).padStart(2);
            const name = (w.twitter_name || w.wallet_address?.slice(0, 12)).slice(0, 18).padEnd(18);
            const pnl = ('$' + Number(w.realized_profit_1d || 0).toFixed(0)).padStart(12);
            const winrate = ((w.winrate_1d || 0) * 100).toFixed(1).padStart(5) + '%';
            const txs = String(w.txs_1d || 0).padStart(6);

            console.log(`#${rank} | ${name} | ${pnl} | ${winrate} | ${txs}`);
        }
    }

    // ========== 4. 获取单个钱包的命中代币 ==========
    console.log('\n\n📊 【命中代币】- 测试获取钱包的交易代币:\n');

    // 取第一个钱包测试
    const testWallet = buyMostData.data?.rank?.[0];
    if (testWallet) {
        const walletAddr = testWallet.wallet_address;
        console.log(`测试钱包: ${testWallet.twitter_name || walletAddr.slice(0, 12)}`);
        console.log(`地址: ${walletAddr}\n`);

        // 尝试 smartmoney/walletNew API
        const walletDetailUrl = `https://gmgn.ai/defi/quotation/v1/smartmoney/sol/walletNew/${walletAddr}?${params}`;
        console.log('📡 smartmoney/walletNew API:');

        const walletDetail = await fetchApi(walletDetailUrl, headers);

        if (walletDetail.error) {
            console.log(`   ❌ 错误: ${walletDetail.error}`);
        } else if (walletDetail.data) {
            console.log('   ✅ 成功!');
            console.log(`   字段: ${Object.keys(walletDetail.data).join(', ')}`);

            // 检查是否有代币相关字段
            if (walletDetail.data.tokens) {
                console.log(`   🎯 tokens: ${walletDetail.data.tokens.length}`);
            }
            if (walletDetail.data.holdings) {
                console.log(`   🎯 holdings: ${walletDetail.data.holdings.length}`);
            }
            if (walletDetail.data.activities) {
                console.log(`   🎯 activities: ${walletDetail.data.activities.length}`);
            }

            // 显示完整数据
            console.log(`\n   完整数据: ${JSON.stringify(walletDetail.data).slice(0, 500)}`);
        }

        // 尝试获取钱包持仓
        console.log('\n📡 钱包持仓 API:');
        const holdingsUrl = `https://gmgn.ai/pf/api/v1/wallet/sol/${walletAddr}/holdings?${params}`;
        const holdingsData = await fetchApi(holdingsUrl, headers);

        if (holdingsData.error) {
            console.log(`   ❌ 错误: ${holdingsData.error}`);
        } else if (holdingsData.data) {
            console.log('   ✅ 成功!');
            if (Array.isArray(holdingsData.data)) {
                console.log(`   持仓数: ${holdingsData.data.length}`);

                // 显示前5个持仓
                for (const h of holdingsData.data.slice(0, 5)) {
                    console.log(`   • ${h.symbol || h.token_address?.slice(0, 8)} | 价值: $${Number(h.usd_value || 0).toFixed(2)}`);
                }
            } else {
                console.log(`   字段: ${Object.keys(holdingsData.data).join(', ')}`);
            }
        }
    }

    // ========== 5. 获取热门代币及其买家（关联命中代币） ==========
    console.log('\n\n📊 【热门代币 Top Buyers】- 模拟命中代币关联:\n');

    const swapsUrl = `https://gmgn.ai/defi/quotation/v1/rank/sol/swaps/1h?limit=5&${params}`;
    const swapsData = await fetchApi(swapsUrl, headers);

    if (swapsData.data?.rank) {
        const tokens = swapsData.data.rank;

        for (const token of tokens.slice(0, 3)) {
            console.log(`\n🪙 ${token.symbol} (${token.address?.slice(0, 12)}...)`);
            console.log(`   市值: $${(Number(token.market_cap || 0) / 1e6).toFixed(2)}M | 涨幅: ${(Number(token.price_change_1h || 0) * 100).toFixed(1)}%`);

            // 获取这个代币的 Top Buyers
            const buyersUrl = `https://gmgn.ai/defi/quotation/v1/tokens/top_buyers/sol/${token.address}?${params}`;
            const buyersData = await fetchApi(buyersUrl, headers);

            if (buyersData.data?.holders?.holderInfo) {
                const buyers = buyersData.data.holders.holderInfo;
                console.log(`   Top Buyers (${buyers.length}个):`);

                for (const buyer of buyers.slice(0, 3)) {
                    const addr = buyer.wallet_address?.slice(0, 10);
                    const status = buyer.status || 'hold';
                    const tags = (buyer.tags || []).join(', ');
                    console.log(`     • ${addr}... | ${status} | [${tags}]`);
                }
            }

            await new Promise(r => setTimeout(r, 300));
        }
    }

    // ========== 6. 总结 ==========
    console.log('\n\n========== 数据获取能力总结 ==========\n');
    console.log('✅ 可稳定获取:');
    console.log('   • 钱包排行榜（按买入次数、利润、胜率等排序）');
    console.log('   • 每个钱包的 1D/7D/30D PNL、胜率、交易次数');
    console.log('   • 钱包标签（KOL、Smart Money、Axiom 等）');
    console.log('   • 热门代币列表及其 Top Buyers');
    console.log('');
    console.log('⚠️ 需要登录才能获取:');
    console.log('   • 命中代币详情（雷达页面 /vas/api/v1/radar/detail）');
    console.log('   • 命中代币 PNL（每个钱包在特定代币上的盈亏）');
    console.log('');
    console.log('💡 替代方案:');
    console.log('   • 通过 Top Buyers API 关联钱包和热门代币');
    console.log('   • 使用 Playwright 登录后截获完整数据');
}

getRadarData().catch(console.error);
