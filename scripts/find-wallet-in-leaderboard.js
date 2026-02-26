/**
 * 检查钱包是否在 GMGN 牛人榜中
 */

import { chromium } from 'playwright';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const sessionPath = path.join(__dirname, '../config/gmgn_session.json');

const targetWallet = process.argv[2] || 'FewEdBFYiz85hMSYCUpTRnHerqg4fJjPvGLUvtNj1gRf';

async function main() {
    console.log(`\n🔍 查找钱包: ${targetWallet}\n`);

    if (!fs.existsSync(sessionPath)) {
        console.log('❌ GMGN Session 不存在');
        return;
    }

    const browser = await chromium.launch({ headless: true });
    const context = await browser.newContext({ storageState: sessionPath });
    const page = await context.newPage();

    let foundWallet = null;
    let allWallets = [];

    // 监听 API 响应
    page.on('response', async (response) => {
        const url = response.url();
        if (response.status() === 200 && url.includes('/wallets/') && url.includes('smart_degen')) {
            try {
                const data = await response.json();
                if (data?.data?.rank) {
                    allWallets = data.data.rank;
                    console.log(`📊 牛人榜获取到 ${allWallets.length} 个钱包`);

                    // 查找目标钱包
                    const target = allWallets.find(w => w.wallet_address === targetWallet);
                    if (target) {
                        foundWallet = target;
                    }
                }
            } catch (e) {}
        }
    });

    try {
        await page.goto('https://gmgn.ai/trade/ZAxgSuiP?chain=sol&tab=all', {
            waitUntil: 'networkidle',
            timeout: 30000
        });
        await page.waitForTimeout(5000);
    } catch (e) {
        console.log('页面加载错误:', e.message);
    }

    await browser.close();

    console.log('═'.repeat(60));

    if (foundWallet) {
        console.log('✅ 找到目标钱包!\n');
        console.log('📋 钱包信息:');
        console.log(`   名称: ${foundWallet.twitter_name || foundWallet.name || 'N/A'}`);
        console.log(`   Twitter: @${foundWallet.twitter_username || 'N/A'}`);
        console.log(`   地址: ${targetWallet.slice(0, 8)}...${targetWallet.slice(-8)}`);

        const winRate1d = foundWallet.winrate_1d || foundWallet.winrate || 0;
        const winRate7d = foundWallet.winrate_7d || 0;
        console.log(`\n🎯 胜率:`);
        console.log(`   1日胜率: ${(winRate1d * 100).toFixed(1)}% ${winRate1d >= 0.7 ? '✅' : '❌'}`);
        console.log(`   7日胜率: ${(winRate7d * 100).toFixed(1)}% ${winRate7d >= 0.7 ? '✅' : '❌'}`);

        const dailyTrades = foundWallet.buy_1d || foundWallet.txs_1d || 0;
        console.log(`\n📈 交易:`);
        console.log(`   日交易数: ${dailyTrades} ${dailyTrades <= 200 ? '✅' : '❌'}`);

        const profit7d = parseFloat(foundWallet.realized_profit_7d || 0);
        console.log(`\n💰 盈利:`);
        console.log(`   7天盈利: $${(profit7d / 1000).toFixed(1)}K`);

        const totalTokens = foundWallet.token_num_7d || foundWallet.token_num || 1;
        const bigWinners = foundWallet.pnl_gt_500_num || foundWallet.pnl_gt_5x_num || 0;
        const severeLosses = foundWallet.pnl_lt_minus_50_num || 0;
        const moderateLosses = foundWallet.pnl_minus_50_0_num || 0;

        console.log(`\n📊 收益分布 (${totalTokens} 个代币):`);
        console.log(`   大赢(>500%): ${bigWinners} ${bigWinners >= 1 ? '✅' : '❌'}`);
        console.log(`   重亏(<-50%): ${severeLosses} (${(severeLosses/totalTokens*100).toFixed(0)}%) ${severeLosses/totalTokens <= 0.1 ? '✅' : '❌'}`);
        console.log(`   亏损(-50%~0%): ${moderateLosses} (${(moderateLosses/totalTokens*100).toFixed(0)}%) ${moderateLosses/totalTokens <= 0.3 ? '✅' : '❌'}`);

        // 综合判断
        const passed =
            winRate1d >= 0.7 &&
            winRate7d >= 0.7 &&
            dailyTrades <= 200 &&
            bigWinners >= 1 &&
            (severeLosses / totalTokens) <= 0.1 &&
            (moderateLosses / totalTokens) <= 0.3;

        console.log('\n' + '═'.repeat(60));
        console.log(passed ? '🎉 结果: ✅ 符合筛选条件!' : '❌ 结果: 不符合筛选条件');

    } else {
        console.log('❌ 未找到该钱包\n');
        console.log('可能原因:');
        console.log('  1. 该钱包不在 Smart Money / smart_degen 排行');
        console.log('  2. 排名在50名之后');
        console.log('  3. 不是 GMGN 认定的 Smart Money\n');

        if (allWallets.length > 0) {
            console.log('当前 Top 5 钱包:');
            allWallets.slice(0, 5).forEach((w, i) => {
                const name = w.twitter_name || w.name || w.wallet_address?.slice(0, 8);
                const wr = ((w.winrate_7d || 0) * 100).toFixed(0);
                console.log(`   ${i + 1}. ${name.padEnd(15)} | 7d胜率: ${wr}%`);
            });
        }
    }

    console.log('═'.repeat(60) + '\n');
}

main();
