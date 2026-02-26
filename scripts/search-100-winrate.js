/**
 * 搜索100%胜率钱包
 */
import { chromium } from 'playwright';
import fs from 'fs';

const sessionPath = './config/gmgn_session.json';
const targetWallet = process.argv[2] || 'FewEdBFYiz85hMSYCUpTRnHerqg4fJjPvGLUvtNj1gRf';

async function main() {
    if (!fs.existsSync(sessionPath)) {
        console.log('Session 不存在');
        return;
    }

    const browser = await chromium.launch({ headless: true });
    const context = await browser.newContext({ storageState: sessionPath });
    const page = await context.newPage();

    let allWallets = [];

    page.on('response', async (response) => {
        const url = response.url();
        if (response.status() === 200 && url.includes('quotation') && url.includes('rank')) {
            try {
                const data = await response.json();
                if (data?.data?.rank) {
                    console.log(`获取到 ${data.data.rank.length} 个钱包`);
                    allWallets.push(...data.data.rank);
                }
            } catch (e) {}
        }
    });

    console.log('访问牛人榜...\n');
    await page.goto('https://gmgn.ai/trade/ZAxgSuiP?chain=sol&tab=all', {
        waitUntil: 'networkidle',
        timeout: 60000
    });
    await page.waitForTimeout(5000);

    await browser.close();

    if (allWallets.length === 0) {
        console.log('❌ 未获取到数据，Session 可能已过期');
        return;
    }

    console.log(`\n总共 ${allWallets.length} 个钱包\n`);
    console.log('═'.repeat(70));

    // 1. 搜索目标钱包
    const target = allWallets.find(w => w.wallet_address === targetWallet);
    if (target) {
        console.log('✅ 找到目标钱包!');
        printWallet(target);
    } else {
        console.log(`❌ 目标钱包 ${targetWallet.slice(0, 16)}... 不在列表中`);
    }

    console.log('\n' + '═'.repeat(70));

    // 2. 显示100%胜率的钱包
    const perfect = allWallets.filter(w => {
        const wr7d = w.winrate_7d || 0;
        return wr7d >= 0.99; // 接近100%
    });

    console.log(`\n🏆 高胜率钱包 (7d >= 99%): ${perfect.length} 个\n`);
    perfect.slice(0, 10).forEach((w, i) => {
        const name = w.twitter_username || w.wallet_address?.slice(0, 12);
        const wr = ((w.winrate_7d || 0) * 100).toFixed(1);
        const profit = (parseFloat(w.realized_profit_7d || 0) / 1000).toFixed(1);
        console.log(`${i + 1}. ${name.padEnd(15)} | 7d胜率: ${wr}% | 盈利: $${profit}K`);
        console.log(`   地址: ${w.wallet_address}`);
    });

    // 3. 显示符合我们筛选条件的钱包
    console.log('\n' + '═'.repeat(70));
    console.log('\n🎯 符合 Shadow v2 筛选条件的钱包:\n');

    const qualified = allWallets.filter(w => {
        const wr1d = w.winrate_1d || w.winrate || 0;
        const wr7d = w.winrate_7d || 0;
        const daily = w.buy_1d || w.txs_1d || 0;
        const total = w.token_num_7d || w.token_num || 1;
        const bigWin = w.pnl_gt_500_num || 0;
        const severe = w.pnl_lt_minus_50_num || 0;
        const moderate = w.pnl_minus_50_0_num || 0;

        return wr1d >= 0.7 &&
            wr7d >= 0.7 &&
            daily <= 200 &&
            bigWin >= 1 &&
            (severe / total) <= 0.1 &&
            (moderate / total) <= 0.3;
    });

    console.log(`符合条件: ${qualified.length}/${allWallets.length}\n`);

    qualified.slice(0, 15).forEach((w, i) => {
        const name = w.twitter_username || w.wallet_address?.slice(0, 12);
        const wr1d = ((w.winrate_1d || w.winrate || 0) * 100).toFixed(0);
        const wr7d = ((w.winrate_7d || 0) * 100).toFixed(0);
        const bigWin = w.pnl_gt_500_num || 0;
        console.log(`${(i + 1).toString().padStart(2)}. ${name.padEnd(15)} | 1d:${wr1d}% 7d:${wr7d}% | 🚀${bigWin}`);
    });
}

function printWallet(w) {
    console.log(`\n地址: ${w.wallet_address}`);
    console.log(`Twitter: @${w.twitter_username || 'N/A'}`);
    console.log(`名称: ${w.twitter_name || w.name || 'N/A'}`);
    console.log(`1d胜率: ${((w.winrate_1d || w.winrate || 0) * 100).toFixed(1)}%`);
    console.log(`7d胜率: ${((w.winrate_7d || 0) * 100).toFixed(1)}%`);
    console.log(`7d盈利: $${(parseFloat(w.realized_profit_7d || 0) / 1000).toFixed(1)}K`);
    console.log(`日交易: ${w.buy_1d || w.txs_1d || 0}`);
    console.log(`大赢>500%: ${w.pnl_gt_500_num || 0}`);
    console.log(`重亏<-50%: ${w.pnl_lt_minus_50_num || 0}`);
}

main();
