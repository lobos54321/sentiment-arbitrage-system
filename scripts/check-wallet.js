/**
 * 检查特定钱包是否符合 Shadow Protocol v2 筛选条件
 * 用法: node scripts/check-wallet.js <wallet_address>
 */

import { chromium } from 'playwright';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const sessionPath = path.join(__dirname, '../config/gmgn_session.json');

// 筛选条件 (与 ShadowProtocolV2 一致)
const CRITERIA = {
    minWinRate1d: 0.7,         // 1日胜率 >= 70%
    minWinRate7d: 0.7,         // 7日胜率 >= 70%
    minBigWinners: 1,          // 至少1个 >500% 的代币
    maxSevereLossRate: 0.1,    // 重亏 <-50% 不超过 10%
    maxModerateLossRate: 0.3,  // 亏损 -50%~0% 不超过 30%
    maxDailyTrades: 200        // 日交易 < 200
};

async function checkWallet(walletAddress) {
    console.log(`\n🔍 检查钱包: ${walletAddress}\n`);
    console.log('═'.repeat(60));

    if (!fs.existsSync(sessionPath)) {
        console.error('❌ GMGN Session 不存在，请先运行 gmgn-login-setup.js');
        process.exit(1);
    }

    const browser = await chromium.launch({ headless: true });
    const context = await browser.newContext({ storageState: sessionPath });
    const page = await context.newPage();

    let walletData = null;

    // 监听 API 响应
    page.on('response', async (response) => {
        const url = response.url();
        if (response.status() === 200 && url.includes(walletAddress)) {
            try {
                const data = await response.json();
                if (data?.data) {
                    walletData = data.data;
                }
            } catch (e) {}
        }
    });

    try {
        // 访问钱包页面
        console.log('📡 获取钱包数据...\n');
        await page.goto(`https://gmgn.ai/sol/address/${walletAddress}`, {
            waitUntil: 'networkidle',
            timeout: 30000
        });
        await page.waitForTimeout(3000);

        if (!walletData) {
            console.log('⚠️ 未能获取钱包数据，尝试从页面提取...');
        }

    } catch (error) {
        console.error('获取数据失败:', error.message);
    }

    await browser.close();

    if (!walletData) {
        console.log('❌ 无法获取钱包数据');
        return;
    }

    // 显示钱包信息
    console.log('📊 钱包信息:');
    console.log('─'.repeat(60));

    const name = walletData.twitter_name || walletData.name || walletAddress.slice(0, 8);
    const twitter = walletData.twitter_username || 'N/A';
    console.log(`   名称: ${name} (@${twitter})`);
    console.log(`   地址: ${walletAddress.slice(0, 8)}...${walletAddress.slice(-8)}`);

    // 胜率数据
    const winRate1d = walletData.winrate_1d || walletData.winrate || 0;
    const winRate7d = walletData.winrate_7d || 0;
    console.log(`\n🎯 胜率:`);
    console.log(`   1日胜率: ${(winRate1d * 100).toFixed(1)}% ${winRate1d >= CRITERIA.minWinRate1d ? '✅' : '❌'} (要求 >= ${CRITERIA.minWinRate1d * 100}%)`);
    console.log(`   7日胜率: ${(winRate7d * 100).toFixed(1)}% ${winRate7d >= CRITERIA.minWinRate7d ? '✅' : '❌'} (要求 >= ${CRITERIA.minWinRate7d * 100}%)`);

    // 交易数据
    const dailyTrades = walletData.buy_1d || walletData.txs_1d || 0;
    console.log(`\n📈 交易量:`);
    console.log(`   日交易数: ${dailyTrades} ${dailyTrades <= CRITERIA.maxDailyTrades ? '✅' : '❌'} (要求 <= ${CRITERIA.maxDailyTrades})`);

    // 盈利数据
    const profit7d = parseFloat(walletData.realized_profit_7d || 0);
    console.log(`\n💰 盈利:`);
    console.log(`   7天盈利: $${(profit7d / 1000).toFixed(1)}K`);

    // 收益分布
    const totalTokens = walletData.token_num_7d || walletData.token_num || 1;
    const bigWinners = walletData.pnl_gt_500_num || walletData.pnl_gt_5x_num || 0;
    const severeLosses = walletData.pnl_lt_minus_50_num || 0;
    const moderateLosses = walletData.pnl_minus_50_0_num || 0;
    const severeLossRate = totalTokens > 0 ? severeLosses / totalTokens : 0;
    const moderateLossRate = totalTokens > 0 ? moderateLosses / totalTokens : 0;

    console.log(`\n📊 收益分布 (7天交易 ${totalTokens} 个代币):`);
    console.log(`   大赢(>500%): ${bigWinners}个 ${bigWinners >= CRITERIA.minBigWinners ? '✅' : '❌'} (要求 >= ${CRITERIA.minBigWinners})`);
    console.log(`   重亏(<-50%): ${severeLosses}个 (${(severeLossRate * 100).toFixed(0)}%) ${severeLossRate <= CRITERIA.maxSevereLossRate ? '✅' : '❌'} (要求 <= ${CRITERIA.maxSevereLossRate * 100}%)`);
    console.log(`   亏损(-50%~0%): ${moderateLosses}个 (${(moderateLossRate * 100).toFixed(0)}%) ${moderateLossRate <= CRITERIA.maxModerateLossRate ? '✅' : '❌'} (要求 <= ${CRITERIA.maxModerateLossRate * 100}%)`);

    // 综合判断
    console.log('\n' + '═'.repeat(60));

    const passed =
        winRate1d >= CRITERIA.minWinRate1d &&
        winRate7d >= CRITERIA.minWinRate7d &&
        dailyTrades <= CRITERIA.maxDailyTrades &&
        bigWinners >= CRITERIA.minBigWinners &&
        severeLossRate <= CRITERIA.maxSevereLossRate &&
        moderateLossRate <= CRITERIA.maxModerateLossRate;

    if (passed) {
        console.log('🎉 结果: ✅ 符合筛选条件，会被追踪!');
    } else {
        console.log('❌ 结果: 不符合筛选条件');

        // 显示不通过的原因
        const reasons = [];
        if (winRate1d < CRITERIA.minWinRate1d) reasons.push(`1d胜率${(winRate1d*100).toFixed(0)}% < ${CRITERIA.minWinRate1d*100}%`);
        if (winRate7d < CRITERIA.minWinRate7d) reasons.push(`7d胜率${(winRate7d*100).toFixed(0)}% < ${CRITERIA.minWinRate7d*100}%`);
        if (dailyTrades > CRITERIA.maxDailyTrades) reasons.push(`日交易${dailyTrades}次 > ${CRITERIA.maxDailyTrades}`);
        if (bigWinners < CRITERIA.minBigWinners) reasons.push(`大赢${bigWinners}个 < ${CRITERIA.minBigWinners}`);
        if (severeLossRate > CRITERIA.maxSevereLossRate) reasons.push(`重亏率${(severeLossRate*100).toFixed(0)}% > ${CRITERIA.maxSevereLossRate*100}%`);
        if (moderateLossRate > CRITERIA.maxModerateLossRate) reasons.push(`亏损率${(moderateLossRate*100).toFixed(0)}% > ${CRITERIA.maxModerateLossRate*100}%`);

        console.log('   原因: ' + reasons.join(', '));
    }
    console.log('═'.repeat(60) + '\n');

    // 输出原始数据供调试
    if (process.argv.includes('--debug')) {
        console.log('\n📋 原始数据:');
        console.log(JSON.stringify(walletData, null, 2));
    }
}

// 主函数
const walletAddress = process.argv[2] || 'FewEdBFYiz85hMSYCUpTRnHerqg4fJjPvGLUvtNj1gRf';
checkWallet(walletAddress);
