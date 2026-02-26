/**
 * 检查 Shadow Protocol 钱包活跃度
 * 使用 GMGN session 查询钱包最近交易情况
 */

import { chromium } from 'playwright';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SESSION_PATH = path.join(__dirname, '../config/gmgn_session.json');

// 候选影子钱包
const SHADOW_CANDIDATES = [
  { address: 'H2ikJvq8or5MyjvFowD7CDY6fG3Sc2yi4mxTnfovXy3K', name: 'shatter.sol', tier: 'S' },
  { address: '5CP6zv8a17mz91v6rMruVH6ziC5qAL8GFaJzwrX9Fvup', name: 'naseem', tier: 'S' },
  { address: 'EdCNh8EzETJLFphW8yvdY7rDd8zBiyweiz8DU5gUUUka', name: 'cifwifhatday.sol', tier: 'S' },
  { address: '4EtAJ1p8RjqccEVhEhaYnEgQ6kA4JHR8oYqyLFwARUj6', name: 'Trump whale', tier: 'A' },
  { address: '8zFZHuSRuDpuAR7J6FzwyF3vKNx4CVW3DFHJerQhc7Zd', name: 'traderpow', tier: 'A' },
  { address: '8mZYBV8aPvPCo34CyCmt6fWkZRFviAUoBZr1Bn993gro', name: 'popchad.sol', tier: 'A' },
  { address: '2h7s3FpSvc6v2oHke6Uqg191B5fPCeFTmMGnh5oPWhX7', name: 'tonka.sol', tier: 'B' },
  { address: 'HWdeCUjBvPP1HJ5oCJt7aNsvMWpWoDgiejUWvfFX6T7R', name: '匿名高手', tier: 'B' },
];

async function checkWalletActivity(page, wallet) {
  const url = `https://gmgn.ai/sol/address/${wallet.address}`;

  try {
    console.log(`\n🔍 检查: ${wallet.name} (${wallet.address.slice(0, 8)}...)`);

    await page.goto(url, { waitUntil: 'networkidle', timeout: 30000 });
    await page.waitForTimeout(2000);

    // 尝试获取页面信息
    const pageContent = await page.content();

    // 检查是否有交易数据
    const stats = await page.evaluate(() => {
      const result = {
        totalPnl: null,
        winRate: null,
        lastActive: null,
        recentTrades: 0,
        isActive: false
      };

      // 尝试获取 PnL
      const pnlElements = document.querySelectorAll('[class*="pnl"], [class*="profit"]');
      pnlElements.forEach(el => {
        const text = el.textContent;
        if (text && text.includes('$')) {
          result.totalPnl = text.trim();
        }
      });

      // 尝试获取胜率
      const winRateElements = document.querySelectorAll('[class*="win"], [class*="rate"]');
      winRateElements.forEach(el => {
        const text = el.textContent;
        if (text && text.includes('%')) {
          result.winRate = text.trim();
        }
      });

      // 检查是否有最近交易（7天内）
      const timeElements = document.querySelectorAll('[class*="time"], [class*="ago"], time');
      timeElements.forEach(el => {
        const text = el.textContent?.toLowerCase() || '';
        if (text.includes('hour') || text.includes('min') || text.includes('sec') ||
            text.includes('小时') || text.includes('分钟') || text.includes('秒')) {
          result.recentTrades++;
          result.isActive = true;
        }
        if (text.includes('day') || text.includes('天')) {
          const days = parseInt(text.match(/\d+/)?.[0] || '99');
          if (days <= 7) {
            result.recentTrades++;
            result.isActive = true;
          }
        }
      });

      return result;
    });

    // 截图保存
    const screenshotPath = path.join(__dirname, `../logs/wallet_${wallet.address.slice(0, 8)}.png`);
    await page.screenshot({ path: screenshotPath, fullPage: false });

    return {
      ...wallet,
      ...stats,
      checked: true,
      error: null
    };

  } catch (error) {
    console.error(`   ❌ 检查失败: ${error.message}`);
    return {
      ...wallet,
      checked: false,
      error: error.message
    };
  }
}

async function main() {
  console.log('🔍 Shadow Protocol 钱包活跃度检查');
  console.log('═'.repeat(60));

  if (!fs.existsSync(SESSION_PATH)) {
    console.error('❌ 未找到 GMGN Session! 请先运行 gmgn-login-setup.js');
    process.exit(1);
  }

  // 确保 logs 目录存在
  const logsDir = path.join(__dirname, '../logs');
  if (!fs.existsSync(logsDir)) {
    fs.mkdirSync(logsDir, { recursive: true });
  }

  const browser = await chromium.launch({ headless: false }); // 非无头模式方便调试
  const context = await browser.newContext({
    storageState: SESSION_PATH,
    viewport: { width: 1920, height: 1080 }
  });
  const page = await context.newPage();

  const results = [];

  for (const wallet of SHADOW_CANDIDATES) {
    const result = await checkWalletActivity(page, wallet);
    results.push(result);

    // 打印状态
    const status = result.isActive ? '✅ 活跃' : '⚠️ 不活跃';
    const pnl = result.totalPnl || 'N/A';
    const winRate = result.winRate || 'N/A';

    console.log(`   ${status} | PnL: ${pnl} | 胜率: ${winRate} | 近期交易: ${result.recentTrades}`);

    // 避免请求过快
    await page.waitForTimeout(3000);
  }

  await browser.close();

  // 汇总结果
  console.log('\n' + '═'.repeat(60));
  console.log('📊 检查结果汇总');
  console.log('═'.repeat(60));

  const activeWallets = results.filter(r => r.isActive);
  const inactiveWallets = results.filter(r => !r.isActive && r.checked);
  const failedWallets = results.filter(r => !r.checked);

  console.log(`\n✅ 活跃钱包 (${activeWallets.length}个):`);
  activeWallets.forEach(w => {
    console.log(`   [${w.tier}] ${w.name}: ${w.address.slice(0, 16)}...`);
  });

  console.log(`\n⚠️ 不活跃钱包 (${inactiveWallets.length}个):`);
  inactiveWallets.forEach(w => {
    console.log(`   [${w.tier}] ${w.name}: ${w.address.slice(0, 16)}...`);
  });

  if (failedWallets.length > 0) {
    console.log(`\n❌ 检查失败 (${failedWallets.length}个):`);
    failedWallets.forEach(w => {
      console.log(`   [${w.tier}] ${w.name}: ${w.error}`);
    });
  }

  // 推荐配置
  console.log('\n' + '═'.repeat(60));
  console.log('🎯 推荐 Shadow Protocol 配置');
  console.log('═'.repeat(60));
  console.log('\nconst SHADOW_WALLETS = [');
  activeWallets.forEach(w => {
    console.log(`  '${w.address}',  // ${w.name} [Tier ${w.tier}]`);
  });
  console.log('];');
}

main().catch(console.error);
