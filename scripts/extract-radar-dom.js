/**
 * 从雷达页面 DOM 中提取数据
 *
 * 数据可能存储在 React state、window 对象或 DOM 属性中
 */

import { chromium } from 'playwright';
import fs from 'fs';

const RADAR_URL = 'https://gmgn.ai/trade/ZAxgSuiP?chain=sol';

async function extractFromDOM() {
    console.log('\n========== 从 DOM 提取雷达数据 ==========\n');

    const sessionPath = './config/gmgn_session.json';
    let storageState = null;

    if (fs.existsSync(sessionPath)) {
        try {
            storageState = JSON.parse(fs.readFileSync(sessionPath, 'utf8'));
            console.log('✅ 加载登录状态\n');
        } catch (e) {
            console.log('⚠️ 无法加载 session\n');
        }
    }

    const browser = await chromium.launch({ headless: true });
    const context = await browser.newContext({
        storageState: storageState,
        userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        viewport: { width: 1920, height: 1080 }
    });

    const page = await context.newPage();

    console.log(`🌐 访问: ${RADAR_URL}\n`);

    try {
        await page.goto(RADAR_URL, {
            waitUntil: 'networkidle',
            timeout: 60000
        });

        console.log('✅ 页面加载完成');

        // 等待表格渲染
        await page.waitForTimeout(5000);

        // 尝试从 window 对象获取数据
        console.log('\n📊 检查 window 对象...');

        const windowData = await page.evaluate(() => {
            const results = {};

            // 检查常见的数据存储位置
            const checkKeys = [
                '__NEXT_DATA__',
                '__NUXT__',
                '__INITIAL_STATE__',
                'gmgn',
                'app',
                'data',
                'store',
                'state'
            ];

            for (const key of checkKeys) {
                if (window[key]) {
                    results[key] = typeof window[key] === 'object'
                        ? JSON.stringify(window[key]).slice(0, 500)
                        : String(window[key]).slice(0, 200);
                }
            }

            // 检查所有以 __ 开头的键
            for (const key of Object.keys(window)) {
                if (key.startsWith('__') && window[key] && typeof window[key] === 'object') {
                    try {
                        const str = JSON.stringify(window[key]);
                        if (str.length > 100 && str.length < 10000) {
                            results[key] = str.slice(0, 500);
                        }
                    } catch (e) {
                        // ignore
                    }
                }
            }

            return results;
        });

        console.log('Window 数据:');
        for (const [key, value] of Object.entries(windowData)) {
            console.log(`  ${key}: ${value.slice(0, 150)}...`);
        }

        // 直接从表格提取数据
        console.log('\n📊 从表格 DOM 提取数据...');

        const tableData = await page.evaluate(() => {
            const rows = [];

            // 尝试找到表格行
            const tableRows = document.querySelectorAll('tr, [class*="table"] > div, [class*="row"]');

            for (const row of Array.from(tableRows).slice(0, 20)) {
                const cells = row.querySelectorAll('td, [class*="cell"], [class*="col"]');
                if (cells.length > 3) {
                    const rowData = {
                        cells: Array.from(cells).map(c => c.textContent?.trim().slice(0, 50))
                    };

                    // 检查是否有代币图标/链接
                    const links = row.querySelectorAll('a[href*="/token/"]');
                    if (links.length > 0) {
                        rowData.tokenLinks = Array.from(links).map(l => l.href);
                    }

                    // 检查图片 (代币 logo)
                    const imgs = row.querySelectorAll('img');
                    if (imgs.length > 0) {
                        rowData.images = Array.from(imgs).map(i => i.src?.slice(0, 100));
                    }

                    rows.push(rowData);
                }
            }

            return rows;
        });

        console.log(`表格行数: ${tableData.length}`);
        for (const row of tableData.slice(0, 5)) {
            console.log(`  ${JSON.stringify(row).slice(0, 200)}`);
        }

        // 提取钱包地址和代币地址
        console.log('\n📊 提取钱包和代币地址...');

        const addresses = await page.evaluate(() => {
            const wallets = new Set();
            const tokens = new Set();

            // 查找所有链接
            const links = document.querySelectorAll('a');
            for (const link of links) {
                const href = link.href || '';
                // 钱包地址 (Solana 格式: 32-44 字符的 base58)
                const walletMatch = href.match(/wallet\/([A-Za-z0-9]{32,44})/);
                if (walletMatch) wallets.add(walletMatch[1]);

                // 代币地址
                const tokenMatch = href.match(/token\/([A-Za-z0-9]{32,44})/);
                if (tokenMatch) tokens.add(tokenMatch[1]);
            }

            // 也检查文本内容
            const allText = document.body.innerText;
            const addressPattern = /[A-Za-z0-9]{32,44}/g;
            const matches = allText.match(addressPattern) || [];

            return {
                wallets: Array.from(wallets),
                tokens: Array.from(tokens),
                textAddresses: matches.slice(0, 20)
            };
        });

        console.log(`钱包地址: ${addresses.wallets.length}`);
        for (const w of addresses.wallets.slice(0, 5)) {
            console.log(`  ${w}`);
        }

        console.log(`代币地址: ${addresses.tokens.length}`);
        for (const t of addresses.tokens.slice(0, 5)) {
            console.log(`  ${t}`);
        }

        // 提取表格中的具体数据
        console.log('\n📊 提取表格详细数据...');

        const detailedData = await page.evaluate(() => {
            const data = [];

            // 查找包含 PNL 数据的元素
            const pnlElements = document.querySelectorAll('[class*="pnl"], [class*="profit"], [class*="Pnl"]');
            for (const el of Array.from(pnlElements).slice(0, 10)) {
                data.push({
                    type: 'pnl',
                    text: el.textContent?.trim(),
                    class: el.className
                });
            }

            // 查找代币符号
            const symbolElements = document.querySelectorAll('[class*="symbol"], [class*="Symbol"], [class*="token"]');
            for (const el of Array.from(symbolElements).slice(0, 10)) {
                data.push({
                    type: 'symbol',
                    text: el.textContent?.trim(),
                    class: el.className
                });
            }

            return data;
        });

        console.log('详细数据:');
        for (const item of detailedData.slice(0, 10)) {
            console.log(`  [${item.type}] ${item.text}`);
        }

        // 截图
        const screenshotPath = './logs/radar-screenshot.png';
        await page.screenshot({ path: screenshotPath, fullPage: false });
        console.log(`\n📸 截图保存到: ${screenshotPath}`);

        // 保存完整提取数据
        const outputPath = './logs/radar-dom-data.json';
        fs.writeFileSync(outputPath, JSON.stringify({
            windowData,
            tableData,
            addresses,
            detailedData
        }, null, 2));
        console.log(`💾 数据保存到: ${outputPath}`);

    } catch (error) {
        console.error('错误:', error.message);
    }

    await browser.close();
    console.log('\n✅ 完成');
}

extractFromDOM().catch(console.error);
