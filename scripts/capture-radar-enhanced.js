/**
 * 使用增强配置截获雷达页面
 *
 * 绕过 Cloudflare 检测
 */

import { chromium } from 'playwright';
import fs from 'fs';

const RADAR_URL = 'https://gmgn.ai/trade/ZAxgSuiP?chain=sol';

async function captureRadarEnhanced() {
    console.log('\n========== 增强模式捕获雷达页面 ==========\n');

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

    // 使用更真实的浏览器配置
    const browser = await chromium.launch({
        headless: false,  // 非 headless 模式
        args: [
            '--disable-blink-features=AutomationControlled',
            '--disable-infobars',
            '--window-size=1920,1080',
            '--start-maximized'
        ]
    });

    const context = await browser.newContext({
        storageState: storageState,
        userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        viewport: { width: 1920, height: 1080 },
        locale: 'zh-CN',
        timezoneId: 'Australia/Brisbane',
        permissions: ['geolocation'],
        extraHTTPHeaders: {
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8'
        }
    });

    // 注入脚本来隐藏 webdriver 标志
    await context.addInitScript(() => {
        Object.defineProperty(navigator, 'webdriver', {
            get: () => false,
        });
    });

    const page = await context.newPage();

    const capturedApiData = [];

    // 监听所有网络请求
    page.on('response', async (response) => {
        const url = response.url();

        // 捕获 GMGN API
        if (url.includes('gmgn.ai') && url.includes('/v1/')) {
            try {
                const contentType = response.headers()['content-type'] || '';
                if (!contentType.includes('json')) return;

                const data = await response.json();
                const path = url.split('?')[0].replace('https://gmgn.ai', '');

                console.log(`\n📡 API: ${path}`);

                if (data.data) {
                    if (data.data.cards) {
                        console.log(`   🎯 CARDS: ${data.data.cards.length}`);
                        capturedApiData.push({ type: 'cards', data: data.data.cards, url: path });
                    }
                    if (data.data.rank) {
                        console.log(`   🏆 RANK: ${data.data.rank.length}`);
                        capturedApiData.push({ type: 'rank', data: data.data.rank, url: path });
                    }
                }
            } catch (e) {
                // ignore
            }
        }
    });

    // 监听 WebSocket
    page.on('websocket', ws => {
        console.log(`\n🔌 WebSocket: ${ws.url().slice(0, 60)}...`);

        ws.on('framereceived', frame => {
            if (typeof frame.payload !== 'string') return;

            try {
                const data = JSON.parse(frame.payload);
                if (data.channel && data.channel !== 'heartbeat') {
                    console.log(`📨 WS [${data.channel}]: ${JSON.stringify(data.data || {}).slice(0, 150)}`);

                    if (data.data && Array.isArray(data.data)) {
                        for (const item of data.data) {
                            if (item.cards || item.wallets) {
                                console.log(`   🎯 找到数据!`);
                                capturedApiData.push({ type: 'ws_' + data.channel, data: item });
                            }
                        }
                    }
                }
            } catch (e) {
                // ignore
            }
        });
    });

    console.log(`🌐 访问: ${RADAR_URL}\n`);

    try {
        await page.goto(RADAR_URL, { waitUntil: 'load', timeout: 30000 });

        console.log('✅ 页面加载中...\n');
        console.log('⏳ 等待 20 秒让页面完全渲染...\n');

        await page.waitForTimeout(20000);

        // 截图
        const screenshotPath = './logs/radar-enhanced-screenshot.png';
        await page.screenshot({ path: screenshotPath, fullPage: false });
        console.log(`\n📸 截图: ${screenshotPath}`);

        // 提取页面数据
        const pageData = await page.evaluate(() => {
            const result = {
                title: document.title,
                bodyText: document.body.innerText.slice(0, 2000),
                walletLinks: [],
                tokenLinks: []
            };

            // 提取链接
            document.querySelectorAll('a').forEach(a => {
                const href = a.href || '';
                if (href.includes('/sol/address/')) {
                    result.walletLinks.push(href);
                }
                if (href.includes('/sol/token/')) {
                    result.tokenLinks.push(href);
                }
            });

            return result;
        });

        console.log(`\n页面标题: ${pageData.title}`);
        console.log(`页面文本预览: ${pageData.bodyText.slice(0, 300)}...`);
        console.log(`钱包链接: ${pageData.walletLinks.length}`);
        console.log(`代币链接: ${pageData.tokenLinks.length}`);

    } catch (error) {
        console.error('错误:', error.message);
    }

    // 保存数据
    console.log('\n\n========== 结果 ==========\n');
    console.log(`捕获的 API 数据: ${capturedApiData.length}`);

    if (capturedApiData.length > 0) {
        const outputPath = './logs/radar-enhanced-data.json';
        fs.writeFileSync(outputPath, JSON.stringify(capturedApiData, null, 2));
        console.log(`💾 保存到: ${outputPath}`);
    }

    // 等待用户查看
    console.log('\n⏳ 保持浏览器打开 10 秒...');
    await new Promise(r => setTimeout(r, 10000));

    await browser.close();
    console.log('\n✅ 完成');
}

captureRadarEnhanced().catch(console.error);
