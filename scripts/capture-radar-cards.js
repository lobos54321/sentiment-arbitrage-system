/**
 * 专门捕获雷达页面的 cards 数据（命中代币）
 */

import { chromium } from 'playwright';
import fs from 'fs';

const RADAR_URL = 'https://gmgn.ai/trade/ZAxgSuiP?chain=sol';

async function captureRadarCards() {
    console.log('\n========== 捕获雷达 Cards 数据 ==========\n');

    const sessionPath = './config/gmgn_session.json';
    let storageState = null;

    if (fs.existsSync(sessionPath)) {
        storageState = JSON.parse(fs.readFileSync(sessionPath, 'utf8'));
        console.log('✅ 加载登录状态\n');
    }

    const browser = await chromium.launch({
        headless: false,
        args: [
            '--disable-blink-features=AutomationControlled',
            '--disable-infobars',
            '--window-size=1920,1080'
        ]
    });

    const context = await browser.newContext({
        storageState: storageState,
        userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        viewport: { width: 1920, height: 1080 },
        locale: 'zh-CN',
        timezoneId: 'Australia/Brisbane'
    });

    await context.addInitScript(() => {
        Object.defineProperty(navigator, 'webdriver', { get: () => false });
    });

    const page = await context.newPage();

    const capturedData = {
        cards: [],
        wallets: [],
        radarDetail: null,
        allApiCalls: []
    };

    // 监听所有 API 响应
    page.on('response', async (response) => {
        const url = response.url();
        if (!url.includes('gmgn.ai')) return;

        try {
            const contentType = response.headers()['content-type'] || '';
            if (!contentType.includes('json')) return;

            const data = await response.json();
            const path = url.split('?')[0].replace('https://gmgn.ai', '');

            // 记录所有 API 调用
            const topKeys = data.data ? Object.keys(data.data) : [];
            capturedData.allApiCalls.push({
                path,
                hasData: !!data.data,
                topKeys: topKeys,
                code: data.code
            });

            console.log('📡 ' + path);

            // 检查 cards
            if (data.data && data.data.cards) {
                const cardsLen = data.data.cards.length;
                console.log('   🎯 CARDS: ' + cardsLen + ' 条');
                capturedData.cards = data.data.cards;

                // 显示第一个 card 的结构
                const firstCard = data.data.cards[0];
                if (firstCard) {
                    const cardKeys = Object.keys(firstCard);
                    console.log('   Card 结构: ' + cardKeys.join(', '));

                    // 检查 wallets
                    if (firstCard.wallets) {
                        console.log('   Wallets: ' + firstCard.wallets.length);
                        const firstWallet = firstCard.wallets[0];
                        if (firstWallet) {
                            const walletKeys = Object.keys(firstWallet);
                            console.log('   Wallet 结构: ' + walletKeys.join(', '));
                        }
                    }
                }
            }

            // 检查 rank
            if (data.data && data.data.rank) {
                console.log('   📊 RANK: ' + data.data.rank.length + ' 条');
                capturedData.wallets = data.data.rank;
            }

            // 雷达详情
            if (path.includes('/radar/')) {
                const preview = JSON.stringify(data.data || {}).slice(0, 200);
                console.log('   📋 雷达数据: ' + preview);
                capturedData.radarDetail = data.data;
            }

        } catch (e) {
            // ignore
        }
    });

    console.log('🌐 访问: ' + RADAR_URL + '\n');

    try {
        await page.goto(RADAR_URL, { waitUntil: 'domcontentloaded', timeout: 30000 });

        console.log('✅ 页面加载中...\n');
        console.log('⏳ 等待 30 秒让页面完全加载...\n');

        await page.waitForTimeout(30000);

        // 尝试点击不同的排序按钮
        const buttons = ['买入最多', '利润最高', '买入最早', '共同持仓'];
        for (const btn of buttons) {
            try {
                const button = page.locator('text=' + btn).first();
                const isVisible = await button.isVisible({ timeout: 2000 });
                if (isVisible) {
                    console.log('\n🔘 点击: ' + btn);
                    await button.click();
                    await page.waitForTimeout(3000);
                }
            } catch (e) {
                // 按钮可能不存在
            }
        }

        // 截图
        await page.screenshot({ path: './logs/radar-cards-screenshot.png', fullPage: false });
        console.log('\n📸 截图已保存');

    } catch (error) {
        console.error('错误:', error.message);
    }

    // 结果
    console.log('\n\n========== 捕获结果 ==========\n');
    console.log('Cards 数据: ' + capturedData.cards.length + ' 条');
    console.log('Wallets 数据: ' + capturedData.wallets.length + ' 条');
    console.log('API 调用: ' + capturedData.allApiCalls.length + ' 次');

    if (capturedData.cards.length > 0) {
        console.log('\n📋 Cards 详情:');
        for (const card of capturedData.cards.slice(0, 3)) {
            const tokenName = card.symbol || (card.token_address ? card.token_address.slice(0, 12) : 'unknown');
            console.log('\n  Token: ' + tokenName);
            console.log('  Address: ' + (card.token_address || card.address));
            if (card.wallets) {
                console.log('  Wallets: ' + card.wallets.length);
                for (const w of card.wallets.slice(0, 2)) {
                    const addr = w.wallet_address ? w.wallet_address.slice(0, 12) : 'unknown';
                    const pnl = w.net_inflow || w.pnl || 'N/A';
                    console.log('    - ' + addr + ' | PNL: ' + pnl);
                }
            }
        }
    }

    // 显示所有 API 调用
    console.log('\n📡 所有 API 调用:');
    for (const api of capturedData.allApiCalls) {
        console.log('  ' + api.path + ' | keys: ' + api.topKeys.join(', '));
    }

    // 保存数据
    fs.writeFileSync('./logs/radar-cards-data.json', JSON.stringify(capturedData, null, 2));
    console.log('\n💾 数据已保存到 ./logs/radar-cards-data.json');

    await new Promise(r => setTimeout(r, 5000));
    await browser.close();
    console.log('\n✅ 完成');
}

captureRadarCards().catch(console.error);
