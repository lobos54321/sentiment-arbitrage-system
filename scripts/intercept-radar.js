/**
 * 使用 Playwright 截获 GMGN 雷达页面的实时数据
 *
 * 通过拦截网络请求获取:
 * - 命中代币 (hit tokens)
 * - 命中代币 PNL
 * - 钱包排行实时数据
 */

import { chromium } from 'playwright';
import fs from 'fs';

const RADAR_URL = 'https://gmgn.ai/trade/ZAxgSuiP?chain=sol';

async function interceptRadarData() {
    console.log('\n========== Playwright 截获雷达页面数据 ==========\n');

    // 加载已保存的 session
    const sessionPath = './config/gmgn_session.json';
    let storageState = null;

    if (fs.existsSync(sessionPath)) {
        try {
            storageState = JSON.parse(fs.readFileSync(sessionPath, 'utf8'));
            console.log('✅ 加载已保存的登录状态');
        } catch (e) {
            console.log('⚠️ 无法加载 session，将以匿名模式访问');
        }
    }

    const browser = await chromium.launch({
        headless: true  // 改为 false 可以看到浏览器
    });

    const context = await browser.newContext({
        storageState: storageState,
        userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        viewport: { width: 1920, height: 1080 }
    });

    const page = await context.newPage();

    // 收集的数据
    const capturedData = {
        wallets: [],
        tokens: [],
        cards: [],
        apiResponses: []
    };

    // 拦截所有 API 请求
    page.on('response', async (response) => {
        const url = response.url();

        // 只关注 GMGN API
        if (!url.includes('gmgn.ai/defi/quotation')) return;

        try {
            const contentType = response.headers()['content-type'] || '';
            if (!contentType.includes('application/json')) return;

            const data = await response.json();

            // 提取 API 路径
            const apiPath = url.split('?')[0].replace('https://gmgn.ai/defi/quotation/', '');

            console.log(`\n📡 捕获 API: ${apiPath}`);

            // 检查是否有关键数据
            if (data.data) {
                // 检查 cards 数据 (命中代币)
                if (data.data.cards && Array.isArray(data.data.cards)) {
                    console.log(`   🎯 找到 cards 数据! 数量: ${data.data.cards.length}`);

                    for (const card of data.data.cards.slice(0, 3)) {
                        console.log(`   代币: ${card.symbol} | MC: $${(Number(card.market_cap) / 1e6).toFixed(2)}M`);

                        if (card.wallets && card.wallets.length > 0) {
                            console.log(`   📊 包含 ${card.wallets.length} 个钱包:`);
                            for (const w of card.wallets.slice(0, 2)) {
                                console.log(`      ${w.twitter_name || w.wallet_address?.slice(0, 8)} | net_inflow: $${Number(w.net_inflow || 0).toFixed(2)} | ${w.side}`);
                            }
                        }
                    }

                    capturedData.cards.push(...data.data.cards);
                }

                // 检查钱包排行数据
                if (data.data.rank && Array.isArray(data.data.rank)) {
                    console.log(`   🏆 找到钱包排行! 数量: ${data.data.rank.length}`);

                    // 检查第一个钱包的字段
                    const firstWallet = data.data.rank[0];
                    if (firstWallet) {
                        const hitTokenFields = Object.keys(firstWallet).filter(k =>
                            k.includes('token') || k.includes('hit') || k.includes('card')
                        );
                        if (hitTokenFields.length > 0) {
                            console.log(`   命中代币相关字段: ${hitTokenFields.join(', ')}`);
                        }

                        // 检查是否有嵌套的 tokens/cards 数组
                        if (firstWallet.tokens || firstWallet.cards || firstWallet.hit_tokens) {
                            console.log(`   🎯🎯🎯 钱包数据包含命中代币!!!`);
                            console.log(`   ${JSON.stringify(firstWallet.tokens || firstWallet.cards || firstWallet.hit_tokens).slice(0, 300)}`);
                        }
                    }

                    capturedData.wallets.push(...data.data.rank);
                }

                // 保存完整响应
                capturedData.apiResponses.push({
                    url: apiPath,
                    timestamp: new Date().toISOString(),
                    data: data.data
                });
            }

        } catch (e) {
            // JSON 解析失败，忽略
        }
    });

    // 拦截 WebSocket 消息
    page.on('websocket', ws => {
        console.log(`\n🔌 WebSocket 连接: ${ws.url()}`);

        ws.on('framereceived', frame => {
            const payload = frame.payload;
            if (typeof payload === 'string' && payload.length > 10) {
                // 检查是否包含命中代币数据
                if (payload.includes('cards') || payload.includes('wallets') || payload.includes('net_inflow')) {
                    console.log(`\n🎯 WebSocket 数据包含目标字段!`);
                    console.log(`   ${payload.slice(0, 500)}`);

                    try {
                        const wsData = JSON.parse(payload);
                        if (wsData.data?.cards) {
                            capturedData.cards.push(...wsData.data.cards);
                        }
                    } catch (e) {
                        // 可能不是纯 JSON
                    }
                }
            }
        });
    });

    console.log(`\n🌐 正在访问: ${RADAR_URL}\n`);

    try {
        await page.goto(RADAR_URL, {
            waitUntil: 'networkidle',
            timeout: 60000
        });

        console.log('\n✅ 页面加载完成，等待更多数据...\n');

        // 等待更多数据加载
        await page.waitForTimeout(10000);

        // 尝试滚动页面以触发更多数据加载
        await page.evaluate(() => {
            window.scrollBy(0, 500);
        });

        await page.waitForTimeout(5000);

    } catch (error) {
        console.error('页面加载错误:', error.message);
    }

    // 输出结果
    console.log('\n\n========== 捕获结果汇总 ==========\n');
    console.log(`捕获的 API 响应: ${capturedData.apiResponses.length}`);
    console.log(`捕获的钱包数据: ${capturedData.wallets.length}`);
    console.log(`捕获的 Cards 数据: ${capturedData.cards.length}`);

    // 保存捕获的数据
    if (capturedData.apiResponses.length > 0 || capturedData.cards.length > 0) {
        const outputPath = './logs/radar-captured-data.json';
        fs.mkdirSync('./logs', { recursive: true });
        fs.writeFileSync(outputPath, JSON.stringify(capturedData, null, 2));
        console.log(`\n💾 数据已保存到: ${outputPath}`);
    }

    // 显示捕获的 API 端点
    if (capturedData.apiResponses.length > 0) {
        console.log('\n\n========== 捕获的 API 端点 ==========\n');
        const uniqueUrls = [...new Set(capturedData.apiResponses.map(r => r.url))];
        for (const url of uniqueUrls) {
            console.log(`📡 ${url}`);
        }
    }

    // 如果捕获到 cards 数据，显示详情
    if (capturedData.cards.length > 0) {
        console.log('\n\n========== 命中代币数据 (Cards) ==========\n');

        // 去重
        const uniqueCards = [];
        const seenAddresses = new Set();
        for (const card of capturedData.cards) {
            if (!seenAddresses.has(card.address)) {
                seenAddresses.add(card.address);
                uniqueCards.push(card);
            }
        }

        for (const card of uniqueCards.slice(0, 10)) {
            console.log(`\n🪙 ${card.symbol} (${card.address?.slice(0, 12)}...)`);
            console.log(`   Market Cap: $${(Number(card.market_cap) / 1e6).toFixed(2)}M`);

            if (card.wallets && card.wallets.length > 0) {
                console.log(`   交易钱包 (${card.wallets.length}个):`);
                for (const w of card.wallets.slice(0, 5)) {
                    const name = w.twitter_name || w.wallet_address?.slice(0, 10);
                    const inflow = Number(w.net_inflow || 0).toFixed(2);
                    const tags = (w.tags || []).join(', ');
                    console.log(`     • ${name} | $${inflow} | ${w.side} | [${tags}]`);
                }
            }
        }
    }

    await browser.close();
    console.log('\n✅ 浏览器已关闭');
}

interceptRadarData().catch(console.error);
