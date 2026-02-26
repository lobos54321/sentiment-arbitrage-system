/**
 * 使用 Playwright 截获 GMGN 雷达页面的 WebSocket 数据
 *
 * 数据通过 WebSocket 实时推送
 */

import { chromium } from 'playwright';
import fs from 'fs';

const RADAR_URL = 'https://gmgn.ai/trade/ZAxgSuiP?chain=sol';

async function interceptRadarWebSocket() {
    console.log('\n========== Playwright 截获雷达 WebSocket 数据 ==========\n');

    // 加载已保存的 session
    const sessionPath = './config/gmgn_session.json';
    let storageState = null;

    if (fs.existsSync(sessionPath)) {
        try {
            storageState = JSON.parse(fs.readFileSync(sessionPath, 'utf8'));
            console.log('✅ 加载已保存的登录状态');
        } catch (e) {
            console.log('⚠️ 无法加载 session');
        }
    }

    const browser = await chromium.launch({
        headless: true
    });

    const context = await browser.newContext({
        storageState: storageState,
        userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        viewport: { width: 1920, height: 1080 }
    });

    const page = await context.newPage();

    // 收集的数据
    const capturedData = {
        wsMessages: [],
        cards: [],
        wallets: []
    };

    let messageCount = 0;

    // 拦截 WebSocket 消息
    page.on('websocket', ws => {
        const wsUrl = ws.url();
        console.log(`\n🔌 WebSocket 连接: ${wsUrl.slice(0, 80)}...`);

        ws.on('framereceived', frame => {
            messageCount++;
            const payload = frame.payload;

            if (typeof payload !== 'string') return;

            // 尝试解析 JSON
            try {
                // Socket.IO 格式: "42["event",{data}]"
                let jsonData = null;

                if (payload.startsWith('42')) {
                    // Socket.IO 消息格式
                    const jsonStr = payload.slice(2);
                    const parsed = JSON.parse(jsonStr);
                    if (Array.isArray(parsed) && parsed.length >= 2) {
                        const eventName = parsed[0];
                        const eventData = parsed[1];

                        console.log(`\n📨 WS消息 #${messageCount}: 事件=${eventName}`);

                        // 检查是否是我们需要的数据
                        if (eventData?.data?.cards || eventData?.cards) {
                            const cards = eventData?.data?.cards || eventData?.cards;
                            console.log(`   🎯🎯🎯 找到 cards 数据! 数量: ${cards.length}`);

                            for (const card of cards.slice(0, 3)) {
                                console.log(`   代币: ${card.symbol}`);
                                if (card.wallets) {
                                    console.log(`   钱包数: ${card.wallets.length}`);
                                    const w = card.wallets[0];
                                    if (w) {
                                        console.log(`   示例: ${w.twitter_name || w.wallet_address?.slice(0, 8)} | net_inflow: ${w.net_inflow}`);
                                    }
                                }
                            }

                            capturedData.cards.push(...cards);
                        }

                        // 检查钱包数据
                        if (eventData?.data?.rank || eventData?.rank) {
                            const rank = eventData?.data?.rank || eventData?.rank;
                            console.log(`   🏆 找到钱包排行! 数量: ${rank.length}`);
                            capturedData.wallets.push(...rank);
                        }

                        capturedData.wsMessages.push({
                            event: eventName,
                            data: eventData,
                            timestamp: new Date().toISOString()
                        });
                    }
                } else if (payload.startsWith('{')) {
                    // 普通 JSON
                    jsonData = JSON.parse(payload);
                    console.log(`\n📨 WS消息 #${messageCount}: JSON`);

                    if (jsonData.data?.cards) {
                        console.log(`   🎯 cards: ${jsonData.data.cards.length}`);
                        capturedData.cards.push(...jsonData.data.cards);
                    }

                    capturedData.wsMessages.push({
                        event: 'json',
                        data: jsonData,
                        timestamp: new Date().toISOString()
                    });
                } else if (payload.length > 5) {
                    // 其他消息
                    if (messageCount <= 20) {
                        console.log(`\n📨 WS消息 #${messageCount}: ${payload.slice(0, 100)}`);
                    }
                }

            } catch (e) {
                // 非 JSON 消息
                if (payload.length > 10 && messageCount <= 10) {
                    console.log(`\n📨 WS消息 #${messageCount} (非JSON): ${payload.slice(0, 80)}`);
                }
            }
        });

        ws.on('framesent', frame => {
            const payload = frame.payload;
            if (typeof payload === 'string' && payload.length > 5 && messageCount <= 20) {
                console.log(`\n📤 发送: ${payload.slice(0, 100)}`);
            }
        });
    });

    // 也拦截 HTTP API
    page.on('response', async (response) => {
        const url = response.url();
        if (!url.includes('gmgn.ai/defi/quotation')) return;

        try {
            const data = await response.json();
            const apiPath = url.split('?')[0].replace('https://gmgn.ai/defi/quotation/', '');

            if (data.data?.cards) {
                console.log(`\n📡 HTTP API 也返回 cards: ${apiPath}`);
                console.log(`   数量: ${data.data.cards.length}`);
                capturedData.cards.push(...data.data.cards);
            }

            if (data.data?.rank) {
                console.log(`\n📡 HTTP API: ${apiPath} (rank: ${data.data.rank.length})`);
            }
        } catch (e) {
            // 忽略
        }
    });

    console.log(`\n🌐 正在访问: ${RADAR_URL}\n`);

    try {
        await page.goto(RADAR_URL, {
            waitUntil: 'domcontentloaded',
            timeout: 30000
        });

        console.log('\n✅ 页面加载中，监听 WebSocket...\n');

        // 等待 WebSocket 数据
        await page.waitForTimeout(15000);

        // 滚动以触发更多数据
        await page.evaluate(() => window.scrollBy(0, 300));
        await page.waitForTimeout(5000);

    } catch (error) {
        console.error('错误:', error.message);
    }

    // 输出结果
    console.log('\n\n========== 捕获结果汇总 ==========\n');
    console.log(`WebSocket 消息数: ${capturedData.wsMessages.length}`);
    console.log(`捕获的 Cards: ${capturedData.cards.length}`);
    console.log(`捕获的钱包: ${capturedData.wallets.length}`);

    // 保存数据
    if (capturedData.cards.length > 0 || capturedData.wsMessages.length > 0) {
        const outputPath = './logs/radar-ws-data.json';
        fs.mkdirSync('./logs', { recursive: true });
        fs.writeFileSync(outputPath, JSON.stringify(capturedData, null, 2));
        console.log(`\n💾 数据已保存到: ${outputPath}`);
    }

    // 显示 cards 数据
    if (capturedData.cards.length > 0) {
        console.log('\n\n========== 命中代币数据 ==========\n');

        // 去重
        const uniqueCards = [];
        const seen = new Set();
        for (const card of capturedData.cards) {
            if (card.address && !seen.has(card.address)) {
                seen.add(card.address);
                uniqueCards.push(card);
            }
        }

        for (const card of uniqueCards.slice(0, 10)) {
            console.log(`\n🪙 ${card.symbol}`);
            console.log(`   地址: ${card.address?.slice(0, 20)}...`);
            console.log(`   Market Cap: $${(Number(card.market_cap || 0) / 1e6).toFixed(2)}M`);

            if (card.wallets && card.wallets.length > 0) {
                console.log(`   交易钱包 (${card.wallets.length}个):`);
                for (const w of card.wallets.slice(0, 3)) {
                    const name = w.twitter_name || w.wallet_address?.slice(0, 10);
                    const inflow = Number(w.net_inflow || 0).toFixed(2);
                    console.log(`     • ${name} | net_inflow: $${inflow} | buys: ${w.buys} | sells: ${w.sells}`);
                }
            }
        }
    }

    // 列出所有 WS 事件类型
    if (capturedData.wsMessages.length > 0) {
        console.log('\n\n========== WebSocket 事件类型 ==========\n');
        const eventTypes = [...new Set(capturedData.wsMessages.map(m => m.event))];
        for (const evt of eventTypes) {
            const count = capturedData.wsMessages.filter(m => m.event === evt).length;
            console.log(`• ${evt}: ${count} 条`);
        }
    }

    await browser.close();
    console.log('\n✅ 完成');
}

interceptRadarWebSocket().catch(console.error);
