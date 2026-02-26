/**
 * 长时间监听 GMGN 雷达页面 WebSocket
 *
 * 等待实时交易数据推送
 */

import { chromium } from 'playwright';
import fs from 'fs';

const RADAR_URL = 'https://gmgn.ai/trade/ZAxgSuiP?chain=sol';
const LISTEN_DURATION = 60000; // 监听 60 秒

async function longListenRadar() {
    console.log('\n========== 长时间监听雷达 WebSocket ==========\n');
    console.log(`监听时长: ${LISTEN_DURATION / 1000} 秒\n`);

    const sessionPath = './config/gmgn_session.json';
    let storageState = null;

    if (fs.existsSync(sessionPath)) {
        try {
            storageState = JSON.parse(fs.readFileSync(sessionPath, 'utf8'));
            console.log('✅ 加载登录状态');
        } catch (e) {
            console.log('⚠️ 无法加载 session');
        }
    }

    const browser = await chromium.launch({ headless: true });
    const context = await browser.newContext({
        storageState: storageState,
        userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        viewport: { width: 1920, height: 1080 }
    });

    const page = await context.newPage();

    const allMessages = [];
    const tradeData = [];
    let messageCount = 0;

    // 监听 WebSocket
    page.on('websocket', ws => {
        console.log(`🔌 WebSocket 已连接\n`);

        ws.on('framereceived', frame => {
            messageCount++;
            const payload = frame.payload;
            if (typeof payload !== 'string') return;

            try {
                const data = JSON.parse(payload);
                const channel = data.channel;

                // 忽略心跳
                if (channel === 'heartbeat') {
                    if (messageCount % 10 === 0) {
                        process.stdout.write('💓');
                    }
                    return;
                }

                // 忽略 ack
                if (channel === 'ack') {
                    console.log(`✅ 订阅确认: ${JSON.stringify(data.data)}`);
                    return;
                }

                // 打印其他所有消息
                console.log(`\n📨 [${channel}] 消息 #${messageCount}:`);
                console.log(JSON.stringify(data, null, 2).slice(0, 1000));

                allMessages.push({
                    channel,
                    data: data.data,
                    timestamp: new Date().toISOString()
                });

                // 检查是否是交易数据
                if (channel === 'wallet_trade_data' || channel === 'trade' || channel === 'swap') {
                    console.log(`\n🎯🎯🎯 交易数据!`);
                    tradeData.push(data);
                }

                // 检查 cards 或 wallets
                if (data.data) {
                    if (Array.isArray(data.data)) {
                        for (const item of data.data) {
                            if (item.cards || item.wallets || item.token_address) {
                                console.log(`\n🎯 找到目标数据结构!`);
                                console.log(JSON.stringify(item, null, 2).slice(0, 500));
                            }
                        }
                    }
                }

            } catch (e) {
                // 非 JSON
                if (payload.length > 10) {
                    console.log(`\n📨 非JSON消息: ${payload.slice(0, 100)}`);
                }
            }
        });

        ws.on('framesent', frame => {
            const payload = frame.payload;
            if (typeof payload === 'string' && payload.length > 20) {
                try {
                    const data = JSON.parse(payload);
                    if (data.action === 'subscribe') {
                        console.log(`📤 订阅: ${data.channel}`);
                        console.log(`   数据: ${JSON.stringify(data.data || {}).slice(0, 200)}`);
                    }
                } catch (e) {
                    // ignore
                }
            }
        });
    });

    console.log(`🌐 访问: ${RADAR_URL}\n`);

    try {
        await page.goto(RADAR_URL, {
            waitUntil: 'domcontentloaded',
            timeout: 30000
        });

        console.log('✅ 页面已加载\n');
        console.log('⏳ 监听中... (心跳: 💓)\n');

        // 长时间等待
        await page.waitForTimeout(LISTEN_DURATION);

    } catch (error) {
        console.error('错误:', error.message);
    }

    // 结果
    console.log('\n\n========== 监听结果 ==========\n');
    console.log(`总消息数: ${messageCount}`);
    console.log(`非心跳消息: ${allMessages.length}`);
    console.log(`交易数据: ${tradeData.length}`);

    // 保存
    if (allMessages.length > 0) {
        const outputPath = './logs/radar-all-messages.json';
        fs.writeFileSync(outputPath, JSON.stringify({
            allMessages,
            tradeData,
            totalCount: messageCount
        }, null, 2));
        console.log(`\n💾 保存到: ${outputPath}`);
    }

    // 显示所有频道类型
    const channels = [...new Set(allMessages.map(m => m.channel))];
    if (channels.length > 0) {
        console.log('\n\n收到的频道类型:');
        for (const ch of channels) {
            const count = allMessages.filter(m => m.channel === ch).length;
            console.log(`  • ${ch}: ${count} 条`);
        }
    }

    await browser.close();
    console.log('\n✅ 完成');
}

longListenRadar().catch(console.error);
