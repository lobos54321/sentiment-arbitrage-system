/**
 * 测试 GMGN WebSocket 连接
 *
 * 雷达页面的实时数据可能通过 WebSocket 推送
 */

import WebSocket from 'ws';
import fs from 'fs';

const DEVICE_ID = '1d29f750-687f-42e1-851d-59a43e5d2ffa';

async function testWebSocket() {
    console.log('\n========== 测试 GMGN WebSocket 连接 ==========\n');

    const sessionPath = './config/gmgn_session.json';
    const sessionData = JSON.parse(fs.readFileSync(sessionPath, 'utf8'));
    const cookies = sessionData?.cookies || [];
    const cookieStr = cookies
        .filter(c => c.domain && c.domain.includes('gmgn'))
        .map(c => `${c.name}=${c.value}`)
        .join('; ');

    // 可能的 WebSocket 端点
    const wsEndpoints = [
        'wss://gmgn.ai/defi/quotation/v1/ws',
        'wss://gmgn.ai/ws',
        'wss://gmgn.ai/socket.io/?EIO=4&transport=websocket',
        'wss://gmgn.ai/defi/ws',
        'wss://ws.gmgn.ai',
        'wss://gmgn.ai/defi/quotation/v1/smartmoney/ws',
        'wss://gmgn.ai/api/ws',
    ];

    for (const wsUrl of wsEndpoints) {
        console.log(`\n📡 测试: ${wsUrl}`);

        try {
            const ws = new WebSocket(wsUrl, {
                headers: {
                    'Cookie': cookieStr,
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                    'Origin': 'https://gmgn.ai'
                }
            });

            await new Promise((resolve) => {
                const timeout = setTimeout(() => {
                    console.log('   ⏱️ 连接超时');
                    ws.close();
                    resolve();
                }, 5000);

                ws.on('open', () => {
                    console.log('   ✅ 连接成功!');

                    // 尝试发送订阅消息
                    const subscribeMessages = [
                        { type: 'subscribe', channel: 'smartmoney', chain: 'sol' },
                        { event: 'subscribe', data: { channel: 'monitor', chain: 'sol' } },
                        { action: 'subscribe', topic: 'sol:radar' },
                        '40', // Socket.IO ping
                        JSON.stringify({ t: 'sub', c: 'sol:monitor' }),
                    ];

                    for (const msg of subscribeMessages) {
                        try {
                            ws.send(typeof msg === 'string' ? msg : JSON.stringify(msg));
                        } catch (e) {
                            // ignore
                        }
                    }
                });

                ws.on('message', (data) => {
                    const msg = data.toString().slice(0, 300);
                    console.log(`   📨 收到消息: ${msg}`);

                    // 检查是否包含 cards 或 wallets 数据
                    if (msg.includes('cards') || msg.includes('wallets') || msg.includes('net_inflow')) {
                        console.log('   🎯🎯🎯 找到目标数据!!!');
                        console.log(`   完整消息: ${data.toString().slice(0, 500)}`);
                    }
                });

                ws.on('error', (err) => {
                    console.log(`   ❌ 错误: ${err.message}`);
                    clearTimeout(timeout);
                    resolve();
                });

                ws.on('close', () => {
                    clearTimeout(timeout);
                    resolve();
                });
            });

        } catch (error) {
            console.log(`   ❌ ${error.message}`);
        }

        await new Promise(r => setTimeout(r, 500));
    }

    // 测试 HTTPS 轮询获取 WebSocket 初始数据
    console.log('\n\n========== 测试 HTTP 轮询端点 ==========\n');

    const params = new URLSearchParams({
        device_id: DEVICE_ID,
        client_id: 'gmgn_web_test',
        from_app: 'gmgn',
        app_ver: '20260101',
        tz_name: 'Australia/Brisbane',
        app_lang: 'en',
        os: 'web'
    });

    const headers = {
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Cookie': cookieStr,
        'Origin': 'https://gmgn.ai',
        'Referer': 'https://gmgn.ai/sol/radar'
    };

    // 测试一些可能的轮询端点
    const pollEndpoints = [
        `https://gmgn.ai/defi/quotation/v1/smartmoney/sol/walletNew/all?${params}`,
        `https://gmgn.ai/defi/quotation/v1/smartmoney/sol/wallets/activity?${params}`,
        `https://gmgn.ai/defi/quotation/v1/alerts/sol/tokens?${params}`,
        `https://gmgn.ai/defi/quotation/v1/notifications/sol?${params}`,
    ];

    for (const url of pollEndpoints) {
        const path = url.split('?')[0].replace('https://gmgn.ai/defi/quotation/', '');
        console.log(`📡 ${path}:`);

        try {
            const response = await fetch(url, { headers });
            if (!response.ok) {
                console.log(`   ❌ ${response.status}`);
            } else {
                const data = await response.json();
                if (data.code && data.code !== 0) {
                    console.log(`   ⚠️ code: ${data.code}`);
                } else {
                    console.log(`   ✅ 成功!`);
                    if (data.data?.cards) {
                        console.log(`   🎯 找到 cards: ${data.data.cards.length}`);
                    } else if (data.data) {
                        console.log(`   字段: ${JSON.stringify(data.data).slice(0, 200)}`);
                    }
                }
            }
        } catch (e) {
            console.log(`   ❌ ${e.message}`);
        }

        await new Promise(r => setTimeout(r, 200));
    }
}

testWebSocket().catch(console.error);
