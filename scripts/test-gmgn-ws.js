#!/usr/bin/env node
/**
 * Test GMGN WebSocket connection
 */

import WebSocket from 'ws';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DEVICE_ID = '1d29f750-687f-42e1-851d-59a43e5d2ffa';
const CLIENT_ID = 'gmgn_web_20260107';

async function testWebSocket() {
    // Load session
    const sessionPath = path.join(__dirname, '..', 'config', 'gmgn_session.json');
    let sessionData = null;

    if (fs.existsSync(sessionPath)) {
        sessionData = JSON.parse(fs.readFileSync(sessionPath, 'utf8'));
        console.log(`✅ Session 已加载 (${sessionData?.cookies?.length || 0} cookies)`);
    } else {
        console.log('❌ No session file found');
    }

    // Build cookies
    const cookies = sessionData?.cookies || [];
    const cookieStr = cookies
        .filter(c => c.domain && c.domain.includes('gmgn'))
        .map(c => `${c.name}=${c.value}`)
        .join('; ');

    console.log('\n=== Cookie Info ===');
    console.log('Cookie names:', cookies.map(c => c.name).join(', '));

    // Test different WebSocket URLs
    const testCases = [
        {
            name: 'Standard WS with Auth',
            url: `wss://ws.gmgn.ai/quotation?device_id=${DEVICE_ID}&client_id=${CLIENT_ID}&from_app=gmgn&app_ver=20260107&tz_name=Australia/Brisbane&tz_offset=36000&app_lang=en&os=web`,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                'Origin': 'https://gmgn.ai',
                'Cookie': cookieStr
            }
        },
        {
            name: 'WS without Cookie',
            url: `wss://ws.gmgn.ai/quotation?device_id=${DEVICE_ID}&client_id=${CLIENT_ID}&from_app=gmgn&app_ver=20260107&tz_name=Australia/Brisbane&tz_offset=36000&app_lang=en&os=web`,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                'Origin': 'https://gmgn.ai'
            }
        },
        {
            name: 'Simple WS (minimal params)',
            url: `wss://ws.gmgn.ai/quotation`,
            headers: {
                'Origin': 'https://gmgn.ai'
            }
        }
    ];

    for (const tc of testCases) {
        console.log(`\n=== Testing: ${tc.name} ===`);
        console.log(`URL: ${tc.url.substring(0, 80)}...`);

        try {
            await new Promise((resolve, reject) => {
                const ws = new WebSocket(tc.url, { headers: tc.headers });

                const timeout = setTimeout(() => {
                    ws.close();
                    reject(new Error('Timeout'));
                }, 10000);

                ws.on('open', () => {
                    clearTimeout(timeout);
                    console.log('✅ Connected!');

                    // Try to subscribe to something
                    const subscribeMsg = {
                        action: 'subscribe',
                        channel: 'wallet_trade_data',
                        params: {
                            wallet_address: 'BU72eMt3xw3MXqjxfAPHhkAu9xD7bvAxSgsrJCZspump'
                        }
                    };
                    ws.send(JSON.stringify(subscribeMsg));
                    console.log('📤 Sent subscription request');

                    // Wait for response
                    setTimeout(() => {
                        ws.close();
                        resolve();
                    }, 5000);
                });

                ws.on('message', (data) => {
                    console.log('📥 Message:', data.toString().substring(0, 200));
                });

                ws.on('error', (error) => {
                    clearTimeout(timeout);
                    console.log('❌ Error:', error.message);
                    reject(error);
                });

                ws.on('close', () => {
                    clearTimeout(timeout);
                    console.log('🔌 Closed');
                });
            });
        } catch (e) {
            console.log(`Failed: ${e.message}`);
        }
    }
}

testWebSocket().catch(console.error);
