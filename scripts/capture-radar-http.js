/**
 * 捕获雷达页面初始加载的 HTTP 请求
 *
 * 重点关注 HTTP API 返回的初始数据
 */

import { chromium } from 'playwright';
import fs from 'fs';

const RADAR_URL = 'https://gmgn.ai/trade/ZAxgSuiP?chain=sol';

async function captureInitialLoad() {
    console.log('\n========== 捕获雷达页面初始 HTTP 请求 ==========\n');

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

    const capturedRequests = [];

    // 拦截所有请求
    page.on('request', request => {
        const url = request.url();
        if (url.includes('gmgn.ai') && !url.includes('.js') && !url.includes('.css') && !url.includes('.png')) {
            const method = request.method();
            const path = url.split('?')[0].replace('https://gmgn.ai', '');
            console.log(`📤 ${method} ${path}`);
        }
    });

    // 拦截所有响应
    page.on('response', async (response) => {
        const url = response.url();

        // 只关注 API 请求
        if (!url.includes('gmgn.ai/defi') && !url.includes('gmgn.ai/api')) return;

        const status = response.status();
        const path = url.split('?')[0].replace('https://gmgn.ai', '');

        console.log(`\n📥 ${status} ${path}`);

        try {
            const contentType = response.headers()['content-type'] || '';
            if (!contentType.includes('json')) return;

            const text = await response.text();
            const data = JSON.parse(text);

            capturedRequests.push({
                url: url,
                path: path,
                status: status,
                data: data,
                timestamp: new Date().toISOString()
            });

            // 分析响应数据
            if (data.data) {
                // 检查 cards
                if (data.data.cards) {
                    console.log(`   🎯🎯🎯 找到 CARDS 数据! 数量: ${data.data.cards.length}`);
                    console.log(`   完整 URL: ${url}`);

                    // 显示第一个 card
                    const card = data.data.cards[0];
                    if (card) {
                        console.log(`\n   第一个代币: ${card.symbol}`);
                        console.log(`   字段: ${Object.keys(card).join(', ')}`);

                        if (card.wallets) {
                            console.log(`   钱包数: ${card.wallets.length}`);
                            const w = card.wallets[0];
                            if (w) {
                                console.log(`   钱包字段: ${Object.keys(w).join(', ')}`);
                                console.log(`   示例: ${w.twitter_name || w.wallet_address?.slice(0, 8)} | net_inflow: ${w.net_inflow}`);
                            }
                        }
                    }
                }

                // 检查 rank (钱包排行)
                if (data.data.rank) {
                    console.log(`   🏆 钱包排行数据! 数量: ${data.data.rank.length}`);

                    const wallet = data.data.rank[0];
                    if (wallet) {
                        // 检查钱包中是否有 hit_tokens 或类似字段
                        const tokenFields = Object.keys(wallet).filter(k =>
                            k.includes('token') || k.includes('card') || k.includes('hit')
                        );
                        if (tokenFields.length > 0) {
                            console.log(`   🎯 命中代币相关字段: ${tokenFields.join(', ')}`);

                            for (const field of tokenFields) {
                                const value = wallet[field];
                                if (value) {
                                    console.log(`   ${field}: ${JSON.stringify(value).slice(0, 200)}`);
                                }
                            }
                        }

                        // 显示所有字段
                        console.log(`   钱包字段: ${Object.keys(wallet).slice(0, 20).join(', ')}`);
                    }
                }

                // 检查其他可能的数据结构
                const keys = Object.keys(data.data);
                if (!data.data.cards && !data.data.rank) {
                    console.log(`   数据字段: ${keys.slice(0, 15).join(', ')}`);

                    // 递归检查
                    for (const key of keys) {
                        const val = data.data[key];
                        if (Array.isArray(val) && val.length > 0 && typeof val[0] === 'object') {
                            console.log(`   ${key}: Array[${val.length}] 字段: ${Object.keys(val[0]).slice(0, 10).join(', ')}`);
                        }
                    }
                }
            }

        } catch (e) {
            // 忽略解析错误
        }
    });

    console.log(`🌐 访问: ${RADAR_URL}\n`);

    try {
        await page.goto(RADAR_URL, {
            waitUntil: 'networkidle',
            timeout: 60000
        });

        console.log('\n✅ 页面完全加载');

        // 额外等待一些后续请求
        await page.waitForTimeout(5000);

    } catch (error) {
        console.error('错误:', error.message);
    }

    // 保存结果
    console.log('\n\n========== 捕获结果 ==========\n');
    console.log(`总共捕获 ${capturedRequests.length} 个 API 响应\n`);

    // 列出所有 API 端点
    console.log('捕获的 API 端点:');
    for (const req of capturedRequests) {
        const hasCards = req.data?.data?.cards ? '🎯 cards' : '';
        const hasRank = req.data?.data?.rank ? '🏆 rank' : '';
        console.log(`  ${req.path} ${hasCards} ${hasRank}`);
    }

    // 保存完整数据
    if (capturedRequests.length > 0) {
        const outputPath = './logs/radar-http-requests.json';
        fs.writeFileSync(outputPath, JSON.stringify(capturedRequests, null, 2));
        console.log(`\n💾 完整数据保存到: ${outputPath}`);
    }

    await browser.close();
    console.log('\n✅ 完成');
}

captureInitialLoad().catch(console.error);
