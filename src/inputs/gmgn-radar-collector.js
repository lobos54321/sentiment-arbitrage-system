/**
 * 🚀 GMGN 雷达数据采集器 - Playwright 版本
 *
 * 功能：
 * 1. 自动捕获不同排序的钱包排行（买入最多、利润最高、买入最早、共同持仓）
 * 2. 获取命中代币、命中代币PNL、1D PNL、1D胜率
 * 3. 通过 WebSocket 获取实时数据
 */

import { chromium } from 'playwright';
import fs from 'fs';
import { EventEmitter } from 'events';

export class GMGNRadarCollector extends EventEmitter {
    constructor(config = {}) {
        super();

        this.config = {
            sessionPath: config.sessionPath || './config/gmgn_session.json',
            headless: config.headless !== false,
            pollInterval: config.pollInterval || 60000, // 1分钟
            maxWallets: config.maxWallets || 50,
            chains: config.chains || ['sol'],
            ...config
        };

        this.browser = null;
        this.context = null;
        this.page = null;
        this.isRunning = false;
        this.capturedData = {
            wallets: [],
            cards: [],
            radarTokens: [],      // 命中代币列表
            radarWallets: [],     // 雷达钱包详情（含命中代币PNL）
            apiResponses: [],
            wsMessages: []
        };

        console.log('[Radar] 采集器初始化');
    }

    async init() {
        console.log('[Radar] 启动浏览器...');

        // 加载 session
        let storageState = null;
        if (fs.existsSync(this.config.sessionPath)) {
            try {
                storageState = JSON.parse(fs.readFileSync(this.config.sessionPath, 'utf8'));
                console.log('[Radar] ✅ 加载登录状态');
            } catch (e) {
                console.log('[Radar] ⚠️ 无法加载 session');
            }
        }

        this.browser = await chromium.launch({
            headless: this.config.headless,
            args: [
                '--disable-blink-features=AutomationControlled',
                '--disable-infobars',
                '--window-size=1920,1080'
            ]
        });

        this.context = await this.browser.newContext({
            storageState: storageState,
            userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport: { width: 1920, height: 1080 },
            locale: 'zh-CN',
            timezoneId: 'Australia/Brisbane'
        });

        // 隐藏 webdriver 标志
        await this.context.addInitScript(() => {
            Object.defineProperty(navigator, 'webdriver', { get: () => false });
        });

        this.page = await this.context.newPage();

        // 设置网络拦截
        this.setupNetworkInterception();

        console.log('[Radar] ✅ 浏览器已启动');
    }

    setupNetworkInterception() {
        // 拦截 HTTP 响应
        this.page.on('response', async (response) => {
            const url = response.url();

            if (!url.includes('gmgn.ai')) return;

            try {
                const contentType = response.headers()['content-type'] || '';
                if (!contentType.includes('json')) return;

                const data = await response.json();
                const path = url.split('?')[0].replace('https://gmgn.ai', '');

                // 钱包排行数据
                if (data.data?.rank && path.includes('/rank/') && path.includes('/wallets/')) {
                    console.log(`[Radar] 📡 捕获钱包排行: ${data.data.rank.length} 条`);

                    this.capturedData.wallets = data.data.rank;
                    this.capturedData.apiResponses.push({
                        type: 'wallets_rank',
                        url: path,
                        data: data.data.rank,
                        timestamp: new Date().toISOString()
                    });

                    this.emit('wallets', data.data.rank);
                }

                // Cards 数据 (命中代币)
                if (data.data?.cards) {
                    console.log(`[Radar] 🎯 捕获 Cards 数据: ${data.data.cards.length} 条`);

                    this.capturedData.cards = data.data.cards;
                    this.emit('cards', data.data.cards);
                }

                // SmartMoney 钱包详情
                if (path.includes('/smartmoney/') && path.includes('/walletNew/')) {
                    console.log(`[Radar] 📊 捕获钱包详情`);
                    this.capturedData.apiResponses.push({
                        type: 'wallet_detail',
                        url: path,
                        data: data.data,
                        timestamp: new Date().toISOString()
                    });
                }

                // 🎯 雷达详情 API - 核心数据（命中代币 + 命中代币PNL）
                if (path.includes('/vas/api/v1/radar/detail')) {
                    console.log(`[Radar] 🎯 捕获雷达详情数据!`);

                    // 命中代币列表
                    if (data.data?.token) {
                        this.capturedData.radarTokens = data.data.token;
                        console.log(`[Radar]    命中代币: ${data.data.token.length} 个`);
                        this.emit('radar_tokens', data.data.token);
                    }

                    // 钱包详情（含命中代币PNL）
                    if (data.data?.wallet) {
                        this.capturedData.radarWallets = data.data.wallet;
                        console.log(`[Radar]    雷达钱包: ${data.data.wallet.length} 个`);
                        this.emit('radar_wallets', data.data.wallet);
                    }

                    this.capturedData.apiResponses.push({
                        type: 'radar_detail',
                        url: path,
                        data: data.data,
                        timestamp: new Date().toISOString()
                    });
                }

            } catch (e) {
                // 忽略解析错误
            }
        });

        // 拦截 WebSocket
        this.page.on('websocket', ws => {
            console.log(`[Radar] 🔌 WebSocket 连接`);

            ws.on('framereceived', frame => {
                if (typeof frame.payload !== 'string') return;

                try {
                    const data = JSON.parse(frame.payload);
                    const channel = data.channel;

                    // 忽略心跳
                    if (channel === 'heartbeat' || channel === 'ack') return;

                    // 检查是否包含交易数据
                    if (channel === 'wallet_trade_data' || channel === 'public_broadcast') {
                        console.log(`[Radar] 📨 WS [${channel}]`);

                        this.capturedData.wsMessages.push({
                            channel,
                            data: data.data,
                            timestamp: new Date().toISOString()
                        });

                        this.emit('ws_message', { channel, data: data.data });
                    }

                } catch (e) {
                    // 忽略
                }
            });
        });
    }

    /**
     * 收集雷达数据 - 按不同排序方式
     */
    async collectRadarData(chain = 'sol', period = '1d') {
        console.log(`\n[Radar] 📊 收集 ${chain} ${period} 雷达数据...\n`);

        const results = {
            buyMost: [],      // 买入最多
            profitHighest: [], // 利润最高
            buyEarliest: [],  // 买入最早
            commonHoldings: [] // 共同持仓
        };

        // 排序方式配置
        const sortings = [
            { key: 'buyMost', orderby: 'buy', name: '买入最多' },
            { key: 'profitHighest', orderby: 'profit', name: '利润最高' },
            // 注：买入最早和共同持仓可能需要特殊端点
        ];

        for (const sorting of sortings) {
            const url = `https://gmgn.ai/sol/radar?orderby=${sorting.orderby}`;
            console.log(`[Radar] 访问: ${sorting.name} (${url})`);

            try {
                // 清空之前的数据
                this.capturedData.wallets = [];

                await this.page.goto(url, {
                    waitUntil: 'networkidle',
                    timeout: 30000
                });

                // 等待数据加载
                await this.page.waitForTimeout(5000);

                // 获取捕获的钱包数据
                if (this.capturedData.wallets.length > 0) {
                    results[sorting.key] = this.capturedData.wallets.slice(0, this.config.maxWallets);
                    console.log(`[Radar] ✅ ${sorting.name}: ${results[sorting.key].length} 条`);
                }

            } catch (error) {
                console.error(`[Radar] ❌ ${sorting.name} 失败:`, error.message);
            }

            await new Promise(r => setTimeout(r, 2000));
        }

        return results;
    }

    /**
     * 快速收集 - 单次执行
     */
    async quickCollect(chain = 'sol') {
        if (!this.browser) {
            await this.init();
        }

        console.log(`\n[Radar] 🚀 开始快速收集 ${chain} 数据...\n`);

        // 访问雷达页面
        const radarUrl = `https://gmgn.ai/trade/ZAxgSuiP?chain=${chain}`;

        try {
            await this.page.goto(radarUrl, {
                waitUntil: 'domcontentloaded',
                timeout: 30000
            });

            console.log('[Radar] ✅ 页面加载中...');

            // 等待数据加载
            await this.page.waitForTimeout(15000);

            // 尝试切换不同排序
            const sortButtons = ['买入最多', '利润最高', '买入最早', '共同持仓'];

            for (const buttonText of sortButtons) {
                try {
                    const button = this.page.locator(`text=${buttonText}`).first();
                    if (await button.isVisible()) {
                        console.log(`[Radar] 点击: ${buttonText}`);
                        await button.click();
                        await this.page.waitForTimeout(3000);
                    }
                } catch (e) {
                    // 按钮可能不存在
                }
            }

            // 返回收集的数据
            return {
                wallets: this.capturedData.wallets,
                cards: this.capturedData.cards,
                radarTokens: this.capturedData.radarTokens,
                radarWallets: this.capturedData.radarWallets,
                apiResponses: this.capturedData.apiResponses,
                wsMessages: this.capturedData.wsMessages
            };

        } catch (error) {
            console.error('[Radar] ❌ 收集失败:', error.message);
            return null;
        }
    }

    /**
     * 格式化钱包数据
     */
    formatWalletData(wallets) {
        return wallets.map(w => ({
            // 基本信息
            address: w.wallet_address,
            name: w.twitter_name || w.name || w.wallet_address?.slice(0, 12),
            avatar: w.avatar,
            tags: w.tags || [],

            // 1D 数据
            pnl_1d: parseFloat(w.realized_profit_1d || 0),
            winrate_1d: (w.winrate_1d || 0) * 100,
            buys_1d: w.buy_1d || 0,
            sells_1d: w.sell_1d || 0,
            txs_1d: w.txs_1d || 0,

            // 7D 数据
            pnl_7d: parseFloat(w.realized_profit_7d || 0),
            winrate_7d: (w.winrate_7d || 0) * 100,

            // 30D 数据
            pnl_30d: parseFloat(w.realized_profit_30d || 0),
            winrate_30d: (w.winrate_30d || 0) * 100,

            // 其他
            balance: parseFloat(w.sol_balance || w.balance || 0),
            volume_1d: parseFloat(w.volume_1d || 0),
            avg_holding_period_1d: w.avg_holding_period_1d || 0,
            daily_profit_7d: w.daily_profit_7d || [],

            // 原始数据
            raw: w
        }));
    }

    /**
     * 启动持续监控
     */
    async startMonitoring() {
        if (this.isRunning) {
            console.log('[Radar] 已在运行中');
            return;
        }

        this.isRunning = true;

        if (!this.browser) {
            await this.init();
        }

        console.log('[Radar] 🚀 启动持续监控...');

        // 定时轮询
        const poll = async () => {
            if (!this.isRunning) return;

            try {
                for (const chain of this.config.chains) {
                    const data = await this.quickCollect(chain);
                    if (data?.wallets?.length > 0) {
                        const formatted = this.formatWalletData(data.wallets);
                        this.emit('data', { chain, wallets: formatted, raw: data });
                    }
                }
            } catch (error) {
                console.error('[Radar] 轮询错误:', error.message);
            }

            // 下一轮
            if (this.isRunning) {
                setTimeout(poll, this.config.pollInterval);
            }
        };

        // 立即执行第一次
        await poll();
    }

    async stop() {
        this.isRunning = false;

        if (this.browser) {
            await this.browser.close();
            this.browser = null;
            this.context = null;
            this.page = null;
        }

        console.log('[Radar] ⏹️ 已停止');
    }

    /**
     * 保存数据
     */
    saveData(filename = './logs/radar-data.json') {
        fs.mkdirSync('./logs', { recursive: true });
        fs.writeFileSync(filename, JSON.stringify({
            wallets: this.capturedData.wallets,
            cards: this.capturedData.cards,
            radarTokens: this.capturedData.radarTokens,
            radarWallets: this.capturedData.radarWallets,
            apiResponses: this.capturedData.apiResponses,
            wsMessages: this.capturedData.wsMessages,
            savedAt: new Date().toISOString()
        }, null, 2));
        console.log(`[Radar] 💾 数据已保存到: ${filename}`);
    }

    /**
     * 格式化雷达钱包数据（包含命中代币）
     */
    formatRadarWalletData(wallets, tokens = []) {
        const tokenMap = {};
        for (const t of tokens) {
            tokenMap[t.address] = t;
        }

        return wallets.map(w => ({
            // 基本信息
            address: w.address,
            name: w.twitter_name || w.name || w.address?.slice(0, 12),
            avatar: w.avatar,
            tags: w.tags || [],

            // 命中代币数据
            matched_count: w.matched_count || 0,
            matched_tokens: (w.matched_tokens || []).map(addr => ({
                address: addr,
                symbol: tokenMap[addr]?.symbol || addr.slice(0, 8),
                name: tokenMap[addr]?.name || '',
                logo: tokenMap[addr]?.logo || '',
                market_cap: parseFloat(tokenMap[addr]?.market_cap || 0)
            })),
            matched_profit: w.matched_profit || 0,          // 命中代币 PNL
            matched_profit_pnl: w.matched_profit_pnl || 0,  // 命中代币收益率

            // 综合数据
            realized_profit: w.realized_profit || 0,         // 已实现利润
            realized_profit_pnl: w.realized_profit_pnl || 0, // 已实现收益率
            winrate: (w.winrate || 0) * 100,                 // 胜率
            balance: parseFloat(w.balance || 0),
            total_buy: w.total_buy || 0,
            total_sell: w.total_sell || 0,
            avg_holding_period: w.avg_holding_period || 0,

            // 原始数据
            raw: w
        }));
    }
}

// 如果直接运行
const isMainModule = import.meta.url === `file://${process.argv[1]}`;
if (isMainModule) {
    const collector = new GMGNRadarCollector({
        headless: false,  // 设为 true 在生产环境
        pollInterval: 60000
    });

    // 监听钱包排行事件
    collector.on('wallets', (wallets) => {
        console.log(`\n📊 收到钱包排行: ${wallets.length} 条`);

        const formatted = collector.formatWalletData(wallets.slice(0, 5));
        for (const w of formatted) {
            console.log(`\n👤 ${w.name}`);
            console.log(`   1D PNL: $${w.pnl_1d.toFixed(2)} | 胜率: ${w.winrate_1d.toFixed(1)}%`);
            console.log(`   买入: ${w.buys_1d} | 卖出: ${w.sells_1d}`);
        }
    });

    // 监听命中代币事件
    collector.on('radar_tokens', (tokens) => {
        console.log(`\n🎯 收到命中代币: ${tokens.length} 个`);
        for (const t of tokens.slice(0, 5)) {
            const mcap = (parseFloat(t.market_cap || 0) / 1e6).toFixed(2);
            console.log(`   • ${t.symbol} | 市值: $${mcap}M`);
        }
    });

    // 监听雷达钱包事件（包含命中代币PNL）
    collector.on('radar_wallets', (wallets) => {
        console.log(`\n💰 收到雷达钱包详情: ${wallets.length} 个`);

        // 获取 tokens 用于格式化
        const tokens = collector.capturedData.radarTokens || [];
        const formatted = collector.formatRadarWalletData(wallets.slice(0, 5), tokens);

        for (const w of formatted) {
            console.log(`\n👤 ${w.name || w.address.slice(0, 12)}`);
            console.log(`   命中代币数: ${w.matched_count}`);
            console.log(`   命中代币PNL: $${w.matched_profit.toFixed(2)} (${(w.matched_profit_pnl * 100).toFixed(2)}%)`);
            console.log(`   胜率: ${w.winrate.toFixed(1)}%`);
            if (w.matched_tokens.length > 0) {
                console.log(`   命中: ${w.matched_tokens.map(t => t.symbol).join(', ')}`);
            }
        }
    });

    collector.on('cards', (cards) => {
        console.log(`\n🃏 收到 Cards 数据: ${cards.length} 条`);
    });

    // 运行
    (async () => {
        await collector.init();
        const data = await collector.quickCollect('sol');

        console.log('\n\n========== 收集结果汇总 ==========\n');
        console.log(`钱包排行: ${data?.wallets?.length || 0} 条`);
        console.log(`命中代币: ${data?.radarTokens?.length || 0} 个`);
        console.log(`雷达钱包: ${data?.radarWallets?.length || 0} 个`);
        console.log(`Cards 数据: ${data?.cards?.length || 0} 条`);
        console.log(`API 响应: ${data?.apiResponses?.length || 0} 条`);
        console.log(`WS 消息: ${data?.wsMessages?.length || 0} 条`);

        // 保存数据
        collector.saveData();

        // 等待一下再关闭
        await new Promise(r => setTimeout(r, 5000));
        await collector.stop();
    })();
}

export default GMGNRadarCollector;
