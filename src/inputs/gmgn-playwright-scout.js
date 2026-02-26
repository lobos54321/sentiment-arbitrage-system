/**
 * GMGN Playwright Scout - 全自动数据抓取
 * 
 * 核心原理:
 * 1. 使用保存的登录态访问 GMGN
 * 2. 拦截浏览器发出的 API 请求
 * 3. 直接获取 JSON 数据，绕过 Cloudflare
 * 
 * 支持的信号:
 * - 🐋 Smart Money (聪明钱)
 * - 👑 KOL (KOL持仓)
 * - 🚀 Trending (飙升榜)
 * - 🔥 Hot (热门榜)
 */

import { chromium } from 'playwright-extra';
import stealth from 'puppeteer-extra-plugin-stealth';
import { EventEmitter } from 'events';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

// 加载 Stealth 插件
chromium.use(stealth());

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export class GMGNPlaywrightScout extends EventEmitter {
    constructor(config = {}) {
        super();
        
        this.config = {
            sessionPath: config.sessionPath || path.join(__dirname, '../../config/gmgn_session.json'),
            chains: config.chains || ['sol'],
            refreshInterval: config.refreshInterval || 15000 + Math.random() * 5000, // 15-20秒随机
            headless: config.headless !== false,
            userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        };
        
        this.browser = null;
        this.context = null;
        this.page = null;
        this.isRunning = false;
        this.refreshTimer = null;
        this.lastSeenTokens = new Map();
        
        // API 端点匹配规则
        this.apiPatterns = {
            smartMoney: /\/rank\/\w+\/swaps.*orderby=smartmoney/i,
            kol: /\/rank\/\w+\/swaps.*orderby=kol/i,
            trending: /\/rank\/\w+\/swaps/i,
            signals: /\/signal/i,
            tokenInfo: /\/tokens\/\w+\/[A-Za-z0-9]+/i
        };
        
        console.log('[GMGN Scout] Playwright 模式初始化');
    }
    
    /**
     * 检查 Session 是否存在
     */
    hasSession() {
        return fs.existsSync(this.config.sessionPath);
    }
    
    /**
     * 启动 Scout
     */
    async start() {
        if (this.isRunning) {
            console.log('[GMGN Scout] 已经在运行中');
            return;
        }
        
        // 检查 Session
        if (!this.hasSession()) {
            console.error('[GMGN Scout] ❌ 未找到登录 Session!');
            console.error('[GMGN Scout] 请先运行: node scripts/gmgn-login-setup.js');
            return;
        }
        
        console.log('[GMGN Scout] 🚀 启动中...');
        
        try {
            // 启动浏览器
            this.browser = await chromium.launch({
                headless: this.config.headless,
                args: [
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-blink-features=AutomationControlled'
                ]
            });
            
            // 加载 Session
            this.context = await this.browser.newContext({
                storageState: this.config.sessionPath,
                userAgent: this.config.userAgent,
                viewport: { width: 1920, height: 1080 }
            });
            
            this.page = await this.context.newPage();
            
            // 设置网络拦截
            this.setupNetworkInterceptor();
            
            // 访问 GMGN 战壕页面 (主页，有实时信号)
            console.log('[GMGN Scout] 正在加载 GMGN 战壕页面...');
            await this.page.goto('https://gmgn.ai/?chain=sol', {
                waitUntil: 'load',
                timeout: 60000
            });
            
            // 等待页面完全加载
            await this.page.waitForTimeout(5000);
            
            // 尝试点击"信号"按钮
            try {
                console.log('[GMGN Scout] 尝试打开信号面板...');
                const signalBtn = await this.page.$('text=信号') || await this.page.$('text=Signal');
                if (signalBtn) {
                    await signalBtn.click();
                    await this.page.waitForTimeout(2000);
                    console.log('[GMGN Scout] ✅ 信号面板已打开');
                }
            } catch (e) {
                console.log('[GMGN Scout] 信号按钮未找到，继续监听页面数据');
            }
            
            console.log('[GMGN Scout] ✅ 页面加载完成');
            console.log('[GMGN Scout] ✅ 正在监听实时信号...');
            
            // 设置定时刷新 (较长间隔，因为页面本身有实时推送)
            this.isRunning = true;
            this.scheduleRefresh();
            
            console.log('[GMGN Scout] ✅ 信号监控已启动');
            
        } catch (error) {
            console.error('[GMGN Scout] ❌ 启动失败:', error.message);
            await this.stop();
        }
    }
    
    /**
     * 设置网络请求拦截器
     */
    setupNetworkInterceptor() {
        this.page.on('response', async (response) => {
            const url = response.url();
            
            // 捕获所有 GMGN 相关请求
            if (!url.includes('gmgn')) return;
            
            // 跳过静态资源
            if (url.includes('/static/') || url.includes('.js') || 
                url.includes('.css') || url.includes('.woff') ||
                url.includes('google-analytics') || url.includes('cdn-cgi')) {
                return;
            }
            
            // 调试：打印 API 请求
            const shortUrl = url.split('?')[0].split('/').slice(-3).join('/');
            console.log(`[GMGN Scout] 📡 API: ${shortUrl}`);
            
            try {
                const contentType = response.headers()['content-type'] || '';
                if (!contentType.includes('json')) return;
                
                const data = await response.json();
                
                // 打印数据结构用于调试
                if (data?.data) {
                    const keys = Object.keys(data.data);
                    console.log(`[GMGN Scout] 📊 数据: ${keys.slice(0, 5).join(', ')}`);
                }
                
                // 处理所有可能包含代币数据的响应
                this.handleGenericData(url, data);
                
            } catch (error) {
                // 忽略解析错误
            }
        });
    }
    
    /**
     * 通用数据处理
     * 
     * GMGN trendy API 返回格式:
     * {
     *   data: {
     *     rank: [
     *       {
     *         address: "代币地址",
     *         name: "代币名",
     *         symbol: "SYMBOL",
     *         smart_degen_count: 11,    // 🐋 聪明钱数量
     *         renowned_count: 3,         // 👑 KOL 数量
     *         sniper_count: 11,          // 狙击手数量
     *         price_change_percent1h: 1426.59,
     *         market_cap: 61099.7,
     *         liquidity: 23636.2,
     *         holder_count: 465,
     *         top_10_holder_rate: 0.2244,
     *         bundler_rate: 0.3876,      // Bundler 比例
     *         is_honeypot: 0,
     *         rug_ratio: 0.007,
     *         ...
     *       }
     *     ]
     *   }
     * }
     */
    handleGenericData(url, data) {
        // 尝试从不同格式中提取代币列表
        let tokens = [];
        
        if (data?.data?.rank && Array.isArray(data.data.rank)) {
            tokens = data.data.rank;
        } else if (data?.data?.list && Array.isArray(data.data.list)) {
            tokens = data.data.list;
        } else if (data?.data && Array.isArray(data.data)) {
            tokens = data.data;
        } else if (Array.isArray(data)) {
            tokens = data;
        }
        
        if (tokens.length === 0) return;
        
        console.log(`[GMGN Scout] 📊 获取到 ${tokens.length} 个代币`);
        
        // 处理每个代币
        for (const token of tokens.slice(0, 20)) {
            // 提取 GMGN 特有的关键数据
            const smartDegenCount = token.smart_degen_count || 0;
            const renownedCount = token.renowned_count || 0;
            const priceChange1h = parseFloat(token.price_change_percent1h || token.price_change_percent || 0);
            const sniperCount = token.sniper_count || 0;
            
            // 判断信号类型和优先级
            let signalType = 'trending';
            let emoji = '📈';
            let priority = 0;
            
            // 聪明钱信号 (最高优先)
            if (smartDegenCount >= 5) {
                signalType = 'smart_money';
                emoji = '🐋🐋';
                priority = 100;
            } else if (smartDegenCount >= 2) {
                signalType = 'smart_money';
                emoji = '🐋';
                priority = 80;
            }
            // KOL 信号
            else if (renownedCount >= 3) {
                signalType = 'kol';
                emoji = '👑👑';
                priority = 70;
            } else if (renownedCount >= 1) {
                signalType = 'kol';
                emoji = '👑';
                priority = 60;
            }
            // 飙升信号
            else if (priceChange1h >= 100) {
                signalType = 'surge';
                emoji = '🚀🚀';
                priority = 50;
            } else if (priceChange1h >= 30) {
                signalType = 'surge';
                emoji = '🚀';
                priority = 40;
            }
            
            const signal = this.createSignal(token, signalType, emoji);
            if (signal && this.isNewSignal(signal)) {
                // 打印有价值的信号
                let info = [];
                if (smartDegenCount > 0) info.push(`${smartDegenCount}聪明钱`);
                if (renownedCount > 0) info.push(`${renownedCount}KOL`);
                if (priceChange1h > 10) info.push(`1h +${priceChange1h.toFixed(0)}%`);
                if (sniperCount > 5) info.push(`${sniperCount}狙击手`);
                
                console.log(`[GMGN Scout] ${emoji} ${signal.symbol} (${signal.chain}) - ${info.join(', ')}`);
                
                // 添加额外数据到信号
                signal.smart_degen_count = smartDegenCount;
                signal.renowned_count = renownedCount;
                signal.sniper_count = sniperCount;
                signal.price_change_1h = priceChange1h;
                signal.bundler_rate = token.bundler_rate || 0;
                signal.rug_ratio = token.rug_ratio || 0;
                signal.is_honeypot = token.is_honeypot || 0;
                signal.priority = priority;
                
                this.emit('signal', signal);
            }
        }
    }
    
    /**
     * 处理 KOL 数据
     */
    handleKOLData(data) {
        if (!data?.data?.rank) return;
        
        const tokens = data.data.rank.slice(0, 10);
        
        for (const token of tokens) {
            if ((token.kol_count || 0) >= 1) {
                const signal = this.createSignal(token, 'kol', '👑');
                if (signal && this.isNewSignal(signal)) {
                    console.log(`[GMGN Scout] 👑 KOL: ${signal.symbol} - ${signal.kol_count} 个KOL`);
                    this.emit('signal', signal);
                }
            }
        }
    }
    
    /**
     * 处理趋势数据
     */
    handleTrendingData(data) {
        if (!data?.data?.rank) return;
        
        const tokens = data.data.rank.slice(0, 15);
        
        for (const token of tokens) {
            // 飙升: 5分钟涨幅 > 20%
            const priceChange5m = parseFloat(token.price_change_5m || token.change_5m || 0);
            if (priceChange5m >= 20) {
                const signal = this.createSignal(token, 'surge', '🚀');
                signal.price_change_5m = priceChange5m;
                if (this.isNewSignal(signal)) {
                    console.log(`[GMGN Scout] 🚀 Surge: ${signal.symbol} - 5m +${priceChange5m.toFixed(1)}%`);
                    this.emit('signal', signal);
                }
            }
        }
    }
    
    /**
     * 处理信号数据
     */
    handleSignalData(data) {
        // 如果有专门的信号端点数据
        if (!data?.data) return;
        
        const signals = Array.isArray(data.data) ? data.data : [data.data];
        
        for (const item of signals.slice(0, 10)) {
            const signal = this.createSignal(item, 'signal', '📡');
            if (signal && this.isNewSignal(signal)) {
                console.log(`[GMGN Scout] 📡 Signal: ${signal.symbol}`);
                this.emit('signal', signal);
            }
        }
    }
    
    /**
     * 创建信号对象
     */
    createSignal(token, signalType, emoji) {
        const tokenCA = token.address || token.token_address || token.ca || token.contract;
        if (!tokenCA) return null;
        
        // 检测链 - 0x 开头是 BSC/ETH，否则是 SOL
        let chain = 'SOL';
        if (tokenCA.startsWith('0x')) {
            chain = 'BSC';
        } else if (token.chain) {
            chain = token.chain.toUpperCase();
            if (chain === 'SOLANA') chain = 'SOL';
        }
        
        return {
            token_ca: tokenCA,
            chain: chain,
            symbol: token.symbol || 'Unknown',
            name: token.name || token.symbol || 'Unknown',
            signal_type: signalType,
            emoji: emoji,
            
            // 聪明钱/KOL 数据 (截图中的 "聪明钱/KOL" 列)
            smart_money_count: token.smart_money_count || token.smartmoney || token.smart_count || 0,
            kol_count: token.kol_count || token.kol || 0,
            
            // 市值相关 (截图中的 "市值" 和 "历史最高市值")
            market_cap: token.market_cap || token.marketcap || token.mc || 0,
            ath_market_cap: token.ath_market_cap || token.ath_mc || 0,
            
            // 流动性 (截图中的 "池子")
            liquidity: token.liquidity || token.pool || token.lp || 0,
            
            // 交易数据 (截图中的 "1h成交额" 和 "1h交易数")
            volume_1h: token.volume_1h || token.swaps_1h_amount || 0,
            volume_24h: token.volume_24h || token.volume || 0,
            tx_count_1h: token.tx_count_1h || token.swaps_1h || 0,
            buy_count_1h: token.buy_count_1h || token.buys_1h || 0,
            sell_count_1h: token.sell_count_1h || token.sells_1h || 0,
            
            // 持有者 (截图中的 "持有者")
            holder_count: token.holder_count || token.holders || 0,
            
            // 价格变化
            price: token.price || 0,
            price_change_5m: token.price_change_5m || token.change_5m || 0,
            price_change_1h: token.price_change_1h || token.change_1h || 0,
            price_change_24h: token.price_change_24h || token.change_24h || 0,
            
            // 代币年龄 (截图中的 "123d", "325d")
            age_days: token.age || token.created_days || 0,
            
            source: `gmgn_playwright_${signalType}`,
            timestamp: Date.now(),
            raw: token
        };
    }
    
    /**
     * 检查是否是新信号
     */
    isNewSignal(signal) {
        const cacheKey = `${signal.chain}:${signal.token_ca}:${signal.signal_type}`;
        const now = Date.now();
        
        if (this.lastSeenTokens.has(cacheKey)) {
            const lastSeen = this.lastSeenTokens.get(cacheKey);
            if (now - lastSeen < 30 * 60 * 1000) { // 30分钟内不重复
                return false;
            }
        }
        
        this.lastSeenTokens.set(cacheKey, now);
        return true;
    }
    
    /**
     * 定时刷新页面 (轮换不同页面获取更多数据)
     */
    scheduleRefresh() {
        if (!this.isRunning) return;
        
        // 45-75秒间隔
        const interval = 45000 + Math.random() * 30000;
        
        this.refreshTimer = setTimeout(async () => {
            if (!this.isRunning) return;
            
            try {
                // 轮换不同页面 (包含热门榜)
                const pages = [
                    'https://gmgn.ai/?chain=sol',                                    // SOL 战壕
                    'https://gmgn.ai/trend/ZAxgSuiP?chain=sol',                       // SOL 热门
                    'https://gmgn.ai/trend/ZAxgSuiP?chain=sol&tab=surge',             // SOL 飙升
                    'https://gmgn.ai/trend/ZAxgSuiP?chain=sol&tab=new_pair',          // SOL 新币
                    'https://gmgn.ai/?chain=bsc',                                     // BSC 战壕
                    'https://gmgn.ai/trend/ZAxgSuiP?chain=bsc',                        // BSC 热门
                    'https://gmgn.ai/trend/ZAxgSuiP?chain=bsc&tab=surge',             // BSC 飙升
                    'https://gmgn.ai/trend/ZAxgSuiP?chain=bsc&tab=new_pair',          // BSC 新币
                ];
                const randomPage = pages[Math.floor(Math.random() * pages.length)];
                const pageName = randomPage.includes('bsc') ? 'BSC' : 'SOL';
                const pageType = randomPage.includes('surge') ? '飙升' : 
                                 randomPage.includes('new_pair') ? '新币' :
                                 randomPage.includes('trend') ? '热门' : '战壕';
                
                console.log(`[GMGN Scout] 🔄 切换到 ${pageName} ${pageType}`);
                await this.page.goto(randomPage, { 
                    waitUntil: 'load',
                    timeout: 60000
                });
                
                // 等待数据加载
                await this.page.waitForTimeout(3000);
                
            } catch (error) {
                console.error('[GMGN Scout] 刷新错误:', error.message.split('\n')[0]);
            }
            
            // 继续下一次刷新
            this.scheduleRefresh();
            
        }, interval);
    }
    
    /**
     * 停止 Scout
     */
    async stop() {
        this.isRunning = false;
        
        if (this.refreshTimer) {
            clearTimeout(this.refreshTimer);
            this.refreshTimer = null;
        }
        
        if (this.browser) {
            await this.browser.close();
            this.browser = null;
        }
        
        console.log('[GMGN Scout] ⏹️ 已停止');
    }
    
    /**
     * 获取状态
     */
    getStatus() {
        return {
            isRunning: this.isRunning,
            hasSession: this.hasSession(),
            cachedTokens: this.lastSeenTokens.size
        };
    }
}

export default GMGNPlaywrightScout;
