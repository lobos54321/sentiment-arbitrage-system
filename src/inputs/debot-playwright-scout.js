/**
 * DeBot Playwright Scout - 聪明钱追踪
 * 
 * 通过 Playwright 访问 DeBot 页面，拦截 API 数据
 * 获取聪明钱买入/卖出信号
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

export class DebotPlaywrightScout extends EventEmitter {
    constructor(config = {}) {
        super();

        this.config = {
            sessionPath: config.sessionPath || path.join(__dirname, '../../config/debot_session.json'),
            chains: config.chains || ['sol', 'bsc'],
            headless: config.headless !== false,
            userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        };

        this.browser = null;
        this.context = null;
        this.page = null;
        this.isRunning = false;
        this.refreshTimer = null;
        this.lastSeenTokens = new Map();
        this.cardState = new Map();
        this.lastSessionCheck = 0; // 🔥 记录上次检查时间

        // 🔥 v6.7 主动预防崩溃
        this.browserStartTime = null;  // 浏览器启动时间
        this.proactiveRestartHours = 12; // 每12小时主动重启浏览器
        this.proactiveRestartTimer = null;
        this.consecutiveErrors = 0;

        // 🔥 v6.7 每日重新登录提醒
        this.sessionRefreshHours = 24;  // 每24小时提醒重新登录
        this.lastLoginReminder = 0;
        this.loginReminderTimer = null;

        console.log('[DeBot Scout] Playwright 模式初始化');
        console.log(`[DeBot Scout] 🛡️ 主动重启浏览器: 每 ${this.proactiveRestartHours} 小时`);
        console.log(`[DeBot Scout] 🔑 重新登录提醒: 每 ${this.sessionRefreshHours} 小时`);
    }

    /**
     * 🔥 v6.7 检查 Session 文件年龄
     */
    getSessionAge() {
        try {
            const stats = fs.statSync(this.config.sessionPath);
            const ageMs = Date.now() - stats.mtimeMs;
            return {
                hours: Math.floor(ageMs / 3600000),
                days: Math.floor(ageMs / 86400000),
                isOld: ageMs > 48 * 3600000  // 超过48小时算老
            };
        } catch (e) {
            return { hours: 0, days: 0, isOld: false };
        }
    }

    /**
     * 🔥 v6.7 启动主动重启定时器
     */
    startProactiveRestartTimer() {
        if (this.proactiveRestartTimer) {
            clearInterval(this.proactiveRestartTimer);
        }

        this.browserStartTime = Date.now();

        // 每小时检查一次是否需要重启
        this.proactiveRestartTimer = setInterval(async () => {
            const runningHours = (Date.now() - this.browserStartTime) / 3600000;

            if (runningHours >= this.proactiveRestartHours) {
                console.log(`\n🔄 [DeBot Scout] 主动重启: 已运行 ${runningHours.toFixed(1)} 小时`);

                try {
                    await this.restartBrowser();
                    this.browserStartTime = Date.now();
                    console.log('✅ [DeBot Scout] 主动重启成功\n');

                    // 发送通知
                    this.emit('proactive-restart', {
                        at: new Date().toLocaleString(),
                        runningHours: runningHours.toFixed(1)
                    });
                } catch (e) {
                    console.error('❌ [DeBot Scout] 主动重启失败:', e.message);
                }
            }
        }, 3600000); // 每小时检查
    }

    /**
     * 🔥 v6.7 启动每日登录提醒定时器
     */
    startLoginReminderTimer() {
        if (this.loginReminderTimer) {
            clearInterval(this.loginReminderTimer);
        }

        this.lastLoginReminder = Date.now();

        // 每小时检查一次 Session 年龄
        this.loginReminderTimer = setInterval(() => {
            const sessionAge = this.getSessionAge();
            const hoursSinceReminder = (Date.now() - this.lastLoginReminder) / 3600000;

            // 每24小时提醒一次，或者 Session 超过24小时就提醒
            if (sessionAge.hours >= 24 && hoursSinceReminder >= this.sessionRefreshHours) {
                console.log('\n' + '🔑'.repeat(20));
                console.log(`🔑  [提醒] Session 已使用 ${sessionAge.days}天${sessionAge.hours % 24}小时`);
                console.log('🔑  建议重新登录以保持稳定: node scripts/debot-login-setup.js');
                console.log('🔑'.repeat(20) + '\n');

                this.lastLoginReminder = Date.now();

                // 发送事件通知
                this.emit('relogin-reminder', {
                    at: new Date().toLocaleString(),
                    sessionAge: sessionAge
                });
            }
        }, 3600000); // 每小时检查
    }

    /**
     * 检查 Session 是否存在
     */
    hasSession() {
        return fs.existsSync(this.config.sessionPath);
    }

    /**
     * 🔥 v7.5 安全的 page.evaluate 包装方法
     * 处理 "Execution context was destroyed" 错误
     * @param {Function} fn - 要执行的函数
     * @param {any} arg - 传递给函数的参数
     * @param {number} maxRetries - 最大重试次数
     * @returns {Promise<any>} 执行结果
     */
    async safeEvaluate(fn, arg, maxRetries = 3) {
        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                // 确保页面处于稳定状态
                if (this.page && !this.page.isClosed()) {
                    const result = await this.page.evaluate(fn, arg);
                    return result;
                }
                return null;
            } catch (error) {
                const isContextDestroyed = error.message.includes('Execution context was destroyed') ||
                                          error.message.includes('Target closed') ||
                                          error.message.includes('Session closed');

                if (isContextDestroyed && attempt < maxRetries) {
                    // 等待页面稳定后重试
                    console.log(`[DeBot Scout] ⚠️ 执行上下文丢失，重试 ${attempt}/${maxRetries}...`);
                    await new Promise(r => setTimeout(r, 1000 * attempt));

                    // 尝试等待页面加载完成
                    try {
                        await this.page.waitForLoadState('domcontentloaded', { timeout: 5000 });
                    } catch (e) {
                        // 忽略超时错误
                    }
                    continue;
                }

                // 非上下文错误或已达到最大重试次数
                if (!isContextDestroyed) {
                    throw error;
                }
                return null;
            }
        }
        return null;
    }

    /**
     * 启动 Scout
     */
    async start() {
        if (this.isRunning) {
            console.log('[DeBot Scout] 已经在运行中');
            return;
        }

        // 检查 Session
        if (!this.hasSession()) {
            console.error('[DeBot Scout] ❌ 未找到登录 Session!');
            console.error('[DeBot Scout] 请先运行: node scripts/debot-login-setup.js');
            return;
        }

        // 🔥 v6.7 检查 Session 年龄
        const sessionAge = this.getSessionAge();
        console.log(`[DeBot Scout] 📅 Session 年龄: ${sessionAge.days}天${sessionAge.hours % 24}小时`);
        if (sessionAge.isOld) {
            console.warn('[DeBot Scout] ⚠️ Session 已超过 48 小时，建议重新登录!');
            console.warn('[DeBot Scout] 运行: node scripts/debot-login-setup.js');
            this.emit('session-old', {
                at: new Date().toLocaleString(),
                age: sessionAge
            });
        }

        console.log('[DeBot Scout] 🚀 启动中...');

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

            // 访问 DeBot 聪明钱页面
            console.log('[DeBot Scout] 正在加载 DeBot SOL 页面...');
            await this.page.goto('https://debot.ai/?chain=solana', {
                waitUntil: 'load',
                timeout: 60000
            });

            // 等待页面加载
            await this.page.waitForTimeout(5000);

            console.log('[DeBot Scout] ✅ 页面加载完成');

            // 设置定时刷新
            this.isRunning = true;
            this.scheduleRefresh();

            // 🔥 v6.7 启动主动重启定时器
            this.startProactiveRestartTimer();

            // 🔥 v6.7 启动每日登录提醒定时器
            this.startLoginReminderTimer();

            console.log('[DeBot Scout] ✅ 聪明钱监控已启动');
            console.log(`[DeBot Scout] 🛡️ 将在 ${this.proactiveRestartHours} 小时后主动重启浏览器`);
            console.log(`[DeBot Scout] 🔑 将在 ${this.sessionRefreshHours} 小时后提醒重新登录`);

        } catch (error) {
            console.error('[DeBot Scout] ❌ 启动失败:', error.message);
            await this.stop();
        }
    }

    /**
     * 设置网络请求拦截器
     */
    setupNetworkInterceptor() {
        this.page.on('response', async (response) => {
            const url = response.url();

            // 跳过非 API 请求
            if (url.includes('.js') || url.includes('.css') || url.includes('.png') ||
                url.includes('.svg') || url.includes('.woff') || url.includes('google') ||
                url.includes('cdn-cgi') || url.includes('cloudflare')) {
                return;
            }

            try {
                const contentType = response.headers()['content-type'] || '';
                if (!contentType.includes('json')) return;

                const data = await response.json();
                const shortUrl = url.split('?')[0].split('/').slice(-2).join('/');

                // 跳过无用的 API
                if (url.includes('debot/wallets') || url.includes('debot/connect') ||
                    url.includes('notification') || url.includes('unread') ||
                    url.includes('user/info') || url.includes('config/list')) {
                    return;
                }

                // 信号/榜单 API - 这是核心数据！
                if (url.includes('signal') || url.includes('rank') || url.includes('list')) {
                    const endpoint = url.includes('activity/rank') ? 'activity/rank' :
                        url.includes('channel/heatmap') ? 'channel/heatmap' :
                            url.includes('channel/list') ? 'channel/list' :
                                shortUrl;

                    console.log(`[DeBot Scout] 📡 信号API: ${endpoint}`);
                    await this.handleSignalData(url, data);
                    return;
                }

                // 打印其他 API 用于调试
                if (data?.data) {
                    const sample = JSON.stringify(data.data).slice(0, 200);
                    console.log(`[DeBot Scout] 📡 ${shortUrl}: ${sample}...`);
                }

            } catch (error) {
                // 忽略解析错误
            }
        });
    }

    /**
     * 处理信号数据 (AI信号卡片)
     * 
     * DeBot API 返回格式:
     * 
     * 1. Heatmap API (信号统计):
     * {
     *   data: {
     *     meta: {
     *       signals: { "代币地址": { signal_count, max_price_gain, token_level } }
     *     }
     *   }
     * }
     * 
     * 2. Rank API (热门代币详情) - 最丰富的数据!
     * {
     *   data: [
     *     {
     *       address, symbol, name, logo,
     *       market_info: { price, holders, mkt_cap, volume, buys, sells },
     *       pair_summary_info: { liquidity },
     *       smart_wallet_online_count, smart_wallet_total_count,
     *       max_price_gain, token_tier, activity_score
     *     }
     *   ]
     * }
     */
    async handleSignalData(url, data) {
        // 1. Heatmap API (包含 meta.signals)
        if (data?.data?.meta?.signals) {
            await this.handleHeatmapSignals(data.data.meta.signals);
            return;
        }

        // 2. Rank API (activity/rank) - 包含最丰富的代币数据
        if (url.includes('activity/rank') && data?.data && Array.isArray(data.data)) {
            await this.handleRankData(data.data);
            return;
        }

        // 3. 其他格式的信号数据 (channel/list 等)
        let items = [];

        if (data?.data?.list) items = data.data.list;
        else if (data?.data?.items) items = data.data.items;
        else if (data?.data && Array.isArray(data.data)) items = data.data;
        else if (data?.list) items = data.list;
        else if (Array.isArray(data)) items = data;

        if (items.length === 0) {
            return;
        }

        const first = items[0];
        const firstAddr = first?.token_address || first?.tokenAddress || first?.address || first?.mint || first?.contract || first?.token || first?.ca;
        const firstKeys = first ? Object.keys(first).slice(0, 20) : [];

        console.log(`[DeBot Scout] 📊 获取到 ${items.length} 个 List 信号代币`);
        if (first) {
            console.log(`[DeBot Scout] 🧾 List样例: token=${firstAddr ? firstAddr.slice(0, 12) + '...' : 'N/A'} keys=[${firstKeys.join(', ')}]`);
        }

        for (const item of items) {
            await this.processSignalItem(item);
        }
    }

    /**
     * 处理 Rank API 数据 (最丰富的代币信息)
     */
    async handleRankData(tokens) {
        if (!tokens || tokens.length === 0) return;

        const first = tokens[0];
        const firstKeys = first ? Object.keys(first).slice(0, 20) : [];

        console.log(`[DeBot Scout] 📊 Rank API: ${tokens.length} 个热门代币`);
        if (first) {
            console.log(`[DeBot Scout] 🧾 Rank样例: symbol=${first.symbol || 'N/A'} addr=${first.address ? first.address.slice(0, 12) + '...' : 'N/A'} keys=[${firstKeys.join(', ')}]`);
        }

        for (const token of tokens) {
            await this.processRankToken(token);
        }
    }

    /**
     * 处理单个 Rank 代币
     */
    async processRankToken(token) {
        const tokenAddress = token.address;
        if (!tokenAddress) return;

        const now = Date.now();

        // 检测链 - 使用大写以匹配数据库约束
        const chain = token.chain === 'solana' ? 'SOL' :
            token.chain === 'bsc' ? 'BSC' :
                tokenAddress.startsWith('0x') ? 'BSC' : 'SOL';
        const chainLower = chain === 'SOL' ? 'solana' : 'bsc';

        // 提取 market_info
        const marketInfo = token.market_info || {};
        const pairInfo = token.pair_summary_info || {};
        const socialInfo = token.social_info || {};

        // 第一层漏斗：检查聪明钱数量和流动性
        const smartWalletOnline = token.smart_wallet_online_count || 0;
        const liquidity = pairInfo.liquidity || 0;
        const isMintAbandoned = token.safe_info?.solana?.is_mint_abandoned === 1;

        // 并行获取额外数据
        // v6.4: 降低门槛，任何金狗都从 SM=1 开始！
        let aiReport = null;
        let tokenMetrics = null;
        let tokenKline = null;

        // v6.4 极速捕获: SM >= 1 且 流动性 >= $3K 就开始追踪
        const MIN_SM_THRESHOLD = 1;     // 从 3 降到 1
        const MIN_LIQ_THRESHOLD = 3000; // 从 10K 降到 3K

        if (smartWalletOnline >= MIN_SM_THRESHOLD && liquidity >= MIN_LIQ_THRESHOLD) {
            console.log(`⚡ [极速捕获] 早期信号 ${token.symbol} (SM:${smartWalletOnline}, Liq:$${(liquidity / 1000).toFixed(1)}K)`);
            // 并行请求 AI Report、Metrics 和 Kline
            const [aiRes, metricsRes, klineRes] = await Promise.all([
                this.fetchAIReport(tokenAddress),
                this.fetchTokenMetrics(tokenAddress, chainLower),
                this.fetchTokenKline(tokenAddress, chainLower)
            ]);
            aiReport = aiRes;
            tokenMetrics = metricsRes;
            tokenKline = klineRes;
        }

        // 构建信号
        const signal = {
            source: 'DeBot_Rank',
            type: 'HOT_TOKEN',
            emoji: token.token_tier === 'gold' ? '🥇' :
                token.token_tier === 'silver' ? '🥈' : '🔥',
            action: 'watch',
            chain: chain,
            token_ca: tokenAddress,
            tokenAddress: tokenAddress,
            symbol: token.symbol || 'Unknown',
            tokenName: token.name || token.symbol || 'Unknown',
            logo: token.logo || '',

            // 聪明钱数据 - Rank API 特有
            smart_wallet_online: smartWalletOnline,
            smart_wallet_total: token.smart_wallet_total_count || 0,
            smart_money_count: token.smart_wallet_total_count || 0,

            // 代币等级和分数
            tokenTier: token.token_tier || '',
            tokenLevel: token.token_tier || 'bronze',
            activityScore: token.activity_score || 0,
            maxPriceGain: token.max_price_gain || 0,

            // 市场数据（优先使用 Metrics API 数据）
            price: tokenMetrics?.price || marketInfo.price || 0,
            marketCap: tokenMetrics?.mkt_cap || marketInfo.mkt_cap || marketInfo.fdv || 0,
            holders: tokenMetrics?.holders || marketInfo.holders || 0,
            volume: tokenMetrics?.volume_24h || marketInfo.volume || 0,
            buys: marketInfo.buys || 0,
            sells: marketInfo.sells || 0,
            liquidity: tokenMetrics?.liquidity || liquidity,

            // 价格变化（优先使用 Kline API 数据）
            priceChange5m: marketInfo.percent_5m || 0,
            priceChange1h: tokenKline?.price_change_1h || marketInfo.percent_1h || 0,
            priceChange24h: tokenKline?.price_change_24h || marketInfo.percent_24h || 0,

            // Metrics API 额外数据
            buySellRatio: tokenMetrics?.buy_sell_ratio || null,
            smartMoneyFlow: tokenMetrics?.smart_money_flow || null,

            // Kline API 数据
            klineData: tokenKline?.kline || null,
            klineCount: tokenKline?.kline?.length || 0,

            // 社交信息
            twitter: socialInfo.twitter || '',
            website: socialInfo.website || '',
            description: socialInfo.description || '',

            // 安全信息
            isMintAbandoned: isMintAbandoned,

            // AI Report 数据 (如果有)
            aiReport: aiReport,
            aiScore: aiReport?.rating?.score ? parseInt(aiReport.rating.score) : null,
            aiNarrative: aiReport?.background?.origin?.text || null,
            aiNarrativeType: aiReport?.narrative_type || null,
            hasNegativeIncidents: aiReport?.distribution?.negative_incidents?.text ? true : false,

            timestamp: now,
            raw: token
        };

        // UI 对齐：同币聚合成卡片，变化才计数+1（并记录本次价格）
        const sig = [
            signal.smart_wallet_online,
            signal.smart_wallet_total,
            Number(signal.price || 0).toFixed(12),
            Math.round(Number(signal.marketCap || 0)),
            Math.round(Number(signal.liquidity || 0)),
            Math.round(Number(signal.volume || 0)),
            signal.buys,
            signal.sells
        ].join('|');
        const state = this.updateCardState(`rank:${tokenAddress}`, sig);
        if (!state.changed) return;
        signal.alertCount = state.count;

        // 打印完整信号信息 (让后台可见)
        const tierEmoji = signal.tokenTier === 'gold' ? '🥇' :
            signal.tokenTier === 'silver' ? '🥈' : '🔥';
        console.log(`\n[DeBot Scout] ═══════════════════════════════════════════════════════════════`);
        console.log(`[DeBot Scout] ${tierEmoji} HOT TOKEN #${signal.alertCount}: ${signal.symbol} (${signal.tokenName})`);
        console.log(`[DeBot Scout] ─────────────────────────────────────────────────────────────────`);
        console.log(`[DeBot Scout] 📍 地址: ${tokenAddress}`);
        console.log(`[DeBot Scout] ⛓️  链: ${chain}`);
        console.log(`[DeBot Scout] ─────────────────────────────────────────────────────────────────`);
        console.log(`[DeBot Scout] 🐋 聪明钱: ${signal.smart_wallet_online}在线 / ${signal.smart_wallet_total}总数`);
        console.log(`[DeBot Scout] 💰 市值: $${(signal.marketCap / 1000).toFixed(1)}K | 流动性: $${(signal.liquidity / 1000).toFixed(1)}K`);
        console.log(`[DeBot Scout] 💵 价格: $${signal.price}`);
        console.log(`[DeBot Scout] 📈 涨跌: 5m ${(signal.priceChange5m * 100).toFixed(1)}% | 1h ${(signal.priceChange1h * 100).toFixed(1)}% | 24h ${(signal.priceChange24h * 100).toFixed(1)}%`);
        console.log(`[DeBot Scout] 📊 24h交易量: $${(signal.volume / 1000).toFixed(1)}K`);
        console.log(`[DeBot Scout] 👥 持有人: ${signal.holders} | 买/卖: ${signal.buys}/${signal.sells}`);
        console.log(`[DeBot Scout] 🏷️  等级: ${signal.tokenTier || 'bronze'} | 活跃分: ${(signal.activityScore * 100).toFixed(0)}%`);
        console.log(`[DeBot Scout] 📈 最大涨幅: ${signal.maxPriceGain.toFixed(1)}x`);
        console.log(`[DeBot Scout] 🔒 Mint权限: ${signal.isMintAbandoned ? '已丢弃✅' : '未丢弃⚠️'}`);

        // Metrics API 额外数据
        if (signal.buySellRatio !== null) {
            console.log(`[DeBot Scout] ⚖️  买卖比: ${signal.buySellRatio.toFixed(2)}`);
        }
        if (signal.smartMoneyFlow !== null) {
            const flowEmoji = signal.smartMoneyFlow > 0 ? '🟢流入' : signal.smartMoneyFlow < 0 ? '🔴流出' : '⚪持平';
            console.log(`[DeBot Scout] 💹 聪明钱流向: ${flowEmoji} $${Math.abs(signal.smartMoneyFlow).toFixed(0)}`);
        }

        // Kline 数据
        if (signal.klineCount > 0) {
            console.log(`[DeBot Scout] 📉 K线数据: ${signal.klineCount}条`);
        }

        // 社交信息
        if (signal.twitter) console.log(`[DeBot Scout] 🐦 Twitter: ${signal.twitter}`);
        if (signal.website) console.log(`[DeBot Scout] 🌐 Website: ${signal.website}`);
        if (signal.description) console.log(`[DeBot Scout] 📝 描述: ${signal.description.slice(0, 100)}...`);

        // AI Report
        if (signal.aiScore) {
            console.log(`[DeBot Scout] ─────────────────────────────────────────────────────────────────`);
            console.log(`[DeBot Scout] 🤖 AI叙事报告:`);
            console.log(`[DeBot Scout]    评分: ${signal.aiScore}/10`);
            console.log(`[DeBot Scout]    类型: ${signal.aiNarrativeType || 'Unknown'}`);
            if (signal.aiNarrative) console.log(`[DeBot Scout]    叙事: ${signal.aiNarrative.slice(0, 100)}...`);
            if (signal.hasNegativeIncidents) console.log(`[DeBot Scout]    ⚠️ 警告: 存在负面事件`);
        }
        console.log(`[DeBot Scout] ═══════════════════════════════════════════════════════════════\n`);

        // 发送信号
        this.emit('signal', signal);
    }

    /**
     * 获取 AI Report (叙事分析)
     * API: GET https://debot.ai/api/v1/nitter/story/latest?ca_address={TOKEN_ADDRESS}
     */
    async fetchAIReport(tokenAddress) {
        try {
            const url = `https://debot.ai/api/v1/nitter/story/latest?ca_address=${tokenAddress}`;

            // 使用 Playwright page 发起请求 (复用 session cookies) - v7.5 使用 safeEvaluate
            const response = await this.safeEvaluate(async (url) => {
                try {
                    const res = await fetch(url, {
                        method: 'GET',
                        headers: {
                            'Accept': 'application/json',
                        },
                        credentials: 'include'
                    });
                    if (!res.ok) return null;
                    return await res.json();
                } catch (e) {
                    return null;
                }
            }, url);

            if (response?.success && response?.data?.history?.story) {
                const story = response.data.history.story;
                console.log(`[DeBot Scout] 📖 AI Report: ${story.project_name}, 评分: ${story.rating?.score || 'N/A'}`);
                return story;
            }

            return null;
        } catch (error) {
            console.log(`[DeBot Scout] ⚠️ AI Report 获取失败: ${error.message}`);
            return null;
        }
    }

    /**
     * 获取代币详细指标
     * API: GET https://debot.ai/api/community/signal/token/metrics?chain={CHAIN}&token={TOKEN_ADDRESS}
     * 
     * 返回数据包含：
     * - price: 当前价格
     * - holders: 持有人数
     * - mkt_cap: 市值
     * - volume_24h: 24小时交易量
     * - liquidity: 流动性
     * - buy_sell_ratio: 买卖比
     * - smart_money_flow: 聪明钱流向
     */
    async fetchTokenMetrics(tokenAddress, chain = 'solana') {
        try {
            // 正确的 API 路径
            const requestId = crypto.randomUUID ? crypto.randomUUID() : `${Date.now()}-${Math.random().toString(36).slice(2)}`;
            const url = `https://debot.ai/api/dashboard/token/market/metrics?request_id=${requestId}&chain=${chain}&token=${tokenAddress}`;

            // v7.5 使用 safeEvaluate 防止上下文崩溃
            const response = await this.safeEvaluate(async (url) => {
                try {
                    const res = await fetch(url, {
                        method: 'GET',
                        headers: { 'Accept': 'application/json' },
                        credentials: 'include'
                    });
                    if (!res.ok) {
                        return { error: `HTTP ${res.status}` };
                    }
                    return await res.json();
                } catch (e) {
                    return { error: e.message };
                }
            }, url);

            if (response?.error) {
                console.log(`[DeBot Scout] ⚠️ Token Metrics 错误: ${response.error}`);
                return null;
            }

            if (response?.code === 0 && response?.data) {
                const liq = response.data.liquidity || 0;
                console.log(`[DeBot Scout] 📊 Token Metrics: ${tokenAddress.slice(0, 8)}... 流动性=$${(liq / 1000).toFixed(1)}K`);
                return response.data;
            }

            console.log(`[DeBot Scout] ⚠️ Token Metrics 响应异常: code=${response?.code}`);
            return null;
        } catch (error) {
            console.log(`[DeBot Scout] ⚠️ Token Metrics 获取失败: ${error.message}`);
            return null;
        }
    }

    /**
     * 获取代币K线价格历史
     * API: GET https://debot.ai/api/community/signal/channel/token/kline?chain={CHAIN}&token={TOKEN_ADDRESS}
     * 
     * 返回数据包含：
     * - kline: K线数据数组 [{time, open, high, low, close, volume}]
     * - price_change_1h: 1小时涨跌幅
     * - price_change_24h: 24小时涨跌幅
     */
    async fetchTokenKline(tokenAddress, chain = 'solana') {
        try {
            const url = `https://debot.ai/api/community/signal/channel/token/kline?chain=${chain}&token=${tokenAddress}`;

            // v7.5 使用 safeEvaluate 防止上下文崩溃
            const response = await this.safeEvaluate(async (url) => {
                try {
                    const res = await fetch(url, {
                        method: 'GET',
                        headers: { 'Accept': 'application/json' },
                        credentials: 'include'
                    });
                    if (!res.ok) return null;
                    return await res.json();
                } catch (e) {
                    return null;
                }
            }, url);

            if (response?.code === 0 && response?.data) {
                const klineCount = response.data.kline?.length || 0;
                console.log(`[DeBot Scout] 📈 Token Kline: ${tokenAddress.slice(0, 8)}... (${klineCount} 条K线)`);
                return response.data;
            }

            return null;
        } catch (error) {
            console.log(`[DeBot Scout] ⚠️ Token Kline 获取失败: ${error.message}`);
            return null;
        }
    }

    /**
     * 处理 heatmap API 的 signals 数据
     */
    async handleHeatmapSignals(signals) {
        const tokenAddresses = Object.keys(signals);
        if (tokenAddresses.length === 0) return;

        console.log(`[DeBot Scout] 📊 获取到 ${tokenAddresses.length} 个 Heatmap AI 信号代币`);
        const sampleAddr = tokenAddresses[0];
        if (sampleAddr) {
            const sampleKeys = Object.keys(signals[sampleAddr] || {}).slice(0, 20);
            console.log(`[DeBot Scout] 🧾 Heatmap样例: token=${sampleAddr.slice(0, 12)}... keys=[${sampleKeys.join(', ')}]`);
        }

        // 按 signal_count 或 max_price_gain 排序
        const sortedTokens = tokenAddresses
            .map(addr => ({ address: addr, ...signals[addr] }))
            .sort((a, b) => (b.signal_count || 0) - (a.signal_count || 0))
            .slice(0, 20);

        for (const token of sortedTokens) {
            await this.processHeatmapSignal(token);
        }
    }

    /**
     * 处理单个 heatmap 信号
     * 
     * Heatmap API 原始数据格式:
     * {
     *   signal_count: 信号次数,
     *   first_time: 首次信号时间戳,
     *   first_price: 首次信号价格,
     *   max_price: 最高价格,
     *   max_price_gain: 最大涨幅倍数,
     *   token_level: bronze/silver/gold,
     *   signal_tags: 信号标签数组
     * }
     */
    async processHeatmapSignal(token) {
        const tokenAddress = token.address;
        if (!tokenAddress) return;

        const now = Date.now();

        // 检测链 - SOL 地址通常不以 0x 开头，使用大写
        const chain = tokenAddress.startsWith('0x') ? 'BSC' : 'SOL';
        const chainLower = chain === 'SOL' ? 'solana' : 'bsc';

        const signalCount = token.signal_count || 0;
        const maxPriceGain = token.max_price_gain || 0;

        // 并行获取额外数据（仅对高质量信号，节省费用）
        let aiReport = null;
        let tokenMetrics = null;
        let tokenKline = null;

        if (signalCount >= 3) {
            console.log(`[DeBot Scout] 🚀 高热度信号 ${tokenAddress.slice(0, 8)} (Count:${signalCount}), 启动数据获取...`);
            const results = await Promise.all([
                this.fetchAIReport(tokenAddress),
                this.fetchTokenMetrics(tokenAddress, chainLower),
                this.fetchTokenKline(tokenAddress, chainLower)
            ]);
            aiReport = results[0];
            tokenMetrics = results[1];
            tokenKline = results[2];
        }

        // 构建信号 - 使用 injectSignal 兼容的字段名
        const signal = {
            source: 'DeBot_AI',
            type: 'AI_SIGNAL',
            emoji: token.token_level === 'gold' ? '🥇' :
                token.token_level === 'silver' ? '🥈' : '🥉',
            action: 'buy',
            chain: chain,
            token_ca: tokenAddress,  // injectSignal 期望的字段名
            tokenAddress: tokenAddress,
            symbol: tokenAddress.slice(0, 8) + '...',
            tokenName: tokenAddress.slice(0, 8) + '...',

            // DeBot heatmap 特有数据
            signalCount: signalCount,
            smart_money_count: signalCount,  // 复用信号次数作为聪明钱数量
            firstTime: token.first_time || 0,
            firstPrice: token.first_price || 0,
            maxPrice: token.max_price || 0,
            maxPriceGain: maxPriceGain,  // 🔥 最大涨幅倍数
            tokenLevel: token.token_level || 'bronze', // bronze/silver/gold
            signalTags: token.signal_tags || [],

            // Metrics API 数据（如果获取到）
            price: tokenMetrics?.price || token.max_price || 0,
            marketCap: tokenMetrics?.mkt_cap || 0,
            holders: tokenMetrics?.holders || 0,
            volume: tokenMetrics?.volume_24h || 0,
            liquidity: tokenMetrics?.liquidity || 0,
            buySellRatio: tokenMetrics?.buy_sell_ratio || null,
            smartMoneyFlow: tokenMetrics?.smart_money_flow || null,

            // Kline API 数据
            priceChange1h: tokenKline?.price_change_1h || 0,
            priceChange24h: tokenKline?.price_change_24h || 0,
            klineData: tokenKline?.kline || null,
            klineCount: tokenKline?.kline?.length || 0,

            // AI Report 数据
            aiReport: aiReport,
            aiScore: aiReport?.rating?.score ? parseInt(aiReport.rating.score) : null,
            aiNarrative: aiReport?.background?.origin?.text || null,
            aiNarrativeType: aiReport?.narrative_type || null,
            hasNegativeIncidents: aiReport?.distribution?.negative_incidents?.text ? true : false,

            timestamp: now,
            raw: token
        };

        // UI 对齐：同币聚合成卡片，变化才计数+1（并记录本次价格）
        const sig = [
            signal.signalCount,
            Number(signal.price || 0).toFixed(12),
            Number(signal.maxPrice || 0).toFixed(12),
            Number(signal.maxPriceGain || 0).toFixed(4),
            Math.round(Number(signal.marketCap || 0)),
            Math.round(Number(signal.liquidity || 0)),
            Math.round(Number(signal.volume || 0))
        ].join('|');
        const state = this.updateCardState(`heatmap:${tokenAddress}`, sig);
        if (!state.changed) return;
        signal.alertCount = state.count;

        // 打印完整信号信息 (让后台可见)
        const levelEmoji = signal.tokenLevel === 'gold' ? '🥇' :
            signal.tokenLevel === 'silver' ? '🥈' : '🥉';
        console.log(`\n[DeBot Scout] ═══════════════════════════════════════════════════════════════`);
        console.log(`[DeBot Scout] ${levelEmoji} AI SIGNAL #${signal.alertCount}: ${tokenAddress}`);
        console.log(`[DeBot Scout] ─────────────────────────────────────────────────────────────────`);
        console.log(`[DeBot Scout] ⛓️  链: ${chain}`);
        console.log(`[DeBot Scout] 🏷️  等级: ${signal.tokenLevel}`);
        console.log(`[DeBot Scout] 📊 信号次数: ${signal.signalCount}`);
        console.log(`[DeBot Scout] 📈 最大涨幅: ${signal.maxPriceGain.toFixed(1)}x`);
        console.log(`[DeBot Scout] 💵 首次价格: $${signal.firstPrice}`);
        console.log(`[DeBot Scout] 💰 最高价格: $${signal.maxPrice}`);
        console.log(`[DeBot Scout] ⏰ 首次时间: ${signal.firstTime ? new Date(signal.firstTime * 1000).toLocaleString() : 'N/A'}`);
        if (signal.signalTags?.length > 0) {
            console.log(`[DeBot Scout] 🏷️  标签: ${signal.signalTags.join(', ')}`);
        }

        // Metrics 数据
        if (tokenMetrics) {
            console.log(`[DeBot Scout] ─────────────────────────────────────────────────────────────────`);
            console.log(`[DeBot Scout] 📊 Token Metrics (详细指标):`);
            console.log(`[DeBot Scout]    当前价格: $${signal.price}`);
            console.log(`[DeBot Scout]    市值: $${(signal.marketCap / 1000).toFixed(1)}K`);
            console.log(`[DeBot Scout]    流动性: $${(signal.liquidity / 1000).toFixed(1)}K`);
            console.log(`[DeBot Scout]    持有人: ${signal.holders}`);
            console.log(`[DeBot Scout]    24h交易量: $${(signal.volume / 1000).toFixed(1)}K`);
            if (signal.buySellRatio !== null) {
                console.log(`[DeBot Scout]    买卖比: ${signal.buySellRatio.toFixed(2)}`);
            }
            if (signal.smartMoneyFlow !== null) {
                const flowEmoji = signal.smartMoneyFlow > 0 ? '🟢流入' : signal.smartMoneyFlow < 0 ? '🔴流出' : '⚪持平';
                console.log(`[DeBot Scout]    聪明钱流向: ${flowEmoji} $${Math.abs(signal.smartMoneyFlow).toFixed(0)}`);
            }
        }

        // Kline 数据
        if (tokenKline) {
            console.log(`[DeBot Scout] ─────────────────────────────────────────────────────────────────`);
            console.log(`[DeBot Scout] 📈 Token Kline (K线数据):`);
            console.log(`[DeBot Scout]    1h涨跌: ${(signal.priceChange1h * 100).toFixed(1)}%`);
            console.log(`[DeBot Scout]    24h涨跌: ${(signal.priceChange24h * 100).toFixed(1)}%`);
            console.log(`[DeBot Scout]    K线条数: ${signal.klineCount}`);
        }

        // AI Report
        if (signal.aiScore) {
            console.log(`[DeBot Scout] ─────────────────────────────────────────────────────────────────`);
            console.log(`[DeBot Scout] 🤖 AI叙事报告:`);
            console.log(`[DeBot Scout]    评分: ${signal.aiScore}/10`);
            console.log(`[DeBot Scout]    类型: ${signal.aiNarrativeType || 'Unknown'}`);
            if (signal.aiNarrative) console.log(`[DeBot Scout]    叙事: ${signal.aiNarrative.slice(0, 100)}...`);
            if (signal.hasNegativeIncidents) console.log(`[DeBot Scout]    ⚠️ 警告: 存在负面事件`);
        }
        console.log(`[DeBot Scout] ═══════════════════════════════════════════════════════════════\n`);

        // 发送所有信号，不做过滤
        this.emit('signal', signal);
    }

    /**
     * 处理单个信号项 (channel/list 等其他 API 数据)
     */
    async processSignalItem(item) {
        // 尝试提取代币地址（不同字段名）
        const tokenAddress = item.token_address || item.tokenAddress || item.address ||
            item.mint || item.contract || item.token || item.ca;

        if (!tokenAddress) {
            // 打印数据结构以便调试
            const keys = Object.keys(item).slice(0, 10);
            console.log(`[DeBot Scout] ⚠️ 信号无代币地址, 字段: ${keys.join(', ')}`);
            return;
        }

        const now = Date.now();

        // 检测链 - 使用大写
        const chain = (item.chain || 'SOL').toUpperCase();
        const normalizedChain = chain.includes('BSC') || chain.includes('BNB') ? 'BSC' :
            chain.includes('SOL') || chain.includes('SOLANA') ? 'SOL' : chain;
        const chainLower = normalizedChain === 'SOL' ? 'solana' : 'bsc';

        // 获取所有额外数据（不过滤）
        const [aiReport, tokenMetrics, tokenKline] = await Promise.all([
            this.fetchAIReport(tokenAddress),
            this.fetchTokenMetrics(tokenAddress, chainLower),
            this.fetchTokenKline(tokenAddress, chainLower)
        ]);

        // 提取信号详情 - 使用 injectSignal 期望的字段名
        const signal = {
            source: 'DeBot_List',
            type: 'AI_SIGNAL',
            emoji: '🤖',
            action: 'buy',
            chain: normalizedChain,
            token_ca: tokenAddress,
            tokenAddress: tokenAddress,
            symbol: item.name || item.symbol || item.token_name || tokenAddress.slice(0, 8) + '...',
            tokenName: item.name || item.symbol || item.token_name || 'Unknown',

            // 原始数据
            smart_money_count: item.smart_money_count || item.smartMoneyCount || item.whale_count || 0,
            smartMoneyCount: item.smart_money_count || item.smartMoneyCount || item.whale_count || 0,
            avgBuyAmount: item.avg_buy_amount || item.avgBuyAmount || 0,
            signalCount: item.signal_count || 0,
            maxPriceGain: item.max_price_gain || item.multiplier || item.x || 0,
            tokenLevel: item.token_level || 'unknown',

            // 市场数据（优先使用 Metrics API）
            price: tokenMetrics?.price || item.price || 0,
            marketCap: tokenMetrics?.mkt_cap || item.market_cap || item.marketCap || item.mc || 0,
            holders: tokenMetrics?.holders || item.holders || item.holder_count || 0,
            volume: tokenMetrics?.volume_24h || item.volume || 0,
            liquidity: tokenMetrics?.liquidity || item.liquidity || item.pool || item.lp || 0,
            top10Percent: item.top10_percent || item.top10 || 0,

            // Metrics 额外数据
            buySellRatio: tokenMetrics?.buy_sell_ratio || null,
            smartMoneyFlow: tokenMetrics?.smart_money_flow || null,

            // Kline 数据
            priceChange1h: tokenKline?.price_change_1h || item.price_change || item.priceChange || 0,
            priceChange24h: tokenKline?.price_change_24h || 0,
            klineCount: tokenKline?.kline?.length || 0,

            // AI Report
            aiReport: aiReport,
            aiScore: aiReport?.rating?.score ? parseInt(aiReport.rating.score) : null,
            aiNarrativeType: aiReport?.narrative_type || null,

            timestamp: Date.now(),
            raw: item
        };

        // UI 对齐：同币聚合成卡片，变化才计数+1（并记录本次价格）
        const sig = [
            signal.smartMoneyCount,
            Number(signal.price || 0).toFixed(12),
            Math.round(Number(signal.marketCap || 0)),
            Math.round(Number(signal.liquidity || 0)),
            Math.round(Number(signal.volume || 0)),
            signal.signalCount,
            signal.maxPriceGain
        ].join('|');
        const state = this.updateCardState(`list:${tokenAddress}`, sig);
        if (!state.changed) return;
        signal.alertCount = state.count;

        // 打印完整信息（不过滤，显示所有数据）
        console.log(`\n[DeBot Scout] ═══════════════════════════════════════════════════════════════`);
        console.log(`[DeBot Scout] 🤖 LIST SIGNAL #${signal.alertCount}: ${signal.symbol}`);
        console.log(`[DeBot Scout] ─────────────────────────────────────────────────────────────────`);
        console.log(`[DeBot Scout] 📍 地址: ${tokenAddress}`);
        console.log(`[DeBot Scout] ⛓️  链: ${normalizedChain}`);
        console.log(`[DeBot Scout] 🏷️  等级: ${signal.tokenLevel}`);
        console.log(`[DeBot Scout] 📊 信号次数: ${signal.signalCount}`);
        console.log(`[DeBot Scout] 📈 最大涨幅: ${signal.maxPriceGain}x`);
        console.log(`[DeBot Scout] 🐋 聪明钱: ${signal.smartMoneyCount}个, 平均买入 $${signal.avgBuyAmount}`);

        // Metrics 数据
        if (tokenMetrics) {
            console.log(`[DeBot Scout] ─────────────────────────────────────────────────────────────────`);
            console.log(`[DeBot Scout] 📊 Token Metrics:`);
            console.log(`[DeBot Scout]    价格: $${signal.price}`);
            console.log(`[DeBot Scout]    市值: $${(signal.marketCap / 1000).toFixed(1)}K`);
            console.log(`[DeBot Scout]    流动性: $${(signal.liquidity / 1000).toFixed(1)}K`);
            console.log(`[DeBot Scout]    持有人: ${signal.holders}`);
            console.log(`[DeBot Scout]    24h交易量: $${(signal.volume / 1000).toFixed(1)}K`);
            if (signal.buySellRatio !== null) {
                console.log(`[DeBot Scout]    买卖比: ${signal.buySellRatio}`);
            }
            if (signal.smartMoneyFlow !== null) {
                const flowEmoji = signal.smartMoneyFlow > 0 ? '🟢流入' : signal.smartMoneyFlow < 0 ? '🔴流出' : '⚪持平';
                console.log(`[DeBot Scout]    聪明钱流向: ${flowEmoji} $${Math.abs(signal.smartMoneyFlow).toFixed(0)}`);
            }
        } else {
            console.log(`[DeBot Scout] 💰 市值: $${signal.marketCap} | 流动性: $${signal.liquidity}`);
        }

        // Kline 数据
        if (tokenKline) {
            console.log(`[DeBot Scout] ─────────────────────────────────────────────────────────────────`);
            console.log(`[DeBot Scout] 📈 Token Kline:`);
            console.log(`[DeBot Scout]    1h涨跌: ${(signal.priceChange1h * 100).toFixed(1)}%`);
            console.log(`[DeBot Scout]    24h涨跌: ${(signal.priceChange24h * 100).toFixed(1)}%`);
            console.log(`[DeBot Scout]    K线条数: ${signal.klineCount}`);
        }

        // AI Report
        if (signal.aiScore) {
            console.log(`[DeBot Scout] ─────────────────────────────────────────────────────────────────`);
            console.log(`[DeBot Scout] 🤖 AI Report:`);
            console.log(`[DeBot Scout]    评分: ${signal.aiScore}/10`);
            console.log(`[DeBot Scout]    类型: ${signal.aiNarrativeType || 'Unknown'}`);
        }

        // 打印原始数据字段（调试用）
        const rawKeys = Object.keys(item);
        console.log(`[DeBot Scout] ─────────────────────────────────────────────────────────────────`);
        console.log(`[DeBot Scout] 📦 原始数据字段: ${rawKeys.join(', ')}`);
        console.log(`[DeBot Scout] ═══════════════════════════════════════════════════════════════\n`);

        // 发送所有信号（不过滤）
        this.emit('signal', signal);
    }

    /**
     * UI 对齐：按 token 聚合成“卡片”，只在关键信息变化时计数+1
     */
    updateCardState(key, signature) {
        const prev = this.cardState.get(key);
        if (prev?.signature === signature) {
            return { changed: false, count: prev.count };
        }
        const count = (prev?.count || 0) + 1;
        this.cardState.set(key, { count, signature, lastAt: Date.now() });
        return { changed: true, count };
    }

    /**
     * 检查是否是新信号
     */
    isNewSignal(signal) {
        const cacheKey = `${signal.chain}:${signal.tokenAddress}`;
        const now = Date.now();

        if (this.lastSeenTokens.has(cacheKey)) {
            const lastSeen = this.lastSeenTokens.get(cacheKey);
            if (now - lastSeen < 10 * 60 * 1000) { // 10分钟内不重复
                return false;
            }
        }

        this.lastSeenTokens.set(cacheKey, now);
        return true;
    }

    /**
     * 定时刷新页面
     */
    scheduleRefresh() {
        if (!this.isRunning) return;

        // 30-60秒间隔
        const interval = 30000 + Math.random() * 30000;

        this.refreshTimer = setTimeout(async () => {
            if (!this.isRunning) return;

            try {
                // 轮换 SOL 和 BSC
                const pages = [
                    'https://debot.ai/?chain=solana',
                    'https://debot.ai/?chain=bsc',
                ];
                const randomPage = pages[Math.floor(Math.random() * pages.length)];
                const chain = randomPage.includes('bsc') ? 'BSC' : 'SOL';

                console.log(`[DeBot Scout] 🔄 切换到 ${chain}`);
                await this.page.goto(randomPage, {
                    waitUntil: 'load',
                    timeout: 60000
                });

                await this.page.waitForTimeout(3000);

                // 🔥 Session 状态检查 - 每 30 分钟检查一次
                const now = Date.now();
                const sessionCheckInterval = 30 * 60 * 1000; // 30 分钟
                if (now - this.lastSessionCheck >= sessionCheckInterval) {
                    await this.checkSession();
                    this.lastSessionCheck = now;
                }

            } catch (error) {
                const errorMsg = error.message.split('\n')[0];
                console.error('[DeBot Scout] 刷新错误:', errorMsg);

                // 🔥 v6.7 浏览器崩溃检测和自动恢复
                const isBrowserCrash = errorMsg.includes('browser has been closed') ||
                    errorMsg.includes('Target page') ||
                    errorMsg.includes('Target closed') ||
                    errorMsg.includes('Session closed');

                if (isBrowserCrash) {
                    this.consecutiveErrors = (this.consecutiveErrors || 0) + 1;

                    console.error('\n' + '🚨'.repeat(20));
                    console.error(`🚨  [CRITICAL] DEBOT BROWSER CRASHED! (连续失败: ${this.consecutiveErrors}次)`);
                    console.error('🚨'.repeat(20) + '\n');

                    // 发送崩溃事件
                    this.emit('browser-crashed', {
                        at: new Date().toLocaleString(),
                        consecutiveErrors: this.consecutiveErrors,
                        error: errorMsg
                    });

                    // 尝试自动重启 (最多3次)
                    if (this.consecutiveErrors <= 3) {
                        console.log(`[DeBot Scout] 🔄 尝试自动重启... (${this.consecutiveErrors}/3)`);
                        try {
                            await this.restartBrowser();
                            this.consecutiveErrors = 0;
                            console.log('[DeBot Scout] ✅ 浏览器重启成功!');
                        } catch (restartError) {
                            console.error('[DeBot Scout] ❌ 重启失败:', restartError.message);
                        }
                    } else {
                        console.error('[DeBot Scout] ❌ 连续失败3次，停止重试。请手动检查!');
                        this.isRunning = false;
                        return; // 停止调度
                    }
                } else {
                    this.consecutiveErrors = 0; // 非浏览器崩溃错误，重置计数
                }
            }

            this.scheduleRefresh();

        }, interval);
    }

    /**
     * 检查当前 Session 是否依然有效
     */
    async checkSession() {
        try {
            const content = await this.page.content();

            // 改进检测逻辑：检查是否有钱包地址（已登录的标志）
            // DeBot登录后会在页面显示钱包地址，格式如 0x...
            const hasWalletAddress = /0x[a-fA-F0-9]{4,}/.test(content);
            const hasUserProfile = content.includes('Profile') || content.includes('Wallet') || content.includes('Settings');

            // 只有当明确看到"Connect Wallet"按钮时才判断为未登录
            // 注意：页面HTML可能包含"Login"文本但仍是已登录状态
            const hasConnectWalletButton = content.includes('Connect Wallet') && !hasWalletAddress;

            if (hasConnectWalletButton) {
                console.error('⚠️ [DeBot Scout] 检测到登录超时或 Session 失效!');
                console.error('🛑 [DeBot Scout] 立即停止信号抓取，防止循环老数据');

                // 立即停止运行
                this.isRunning = false;

                // 发出session过期事件
                this.emit('session-expired', {
                    at: new Date().toLocaleString(),
                    reason: 'Connect Wallet button detected, no wallet address found'
                });

                // 关闭浏览器
                if (this.browser) {
                    await this.browser.close();
                    this.browser = null;
                    this.page = null;
                }

                console.error('🔴 [DeBot Scout] 已完全停止，请重新登录后重启系统');
            } else {
                // 每小时打印一次在线状态
                if (!this.lastStatusLog || Date.now() - this.lastStatusLog > 3600000) {
                    const indicator = hasWalletAddress ? '有钱包地址' : '有用户配置';
                    console.log(`✅ [DeBot Scout] Session 状态验证通过: 已登录 (${indicator})`);
                    this.lastStatusLog = Date.now();
                }
            }
        } catch (e) {
            console.error('[DeBot Scout] Session 检查失败:', e.message);
        }
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

        // 🔥 v6.7 清理主动重启定时器
        if (this.proactiveRestartTimer) {
            clearInterval(this.proactiveRestartTimer);
            this.proactiveRestartTimer = null;
        }

        // 🔥 v6.7 清理登录提醒定时器
        if (this.loginReminderTimer) {
            clearInterval(this.loginReminderTimer);
            this.loginReminderTimer = null;
        }

        if (this.browser) {
            await this.browser.close();
            this.browser = null;
        }

        console.log('[DeBot Scout] ⏹️ 已停止');
    }

    /**
     * 🔥 v6.7 重启浏览器 (崩溃恢复)
     */
    async restartBrowser() {
        console.log('[DeBot Scout] 🔄 正在重启浏览器...');

        // 清理旧资源
        if (this.refreshTimer) {
            clearTimeout(this.refreshTimer);
            this.refreshTimer = null;
        }

        try {
            if (this.browser) {
                await this.browser.close().catch(() => { });
            }
        } catch (e) {
            // 忽略关闭错误
        }

        this.browser = null;
        this.context = null;
        this.page = null;

        // 等待一下让资源释放
        await new Promise(r => setTimeout(r, 3000));

        // 重新启动浏览器
        const { chromium } = await import('playwright');

        this.browser = await chromium.launch({
            headless: this.config.headless,
            args: [
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-blink-features=AutomationControlled'
            ]
        });

        this.context = await this.browser.newContext({
            storageState: this.config.sessionPath,
            userAgent: this.config.userAgent,
            viewport: { width: 1920, height: 1080 }
        });

        this.page = await this.context.newPage();

        // 重新设置网络拦截
        this.setupNetworkInterceptor();

        // 访问页面
        await this.page.goto('https://debot.ai/?chain=solana', {
            waitUntil: 'load',
            timeout: 60000
        });

        await this.page.waitForTimeout(3000);

        console.log('[DeBot Scout] ✅ 浏览器重启完成');
    }

    /**
     * 获取状态
     */
    getStatus() {
        return {
            isRunning: this.isRunning,
            hasSession: this.hasSession(),
            cachedTokens: this.lastSeenTokens.size,
            cardStates: this.cardState.size
        };
    }
}

export default DebotPlaywrightScout;
