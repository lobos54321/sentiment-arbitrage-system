/**
 * GMGN Cookie 自动刷新器
 * 
 * 使用 Playwright 无头浏览器自动获取 GMGN 的 Cloudflare Cookie
 * 每 25 分钟自动刷新一次（cf_clearance 有效期约 30 分钟）
 * 
 * 使用方法:
 * 1. 首次运行需要手动登录一次
 * 2. 之后会自动保存登录状态并定时刷新 Cookie
 */

import { chromium } from 'playwright';
import { EventEmitter } from 'events';
import fs from 'fs';
import path from 'path';

export class GMGNCookieRefresher extends EventEmitter {
    constructor(config = {}) {
        super();
        
        this.config = {
            // 刷新间隔（毫秒）- 25分钟
            refreshInterval: config.refreshInterval || 25 * 60 * 1000,
            
            // 浏览器状态保存路径
            statePath: config.statePath || './data/gmgn-browser-state.json',
            
            // GMGN 网址
            gmgnUrl: 'https://gmgn.ai/',
            
            // 是否使用无头模式
            headless: config.headless !== false,
            
            // 超时时间
            timeout: config.timeout || 60000
        };
        
        this.browser = null;
        this.context = null;
        this.page = null;
        this.cookies = null;
        this.refreshTimer = null;
        this.isRunning = false;
        
        console.log('[GMGN Cookie] 刷新器初始化');
    }
    
    /**
     * 启动 Cookie 刷新器
     */
    async start() {
        if (this.isRunning) {
            console.log('[GMGN Cookie] 已经在运行');
            return;
        }
        
        this.isRunning = true;
        console.log('[GMGN Cookie] 🚀 启动自动刷新...');
        
        try {
            // 初始化浏览器
            await this.initBrowser();
            
            // 首次获取 Cookie
            await this.refreshCookies();
            
            // 设置定时刷新
            this.refreshTimer = setInterval(async () => {
                if (!this.isRunning) return;
                
                try {
                    await this.refreshCookies();
                } catch (error) {
                    console.error('[GMGN Cookie] 刷新失败:', error.message);
                    this.emit('error', error);
                }
            }, this.config.refreshInterval);
            
            console.log(`[GMGN Cookie] ✅ 自动刷新已启动 (每 ${this.config.refreshInterval / 60000} 分钟)`);
            
        } catch (error) {
            console.error('[GMGN Cookie] 启动失败:', error.message);
            throw error;
        }
    }
    
    /**
     * 初始化浏览器
     */
    async initBrowser() {
        console.log('[GMGN Cookie] 初始化浏览器...');
        
        this.browser = await chromium.launch({
            headless: this.config.headless,
            args: [
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',
                '--disable-setuid-sandbox'
            ]
        });
        
        // 尝试加载保存的状态
        let storageState = undefined;
        if (fs.existsSync(this.config.statePath)) {
            try {
                storageState = JSON.parse(fs.readFileSync(this.config.statePath, 'utf8'));
                console.log('[GMGN Cookie] 已加载保存的登录状态');
            } catch (e) {
                console.log('[GMGN Cookie] 无法加载保存的状态，将使用新会话');
            }
        }
        
        this.context = await this.browser.newContext({
            storageState,
            userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport: { width: 1920, height: 1080 }
        });
        
        this.page = await this.context.newPage();
    }
    
    /**
     * 刷新 Cookie
     */
    async refreshCookies() {
        console.log('[GMGN Cookie] 刷新中...');
        
        try {
            // 访问 GMGN 首页
            await this.page.goto(this.config.gmgnUrl, {
                waitUntil: 'networkidle',
                timeout: this.config.timeout
            });
            
            // 等待 Cloudflare 验证通过
            await this.page.waitForTimeout(5000);
            
            // 检查是否需要登录
            const needLogin = await this.checkNeedLogin();
            if (needLogin) {
                console.log('[GMGN Cookie] ⚠️ 需要登录，请手动完成登录');
                this.emit('need_login');
                
                // 如果不是无头模式，等待用户登录
                if (!this.config.headless) {
                    console.log('[GMGN Cookie] 等待用户登录...');
                    await this.page.waitForTimeout(120000); // 等待 2 分钟
                }
            }
            
            // 获取所有 Cookie
            const cookies = await this.context.cookies();
            
            // 提取关键 Cookie
            const cfClearance = cookies.find(c => c.name === 'cf_clearance');
            const sessionId = cookies.find(c => c.name === 'beegosessionID');
            
            if (cfClearance) {
                this.cookies = cookies;
                
                // 格式化为字符串
                const cookieString = cookies
                    .map(c => `${c.name}=${c.value}`)
                    .join('; ');
                
                console.log('[GMGN Cookie] ✅ Cookie 已刷新');
                console.log(`   cf_clearance: ${cfClearance.value.slice(0, 20)}...`);
                if (sessionId) {
                    console.log(`   beegosessionID: ${sessionId.value.slice(0, 20)}...`);
                }
                
                // 保存状态
                await this.saveState();
                
                // 发送事件
                this.emit('cookies', cookieString);
                
                return cookieString;
            } else {
                console.log('[GMGN Cookie] ⚠️ 未获取到 cf_clearance');
                return null;
            }
            
        } catch (error) {
            console.error('[GMGN Cookie] 刷新出错:', error.message);
            throw error;
        }
    }
    
    /**
     * 检查是否需要登录
     */
    async checkNeedLogin() {
        try {
            // 检查页面上是否有登录按钮
            const loginBtn = await this.page.$('text=Connect Wallet');
            const signInBtn = await this.page.$('text=Sign In');
            return !!(loginBtn || signInBtn);
        } catch (e) {
            return false;
        }
    }
    
    /**
     * 保存浏览器状态
     */
    async saveState() {
        try {
            const state = await this.context.storageState();
            
            // 确保目录存在
            const dir = path.dirname(this.config.statePath);
            if (!fs.existsSync(dir)) {
                fs.mkdirSync(dir, { recursive: true });
            }
            
            fs.writeFileSync(this.config.statePath, JSON.stringify(state, null, 2));
            console.log('[GMGN Cookie] 状态已保存');
        } catch (error) {
            console.error('[GMGN Cookie] 保存状态失败:', error.message);
        }
    }
    
    /**
     * 获取当前 Cookie 字符串
     */
    getCookieString() {
        if (!this.cookies) return null;
        return this.cookies.map(c => `${c.name}=${c.value}`).join('; ');
    }
    
    /**
     * 停止刷新器
     */
    async stop() {
        this.isRunning = false;
        
        if (this.refreshTimer) {
            clearInterval(this.refreshTimer);
            this.refreshTimer = null;
        }
        
        if (this.browser) {
            await this.browser.close();
            this.browser = null;
        }
        
        console.log('[GMGN Cookie] 已停止');
    }
    
    /**
     * 获取状态
     */
    getStatus() {
        return {
            isRunning: this.isRunning,
            hasCookies: !!this.cookies,
            refreshInterval: this.config.refreshInterval
        };
    }
}

export default GMGNCookieRefresher;
