/**
 * GMGN Telegram Bot 数据查询器
 * 
 * 使用 GMGN 官方 Telegram Bot (@GMGN_sol_bot) 查询代币数据
 * 比爬网页 API 更稳定，不需要处理 Cloudflare
 * 
 * 工作原理:
 * 1. 当系统收到信号时，发送代币地址给 GMGN Bot
 * 2. 解析 Bot 返回的消息，提取聪明钱/KOL 数据
 * 3. 将数据合并到信号评分中
 */

import { EventEmitter } from 'events';

export class GMGNTelegramBot extends EventEmitter {
    constructor(telegramClient, config = {}) {
        super();
        
        this.client = telegramClient;  // 复用现有的 Telegram 客户端
        this.config = {
            botUsername: config.botUsername || 'GMGN_sol_bot',
            timeout: config.timeout || 30000,  // 等待回复超时
            retries: config.retries || 2
        };
        
        this.isReady = false;
        this.botEntity = null;
        this.pendingQueries = new Map();  // token -> { resolve, reject, timeout }
        
        console.log('[GMGN Bot] 初始化 - 使用 Telegram Bot 数据源');
    }
    
    /**
     * 初始化 Bot
     */
    async init() {
        if (!this.client) {
            console.error('[GMGN Bot] ❌ 需要 Telegram 客户端');
            return false;
        }
        
        try {
            // 获取 GMGN Bot 实体
            this.botEntity = await this.client.getEntity(this.config.botUsername);
            console.log(`[GMGN Bot] ✅ 已连接到 @${this.config.botUsername}`);
            
            // 监听 Bot 回复
            this.setupMessageHandler();
            
            this.isReady = true;
            return true;
            
        } catch (error) {
            console.error('[GMGN Bot] ❌ 初始化失败:', error.message);
            return false;
        }
    }
    
    /**
     * 设置消息处理器，监听 Bot 回复
     */
    setupMessageHandler() {
        // 监听来自 GMGN Bot 的消息
        this.client.addEventHandler(async (event) => {
            const message = event.message;
            if (!message || !message.peerId) return;
            
            // 检查是否来自 GMGN Bot
            try {
                const sender = await message.getSender();
                if (sender?.username?.toLowerCase() !== this.config.botUsername.toLowerCase()) {
                    return;
                }
                
                // 解析消息内容
                const text = message.text || message.message || '';
                this.handleBotResponse(text);
                
            } catch (error) {
                // 忽略解析错误
            }
        });
    }
    
    /**
     * 处理 Bot 回复
     */
    handleBotResponse(text) {
        // 尝试从回复中提取代币地址
        const addressMatch = text.match(/([A-Za-z0-9]{32,44})/);
        if (!addressMatch) return;
        
        const tokenAddress = addressMatch[1];
        
        // 检查是否有等待中的查询
        if (this.pendingQueries.has(tokenAddress)) {
            const query = this.pendingQueries.get(tokenAddress);
            clearTimeout(query.timeout);
            
            // 解析数据
            const data = this.parseGMGNResponse(text);
            query.resolve(data);
            
            this.pendingQueries.delete(tokenAddress);
        }
    }
    
    /**
     * 解析 GMGN Bot 回复
     */
    parseGMGNResponse(text) {
        const data = {
            smart_money_count: 0,
            smart_money_buy: 0,
            smart_money_sell: 0,
            kol_count: 0,
            kol_list: [],
            top_holders: [],
            degen_score: 0,
            blue_chip_index: 0,
            is_honeypot: false,
            raw_text: text
        };
        
        try {
            // 聪明钱数量 (Smart Money: 5 buy, 2 sell)
            const smartMoneyMatch = text.match(/Smart\s*Money[:\s]*(\d+)\s*buy[,\s]*(\d+)\s*sell/i);
            if (smartMoneyMatch) {
                data.smart_money_buy = parseInt(smartMoneyMatch[1]) || 0;
                data.smart_money_sell = parseInt(smartMoneyMatch[2]) || 0;
                data.smart_money_count = data.smart_money_buy + data.smart_money_sell;
            }
            
            // 备用格式: Smart Money: 5
            const smartMoneySimple = text.match(/Smart\s*Money[:\s]*(\d+)/i);
            if (smartMoneySimple && !smartMoneyMatch) {
                data.smart_money_count = parseInt(smartMoneySimple[1]) || 0;
            }
            
            // KOL 数量 (KOL: 3)
            const kolMatch = text.match(/KOL[:\s]*(\d+)/i);
            if (kolMatch) {
                data.kol_count = parseInt(kolMatch[1]) || 0;
            }
            
            // Degen 分数 (Degen Score: 85)
            const degenMatch = text.match(/Degen\s*Score[:\s]*(\d+)/i);
            if (degenMatch) {
                data.degen_score = parseInt(degenMatch[1]) || 0;
            }
            
            // Blue Chip Index (BCI: 2.5)
            const bciMatch = text.match(/(?:Blue\s*Chip|BCI)[:\s]*([\d.]+)/i);
            if (bciMatch) {
                data.blue_chip_index = parseFloat(bciMatch[1]) || 0;
            }
            
            // Honeypot 检测
            if (text.toLowerCase().includes('honeypot') && 
                (text.toLowerCase().includes('yes') || text.toLowerCase().includes('detected'))) {
                data.is_honeypot = true;
            }
            
            // 提取 KOL 列表（如果有）
            const kolListMatch = text.match(/KOL[^:]*:\s*([^\n]+)/i);
            if (kolListMatch) {
                data.kol_list = kolListMatch[1].split(/[,，]/).map(s => s.trim()).filter(Boolean);
            }
            
        } catch (error) {
            console.error('[GMGN Bot] 解析错误:', error.message);
        }
        
        return data;
    }
    
    /**
     * 查询代币数据
     */
    async queryToken(tokenAddress, chain = 'sol') {
        if (!this.isReady) {
            console.log('[GMGN Bot] ⚠️ Bot 未就绪');
            return null;
        }
        
        return new Promise(async (resolve, reject) => {
            try {
                // 设置超时
                const timeoutId = setTimeout(() => {
                    this.pendingQueries.delete(tokenAddress);
                    resolve(null);  // 超时返回 null，不抛错
                }, this.config.timeout);
                
                // 存储等待中的查询
                this.pendingQueries.set(tokenAddress, {
                    resolve,
                    reject,
                    timeout: timeoutId
                });
                
                // 发送查询消息给 Bot
                await this.client.sendMessage(this.botEntity, {
                    message: tokenAddress
                });
                
                console.log(`[GMGN Bot] 📤 查询: ${tokenAddress.slice(0, 8)}...`);
                
            } catch (error) {
                this.pendingQueries.delete(tokenAddress);
                console.error('[GMGN Bot] 查询失败:', error.message);
                resolve(null);
            }
        });
    }
    
    /**
     * 批量查询多个代币
     */
    async queryTokens(tokens, chain = 'sol') {
        const results = [];
        
        for (const token of tokens) {
            const data = await this.queryToken(token, chain);
            if (data) {
                results.push({ token, data });
            }
            
            // 避免发送太快
            await this.sleep(2000);
        }
        
        return results;
    }
    
    /**
     * 获取状态
     */
    getStatus() {
        return {
            isReady: this.isReady,
            botUsername: this.config.botUsername,
            pendingQueries: this.pendingQueries.size
        };
    }
    
    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

export default GMGNTelegramBot;
