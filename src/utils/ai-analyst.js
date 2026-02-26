/**
 * AI Analyst v1.1
 * 
 * 调用 Grok (X.AI) API 进行交易决策分析
 * 使用 strategy.js 配置
 */

import OpenAI from 'openai';
import { AI as AI_CONFIG } from '../config/strategy.js';

class AIAnalyst {
    constructor() {
        this.client = null;
        this.initialized = false;
        this.callCount = 0;
        this.lastCallTime = 0;

        // 从配置文件加载参数
        this.config = {
            model: AI_CONFIG.MODEL,
            temperature: AI_CONFIG.TEMPERATURE,
            timeout: AI_CONFIG.TIMEOUT_MS,
            maxRetries: AI_CONFIG.MAX_RETRIES
        };
    }

    /**
     * 初始化 OpenAI 客户端 (Grok 兼容)
     */
    init() {
        if (this.initialized) return;

        const apiKey = process.env.XAI_API_KEY;
        if (!apiKey) {
            console.error('❌ [AI Analyst] XAI_API_KEY 未配置');
            return;
        }

        this.client = new OpenAI({
            apiKey: apiKey,
            baseURL: 'https://api.x.ai/v1'
        });

        this.initialized = true;
        console.log('🤖 [AI Analyst] Grok API 已初始化');
    }

    /**
     * 分析代币是否应该买入
     * @param {string} prompt - 完整的分析 Prompt
     * @returns {Object} AI 决策结果
     */
    async analyze(prompt) {
        if (!this.initialized) {
            this.init();
        }

        if (!this.client) {
            console.error('❌ [AI Analyst] API 未初始化');
            return this.getDefaultResponse('API未初始化');
        }

        // 限流：至少间隔 1 秒
        const now = Date.now();
        if (now - this.lastCallTime < 1000) {
            await this.sleep(1000 - (now - this.lastCallTime));
        }
        this.lastCallTime = Date.now();

        try {
            console.log(`🤖 [AI Analyst] 正在分析... (调用 #${++this.callCount})`);

            const completion = await Promise.race([
                this.client.chat.completions.create({
                    model: this.config.model,
                    messages: [
                        {
                            role: 'system',
                            content: '你是一个专业的加密货币交易分析师，专注于Solana链上的Meme币交易。请严格按照JSON格式回复，不要有其他文字。'
                        },
                        {
                            role: 'user',
                            content: prompt
                        }
                    ],
                    temperature: this.config.temperature,
                    response_format: { type: 'json_object' }
                }),
                this.timeoutPromise(this.config.timeout)
            ]);

            const content = completion.choices[0]?.message?.content;
            if (!content) {
                console.error('❌ [AI Analyst] 无响应内容');
                return this.getDefaultResponse('无响应内容');
            }

            // 解析 JSON
            const result = JSON.parse(content);

            console.log(`✅ [AI Analyst] 分析完成: ${result.action} (${result.position || 'N/A'})`);
            console.log(`   理由: ${result.reason}`);
            if (result.exit_mcap) {
                console.log(`   预计见顶: ${result.exit_mcap} (${result.exit_multiplier || 'N/A'})`);
            }

            return {
                action: result.action || 'SKIP',
                position: result.position || 'small',
                reason: result.reason || '未知原因',
                exit_mcap: result.exit_mcap || null,
                exit_multiplier: result.exit_multiplier || null,
                risk_level: result.risk_level || 'MEDIUM',
                kline_health: result.kline_health || 'NEUTRAL',
                confidence: result.confidence || 50,
                raw: result
            };

        } catch (error) {
            console.error(`❌ [AI Analyst] 分析失败: ${error.message}`);
            return this.getDefaultResponse(error.message);
        }
    }

    /**
     * 获取默认响应（故障安全）
     */
    getDefaultResponse(reason) {
        return {
            action: 'SKIP',
            position: null,
            reason: `AI故障: ${reason}`,
            exit_mcap: null,
            exit_multiplier: null,
            risk_level: 'HIGH',
            kline_health: 'UNKNOWN',
            confidence: 0,
            error: true
        };
    }

    /**
     * 超时 Promise
     */
    timeoutPromise(ms) {
        return new Promise((_, reject) => {
            setTimeout(() => reject(new Error('请求超时')), ms);
        });
    }

    /**
     * 休眠
     */
    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    /**
     * v8.0 止损确认：询问 AI 是否应该卖出
     * @param {Object} position - 持仓信息
     * @param {Object} pnl - 盈亏信息
     * @param {Object} snapshot - 当前快照
     * @returns {Object} { action: 'SELL' | 'HOLD', reason: string }
     */
    async shouldStopLoss(position, pnl, snapshot) {
        if (!this.initialized) {
            this.init();
        }

        if (!this.client) {
            return { action: 'SELL', reason: 'AI未初始化，执行止损' };
        }

        const symbol = position.symbol || position.token_ca?.substring(0, 8);
        const tier = position.intention_tier || position.intentionTier || 'UNKNOWN';
        const entryPrice = position.entry_price || 0;
        const currentPrice = pnl.current_price || snapshot?.current_price || 0;
        const pnlPercent = pnl.pnl_percent || 0;
        const holdingMinutes = Math.floor((Date.now() - new Date(position.entry_time).getTime()) / 60000);

        // 链上实时数据
        const smartMoney = snapshot?.smart_wallet_online || snapshot?.smartMoneyCount || 0;
        const smartMoneyTotal = snapshot?.smart_wallet_total || smartMoney;
        const liquidity = snapshot?.liquidity_usd || 0;
        const volume24h = snapshot?.volume_24h || 0;
        const holders = snapshot?.holders || 0;
        const buySellRatio = snapshot?.buy_sell_ratio || snapshot?.buys_24h / (snapshot?.sells_24h || 1) || 1;

        // 价格趋势
        const change5m = snapshot?.price_change_5m || snapshot?.priceChange5m || 0;
        const change1h = snapshot?.price_change_1h || snapshot?.priceChange1h || 0;
        const highWaterMark = position.high_water_mark || entryPrice;
        const drawdownFromHigh = highWaterMark > 0 ? ((currentPrice - highWaterMark) / highWaterMark * 100) : 0;

        // 社群情绪
        const tgMentions = snapshot?.tg_mentions || snapshot?.tgMentions || 0;
        const sentiment = snapshot?.sentiment || 'unknown';

        const chain = position.chain || 'SOL';
        const tokenCA = position.token_ca || '';

        const prompt = `我持仓的一个 ${chain} 链上的 MEME 币触发了止损阈值，请帮我判断应该卖出还是继续持有。

**代币信息**：
- 代币名称: ${symbol}
- 合约地址 (CA): ${tokenCA}
- 链: ${chain}
- 叙事层级: ${tier}
- 当前亏损: ${pnlPercent.toFixed(1)}%
- 持仓时间: ${holdingMinutes} 分钟

请你去推特 (X) 和 Telegram 上搜索这个代币或 CA 的相关讨论，查看社区热度、情绪、有没有 KOL 在推，然后告诉我应该 SELL 还是 HOLD。

请用 JSON 格式回复：
{
  "action": "SELL" 或 "HOLD",
  "reason": "基于推特/TG查询的理由"
}`;

        try {
            const completion = await Promise.race([
                this.client.chat.completions.create({
                    model: this.config.model,
                    messages: [
                        { role: 'system', content: '你是一个冷静的交易决策者。请用JSON格式回复。' },
                        { role: 'user', content: prompt }
                    ],
                    temperature: 0.3,
                    response_format: { type: 'json_object' }
                }),
                this.timeoutPromise(10000) // 10秒超时
            ]);

            const content = completion.choices[0]?.message?.content;
            if (!content) {
                return { action: 'SELL', reason: 'AI无响应，执行止损' };
            }

            const result = JSON.parse(content);
            return {
                action: result.action === 'HOLD' ? 'HOLD' : 'SELL',
                reason: result.reason || '未知原因'
            };

        } catch (error) {
            console.error(`❌ [AI Analyst] 止损确认失败: ${error.message}`);
            return { action: 'SELL', reason: `AI故障: ${error.message}` };
        }
    }

    /**
     * 获取统计信息
     */
    getStats() {
        return {
            initialized: this.initialized,
            callCount: this.callCount,
            model: this.config.model
        };
    }
}

export default new AIAnalyst();
