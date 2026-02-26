/**
 * Claude AI Analyst
 *
 * 使用 Anthropic Claude 替代 Grok 进行交易分析
 * 专为 Premium Signal 渠道优化
 *
 * 模型: claude-opus-4-6
 * 温度: 0.3 (偏保守但允许灵活判断)
 * 超时: 15s
 * 限速: 1s 间隔
 */

import Anthropic from '@anthropic-ai/sdk';

class ClaudeAnalyst {
  constructor() {
    this.client = null;
    this.initialized = false;
    this.callCount = 0;
    this.lastCallTime = 0;
    this.model = 'claude-opus-4-6';
    this.temperature = 0.3;
    this.timeoutMs = 15000;
    this.maxRetries = 2;
    this.minInterval = 1000; // 1s 限速

    this.systemPrompt = `你是一个专业的加密货币交易分析师，专注于 Solana 链上的 meme coin 交易。
你的任务是根据提供的代币数据和信号信息，做出买入决策。

核心原则：
1. 安全第一 - 任何可疑信号立即 SKIP
2. 数据驱动 - 基于链上数据而非情绪判断
3. 风险控制 - 严格遵守仓位和止损规则
4. 叙事判断 - 评估代币的叙事热度和持续性

你必须返回严格的 JSON 格式，不要包含任何其他文本。`;
  }

  /**
   * 初始化 Claude 客户端
   */
  init() {
    if (this.initialized) return;

    const apiKey = process.env.ANTHROPIC_API_KEY;
    if (!apiKey) {
      console.error('❌ [Claude Analyst] ANTHROPIC_API_KEY 未设置');
      throw new Error('ANTHROPIC_API_KEY not configured');
    }

    const baseURL = process.env.ANTHROPIC_BASE_URL;
    const opts = { apiKey };
    if (baseURL) opts.baseURL = baseURL;

    this.client = new Anthropic(opts);
    this.initialized = true;
    console.log(`🤖 [Claude Analyst] 初始化完成 | 模型: ${this.model} | 温度: ${this.temperature}${baseURL ? ' | Base: ' + baseURL : ''}`);
  }

  /**
   * 分析交易信号
   *
   * @param {string} prompt - 完整的分析 prompt
   * @returns {Object} 分析结果 JSON
   */
  async analyze(prompt) {
    if (!this.initialized) this.init();

    // 限速控制
    await this.rateLimit();

    for (let attempt = 0; attempt <= this.maxRetries; attempt++) {
      try {
        console.log(`🤖 [Claude Analyst] 分析中... (第 ${attempt + 1} 次)`);

        const response = await Promise.race([
          this.client.messages.create({
            model: this.model,
            max_tokens: 1024,
            temperature: this.temperature,
            system: this.systemPrompt,
            messages: [{ role: 'user', content: prompt }]
          }),
          this.timeout(this.timeoutMs)
        ]);

        this.callCount++;
        this.lastCallTime = Date.now();

        // 提取文本内容
        const text = response.content
          .filter(block => block.type === 'text')
          .map(block => block.text)
          .join('');

        if (!text) {
          console.error('❌ [Claude Analyst] 空响应');
          return this.getDefaultResponse('Empty response from Claude');
        }

        // 解析 JSON
        const parsed = this.parseJSON(text);
        if (!parsed) {
          console.error('❌ [Claude Analyst] JSON 解析失败:', text.substring(0, 200));
          return this.getDefaultResponse('JSON parse failed');
        }

        console.log(`✅ [Claude Analyst] 分析完成: ${parsed.action} | 信心: ${parsed.confidence}`);
        return {
          action: parsed.action || 'SKIP',
          position_percent: parsed.position_percent || 0,
          narrative_tier: parsed.narrative_tier || 'D',
          narrative_reason: parsed.narrative_reason || '',
          entry_timing: parsed.entry_timing || 'LATE',
          target_mcap: parsed.target_mcap || 0,
          stop_loss_percent: parsed.stop_loss_percent || 15,
          confidence: parsed.confidence || 0,
          risk_flags: parsed.risk_flags || [],
          raw: text
        };

      } catch (error) {
        console.error(`❌ [Claude Analyst] 第 ${attempt + 1} 次失败:`, error.message);

        if (attempt === this.maxRetries) {
          return this.getDefaultResponse(`All ${this.maxRetries + 1} attempts failed: ${error.message}`);
        }

        // 重试前等待
        await this.sleep(1000 * (attempt + 1));
      }
    }

    return this.getDefaultResponse('Unexpected error');
  }

  /**
   * 止损分析
   *
   * @param {Object} position - 当前持仓
   * @param {number} pnl - 当前盈亏百分比
   * @param {Object} snapshot - 最新链上快照
   * @returns {Object} { action: 'SELL'|'HOLD', reason }
   */
  async analyzeStopLoss(position, pnl, snapshot) {
    if (!this.initialized) this.init();

    await this.rateLimit();

    const prompt = `当前持仓分析：
- 代币: ${position.symbol || position.token_ca}
- 链: ${position.chain}
- 入场价格: ${position.executed_price || 'N/A'}
- 当前盈亏: ${pnl.toFixed(2)}%
- 持仓时间: ${Math.round((Date.now() - position.timestamp) / 60000)} 分钟

链上数据：
- 流动性: ${snapshot.liquidity || 'Unknown'}
- Top10 持仓: ${snapshot.top10_percent || 'Unknown'}%
- 滑点: ${snapshot.slippage_sell_20pct || 'Unknown'}%

请判断是否应该止损。返回 JSON：
{"action": "SELL" 或 "HOLD", "reason": "原因说明"}`;

    try {
      const response = await Promise.race([
        this.client.messages.create({
          model: this.model,
          max_tokens: 512,
          temperature: 0.2,
          system: this.systemPrompt,
          messages: [{ role: 'user', content: prompt }]
        }),
        this.timeout(this.timeoutMs)
      ]);

      this.callCount++;
      this.lastCallTime = Date.now();

      const text = response.content
        .filter(block => block.type === 'text')
        .map(block => block.text)
        .join('');

      const parsed = this.parseJSON(text);
      if (!parsed) {
        return { action: 'HOLD', reason: 'Claude 响应解析失败，保持持仓' };
      }

      return {
        action: parsed.action === 'SELL' ? 'SELL' : 'HOLD',
        reason: parsed.reason || 'No reason provided'
      };

    } catch (error) {
      console.error('❌ [Claude Analyst] 止损分析失败:', error.message);
      return { action: 'HOLD', reason: `分析失败: ${error.message}` };
    }
  }

  /**
   * 解析 JSON（容错处理）
   */
  parseJSON(text) {
    try {
      // 尝试直接解析
      return JSON.parse(text);
    } catch {
      // 尝试提取 JSON 块
      const match = text.match(/\{[\s\S]*\}/);
      if (match) {
        try {
          return JSON.parse(match[0]);
        } catch {
          return null;
        }
      }
      return null;
    }
  }

  /**
   * 默认响应（错误时返回 SKIP）
   */
  getDefaultResponse(reason) {
    return {
      action: 'SKIP',
      position_percent: 0,
      narrative_tier: 'D',
      narrative_reason: reason,
      entry_timing: 'TOO_LATE',
      target_mcap: 0,
      stop_loss_percent: 15,
      confidence: 0,
      risk_flags: ['AI_ERROR'],
      error: true,
      error_reason: reason
    };
  }

  /**
   * 限速控制（最少 1s 间隔）
   */
  async rateLimit() {
    const elapsed = Date.now() - this.lastCallTime;
    if (elapsed < this.minInterval) {
      await this.sleep(this.minInterval - elapsed);
    }
  }

  /**
   * 超时 Promise
   */
  timeout(ms) {
    return new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`Claude API timeout (${ms}ms)`)), ms)
    );
  }

  /**
   * Sleep
   */
  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  /**
   * 获取统计信息
   */
  getStats() {
    return {
      model: this.model,
      callCount: this.callCount,
      lastCallTime: this.lastCallTime,
      initialized: this.initialized
    };
  }
}

// 单例导出
const claudeAnalyst = new ClaudeAnalyst();
export { ClaudeAnalyst };
export default claudeAnalyst;
