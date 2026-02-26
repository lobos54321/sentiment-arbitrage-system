/**
 * 优化后的 AI 买入决策 Prompt v2.0
 *
 * 设计原则:
 * 1. AI作为第二层过滤，在BALANCED策略之后
 * 2. 重点判断：叙事质量、社交热度、入场时机
 * 3. 结合仓位建议（基于信号类型）
 * 4. 不过度筛选，避免漏掉金狗
 */

/**
 * 生成买入决策Prompt
 * @param {Object} token - 代币数据（已通过BALANCED过滤）
 * @param {Object} options - 配置选项
 */
export function generateBuyDecisionPrompt(token, options = {}) {
    const {
        maxPositionPercent = 15,
        basePositionPercent = 5,
        stopLossPercent = 15
    } = options;

    // 计算建议仓位
    const signalWeight = token.signalTrendType === 'ACCELERATING' ? 1.5 : 1.0;
    const smBonus = token.smCount >= 3 ? 1.2 : 1.0;
    const scoreBonus = token.baseScore >= 60 ? 1.15 : 1.0;
    const suggestedPosition = Math.min(
        basePositionPercent * signalWeight * smBonus * scoreBonus,
        maxPositionPercent
    );

    return `你是一个专业的 Solana/BSC Meme 币交易员，只做高赔率交易。

**代币信息**（已通过初步过滤）:
- 符号: $${token.symbol || token.name || 'UNKNOWN'}
- 合约: ${token.tokenAddress || token.ca || '未知'}
- 链: ${token.chain || 'SOL'}
- 市值: $${((token.mcap || 0) / 1000).toFixed(1)}K
- 流动性: $${((token.liquidity || 0) / 1000).toFixed(1)}K

**链上数据** (事实层，已验证):
- 聪明钱在线: ${token.smCount || token.smartWalletOnline || 0} 个
- 信号类型: ${token.signalTrendType || 'STABLE'}
- 基础评分: ${token.baseScore || 0}/100
- 代币等级: ${token.tokenLevel || token.tokenTier || '未知'}

**社交数据** (如有):
${token.xData ? `
- X/Twitter提及: ${token.xData.mention_count || 0} 次
- 情绪: ${token.xData.sentiment || '未知'}
- KOL参与: ${token.xData.kol_involvement?.real_kol_count || 0} 个
- 有机比例: ${((token.xData.bot_detection?.organic_tweet_ratio || 0) * 100).toFixed(0)}%
` : '- 无X数据'}

**你的任务**:
1. 快速判断这个代币是否值得买入
2. 评估叙事强度和社交热度
3. 给出仓位建议

**重要约束**:
- 此代币已通过 BALANCED 过滤（SM≥1, Score≥50, 非DECAYING）
- 你的职责是判断**叙事质量**和**入场时机**
- 不要过度保守，我们可以承受${stopLossPercent}%止损
- 信号类型=${token.signalTrendType || 'STABLE'}时，建议仓位权重=${signalWeight.toFixed(1)}x

**决策选项**:
- BUY_FULL: 全仓买入（${suggestedPosition.toFixed(1)}%仓位）
- BUY_HALF: 半仓买入（${(suggestedPosition / 2).toFixed(1)}%仓位）
- SKIP: 跳过（必须说明原因）

**输出格式** (只返回JSON):
{
  "action": "BUY_FULL" | "BUY_HALF" | "SKIP",
  "position_percent": <建议仓位百分比>,
  "narrative_tier": "S" | "A" | "B" | "C" | "D",
  "narrative_reason": "<20字内叙事评价>",
  "entry_timing": "EARLY" | "OPTIMAL" | "LATE" | "TOO_LATE",
  "target_mcap": <目标市值，单位美元>,
  "stop_loss_percent": ${stopLossPercent},
  "confidence": <0-100置信度>,
  "risk_flags": ["<如有风险点>"]
}`;
}

/**
 * 生成止损确认Prompt
 * @param {Object} position - 持仓信息
 * @param {Object} currentData - 当前市场数据
 */
export function generateStopLossPrompt(position, currentData) {
    return `我持仓的代币触发了止损阈值，请帮我决定是否卖出。

**持仓信息**:
- 代币: $${position.symbol || position.token_ca?.slice(0, 8)}
- 入场价格: $${position.entryPrice || 0}
- 当前价格: $${currentData.currentPrice || 0}
- 当前亏损: ${currentData.pnlPercent?.toFixed(1) || 0}%
- 持仓时间: ${currentData.holdingMinutes || 0} 分钟
- 叙事等级: ${position.narrativeTier || 'UNKNOWN'}

**当前链上状态**:
- 聪明钱在线: ${currentData.smCount || 0}
- 买卖比: ${currentData.buySellRatio?.toFixed(2) || 1}
- 5分钟价格变化: ${currentData.priceChange5m?.toFixed(1) || 0}%

**决策原则**:
- 止损阈值: -${position.stopLossPercent || 15}%
- 如果聪明钱还在且买压>卖压，可以HOLD
- 如果聪明钱撤离或卖压增加，应该SELL

请用 X/Twitter 和社区渠道查询当前热度，然后决定。

**输出格式** (只返回JSON):
{
  "action": "SELL" | "HOLD",
  "reason": "<20字内决策理由>",
  "confidence": <0-100>
}`;
}

/**
 * 生成止盈判断Prompt
 * @param {Object} position - 持仓信息
 * @param {Object} currentData - 当前市场数据
 */
export function generateTakeProfitPrompt(position, currentData) {
    return `我持仓的代币已经盈利，请帮我判断是否应该止盈。

**持仓信息**:
- 代币: $${position.symbol}
- 入场市值: $${position.entryMcap / 1000}K
- 当前市值: $${currentData.mcap / 1000}K
- 当前盈利: +${currentData.pnlPercent?.toFixed(1)}%
- 目标市值: $${position.targetMcap / 1000}K
- 叙事等级: ${position.narrativeTier}

**当前状态**:
- 聪明钱在线: ${currentData.smCount}（入场时: ${position.entrySmCount}）
- 信号趋势: ${currentData.signalTrendType}
- 5分钟动量: ${currentData.priceChange5m?.toFixed(1)}%

**止盈阶梯参考**:
- +50%: 卖20%保护利润
- +100%: 卖30%回本+利润
- +200%: 卖30%锁定利润
- 剩余: 追踪止盈（跌20%退出）

请判断当前是否应该执行止盈。

**输出格式** (只返回JSON):
{
  "action": "TAKE_PROFIT" | "HOLD" | "TRAILING_STOP",
  "sell_percent": <建议卖出百分比，0-100>,
  "reason": "<20字内理由>",
  "new_stop_loss": <如果HOLD，建议新止损价格>
}`;
}

export default {
    generateBuyDecisionPrompt,
    generateStopLossPrompt,
    generateTakeProfitPrompt
};
