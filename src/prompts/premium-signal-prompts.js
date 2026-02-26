/**
 * Premium Signal Prompts
 *
 * 为付费频道信号生成 AI 分析 Prompt
 * 信号来自 Egeye AI Gems 100X Vip 频道
 */

/**
 * 生成买入决策 Prompt
 * @param {Object} signal - 频道解析出的信号数据
 * @param {Object} snapshot - 链上快照数据（可选）
 * @param {Object} options - 配置选项
 */
export function generatePremiumBuyPrompt(signal, snapshot = null, options = {}) {
    const {
        maxPositionPercent = 15,
        basePositionPercent = 5,
        stopLossPercent = 15,
        gmgnData = null,
        backtestMode = false
    } = options;

    const snapshotSection = snapshot ? `
**链上安全检查**:
- 流动性: ${snapshot.liquidity ? snapshot.liquidity.toFixed(1) + ' SOL (~$' + (snapshot.liquidity * 150 / 1000).toFixed(1) + 'K)' : '未知'}
- Freeze Authority: ${snapshot.freeze_authority === 'Burned' ? '✅ 已销毁' : snapshot.freeze_authority || '未知'}
- Mint Authority: ${snapshot.mint_authority === 'Burned' ? '✅ 已销毁' : snapshot.mint_authority || '未知'}
- LP 状态: ${snapshot.lp_status || '未知'}
- 持有人数: ${snapshot.holder_count || '未知'}
- Top10 持仓: ${snapshot.top10_percent ? snapshot.top10_percent + '%' : '未知'}
- 洗盘检测: ${snapshot.wash_flag || '未知'}
- 风险钱包: ${snapshot.key_risk_wallets?.length || 0} 个
` : '';

    // 合并流动性
    let liquidityDisplay = '未知';
    if (gmgnData?.liquidity_usd > 0) {
        liquidityDisplay = '$' + (gmgnData.liquidity_usd / 1000).toFixed(1) + 'K';
    } else if (snapshot?.liquidity > 0) {
        const estUsd = snapshot.liquidity * 150;
        liquidityDisplay = '~$' + (estUsd / 1000).toFixed(1) + 'K (' + snapshot.liquidity.toFixed(1) + ' SOL)';
    }

    // 回测模式：不包含价格变化（那是事后数据）
    const gmgnSection = gmgnData ? `
**交易数据**:
- 24H 买入: ${gmgnData.buy_count_24h} 笔 | 卖出: ${gmgnData.sell_count_24h} 笔 | 买卖比: ${gmgnData.sell_count_24h > 0 ? (gmgnData.buy_count_24h / gmgnData.sell_count_24h).toFixed(2) : '∞'}
- 24H 交易量: $${gmgnData.volume_24h ? (gmgnData.volume_24h / 1000).toFixed(1) + 'K' : '0'}
- 流动性: ${liquidityDisplay}${!backtestMode ? `
- 1H 买入: ${gmgnData.buy_count_1h || 0} 笔 | 卖出: ${gmgnData.sell_count_1h || 0} 笔
- 5M 买入: ${gmgnData.buy_count_5m || 0} 笔 | 卖出: ${gmgnData.sell_count_5m || 0} 笔
- 5分钟涨幅: ${gmgnData.price_change_5m}%
- 1小时涨幅: ${gmgnData.price_change_1h}%` : ''}
` : '';

    return `你是一个 Solana Meme 币量化交易分析师。基于数据和叙事综合判断。
目标：从频道信号中筛选出最有潜力的 15-25%。

**代币信息**:
- 符号: $${signal.symbol || 'UNKNOWN'}
- 合约: ${signal.token_ca}
- 市值: $${signal.market_cap ? (signal.market_cap / 1000).toFixed(1) + 'K' : '未知'}
- 持有人: ${signal.holders || '未知'}
- 代币年龄: ${signal.age || '未知'}
- Freeze: ${signal.freeze_ok ? '✅' : '❌/未知'}
- Mint: ${signal.mint_ok ? '✅' : '❌/未知'}
${snapshotSection}${gmgnSection}
**频道描述**:
${signal.description || '无'}

**决策框架**:

1. **安全硬性过滤**（任一触发 → SKIP）:
   - 洗盘检测 = HIGH 且 买卖比 < 1.2 → SKIP
   - Top10 > 50% → SKIP
   - 市值 > $300K → SKIP

2. **正面信号**（每满足一条 +20 分）:
   - 买卖比 > 1.3（买压 > 卖压）
   - 24H 交易量 > $20K（有真实交易）
   - 流动性 > $5K（可安全退出）
   - 市值 $5K-$60K（甜蜜区）
   - 代币概念有社交传播力（AI、知名IP、热门梗、病毒事件）
   - Freeze/Mint 已禁用

3. **负面信号**（每满足一条 -20 分）:
   - 买卖比 < 0.8（卖压重）
   - 交易量 < $5K（无人关注）
   - 洗盘检测 = MEDIUM（有风险但不致命）
   - 概念模糊/无叙事

4. **评分决策**:
   - 总分 >= 60 → BUY_FULL
   - 总分 40-59 → BUY_HALF
   - 总分 < 40 → SKIP

**重要**: 不要因为你不熟悉某个概念就判定无叙事。Meme 币经常是新热点。如果名字有趣、独特、或与知名事物相关，给正面评价。

**输出格式** (只返回 JSON):
{
  "action": "BUY_FULL" | "BUY_HALF" | "SKIP",
  "position_percent": <仓位%>,
  "narrative_tier": "S" | "A" | "B" | "C" | "D",
  "narrative_reason": "<20字内评价>",
  "entry_timing": "EARLY" | "OPTIMAL" | "LATE" | "TOO_LATE",
  "target_mcap": <目标市值>,
  "stop_loss_percent": ${stopLossPercent},
  "confidence": <0-100>,
  "risk_flags": ["<风险点>"],
  "score_breakdown": "<正面X分 - 负面Y分 = Z分>"
}`;
}

export default { generatePremiumBuyPrompt };
