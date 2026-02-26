/**
 * Prompt Builder v1.0
 * 
 * 构建 AI 分析 Prompt，融合：
 * 1. 算法特征识别结果
 * 2. 观察期动态变化
 * 3. K 线走势
 * 4. AI 叙事报告
 */

/**
 * 构建完整的 AI 判断 Prompt
 * @param {Object} options
 * @param {Object} options.record - 观察室记录（含入池快照）
 * @param {Object} options.current - 当前代币数据
 * @param {Object} options.factors - DynamicCalculator 计算的因子
 * @param {Object} options.aiReport - DeBot AI 叙事报告
 * @param {Array} options.klines - K 线数据
 * @param {Object} options.narrativeContext - v6.9 AI 叙事验证结果
 * @returns {string} 完整的 Prompt
 */
export function buildEntryPrompt(options) {
    const { record, current, factors, aiReport, klines, narrativeContext } = options;

    // 计算变化量
    const smDelta = (current.smartWalletOnline || 0) - (record.initial?.smartMoney || 0);
    const smPercentChange = record.initial?.smartMoney > 0
        ? ((smDelta / record.initial.smartMoney) * 100).toFixed(1)
        : '0';
    const priceDelta = record.initial?.price > 0
        ? ((current.price - record.initial.price) / record.initial.price * 100).toFixed(1)
        : 'N/A';
    const liqDelta = record.initial?.liquidity > 0
        ? ((current.liquidity - record.initial.liquidity) / record.initial.liquidity * 100).toFixed(1)
        : 'N/A';
    const holdersDelta = (current.holders || 0) - (record.initial?.holders || 0);

    const observeMinutes = Math.floor((Date.now() - record.startTime) / 60000);

    // 判断是否是 Pump.fun 代币
    const isPumpFun = record.address?.endsWith('pump') || record.chain === 'SOL';

    // K 线分析
    const klineSection = buildKlineSection(klines);

    // v6.9 AI 叙事验证部分
    const narrativeSection = buildNarrativeSection(narrativeContext);

    // AI 报告部分
    const aiReportSection = buildAIReportSection(aiReport);

    // SM 流动判断
    let smTrend = 'NEUTRAL';
    if (smDelta >= 1) smTrend = 'BULLISH (流入)';
    else if (parseFloat(smPercentChange) < -30) smTrend = 'BEARISH (大量流出)';
    else if (smDelta < 0) smTrend = 'NEUTRAL (正常换手)';

    return `你是一个专业的 Meme 币交易分析师，专注于 Solana/BSC 链上的早期代币交易。

该币 **${record.symbol}** (${record.address?.slice(0, 8) || 'Unknown'}...) 已在观察池中等待了 ${observeMinutes} 分钟。
${isPumpFun ? '⛽ 这是一个 Pump.fun 代币' : ''}

══════════════════════════════════════════════════════════
🧮 算法特征识别
══════════════════════════════════════════════════════════
- 判定标签: ${factors.tag}
- 判定理由: ${factors.reason}
- 资金/舆论背离度: ${factors.divergence} (>0.8=潜伏期, <0.1=出货期)
- 盘面健康度: ${factors.healthRatio} (>0.15=池厚, <0.05=池薄)
- 确信度: ${factors.conviction} (聪明钱买入力度)

══════════════════════════════════════════════════════════
📊 动态变化 (过去 ${observeMinutes} 分钟)
══════════════════════════════════════════════════════════
- 价格: $${record.initial?.price?.toFixed(8) || 'N/A'} → $${current.price?.toFixed(8) || 'N/A'} (${priceDelta}%)
- 聪明钱: ${record.initial?.smartMoney || 0} → ${current.smartWalletOnline || 0} (${smDelta >= 0 ? '+' : ''}${smDelta}, ${smPercentChange}%)
- 聪明钱趋势: ${smTrend}
- 流动性: $${formatNumber(record.initial?.liquidity)} → $${formatNumber(current.liquidity)} (${liqDelta}%)
- 持有人: ${record.initial?.holders || 0} → ${current.holders || 0} (${holdersDelta >= 0 ? '+' : ''}${holdersDelta})
- 市值: $${formatNumber(current.marketCap)}

${klineSection}

${narrativeSection}

${aiReportSection}

══════════════════════════════════════════════════════════
⚠️ 风险指标
══════════════════════════════════════════════════════════
- Mint权限: ${current.isMintAbandoned ? '已丢弃 ✅' : '未丢弃 ⚠️'}${isPumpFun ? ' (Pump.fun代币可忽略)' : ''}
- Top10占比: ${current.top10Percent || 'N/A'}%
- 链: ${record.chain || 'SOL'}

══════════════════════════════════════════════════════════
❓ 最终决策 (v6.7 逻辑优先级: 资金 > K线 > 叙事)
══════════════════════════════════════════════════════════

**【核心校准规则】**

1. **资金就是正义**: 只要 "聪明钱趋势" 是 BULLISH，即使叙事评分低 (e.g. 2/5)，也必须买入。
2. **Pump.fun 特权**: 如果是 Pump.fun 代币，忽略 Mint 权限未丢弃的风险。
3. **小市值豁免 (v6.3新增)**: 如果市值 < $100K，聪明钱微量流出 (例如 -1) 是**正常换手**，视为 NEUTRAL 甚至 BULLISH (回调上车机会)。只有流出 ≥2 个或占比 >20% 才是真正的卖出信号。
4. **聪明钱波动**: 对于大市值 (> $250K) 的币，任何流出都要警惕。

**【操作指令映射】**

- **GOLDEN 标签**:
  - K线 BULLISH/NEUTRAL → 必须 BUY，仓位 **premium**
  - K线 BEARISH → 可以 BUY，仓位降为 **normal**
- **SILVER 标签**: 建议 BUY，仓位 **normal**
- **TRAP 标签**: 必须 SKIP

══════════════════════════════════════════════════════════
🎯 v6.7 叙事立意评估 (Grand Narrative)
══════════════════════════════════════════════════════════

**【评估叙事的"天花板"】**
请根据代币名称、叙事内容、市场热点，评估该币的**叙事立意等级 (intention_tier)**：

- **TIER_S (顶级叙事)**: 具有2B市值潜力的宏大叙事
  - 例如：马斯克相关、AI革命、政治热点(Trump/Biden)、重大事件(SpaceX/Bitcoin ETF)
  - 特征：能承载大资金、有持续炒作空间、可能被主流媒体报道
  - 止盈策略：可以死拿，目标 10x-100x

- **TIER_A (优质叙事)**: 有爆发潜力的热门叙事
  - 例如：热门IP二创、链上热点(新链/新协议)、KOL带货币
  - 特征：有一定群众基础、可能冲上热搜
  - 止盈策略：可持 3x-5x

- **TIER_B (普通叙事)**: 普通的 meme/搞笑内容
  - 例如：动物系(猫狗青蛙)、简单搞笑图、跟风币
  - 特征：没有独特卖点、容易被替代
  - 止盈策略：赚 50%-100% 就跑，不要有信仰

- **TIER_C (垃圾叙事)**: 低质量或负面叙事
  - 例如：明显蹭热点山寨、负面词汇(SCAM/RUG变体)、过时叙事
  - 特征：连名字都懒得起好、没有任何创意
  - 止盈策略：不建议买入，若买入则尽快出场

**【止盈预测】**
请根据当前价格 ($${current.price?.toFixed(8) || 'N/A'}) 和叙事等级，给出一个具体的**卖出价格 (exit_price)**。
- TIER_S: exit_multiplier 可设 5-20x
- TIER_A: exit_multiplier 可设 2-5x
- TIER_B: exit_multiplier 建议 1.5-2x
- TIER_C: exit_multiplier 建议 1.2-1.5x (保守止盈)

请严格按以下 JSON 格式回答，不要有其他文字：
{
  "action": "BUY" | "SKIP",
  "position": "small" | "normal" | "premium",
  "reason": "简短理由 (e.g. 资金强劲+金狗标签+Pump豁免)",
  "exit_price": "0.00xxxx (纯数字，预计止盈价)",
  "exit_multiplier": "2.5 (预计倍数)",
  "risk_level": "LOW" | "MEDIUM" | "HIGH",
  "kline_health": "BULLISH" | "NEUTRAL" | "BEARISH",
  "confidence": 0-100,
  "intention_tier": "TIER_S" | "TIER_A" | "TIER_B" | "TIER_C",
  "intention_reason": "简短说明为什么是这个等级 (e.g. 马斯克概念+AI热点=S级)"
}`;
}

/**
 * v7.4.5 构建简化版 Prompt (用于交集信号/黄金信号)
 * 仅做风控和最终确认，跳过复杂的叙事分析
 */
export function buildSimplifiedPrompt(options) {
    const { record, current, factors, klines, intersectionBoost } = options;

    const observeMinutes = Math.floor((Date.now() - record.startTime) / 60000);
    const klineSection = buildKlineSection(klines);

    // 简化版只关注核心数据
    return `你是一个交易风控官。代币 **${record.symbol}** 触发了 **${intersectionBoost.level || '多重信号'}**。
已由多个独立信源(DeBot + 猎人)交叉验证。你的任务是进行最后的 **风控检查**。

⏰ 观察时间: ${observeMinutes} 分钟
💎 信号强度: ${intersectionBoost.level} (倍数 x${intersectionBoost.multiplier})
💰 价格变化: $${record.initial?.price?.toFixed(8)} → $${current.price?.toFixed(8)}
🐳 聪明钱: ${current.smartWalletOnline} (初始 ${record.initial?.smartMoney})

${klineSection}

══════════════════════════════════════════════════════════
🛡️ 风控检查清单
══════════════════════════════════════════════════════════
1. 是否有明显的出货嫌疑 (聪明钱大量流出)?
2. K线是否已经崩盘 (长阴线)?
3. 市值是否已经过高 (>$2M 对于早期盘)?

请快速决策：
- 默认 **BUY** (相信交集信号)
- 只有发现致命风险时才 **SKIP**

回复 JSON:
{
  "action": "BUY" | "SKIP",
  "position": "premium" | "normal" | "small",
  "reason": "简短的一句话理由",
  "risk_level": "LOW" | "MEDIUM" | "HIGH"
}`;
}

/**
 * 构建 K 线部分
 */
function buildKlineSection(klines) {
    if (!klines || klines.length === 0) {
        return `══════════════════════════════════════════════════════════
📈 K线数据
══════════════════════════════════════════════════════════
无 K 线数据`;
    }

    const recent = klines.slice(-10); // 最近10根

    let table = `══════════════════════════════════════════════════════════
📈 K线数据(最近 ${recent.length} 根)
══════════════════════════════════════════════════════════
| 序号 | 开盘 | 最高 | 最低 | 收盘 | 涨跌 |
| ------| ------| ------| ------| ------| ------|
        `;

    let bullish = 0;
    let bearish = 0;

    for (let i = 0; i < recent.length; i++) {
        const k = recent[i];
        const open = k.open || k.o || 0;
        const high = k.high || k.h || 0;
        const low = k.low || k.l || 0;
        const close = k.close || k.c || 0;

        if (open === 0) continue;

        const change = ((close - open) / open * 100).toFixed(1);
        const emoji = close >= open ? '🟢' : '🔴';

        if (close >= open) bullish++;
        else bearish++;

        table += `| ${i + 1} ${emoji} | ${open.toFixed(8)} | ${high.toFixed(8)} | ${low.toFixed(8)} | ${close.toFixed(8)} | ${change}% |\n`;
    }

    table += `\n📊 K线统计: ${bullish} 阳 / ${bearish} 阴`;

    // 健康判断
    if (bullish > bearish * 1.5) {
        table += ` → 走势偏强 ✅`;
    } else if (bearish > bullish * 1.5) {
        table += ` → 走势偏弱 ⚠️`;
    } else {
        table += ` → 走势中性`;
    }

    return table;
}

/**
 * v6.9 构建 AI 叙事验证部分
 */
function buildNarrativeSection(narrativeContext) {
    if (!narrativeContext) {
        return `══════════════════════════════════════════════════════════
🎭 AI 叙事验证 (v6.9)
══════════════════════════════════════════════════════════
⚠️ 无叙事验证数据 (未绑定 AINarrativeSystem 或验证失败)`;
    }

    // 生命周期阶段映射
    const lifecycleLabels = {
        'early_explosion': '🚀 早期爆发 (极佳！)',
        'early_growth': '📈 早期成长 (优秀)',
        'growth': '💪 增长期 (良好)',
        'peak': '🏔️ 峰值期 (谨慎)',
        'mature': '📊 成熟期 (稳定)',
        'decline': '📉 衰退期 (危险！)',
        'evergreen': '🌲 常青 (持续热门)',
        'unknown': '❓ 未知'
    };

    const lifecycleLabel = lifecycleLabels[narrativeContext.lifecycle_stage] || lifecycleLabels['unknown'];

    // 热度评级
    let heatLevel = '❄️ 冷门';
    if (narrativeContext.market_heat >= 8) heatLevel = '🔥 极热';
    else if (narrativeContext.market_heat >= 6) heatLevel = '♨️ 热门';
    else if (narrativeContext.market_heat >= 4) heatLevel = '🌡️ 温和';

    // 可持续性评级
    let sustainLevel = '⚠️ 低';
    if (narrativeContext.sustainability >= 7) sustainLevel = '✅ 高';
    else if (narrativeContext.sustainability >= 5) sustainLevel = '🔸 中';

    // 是否新叙事
    const newNarrativeFlag = narrativeContext.is_new_narrative ? '🆕 [新发现叙事]' : '';

    return `══════════════════════════════════════════════════════════
🎭 AI 叙事验证 (v6.9 Twitter 实时验证)
══════════════════════════════════════════════════════════
- 识别叙事: ${narrativeContext.narrative} ${newNarrativeFlag}
- 置信度: ${(narrativeContext.confidence * 100).toFixed(0)}%
- 生命周期: ${lifecycleLabel}
- 市场热度: ${narrativeContext.market_heat}/10 ${heatLevel}
- 可持续性: ${narrativeContext.sustainability}/10 ${sustainLevel}
- AI 分析: ${narrativeContext.reasoning || 'N/A'}

**【叙事对决策的影响】**
${narrativeContext.lifecycle_stage === 'decline' ? '⛔ 衰退期叙事 = 资金持续流出，不建议入场！' : ''}
${narrativeContext.lifecycle_stage === 'early_explosion' || narrativeContext.lifecycle_stage === 'early_growth' ? '✅ 上升期叙事 = 可能有更大涨幅空间，可加仓位！' : ''}
${narrativeContext.market_heat < 3 ? '⚠️ 低热度叙事 = 缺乏市场关注，流动性风险！' : ''}
${narrativeContext.is_new_narrative ? '🆕 新兴叙事 = 未经验证但可能是新热点！' : ''}`;
}

/**
 * 构建 AI 报告部分
 */
function buildAIReportSection(aiReport) {
    if (!aiReport) {
        return `══════════════════════════════════════════════════════════
🤖 AI叙事报告
══════════════════════════════════════════════════════════
无 AI 报告数据`;
    }

    const score = aiReport.rating?.score || aiReport.score || 'N/A';
    const projectName = aiReport.project_name || aiReport.projectName || 'Unknown';
    const narrativeType = aiReport.narrative_type || aiReport.narrativeType || 'Unknown';
    const narrative = aiReport.background?.origin?.text || aiReport.narrative || '无';
    const risk = aiReport.distribution?.negative_incidents?.text || aiReport.risk || '无';

    return `══════════════════════════════════════════════════════════
🤖 AI叙事报告(来自 DeBot)
══════════════════════════════════════════════════════════
    - 项目名称: ${projectName}
    - 叙事评分: ${score}/5 (DeBot 5分制)
        - 叙事类型: ${narrativeType}
    - 叙事内容: "${typeof narrative === 'string' ? narrative.slice(0, 200) : '无'}${narrative?.length > 200 ? '...' : ''}"
        - 风险提示: ${risk} `;
}

/**
 * 格式化数字
 */
function formatNumber(num) {
    if (!num || num === 0) return '0';
    if (num >= 1000000) return (num / 1000000).toFixed(2) + 'M';
    if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
    return num.toFixed(2);
}

export default { buildEntryPrompt, buildSimplifiedPrompt };
