/**
 * Narrative Analyst Prompt
 * 
 * 用于 LLM 二次分析 DeBot AI Report，判断叙事质量
 * 
 * 输入: DeBot 的数据 (Rank + AI Report + Heatmap) + X/Twitter 数据
 * 输出: JSON { score: 0-100, reason: string, risk_level: LOW|MEDIUM|HIGH }
 */

export function generateNarrativePrompt(data) {
    // X/Twitter 数据部分
    let xSection = '';
    if (data.xData && data.xData.mention_count > 0) {
        const x = data.xData;
        xSection = `
【X/Twitter 实时数据 - 关键！】
- 提及次数: ${x.mention_count || 0}
- 独立作者: ${x.unique_authors || 0}
- 情绪: ${x.sentiment || '未知'}
- 总互动: ${x.engagement?.total_likes || 0} 赞 / ${x.engagement?.total_retweets || 0} 转发
${x.origin_source ? `
- 信号源头类型: ${x.origin_source.type || '未知'}
- 源头是否真实: ${x.origin_source.is_authentic ? '✅是' : '❌可疑'}
- 源头分析: ${x.origin_source.explanation || '无'}
` : ''}
${x.kol_involvement ? `
- 真实KOL发帖: ${x.kol_involvement.real_kol_count || 0} 个
- 被假冒提及KOL: ${x.kol_involvement.fake_kol_mentions || 0} 次
` : ''}
${x.bot_detection ? `
- 疑似机器人: ${x.bot_detection.suspected_bot_tweets || 0} 条
- 有机比例: ${((x.bot_detection.organic_tweet_ratio || 0) * 100).toFixed(0)}%
` : ''}
${x.narrative_score ? `
- X叙事评分: ${x.narrative_score.total || 0}/100 (${x.narrative_score.grade || 'N/A'})
- X推荐: ${x.narrative_score.recommendation || '无'}
` : ''}
${x.top_tweets && x.top_tweets.length > 0 ? `
- 热门推文:
${x.top_tweets.slice(0, 3).map(t => `  • @${t.author}: "${t.text?.slice(0, 80)}..." (${t.engagement?.likes || 0}赞)`).join('\n')}
` : ''}
${x.risk_flags && x.risk_flags.length > 0 ? `
- ⚠️ X风险标记: ${x.risk_flags.join(', ')}
` : ''}`;
    } else {
        xSection = `
【X/Twitter 数据】
- 未找到相关推文，可能是新币或搜索失败`;
    }

    return `你是一个身经百战的 Solana Meme 币交易专家，风格犀利，只看赔率。

请分析代币 ${data.symbol || data.tokenAddress?.slice(0, 8)} 的炒作潜力。

【链上数据 - 事实层】
- 聪明钱在线: ${data.smartWalletOnline || 0} 个
- 流动性: $${(data.liquidity || 0).toLocaleString()}
- 报警次数: ${data.signalCount || 0} 次
- 代币等级: ${data.tokenLevel || data.tokenTier || '未知'}
- 最大涨幅: ${(data.maxPriceGain || 0).toFixed(1)}x

【叙事数据 - DeBot AI Report】
- DeBot评分: ${data.debotScore || 0}/10
- 叙事类型: ${data.narrativeType || '未知'}
- 叙事描述: "${data.narrative || '无描述'}"
- 负面信息: ${data.negativeIncidents || '无'}

【社交数据 - TG热度】
- TG频道数: ${data.tgChannelCount || 0}
- 是否有 Tier1 频道: ${data.hasTier1 ? '是' : '否'}
${xSection}

【评分任务】
请结合以上所有数据，给出 0-100 分，评判这个币的"炒作潜力"和"风险等级"。

评分标准:
- 0-40 (垃圾): 老梗换皮、蹭热点生硬、黑料明显、X上无热度或全是机器人
- 41-60 (普通): 有资金但叙事弱，或好叙事但资金/社交还没起来
- 61-80 (优质): 强叙事(原创/顶级IP) + 资金确认入场 + X上有真实KOL参与
- 81-100 (顶级): 现象级叙事 + 聪明钱扎堆 + X上病毒式传播

【硬约束 - 必须遵守】
1. 如果 smartWalletOnline < 2，最高给 40 分
2. 如果有明确负面信息(scam/rug/honeypot)，最高给 30 分
3. 如果 X 上发现大量机器人或假KOL提及，最高给 50 分
4. 如果 X 上有真实KOL发帖且互动高，可以加 10-20 分
5. 如果 signalCount > 50，说明可能已经过热，扣 10-20 分

【输出格式 - 只返回JSON，不要其他内容】
{
  "score": <0-100的整数>,
  "reason": "<一句话评价，不超过30字，要提到关键发现>",
  "risk_level": "<LOW|MEDIUM|HIGH>"
}`;
}

export default generateNarrativePrompt;
