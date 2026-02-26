/**
 * Social Heat Scorer v1.0
 * 
 * 评估社交热度 - Meme 币的"燃料"
 * 
 * 评分维度：
 * 1. X/Twitter 提及数
 * 2. Engagement (点赞、评论、转发)
 * 3. 趋势方向 (热度上升还是下降)
 * 4. 关键人物提及
 * 
 * 满分 10 分
 */

export class SocialHeatScorer {
    constructor() {
        // 提及数评分配置
        this.mentionConfig = {
            viral: { minMentions: 50, score: 5 },      // 50+ 提及: +5分
            hot: { minMentions: 20, score: 4 },        // 20-50: +4分
            warm: { minMentions: 10, score: 3 },       // 10-20: +3分
            tepid: { minMentions: 5, score: 2 },       // 5-10: +2分
            cold: { minMentions: 1, score: 1 },        // 1-5: +1分
            dead: { minMentions: 0, score: 0 }         // 0: 0分
        };

        // Engagement 评分配置
        this.engagementConfig = {
            massive: { minTotal: 1000, score: 3 },     // 1000+ total: +3分
            high: { minTotal: 500, score: 2.5 },       // 500-1000: +2.5分
            moderate: { minTotal: 100, score: 2 },     // 100-500: +2分
            low: { minTotal: 20, score: 1 },           // 20-100: +1分
            minimal: { minTotal: 0, score: 0 }         // < 20: 0分
        };

        // 关键人物提及加成
        this.keyInfluencerBonus = {
            tierS: 3,    // Tier S 提及: +3分
            tierA: 2,    // Tier A 提及: +2分
            tierB: 1,    // Tier B 提及: +1分
            tierC: 0.5   // Tier C 提及: +0.5分
        };
    }

    /**
     * 计算社交热度评分
     * 
     * @param {Object} xData - X/Twitter 搜索数据
     * @param {Object} keyInfluencerResult - 关键人物评分结果
     * @returns {Object} { score, maxScore, details, heatLevel }
     */
    score(xData, keyInfluencerResult = null) {
        const result = {
            score: 0,
            maxScore: 10,
            details: [],
            breakdown: {
                mentions: 0,
                engagement: 0,
                keyInfluencer: 0
            },
            heatLevel: 'COLD',
            sentiment: 'NEUTRAL'
        };

        // 1. 提及数评分
        const mentionScore = this.scoreMentions(xData);
        result.breakdown.mentions = mentionScore.score;
        result.score += mentionScore.score;
        result.details.push(mentionScore.detail);

        // 2. Engagement 评分
        const engagementScore = this.scoreEngagement(xData);
        result.breakdown.engagement = engagementScore.score;
        result.score += engagementScore.score;
        result.details.push(engagementScore.detail);

        // 3. 关键人物加成 (不重复计算，只取额外加成)
        if (keyInfluencerResult && keyInfluencerResult.tierSInteractions?.length > 0) {
            result.breakdown.keyInfluencer = 2;
            result.score += 2;
            result.details.push('关键人物: Tier S 提及 (+2)');
        } else if (keyInfluencerResult && keyInfluencerResult.tierAInteractions?.length > 0) {
            result.breakdown.keyInfluencer = 1;
            result.score += 1;
            result.details.push('关键人物: Tier A 提及 (+1)');
        }

        // 限制分数范围 [0, 10]
        result.score = Math.max(0, Math.min(result.score, result.maxScore));

        // 确定热度级别
        result.heatLevel = this.getHeatLevel(result.score);

        // 情绪分析
        result.sentiment = this.getSentiment(xData);

        return result;
    }

    /**
     * 提及数评分
     */
    scoreMentions(xData) {
        const mentions = xData?.mention_count || xData?.mentions || 0;

        for (const [level, config] of Object.entries(this.mentionConfig)) {
            if (mentions >= config.minMentions) {
                return {
                    score: config.score,
                    detail: `X提及: ${mentions}条 (+${config.score})`
                };
            }
        }

        return { score: 0, detail: 'X提及: 0条 (+0)' };
    }

    /**
     * Engagement 评分
     */
    scoreEngagement(xData) {
        // 计算总 engagement
        const engagement = xData?.engagement || {};
        const totalLikes = engagement.total_likes || 0;
        const totalRetweets = engagement.total_retweets || 0;
        const totalReplies = engagement.total_replies || 0;
        const total = totalLikes + totalRetweets * 2 + totalReplies * 1.5;

        for (const [level, config] of Object.entries(this.engagementConfig)) {
            if (total >= config.minTotal) {
                return {
                    score: config.score,
                    detail: `Engagement: ${Math.floor(total)} (+${config.score})`
                };
            }
        }

        return { score: 0, detail: 'Engagement: 低 (+0)' };
    }

    /**
     * 确定热度级别
     */
    getHeatLevel(score) {
        if (score >= 8) return 'ON_FIRE';       // 🔥🔥🔥
        if (score >= 6) return 'HOT';           // 🔥🔥
        if (score >= 4) return 'WARM';          // 🔥
        if (score >= 2) return 'TEPID';         // 温
        return 'COLD';                          // 冷
    }

    /**
     * 情绪分析
     */
    getSentiment(xData) {
        const sentiment = xData?.sentiment || xData?.overall_sentiment || 'unknown';

        if (typeof sentiment === 'string') {
            const lower = sentiment.toLowerCase();
            if (lower.includes('positive') || lower.includes('bullish')) return 'POSITIVE';
            if (lower.includes('negative') || lower.includes('bearish')) return 'NEGATIVE';
            if (lower.includes('mixed')) return 'MIXED';
        }

        return 'NEUTRAL';
    }

    /**
     * 快速热度检查 (用于 Hard Gate)
     */
    quickCheck(xData) {
        const mentions = xData?.mention_count || 0;
        const engagement = xData?.engagement?.total_likes || 0;

        return {
            hasHeat: mentions >= 5 || engagement >= 50,
            level: mentions >= 20 ? 'HOT' : (mentions >= 5 ? 'WARM' : 'COLD'),
            mentions,
            engagement
        };
    }
}

export default SocialHeatScorer;
