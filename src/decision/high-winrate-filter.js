/**
 * 高胜率筛选系统 v1.0
 *
 * 目标: 从BALANCED过滤的代币中筛选出70%胜率的金狗/银狗
 *
 * 核心发现:
 * - 纯规则最高精确率约25%（SM=2时）
 * - 金狗与噪音的链上特征高度重叠
 * - 必须依赖AI判断叙事和社交热度
 *
 * 策略:
 * 1. BALANCED过滤 → 保证82%金狗通过
 * 2. AI实时查询X/Twitter热度和叙事
 * 3. 横向对比同批次代币，选最优
 * 4. 严格信号分层，高置信才高仓位
 */

import GrokTwitterClient from '../social/grok-twitter-client.js';
import { applyFilter, FILTER_PARAMS } from '../config/filter-params.js';

class HighWinRateFilter {
    constructor() {
        this.grokClient = new GrokTwitterClient();
        this.riskConfig = FILTER_PARAMS.RISK_CONTROL;

        // 统计
        this.stats = {
            analyzed: 0,
            passed: 0,
            goldPredictions: 0,
            silverPredictions: 0
        };
    }

    /**
     * 批量筛选 - 横向对比选最优
     * @param {Array} tokens - 同批次的代币列表
     * @returns {Object} 筛选结果
     */
    async filterBatch(tokens) {
        // 第一步: BALANCED过滤
        const passedTokens = tokens.filter(t => applyFilter(t).pass);

        if (passedTokens.length === 0) {
            return { selected: [], reason: '全部被BALANCED过滤' };
        }

        // 第二步: 计算初步评分（链上数据）
        const scoredTokens = passedTokens.map(t => ({
            ...t,
            chainScore: this.calculateChainScore(t)
        }));

        // 按链上评分排序，取前3个进行AI分析（控制成本）
        scoredTokens.sort((a, b) => b.chainScore - a.chainScore);
        const topCandidates = scoredTokens.slice(0, Math.min(3, scoredTokens.length));

        // 第三步: AI分析叙事和社交
        const aiAnalyzedTokens = await Promise.all(
            topCandidates.map(t => this.analyzeWithAI(t))
        );

        // 第四步: 综合评分并筛选
        const finalScored = aiAnalyzedTokens.map(t => ({
            ...t,
            finalScore: this.calculateFinalScore(t),
            prediction: this.predictCategory(t)
        }));

        // 按最终评分排序
        finalScored.sort((a, b) => b.finalScore - a.finalScore);

        // 筛选：只选金狗/银狗预测
        const selected = finalScored.filter(t =>
            t.prediction === 'GOLD' || t.prediction === 'SILVER'
        );

        // 更新统计
        this.stats.analyzed += topCandidates.length;
        this.stats.passed += selected.length;
        selected.forEach(t => {
            if (t.prediction === 'GOLD') this.stats.goldPredictions++;
            if (t.prediction === 'SILVER') this.stats.silverPredictions++;
        });

        return {
            selected,
            analyzed: aiAnalyzedTokens,
            reason: selected.length > 0 ? '找到高概率标的' : '无高概率标的'
        };
    }

    /**
     * 计算链上评分 (基于回测数据优化的权重)
     */
    calculateChainScore(token) {
        let score = 0;

        // 信号类型 (区分度2.82x)
        if (token.signalTrendType === 'ACCELERATING') {
            score += 30; // 高权重
        } else if (token.signalTrendType === 'STABLE') {
            score += 10;
        }

        // SM数量 (SM=2精确率最高25.5%)
        if (token.smCount === 2) {
            score += 25; // 最优
        } else if (token.smCount === 1) {
            score += 15;
        } else if (token.smCount >= 3 && token.smCount <= 4) {
            score += 10;
        } else if (token.smCount >= 5) {
            score += 5; // SM过多反而不好
        }

        // 基础评分 (50-54最优)
        if (token.baseScore >= 50 && token.baseScore < 55) {
            score += 15;
        } else if (token.baseScore >= 55 && token.baseScore < 60) {
            score += 12;
        } else if (token.baseScore >= 60) {
            score += 8;
        }

        return score;
    }

    /**
     * AI分析叙事和社交热度
     */
    async analyzeWithAI(token) {
        try {
            // 调用Grok搜索X/Twitter
            const xData = await this.grokClient.searchToken(
                token.symbol || token.name,
                token.tokenAddress || token.ca,
                15 // 15分钟时间窗
            );

            // 计算AI评分
            const aiScore = this.calculateAIScore(xData);

            return {
                ...token,
                xData,
                aiScore,
                aiAnalyzed: true
            };
        } catch (error) {
            console.warn(`⚠️ AI分析失败 ${token.symbol}: ${error.message}`);
            return {
                ...token,
                xData: null,
                aiScore: 0,
                aiAnalyzed: false,
                aiError: error.message
            };
        }
    }

    /**
     * 计算AI评分 (基于X/Twitter数据)
     */
    calculateAIScore(xData) {
        if (!xData) return 0;

        let score = 0;

        // 提及数量 (max 20分)
        if (xData.mention_count >= 50) score += 20;
        else if (xData.mention_count >= 20) score += 15;
        else if (xData.mention_count >= 10) score += 10;
        else if (xData.mention_count >= 5) score += 5;

        // KOL参与 (max 30分) - 关键指标
        const realKolCount = xData.kol_involvement?.real_kol_count || 0;
        if (realKolCount >= 3) score += 30;
        else if (realKolCount >= 2) score += 25;
        else if (realKolCount >= 1) score += 15;

        // 有机比例 (max 15分)
        const organicRatio = xData.bot_detection?.organic_tweet_ratio || 0;
        if (organicRatio >= 0.8) score += 15;
        else if (organicRatio >= 0.6) score += 10;
        else if (organicRatio >= 0.4) score += 5;

        // 情绪 (max 10分)
        if (xData.sentiment === 'positive') score += 10;
        else if (xData.sentiment === 'neutral') score += 5;

        // 叙事评分 (max 25分)
        const narrativeScore = xData.narrative_score?.total || 0;
        score += Math.min(narrativeScore * 0.25, 25);

        // 风险扣分
        if (xData.origin_source?.is_authentic === false) {
            score -= 20; // 假热度
        }
        if ((xData.kol_involvement?.fake_kol_mentions || 0) > 2) {
            score -= 15; // 虚假KOL炒作
        }

        return Math.max(0, score);
    }

    /**
     * 计算最终综合评分
     */
    calculateFinalScore(token) {
        // 链上 40% + AI 60%
        const chainWeight = 0.4;
        const aiWeight = 0.6;

        const chainNormalized = Math.min(token.chainScore / 70 * 100, 100);
        const aiNormalized = Math.min(token.aiScore / 100 * 100, 100);

        return chainNormalized * chainWeight + aiNormalized * aiWeight;
    }

    /**
     * 预测类别: GOLD / SILVER / BRONZE / NOISE
     */
    predictCategory(token) {
        const finalScore = token.finalScore;
        const aiScore = token.aiScore || 0;
        const chainScore = token.chainScore || 0;

        // 金狗条件 (目标高胜率)
        if (finalScore >= 70 && aiScore >= 50 && chainScore >= 40) {
            return 'GOLD';
        }

        // 银狗条件
        if (finalScore >= 55 && (aiScore >= 40 || chainScore >= 50)) {
            return 'SILVER';
        }

        // 铜狗条件
        if (finalScore >= 40) {
            return 'BRONZE';
        }

        return 'NOISE';
    }

    /**
     * 获取建议仓位
     */
    getPositionSize(token) {
        const { positionSizing } = this.riskConfig;
        let position = positionSizing.basePositionPercent;

        // 信号类型权重
        const signalWeight = positionSizing.signalWeights[token.signalTrendType] || 1.0;
        position *= signalWeight;

        // 预测类别权重
        if (token.prediction === 'GOLD') {
            position *= 1.5;
        } else if (token.prediction === 'SILVER') {
            position *= 1.0;
        } else {
            position *= 0.5;
        }

        // AI置信度调整
        if (token.aiScore >= 70) {
            position *= 1.2;
        } else if (token.aiScore < 30) {
            position *= 0.7;
        }

        // 上限
        return Math.min(position, positionSizing.maxPositionPercent);
    }

    /**
     * 获取止损比例
     */
    getStopLoss(token) {
        // 金狗给予更宽的止损
        if (token.prediction === 'GOLD') {
            return this.riskConfig.maxStopLossPercent; // 20%
        }
        return this.riskConfig.stopLossPercent; // 15%
    }

    /**
     * 生成交易建议
     */
    generateRecommendation(token) {
        if (!token.prediction || token.prediction === 'NOISE') {
            return {
                action: 'SKIP',
                reason: '不符合高胜率条件'
            };
        }

        const positionSize = this.getPositionSize(token);
        const stopLoss = this.getStopLoss(token);

        return {
            action: 'BUY',
            symbol: token.symbol || token.name,
            ca: token.tokenAddress || token.ca,
            prediction: token.prediction,
            positionPercent: Math.round(positionSize * 10) / 10,
            stopLossPercent: stopLoss,
            scores: {
                chain: token.chainScore,
                ai: token.aiScore,
                final: Math.round(token.finalScore * 10) / 10
            },
            xSummary: token.xData ? {
                mentions: token.xData.mention_count,
                kolCount: token.xData.kol_involvement?.real_kol_count || 0,
                sentiment: token.xData.sentiment
            } : null
        };
    }

    /**
     * 获取统计
     */
    getStats() {
        return {
            ...this.stats,
            passRate: this.stats.analyzed > 0
                ? (this.stats.passed / this.stats.analyzed * 100).toFixed(1) + '%'
                : '0%',
            goldRate: this.stats.passed > 0
                ? (this.stats.goldPredictions / this.stats.passed * 100).toFixed(1) + '%'
                : '0%'
        };
    }
}

export default new HighWinRateFilter();
