/**
 * 高胜率筛选系统 v2.0 - 优化版
 *
 * 优化目标:
 * - 保持胜率 ≥70%
 * - 提高召回率从30%到50%+
 *
 * 核心改进:
 * 1. 放宽SILVER条件
 * 2. 对ACCELERATING信号给予更高权重
 * 3. 增加BRONZE-HIGH类别（中等置信）
 */

import GrokTwitterClient from '../social/grok-twitter-client.js';
import { applyFilter, FILTER_PARAMS } from '../config/filter-params.js';
import garbageDetector from '../scoring/garbage-signal-detector.js';

class HighWinRateFilterV2 {
    constructor() {
        this.grokClient = new GrokTwitterClient();
        this.riskConfig = FILTER_PARAMS.RISK_CONTROL;

        // 优化后的阈值
        this.thresholds = {
            GOLD: {
                finalScore: 65,    // 从70降到65
                aiScore: 45,       // 从50降到45
                chainScore: 35     // 从40降到35
            },
            SILVER: {
                finalScore: 50,    // 从55降到50
                aiScore: 35,       // 从40降到35
                chainScore: 30     // 保持
            },
            BRONZE_HIGH: {        // 新增：高置信铜狗
                finalScore: 42,
                minCondition: 'ACCELERATING_OR_SM2'
            }
        };

        this.stats = {
            analyzed: 0,
            goldPredictions: 0,
            silverPredictions: 0,
            bronzeHighPredictions: 0
        };
    }

    /**
     * 单个代币分析
     */
    async analyzeToken(token) {
        // ── 0. 垃圾信号预检 (最早拦截，节省后续算力) ──────────────────
        const garbageConfig = FILTER_PARAMS.GARBAGE_FILTER;
        if (garbageConfig?.enabled !== false) {
            const socialData = token.socialData || token._socialSnapshot || {};
            const dynamicFactors = token.dynamicFactors || {};
            const garbageResult = garbageDetector.detect(token, socialData, dynamicFactors);

            if (garbageResult.verdict === 'GARBAGE') {
                return {
                    action: 'GARBAGE_REJECTED',
                    reason: `垃圾信号拦截 (垃圾分=${garbageResult.garbageScore}): ${garbageResult.reasons.slice(0, 2).join(' | ')}`,
                    garbageScore: garbageResult.garbageScore,
                    garbageBreakdown: garbageResult.breakdown
                };
            }

            // SUSPECT: 存入 token 以便后续降级决策
            if (garbageResult.verdict === 'SUSPECT') {
                token._garbageSuspect = true;
                token._garbageScore = garbageResult.garbageScore;
                token._garbagePositionMultiplier = garbageResult.positionMultiplier;
                console.log(`⚠️  [GarbageDetector] SUSPECT 信号: ${token.symbol || token.ca} ` +
                    `垃圾分=${garbageResult.garbageScore}，仓位降至 0.5x`);
            }
        }

        // ── 1. 检查是否通过BALANCED ────────────────────────────────────
        const filterResult = applyFilter(token);
        if (!filterResult.pass) {
            return {
                action: 'FILTERED',
                reason: filterResult.reason
            };
        }

        this.stats.analyzed++;

        // 计算链上评分
        const chainScore = this.calculateChainScore(token);

        // 获取AI评分
        let aiScore = 0;
        let xData = null;

        try {
            xData = await this.grokClient.searchToken(
                token.symbol || token.name,
                token.tokenAddress || token.ca,
                15
            );
            aiScore = this.calculateAIScore(xData);
        } catch (error) {
            console.warn(`⚠️ Grok查询失败: ${error.message}`);
            // 失败时使用链上数据做降级判断
            aiScore = this.estimateAIScore(token);
        }

        // 计算最终评分
        const finalScore = this.calculateFinalScore(chainScore, aiScore, token);

        // 预测类别
        const prediction = this.predictCategory(token, chainScore, aiScore, finalScore);

        // 更新统计
        if (prediction === 'GOLD') this.stats.goldPredictions++;
        else if (prediction === 'SILVER') this.stats.silverPredictions++;
        else if (prediction === 'BRONZE_HIGH') this.stats.bronzeHighPredictions++;

        return {
            symbol: token.symbol || token.name,
            ca: token.tokenAddress || token.ca,
            prediction,
            scores: { chain: chainScore, ai: aiScore, final: Math.round(finalScore) },
            position: this.getPositionSize(prediction, token),
            stopLoss: this.getStopLoss(prediction),
            xSummary: xData ? this.summarizeXData(xData) : null,
            action: this.getAction(prediction),
            // 垃圾检测附加信息 (供 analytics 追踪)
            garbageSuspect: token._garbageSuspect ?? false,
            garbageScore: token._garbageScore ?? 0
        };
    }

    /**
     * 优化后的链上评分
     */
    calculateChainScore(token) {
        let score = 0;

        // 信号类型 (提高ACCELERATING权重)
        if (token.signalTrendType === 'ACCELERATING') {
            score += 35; // 从30提高到35
        } else if (token.signalTrendType === 'STABLE') {
            score += 10;
        }

        // SM数量 (优化分布)
        const sm = token.smCount || 0;
        if (sm === 2) {
            score += 25; // SM=2最优
        } else if (sm === 1) {
            score += 18; // 从15提高到18
        } else if (sm === 3) {
            score += 15;
        } else if (sm >= 4 && sm <= 5) {
            score += 10;
        } else if (sm > 5) {
            score += 5;
        }

        // 基础评分
        const baseScore = token.baseScore || 0;
        if (baseScore >= 50 && baseScore < 55) {
            score += 15;
        } else if (baseScore >= 55 && baseScore < 60) {
            score += 12;
        } else if (baseScore >= 60 && baseScore < 65) {
            score += 10;
        } else if (baseScore >= 65) {
            score += 8;
        }

        return score;
    }

    /**
     * AI评分 (基于X/Twitter数据)
     */
    calculateAIScore(xData) {
        if (!xData) return 0;

        let score = 0;

        // 提及数量 (max 20)
        const mentions = xData.mention_count || 0;
        if (mentions >= 50) score += 20;
        else if (mentions >= 20) score += 15;
        else if (mentions >= 10) score += 10;
        else if (mentions >= 5) score += 5;

        // KOL参与 (max 35) - 关键指标，提高权重
        const realKolCount = xData.kol_involvement?.real_kol_count || 0;
        if (realKolCount >= 3) score += 35;
        else if (realKolCount >= 2) score += 28;
        else if (realKolCount >= 1) score += 18;

        // 有机比例 (max 15)
        const organicRatio = xData.bot_detection?.organic_tweet_ratio || 0.5;
        if (organicRatio >= 0.8) score += 15;
        else if (organicRatio >= 0.6) score += 10;
        else if (organicRatio >= 0.4) score += 5;

        // 情绪 (max 10)
        if (xData.sentiment === 'positive') score += 10;
        else if (xData.sentiment === 'neutral') score += 5;

        // 叙事评分 (max 20)
        const narrativeTotal = xData.narrative_score?.total || 0;
        score += Math.min(narrativeTotal * 0.2, 20);

        // 风险扣分
        if (xData.origin_source?.is_authentic === false) {
            score -= 25;
        }
        if ((xData.kol_involvement?.fake_kol_mentions || 0) > 2) {
            score -= 15;
        }

        return Math.max(0, Math.min(100, score));
    }

    /**
     * 估算AI评分 (Grok失败时的降级方案)
     */
    estimateAIScore(token) {
        let score = 30; // 基础分

        // 基于链上数据估算
        if (token.signalTrendType === 'ACCELERATING') {
            score += 15; // 加速信号通常有更好的社交热度
        }

        if (token.smCount >= 3) {
            score += 10; // 多SM通常意味着有讨论
        }

        return score;
    }

    /**
     * 计算最终评分
     */
    calculateFinalScore(chainScore, aiScore, token) {
        // 基础权重: 链上40% + AI 60%
        let chainWeight = 0.4;
        let aiWeight = 0.6;

        // 如果是ACCELERATING信号，增加链上权重
        if (token.signalTrendType === 'ACCELERATING') {
            chainWeight = 0.5;
            aiWeight = 0.5;
        }

        const chainNormalized = Math.min(chainScore / 75 * 100, 100);
        const aiNormalized = Math.min(aiScore / 100 * 100, 100);

        return chainNormalized * chainWeight + aiNormalized * aiWeight;
    }

    /**
     * 预测类别
     */
    predictCategory(token, chainScore, aiScore, finalScore) {
        const t = this.thresholds;

        // GOLD
        if (finalScore >= t.GOLD.finalScore &&
            aiScore >= t.GOLD.aiScore &&
            chainScore >= t.GOLD.chainScore) {
            return 'GOLD';
        }

        // SILVER
        if (finalScore >= t.SILVER.finalScore &&
            (aiScore >= t.SILVER.aiScore || chainScore >= t.SILVER.chainScore + 15)) {
            return 'SILVER';
        }

        // BRONZE_HIGH (ACCELERATING或SM=2的加分)
        if (finalScore >= t.BRONZE_HIGH.finalScore) {
            if (token.signalTrendType === 'ACCELERATING' || token.smCount === 2) {
                return 'BRONZE_HIGH';
            }
        }

        // BRONZE
        if (finalScore >= 35) {
            return 'BRONZE';
        }

        return 'NOISE';
    }

    /**
     * 获取交易动作
     */
    getAction(prediction) {
        switch (prediction) {
            case 'GOLD':
                return 'BUY_FULL';
            case 'SILVER':
                return 'BUY_STANDARD';
            case 'BRONZE_HIGH':
                return 'BUY_SMALL';
            default:
                return 'SKIP';
        }
    }

    /**
     * 获取仓位大小
     */
    getPositionSize(prediction, token) {
        const base = this.riskConfig.positionSizing.basePositionPercent;
        const max = this.riskConfig.positionSizing.maxPositionPercent;
        const signalWeight = this.riskConfig.positionSizing.signalWeights[token.signalTrendType] || 1.0;

        let multiplier;
        switch (prediction) {
            case 'GOLD':
                multiplier = 2.0;
                break;
            case 'SILVER':
                multiplier = 1.2;
                break;
            case 'BRONZE_HIGH':
                multiplier = 0.7;
                break;
            default:
                return 0;
        }

        // 垃圾预警降级: SUSPECT 信号仓位缩减至 0.5x
        const garbageMultiplier = token._garbagePositionMultiplier ?? 1.0;

        return Math.min(base * multiplier * signalWeight * garbageMultiplier, max);
    }

    /**
     * 获取止损比例
     */
    getStopLoss(prediction) {
        switch (prediction) {
            case 'GOLD':
                return 25; // 金狗给更宽止损
            case 'SILVER':
                return 20;
            case 'BRONZE_HIGH':
                return 15;
            default:
                return 15;
        }
    }

    /**
     * 汇总X数据
     */
    summarizeXData(xData) {
        return {
            mentions: xData.mention_count || 0,
            kols: xData.kol_involvement?.real_kol_count || 0,
            sentiment: xData.sentiment || 'unknown',
            organic: ((xData.bot_detection?.organic_tweet_ratio || 0) * 100).toFixed(0) + '%',
            authentic: xData.origin_source?.is_authentic !== false
        };
    }

    /**
     * 批量筛选 - 横向对比
     */
    async filterBatch(tokens) {
        // 先用BALANCED过滤
        const passed = tokens.filter(t => applyFilter(t).pass);

        if (passed.length === 0) {
            return { selected: [], reason: '全部被过滤' };
        }

        // 计算链上评分并排序
        const scored = passed.map(t => ({
            ...t,
            chainScore: this.calculateChainScore(t)
        })).sort((a, b) => b.chainScore - a.chainScore);

        // 取前N个进行AI分析
        const topN = Math.min(5, scored.length);
        const candidates = scored.slice(0, topN);

        // 并行分析
        const results = await Promise.all(
            candidates.map(t => this.analyzeToken(t))
        );

        // 筛选可交易的
        const tradable = results.filter(r =>
            r.action && r.action !== 'SKIP' && r.action !== 'FILTERED'
        );

        // 按评分排序
        tradable.sort((a, b) => b.scores.final - a.scores.final);

        return {
            selected: tradable,
            analyzed: results,
            reason: tradable.length > 0 ? `找到${tradable.length}个候选` : '无合适标的'
        };
    }

    getStats() {
        const total = this.stats.goldPredictions + this.stats.silverPredictions + this.stats.bronzeHighPredictions;
        return {
            ...this.stats,
            totalPredictions: total,
            passRate: this.stats.analyzed > 0 ? (total / this.stats.analyzed * 100).toFixed(1) + '%' : '0%'
        };
    }
}

export default new HighWinRateFilterV2();
