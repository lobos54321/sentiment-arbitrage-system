/**
 * Entry Timing Scorer v1.0
 * 
 * 评估入场时机 - Meme 币交易中最关键的因素
 * 
 * 评分维度：
 * 1. 发币时间 - 越新越好
 * 2. 已涨幅度 - 涨太多就危险
 * 3. 离 ATH 距离 - 接近 ATH 追高风险
 * 4. 价格动量 - 正在拉升还是砸盘
 * 
 * 满分 15 分
 */

export class EntryTimingScorer {
    constructor() {
        // 发币时间评分配置
        this.ageConfig = {
            ultraEarly: { maxMinutes: 30, score: 8 },      // < 30分钟: +8分
            early: { maxMinutes: 120, score: 5 },          // 30分钟-2小时: +5分
            moderate: { maxMinutes: 360, score: 2 },       // 2-6小时: +2分
            old: { maxMinutes: 1440, score: 0 },           // 6-24小时: 0分
            veryOld: { maxMinutes: Infinity, score: -3 }   // > 24小时: -3分
        };

        // 涨幅惩罚配置
        this.gainPenalty = {
            safe: { maxGain: 2, penalty: 0 },         // < 2x: 无惩罚
            moderate: { maxGain: 5, penalty: -2 },    // 2-5x: -2分
            risky: { maxGain: 10, penalty: -5 },      // 5-10x: -5分
            danger: { maxGain: 20, penalty: -8 },     // 10-20x: -8分
            extreme: { maxGain: Infinity, penalty: -12 }  // > 20x: -12分
        };

        // ATH 距离评分
        this.athConfig = {
            atATH: { maxRatio: 0.9, score: -5 },      // 接近 ATH (90%+): -5分
            nearATH: { maxRatio: 0.7, score: -2 },    // 70-90% of ATH: -2分
            pullback: { maxRatio: 0.5, score: 2 },    // 50-70% of ATH: +2分 (回调)
            dip: { maxRatio: 0.3, score: 4 },         // 30-50% of ATH: +4分 (深度回调)
            crash: { maxRatio: 0, score: 1 }          // < 30% of ATH: +1分 (可能死了)
        };

        // 价格动量配置
        this.momentumConfig = {
            pumping: { min5mChange: 0.1, score: 3 },   // 5分钟涨 >10%: +3分
            rising: { min5mChange: 0.03, score: 2 },   // 5分钟涨 3-10%: +2分
            stable: { min5mChange: -0.03, score: 1 },  // ±3%: +1分
            falling: { min5mChange: -0.1, score: -1 }, // 跌 3-10%: -1分
            dumping: { min5mChange: -Infinity, score: -4 } // 跌 >10%: -4分
        };
    }

    /**
     * 计算入场时机评分
     * 
     * @param {Object} token - 代币数据
     * @param {Object} options - 额外选项
     *   - alphaBonus: 来自 Alpha 信号的入场加成
     * @returns {Object} { score, maxScore, details, recommendation }
     */
    score(token, options = {}) {
        const result = {
            score: 0,
            maxScore: 15,
            details: [],
            breakdown: {
                age: 0,
                gain: 0,
                ath: 0,
                momentum: 0,
                alphaBonus: 0
            },
            recommendation: 'NEUTRAL',
            warnings: [],
            isAlphaSignal: false
        };

        // 1. 发币时间评分
        const ageScore = this.scoreAge(token);
        result.breakdown.age = ageScore.score;
        result.score += ageScore.score;
        result.details.push(ageScore.detail);
        if (ageScore.warning) result.warnings.push(ageScore.warning);

        // 2. 已涨幅度评分 (惩罚)
        const gainScore = this.scoreGain(token);
        result.breakdown.gain = gainScore.score;
        result.score += gainScore.score;
        result.details.push(gainScore.detail);
        if (gainScore.warning) result.warnings.push(gainScore.warning);

        // 3. ATH 距离评分
        const athScore = this.scoreATHDistance(token);
        result.breakdown.ath = athScore.score;
        result.score += athScore.score;
        result.details.push(athScore.detail);
        if (athScore.warning) result.warnings.push(athScore.warning);

        // 4. 价格动量评分
        const momentumScore = this.scoreMomentum(token);
        result.breakdown.momentum = momentumScore.score;
        result.score += momentumScore.score;
        result.details.push(momentumScore.detail);

        // 🔥 5. Alpha 信号加成 (来自 @lookonchain 等可信源)
        if (options.alphaBonus && options.alphaBonus > 0) {
            result.breakdown.alphaBonus = options.alphaBonus;
            result.score += options.alphaBonus;
            result.details.push(`Alpha加成: +${options.alphaBonus} (@${options.alphaSource || 'unknown'})`);
            result.isAlphaSignal = true;
            console.log(`🎯 [入场时机] Alpha 信号加成 +${options.alphaBonus} 来自 @${options.alphaSource}`);
        }

        // 限制分数范围 [0, 15]
        result.score = Math.max(0, Math.min(result.score, result.maxScore));

        // 生成建议
        result.recommendation = this.getRecommendation(result);

        return result;
    }

    /**
     * 发币时间评分
     */
    scoreAge(token) {
        // 尝试从多个来源获取发币时间
        let creationTime = null;

        if (token.creation_timestamp) {
            creationTime = new Date(token.creation_timestamp * 1000);
        } else if (token.firstTime) {
            creationTime = new Date(token.firstTime);
        } else if (token.launchTime) {
            creationTime = new Date(token.launchTime);
        }

        if (!creationTime) {
            return {
                score: 0,
                detail: '发币时间: 未知',
                warning: null
            };
        }

        const ageMinutes = (Date.now() - creationTime.getTime()) / 1000 / 60;

        for (const [level, config] of Object.entries(this.ageConfig)) {
            if (ageMinutes < config.maxMinutes) {
                const timeStr = this.formatAge(ageMinutes);
                let warning = null;

                if (level === 'ultraEarly') {
                    warning = '🔥 超早期代币，高风险高回报';
                } else if (level === 'veryOld') {
                    warning = '⚠️ 代币已发行超过24小时，需谨慎';
                }

                return {
                    score: config.score,
                    detail: `发币时间: ${timeStr} (${config.score >= 0 ? '+' : ''}${config.score})`,
                    warning
                };
            }
        }

        return { score: 0, detail: '发币时间: 未知', warning: null };
    }

    /**
     * 已涨幅度评分 (惩罚)
     */
    scoreGain(token) {
        // 计算从首次价格到现在的涨幅
        const currentPrice = token.price || token.currentPrice || 0;
        const firstPrice = token.firstPrice || token.first_price || 0;

        if (!currentPrice || !firstPrice || firstPrice <= 0) {
            return { score: 0, detail: '涨幅: 未知', warning: null };
        }

        const gain = currentPrice / firstPrice;

        for (const [level, config] of Object.entries(this.gainPenalty)) {
            if (gain < config.maxGain) {
                let warning = null;

                if (level === 'danger' || level === 'extreme') {
                    warning = `⚠️ 已涨 ${gain.toFixed(1)}x，追高风险极高`;
                }

                return {
                    score: config.penalty,
                    detail: `已涨: ${gain.toFixed(1)}x (${config.penalty >= 0 ? '+' : ''}${config.penalty})`,
                    warning
                };
            }
        }

        return { score: -12, detail: '已涨: 极高', warning: '🚨 已涨幅过大，强烈不建议追高' };
    }

    /**
     * ATH 距离评分
     */
    scoreATHDistance(token) {
        const currentPrice = token.price || token.currentPrice || 0;
        const athPrice = token.maxPrice || token.ath_price || token.max_price || 0;

        if (!currentPrice || !athPrice || athPrice <= 0) {
            return { score: 0, detail: 'ATH距离: 未知', warning: null };
        }

        const athRatio = currentPrice / athPrice;

        for (const [level, config] of Object.entries(this.athConfig)) {
            if (athRatio > config.maxRatio) {
                let warning = null;

                if (level === 'atATH') {
                    warning = '⚠️ 接近历史最高价，追高风险';
                }

                const percentage = (athRatio * 100).toFixed(0);
                return {
                    score: config.score,
                    detail: `离ATH: ${percentage}% (${config.score >= 0 ? '+' : ''}${config.score})`,
                    warning
                };
            }
        }

        return { score: 1, detail: '离ATH: 很远', warning: null };
    }

    /**
     * 价格动量评分
     */
    scoreMomentum(token) {
        // 获取 5 分钟涨跌幅
        const change5m = token.change5m || token.percent_5m || token.priceChange5m || 0;

        for (const [level, config] of Object.entries(this.momentumConfig)) {
            if (change5m > config.min5mChange) {
                const changeStr = (change5m * 100).toFixed(1);
                const emoji = change5m > 0 ? '📈' : (change5m < -0.05 ? '📉' : '➡️');

                return {
                    score: config.score,
                    detail: `动量: ${emoji}${changeStr}% (${config.score >= 0 ? '+' : ''}${config.score})`
                };
            }
        }

        return { score: -4, detail: '动量: 暴跌中', warning: '🚨 价格正在暴跌' };
    }

    /**
     * 综合建议
     */
    getRecommendation(result) {
        if (result.score >= 12) {
            return 'EXCELLENT_ENTRY';  // 绝佳入场点
        } else if (result.score >= 8) {
            return 'GOOD_ENTRY';       // 好入场点
        } else if (result.score >= 4) {
            return 'MODERATE_ENTRY';   // 一般入场点
        } else if (result.score >= 0) {
            return 'LATE_ENTRY';       // 偏晚入场
        } else {
            return 'TOO_LATE';         // 太晚了
        }
    }

    /**
     * 格式化时间
     */
    formatAge(minutes) {
        if (minutes < 60) {
            return `${Math.floor(minutes)}分钟`;
        } else if (minutes < 1440) {
            return `${(minutes / 60).toFixed(1)}小时`;
        } else {
            return `${(minutes / 1440).toFixed(1)}天`;
        }
    }
}

export default EntryTimingScorer;
