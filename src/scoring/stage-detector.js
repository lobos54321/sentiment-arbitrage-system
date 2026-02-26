/**
 * Stage Detector
 * 
 * 检测代币所处的生命周期阶段
 * 根据阶段返回不同的链上活跃度评分标准
 * 
 * 阶段:
 * - 🐣 birth: MC < $30K, Age < 30min
 * - 🌱 growth: $30K-$150K, 30min-3hr
 * - 🔥 explosion: $150K-$1M, 3hr-24hr
 * - 🏆 mature: > $1M, > 24hr
 */

export class StageDetector {
    constructor(config) {
        this.config = config;

        // 阶段阈值配置
        this.stageConfig = {
            birth: {
                maxMarketCap: 30000,      // < $30K
                maxAgeMinutes: 30,         // < 30 min
                emoji: '🐣',
                name: 'birth',
                nameZh: '出生期'
            },
            growth: {
                maxMarketCap: 150000,     // < $150K
                maxAgeMinutes: 180,        // < 3 hr
                emoji: '🌱',
                name: 'growth',
                nameZh: '萌芽期'
            },
            explosion: {
                maxMarketCap: 1000000,    // < $1M
                maxAgeMinutes: 1440,       // < 24 hr
                emoji: '🔥',
                name: 'explosion',
                nameZh: '爆发期'
            },
            mature: {
                maxMarketCap: Infinity,
                maxAgeMinutes: Infinity,
                emoji: '🏆',
                name: 'mature',
                nameZh: '成熟期'
            }
        };
    }

    /**
     * 检测代币所处阶段
     * 
     * @param {Object} tokenData - 代币数据
     * @returns {Object} { stage, emoji, scores }
     */
    detect(tokenData) {
        const marketCap = tokenData.marketCap || tokenData.market_cap || 0;
        const ageMinutes = this.calculateAgeMinutes(tokenData);
        const volume24h = tokenData.volume24h || tokenData.volume_24h || 0;
        const holderCount = tokenData.holderCount || tokenData.holders || 0;

        // 确定阶段
        let stage = 'mature';

        if (marketCap < this.stageConfig.birth.maxMarketCap &&
            ageMinutes < this.stageConfig.birth.maxAgeMinutes) {
            stage = 'birth';
        } else if (marketCap < this.stageConfig.growth.maxMarketCap &&
            ageMinutes < this.stageConfig.growth.maxAgeMinutes) {
            stage = 'growth';
        } else if (marketCap < this.stageConfig.explosion.maxMarketCap &&
            ageMinutes < this.stageConfig.explosion.maxAgeMinutes) {
            stage = 'explosion';
        }

        const config = this.stageConfig[stage];

        // 计算该阶段的链上活跃度评分
        const chainActivityScore = this.calculateChainActivityScore(
            stage,
            tokenData
        );

        return {
            stage: config.name,
            stageZh: config.nameZh,
            emoji: config.emoji,
            marketCap,
            ageMinutes,
            chainActivityScore,
            details: chainActivityScore.details
        };
    }

    /**
     * 计算代币年龄 (分钟)
     */
    calculateAgeMinutes(tokenData) {
        // 从多个可能的字段获取创建时间
        const createdAt = tokenData.creationTimestamp ||
            tokenData.creation_timestamp ||
            tokenData.created_at ||
            tokenData.first_seen_at;

        if (!createdAt) {
            return 60; // 默认 1 小时
        }

        // Unix 时间戳 (秒或毫秒)
        let timestamp = createdAt;
        if (timestamp > 1e12) {
            timestamp = timestamp / 1000; // 毫秒转秒
        }

        const now = Date.now() / 1000;
        const ageSeconds = now - timestamp;

        return Math.max(0, ageSeconds / 60);
    }

    /**
     * 根据阶段计算链上活跃度评分 (0-5分)
     */
    calculateChainActivityScore(stage, tokenData) {
        const result = {
            score: 0,
            maxScore: 5,
            details: []
        };

        const volume24h = tokenData.volume24h || tokenData.volume_24h || 0;
        const holderCount = tokenData.holderCount || tokenData.holders || 0;
        const txns = tokenData.txns24h || tokenData.txns_24h || 0;
        const buys = tokenData.buys || 0;
        const sells = tokenData.sells || 0;
        const smartWallets = tokenData.smartWalletOnline || tokenData.smart_wallets || 0;

        switch (stage) {
            case 'birth':
                // 🐣 出生期: 重点看买卖比、交易活跃度
                result.score += this.scoreBuySellRatio(buys, sells, 2);
                result.score += this.scoreTransactionActivity(txns, 50, 1.5);
                result.score += this.scoreSmartMoney(smartWallets, 3, 1.5);
                result.details.push(`出生期评分: 买卖比+交易数+聪明钱`);
                break;

            case 'growth':
                // 🌱 萌芽期: 看交易量、持有人增长
                result.score += this.scoreVolume(volume24h, 30000, 2);
                result.score += this.scoreHolders(holderCount, 100, 1.5);
                result.score += this.scoreSmartMoney(smartWallets, 5, 1.5);
                result.details.push(`萌芽期评分: 交易量+持有人+聪明钱`);
                break;

            case 'explosion':
                // 🔥 爆发期: 看交易量、聪明钱持仓
                result.score += this.scoreVolume(volume24h, 100000, 2);
                result.score += this.scoreSmartMoney(smartWallets, 8, 2);
                result.score += this.scoreHolders(holderCount, 300, 1);
                result.details.push(`爆发期评分: 交易量+聪明钱+持有人`);
                break;

            case 'mature':
                // 🏆 成熟期: 看交易量持续性、聪明钱动向
                result.score += this.scoreVolume(volume24h, 200000, 2.5);
                result.score += this.scoreSmartMoney(smartWallets, 10, 2.5);
                result.details.push(`成熟期评分: 交易量+聪明钱动向`);
                break;
        }

        // 限制最大分数
        result.score = Math.min(result.score, result.maxScore);

        return result;
    }

    /**
     * 评分: 买卖比
     */
    scoreBuySellRatio(buys, sells, maxPoints) {
        if (sells === 0) sells = 1;
        const ratio = buys / sells;

        if (ratio >= 3) return maxPoints;        // 3:1 买压
        if (ratio >= 2) return maxPoints * 0.8;  // 2:1 买压
        if (ratio >= 1.5) return maxPoints * 0.6;
        if (ratio >= 1) return maxPoints * 0.3;
        return 0; // 卖压大于买压
    }

    /**
     * 评分: 交易活跃度
     */
    scoreTransactionActivity(txns, threshold, maxPoints) {
        if (txns >= threshold * 2) return maxPoints;
        if (txns >= threshold) return maxPoints * 0.7;
        if (txns >= threshold * 0.5) return maxPoints * 0.4;
        return 0;
    }

    /**
     * 评分: 交易量
     */
    scoreVolume(volume, threshold, maxPoints) {
        if (volume >= threshold * 2) return maxPoints;
        if (volume >= threshold) return maxPoints * 0.7;
        if (volume >= threshold * 0.5) return maxPoints * 0.4;
        if (volume >= threshold * 0.2) return maxPoints * 0.2;
        return 0;
    }

    /**
     * 评分: 持有人数
     */
    scoreHolders(holders, threshold, maxPoints) {
        if (holders >= threshold * 3) return maxPoints;
        if (holders >= threshold * 2) return maxPoints * 0.8;
        if (holders >= threshold) return maxPoints * 0.5;
        if (holders >= threshold * 0.5) return maxPoints * 0.2;
        return 0;
    }

    /**
     * 评分: 聪明钱
     */
    scoreSmartMoney(smartWallets, threshold, maxPoints) {
        if (smartWallets >= threshold) return maxPoints;
        if (smartWallets >= threshold * 0.75) return maxPoints * 0.8;
        if (smartWallets >= threshold * 0.5) return maxPoints * 0.5;
        if (smartWallets >= threshold * 0.25) return maxPoints * 0.25;
        return 0;
    }

    /**
     * 获取阶段建议的 X 热度阈值
     */
    getExpectedXHeat(stage) {
        switch (stage) {
            case 'birth':
                return { min: 0, expected: 1, good: 5 };
            case 'growth':
                return { min: 3, expected: 10, good: 25 };
            case 'explosion':
                return { min: 15, expected: 40, good: 80 };
            case 'mature':
                return { min: 30, expected: 80, good: 150 };
            default:
                return { min: 0, expected: 10, good: 30 };
        }
    }
}

export default StageDetector;
