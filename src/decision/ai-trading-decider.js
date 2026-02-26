/**
 * AI辅助交易决策器 v2.0
 *
 * 工作流程:
 * 1. BALANCED过滤 → 筛选出候选代币
 * 2. AI分析 → 判断叙事和入场时机
 * 3. 仓位计算 → 基于信号类型和AI置信度
 * 4. 执行/跳过
 *
 * 设计目标:
 * - 不漏掉金狗（BALANCED保证82%通过）
 * - AI提升精确率（从5.4%提升到10%+）
 * - 动态仓位控制风险
 */

import { applyFilter, getActiveFilterParams, FILTER_PARAMS } from '../config/filter-params.js';
import { generateBuyDecisionPrompt, generateStopLossPrompt, generateTakeProfitPrompt } from '../prompts/ai-trading-prompts-v2.js';
import AIAnalyst from '../utils/ai-analyst.js';

class AITradingDecider {
    constructor() {
        this.riskConfig = FILTER_PARAMS.RISK_CONTROL;
        this.stats = {
            tokensAnalyzed: 0,
            aiCalls: 0,
            buyDecisions: 0,
            skipDecisions: 0,
            errors: 0
        };
    }

    /**
     * 完整的买入决策流程
     * @param {Object} token - 代币数据
     * @returns {Object} 决策结果
     */
    async decide(token) {
        this.stats.tokensAnalyzed++;

        // 第一层: BALANCED过滤
        const filterResult = applyFilter(token);
        if (!filterResult.pass) {
            return {
                action: 'FILTERED',
                reason: filterResult.reason,
                stage: 'BALANCED_FILTER'
            };
        }

        // 第二层: AI分析
        const aiDecision = await this.getAIDecision(token);
        if (aiDecision.error) {
            // AI故障时，根据链上数据降级决策
            return this.fallbackDecision(token);
        }

        // 第三层: 最终决策
        return this.finalizeDecision(token, aiDecision);
    }

    /**
     * 获取AI决策 (带重试)
     * v9.5: AI故障时重试，不再使用fallback自动交易
     */
    async getAIDecision(token, retryCount = 0) {
        const MAX_RETRIES = 2;
        const RETRY_DELAY_MS = 1000;

        this.stats.aiCalls++;

        try {
            const prompt = generateBuyDecisionPrompt(token, {
                maxPositionPercent: this.riskConfig.positionSizing.maxPositionPercent,
                basePositionPercent: this.riskConfig.positionSizing.basePositionPercent,
                stopLossPercent: this.riskConfig.stopLossPercent
            });

            const result = await AIAnalyst.analyze(prompt);

            if (result.error) {
                this.stats.errors++;
                // 重试逻辑
                if (retryCount < MAX_RETRIES) {
                    console.log(`⚠️ AI决策失败，${RETRY_DELAY_MS}ms后重试 (${retryCount + 1}/${MAX_RETRIES})...`);
                    await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS));
                    return this.getAIDecision(token, retryCount + 1);
                }
                return { error: true, reason: result.reason };
            }

            return {
                action: result.action || 'SKIP',
                positionPercent: result.position_percent || 5,
                narrativeTier: result.narrative_tier || 'C',
                narrativeReason: result.narrative_reason || '',
                entryTiming: result.entry_timing || 'OPTIMAL',
                targetMcap: result.target_mcap || null,
                stopLossPercent: result.stop_loss_percent || this.riskConfig.stopLossPercent,
                confidence: result.confidence || 50,
                riskFlags: result.risk_flags || [],
                raw: result.raw
            };

        } catch (error) {
            this.stats.errors++;
            // 重试逻辑
            if (retryCount < MAX_RETRIES) {
                console.log(`⚠️ AI决策异常: ${error.message}，${RETRY_DELAY_MS}ms后重试 (${retryCount + 1}/${MAX_RETRIES})...`);
                await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS));
                return this.getAIDecision(token, retryCount + 1);
            }
            console.error(`❌ AI决策失败 (已重试${MAX_RETRIES}次): ${error.message}`);
            return { error: true, reason: error.message };
        }
    }

    /**
     * AI故障时的降级决策
     * v9.5: 不再自动交易，全部跳过
     */
    fallbackDecision(token) {
        console.log(`🚫 AI故障，跳过交易: ${token.symbol || token.address}`);
        return {
            action: 'SKIP',
            reason: 'AI故障: 重试失败，安全跳过',
            stage: 'FALLBACK'
        };
    }

    /**
     * 最终决策整合
     */
    finalizeDecision(token, aiDecision) {
        if (aiDecision.action === 'SKIP') {
            this.stats.skipDecisions++;
            return {
                action: 'SKIP',
                reason: aiDecision.narrativeReason || 'AI建议跳过',
                narrativeTier: aiDecision.narrativeTier,
                confidence: aiDecision.confidence,
                riskFlags: aiDecision.riskFlags,
                stage: 'AI_DECISION'
            };
        }

        // 计算最终仓位
        let positionPercent = aiDecision.positionPercent;

        // 应用信号权重
        const signalWeight = this.riskConfig.positionSizing.signalWeights[token.signalTrendType] || 1.0;
        positionPercent *= signalWeight;

        // 应用SM加成
        if (token.smCount >= this.riskConfig.positionSizing.smCountBonus.threshold) {
            positionPercent *= this.riskConfig.positionSizing.smCountBonus.bonusMultiplier;
        }

        // 应用评分加成
        if (token.baseScore >= this.riskConfig.positionSizing.scoreBonus.threshold) {
            positionPercent *= this.riskConfig.positionSizing.scoreBonus.bonusMultiplier;
        }

        // 确保不超过上限
        positionPercent = Math.min(positionPercent, this.riskConfig.positionSizing.maxPositionPercent);

        // 根据置信度调整
        if (aiDecision.confidence < 50) {
            positionPercent *= 0.5; // 低置信度减半
        }

        this.stats.buyDecisions++;

        return {
            action: aiDecision.action,
            positionPercent: Math.round(positionPercent * 10) / 10,
            narrativeTier: aiDecision.narrativeTier,
            narrativeReason: aiDecision.narrativeReason,
            entryTiming: aiDecision.entryTiming,
            targetMcap: aiDecision.targetMcap,
            stopLossPercent: aiDecision.stopLossPercent,
            confidence: aiDecision.confidence,
            riskFlags: aiDecision.riskFlags,
            stage: 'AI_DECISION',
            tokenData: {
                symbol: token.symbol,
                signalTrendType: token.signalTrendType,
                smCount: token.smCount,
                baseScore: token.baseScore
            }
        };
    }

    /**
     * 止损确认
     */
    async shouldStopLoss(position, currentData) {
        try {
            const prompt = generateStopLossPrompt(position, currentData);
            const result = await AIAnalyst.analyze(prompt);

            return {
                action: result.action === 'HOLD' ? 'HOLD' : 'SELL',
                reason: result.reason || result.narrative_reason || '未知',
                confidence: result.confidence || 50
            };
        } catch (error) {
            // AI故障时默认卖出
            return {
                action: 'SELL',
                reason: `AI故障: ${error.message}`,
                confidence: 0
            };
        }
    }

    /**
     * 止盈判断
     */
    async shouldTakeProfit(position, currentData) {
        try {
            const prompt = generateTakeProfitPrompt(position, currentData);
            const result = await AIAnalyst.analyze(prompt);

            return {
                action: result.action || 'HOLD',
                sellPercent: result.sell_percent || 0,
                reason: result.reason || result.narrative_reason || '未知',
                newStopLoss: result.new_stop_loss
            };
        } catch (error) {
            // AI故障时使用默认阶梯
            const pnlPercent = currentData.pnlPercent || 0;
            if (pnlPercent >= 100) {
                return { action: 'TAKE_PROFIT', sellPercent: 30, reason: '翻倍止盈' };
            }
            if (pnlPercent >= 50) {
                return { action: 'TAKE_PROFIT', sellPercent: 20, reason: '+50%止盈' };
            }
            return { action: 'HOLD', sellPercent: 0, reason: '继续持有' };
        }
    }

    /**
     * 获取统计信息
     */
    getStats() {
        return {
            ...this.stats,
            buyRate: this.stats.tokensAnalyzed > 0
                ? (this.stats.buyDecisions / this.stats.tokensAnalyzed * 100).toFixed(1) + '%'
                : '0%',
            errorRate: this.stats.aiCalls > 0
                ? (this.stats.errors / this.stats.aiCalls * 100).toFixed(1) + '%'
                : '0%'
        };
    }

    /**
     * 重置统计
     */
    resetStats() {
        this.stats = {
            tokensAnalyzed: 0,
            aiCalls: 0,
            buyDecisions: 0,
            skipDecisions: 0,
            errors: 0
        };
    }
}

export default new AITradingDecider();
