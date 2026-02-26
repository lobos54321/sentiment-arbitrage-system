/**
 * Smart Exit Engine v7.1
 *
 * 智能退出引擎 - 基于多维度信号的动态退出决策
 * v7.1: 新增 AI 辅助退出判断
 *
 * 核心理念:
 * - 不要单纯依赖固定止盈止损
 * - 结合市场结构、聪明钱动向、社交热度判断最佳退出时机
 * - 早期高波动期放宽止损，稳定期收紧保护
 * - AI 分析复杂边界情况
 *
 * 信号维度:
 * 1. 价格动量 - 上涨/下跌速度、K线形态
 * 2. 聪明钱 - 流入流出、持仓变化
 * 3. 流动性 - 深度变化、滑点监控
 * 4. 社交热度 - TG提及量、情绪变化
 * 5. 市场结构 - Top10变化、新旧钱包比例
 * 6. AI 判断 - 边界情况分析 (v7.1)
 */

import { EventEmitter } from 'events';

export class SmartExitEngine extends EventEmitter {
    constructor(config = {}) {
        super();

        // v7.1 AI client injection
        this.aiClient = config.aiClient || null;

        this.config = {
            // 基础配置
            enabled: config.enabled !== false,
            checkIntervalMs: config.checkIntervalMs || 30000, // 30秒检查一次

            // ═══════════════════════════════════════════════════════════════
            // 1. 分阶段止损 (v8.0: 已禁用，改用叙事层级止损)
            // 由 position-monitor-v2 根据 NARRATIVE_STOP_LOSS 配置处理
            // ═══════════════════════════════════════════════════════════════
            phasedStopLoss: {
                // v8.0: 禁用分阶段止损，设置为极宽松值不触发
                PHASE_1: {
                    maxMinutes: 10,
                    stopLoss: -0.99,      // 禁用：由叙事层级控制
                    description: '已禁用'
                },
                PHASE_2: {
                    maxMinutes: 30,
                    stopLoss: -0.99,
                    description: '已禁用'
                },
                PHASE_3: {
                    maxMinutes: 60,
                    stopLoss: -0.99,
                    description: '已禁用'
                },
                PHASE_4: {
                    maxMinutes: Infinity,
                    stopLoss: -0.99,
                    description: '已禁用'
                }
            },

            // ═══════════════════════════════════════════════════════════════
            // 2. 动量退出 (价格/成交量加速度)
            // ═══════════════════════════════════════════════════════════════
            momentumExit: {
                enabled: true,
                // 价格急跌触发
                priceDropAlert: -0.10,    // 1分钟内跌10%触发警报
                priceDropExit: -0.20,     // 1分钟内跌20%强制退出
                // 成交量异常
                volumeSpike: 5.0,         // 成交量突增5倍 = 异常
                volumeDry: 0.2,           // 成交量萎缩到20% = 流动性枯竭
            },

            // ═══════════════════════════════════════════════════════════════
            // 3. 聪明钱跟踪退出
            // ═══════════════════════════════════════════════════════════════
            smartMoneyExit: {
                enabled: true,
                // 绝对数量阈值
                minToHold: 2,             // 聪明钱 < 2 个考虑退出
                // 相对变化阈值
                dropRateAlert: 0.30,      // 减少30%触发警报
                dropRateExit: 0.50,       // 减少50%强制退出
                // 反向指标: 聪明钱加仓
                inflowHoldBonus: true,    // 聪明钱流入时延长持仓
            },

            // ═══════════════════════════════════════════════════════════════
            // 4. 流动性保护
            // ═══════════════════════════════════════════════════════════════
            liquidityExit: {
                enabled: true,
                // 流动性阈值
                minLiquidityUsd: 5000,    // 流动性 < $5k 强制退出
                dropRateAlert: 0.30,      // 流动性减少30%警报
                dropRateExit: 0.50,       // 流动性减少50%退出
                // 滑点监控
                maxSlippageBps: 500,      // 滑点 > 5% 退出
            },

            // ═══════════════════════════════════════════════════════════════
            // 5. 社交热度衰减
            // ═══════════════════════════════════════════════════════════════
            socialDecay: {
                enabled: true,
                // 热度衰减退出
                decayThreshold: 0.30,     // 热度衰减到30%以下退出
                // 负面情绪
                negativeSentimentExit: true,
            },

            // ═══════════════════════════════════════════════════════════════
            // 6. 综合评分退出
            // 当多个信号同时恶化时触发
            // ═══════════════════════════════════════════════════════════════
            compositeExit: {
                enabled: true,
                // 信号权重
                weights: {
                    momentum: 0.25,
                    smartMoney: 0.30,
                    liquidity: 0.25,
                    social: 0.20
                },
                // 综合得分阈值
                alertThreshold: 0.40,     // < 40% 警报
                exitThreshold: 0.25,      // < 25% 退出
            },

            // ═══════════════════════════════════════════════════════════════
            // 7. 保护性止盈 (利润保护)
            // ═══════════════════════════════════════════════════════════════
            profitProtection: {
                enabled: true,
                tiers: [
                    { profit: 0.50, protect: 0.20 },  // 赚50%后，保护20%利润
                    { profit: 1.00, protect: 0.50 },  // 翻倍后，保护50%利润
                    { profit: 2.00, protect: 0.70 },  // 3倍后，保护70%利润
                    { profit: 5.00, protect: 0.80 },  // 6倍后，保护80%利润
                ]
            },

            // ═══════════════════════════════════════════════════════════════
            // 8. v7.1 AI 辅助退出判断
            // 用于分析边界情况和复杂市场状态
            // ═══════════════════════════════════════════════════════════════
            aiExit: {
                enabled: config.aiExitEnabled !== false,
                // 触发 AI 分析的条件
                triggerConditions: {
                    // 综合得分在模糊区间
                    scoreRange: { min: 0.25, max: 0.50 },
                    // 持仓时间超过阈值
                    minHoldingMinutes: 15,
                    // 盈利在模糊区间
                    profitRange: { min: -0.15, max: 0.30 }
                },
                // AI 调用限制
                cooldownMs: 60000,  // 同一持仓60秒内只调用一次
                timeoutMs: 10000    // AI 超时10秒
            }
        };

        // 持仓状态缓存
        this.positionStates = new Map();

        // v7.1 AI 调用冷却追踪
        this.aiCooldowns = new Map();

        console.log('[SmartExit] ⚡ Smart Exit Engine v7.1 初始化');
        console.log(`[SmartExit] 分阶段止损: ${Object.keys(this.config.phasedStopLoss).length} 个阶段`);
        console.log(`[SmartExit] 信号维度: 动量 + 聪明钱 + 流动性 + 社交 + AI`);
        console.log(`[SmartExit] AI 退出: ${this.config.aiExit.enabled ? '已启用' : '已禁用'}`);
    }

    /**
     * 获取分阶段止损阈值
     * @param {number} holdingMinutes - 持仓时间(分钟)
     * @returns {Object} { stopLoss, phase, description }
     */
    getPhasedStopLoss(holdingMinutes) {
        const phases = this.config.phasedStopLoss;

        if (holdingMinutes <= phases.PHASE_1.maxMinutes) {
            return { ...phases.PHASE_1, phase: 1 };
        } else if (holdingMinutes <= phases.PHASE_2.maxMinutes) {
            return { ...phases.PHASE_2, phase: 2 };
        } else if (holdingMinutes <= phases.PHASE_3.maxMinutes) {
            return { ...phases.PHASE_3, phase: 3 };
        } else {
            return { ...phases.PHASE_4, phase: 4 };
        }
    }

    /**
     * 计算动量得分
     * @param {Object} current - 当前数据
     * @param {Object} previous - 上一次数据
     * @returns {Object} { score, signals }
     */
    calculateMomentumScore(current, previous) {
        const signals = [];
        let score = 1.0; // 满分开始

        if (!previous) {
            return { score: 1.0, signals: ['无历史数据'] };
        }

        // 价格变化率 (1分钟)
        const priceChange = previous.price > 0
            ? (current.price - previous.price) / previous.price
            : 0;

        if (priceChange <= this.config.momentumExit.priceDropExit) {
            score = 0; // 急跌强制退出
            signals.push(`🚨 价格急跌 ${(priceChange * 100).toFixed(1)}%`);
        } else if (priceChange <= this.config.momentumExit.priceDropAlert) {
            score *= 0.5;
            signals.push(`⚠️ 价格下跌 ${(priceChange * 100).toFixed(1)}%`);
        } else if (priceChange > 0.05) {
            score = Math.min(1.5, score * 1.2); // 上涨加分
            signals.push(`📈 价格上涨 +${(priceChange * 100).toFixed(1)}%`);
        }

        // 成交量变化
        const volumeRatio = previous.volume > 0
            ? current.volume / previous.volume
            : 1;

        if (volumeRatio >= this.config.momentumExit.volumeSpike && priceChange < 0) {
            score *= 0.6; // 放量下跌，危险信号
            signals.push(`⚠️ 放量下跌 (Vol x${volumeRatio.toFixed(1)})`);
        } else if (volumeRatio <= this.config.momentumExit.volumeDry) {
            score *= 0.7; // 量能萎缩
            signals.push(`⚠️ 量能萎缩 (Vol ${(volumeRatio * 100).toFixed(0)}%)`);
        }

        return { score: Math.max(0, Math.min(1.5, score)), signals };
    }

    /**
     * 计算聪明钱得分
     * @param {Object} current - 当前聪明钱数据
     * @param {Object} entry - 入场时聪明钱数据
     * @returns {Object} { score, signals }
     */
    calculateSmartMoneyScore(current, entry) {
        const signals = [];
        let score = 1.0;

        const currentSM = current.smartMoneyCount || 0;
        const entrySM = entry.smartMoneyCount || 0;

        // 绝对数量检查
        if (currentSM < this.config.smartMoneyExit.minToHold) {
            score *= 0.3;
            signals.push(`🐋 聪明钱仅剩 ${currentSM} 个`);
        }

        // 相对变化检查
        if (entrySM > 0) {
            const dropRate = (entrySM - currentSM) / entrySM;

            if (dropRate >= this.config.smartMoneyExit.dropRateExit) {
                score = 0.1; // 强制退出
                signals.push(`🚨 聪明钱大撤退 -${(dropRate * 100).toFixed(0)}%`);
            } else if (dropRate >= this.config.smartMoneyExit.dropRateAlert) {
                score *= 0.5;
                signals.push(`⚠️ 聪明钱减少 -${(dropRate * 100).toFixed(0)}%`);
            } else if (dropRate < 0) {
                // 聪明钱增加
                const inflowRate = Math.abs(dropRate);
                score = Math.min(1.5, score * (1 + inflowRate * 0.5));
                signals.push(`🐋 聪明钱流入 +${(inflowRate * 100).toFixed(0)}%`);
            }
        }

        return { score: Math.max(0, Math.min(1.5, score)), signals };
    }

    /**
     * 计算流动性得分
     * @param {Object} current - 当前流动性数据
     * @param {Object} entry - 入场时流动性数据
     * @returns {Object} { score, signals }
     */
    calculateLiquidityScore(current, entry) {
        const signals = [];
        let score = 1.0;

        const currentLiq = current.liquidity || 0;
        const entryLiq = entry.liquidity || currentLiq;

        // 绝对值检查
        if (currentLiq < this.config.liquidityExit.minLiquidityUsd) {
            score = 0.1;
            signals.push(`🚨 流动性不足 $${(currentLiq / 1000).toFixed(1)}K`);
        }

        // 相对变化检查
        if (entryLiq > 0) {
            const dropRate = (entryLiq - currentLiq) / entryLiq;

            if (dropRate >= this.config.liquidityExit.dropRateExit) {
                score = 0.1;
                signals.push(`🚨 流动性崩溃 -${(dropRate * 100).toFixed(0)}%`);
            } else if (dropRate >= this.config.liquidityExit.dropRateAlert) {
                score *= 0.5;
                signals.push(`⚠️ 流动性下降 -${(dropRate * 100).toFixed(0)}%`);
            } else if (dropRate < -0.2) {
                // 流动性增加
                score = Math.min(1.3, score * 1.1);
                signals.push(`✅ 流动性增加`);
            }
        }

        // 滑点检查
        const slippage = current.slippageBps || 0;
        if (slippage > this.config.liquidityExit.maxSlippageBps) {
            score *= 0.4;
            signals.push(`⚠️ 滑点过高 ${(slippage / 100).toFixed(1)}%`);
        }

        return { score: Math.max(0, Math.min(1.5, score)), signals };
    }

    /**
     * 计算社交热度得分
     * @param {Object} current - 当前社交数据
     * @param {Object} entry - 入场时社交数据
     * @returns {Object} { score, signals }
     */
    calculateSocialScore(current, entry) {
        const signals = [];
        let score = 1.0;

        const currentHeat = current.socialHeat || current.tgMentions || 0;
        const entryHeat = entry.socialHeat || entry.tgMentions || 1;

        // 热度衰减
        const heatRatio = entryHeat > 0 ? currentHeat / entryHeat : 1;

        if (heatRatio < this.config.socialDecay.decayThreshold) {
            score *= 0.5;
            signals.push(`📉 热度衰减 ${(heatRatio * 100).toFixed(0)}%`);
        } else if (heatRatio > 2) {
            score = Math.min(1.5, score * 1.2);
            signals.push(`🔥 热度上升 x${heatRatio.toFixed(1)}`);
        }

        // 情绪检查 (如有)
        if (current.sentiment === 'negative' && this.config.socialDecay.negativeSentimentExit) {
            score *= 0.6;
            signals.push(`⚠️ 负面情绪`);
        }

        return { score: Math.max(0, Math.min(1.5, score)), signals };
    }

    /**
     * 计算综合退出得分
     * @param {Object} position - 持仓数据
     * @param {Object} currentData - 当前市场数据
     * @returns {Object} { score, breakdown, signals, decision }
     */
    calculateCompositeScore(position, currentData) {
        const entryData = {
            smartMoneyCount: position.entry_smart_money || 0,
            liquidity: position.entry_liquidity_usd || 0,
            socialHeat: position.entry_tg_accel || 1,
            tgMentions: position.entry_tg_accel || 1
        };

        const previousData = this.positionStates.get(position.token_ca) || {
            price: position.entry_price,
            volume: 0,
            timestamp: new Date(position.entry_time).getTime()
        };

        // 计算各维度得分
        const momentum = this.calculateMomentumScore(currentData, previousData);
        const smartMoney = this.calculateSmartMoneyScore(currentData, entryData);
        const liquidity = this.calculateLiquidityScore(currentData, entryData);
        const social = this.calculateSocialScore(currentData, entryData);

        // 加权综合得分
        const weights = this.config.compositeExit.weights;
        const compositeScore =
            momentum.score * weights.momentum +
            smartMoney.score * weights.smartMoney +
            liquidity.score * weights.liquidity +
            social.score * weights.social;

        // 收集所有信号
        const allSignals = [
            ...momentum.signals,
            ...smartMoney.signals,
            ...liquidity.signals,
            ...social.signals
        ];

        // 决策
        let decision = 'HOLD';
        let urgency = 'normal';

        if (compositeScore < this.config.compositeExit.exitThreshold) {
            decision = 'EXIT';
            urgency = 'critical';
        } else if (compositeScore < this.config.compositeExit.alertThreshold) {
            decision = 'ALERT';
            urgency = 'high';
        }

        // 更新状态缓存
        this.positionStates.set(position.token_ca, {
            price: currentData.price,
            volume: currentData.volume || 0,
            timestamp: Date.now()
        });

        return {
            score: compositeScore,
            breakdown: {
                momentum: momentum.score,
                smartMoney: smartMoney.score,
                liquidity: liquidity.score,
                social: social.score
            },
            signals: allSignals,
            decision,
            urgency
        };
    }

    /**
     * 获取利润保护阈值
     * @param {number} currentProfit - 当前利润率
     * @returns {Object|null} { profit, protect, protectedProfit }
     */
    getProfitProtection(currentProfit) {
        if (!this.config.profitProtection.enabled) {
            return null;
        }

        const tiers = this.config.profitProtection.tiers;

        // 从高到低检查
        for (let i = tiers.length - 1; i >= 0; i--) {
            if (currentProfit >= tiers[i].profit) {
                const protectedProfit = currentProfit * tiers[i].protect;
                return {
                    tier: tiers[i],
                    protectedProfit,
                    stopLossPrice: (1 + protectedProfit) // 相对入场价的止损位
                };
            }
        }

        return null;
    }

    /**
     * 评估单个持仓的退出决策
     * @param {Object} position - 持仓数据
     * @param {Object} currentData - 当前市场数据 { price, volume, liquidity, smartMoneyCount, tgMentions, etc }
     * @returns {Object} { action, reason, urgency, details }
     */
    evaluate(position, currentData) {
        if (!this.config.enabled) {
            return { action: 'HOLD', reason: 'Smart Exit 已禁用' };
        }

        const symbol = position.symbol || position.token_ca?.slice(0, 8);
        const entryPrice = position.entry_price || 0;
        const currentPrice = currentData.price || 0;

        // 计算持仓时间
        const entryTime = new Date(position.entry_time || position.created_at);
        const holdingMinutes = (Date.now() - entryTime.getTime()) / 1000 / 60;

        // 计算当前盈亏
        const pnlPercent = entryPrice > 0
            ? (currentPrice - entryPrice) / entryPrice
            : 0;

        // 1. 检查分阶段止损
        const phaseInfo = this.getPhasedStopLoss(holdingMinutes);
        if (pnlPercent <= phaseInfo.stopLoss) {
            return {
                action: 'STOP_LOSS',
                reason: `分阶段止损(${phaseInfo.description}): ${(pnlPercent * 100).toFixed(1)}% < ${(phaseInfo.stopLoss * 100).toFixed(0)}%`,
                urgency: 'critical',
                details: { phase: phaseInfo.phase, holdingMinutes }
            };
        }

        // 2. 检查利润保护
        if (pnlPercent > 0) {
            const protection = this.getProfitProtection(pnlPercent);
            if (protection) {
                // 计算回撤
                const highWaterMark = position.high_water_mark || entryPrice;
                const maxProfit = highWaterMark > 0 ? (highWaterMark - entryPrice) / entryPrice : 0;
                const drawdown = maxProfit - pnlPercent;

                if (pnlPercent < protection.protectedProfit && drawdown > 0.1) {
                    return {
                        action: 'PROFIT_PROTECT',
                        reason: `利润保护: 从 +${(maxProfit * 100).toFixed(0)}% 回撤到 +${(pnlPercent * 100).toFixed(0)}%，保护 ${(protection.tier.protect * 100).toFixed(0)}% 利润`,
                        urgency: 'high',
                        details: { maxProfit, currentProfit: pnlPercent, protection }
                    };
                }
            }
        }

        // 3. 计算综合得分
        const composite = this.calculateCompositeScore(position, currentData);

        if (composite.decision === 'EXIT') {
            return {
                action: 'SMART_EXIT',
                reason: `综合信号退出 (Score: ${(composite.score * 100).toFixed(0)}%): ${composite.signals.slice(0, 3).join(', ')}`,
                urgency: composite.urgency,
                details: composite
            };
        }

        if (composite.decision === 'ALERT') {
            return {
                action: 'ALERT',
                reason: `多信号警报 (Score: ${(composite.score * 100).toFixed(0)}%): ${composite.signals.slice(0, 2).join(', ')}`,
                urgency: composite.urgency,
                details: composite
            };
        }

        // 4. 正常持有
        return {
            action: 'HOLD',
            reason: `继续持有 (Phase ${phaseInfo.phase}, Score: ${(composite.score * 100).toFixed(0)}%)`,
            urgency: 'normal',
            details: {
                phase: phaseInfo,
                composite,
                pnlPercent,
                holdingMinutes
            }
        };
    }

    /**
     * v7.1 检查是否应该触发 AI 分析
     * @param {Object} position - 持仓数据
     * @param {number} compositeScore - 综合得分
     * @param {number} pnlPercent - 盈亏百分比
     * @param {number} holdingMinutes - 持仓时间
     * @returns {boolean}
     */
    shouldTriggerAI(position, compositeScore, pnlPercent, holdingMinutes) {
        if (!this.config.aiExit.enabled || !this.aiClient) {
            return false;
        }

        const conditions = this.config.aiExit.triggerConditions;
        const tokenCa = position.token_ca;

        // 检查冷却
        const lastCall = this.aiCooldowns.get(tokenCa) || 0;
        if (Date.now() - lastCall < this.config.aiExit.cooldownMs) {
            return false;
        }

        // 检查综合得分是否在模糊区间
        const inScoreRange = compositeScore >= conditions.scoreRange.min &&
            compositeScore <= conditions.scoreRange.max;

        // 检查盈亏是否在模糊区间
        const inProfitRange = pnlPercent >= conditions.profitRange.min &&
            pnlPercent <= conditions.profitRange.max;

        // 检查持仓时间
        const holdingEnough = holdingMinutes >= conditions.minHoldingMinutes;

        // 至少满足两个条件
        const conditionsMet = [inScoreRange, inProfitRange, holdingEnough]
            .filter(Boolean).length;

        return conditionsMet >= 2;
    }

    /**
     * v7.5 AI 辅助退出分析 (修复兼容性)
     * @param {Object} position - 持仓数据
     * @param {Object} currentData - 当前市场数据
     * @param {Object} composite - 综合评分结果
     * @returns {Promise<Object>} AI 分析结果
     */
    async analyzeWithAI(position, currentData, composite) {
        if (!this.aiClient) {
            return { action: 'HOLD', reason: 'AI client not available', aiUsed: false };
        }

        const tokenCa = position.token_ca;
        this.aiCooldowns.set(tokenCa, Date.now());

        try {
            // v7.5 修改 prompt 为 JSON 格式，与 AIAnalyst 兼容
            const pnlPercent = position.entry_price > 0
                ? ((currentData.price - position.entry_price) / position.entry_price * 100).toFixed(1)
                : '0';
            const holdingMinutes = ((Date.now() - new Date(position.entry_time).getTime()) / 60000).toFixed(0);

            const prompt = `
分析是否应该退出这个 meme coin 持仓。

## 持仓信息
- 代币: ${position.symbol} (${position.chain})
- 入场价: $${position.entry_price?.toFixed(8) || 'N/A'}
- 当前价: $${currentData.price?.toFixed(8) || 'N/A'}
- 盈亏: ${pnlPercent}%
- 持仓时间: ${holdingMinutes} 分钟

## 市场信号
- 综合评分: ${(composite.score * 100).toFixed(0)}% (25%以下应退出，50%以上应持有)
- 动量得分: ${(composite.breakdown.momentum * 100).toFixed(0)}%
- 聪明钱得分: ${(composite.breakdown.smartMoney * 100).toFixed(0)}%
- 流动性得分: ${(composite.breakdown.liquidity * 100).toFixed(0)}%
- 社交热度: ${(composite.breakdown.social * 100).toFixed(0)}%

## 信号详情
${composite.signals.join('\n')}

## 当前数据
- 聪明钱数量: ${currentData.smartMoneyCount || 0}
- 流动性: $${((currentData.liquidity || 0) / 1000).toFixed(1)}K
- 市值: $${((currentData.marketCap || 0) / 1000).toFixed(0)}K

请返回 JSON 格式的退出建议:
{
  "action": "HOLD 或 EXIT 或 PARTIAL_EXIT",
  "confidence": 0-100的置信度,
  "reason": "一句话理由"
}
`;

            const response = await Promise.race([
                this.aiClient.analyze(prompt),
                new Promise((_, reject) =>
                    setTimeout(() => reject(new Error('AI timeout')), this.config.aiExit.timeoutMs)
                )
            ]);

            // v7.5 修复: AIAnalyst.analyze() 返回的是 Object，不是字符串
            // 需要正确处理返回的对象格式
            let aiAction = 'HOLD';
            let aiConfidence = 50;
            let aiReason = 'AI analysis complete';

            if (response && typeof response === 'object') {
                // AIAnalyst 返回格式: { action, position, reason, confidence, raw, ... }
                // 需要从 raw 或直接字段中提取退出建议
                const rawResult = response.raw || response;

                // 尝试从返回结果中获取 action
                // AIAnalyst 的 action 可能是 BUY/SKIP/WATCH，需要映射到退出决策
                const originalAction = (rawResult.action || response.action || '').toUpperCase();

                // 映射逻辑:
                // - BUY/WATCH -> HOLD (继续持有)
                // - SKIP/EXIT -> EXIT (退出)
                // - PARTIAL_EXIT -> PARTIAL_EXIT
                if (originalAction === 'EXIT' || originalAction === 'SKIP') {
                    aiAction = 'EXIT';
                } else if (originalAction === 'PARTIAL_EXIT') {
                    aiAction = 'PARTIAL_EXIT';
                } else {
                    aiAction = 'HOLD';
                }

                aiConfidence = rawResult.confidence || response.confidence || 50;
                aiReason = rawResult.reason || response.reason || 'AI analysis complete';
            } else if (typeof response === 'string') {
                // 兼容字符串格式（旧版本）
                const actionMatch = response.match(/ACTION:\s*(HOLD|EXIT|PARTIAL_EXIT)/i);
                const confidenceMatch = response.match(/CONFIDENCE:\s*(\d+)/i);
                const reasonMatch = response.match(/REASON:\s*(.+)/i);

                aiAction = actionMatch ? actionMatch[1].toUpperCase() : 'HOLD';
                aiConfidence = confidenceMatch ? parseInt(confidenceMatch[1]) : 50;
                aiReason = reasonMatch ? reasonMatch[1].trim() : 'AI analysis complete';
            }

            console.log(`[SmartExit] 🤖 AI 分析: ${aiAction} (${aiConfidence}% 置信) - ${aiReason}`);

            return {
                action: aiAction === 'EXIT' ? 'AI_EXIT' :
                    aiAction === 'PARTIAL_EXIT' ? 'AI_PARTIAL_EXIT' : 'HOLD',
                reason: `AI建议: ${aiReason}`,
                aiUsed: true,
                aiConfidence,
                aiRaw: response
            };

        } catch (error) {
            console.error(`[SmartExit] AI 分析失败: ${error.message}`);
            return { action: 'HOLD', reason: 'AI analysis failed', aiUsed: false, error: error.message };
        }
    }

    /**
     * v7.1 增强版评估 - 支持 AI 辅助
     * @param {Object} position - 持仓数据
     * @param {Object} currentData - 当前市场数据
     * @param {Object} options - 选项 { useAI: boolean }
     * @returns {Promise<Object>}
     */
    async evaluateAsync(position, currentData, options = {}) {
        // 先执行常规评估
        const result = this.evaluate(position, currentData);

        // 如果明确需要退出或持有，不需要 AI
        if (result.action === 'STOP_LOSS' || result.action === 'PROFIT_PROTECT') {
            return result;
        }

        // 检查是否应该触发 AI
        const pnlPercent = result.details?.pnlPercent || 0;
        const holdingMinutes = result.details?.holdingMinutes || 0;
        const compositeScore = result.details?.composite?.score || 0.5;

        if (options.useAI !== false && this.shouldTriggerAI(position, compositeScore, pnlPercent, holdingMinutes)) {
            const aiResult = await this.analyzeWithAI(position, currentData, result.details.composite);

            // AI 建议退出且置信度高
            if (aiResult.aiUsed && aiResult.aiConfidence >= 70) {
                if (aiResult.action === 'AI_EXIT') {
                    return {
                        ...result,
                        action: 'AI_EXIT',
                        reason: aiResult.reason,
                        urgency: 'high',
                        aiAnalysis: aiResult
                    };
                } else if (aiResult.action === 'AI_PARTIAL_EXIT') {
                    return {
                        ...result,
                        action: 'AI_PARTIAL_EXIT',
                        reason: aiResult.reason,
                        urgency: 'medium',
                        aiAnalysis: aiResult
                    };
                }
            }

            // 附加 AI 分析结果
            result.aiAnalysis = aiResult;
        }

        return result;
    }

    /**
     * 设置 AI 客户端
     * @param {Object} aiClient - AI 客户端实例
     */
    setAIClient(aiClient) {
        this.aiClient = aiClient;
        console.log('[SmartExit] 🤖 AI client configured');
    }

    /**
     * 清理过期的状态缓存
     */
    cleanup() {
        const now = Date.now();
        const maxAge = 4 * 60 * 60 * 1000; // 4小时

        for (const [key, state] of this.positionStates) {
            if (now - state.timestamp > maxAge) {
                this.positionStates.delete(key);
            }
        }
    }

    /**
     * 获取统计信息
     */
    getStats() {
        return {
            enabled: this.config.enabled,
            cachedPositions: this.positionStates.size,
            aiEnabled: this.config.aiExit.enabled,
            aiClientAvailable: !!this.aiClient,
            aiCooldownsActive: this.aiCooldowns.size,
            config: {
                phases: Object.keys(this.config.phasedStopLoss).length,
                profitTiers: this.config.profitProtection.tiers.length
            }
        };
    }
}

export default SmartExitEngine;
