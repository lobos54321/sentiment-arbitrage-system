/**
 * 金狗保护规则 v1.0
 *
 * 基于被误杀金狗的特征分析，创建保护规则
 * 防止AI过于保守导致漏掉优质代币
 *
 * 发现：
 * - 79.3% 被误杀金狗是 STABLE 信号
 * - 86.2% 被误杀金狗基础分 ≥55
 * - AI对叙事判断过严
 *
 * 解决方案：
 * - 当链上特征足够强时，覆盖AI的DISCARD决策
 * - 对高分STABLE信号给予保护
 */

/**
 * 保护规则配置
 */
const PROTECTION_RULES = {
    // 规则1: 高分保护 - 基础分≥55的代币不轻易放弃
    HIGH_SCORE_PROTECTION: {
        enabled: true,
        minBaseScore: 55,
        action: 'FORCE_WATCH',  // 即使AI说DISCARD，也强制WATCH
        description: '高分代币保护'
    },

    // 规则2: STABLE信号保护 - STABLE+高分组合
    STABLE_PROTECTION: {
        enabled: true,
        signalType: 'STABLE',
        minBaseScore: 55,
        minSmCount: 2,
        action: 'FORCE_WATCH',
        description: 'STABLE高分保护'
    },

    // 规则3: SM=2黄金组合保护
    SM2_PROTECTION: {
        enabled: true,
        smCount: 2,
        minBaseScore: 50,
        action: 'UPGRADE_TO_BUY',  // 直接升级为BUY候选
        description: 'SM=2黄金组合'
    },

    // 规则4: ACCELERATING信号强制保护
    ACCELERATING_PROTECTION: {
        enabled: true,
        signalType: 'ACCELERATING',
        minBaseScore: 50,
        action: 'FORCE_BUY_CANDIDATE',
        description: 'ACCELERATING强制候选'
    },

    // 规则5: 多SM保护 - SM≥3的代币有多个聪明钱关注
    MULTI_SM_PROTECTION: {
        enabled: true,
        minSmCount: 3,
        minBaseScore: 55,
        action: 'FORCE_WATCH',
        description: '多聪明钱保护'
    }
};

/**
 * 应用保护规则
 *
 * @param {Object} token - 代币数据
 * @param {string} aiDecision - AI的原始决策 (BUY/WATCH/DISCARD)
 * @returns {Object} 最终决策和原因
 */
function applyProtectionRules(token, aiDecision) {
    const result = {
        originalDecision: aiDecision,
        finalDecision: aiDecision,
        protectionApplied: false,
        appliedRules: [],
        reason: null
    };

    const baseScore = token.baseScore || 0;
    const smCount = token.smCount || 0;
    const signalType = token.signalTrendType || 'UNKNOWN';

    // 如果AI已经建议BUY，不需要保护
    if (aiDecision === 'BUY') {
        return result;
    }

    // 规则4: ACCELERATING信号强制保护 (最高优先级)
    if (PROTECTION_RULES.ACCELERATING_PROTECTION.enabled &&
        signalType === 'ACCELERATING' &&
        baseScore >= PROTECTION_RULES.ACCELERATING_PROTECTION.minBaseScore) {

        result.finalDecision = 'BUY_CANDIDATE';
        result.protectionApplied = true;
        result.appliedRules.push('ACCELERATING_PROTECTION');
        result.reason = `ACCELERATING信号 + score=${baseScore} → 强制BUY候选`;
        return result;
    }

    // 规则3: SM=2黄金组合保护
    if (PROTECTION_RULES.SM2_PROTECTION.enabled &&
        smCount === 2 &&
        baseScore >= PROTECTION_RULES.SM2_PROTECTION.minBaseScore) {

        result.finalDecision = 'BUY_CANDIDATE';
        result.protectionApplied = true;
        result.appliedRules.push('SM2_PROTECTION');
        result.reason = `SM=2黄金组合 + score=${baseScore} → 升级为BUY候选`;
        return result;
    }

    // 规则2: STABLE信号保护
    if (PROTECTION_RULES.STABLE_PROTECTION.enabled &&
        signalType === 'STABLE' &&
        baseScore >= PROTECTION_RULES.STABLE_PROTECTION.minBaseScore &&
        smCount >= PROTECTION_RULES.STABLE_PROTECTION.minSmCount) {

        if (aiDecision === 'DISCARD') {
            result.finalDecision = 'WATCH';
            result.protectionApplied = true;
            result.appliedRules.push('STABLE_PROTECTION');
            result.reason = `STABLE + score=${baseScore} + SM=${smCount} → 覆盖DISCARD为WATCH`;
        }
        return result;
    }

    // 规则1: 高分保护
    if (PROTECTION_RULES.HIGH_SCORE_PROTECTION.enabled &&
        baseScore >= PROTECTION_RULES.HIGH_SCORE_PROTECTION.minBaseScore) {

        if (aiDecision === 'DISCARD') {
            result.finalDecision = 'WATCH';
            result.protectionApplied = true;
            result.appliedRules.push('HIGH_SCORE_PROTECTION');
            result.reason = `高分保护 score=${baseScore} → 覆盖DISCARD为WATCH`;
        }
        return result;
    }

    // 规则5: 多SM保护
    if (PROTECTION_RULES.MULTI_SM_PROTECTION.enabled &&
        smCount >= PROTECTION_RULES.MULTI_SM_PROTECTION.minSmCount &&
        baseScore >= PROTECTION_RULES.MULTI_SM_PROTECTION.minBaseScore) {

        if (aiDecision === 'DISCARD') {
            result.finalDecision = 'WATCH';
            result.protectionApplied = true;
            result.appliedRules.push('MULTI_SM_PROTECTION');
            result.reason = `多SM保护 SM=${smCount} + score=${baseScore} → 覆盖DISCARD为WATCH`;
        }
        return result;
    }

    return result;
}

/**
 * 批量应用保护规则并统计
 *
 * @param {Array} tokens - 代币列表
 * @param {Map} aiDecisions - AI决策映射 (symbol -> decision)
 * @returns {Object} 保护后的结果
 */
function applyProtectionBatch(tokens, aiDecisions) {
    const results = {
        protected: [],
        unchanged: [],
        stats: {
            totalTokens: tokens.length,
            protectedCount: 0,
            upgradedToBuy: 0,
            upgradedToWatch: 0,
            ruleStats: {}
        }
    };

    for (const token of tokens) {
        const symbol = token.symbol || token.name || 'unknown';
        const aiDecision = aiDecisions.get(symbol) || 'DISCARD';

        const protection = applyProtectionRules(token, aiDecision);

        if (protection.protectionApplied) {
            results.protected.push({
                symbol,
                ...protection,
                token: {
                    baseScore: token.baseScore,
                    smCount: token.smCount,
                    signalTrendType: token.signalTrendType
                }
            });
            results.stats.protectedCount++;

            if (protection.finalDecision === 'BUY_CANDIDATE') {
                results.stats.upgradedToBuy++;
            } else if (protection.finalDecision === 'WATCH') {
                results.stats.upgradedToWatch++;
            }

            // 规则统计
            for (const rule of protection.appliedRules) {
                results.stats.ruleStats[rule] = (results.stats.ruleStats[rule] || 0) + 1;
            }
        } else {
            results.unchanged.push({ symbol, decision: aiDecision });
        }
    }

    return results;
}

/**
 * 获取保护规则配置
 */
function getProtectionConfig() {
    return PROTECTION_RULES;
}

/**
 * 更新保护规则配置
 */
function updateProtectionConfig(updates) {
    Object.assign(PROTECTION_RULES, updates);
}

export {
    applyProtectionRules,
    applyProtectionBatch,
    getProtectionConfig,
    updateProtectionConfig,
    PROTECTION_RULES
};

export default applyProtectionRules;
