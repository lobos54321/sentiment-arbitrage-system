/**
 * 过滤参数配置 v7.7 (基于扩展回测优化)
 *
 * 数据来源 (2026-01-13 扩展回测):
 * - 金狗样本: 62个 (10x+ 涨幅)
 * - 噪音样本: 1043个
 * - 测试组合: 15400种
 *
 * 关键发现:
 * 1. 无法同时达到金狗>50%且噪音过滤>50% - 这是根本性权衡
 * 2. signalTrendType=ACCELERATING 是最强区分因子 (3.1x区分度)
 * 3. 金狗通过率与噪音过滤率呈负相关
 *
 * 帕累托前沿分析结论:
 * - AGGRESSIVE: 追求金狗覆盖 → 90%金狗通过，15%噪音过滤，6%精确率
 * - CONSERVATIVE: 追求精确率 → 29%金狗通过，91%噪音过滤，16%精确率
 */

export const FILTER_PARAMS = {
    // ═══════════════════════════════════════════════════════════════
    // 策略选择 (v7.7 验证后推荐 BALANCED)
    // ═══════════════════════════════════════════════════════════════
    ACTIVE_STRATEGY: 'BALANCED',  // 'AGGRESSIVE' | 'BALANCED' | 'CONSERVATIVE' | 'ULTRA_CONSERVATIVE'


    // ═══════════════════════════════════════════════════════════════
    // 风控配置 (配合过滤策略使用)
    // ═══════════════════════════════════════════════════════════════
    RISK_CONTROL: {
        // 止损设置
        stopLossPercent: 15,           // 默认止损 15%
        maxStopLossPercent: 20,        // 最大止损 20%

        // 仓位控制 (基于信号强度)
        positionSizing: {
            basePositionPercent: 5,     // 基础仓位 5%
            maxPositionPercent: 15,     // 最大仓位 15%

            // 信号类型仓位权重
            signalWeights: {
                ACCELERATING: 1.5,      // 加速信号 1.5x 仓位
                STABLE: 1.0,            // 稳定信号 1.0x 仓位
                DECAYING: 0.5           // 衰退信号 0.5x 仓位 (BALANCED已过滤)
            },

            // 聪明钱数量加成
            smCountBonus: {
                threshold: 3,           // SM≥3 时给予加成
                bonusMultiplier: 1.2    // 额外 20% 仓位
            },

            // 评分加成
            scoreBonus: {
                threshold: 60,          // 评分≥60 时给予加成
                bonusMultiplier: 1.15   // 额外 15% 仓位
            }
        },

        // AI辅助决策 (待回测验证)
        aiAssist: {
            enabled: true,              // 启用AI辅助
            provider: 'grok',           // 使用Grok
            confidenceThreshold: 0.7,   // AI置信度阈值
            vetoEnabled: true           // AI可否决低质量信号
        }
    },

    // ═══════════════════════════════════════════════════════════════
    // 🎯 激进策略 (AGGRESSIVE) - 最大化金狗覆盖
    // 金狗通过: 90.3% (56/62) | 噪音过滤: 15.2% | 精确率: 6.0%
    // 适用: 不怕噪音损失，追求不漏金狗
    // ═══════════════════════════════════════════════════════════════
    AGGRESSIVE: {
        sevenDimThreshold: 17,
        smCountThreshold: 1,          // 放宽聪明钱要求
        requireTrendIncreasing: false,
        requireSignalAccelerating: false, // 不要求信号加速
        allowLateFollower: true,
        baseScoreThreshold: 50,
        maxMcap: null,                // 不限市值
        // 预期效果
        expectedGoldPassRate: 90.3,
        expectedNoiseFilterRate: 15.2,
        expectedPrecision: 6.0
    },

    // ═══════════════════════════════════════════════════════════════
    // 🏆 平衡策略 (BALANCED) - 综合最优
    // 金狗通过: 85.5% (53/62) | 噪音过滤: 19.3% | 精确率: 5.9%
    // 适用: 默认策略，过滤掉明显的衰退信号
    // ═══════════════════════════════════════════════════════════════
    BALANCED: {
        sevenDimThreshold: 17,
        smCountThreshold: 1,
        requireTrendIncreasing: false,
        requireSignalAccelerating: false,
        allowLateFollower: true,
        baseScoreThreshold: 50,
        maxMcap: null,
        // 额外过滤: 排除DECAYING信号
        rejectDecayingSignal: true,   // 新增: 排除衰退信号
        // 预期效果
        expectedGoldPassRate: 85.5,
        expectedNoiseFilterRate: 19.3,
        expectedPrecision: 5.9
    },

    // ═══════════════════════════════════════════════════════════════
    // 🛡️ 保守策略 (CONSERVATIVE) - 追求高精确率
    // 金狗通过: 29.0% (18/62) | 噪音过滤: 90.7% | 精确率: 15.7%
    // 适用: 资金有限，追求每次交易的质量
    // ═══════════════════════════════════════════════════════════════
    CONSERVATIVE: {
        sevenDimThreshold: 17,
        smCountThreshold: 1,
        requireTrendIncreasing: false,
        requireSignalAccelerating: true, // 关键: 只要加速信号
        allowLateFollower: true,
        baseScoreThreshold: 45,          // 可稍放宽基础分
        maxMcap: null,
        // 预期效果
        expectedGoldPassRate: 29.0,
        expectedNoiseFilterRate: 90.7,
        expectedPrecision: 15.7
    },

    // ═══════════════════════════════════════════════════════════════
    // 🔬 超保守策略 (ULTRA_CONSERVATIVE) - 最高精确率
    // 金狗通过: 29.0% (18/62) | 噪音过滤: 90.9% | 精确率: 15.9%
    // 适用: 极度风险厌恶，宁缺毋滥
    // ═══════════════════════════════════════════════════════════════
    ULTRA_CONSERVATIVE: {
        sevenDimThreshold: 17,
        smCountThreshold: 2,             // 更高聪明钱门槛
        requireTrendIncreasing: false,
        requireSignalAccelerating: true, // 只要加速信号
        allowLateFollower: true,
        baseScoreThreshold: 45,
        maxMcap: null,
        // 预期效果
        expectedGoldPassRate: 29.0,
        expectedNoiseFilterRate: 90.9,
        expectedPrecision: 15.9
    }
};

/**
 * 获取当前激活的过滤参数
 */
export function getActiveFilterParams() {
    const strategy = FILTER_PARAMS.ACTIVE_STRATEGY;
    return FILTER_PARAMS[strategy] || FILTER_PARAMS.BALANCED;
}

/**
 * 应用过滤逻辑
 * @param {Object} token - 代币数据
 * @param {Object} params - 可选的自定义参数 (覆盖默认)
 * @returns {Object} { pass: boolean, reason: string }
 */
export function applyFilter(token, params = null) {
    const p = params || getActiveFilterParams();

    // 1. 基础分检查
    const baseScore = token.baseScore || token.score?.total || 0;
    if (baseScore < p.baseScoreThreshold) {
        return { pass: false, reason: `基础分不足 (${baseScore} < ${p.baseScoreThreshold})` };
    }

    // 2. 七维分检查 (如果有)
    const sevenDimScore = token.sevenDimScore || token.analysis?.sevenDimScore || null;
    if (sevenDimScore !== null && sevenDimScore < p.sevenDimThreshold) {
        return { pass: false, reason: `七维分不足 (${sevenDimScore} < ${p.sevenDimThreshold})` };
    }

    // 3. 聪明钱数量检查
    const smCount = token.smartWalletOnline || token.smCount || 0;
    if (smCount < p.smCountThreshold) {
        return { pass: false, reason: `聪明钱不足 (${smCount} < ${p.smCountThreshold})` };
    }

    // 4. 趋势类型检查
    if (p.requireTrendIncreasing) {
        const trendType = token.trendType || token.trends?.smartMoney || 'STABLE';
        if (trendType !== 'INCREASING') {
            return { pass: false, reason: `趋势非上升 (${trendType})` };
        }
    }

    // 5. 信号趋势检查 (关键区分因子!)
    const signalTrendType = token.signalTrendType || token.trends?.signal || 'STABLE';

    // 5a. 如果要求加速信号
    if (p.requireSignalAccelerating) {
        if (signalTrendType !== 'ACCELERATING') {
            return { pass: false, reason: `信号非加速 (${signalTrendType})` };
        }
    }

    // 5b. 如果排除衰退信号 (新增)
    if (p.rejectDecayingSignal && signalTrendType === 'DECAYING') {
        return { pass: false, reason: `信号衰退 (${signalTrendType})` };
    }

    // 6. 接盘警告检查
    if (!p.allowLateFollower && token.lateFollower) {
        return { pass: false, reason: '接盘警告' };
    }

    // 7. 市值检查
    const mcap = token.marketCap || token.mcap || 0;
    if (p.maxMcap !== null && mcap > p.maxMcap) {
        return { pass: false, reason: `市值过大 ($${(mcap/1000).toFixed(0)}K > $${(p.maxMcap/1000).toFixed(0)}K)` };
    }

    return { pass: true, reason: '通过所有检查' };
}

/**
 * 打印当前过滤配置
 */
export function printFilterConfig() {
    const strategy = FILTER_PARAMS.ACTIVE_STRATEGY;
    const p = getActiveFilterParams();

    console.log(`\n${'═'.repeat(60)}`);
    console.log(`🎯 当前过滤策略: ${strategy} (v7.7)`);
    console.log(`${'═'.repeat(60)}`);
    console.log(`   基础分阈值: ≥${p.baseScoreThreshold}`);
    console.log(`   聪明钱阈值: ≥${p.smCountThreshold}`);
    console.log(`   信号加速要求: ${p.requireSignalAccelerating ? '是 (仅ACCELERATING)' : '否'}`);
    console.log(`   排除衰退信号: ${p.rejectDecayingSignal ? '是' : '否'}`);
    console.log(`   市值上限: ${p.maxMcap ? '$' + (p.maxMcap/1000) + 'K' : '无限制'}`);
    console.log(`─`.repeat(60));
    console.log(`   预期金狗通过: ${p.expectedGoldPassRate || '?'}%`);
    console.log(`   预期噪音过滤: ${p.expectedNoiseFilterRate || '?'}%`);
    console.log(`   预期精确率: ${p.expectedPrecision || '?'}%`);
    console.log(`${'═'.repeat(60)}\n`);
}

export default FILTER_PARAMS;
