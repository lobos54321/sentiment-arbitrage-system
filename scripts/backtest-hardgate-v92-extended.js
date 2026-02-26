/**
 * v9.2 硬门槛扩展回测
 *
 * 使用 feature-analysis-results.json 中的真实数据
 * 模拟不同 priceChange 场景下的硬门槛表现
 *
 * 数据时效性说明:
 * priceChange 是时间敏感数据，同一代币在不同时间点有不同值
 * 本回测使用多场景模拟来评估硬门槛在各种情况下的表现
 */

import fs from 'fs';
import path from 'path';

// 读取真实数据
const featureData = JSON.parse(fs.readFileSync('data/feature-analysis-results.json', 'utf8'));

// v9.2 硬门槛逻辑 (从 batch-ai-advisor.js 提取)
function v92HardGate(token, priceChange) {
    const signalType = token.signalTrendType || 'STABLE';
    const intentionTier = token.intentionTier || 'TIER_B';  // 默认 B 级
    const smCount = token.smCount || 0;
    const isProtectedSignal = signalType === 'STABLE' || signalType === 'ACCELERATING';
    const isEarlyGem = (token.mcap || 0) < 50000;

    // 规则 1: SM 净流出检查
    const smDelta = 0; // 假设 SM 稳定，专注测试涨跌幅逻辑

    // 规则 2: 涨幅阈值
    const riseThresholds = {
        'TIER_S': { base: 500, accel: 2000, early: 10000 },
        'TIER_A': { base: 300, accel: 1000, early: 8000 },
        'TIER_B': { base: 150, accel: 500, early: 6000 },
        'TIER_C': { base: 80, accel: 200, early: 5000 },
        'default': { base: 80, accel: 200, early: 5000 }
    };

    const tierThresholds = riseThresholds[intentionTier] || riseThresholds.default;

    let maxPriceChange;
    if (isEarlyGem) {
        maxPriceChange = tierThresholds.early;
    } else if (isProtectedSignal) {
        maxPriceChange = tierThresholds.accel;
    } else {
        maxPriceChange = tierThresholds.base;
    }

    // SM=2 黄金组合放宽
    if (smCount >= 2 && isProtectedSignal) {
        maxPriceChange *= 1.5;
    }

    if (priceChange > maxPriceChange) {
        return { pass: false, reason: `涨幅过高 (+${priceChange.toFixed(1)}% > ${maxPriceChange.toFixed(0)}%)` };
    }

    // 规则 3: 跌幅阈值
    const dropThresholds = {
        'TIER_S': -60,
        'TIER_A': -50,
        'TIER_B': -40,
        'TIER_C': -30,
        'default': -30
    };

    let maxDrop = dropThresholds[intentionTier] || dropThresholds.default;

    if (isProtectedSignal) maxDrop -= 15;
    if (smCount >= 2 && isProtectedSignal) maxDrop -= 10;

    const isPhoenix = signalType === 'ACCELERATING' && priceChange < -40;
    if (isPhoenix) maxDrop -= 20;

    if (priceChange < maxDrop) {
        return { pass: false, reason: `跌幅过大 (${priceChange.toFixed(1)}% < ${maxDrop}%)` };
    }

    return { pass: true, reason: null };
}

// 旧版硬门槛 (固定阈值)
function oldHardGate(priceChange) {
    if (priceChange > 80) return { pass: false, reason: `涨幅过高 (+${priceChange.toFixed(1)}% > 80%)` };
    if (priceChange < -30) return { pass: false, reason: `跌幅过大 (${priceChange.toFixed(1)}% < -30%)` };
    return { pass: true, reason: null };
}

// 典型 priceChange 场景
const priceChangeScenarios = [
    { name: '正常涨幅', range: [0, 50] },
    { name: '中等涨幅', range: [50, 100] },
    { name: '高涨幅', range: [100, 300] },
    { name: '爆发涨幅', range: [300, 1000] },
    { name: '轻微跌幅', range: [-20, 0] },
    { name: '中等跌幅', range: [-40, -20] },
    { name: '大跌幅', range: [-60, -40] },
    { name: 'Phoenix跌幅', range: [-80, -60] }
];

// 按 intentionTier 分配 (基于 signalTrendType 和 smCount 推断)
function inferIntentionTier(token) {
    const signalType = token.signalTrendType || 'STABLE';
    const smCount = token.smCount || 0;

    // 高 SM + ACCELERATING = 高质量
    if (signalType === 'ACCELERATING' && smCount >= 4) return 'TIER_A';
    if (signalType === 'ACCELERATING' && smCount >= 2) return 'TIER_B';
    if (signalType === 'STABLE' && smCount >= 4) return 'TIER_B';
    if (signalType === 'STABLE' && smCount >= 2) return 'TIER_B';
    return 'TIER_C';
}

console.log('═'.repeat(80));
console.log('🔬 v9.2 硬门槛扩展回测 - 使用 feature-analysis-results.json');
console.log('═'.repeat(80));

// 提取金狗和噪音数据
const goldFeatures = featureData.goldFeatures || [];
const noiseFeatures = featureData.noiseFeatures || [];

console.log(`\n📊 数据样本: ${goldFeatures.length} 金狗 + ${noiseFeatures.length} 噪音\n`);

// 去重金狗 (按 symbol)
const uniqueGolds = [];
const seenSymbols = new Set();
for (const gold of goldFeatures) {
    if (!seenSymbols.has(gold.symbol)) {
        seenSymbols.add(gold.symbol);
        gold.intentionTier = inferIntentionTier(gold);
        uniqueGolds.push(gold);
    }
}

console.log(`\n📊 去重后: ${uniqueGolds.length} 个唯一金狗\n`);

// 场景回测
console.log('【场景回测 - 模拟不同 priceChange】');
console.log('─'.repeat(80));

const scenarioResults = [];

for (const scenario of priceChangeScenarios) {
    const priceChange = (scenario.range[0] + scenario.range[1]) / 2; // 取中值

    let goldOldPass = 0, goldV92Pass = 0;
    let noiseOldPass = 0, noiseV92Pass = 0;

    // 金狗测试
    for (const gold of uniqueGolds) {
        const oldResult = oldHardGate(priceChange);
        const v92Result = v92HardGate(gold, priceChange);

        if (oldResult.pass) goldOldPass++;
        if (v92Result.pass) goldV92Pass++;
    }

    // 噪音测试 (取前 100 个)
    const noiseSubset = noiseFeatures.slice(0, 100);
    for (const noise of noiseSubset) {
        noise.intentionTier = inferIntentionTier(noise);
        const oldResult = oldHardGate(priceChange);
        const v92Result = v92HardGate(noise, priceChange);

        if (oldResult.pass) noiseOldPass++;
        if (v92Result.pass) noiseV92Pass++;
    }

    const result = {
        scenario: scenario.name,
        priceChange,
        goldOldPass,
        goldV92Pass,
        goldTotal: uniqueGolds.length,
        noiseOldPass,
        noiseV92Pass,
        noiseTotal: noiseSubset.length
    };

    scenarioResults.push(result);

    console.log(`${scenario.name} (${priceChange >= 0 ? '+' : ''}${priceChange.toFixed(0)}%):`);
    console.log(`  金狗通过: 旧版 ${goldOldPass}/${uniqueGolds.length} → v9.2 ${goldV92Pass}/${uniqueGolds.length} (${goldV92Pass > goldOldPass ? '+' : ''}${goldV92Pass - goldOldPass})`);
    console.log(`  噪音通过: 旧版 ${noiseOldPass}/${noiseSubset.length} → v9.2 ${noiseV92Pass}/${noiseSubset.length} (${noiseV92Pass > noiseOldPass ? '+' : ''}${noiseV92Pass - noiseOldPass})`);
    console.log('');
}

// 特征分析 - 金狗的信号类型分布
console.log('\n' + '═'.repeat(80));
console.log('📊 金狗特征分析');
console.log('═'.repeat(80));

const signalTypeCount = { ACCELERATING: 0, STABLE: 0, DECAYING: 0 };
const smCountDist = {};
const tierDist = {};

for (const gold of uniqueGolds) {
    const signalType = gold.signalTrendType || 'STABLE';
    signalTypeCount[signalType] = (signalTypeCount[signalType] || 0) + 1;

    const sm = gold.smCount || 0;
    const smBucket = sm >= 5 ? '5+' : String(sm);
    smCountDist[smBucket] = (smCountDist[smBucket] || 0) + 1;

    tierDist[gold.intentionTier] = (tierDist[gold.intentionTier] || 0) + 1;
}

console.log('\n信号类型分布:');
console.log(`  ACCELERATING: ${signalTypeCount.ACCELERATING} (${(signalTypeCount.ACCELERATING / uniqueGolds.length * 100).toFixed(1)}%)`);
console.log(`  STABLE: ${signalTypeCount.STABLE} (${(signalTypeCount.STABLE / uniqueGolds.length * 100).toFixed(1)}%)`);
console.log(`  DECAYING: ${signalTypeCount.DECAYING} (${(signalTypeCount.DECAYING / uniqueGolds.length * 100).toFixed(1)}%)`);

console.log('\nSM 数量分布:');
Object.entries(smCountDist).sort((a, b) => parseInt(a[0]) - parseInt(b[0])).forEach(([sm, count]) => {
    console.log(`  SM=${sm}: ${count} (${(count / uniqueGolds.length * 100).toFixed(1)}%)`);
});

console.log('\n推断的叙事等级分布:');
Object.entries(tierDist).forEach(([tier, count]) => {
    console.log(`  ${tier}: ${count} (${(count / uniqueGolds.length * 100).toFixed(1)}%)`);
});

// Phoenix 潜力分析
console.log('\n' + '═'.repeat(80));
console.log('🔥 Phoenix 潜力分析 (ACCELERATING + 深跌)');
console.log('═'.repeat(80));

const phoenixCandidates = uniqueGolds.filter(g => g.signalTrendType === 'ACCELERATING');
console.log(`\nACCELERATING 信号的金狗: ${phoenixCandidates.length} 个`);
console.log('这些金狗如果入场时价格已跌超 40%，会被 Phoenix 保护放行');

phoenixCandidates.slice(0, 10).forEach(g => {
    console.log(`  - ${g.symbol}: SM=${g.smCount}, ${g.intentionTier}`);
});

// 生成总结
console.log('\n' + '═'.repeat(80));
console.log('📋 回测总结');
console.log('═'.repeat(80));

console.log('\n| 场景 | priceChange | 金狗通过变化 | 噪音通过变化 | 收益/风险 |');
console.log('|------|-------------|--------------|--------------|-----------|');

for (const r of scenarioResults) {
    const goldDelta = r.goldV92Pass - r.goldOldPass;
    const noiseDelta = r.noiseV92Pass - r.noiseOldPass;
    const ratio = goldDelta > 0 ? (noiseDelta === 0 ? '∞' : (goldDelta / noiseDelta).toFixed(1)) : '-';

    console.log(`| ${r.scenario.padEnd(12)} | ${(r.priceChange >= 0 ? '+' : '') + r.priceChange.toFixed(0).padStart(4)}% | ${(goldDelta >= 0 ? '+' : '') + goldDelta} 金狗 | ${(noiseDelta >= 0 ? '+' : '') + noiseDelta} 噪音 | ${ratio} |`);
}

console.log('\n💡 关键发现:');
console.log('1. 高涨幅场景: v9.2 允许 ACCELERATING+SM≥2 的金狗通过');
console.log('2. 大跌幅场景: v9.2 的 Phoenix 检测保护反弹中的金狗');
console.log('3. 信号保护: STABLE/ACCELERATING 信号获得更宽松阈值');
console.log('4. SM=2 黄金组合: 额外 50% 涨幅容忍 + 10% 跌幅容忍');

console.log('\n⚠️ 数据时效性说明:');
console.log('priceChange 是时间敏感数据，同一代币在不同时间点有不同值。');
console.log('本回测使用场景模拟，实际表现取决于入场时机。');
console.log('建议: 添加实时 priceChange 日志记录以进行更精确的回测。');

// 保存结果
const resultsPath = path.join('data', 'hardgate-v92-backtest-results.json');
fs.writeFileSync(resultsPath, JSON.stringify({
    timestamp: new Date().toISOString(),
    summary: {
        uniqueGolds: uniqueGolds.length,
        noiseTestSize: 100,
        signalTypeDistribution: signalTypeCount,
        smCountDistribution: smCountDist,
        tierDistribution: tierDist,
        phoenixCandidates: phoenixCandidates.length
    },
    scenarioResults,
    recommendations: [
        'v9.2 在高涨幅场景下保护更多金狗',
        'Phoenix 检测可拯救 ACCELERATING + 深跌的金狗',
        '需要实时 priceChange 日志来进行精确回测'
    ]
}, null, 2));

console.log(`\n✅ 结果已保存到: ${resultsPath}`);
