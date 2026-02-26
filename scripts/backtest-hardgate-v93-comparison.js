/**
 * v9.3 硬门槛优化回测
 *
 * 基于 v9.2 回测发现的问题进行优化:
 * 1. 黄金组合条件更严格: SM≥2 + ACCELERATING (而非任意 protected signal)
 * 2. Phoenix 保护需要 SM≥2 配合
 * 3. 增加 baseScore 门槛作为额外保护
 */

import fs from 'fs';

// 读取真实数据
const featureData = JSON.parse(fs.readFileSync('data/feature-analysis-results.json', 'utf8'));

// v9.2 硬门槛逻辑 (原版)
function v92HardGate(token, priceChange) {
    const signalType = token.signalTrendType || 'STABLE';
    const intentionTier = token.intentionTier || 'TIER_B';
    const smCount = token.smCount || 0;
    const isProtectedSignal = signalType === 'STABLE' || signalType === 'ACCELERATING';

    // 涨幅阈值
    const riseThresholds = {
        'TIER_A': { base: 300, accel: 1000 },
        'TIER_B': { base: 150, accel: 500 },
        'TIER_C': { base: 80, accel: 200 },
        'default': { base: 80, accel: 200 }
    };

    const tierThresholds = riseThresholds[intentionTier] || riseThresholds.default;
    let maxPriceChange = isProtectedSignal ? tierThresholds.accel : tierThresholds.base;

    // SM=2 黄金组合放宽 (v9.2: 任意 protected signal)
    if (smCount >= 2 && isProtectedSignal) {
        maxPriceChange *= 1.5;
    }

    if (priceChange > maxPriceChange) {
        return { pass: false, reason: `涨幅过高 (+${priceChange.toFixed(1)}% > ${maxPriceChange.toFixed(0)}%)` };
    }

    // 跌幅阈值
    const dropThresholds = { 'TIER_A': -50, 'TIER_B': -40, 'TIER_C': -30, 'default': -30 };
    let maxDrop = dropThresholds[intentionTier] || dropThresholds.default;

    if (isProtectedSignal) maxDrop -= 15;
    if (smCount >= 2 && isProtectedSignal) maxDrop -= 10;

    const isPhoenix = signalType === 'ACCELERATING' && priceChange < -40;
    if (isPhoenix) maxDrop -= 20;

    if (priceChange < maxDrop) {
        return { pass: false, reason: `跌幅过大` };
    }

    return { pass: true, reason: null };
}

// v9.3 硬门槛逻辑 (优化版)
function v93HardGate(token, priceChange) {
    const signalType = token.signalTrendType || 'STABLE';
    const intentionTier = token.intentionTier || 'TIER_B';
    const smCount = token.smCount || 0;
    const baseScore = token.baseScore || 50;

    // v9.3 关键改进: 黄金组合必须是 SM≥2 + ACCELERATING
    const isGoldenCombo = smCount >= 2 && signalType === 'ACCELERATING';
    const isProtectedSignal = signalType === 'STABLE' || signalType === 'ACCELERATING';

    // v9.3: 基础分门槛 - baseScore < 55 不享受任何放宽
    const hasQualityBase = baseScore >= 55;

    // 涨幅阈值
    const riseThresholds = {
        'TIER_A': { base: 300, accel: 800 },  // v9.3: 降低 accel 阈值
        'TIER_B': { base: 150, accel: 400 },
        'TIER_C': { base: 80, accel: 150 },
        'default': { base: 80, accel: 150 }
    };

    const tierThresholds = riseThresholds[intentionTier] || riseThresholds.default;

    let maxPriceChange;
    if (isGoldenCombo && hasQualityBase) {
        // v9.3: 只有黄金组合 + 质量基础才能获得高阈值
        maxPriceChange = tierThresholds.accel;
    } else if (isProtectedSignal && hasQualityBase) {
        // 普通 protected signal 获得中等阈值
        maxPriceChange = (tierThresholds.base + tierThresholds.accel) / 2;
    } else {
        maxPriceChange = tierThresholds.base;
    }

    // v9.3: 黄金组合额外放宽，但要求更严格
    if (isGoldenCombo && hasQualityBase && smCount >= 3) {
        maxPriceChange *= 1.3;  // 降低到 30% (原来是 50%)
    }

    if (priceChange > maxPriceChange) {
        return { pass: false, reason: `涨幅过高 (+${priceChange.toFixed(1)}% > ${maxPriceChange.toFixed(0)}%)` };
    }

    // 跌幅阈值
    const dropThresholds = { 'TIER_A': -50, 'TIER_B': -40, 'TIER_C': -30, 'default': -30 };
    let maxDrop = dropThresholds[intentionTier] || dropThresholds.default;

    // v9.3: 只有黄金组合获得放宽
    if (isGoldenCombo && hasQualityBase) {
        maxDrop -= 20;  // 合并放宽 (原来分两次: 15% + 10%)
    } else if (isProtectedSignal && hasQualityBase) {
        maxDrop -= 10;  // 普通 protected signal 只获得小幅放宽
    }

    // v9.3: Phoenix 保护需要黄金组合配合
    const isPhoenix = isGoldenCombo && priceChange < -40;
    if (isPhoenix && hasQualityBase) {
        maxDrop -= 15;  // 降低到 15% (原来是 20%)
    }

    if (priceChange < maxDrop) {
        return { pass: false, reason: `跌幅过大` };
    }

    return { pass: true, reason: null };
}

// 旧版硬门槛
function oldHardGate(priceChange) {
    if (priceChange > 80) return { pass: false };
    if (priceChange < -30) return { pass: false };
    return { pass: true };
}

// 推断 intentionTier
function inferIntentionTier(token) {
    const signalType = token.signalTrendType || 'STABLE';
    const smCount = token.smCount || 0;

    if (signalType === 'ACCELERATING' && smCount >= 4) return 'TIER_A';
    if (signalType === 'ACCELERATING' && smCount >= 2) return 'TIER_B';
    if (signalType === 'STABLE' && smCount >= 4) return 'TIER_B';
    return 'TIER_C';
}

console.log('═'.repeat(80));
console.log('🔬 v9.3 硬门槛优化回测 - 与 v9.2 对比');
console.log('═'.repeat(80));

// 准备数据
const goldFeatures = featureData.goldFeatures || [];
const noiseFeatures = featureData.noiseFeatures || [];

// 去重
const uniqueGolds = [];
const seenG = new Set();
goldFeatures.forEach(g => {
    if (!seenG.has(g.symbol)) {
        seenG.add(g.symbol);
        g.intentionTier = inferIntentionTier(g);
        uniqueGolds.push(g);
    }
});

const uniqueNoises = [];
const seenN = new Set();
noiseFeatures.forEach(n => {
    if (!seenN.has(n.symbol)) {
        seenN.add(n.symbol);
        n.intentionTier = inferIntentionTier(n);
        uniqueNoises.push(n);
    }
});

console.log(`\n📊 样本: ${uniqueGolds.length} 金狗 + ${uniqueNoises.length} 噪音\n`);

// 关键场景测试
const scenarios = [
    { name: '高涨幅 +200%', priceChange: 200 },
    { name: '爆发涨幅 +500%', priceChange: 500 },
    { name: '大跌幅 -50%', priceChange: -50 },
    { name: 'Phoenix -70%', priceChange: -70 }
];

console.log('【关键场景对比】');
console.log('─'.repeat(80));
console.log('| 场景 | 旧版金狗 | v9.2金狗 | v9.3金狗 | 旧版噪音 | v9.2噪音 | v9.3噪音 | v9.3收益/风险 |');
console.log('|------|----------|----------|----------|----------|----------|----------|---------------|');

for (const scenario of scenarios) {
    const pc = scenario.priceChange;

    let oldGold = 0, v92Gold = 0, v93Gold = 0;
    let oldNoise = 0, v92Noise = 0, v93Noise = 0;

    uniqueGolds.forEach(g => {
        if (oldHardGate(pc).pass) oldGold++;
        if (v92HardGate(g, pc).pass) v92Gold++;
        if (v93HardGate(g, pc).pass) v93Gold++;
    });

    uniqueNoises.forEach(n => {
        if (oldHardGate(pc).pass) oldNoise++;
        if (v92HardGate(n, pc).pass) v92Noise++;
        if (v93HardGate(n, pc).pass) v93Noise++;
    });

    // 计算 v9.3 的金狗增益 vs 噪音增加比率
    const goldGainVsV92 = v93Gold - v92Gold;
    const noiseGainVsOld = v93Noise - oldNoise;
    const ratio = noiseGainVsOld > 0 ? ((v93Gold - oldGold) / noiseGainVsOld).toFixed(2) : '∞';

    console.log(`| ${scenario.name.padEnd(12)} | ${oldGold}/${uniqueGolds.length} | ${v92Gold}/${uniqueGolds.length} | ${v93Gold}/${uniqueGolds.length} | ${oldNoise}/${uniqueNoises.length} | ${v92Noise}/${uniqueNoises.length} | ${v93Noise}/${uniqueNoises.length} | ${ratio} |`);
}

// 详细分析金狗通过情况
console.log('\n' + '═'.repeat(80));
console.log('📊 金狗详细分析 (高涨幅 +200% 场景)');
console.log('═'.repeat(80));

const pc = 200;
const goldDetails = {
    v92PassV93Pass: [],
    v92PassV93Fail: [],
    v92FailV93Pass: [],
    v92FailV93Fail: []
};

uniqueGolds.forEach(g => {
    const v92 = v92HardGate(g, pc).pass;
    const v93 = v93HardGate(g, pc).pass;

    const info = `${g.symbol} (SM=${g.smCount}, ${g.signalTrendType}, base=${g.baseScore})`;

    if (v92 && v93) goldDetails.v92PassV93Pass.push(info);
    else if (v92 && !v93) goldDetails.v92PassV93Fail.push(info);
    else if (!v92 && v93) goldDetails.v92FailV93Pass.push(info);
    else goldDetails.v92FailV93Fail.push(info);
});

console.log(`\n两者都通过 (${goldDetails.v92PassV93Pass.length}个): 无变化`);
console.log(`v9.2通过v9.3拒绝 (${goldDetails.v92PassV93Fail.length}个): ${goldDetails.v92PassV93Fail.join(', ') || '无'}`);
console.log(`v9.2拒绝v9.3通过 (${goldDetails.v92FailV93Pass.length}个): ${goldDetails.v92FailV93Pass.join(', ') || '无'}`);
console.log(`两者都拒绝 (${goldDetails.v92FailV93Fail.length}个): ${goldDetails.v92FailV93Fail.join(', ') || '无'}`);

// 噪音分析
console.log('\n' + '═'.repeat(80));
console.log('📊 噪音拦截分析 (高涨幅 +200% 场景)');
console.log('═'.repeat(80));

const noiseDetails = {
    v92PassV93Pass: 0,
    v92PassV93Fail: 0,
    v92FailV93Pass: 0,
    v92FailV93Fail: 0
};

uniqueNoises.forEach(n => {
    const v92 = v92HardGate(n, pc).pass;
    const v93 = v93HardGate(n, pc).pass;

    if (v92 && v93) noiseDetails.v92PassV93Pass++;
    else if (v92 && !v93) noiseDetails.v92PassV93Fail++;
    else if (!v92 && v93) noiseDetails.v92FailV93Pass++;
    else noiseDetails.v92FailV93Fail++;
});

console.log(`\n两者都通过: ${noiseDetails.v92PassV93Pass}`);
console.log(`v9.2通过v9.3拒绝 (改进): ${noiseDetails.v92PassV93Fail}`);
console.log(`v9.2拒绝v9.3通过: ${noiseDetails.v92FailV93Pass}`);
console.log(`两者都拒绝: ${noiseDetails.v92FailV93Fail}`);

console.log('\n' + '═'.repeat(80));
console.log('📋 优化建议');
console.log('═'.repeat(80));

console.log('\n1. v9.3 关键改进:');
console.log('   - 黄金组合条件更严格: SM≥2 + ACCELERATING (而非任意 protected signal)');
console.log('   - Phoenix 保护需要黄金组合配合');
console.log('   - 增加 baseScore ≥ 55 作为质量门槛');
console.log('   - 降低放宽比例: 50% → 30%');

console.log('\n2. 预期效果:');
console.log('   - 金狗通过率略有下降，但高质量金狗仍能通过');
console.log('   - 噪音通过率大幅下降，提高精准度');
console.log('   - 收益/风险比从 0.2 提升到更高');

// 保存结果
fs.writeFileSync('data/hardgate-v93-comparison.json', JSON.stringify({
    timestamp: new Date().toISOString(),
    samples: { golds: uniqueGolds.length, noises: uniqueNoises.length },
    improvements: {
        goldenComboStrict: 'SM>=2 + ACCELERATING only',
        phoenixRequiresGoldenCombo: true,
        baseScoreThreshold: 55,
        reducedBoost: '50% -> 30%'
    }
}, null, 2));

console.log('\n✅ 分析完成');
