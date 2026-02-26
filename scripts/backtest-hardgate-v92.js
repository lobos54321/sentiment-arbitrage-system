/**
 * v9.2 硬门槛回测
 * 使用日志中提取的真实被拒绝数据
 */

// 从日志中提取的被拒绝案例 (手动整理)
const rejectedTokens = [
    // 已知金狗 (从之前的分析)
    { symbol: 'BITLORD', priceChange: -61.7, signalType: 'ACCELERATING', intentionTier: 'TIER_A', smCount: 6, maxGain: 42, isGold: true },
    { symbol: 'BITLORD', priceChange: -67.4, signalType: 'ACCELERATING', intentionTier: 'TIER_A', smCount: 6, maxGain: 42, isGold: true },
    { symbol: 'BOAR', priceChange: -45.3, signalType: 'ACCELERATING', intentionTier: 'TIER_B', smCount: 2, maxGain: 20.1, isGold: true },
    { symbol: 'MEME', priceChange: -75.8, signalType: 'ACCELERATING', intentionTier: 'TIER_A', smCount: 3, maxGain: 14.9, isGold: true },
    { symbol: 'MEME', priceChange: -71.3, signalType: 'ACCELERATING', intentionTier: 'TIER_A', smCount: 3, maxGain: 14.9, isGold: true },
    { symbol: 'MEME', priceChange: 733.4, signalType: 'ACCELERATING', intentionTier: 'TIER_A', smCount: 3, maxGain: 14.9, isGold: true },

    // 高涨幅被拒绝 - 银狗 (3x-10x)
    { symbol: '马牛灵兽', priceChange: 83.3, signalType: 'STABLE', intentionTier: 'TIER_B', smCount: 1, maxGain: 6.6, isGold: false, isSilver: true },
    { symbol: '马牛灵兽', priceChange: 102.6, signalType: 'STABLE', intentionTier: 'TIER_B', smCount: 1, maxGain: 6.6, isGold: false, isSilver: true },
    { symbol: '马牛灵兽', priceChange: 180.8, signalType: 'STABLE', intentionTier: 'TIER_B', smCount: 1, maxGain: 6.6, isGold: false, isSilver: true },
    { symbol: '马牛灵兽', priceChange: 93.7, signalType: 'STABLE', intentionTier: 'TIER_B', smCount: 1, maxGain: 6.6, isGold: false, isSilver: true },

    // 高涨幅被拒绝 - 未知结果
    { symbol: 'WAGBI', priceChange: 1278.6, signalType: 'STABLE', intentionTier: 'TIER_B', smCount: 2, maxGain: null, isGold: false },
    { symbol: 'GLONK(p)', priceChange: 3834.0, signalType: 'ACCELERATING', intentionTier: 'TIER_A', smCount: 4, maxGain: null, isGold: false },
    { symbol: '25%', priceChange: 93.1, signalType: 'STABLE', intentionTier: 'TIER_C', smCount: 0, maxGain: null, isGold: false },

    // 跌幅被拒绝 (非金狗)
    { symbol: 'FhmqNWXL', priceChange: -36.3, signalType: 'DECAYING', intentionTier: 'TIER_C', smCount: 0, maxGain: 0.5, isGold: false },
    { symbol: '拉马克', priceChange: -36.9, signalType: 'DECAYING', intentionTier: 'TIER_C', smCount: 0, maxGain: 0.3, isGold: false },
];

// v9.2 硬门槛逻辑
function v92HardGate(token) {
    const { signalType, intentionTier, smCount, priceChange } = token;
    const isProtectedSignal = signalType === 'STABLE' || signalType === 'ACCELERATING';
    const isEarlyGem = false; // 假设都不是极早期

    // 涨幅检查
    const riseThresholds = {
        'TIER_S': { base: 500, accel: 2000 },
        'TIER_A': { base: 300, accel: 1000 },
        'TIER_B': { base: 150, accel: 500 },
        'TIER_C': { base: 80, accel: 200 },
        'default': { base: 80, accel: 200 }
    };
    const tierThresholds = riseThresholds[intentionTier] || riseThresholds.default;

    let maxPriceChange = isProtectedSignal ? tierThresholds.accel : tierThresholds.base;
    if (smCount >= 2 && isProtectedSignal) maxPriceChange *= 1.5;

    if (priceChange > maxPriceChange) {
        return { pass: false, reason: `涨幅过高 (+${priceChange.toFixed(1)}% > ${maxPriceChange.toFixed(0)}%)` };
    }

    // 跌幅检查
    const dropThresholds = { 'TIER_S': -60, 'TIER_A': -50, 'TIER_B': -40, 'TIER_C': -30, 'default': -30 };
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

// 旧版硬门槛
function oldHardGate(token) {
    const { priceChange } = token;
    if (priceChange > 80) return { pass: false, reason: `涨幅过高 (+${priceChange.toFixed(1)}% > 80%)` };
    if (priceChange < -30) return { pass: false, reason: `跌幅过大 (${priceChange.toFixed(1)}% < -30%)` };
    return { pass: true, reason: null };
}

console.log('═'.repeat(70));
console.log('🔬 v9.2 硬门槛回测 - 真实被拒绝案例');
console.log('═'.repeat(70));

const goldTokens = rejectedTokens.filter(t => t.isGold);
const silverTokens = rejectedTokens.filter(t => t.isSilver);
const noiseTokens = rejectedTokens.filter(t => !t.isGold && !t.isSilver);

console.log(`\n样本: ${goldTokens.length} 金狗 + ${silverTokens.length} 银狗 + ${noiseTokens.length} 噪音 = ${rejectedTokens.length} 总计\n`);

console.log('【金狗回测 (≥10x)】');
console.log('─'.repeat(70));
goldTokens.forEach(token => {
    const oldResult = oldHardGate(token);
    const v92Result = v92HardGate(token);

    const oldStatus = oldResult.pass ? '✅通过' : '❌拒绝';
    const v92Status = v92Result.pass ? '✅通过' : '❌拒绝';
    const saved = !oldResult.pass && v92Result.pass ? '🎯已救' : '';

    console.log(`${token.symbol} (${token.maxGain}x): 价格${token.priceChange >= 0 ? '+' : ''}${token.priceChange.toFixed(1)}%`);
    console.log(`  配置: ${token.intentionTier} + ${token.signalType} + SM=${token.smCount}`);
    console.log(`  旧版: ${oldStatus} ${oldResult.reason || ''}`);
    console.log(`  v9.2: ${v92Status} ${v92Result.reason || ''} ${saved}`);
    console.log('');
});

console.log('【银狗回测 (3x-10x)】');
console.log('─'.repeat(70));
silverTokens.forEach(token => {
    const oldResult = oldHardGate(token);
    const v92Result = v92HardGate(token);

    const oldStatus = oldResult.pass ? '✅通过' : '❌拒绝';
    const v92Status = v92Result.pass ? '✅通过' : '❌拒绝';
    const saved = !oldResult.pass && v92Result.pass ? '🎯已救' : '';

    console.log(`${token.symbol} (${token.maxGain}x): 价格${token.priceChange >= 0 ? '+' : ''}${token.priceChange.toFixed(1)}%`);
    console.log(`  配置: ${token.intentionTier} + ${token.signalType} + SM=${token.smCount}`);
    console.log(`  旧版: ${oldStatus} ${oldResult.reason || ''}`);
    console.log(`  v9.2: ${v92Status} ${v92Result.reason || ''} ${saved}`);
    console.log('');
});

console.log('【噪音回测 (<3x 或未知)】');
console.log('─'.repeat(70));
let noiseOldPass = 0, noiseOldFail = 0;
let noiseV92Pass = 0, noiseV92Fail = 0;

noiseTokens.forEach(token => {
    const oldResult = oldHardGate(token);
    const v92Result = v92HardGate(token);

    if (oldResult.pass) noiseOldPass++; else noiseOldFail++;
    if (v92Result.pass) noiseV92Pass++; else noiseV92Fail++;

    const oldStatus = oldResult.pass ? '✅' : '❌';
    const v92Status = v92Result.pass ? '✅' : '❌';
    const leaked = v92Result.pass && !oldResult.pass ? '⚠️放行' : '';

    console.log(`${token.symbol}: ${token.priceChange >= 0 ? '+' : ''}${token.priceChange.toFixed(1)}% | ${token.intentionTier}+${token.signalType}+SM=${token.smCount} | 旧${oldStatus} v9.2${v92Status} ${leaked}`);
});

console.log('\n' + '═'.repeat(70));
console.log('📊 回测总结');
console.log('═'.repeat(70));

const goldOldSaved = goldTokens.filter(t => oldHardGate(t).pass).length;
const goldV92Saved = goldTokens.filter(t => v92HardGate(t).pass).length;
const silverOldSaved = silverTokens.filter(t => oldHardGate(t).pass).length;
const silverV92Saved = silverTokens.filter(t => v92HardGate(t).pass).length;

console.log(`\n| 指标 | 旧版 | v9.2 | 变化 |`);
console.log(`|------|------|------|------|`);
console.log(`| 金狗存活(≥10x) | ${goldOldSaved}/${goldTokens.length} | ${goldV92Saved}/${goldTokens.length} | +${goldV92Saved - goldOldSaved} |`);
console.log(`| 银狗存活(3-10x) | ${silverOldSaved}/${silverTokens.length} | ${silverV92Saved}/${silverTokens.length} | +${silverV92Saved - silverOldSaved} |`);
console.log(`| 噪音拦截(<3x) | ${noiseOldFail}/${noiseTokens.length} | ${noiseV92Fail}/${noiseTokens.length} | ${noiseV92Fail - noiseOldFail >= 0 ? '+' : ''}${noiseV92Fail - noiseOldFail} |`);

const rescuedGolds = goldTokens.filter(t => !oldHardGate(t).pass && v92HardGate(t).pass);
const rescuedSilvers = silverTokens.filter(t => !oldHardGate(t).pass && v92HardGate(t).pass);
const leakedNoise = noiseTokens.filter(t => !oldHardGate(t).pass && v92HardGate(t).pass);

console.log(`\n🎯 v9.2 救回的金狗 (${rescuedGolds.length}个):`);
rescuedGolds.forEach(t => console.log(`  - ${t.symbol} (${t.maxGain}x)`));

console.log(`\n🥈 v9.2 救回的银狗 (${rescuedSilvers.length}个):`);
rescuedSilvers.forEach(t => console.log(`  - ${t.symbol} (${t.maxGain}x)`));

if (leakedNoise.length > 0) {
    console.log(`\n⚠️ v9.2 额外放行的噪音 (${leakedNoise.length}个):`);
    leakedNoise.forEach(t => console.log(`  - ${t.symbol}`));
} else {
    console.log(`\n✅ v9.2 未额外放行任何真正的噪音`);
}

// 计算综合收益
const goldValue = rescuedGolds.reduce((sum, t) => sum + (t.maxGain || 0), 0);
const silverValue = rescuedSilvers.reduce((sum, t) => sum + (t.maxGain || 0), 0);
console.log(`\n💰 v9.2 潜在收益增加:`);
console.log(`  - 金狗潜在收益: ${goldValue.toFixed(1)}x`);
console.log(`  - 银狗潜在收益: ${silverValue.toFixed(1)}x`);
console.log(`  - 总计: ${(goldValue + silverValue).toFixed(1)}x`);
