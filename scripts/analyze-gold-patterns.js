#!/usr/bin/env node
/**
 * 金狗特征深度分析
 *
 * 目标: 找出能区分金狗和噪音的关键特征组合
 * 目标胜率: 70%
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const dataPath = path.join(__dirname, '..', 'data', 'extended-backtest-results.json');

const data = JSON.parse(fs.readFileSync(dataPath, 'utf-8'));
const gold = data.goldFeatures;
const noise = data.noiseFeatures;

console.log(`\n${'═'.repeat(70)}`);
console.log(`🔬 金狗特征深度分析 - 寻找70%胜率组合`);
console.log(`${'═'.repeat(70)}`);
console.log(`样本: ${gold.length}金狗 vs ${noise.length}噪音`);

// 先应用BALANCED过滤
function balancedFilter(f) {
    return f.signalTrendType !== 'DECAYING' &&
           f.smCount >= 1 &&
           f.baseScore >= 50;
}

const balancedGold = gold.filter(balancedFilter);
const balancedNoise = noise.filter(balancedFilter);

console.log(`\nBALANCED过滤后: ${balancedGold.length}金狗 vs ${balancedNoise.length}噪音`);
console.log(`当前精确率: ${(balancedGold.length / (balancedGold.length + balancedNoise.length) * 100).toFixed(1)}%`);
console.log(`目标精确率: 70%`);

// 分析各特征的分布差异
console.log(`\n${'═'.repeat(70)}`);
console.log(`📊 特征分布分析 (BALANCED过滤后)`);
console.log(`${'═'.repeat(70)}`);

// 1. 信号类型分布
console.log(`\n【信号类型分布】`);
const signalTypes = ['ACCELERATING', 'STABLE', 'DECAYING'];
signalTypes.forEach(type => {
    const goldCount = balancedGold.filter(f => f.signalTrendType === type).length;
    const noiseCount = balancedNoise.filter(f => f.signalTrendType === type).length;
    const goldPct = (goldCount / balancedGold.length * 100).toFixed(1);
    const noisePct = (noiseCount / balancedNoise.length * 100).toFixed(1);
    const ratio = noiseCount > 0 ? (goldCount / balancedGold.length) / (noiseCount / balancedNoise.length) : Infinity;
    console.log(`  ${type.padEnd(15)} | 金狗: ${goldPct.padStart(5)}% (${goldCount}) | 噪音: ${noisePct.padStart(5)}% (${noiseCount}) | 区分度: ${ratio.toFixed(2)}x`);
});

// 2. SM数量分布
console.log(`\n【聪明钱数量分布】`);
const smRanges = [
    { name: 'SM=1', filter: f => f.smCount === 1 },
    { name: 'SM=2', filter: f => f.smCount === 2 },
    { name: 'SM=3-4', filter: f => f.smCount >= 3 && f.smCount <= 4 },
    { name: 'SM≥5', filter: f => f.smCount >= 5 }
];
smRanges.forEach(range => {
    const goldCount = balancedGold.filter(range.filter).length;
    const noiseCount = balancedNoise.filter(range.filter).length;
    const precision = goldCount + noiseCount > 0 ? (goldCount / (goldCount + noiseCount) * 100).toFixed(1) : 0;
    console.log(`  ${range.name.padEnd(10)} | 金狗: ${goldCount} | 噪音: ${noiseCount} | 精确率: ${precision}%`);
});

// 3. 基础评分分布
console.log(`\n【基础评分分布】`);
const scoreRanges = [
    { name: '50-54', filter: f => f.baseScore >= 50 && f.baseScore < 55 },
    { name: '55-59', filter: f => f.baseScore >= 55 && f.baseScore < 60 },
    { name: '60-64', filter: f => f.baseScore >= 60 && f.baseScore < 65 },
    { name: '65-69', filter: f => f.baseScore >= 65 && f.baseScore < 70 },
    { name: '≥70', filter: f => f.baseScore >= 70 }
];
scoreRanges.forEach(range => {
    const goldCount = balancedGold.filter(range.filter).length;
    const noiseCount = balancedNoise.filter(range.filter).length;
    const precision = goldCount + noiseCount > 0 ? (goldCount / (goldCount + noiseCount) * 100).toFixed(1) : 0;
    console.log(`  Score ${range.name.padEnd(6)} | 金狗: ${goldCount.toString().padStart(2)} | 噪音: ${noiseCount.toString().padStart(3)} | 精确率: ${precision}%`);
});

// 4. 寻找高精确率组合
console.log(`\n${'═'.repeat(70)}`);
console.log(`🎯 寻找高精确率组合 (目标≥70%)`);
console.log(`${'═'.repeat(70)}`);

const highPrecisionCombos = [];

// 测试所有可能的组合
const signalOptions = [
    { name: '任意信号', filter: f => true },
    { name: 'ACCEL', filter: f => f.signalTrendType === 'ACCELERATING' }
];
const smOptions = [1, 2, 3, 4, 5, 6, 7];
const scoreOptions = [50, 55, 60, 65, 70];

signalOptions.forEach(sig => {
    smOptions.forEach(smMin => {
        scoreOptions.forEach(scoreMin => {
            const combo = f => sig.filter(f) && f.smCount >= smMin && f.baseScore >= scoreMin;
            const goldPass = balancedGold.filter(combo).length;
            const noisePass = balancedNoise.filter(combo).length;

            if (goldPass === 0) return;

            const precision = goldPass / (goldPass + noisePass) * 100;
            const totalPass = goldPass + noisePass;

            if (precision >= 30 && goldPass >= 3) { // 先看30%以上的
                highPrecisionCombos.push({
                    name: `${sig.name} + SM≥${smMin} + Score≥${scoreMin}`,
                    goldPass,
                    noisePass,
                    totalPass,
                    precision,
                    goldCoverage: goldPass / balancedGold.length * 100
                });
            }
        });
    });
});

// 按精确率排序
highPrecisionCombos.sort((a, b) => b.precision - a.precision);

console.log(`\n| 组合 | 金狗 | 噪音 | 精确率 | 金狗覆盖 |`);
console.log(`|------|------|------|--------|----------|`);
highPrecisionCombos.slice(0, 20).forEach(c => {
    const precisionMark = c.precision >= 70 ? '🏆' : c.precision >= 50 ? '⭐' : '';
    console.log(`| ${c.name.padEnd(30)} | ${c.goldPass.toString().padStart(4)} | ${c.noisePass.toString().padStart(4)} | ${c.precision.toFixed(1).padStart(5)}% ${precisionMark} | ${c.goldCoverage.toFixed(1).padStart(5)}% |`);
});

// 5. 分析横向对比的可能性
console.log(`\n${'═'.repeat(70)}`);
console.log(`📈 横向对比分析 (同批次选优)`);
console.log(`${'═'.repeat(70)}`);

// 按日志来源分组
const logGroups = {};
[...balancedGold, ...balancedNoise].forEach(f => {
    const source = f.source || 'unknown';
    if (!logGroups[source]) {
        logGroups[source] = { gold: [], noise: [] };
    }
    if (balancedGold.includes(f)) {
        logGroups[source].gold.push(f);
    } else {
        logGroups[source].noise.push(f);
    }
});

// 分析每个批次的金狗是否有更高的评分/SM
let batchesWithGold = 0;
let goldIsTopScoreCount = 0;
let goldIsTopSmCount = 0;

Object.entries(logGroups).forEach(([source, group]) => {
    if (group.gold.length === 0) return;

    batchesWithGold++;
    const allInBatch = [...group.gold, ...group.noise];

    // 按评分排序
    allInBatch.sort((a, b) => b.baseScore - a.baseScore);
    const topScorer = allInBatch[0];
    if (group.gold.some(g => g.symbol === topScorer.symbol)) {
        goldIsTopScoreCount++;
    }

    // 按SM排序
    allInBatch.sort((a, b) => b.smCount - a.smCount);
    const topSm = allInBatch[0];
    if (group.gold.some(g => g.symbol === topSm.symbol)) {
        goldIsTopSmCount++;
    }
});

console.log(`\n有金狗的批次: ${batchesWithGold}`);
console.log(`金狗是批次最高评分: ${goldIsTopScoreCount}/${batchesWithGold} (${(goldIsTopScoreCount/batchesWithGold*100).toFixed(1)}%)`);
console.log(`金狗是批次最高SM: ${goldIsTopSmCount}/${batchesWithGold} (${(goldIsTopSmCount/batchesWithGold*100).toFixed(1)}%)`);

// 6. 综合评分模型
console.log(`\n${'═'.repeat(70)}`);
console.log(`🧮 综合评分模型设计`);
console.log(`${'═'.repeat(70)}`);

// 设计一个综合评分公式
function calculateGoldScore(f) {
    let score = 0;

    // 信号类型权重 (基于区分度)
    if (f.signalTrendType === 'ACCELERATING') score += 30;
    else if (f.signalTrendType === 'STABLE') score += 10;

    // SM数量权重
    score += Math.min(f.smCount * 5, 25); // 最多25分

    // 基础评分权重
    score += Math.max(0, (f.baseScore - 50) * 0.5); // 50分以上每分加0.5

    return score;
}

// 测试这个评分模型
const goldScores = balancedGold.map(f => ({ ...f, goldScore: calculateGoldScore(f) }));
const noiseScores = balancedNoise.map(f => ({ ...f, goldScore: calculateGoldScore(f) }));

// 分析不同阈值的精确率
console.log(`\n【综合评分阈值分析】`);
const thresholds = [30, 35, 40, 45, 50, 55, 60];
thresholds.forEach(threshold => {
    const goldPass = goldScores.filter(f => f.goldScore >= threshold).length;
    const noisePass = noiseScores.filter(f => f.goldScore >= threshold).length;
    const precision = goldPass + noisePass > 0 ? (goldPass / (goldPass + noisePass) * 100) : 0;
    const coverage = (goldPass / balancedGold.length * 100);
    console.log(`  阈值≥${threshold}: 金狗${goldPass} + 噪音${noisePass} = 精确率${precision.toFixed(1)}%, 覆盖${coverage.toFixed(1)}%`);
});

// 7. 结论
console.log(`\n${'═'.repeat(70)}`);
console.log(`📋 结论与建议`);
console.log(`${'═'.repeat(70)}`);

const best70 = highPrecisionCombos.find(c => c.precision >= 70);
const best50 = highPrecisionCombos.find(c => c.precision >= 50);

if (best70) {
    console.log(`\n✅ 找到70%+精确率组合:`);
    console.log(`   ${best70.name}`);
    console.log(`   精确率: ${best70.precision.toFixed(1)}% | 金狗覆盖: ${best70.goldCoverage.toFixed(1)}%`);
} else if (best50) {
    console.log(`\n⚠️ 最高精确率组合 (未达70%):`);
    console.log(`   ${best50.name}`);
    console.log(`   精确率: ${best50.precision.toFixed(1)}% | 金狗覆盖: ${best50.goldCoverage.toFixed(1)}%`);
} else {
    console.log(`\n❌ 纯规则无法达到高精确率，需要AI辅助`);
}

console.log(`\n【达到70%胜率的建议方案】`);
console.log(`  1. 基础过滤: BALANCED (保证82%金狗通过)`);
console.log(`  2. 信号优先: 优先选择 ACCELERATING 信号 (3x区分度)`);
console.log(`  3. SM筛选: SM≥5 有更高精确率`);
console.log(`  4. 横向对比: 每批次选评分最高的1-2个`);
console.log(`  5. AI验证: 用Grok判断叙事和社交热度`);
console.log(`  6. 仓位分层: 高置信高仓位，低置信低仓位`);

// 保存分析结果
const analysisOutput = {
    timestamp: new Date().toISOString(),
    balancedStats: {
        gold: balancedGold.length,
        noise: balancedNoise.length,
        precision: balancedGold.length / (balancedGold.length + balancedNoise.length) * 100
    },
    highPrecisionCombos: highPrecisionCombos.slice(0, 30),
    batchAnalysis: {
        batchesWithGold,
        goldIsTopScore: goldIsTopScoreCount / batchesWithGold * 100,
        goldIsTpoSm: goldIsTopSmCount / batchesWithGold * 100
    }
};

fs.writeFileSync(
    path.join(__dirname, '..', 'data', 'gold-dog-pattern-analysis.json'),
    JSON.stringify(analysisOutput, null, 2)
);

console.log(`\n✅ 分析结果已保存到 data/gold-dog-pattern-analysis.json`);
