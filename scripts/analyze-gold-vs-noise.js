/**
 * 分析金狗与噪音的特征差异
 */

import fs from 'fs';

const data = JSON.parse(fs.readFileSync('data/feature-analysis-results.json', 'utf8'));
const golds = data.goldFeatures || [];
const noises = data.noiseFeatures || [];

// 去重
const uniqueGolds = [];
const seenG = new Set();
golds.forEach(g => {
    if (!seenG.has(g.symbol)) {
        seenG.add(g.symbol);
        uniqueGolds.push(g);
    }
});

const uniqueNoises = [];
const seenN = new Set();
noises.forEach(n => {
    if (!seenN.has(n.symbol)) {
        seenN.add(n.symbol);
        uniqueNoises.push(n);
    }
});

// 比较关键指标
const goldStats = { baseScore: 0, smCount: 0, safetyScore: 0, aiScore: 0 };
const noiseStats = { baseScore: 0, smCount: 0, safetyScore: 0, aiScore: 0 };

uniqueGolds.forEach(g => {
    goldStats.baseScore += g.baseScore || 0;
    goldStats.smCount += g.smCount || 0;
    goldStats.safetyScore += g.safetyScore || 0;
    goldStats.aiScore += g.aiScore || 0;
});

uniqueNoises.forEach(n => {
    noiseStats.baseScore += n.baseScore || 0;
    noiseStats.smCount += n.smCount || 0;
    noiseStats.safetyScore += n.safetyScore || 0;
    noiseStats.aiScore += n.aiScore || 0;
});

console.log('═'.repeat(60));
console.log('金狗 vs 噪音 特征分析');
console.log('═'.repeat(60));

console.log(`\n金狗平均值 (${uniqueGolds.length} 个):`);
console.log(`  baseScore: ${(goldStats.baseScore / uniqueGolds.length).toFixed(1)}`);
console.log(`  smCount: ${(goldStats.smCount / uniqueGolds.length).toFixed(1)}`);
console.log(`  safetyScore: ${(goldStats.safetyScore / uniqueGolds.length).toFixed(1)}`);
console.log(`  aiScore: ${(goldStats.aiScore / uniqueGolds.length).toFixed(1)}`);

console.log(`\n噪音平均值 (${uniqueNoises.length} 个):`);
console.log(`  baseScore: ${(noiseStats.baseScore / uniqueNoises.length).toFixed(1)}`);
console.log(`  smCount: ${(noiseStats.smCount / uniqueNoises.length).toFixed(1)}`);
console.log(`  safetyScore: ${(noiseStats.safetyScore / uniqueNoises.length).toFixed(1)}`);
console.log(`  aiScore: ${(noiseStats.aiScore / uniqueNoises.length).toFixed(1)}`);

// 信号类型分布对比
console.log('\n信号类型分布对比:');
const goldSignal = {};
const noiseSignal = {};

uniqueGolds.forEach(g => {
    const sig = g.signalTrendType || 'undefined';
    goldSignal[sig] = (goldSignal[sig] || 0) + 1;
});

uniqueNoises.forEach(n => {
    const sig = n.signalTrendType || 'undefined';
    noiseSignal[sig] = (noiseSignal[sig] || 0) + 1;
});

console.log('  金狗:', goldSignal);
console.log('  噪音:', noiseSignal);

// 找出关键区分因素
console.log('\n关键区分点:');

// baseScore 分布
const goldHighScore = uniqueGolds.filter(g => g.baseScore >= 60).length;
const noiseHighScore = uniqueNoises.filter(n => n.baseScore >= 60).length;
console.log(`  baseScore >= 60: 金狗 ${(goldHighScore/uniqueGolds.length*100).toFixed(0)}% vs 噪音 ${(noiseHighScore/uniqueNoises.length*100).toFixed(0)}%`);

// safetyScore 分布
const goldHighSafety = uniqueGolds.filter(g => g.safetyScore >= 15).length;
const noiseHighSafety = uniqueNoises.filter(n => n.safetyScore >= 15).length;
console.log(`  safetyScore >= 15: 金狗 ${(goldHighSafety/uniqueGolds.length*100).toFixed(0)}% vs 噪音 ${(noiseHighSafety/uniqueNoises.length*100).toFixed(0)}%`);

// SM + ACCELERATING 组合
const goldCombo = uniqueGolds.filter(g => g.smCount >= 2 && g.signalTrendType === 'ACCELERATING').length;
const noiseCombo = uniqueNoises.filter(n => n.smCount >= 2 && n.signalTrendType === 'ACCELERATING').length;
console.log(`  SM>=2 + ACCEL: 金狗 ${(goldCombo/uniqueGolds.length*100).toFixed(0)}% vs 噪音 ${(noiseCombo/uniqueNoises.length*100).toFixed(0)}%`);

console.log('\n建议: 硬门槛应结合 baseScore 和 safetyScore 来区分');
