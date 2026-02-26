#!/usr/bin/env node
/**
 * 帕累托前沿分析 - 寻找最优过滤策略平衡点
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
console.log(`🔍 寻找最优平衡点 (Pareto前沿分析)`);
console.log(`样本: ${gold.length}金狗 vs ${noise.length}噪音`);
console.log(`${'═'.repeat(70)}`);

// 更全面的策略网格
const allStrategies = [];

// 基于signalTrendType的组合
const signalOptions = [
    { name: "任意信号", filter: f => true },
    { name: "非DECAYING", filter: f => f.signalTrendType !== "DECAYING" },
    { name: "STABLE|ACCEL", filter: f => f.signalTrendType === "STABLE" || f.signalTrendType === "ACCELERATING" },
    { name: "仅ACCEL", filter: f => f.signalTrendType === "ACCELERATING" }
];

const smOptions = [1, 2, 3, 4, 5];
const scoreOptions = [45, 50, 55, 60, 65];

signalOptions.forEach(sig => {
    smOptions.forEach(sm => {
        scoreOptions.forEach(score => {
            const filter = f => sig.filter(f) && f.smCount >= sm && f.baseScore >= score;
            const name = `${sig.name} + SM≥${sm} + Score≥${score}`;

            const goldPass = gold.filter(filter).length;
            const noisePass = noise.filter(filter).length;

            if (goldPass === 0) return;

            const goldPassRate = goldPass / gold.length * 100;
            const noiseFilterRate = (1 - noisePass / noise.length) * 100;
            const precision = goldPass / (goldPass + noisePass) * 100;
            const composite = goldPassRate * 0.6 + noiseFilterRate * 0.4;

            allStrategies.push({
                name, goldPassRate, noiseFilterRate, precision, composite,
                goldPass, noisePass, sig: sig.name, sm, score
            });
        });
    });
});

// 找帕累托前沿
const paretoFront = [];
allStrategies.forEach(s => {
    const dominated = allStrategies.some(other =>
        other.goldPassRate >= s.goldPassRate &&
        other.noiseFilterRate >= s.noiseFilterRate &&
        (other.goldPassRate > s.goldPassRate || other.noiseFilterRate > s.noiseFilterRate)
    );
    if (!dominated) {
        paretoFront.push(s);
    }
});

paretoFront.sort((a, b) => b.goldPassRate - a.goldPassRate);

console.log(`\n【帕累托前沿策略】(${paretoFront.length}个非支配解)`);
console.log('─'.repeat(100));
console.log('策略                                  | 金狗通过 | 噪音过滤 | 精确率  | 综合分');
console.log('─'.repeat(100));

paretoFront.forEach(r => {
    console.log(
        `${r.name.padEnd(38)} | ` +
        `${r.goldPassRate.toFixed(1).padStart(5)}% | ` +
        `${r.noiseFilterRate.toFixed(1).padStart(5)}% | ` +
        `${r.precision.toFixed(1).padStart(5)}% | ` +
        `${r.composite.toFixed(1).padStart(5)}`
    );
});

// 分析不同目标下的最佳策略
console.log(`\n${'═'.repeat(70)}`);
console.log(`🎯 不同目标下的推荐策略`);
console.log(`${'═'.repeat(70)}`);

// 最高综合分
const bestComposite = [...allStrategies].sort((a, b) => b.composite - a.composite)[0];
console.log(`\n1. 综合最佳 (平衡金狗通过与噪音过滤):`);
console.log(`   ${bestComposite.name}`);
console.log(`   金狗: ${bestComposite.goldPassRate.toFixed(1)}% (${bestComposite.goldPass}/${gold.length})`);
console.log(`   噪音过滤: ${bestComposite.noiseFilterRate.toFixed(1)}% | 精确率: ${bestComposite.precision.toFixed(1)}%`);

// 最高精确率(金狗>10个)
const highPrecision = [...allStrategies].filter(s => s.goldPass >= 10).sort((a, b) => b.precision - a.precision)[0];
console.log(`\n2. 最高精确率 (至少10个金狗通过):`);
console.log(`   ${highPrecision.name}`);
console.log(`   金狗: ${highPrecision.goldPassRate.toFixed(1)}% (${highPrecision.goldPass}/${gold.length})`);
console.log(`   噪音过滤: ${highPrecision.noiseFilterRate.toFixed(1)}% | 精确率: ${highPrecision.precision.toFixed(1)}%`);

// 金狗通过>50%且噪音过滤>50%
const balanced = [...allStrategies].filter(s => s.goldPassRate >= 50 && s.noiseFilterRate >= 50).sort((a, b) => b.precision - a.precision)[0];
if (balanced) {
    console.log(`\n3. 平衡策略 (金狗>50% & 噪音过滤>50%):`);
    console.log(`   ${balanced.name}`);
    console.log(`   金狗: ${balanced.goldPassRate.toFixed(1)}% | 噪音过滤: ${balanced.noiseFilterRate.toFixed(1)}% | 精确率: ${balanced.precision.toFixed(1)}%`);
} else {
    console.log(`\n3. 平衡策略: 无法同时达到金狗>50%且噪音过滤>50%`);

    const nearBalance = [...allStrategies].sort((a, b) => {
        const aBalance = Math.min(a.goldPassRate, a.noiseFilterRate);
        const bBalance = Math.min(b.goldPassRate, b.noiseFilterRate);
        return bBalance - aBalance;
    })[0];
    console.log(`   最接近平衡: ${nearBalance.name}`);
    console.log(`   金狗: ${nearBalance.goldPassRate.toFixed(1)}% | 噪音过滤: ${nearBalance.noiseFilterRate.toFixed(1)}%`);
}

// 生成推荐的配置
console.log(`\n${'═'.repeat(70)}`);
console.log(`📋 推荐配置预设 (更新 filter-params.js)`);
console.log(`${'═'.repeat(70)}`);

// 找到最佳的三个策略组合
const aggressive = [...allStrategies].filter(s => s.goldPassRate >= 80).sort((a, b) => b.noiseFilterRate - a.noiseFilterRate)[0];
const conservative = [...allStrategies].filter(s => s.precision >= 10).sort((a, b) => b.goldPassRate - a.goldPassRate)[0];
const ultraConservative = [...allStrategies].sort((a, b) => b.precision - a.precision)[0];

const presets = [
    { key: 'AGGRESSIVE', data: aggressive || bestComposite, desc: '金狗优先(≥80%)' },
    { key: 'BALANCED', data: bestComposite, desc: '综合最优' },
    { key: 'CONSERVATIVE', data: conservative || highPrecision, desc: '精确率≥10%' },
    { key: 'ULTRA_CONSERVATIVE', data: ultraConservative, desc: '最高精确率' }
];

presets.forEach(p => {
    if (!p.data) return;
    console.log(`\n${p.key} (${p.desc}):`);

    // 解析信号类型要求
    let requireSignalAccelerating = p.data.sig === '仅ACCEL';
    let signalDesc = p.data.sig;

    console.log(`  信号类型要求: ${signalDesc} → requireSignalAccelerating: ${requireSignalAccelerating}`);
    console.log(`  聪明钱阈值: ≥${p.data.sm}`);
    console.log(`  基础分阈值: ≥${p.data.score}`);
    console.log(`  预期效果:`);
    console.log(`    金狗通过: ${p.data.goldPassRate.toFixed(1)}% (${p.data.goldPass}/${gold.length})`);
    console.log(`    噪音过滤: ${p.data.noiseFilterRate.toFixed(1)}%`);
    console.log(`    精确率: ${p.data.precision.toFixed(1)}%`);
});

// 导出最优配置
const optimalConfig = {
    AGGRESSIVE: aggressive || bestComposite ? {
        smCountThreshold: (aggressive || bestComposite).sm,
        baseScoreThreshold: (aggressive || bestComposite).score,
        requireSignalAccelerating: (aggressive || bestComposite).sig === '仅ACCEL',
        expectedGoldPassRate: (aggressive || bestComposite).goldPassRate,
        expectedNoiseFilterRate: (aggressive || bestComposite).noiseFilterRate,
        expectedPrecision: (aggressive || bestComposite).precision
    } : null,
    BALANCED: {
        smCountThreshold: bestComposite.sm,
        baseScoreThreshold: bestComposite.score,
        requireSignalAccelerating: bestComposite.sig === '仅ACCEL',
        expectedGoldPassRate: bestComposite.goldPassRate,
        expectedNoiseFilterRate: bestComposite.noiseFilterRate,
        expectedPrecision: bestComposite.precision
    },
    CONSERVATIVE: conservative ? {
        smCountThreshold: conservative.sm,
        baseScoreThreshold: conservative.score,
        requireSignalAccelerating: conservative.sig === '仅ACCEL',
        expectedGoldPassRate: conservative.goldPassRate,
        expectedNoiseFilterRate: conservative.noiseFilterRate,
        expectedPrecision: conservative.precision
    } : null,
    ULTRA_CONSERVATIVE: {
        smCountThreshold: ultraConservative.sm,
        baseScoreThreshold: ultraConservative.score,
        requireSignalAccelerating: ultraConservative.sig === '仅ACCEL',
        expectedGoldPassRate: ultraConservative.goldPassRate,
        expectedNoiseFilterRate: ultraConservative.noiseFilterRate,
        expectedPrecision: ultraConservative.precision
    }
};

// 保存分析结果
const outputPath = path.join(__dirname, '..', 'data', 'pareto-analysis.json');
fs.writeFileSync(outputPath, JSON.stringify({
    timestamp: new Date().toISOString(),
    sampleSize: { gold: gold.length, noise: noise.length },
    paretoFront: paretoFront,
    recommendedConfigs: optimalConfig,
    allStrategies: allStrategies.slice(0, 50) // 保存前50个
}, null, 2));

console.log(`\n✅ 分析结果已保存到 data/pareto-analysis.json`);
