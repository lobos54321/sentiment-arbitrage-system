#!/usr/bin/env node
/**
 * 多维参数网格搜索脚本
 * 寻找最优过滤策略组合
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// 加载特征分析数据
const dataPath = path.join(__dirname, '..', 'data', 'feature-analysis-results.json');
const { goldFeatures, noiseFeatures } = JSON.parse(fs.readFileSync(dataPath, 'utf-8'));

console.log(`加载数据: 金狗${goldFeatures.length}个, 噪音${noiseFeatures.length}个`);

// ═══════════════════════════════════════════════════════════════
// 参数空间定义
// ═══════════════════════════════════════════════════════════════

const parameterSpace = {
    // 七维分阈值
    sevenDimThreshold: [15, 16, 17, 18, 19, 20, 21, 22],

    // 聪明钱数量阈值
    smCountThreshold: [1, 2, 3, 4, 5],

    // 趋势类型要求
    requireTrendIncreasing: [true, false],

    // 信号趋势要求
    requireSignalAccelerating: [true, false],

    // 接盘警告处理
    allowLateFollower: [true, false],

    // 基础分阈值
    baseScoreThreshold: [50, 55, 60, 65],

    // 市值过滤 (null = 不限制)
    maxMcap: [null, 500000, 1000000, 1500000, 2000000]
};

// ═══════════════════════════════════════════════════════════════
// 评估函数
// ═══════════════════════════════════════════════════════════════

function evaluateStrategy(params) {
    const {
        sevenDimThreshold,
        smCountThreshold,
        requireTrendIncreasing,
        requireSignalAccelerating,
        allowLateFollower,
        baseScoreThreshold,
        maxMcap
    } = params;

    // 定义过滤条件
    const shouldPass = (f) => {
        // 基础分检查
        if (f.baseScore < baseScoreThreshold) return false;

        // 七维分检查
        if (f.sevenDimScore !== null && f.sevenDimScore < sevenDimThreshold) return false;

        // 聪明钱数量检查
        if (f.smCount !== null && f.smCount < smCountThreshold) return false;

        // 趋势类型检查
        if (requireTrendIncreasing && f.trendType !== 'INCREASING') return false;

        // 信号趋势检查
        if (requireSignalAccelerating && f.signalTrendType !== 'ACCELERATING') return false;

        // 接盘警告检查
        if (!allowLateFollower && f.lateFollower) return false;

        // 市值检查
        if (maxMcap !== null && f.mcap !== null && f.mcap > maxMcap) return false;

        return true;
    };

    // 计算金狗通过率
    const goldPass = goldFeatures.filter(shouldPass).length;
    const goldPassRate = goldPass / goldFeatures.length * 100;

    // 计算噪音过滤率 (被正确过滤的比例)
    const noiseFiltered = noiseFeatures.filter(f => !shouldPass(f)).length;
    const noiseFilterRate = noiseFiltered / noiseFeatures.length * 100;

    // 综合得分: 金狗通过率权重0.6, 噪音过滤率权重0.4
    const compositeScore = goldPassRate * 0.6 + noiseFilterRate * 0.4;

    // 计算精确率 (通过的token中金狗占比)
    const totalPass = goldPass + (noiseFeatures.length - noiseFiltered);
    const precision = totalPass > 0 ? (goldPass / totalPass * 100) : 0;

    return {
        params,
        goldPassRate,
        noiseFilterRate,
        compositeScore,
        precision,
        goldPass,
        noisePass: noiseFeatures.length - noiseFiltered
    };
}

// ═══════════════════════════════════════════════════════════════
// 网格搜索
// ═══════════════════════════════════════════════════════════════

console.log(`\n${'═'.repeat(70)}`);
console.log(`🔍 开始多维参数网格搜索`);
console.log(`${'═'.repeat(70)}\n`);

const results = [];
let totalCombinations = 1;
Object.values(parameterSpace).forEach(values => {
    totalCombinations *= values.length;
});

console.log(`总共 ${totalCombinations} 种参数组合...\n`);

// 遍历所有参数组合
let count = 0;
for (const sevenDimThreshold of parameterSpace.sevenDimThreshold) {
    for (const smCountThreshold of parameterSpace.smCountThreshold) {
        for (const requireTrendIncreasing of parameterSpace.requireTrendIncreasing) {
            for (const requireSignalAccelerating of parameterSpace.requireSignalAccelerating) {
                for (const allowLateFollower of parameterSpace.allowLateFollower) {
                    for (const baseScoreThreshold of parameterSpace.baseScoreThreshold) {
                        for (const maxMcap of parameterSpace.maxMcap) {
                            count++;

                            const result = evaluateStrategy({
                                sevenDimThreshold,
                                smCountThreshold,
                                requireTrendIncreasing,
                                requireSignalAccelerating,
                                allowLateFollower,
                                baseScoreThreshold,
                                maxMcap
                            });

                            results.push(result);
                        }
                    }
                }
            }
        }
    }
}

// 按综合得分排序
results.sort((a, b) => b.compositeScore - a.compositeScore);

// ═══════════════════════════════════════════════════════════════
// 输出最佳策略
// ═══════════════════════════════════════════════════════════════

console.log(`${'═'.repeat(70)}`);
console.log(`🏆 综合得分 TOP 20 策略`);
console.log(`${'═'.repeat(70)}\n`);

console.log('排名 | 金狗通过 | 噪音过滤 | 精确率 | 综合分 | 参数');
console.log('─'.repeat(100));

results.slice(0, 20).forEach((r, i) => {
    const params = [];
    if (r.params.sevenDimThreshold !== 15) params.push(`7维≥${r.params.sevenDimThreshold}`);
    if (r.params.smCountThreshold !== 1) params.push(`SM≥${r.params.smCountThreshold}`);
    if (r.params.requireTrendIncreasing) params.push('趋势↑');
    if (r.params.requireSignalAccelerating) params.push('信号加速');
    if (!r.params.allowLateFollower) params.push('无接盘');
    if (r.params.baseScoreThreshold !== 50) params.push(`基础≥${r.params.baseScoreThreshold}`);
    if (r.params.maxMcap) params.push(`市值<${r.params.maxMcap/1000}K`);

    console.log(
        `${(i+1).toString().padStart(2)} | ` +
        `${r.goldPassRate.toFixed(1).padStart(5)}% | ` +
        `${r.noiseFilterRate.toFixed(1).padStart(5)}% | ` +
        `${r.precision.toFixed(1).padStart(5)}% | ` +
        `${r.compositeScore.toFixed(1).padStart(5)} | ` +
        `${params.join(' + ')}`
    );
});

// ═══════════════════════════════════════════════════════════════
// 帕累托前沿分析 (金狗通过 vs 噪音过滤)
// ═══════════════════════════════════════════════════════════════

console.log(`\n${'═'.repeat(70)}`);
console.log(`📊 帕累托前沿分析 (金狗通过率 vs 噪音过滤率)`);
console.log(`${'═'.repeat(70)}\n`);

// 找帕累托最优解
const paretoFront = [];
for (const r of results) {
    let isDominated = false;
    for (const other of results) {
        if (other.goldPassRate >= r.goldPassRate &&
            other.noiseFilterRate >= r.noiseFilterRate &&
            (other.goldPassRate > r.goldPassRate || other.noiseFilterRate > r.noiseFilterRate)) {
            isDominated = true;
            break;
        }
    }
    if (!isDominated) {
        // 检查是否已有相同的结果
        const exists = paretoFront.some(p =>
            p.goldPassRate === r.goldPassRate && p.noiseFilterRate === r.noiseFilterRate
        );
        if (!exists) {
            paretoFront.push(r);
        }
    }
}

// 按金狗通过率排序
paretoFront.sort((a, b) => b.goldPassRate - a.goldPassRate);

console.log('帕累托最优策略 (无法同时提高金狗通过率和噪音过滤率):');
console.log('金狗通过 | 噪音过滤 | 精确率 | 参数');
console.log('─'.repeat(90));

for (const r of paretoFront.slice(0, 15)) {
    const params = [];
    if (r.params.sevenDimThreshold !== 15) params.push(`7维≥${r.params.sevenDimThreshold}`);
    if (r.params.smCountThreshold !== 1) params.push(`SM≥${r.params.smCountThreshold}`);
    if (r.params.requireTrendIncreasing) params.push('趋势↑');
    if (r.params.requireSignalAccelerating) params.push('信号加速');
    if (!r.params.allowLateFollower) params.push('无接盘');
    if (r.params.baseScoreThreshold !== 50) params.push(`基础≥${r.params.baseScoreThreshold}`);
    if (r.params.maxMcap) params.push(`市值<${r.params.maxMcap/1000}K`);

    console.log(
        `${r.goldPassRate.toFixed(1).padStart(6)}% | ` +
        `${r.noiseFilterRate.toFixed(1).padStart(6)}% | ` +
        `${r.precision.toFixed(1).padStart(5)}% | ` +
        `${params.join(' + ') || '无额外条件'}`
    );
}

// ═══════════════════════════════════════════════════════════════
// 平衡策略推荐
// ═══════════════════════════════════════════════════════════════

console.log(`\n${'═'.repeat(70)}`);
console.log(`💡 平衡策略推荐`);
console.log(`${'═'.repeat(70)}\n`);

// 找到金狗通过率>70%且噪音过滤率最高的策略
const balancedStrategies = results.filter(r => r.goldPassRate >= 70);
balancedStrategies.sort((a, b) => b.noiseFilterRate - a.noiseFilterRate);

console.log('金狗通过率 ≥ 70% 的策略中，噪音过滤率最高的:');
console.log('─'.repeat(90));

for (const r of balancedStrategies.slice(0, 10)) {
    const params = [];
    params.push(`7维≥${r.params.sevenDimThreshold}`);
    params.push(`SM≥${r.params.smCountThreshold}`);
    if (r.params.requireTrendIncreasing) params.push('趋势↑');
    if (r.params.requireSignalAccelerating) params.push('信号加速');
    if (!r.params.allowLateFollower) params.push('无接盘');
    params.push(`基础≥${r.params.baseScoreThreshold}`);
    if (r.params.maxMcap) params.push(`市值<${r.params.maxMcap/1000}K`);

    console.log(
        `金狗: ${r.goldPassRate.toFixed(1).padStart(5)}% | ` +
        `噪音过滤: ${r.noiseFilterRate.toFixed(1).padStart(5)}% | ` +
        `精确率: ${r.precision.toFixed(1).padStart(5)}% | ` +
        `${params.join(' + ')}`
    );
}

// ═══════════════════════════════════════════════════════════════
// 高精确率策略
// ═══════════════════════════════════════════════════════════════

console.log(`\n${'═'.repeat(70)}`);
console.log(`🎯 高精确率策略 (精确率 = 通过的token中金狗占比)`);
console.log(`${'═'.repeat(70)}\n`);

// 找到金狗通过率>50%且精确率最高的策略
const precisionStrategies = results.filter(r => r.goldPassRate >= 50 && r.goldPass > 0);
precisionStrategies.sort((a, b) => b.precision - a.precision);

console.log('金狗通过率 ≥ 50% 的策略中，精确率最高的:');
console.log('─'.repeat(90));

for (const r of precisionStrategies.slice(0, 10)) {
    const params = [];
    params.push(`7维≥${r.params.sevenDimThreshold}`);
    params.push(`SM≥${r.params.smCountThreshold}`);
    if (r.params.requireTrendIncreasing) params.push('趋势↑');
    if (r.params.requireSignalAccelerating) params.push('信号加速');
    if (!r.params.allowLateFollower) params.push('无接盘');
    params.push(`基础≥${r.params.baseScoreThreshold}`);
    if (r.params.maxMcap) params.push(`市值<${r.params.maxMcap/1000}K`);

    console.log(
        `精确率: ${r.precision.toFixed(1).padStart(5)}% (${r.goldPass}金/${r.goldPass + r.noisePass}总) | ` +
        `金狗: ${r.goldPassRate.toFixed(1).padStart(5)}% | ` +
        `${params.join(' + ')}`
    );
}

// ═══════════════════════════════════════════════════════════════
// 保存结果
// ═══════════════════════════════════════════════════════════════

const outputData = {
    totalCombinations,
    top20: results.slice(0, 20),
    paretoFront: paretoFront.slice(0, 15),
    balancedStrategies: balancedStrategies.slice(0, 10),
    precisionStrategies: precisionStrategies.slice(0, 10),
    bestOverall: results[0],
    bestBalanced: balancedStrategies[0],
    bestPrecision: precisionStrategies[0]
};

fs.writeFileSync(
    path.join(__dirname, '..', 'data', 'parameter-grid-search-results.json'),
    JSON.stringify(outputData, null, 2)
);

console.log(`\n${'═'.repeat(70)}`);
console.log(`📋 最终推荐`);
console.log(`${'═'.repeat(70)}\n`);

console.log('【综合最佳】');
const best = results[0];
console.log(`  金狗通过: ${best.goldPassRate.toFixed(1)}% | 噪音过滤: ${best.noiseFilterRate.toFixed(1)}% | 精确率: ${best.precision.toFixed(1)}%`);
console.log(`  参数: 七维分≥${best.params.sevenDimThreshold}, SM≥${best.params.smCountThreshold}, ` +
    `趋势↑=${best.params.requireTrendIncreasing}, 信号加速=${best.params.requireSignalAccelerating}, ` +
    `允许接盘=${best.params.allowLateFollower}, 基础分≥${best.params.baseScoreThreshold}, ` +
    `市值限制=${best.params.maxMcap ? best.params.maxMcap/1000 + 'K' : '无'}`);

console.log('\n【平衡推荐】(金狗≥70%时噪音过滤最高)');
if (balancedStrategies.length > 0) {
    const balanced = balancedStrategies[0];
    console.log(`  金狗通过: ${balanced.goldPassRate.toFixed(1)}% | 噪音过滤: ${balanced.noiseFilterRate.toFixed(1)}% | 精确率: ${balanced.precision.toFixed(1)}%`);
    console.log(`  参数: 七维分≥${balanced.params.sevenDimThreshold}, SM≥${balanced.params.smCountThreshold}, ` +
        `趋势↑=${balanced.params.requireTrendIncreasing}, 信号加速=${balanced.params.requireSignalAccelerating}, ` +
        `允许接盘=${balanced.params.allowLateFollower}, 基础分≥${balanced.params.baseScoreThreshold}, ` +
        `市值限制=${balanced.params.maxMcap ? balanced.params.maxMcap/1000 + 'K' : '无'}`);
}

console.log('\n【高精确率】(通过的token中金狗占比最高)');
if (precisionStrategies.length > 0) {
    const precise = precisionStrategies[0];
    console.log(`  金狗通过: ${precise.goldPassRate.toFixed(1)}% | 噪音过滤: ${precise.noiseFilterRate.toFixed(1)}% | 精确率: ${precise.precision.toFixed(1)}%`);
    console.log(`  参数: 七维分≥${precise.params.sevenDimThreshold}, SM≥${precise.params.smCountThreshold}, ` +
        `趋势↑=${precise.params.requireTrendIncreasing}, 信号加速=${precise.params.requireSignalAccelerating}, ` +
        `允许接盘=${precise.params.allowLateFollower}, 基础分≥${precise.params.baseScoreThreshold}, ` +
        `市值限制=${precise.params.maxMcap ? precise.params.maxMcap/1000 + 'K' : '无'}`);
}

console.log(`\n结果已保存到 data/parameter-grid-search-results.json`);
