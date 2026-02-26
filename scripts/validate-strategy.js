#!/usr/bin/env node
/**
 * 策略验证脚本 - 使用交叉验证测试CONSERVATIVE策略
 *
 * 方法: 时间分割验证
 * - 训练集: 较早的日志文件
 * - 测试集: 较晚的日志文件
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const dataPath = path.join(__dirname, '..', 'data', 'extended-backtest-results.json');

// 加载数据
const data = JSON.parse(fs.readFileSync(dataPath, 'utf-8'));
const allGold = data.goldFeatures;
const allNoise = data.noiseFeatures;

console.log(`\n${'═'.repeat(70)}`);
console.log(`🔬 CONSERVATIVE策略验证测试`);
console.log(`${'═'.repeat(70)}`);
console.log(`\n策略参数:`);
console.log(`  - requireSignalAccelerating: true (只选ACCELERATING信号)`);
console.log(`  - smCountThreshold: ≥1`);
console.log(`  - baseScoreThreshold: ≥45`);

// CONSERVATIVE策略过滤函数
function conservativeFilter(f) {
    return f.signalTrendType === 'ACCELERATING' &&
           f.smCount >= 1 &&
           f.baseScore >= 45;
}

// 按日志来源分组
const logSources = new Set([...allGold.map(g => g.source), ...allNoise.map(g => g.source)]);
const sortedLogs = [...logSources].sort();

console.log(`\n数据来源: ${sortedLogs.length} 个日志文件`);

// 时间分割: 前70%训练，后30%测试
const splitIndex = Math.floor(sortedLogs.length * 0.7);
const trainLogs = new Set(sortedLogs.slice(0, splitIndex));
const testLogs = new Set(sortedLogs.slice(splitIndex));

console.log(`\n【数据分割】`);
console.log(`  训练集: ${trainLogs.size} 个日志`);
console.log(`  测试集: ${testLogs.size} 个日志`);

// 分割数据
const trainGold = allGold.filter(g => trainLogs.has(g.source));
const trainNoise = allNoise.filter(g => trainLogs.has(g.source));
const testGold = allGold.filter(g => testLogs.has(g.source));
const testNoise = allNoise.filter(g => testLogs.has(g.source));

console.log(`\n【样本分布】`);
console.log(`  训练集: ${trainGold.length}金狗 + ${trainNoise.length}噪音`);
console.log(`  测试集: ${testGold.length}金狗 + ${testNoise.length}噪音`);

// 在测试集上评估
console.log(`\n${'═'.repeat(70)}`);
console.log(`📊 测试集验证结果`);
console.log(`${'═'.repeat(70)}`);

const testGoldPass = testGold.filter(conservativeFilter);
const testNoisePass = testNoise.filter(conservativeFilter);

const goldPassRate = testGold.length > 0 ? testGoldPass.length / testGold.length * 100 : 0;
const noiseFilterRate = testNoise.length > 0 ? (1 - testNoisePass.length / testNoise.length) * 100 : 0;
const precision = testGoldPass.length + testNoisePass.length > 0
    ? testGoldPass.length / (testGoldPass.length + testNoisePass.length) * 100 : 0;

console.log(`\n【CONSERVATIVE策略在测试集上的表现】`);
console.log(`  金狗通过: ${testGoldPass.length}/${testGold.length} = ${goldPassRate.toFixed(1)}%`);
console.log(`  噪音过滤: ${testNoise.length - testNoisePass.length}/${testNoise.length} = ${noiseFilterRate.toFixed(1)}%`);
console.log(`  精确率: ${testGoldPass.length}/${testGoldPass.length + testNoisePass.length} = ${precision.toFixed(1)}%`);

// 与训练集对比
const trainGoldPass = trainGold.filter(conservativeFilter);
const trainNoisePass = trainNoise.filter(conservativeFilter);

const trainGoldPassRate = trainGold.length > 0 ? trainGoldPass.length / trainGold.length * 100 : 0;
const trainNoiseFilterRate = trainNoise.length > 0 ? (1 - trainNoisePass.length / trainNoise.length) * 100 : 0;
const trainPrecision = trainGoldPass.length + trainNoisePass.length > 0
    ? trainGoldPass.length / (trainGoldPass.length + trainNoisePass.length) * 100 : 0;

console.log(`\n【训练集表现 (对比)】`);
console.log(`  金狗通过: ${trainGoldPass.length}/${trainGold.length} = ${trainGoldPassRate.toFixed(1)}%`);
console.log(`  噪音过滤: ${trainNoise.length - trainNoisePass.length}/${trainNoise.length} = ${trainNoiseFilterRate.toFixed(1)}%`);
console.log(`  精确率: ${trainGoldPass.length}/${trainGoldPass.length + trainNoisePass.length} = ${trainPrecision.toFixed(1)}%`);

// 泛化能力评估
console.log(`\n${'═'.repeat(70)}`);
console.log(`📈 泛化能力评估`);
console.log(`${'═'.repeat(70)}`);

const goldDiff = goldPassRate - trainGoldPassRate;
const noiseDiff = noiseFilterRate - trainNoiseFilterRate;
const precisionDiff = precision - trainPrecision;

console.log(`\n| 指标       | 训练集 | 测试集 | 差异 |`);
console.log(`|------------|--------|--------|------|`);
console.log(`| 金狗通过   | ${trainGoldPassRate.toFixed(1).padStart(5)}% | ${goldPassRate.toFixed(1).padStart(5)}% | ${(goldDiff >= 0 ? '+' : '') + goldDiff.toFixed(1)}% |`);
console.log(`| 噪音过滤   | ${trainNoiseFilterRate.toFixed(1).padStart(5)}% | ${noiseFilterRate.toFixed(1).padStart(5)}% | ${(noiseDiff >= 0 ? '+' : '') + noiseDiff.toFixed(1)}% |`);
console.log(`| 精确率     | ${trainPrecision.toFixed(1).padStart(5)}% | ${precision.toFixed(1).padStart(5)}% | ${(precisionDiff >= 0 ? '+' : '') + precisionDiff.toFixed(1)}% |`);

// 判断泛化能力
const isGeneralizing = Math.abs(precisionDiff) < 5; // 精确率差异小于5%认为泛化良好
console.log(`\n结论: ${isGeneralizing ? '✅ 策略泛化能力良好' : '⚠️ 可能存在过拟合'}`);

// 列出测试集中通过的金狗
console.log(`\n${'═'.repeat(70)}`);
console.log(`🏆 测试集中通过CONSERVATIVE过滤的金狗`);
console.log(`${'═'.repeat(70)}`);

if (testGoldPass.length > 0) {
    testGoldPass.forEach(g => {
        const goldDogInfo = data.goldDogList.find(d => d.symbol === g.symbol);
        const gain = goldDogInfo ? goldDogInfo.gain : '?';
        console.log(`  ${g.symbol.padEnd(20)} | 涨幅: ${gain}x | 评分: ${g.baseScore} | SM: ${g.smCount} | 信号: ${g.signalTrendType}`);
    });
} else {
    console.log(`  (无)`)
}

// 列出被错误过滤的金狗
console.log(`\n${'═'.repeat(70)}`);
console.log(`⚠️ 测试集中被CONSERVATIVE过滤掉的金狗 (漏掉的机会)`);
console.log(`${'═'.repeat(70)}`);

const missedGold = testGold.filter(g => !conservativeFilter(g));
if (missedGold.length > 0) {
    missedGold.forEach(g => {
        const goldDogInfo = data.goldDogList.find(d => d.symbol === g.symbol);
        const gain = goldDogInfo ? goldDogInfo.gain : '?';
        const reason = g.signalTrendType !== 'ACCELERATING' ? `信号=${g.signalTrendType}` :
                      g.smCount < 1 ? `SM=${g.smCount}` :
                      g.baseScore < 45 ? `Score=${g.baseScore}` : '未知';
        console.log(`  ${g.symbol.padEnd(20)} | 涨幅: ${gain}x | 过滤原因: ${reason}`);
    });
} else {
    console.log(`  (无)`)
}

// 模拟交易结果
console.log(`\n${'═'.repeat(70)}`);
console.log(`💰 模拟交易结果 (假设每笔投入相同资金)`);
console.log(`${'═'.repeat(70)}`);

// 假设: 金狗平均赚取其涨幅的一部分(比如50%)，噪音平均亏损30%
const avgGoldGain = 0.5; // 金狗平均能吃到50%涨幅
const avgNoiseLoss = -0.3; // 噪音平均亏30%

const goldProfit = testGoldPass.length * avgGoldGain;
const noiseLoss = testNoisePass.length * avgNoiseLoss;
const netResult = goldProfit + noiseLoss;

console.log(`\n假设条件:`);
console.log(`  - 每笔投入: 1单位资金`);
console.log(`  - 金狗平均收益: +${avgGoldGain * 100}%`);
console.log(`  - 噪音平均亏损: ${avgNoiseLoss * 100}%`);

console.log(`\n模拟结果:`);
console.log(`  通过信号数: ${testGoldPass.length + testNoisePass.length}`);
console.log(`  其中金狗: ${testGoldPass.length} (贡献 +${(goldProfit * 100).toFixed(0)}%)`);
console.log(`  其中噪音: ${testNoisePass.length} (贡献 ${(noiseLoss * 100).toFixed(0)}%)`);
console.log(`  净收益: ${netResult >= 0 ? '+' : ''}${(netResult * 100).toFixed(0)}% (${testGoldPass.length + testNoisePass.length}单位投入)`);
console.log(`  平均每笔: ${netResult / (testGoldPass.length + testNoisePass.length) >= 0 ? '+' : ''}${(netResult / (testGoldPass.length + testNoisePass.length) * 100).toFixed(1)}%`);

// 与不过滤对比
const noFilterGold = testGold.length;
const noFilterNoise = testNoise.length;
const noFilterGoldProfit = noFilterGold * avgGoldGain;
const noFilterNoiseLoss = noFilterNoise * avgNoiseLoss;
const noFilterNet = noFilterGoldProfit + noFilterNoiseLoss;

console.log(`\n与不过滤对比:`);
console.log(`  不过滤时通过: ${noFilterGold + noFilterNoise} 个信号`);
console.log(`  不过滤时净收益: ${noFilterNet >= 0 ? '+' : ''}${(noFilterNet * 100).toFixed(0)}%`);
console.log(`  过滤后净收益: ${netResult >= 0 ? '+' : ''}${(netResult * 100).toFixed(0)}%`);
console.log(`  过滤效果: ${netResult > noFilterNet ? '✅ 过滤有效提升收益' : '⚠️ 过滤可能过于激进'}`);
