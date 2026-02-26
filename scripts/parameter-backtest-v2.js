#!/usr/bin/env node
/**
 * 参数回测脚本 v2
 * 修复：正确提取七维分和过滤状态
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const logPath = path.join(__dirname, '..', 'logs', 'restart_v75_20260113_002033.log');

console.log('加载日志数据...');
const content = fs.readFileSync(logPath, 'utf-8');
const lines = content.split('\n');

// ═══════════════════════════════════════════════════════════════
// 1. 提取被过滤token的七维分
// ═══════════════════════════════════════════════════════════════
const filteredTokens = new Map(); // symbol -> 七维分

for (const line of lines) {
    const filterMatch = line.match(/\[Filter\]\s+([^\s]+)\s+七维分\s+(\d+)/);
    if (filterMatch) {
        const symbol = filterMatch[1];
        const score = parseInt(filterMatch[2]);
        if (!filteredTokens.has(symbol)) {
            filteredTokens.set(symbol, score);
        }
    }
}

console.log(`被过滤token: ${filteredTokens.size}个`);

// ═══════════════════════════════════════════════════════════════
// 2. 提取毕业token
// ═══════════════════════════════════════════════════════════════
const graduatedTokens = new Set();

for (const line of lines) {
    const gradMatch = line.match(/\[GRADUATE\]\s+([^\s]+)\s+毕业/);
    if (gradMatch) {
        graduatedTokens.add(gradMatch[1]);
    }
}

console.log(`毕业token: ${graduatedTokens.size}个`);

// ═══════════════════════════════════════════════════════════════
// 3. 提取金狗银狗 (使用DeBot的gold/silver标记和涨幅)
// ═══════════════════════════════════════════════════════════════
const goldDogSymbols = new Set();
const silverDogSymbols = new Set();
const allTokenGains = new Map(); // symbol -> gain

// 从 market/metrics 提取 symbol
const addrToSymbol = new Map();
const metricsRegex = /"address":"([A-Za-z0-9]+)".*?"symbol":"([^"]+)"/g;
let match;
while ((match = metricsRegex.exec(content)) !== null) {
    addrToSymbol.set(match[1], match[2]);
}

// 提取AI SIGNAL的涨幅和等级
let currentAddr = null;
let currentLevel = null;

for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    
    const signalMatch = line.match(/\[DeBot Scout\] (🥇|🥈) AI SIGNAL.*: ([A-Za-z0-9]+)/);
    if (signalMatch) {
        currentAddr = signalMatch[2];
        currentLevel = signalMatch[1] === '🥇' ? 'gold' : 'silver';
    }
    
    if (currentAddr && line.includes('等级:')) {
        const levelMatch = line.match(/等级:\s*(gold|silver)/i);
        if (levelMatch) {
            currentLevel = levelMatch[1].toLowerCase();
        }
    }
    
    if (currentAddr && line.includes('最大涨幅:')) {
        const gainMatch = line.match(/最大涨幅:\s*([0-9.]+)x/);
        if (gainMatch) {
            const gain = parseFloat(gainMatch[1]);
            const symbol = addrToSymbol.get(currentAddr) || currentAddr.substring(0, 8);
            
            if (!allTokenGains.has(symbol) || allTokenGains.get(symbol) < gain) {
                allTokenGains.set(symbol, gain);
            }
            
            if (currentLevel === 'gold' || gain >= 10) {
                goldDogSymbols.add(symbol);
            } else if (currentLevel === 'silver' || gain >= 2) {
                silverDogSymbols.add(symbol);
            }
            
            currentAddr = null;
        }
    }
}

console.log(`金狗symbol: ${goldDogSymbols.size}个`);
console.log(`银狗symbol: ${silverDogSymbols.size}个`);

// ═══════════════════════════════════════════════════════════════
// 4. 分析金狗在过滤系统中的状态
// ═══════════════════════════════════════════════════════════════
console.log(`\n${'═'.repeat(70)}`);
console.log(`📊 金狗处理状态分析`);
console.log(`${'═'.repeat(70)}\n`);

let goldFiltered = 0, goldGraduated = 0, goldUnknown = 0;
const goldScores = [];

for (const symbol of goldDogSymbols) {
    const gain = allTokenGains.get(symbol) || 0;
    
    if (filteredTokens.has(symbol)) {
        goldFiltered++;
        const score = filteredTokens.get(symbol);
        goldScores.push({ symbol, score, gain, status: '过滤' });
        console.log(`  ❌ ${symbol.padEnd(15)} | 涨幅:${gain.toFixed(1).padStart(5)}x | 七维分:${score} | 被过滤`);
    } else if (graduatedTokens.has(symbol)) {
        goldGraduated++;
        console.log(`  ✅ ${symbol.padEnd(15)} | 涨幅:${gain.toFixed(1).padStart(5)}x | 已买入`);
    } else {
        goldUnknown++;
        // console.log(`  ❓ ${symbol.padEnd(15)} | 涨幅:${gain.toFixed(1).padStart(5)}x | 未知`);
    }
}

console.log(`\n金狗统计:`);
console.log(`  总数: ${goldDogSymbols.size}`);
console.log(`  ✅ 买入: ${goldGraduated} (${(goldGraduated/goldDogSymbols.size*100).toFixed(1)}%)`);
console.log(`  ❌ 过滤: ${goldFiltered} (${(goldFiltered/goldDogSymbols.size*100).toFixed(1)}%)`);
console.log(`  ❓ 未知: ${goldUnknown} (${(goldUnknown/goldDogSymbols.size*100).toFixed(1)}%)`);

// ═══════════════════════════════════════════════════════════════
// 5. 分析噪音过滤效果
// ═══════════════════════════════════════════════════════════════
console.log(`\n${'═'.repeat(70)}`);
console.log(`📊 噪音过滤分析`);
console.log(`${'═'.repeat(70)}\n`);

let noiseFiltered = 0, goodFiltered = 0;
const noiseScores = [];

for (const [symbol, score] of filteredTokens) {
    if (goldDogSymbols.has(symbol) || silverDogSymbols.has(symbol)) {
        goodFiltered++;
    } else {
        noiseFiltered++;
        noiseScores.push(score);
    }
}

console.log(`总过滤: ${filteredTokens.size}`);
console.log(`✅ 正确过滤(噪音): ${noiseFiltered} (${(noiseFiltered/filteredTokens.size*100).toFixed(1)}%)`);
console.log(`❌ 错误过滤(金银狗): ${goodFiltered} (${(goodFiltered/filteredTokens.size*100).toFixed(1)}%)`);

// ═══════════════════════════════════════════════════════════════
// 6. 模拟不同七维分阈值的效果
// ═══════════════════════════════════════════════════════════════
console.log(`\n${'═'.repeat(70)}`);
console.log(`🧪 七维分阈值模拟`);
console.log(`${'═'.repeat(70)}\n`);

console.log('阈值 | 金狗通过率 | 噪音过滤率 | 综合得分');
console.log('─'.repeat(50));

const thresholdResults = [];

for (let threshold = 10; threshold <= 35; threshold += 2) {
    let goldWouldPass = 0;
    let noiseWouldFilter = 0;
    
    // 金狗：如果七维分 >= 阈值，就会通过
    for (const { symbol, score } of goldScores) {
        if (score >= threshold) {
            goldWouldPass++;
        }
    }
    
    // 噪音：如果七维分 < 阈值，就会被过滤
    for (const score of noiseScores) {
        if (score < threshold) {
            noiseWouldFilter++;
        }
    }
    
    // 但这里的逻辑有问题：被过滤的金狗，降低阈值后应该能通过
    // 重新计算
    const goldTotalInFilter = goldScores.length;
    const goldPassRate = goldTotalInFilter > 0 ? 
        (goldScores.filter(g => g.score >= threshold).length / goldTotalInFilter * 100) : 0;
    
    const noiseTotalInFilter = noiseScores.length;
    const noiseFilterRate = noiseTotalInFilter > 0 ?
        (noiseScores.filter(s => s < threshold).length / noiseTotalInFilter * 100) : 0;
    
    // 综合得分：我们要金狗通过（阈值低），同时噪音被过滤（阈值高）
    // 这是矛盾的！需要找到最佳平衡点
    // 实际上，当阈值降低时，金狗通过率提高，但噪音过滤率下降
    
    // 综合得分 = 金狗通过率 * 0.7 + (100 - 噪音通过率) * 0.3
    // 但这里噪音过滤率已经是"被正确过滤的比例"
    const compositeScore = goldPassRate * 0.6 + noiseFilterRate * 0.4;
    
    console.log(
        `${threshold.toString().padStart(2)} | ` +
        `${goldPassRate.toFixed(1).padStart(5)}% | ` +
        `${noiseFilterRate.toFixed(1).padStart(5)}% | ` +
        `${compositeScore.toFixed(1).padStart(5)}`
    );
    
    thresholdResults.push({ threshold, goldPassRate, noiseFilterRate, compositeScore });
}

// 找最优阈值
const bestThreshold = thresholdResults.sort((a, b) => b.compositeScore - a.compositeScore)[0];

console.log(`\n🏆 最优七维分阈值: ${bestThreshold.threshold}`);
console.log(`   金狗通过率: ${bestThreshold.goldPassRate.toFixed(1)}%`);
console.log(`   噪音过滤率: ${bestThreshold.noiseFilterRate.toFixed(1)}%`);

// ═══════════════════════════════════════════════════════════════
// 7. 分析金狗和噪音的分数分布
// ═══════════════════════════════════════════════════════════════
console.log(`\n${'═'.repeat(70)}`);
console.log(`📊 分数分布分析`);
console.log(`${'═'.repeat(70)}\n`);

const goldScoreValues = goldScores.map(g => g.score);
const avgGoldScore = goldScoreValues.reduce((a, b) => a + b, 0) / goldScoreValues.length;
const minGoldScore = Math.min(...goldScoreValues);
const maxGoldScore = Math.max(...goldScoreValues);

const avgNoiseScore = noiseScores.reduce((a, b) => a + b, 0) / noiseScores.length;
const minNoiseScore = Math.min(...noiseScores);
const maxNoiseScore = Math.max(...noiseScores);

console.log(`金狗七维分分布:`);
console.log(`  最低: ${minGoldScore} | 平均: ${avgGoldScore.toFixed(1)} | 最高: ${maxGoldScore}`);
console.log(`\n噪音七维分分布:`);
console.log(`  最低: ${minNoiseScore} | 平均: ${avgNoiseScore.toFixed(1)} | 最高: ${maxNoiseScore}`);

// 分数重叠区域分析
console.log(`\n分数重叠分析:`);
console.log(`  金狗分数范围: ${minGoldScore} - ${maxGoldScore}`);
console.log(`  噪音分数范围: ${minNoiseScore} - ${maxNoiseScore}`);

if (maxNoiseScore >= minGoldScore) {
    console.log(`  ⚠️ 存在重叠区域: ${minGoldScore} - ${maxNoiseScore}`);
    console.log(`  💡 在重叠区域内无法完美区分金狗和噪音`);
} else {
    console.log(`  ✅ 无重叠，可完美区分`);
}

// 保存结果
const results = {
    goldDogSymbols: Array.from(goldDogSymbols),
    goldFiltered,
    goldGraduated,
    noiseFiltered,
    goodFiltered,
    goldScores,
    thresholdResults,
    bestThreshold
};

fs.writeFileSync(
    path.join(__dirname, '..', 'data', 'parameter-backtest-v2-results.json'),
    JSON.stringify(results, null, 2)
);

console.log(`\n结果已保存到 data/parameter-backtest-v2-results.json`);
