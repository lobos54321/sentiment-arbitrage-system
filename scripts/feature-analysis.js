#!/usr/bin/env node
/**
 * 特征分析脚本
 * 分析金狗和噪音在各个维度上的差异，找出可区分的特征
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const logPath = path.join(__dirname, '..', 'logs', 'restart_v75_20260113_002033.log');

console.log('加载日志数据...');
const content = fs.readFileSync(logPath, 'utf-8');
const lines = content.split('\n');

// 提取金狗和噪音的详细特征
const goldFeatures = [];
const noiseFeatures = [];

// 从 market/metrics 提取 symbol -> address 映射
const addrToSymbol = new Map();
const metricsRegex = /"address":"([A-Za-z0-9]+)".*?"symbol":"([^"]+)"/g;
let match;
while ((match = metricsRegex.exec(content)) !== null) {
    addrToSymbol.set(match[1], match[2]);
}

// 提取金狗symbol
const goldDogSymbols = new Set();
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
            
            if (currentLevel === 'gold' || gain >= 10) {
                goldDogSymbols.add(symbol);
            }
            currentAddr = null;
        }
    }
}

// 提取评分明细
// 格式: 📊 评分明细 [71分]: 聪明钱: 5/25 (10个, ...) | 趋势: 20/20 (INCREASING) | 密度: 5/15 | 信号趋势: 20/15 (ACCELERATING) | 安全: 15/15 | AI: 6/10

for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    
    if (!line.includes('评分明细')) continue;
    
    // 提取各项分数
    const scoreMatch = line.match(/\[(\d+)分\]/);
    const smMatch = line.match(/聪明钱:\s*(\d+)\/25\s*\((\d+)个/);
    const trendMatch = line.match(/趋势:\s*(\d+)\/20\s*\((\w+)\)/);
    const densityMatch = line.match(/密度:\s*(\d+)\/15/);
    const signalTrendMatch = line.match(/信号趋势:\s*(\d+)\/15\s*\((\w+)\)/);
    const safetyMatch = line.match(/安全:\s*(\d+)\/15/);
    const aiMatch = line.match(/AI:\s*(\d+)\/10/);
    
    if (!scoreMatch || !smMatch) continue;
    
    // 往后找symbol (在验证完成行)
    let symbol = null;
    for (let j = i; j < Math.min(i + 30, lines.length); j++) {
        const verifyMatch = lines[j].match(/验证完成:\s*([^\n\s]+)/);
        if (verifyMatch) {
            symbol = verifyMatch[1];
            break;
        }
    }
    
    if (!symbol) continue;
    
    // 查找七维分和过滤状态
    let sevenDimScore = null;
    let isFiltered = false;
    for (let j = i; j < Math.min(i + 30, lines.length); j++) {
        const filterMatch = lines[j].match(/\[Filter\].*七维分\s+(\d+)/);
        if (filterMatch) {
            sevenDimScore = parseInt(filterMatch[1]);
            isFiltered = true;
            break;
        }
    }
    
    if (!isFiltered) continue; // 只分析被过滤的
    
    // 查找市值和时机
    let mcap = null;
    let timing = null;
    for (let j = i; j < Math.min(i + 15, lines.length); j++) {
        const mcapMatch = lines[j].match(/市值\$([0-9.]+)K/);
        if (mcapMatch) mcap = parseFloat(mcapMatch[1]) * 1000;
        
        const timingMatch = lines[j].match(/时机.*?(EARLY|NORMAL|LATE|RISKY)/);
        if (timingMatch) timing = timingMatch[1];
    }
    
    // 查找接盘警告
    let lateFollower = false;
    for (let j = i; j < Math.min(i + 15, lines.length); j++) {
        if (lines[j].includes('接盘警告')) {
            lateFollower = true;
            break;
        }
    }
    
    const feature = {
        symbol,
        baseScore: parseInt(scoreMatch[1]),
        sevenDimScore,
        smScore: smMatch ? parseInt(smMatch[1]) : null,
        smCount: smMatch ? parseInt(smMatch[2]) : null,
        trendScore: trendMatch ? parseInt(trendMatch[1]) : null,
        trendType: trendMatch ? trendMatch[2] : null,
        densityScore: densityMatch ? parseInt(densityMatch[1]) : null,
        signalTrendScore: signalTrendMatch ? parseInt(signalTrendMatch[1]) : null,
        signalTrendType: signalTrendMatch ? signalTrendMatch[2] : null,
        safetyScore: safetyMatch ? parseInt(safetyMatch[1]) : null,
        aiScore: aiMatch ? parseInt(aiMatch[1]) : null,
        mcap,
        timing,
        lateFollower,
        isGold: goldDogSymbols.has(symbol)
    };
    
    if (goldDogSymbols.has(symbol)) {
        goldFeatures.push(feature);
    } else {
        noiseFeatures.push(feature);
    }
}

console.log(`金狗特征样本: ${goldFeatures.length}个`);
console.log(`噪音特征样本: ${noiseFeatures.length}个`);

// ═══════════════════════════════════════════════════════════════
// 分析各维度差异
// ═══════════════════════════════════════════════════════════════
console.log(`\n${'═'.repeat(70)}`);
console.log(`📊 金狗 vs 噪音 特征对比分析`);
console.log(`${'═'.repeat(70)}\n`);

function analyzeFeature(name, goldValues, noiseValues) {
    const validGold = goldValues.filter(v => v !== null && v !== undefined);
    const validNoise = noiseValues.filter(v => v !== null && v !== undefined);
    
    if (validGold.length === 0 || validNoise.length === 0) return;
    
    const avgGold = validGold.reduce((a, b) => a + b, 0) / validGold.length;
    const avgNoise = validNoise.reduce((a, b) => a + b, 0) / validNoise.length;
    const minGold = Math.min(...validGold);
    const maxGold = Math.max(...validGold);
    const minNoise = Math.min(...validNoise);
    const maxNoise = Math.max(...validNoise);
    
    const diff = avgGold - avgNoise;
    const diffPercent = ((avgGold - avgNoise) / avgNoise * 100).toFixed(1);
    const separable = maxNoise < minGold || maxGold < minNoise;
    
    console.log(`${name}:`);
    console.log(`  金狗: 均值=${avgGold.toFixed(1)} 范围=[${minGold}, ${maxGold}]`);
    console.log(`  噪音: 均值=${avgNoise.toFixed(1)} 范围=[${minNoise}, ${maxNoise}]`);
    console.log(`  差异: ${diff > 0 ? '+' : ''}${diff.toFixed(1)} (${diffPercent}%)`);
    console.log(`  可分离: ${separable ? '✅ 是' : '❌ 否 (存在重叠)'}`);
    console.log('');
}

analyzeFeature('聪明钱数量', 
    goldFeatures.map(f => f.smCount),
    noiseFeatures.map(f => f.smCount)
);

analyzeFeature('聪明钱评分', 
    goldFeatures.map(f => f.smScore),
    noiseFeatures.map(f => f.smScore)
);

analyzeFeature('趋势评分', 
    goldFeatures.map(f => f.trendScore),
    noiseFeatures.map(f => f.trendScore)
);

analyzeFeature('密度评分', 
    goldFeatures.map(f => f.densityScore),
    noiseFeatures.map(f => f.densityScore)
);

analyzeFeature('信号趋势评分', 
    goldFeatures.map(f => f.signalTrendScore),
    noiseFeatures.map(f => f.signalTrendScore)
);

analyzeFeature('AI评分', 
    goldFeatures.map(f => f.aiScore),
    noiseFeatures.map(f => f.aiScore)
);

analyzeFeature('市值($)', 
    goldFeatures.map(f => f.mcap),
    noiseFeatures.map(f => f.mcap)
);

// 分析趋势类型分布
console.log('趋势类型分布:');
const goldTrends = {};
const noiseTrends = {};
goldFeatures.forEach(f => { if (f.trendType) goldTrends[f.trendType] = (goldTrends[f.trendType] || 0) + 1; });
noiseFeatures.forEach(f => { if (f.trendType) noiseTrends[f.trendType] = (noiseTrends[f.trendType] || 0) + 1; });
console.log(`  金狗: ${JSON.stringify(goldTrends)}`);
console.log(`  噪音: ${JSON.stringify(noiseTrends)}`);
console.log('');

// 分析信号趋势类型分布
console.log('信号趋势类型分布:');
const goldSignalTrends = {};
const noiseSignalTrends = {};
goldFeatures.forEach(f => { if (f.signalTrendType) goldSignalTrends[f.signalTrendType] = (goldSignalTrends[f.signalTrendType] || 0) + 1; });
noiseFeatures.forEach(f => { if (f.signalTrendType) noiseSignalTrends[f.signalTrendType] = (noiseSignalTrends[f.signalTrendType] || 0) + 1; });
console.log(`  金狗: ${JSON.stringify(goldSignalTrends)}`);
console.log(`  噪音: ${JSON.stringify(noiseSignalTrends)}`);
console.log('');

// 分析接盘警告
console.log('接盘警告分布:');
const goldLateFollower = goldFeatures.filter(f => f.lateFollower).length;
const noiseLateFollower = noiseFeatures.filter(f => f.lateFollower).length;
console.log(`  金狗有接盘警告: ${goldLateFollower}/${goldFeatures.length} (${(goldLateFollower/goldFeatures.length*100).toFixed(1)}%)`);
console.log(`  噪音有接盘警告: ${noiseLateFollower}/${noiseFeatures.length} (${(noiseLateFollower/noiseFeatures.length*100).toFixed(1)}%)`);
console.log('');

// ═══════════════════════════════════════════════════════════════
// 寻找最佳区分特征
// ═══════════════════════════════════════════════════════════════
console.log(`${'═'.repeat(70)}`);
console.log(`🎯 寻找最佳区分策略`);
console.log(`${'═'.repeat(70)}\n`);

// 测试组合条件
function testCondition(name, goldPredicate, noisePredicate) {
    const goldPass = goldFeatures.filter(goldPredicate).length;
    const noiseFilter = noiseFeatures.filter(f => !noisePredicate(f)).length;
    
    const goldPassRate = (goldPass / goldFeatures.length * 100);
    const noiseFilterRate = (noiseFilter / noiseFeatures.length * 100);
    const score = goldPassRate * 0.6 + noiseFilterRate * 0.4;
    
    console.log(`${name}:`);
    console.log(`  金狗通过: ${goldPass}/${goldFeatures.length} (${goldPassRate.toFixed(1)}%)`);
    console.log(`  噪音过滤: ${noiseFilter}/${noiseFeatures.length} (${noiseFilterRate.toFixed(1)}%)`);
    console.log(`  综合得分: ${score.toFixed(1)}`);
    console.log('');
    
    return { name, goldPassRate, noiseFilterRate, score };
}

const strategies = [];

// 策略1: 趋势类型 = INCREASING 或 ACCELERATING
strategies.push(testCondition(
    '策略1: 趋势上升/加速',
    f => f.trendType === 'INCREASING' || f.signalTrendType === 'ACCELERATING',
    f => f.trendType === 'INCREASING' || f.signalTrendType === 'ACCELERATING'
));

// 策略2: 聪明钱 >= 5
strategies.push(testCondition(
    '策略2: 聪明钱 >= 5',
    f => f.smCount >= 5,
    f => f.smCount >= 5
));

// 策略3: 聪明钱 >= 3 且 趋势上升
strategies.push(testCondition(
    '策略3: SM>=3 且 趋势上升',
    f => f.smCount >= 3 && (f.trendType === 'INCREASING' || f.signalTrendType === 'ACCELERATING'),
    f => f.smCount >= 3 && (f.trendType === 'INCREASING' || f.signalTrendType === 'ACCELERATING')
));

// 策略4: 无接盘警告
strategies.push(testCondition(
    '策略4: 无接盘警告',
    f => !f.lateFollower,
    f => !f.lateFollower
));

// 策略5: 聪明钱 >= 5 且 无接盘警告
strategies.push(testCondition(
    '策略5: SM>=5 且 无接盘警告',
    f => f.smCount >= 5 && !f.lateFollower,
    f => f.smCount >= 5 && !f.lateFollower
));

// 策略6: 基础分 >= 60
strategies.push(testCondition(
    '策略6: 基础分 >= 60',
    f => f.baseScore >= 60,
    f => f.baseScore >= 60
));

// 策略7: 基础分 >= 50 且 SM >= 3
strategies.push(testCondition(
    '策略7: 基础分>=50 且 SM>=3',
    f => f.baseScore >= 50 && f.smCount >= 3,
    f => f.baseScore >= 50 && f.smCount >= 3
));

// 策略8: 密度 >= 10 或 趋势上升
strategies.push(testCondition(
    '策略8: 密度>=10 或 趋势上升',
    f => f.densityScore >= 10 || f.trendType === 'INCREASING',
    f => f.densityScore >= 10 || f.trendType === 'INCREASING'
));

// 找最佳策略
strategies.sort((a, b) => b.score - a.score);
console.log(`${'═'.repeat(70)}`);
console.log(`🏆 最佳策略排名`);
console.log(`${'═'.repeat(70)}\n`);

strategies.slice(0, 5).forEach((s, i) => {
    console.log(`${i+1}. ${s.name}`);
    console.log(`   金狗: ${s.goldPassRate.toFixed(1)}% | 噪音过滤: ${s.noiseFilterRate.toFixed(1)}% | 综合: ${s.score.toFixed(1)}`);
});

// 保存详细数据
fs.writeFileSync(
    path.join(__dirname, '..', 'data', 'feature-analysis-results.json'),
    JSON.stringify({ goldFeatures, noiseFeatures, strategies }, null, 2)
);

console.log(`\n结果已保存到 data/feature-analysis-results.json`);
