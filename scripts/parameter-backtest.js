#!/usr/bin/env node
/**
 * 参数回测脚本
 * 
 * 目标：找到最优参数组合，使得：
 * 1. 金狗命中率最高
 * 2. 噪音过滤率最高
 * 
 * 可调参数：
 * - 七维分阈值
 * - 接盘警告条件 (MCAP阈值, SM阈值, 惩罚分数)
 * - 时机惩罚 (MCAP阈值, 惩罚分数)
 * - 聪明钱权重
 * - 信号趋势权重
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const logPath = path.join(__dirname, '..', 'logs', 'restart_v75_20260113_002033.log');

console.log('加载日志数据...');
const content = fs.readFileSync(logPath, 'utf-8');

// ═══════════════════════════════════════════════════════════════
// 1. 提取DeBot官方金狗银狗榜单
// ═══════════════════════════════════════════════════════════════
const goldDogs = new Set();
const silverDogs = new Set();
const tokenData = new Map(); // address -> { symbol, level, gain, ... }

// 提取金狗银狗
const lines = content.split('\n');
let currentAddr = null;
let currentLevel = null;

for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    
    // AI SIGNAL 行
    const signalMatch = line.match(/\[DeBot Scout\] (🥇|🥈) AI SIGNAL.*: ([A-Za-z0-9]+)/);
    if (signalMatch) {
        currentAddr = signalMatch[2];
        currentLevel = signalMatch[1] === '🥇' ? 'gold' : 'silver';
    }
    
    // 等级行
    if (currentAddr && line.includes('等级:')) {
        const levelMatch = line.match(/等级:\s*(gold|silver)/i);
        if (levelMatch) {
            currentLevel = levelMatch[1].toLowerCase();
        }
    }
    
    // 涨幅行
    if (currentAddr && line.includes('最大涨幅:')) {
        const gainMatch = line.match(/最大涨幅:\s*([0-9.]+)x/);
        if (gainMatch) {
            const gain = parseFloat(gainMatch[1]);
            
            if (!tokenData.has(currentAddr)) {
                tokenData.set(currentAddr, { level: currentLevel, gain });
            }
            
            if (currentLevel === 'gold' || gain >= 10) {
                goldDogs.add(currentAddr);
            } else if (currentLevel === 'silver' || gain >= 2) {
                silverDogs.add(currentAddr);
            }
            
            currentAddr = null;
        }
    }
}

console.log(`DeBot官方金狗: ${goldDogs.size}个`);
console.log(`DeBot官方银狗: ${silverDogs.size}个`);

// ═══════════════════════════════════════════════════════════════
// 2. 提取所有token的评分数据（用于模拟不同参数）
// ═══════════════════════════════════════════════════════════════
const allTokenScores = new Map(); // symbol -> { baseScore, smCount, mcap, timing, ... }

// 提取评分明细
const scoreRegex = /📊 评分明细.*\[(\d+)分\].*聪明钱:\s*(\d+)\/25\s*\((\d+)个.*趋势:\s*(\d+)\/20.*密度:\s*(\d+)\/15.*信号趋势:\s*(\d+)\/15.*安全:\s*(\d+)\/15.*AI:\s*(\d+)\/10/g;

let scoreMatch;
while ((scoreMatch = scoreRegex.exec(content)) !== null) {
    // 需要往后找symbol
    const pos = scoreMatch.index;
    const nextLines = content.slice(pos, pos + 2000);
    
    const symbolMatch = nextLines.match(/验证完成:\s*([^\n\s]+)/);
    if (symbolMatch) {
        const symbol = symbolMatch[1];
        const baseScore = parseInt(scoreMatch[1]);
        const smScore = parseInt(scoreMatch[2]);
        const smCount = parseInt(scoreMatch[3]);
        const trendScore = parseInt(scoreMatch[4]);
        const densityScore = parseInt(scoreMatch[5]);
        const signalTrendScore = parseInt(scoreMatch[6]);
        const safetyScore = parseInt(scoreMatch[7]);
        const aiScore = parseInt(scoreMatch[8]);
        
        // 提取市值
        const mcapMatch = nextLines.match(/市值\$([0-9.]+)K/);
        const mcap = mcapMatch ? parseFloat(mcapMatch[1]) * 1000 : 0;
        
        // 提取时机
        const timingMatch = nextLines.match(/时机.*?(EARLY|NORMAL|LATE|RISKY)/);
        const timing = timingMatch ? timingMatch[1] : 'UNKNOWN';
        
        // 提取接盘警告
        const lateFollowerMatch = nextLines.match(/接盘警告.*?扣(\d+)分/);
        const lateFollowerPenalty = lateFollowerMatch ? parseInt(lateFollowerMatch[1]) : 0;
        
        if (!allTokenScores.has(symbol)) {
            allTokenScores.set(symbol, {
                symbol,
                baseScore,
                smScore, smCount,
                trendScore, densityScore, signalTrendScore,
                safetyScore, aiScore,
                mcap, timing, lateFollowerPenalty,
                isGold: false,
                isSilver: false
            });
        }
    }
}

// 标记金狗银狗
// 需要从地址找到symbol的映射
const addrToSymbol = new Map();
const symbolRegex = /"symbol":"([^"]+)"[\s\S]*?"address":"([A-Za-z0-9]+)"/g;
let symMatch;
while ((symMatch = symbolRegex.exec(content)) !== null) {
    addrToSymbol.set(symMatch[2], symMatch[1]);
}

for (const addr of goldDogs) {
    const symbol = addrToSymbol.get(addr);
    if (symbol && allTokenScores.has(symbol)) {
        allTokenScores.get(symbol).isGold = true;
    }
}

for (const addr of silverDogs) {
    const symbol = addrToSymbol.get(addr);
    if (symbol && allTokenScores.has(symbol)) {
        allTokenScores.get(symbol).isSilver = true;
    }
}

console.log(`提取到 ${allTokenScores.size} 个token的评分数据`);

// ═══════════════════════════════════════════════════════════════
// 3. 模拟评分函数（可调参数）
// ═══════════════════════════════════════════════════════════════
function simulateScore(token, params) {
    // 基础分数 = 原始各维度分数
    let score = token.smScore + token.trendScore + token.densityScore + 
                token.signalTrendScore + token.safetyScore + token.aiScore;
    
    // 应用接盘警告惩罚
    if (token.mcap > params.lateFollowerMcapThreshold && token.smCount < params.lateFollowerSmThreshold) {
        score += params.lateFollowerPenalty; // 负数
    }
    
    // 应用时机惩罚
    if (token.timing === 'RISKY' && token.mcap > params.riskyMcapThreshold) {
        score += params.riskyPenalty; // 负数
    }
    
    // 聪明钱权重调整
    const smBonus = token.smCount >= params.smBonusThreshold ? params.smBonus : 0;
    score += smBonus;
    
    return score;
}

// ═══════════════════════════════════════════════════════════════
// 4. 回测函数
// ═══════════════════════════════════════════════════════════════
function runBacktest(params) {
    let goldHit = 0, goldMiss = 0, goldTotal = 0;
    let noisePass = 0, noiseFilter = 0, noiseTotal = 0;
    
    for (const [symbol, token] of allTokenScores) {
        const adjustedScore = simulateScore(token, params);
        const wouldPass = adjustedScore >= params.entryThreshold;
        
        if (token.isGold) {
            goldTotal++;
            if (wouldPass) goldHit++;
            else goldMiss++;
        } else if (!token.isSilver) {
            // 噪音（非金非银）
            noiseTotal++;
            if (wouldPass) noisePass++;
            else noiseFilter++;
        }
    }
    
    const goldHitRate = goldTotal > 0 ? (goldHit / goldTotal * 100) : 0;
    const noiseFilterRate = noiseTotal > 0 ? (noiseFilter / noiseTotal * 100) : 0;
    
    // 综合得分：金狗命中率 * 0.6 + 噪音过滤率 * 0.4
    const compositeScore = goldHitRate * 0.6 + noiseFilterRate * 0.4;
    
    return {
        goldHit, goldMiss, goldTotal, goldHitRate,
        noisePass, noiseFilter, noiseTotal, noiseFilterRate,
        compositeScore
    };
}

// ═══════════════════════════════════════════════════════════════
// 5. 参数搜索
// ═══════════════════════════════════════════════════════════════
console.log(`\n${'═'.repeat(70)}`);
console.log(`🔍 参数搜索开始`);
console.log(`${'═'.repeat(70)}\n`);

const results = [];

// 参数范围
const entryThresholds = [15, 18, 20, 22, 25, 28, 30];
const lateFollowerMcapThresholds = [300000, 500000, 800000, 1000000];
const lateFollowerSmThresholds = [3, 5, 7, 10];
const lateFollowerPenalties = [0, -10, -20, -30];
const riskyMcapThresholds = [500000, 800000, 1000000];
const riskyPenalties = [0, -5, -10, -15];
const smBonusThresholds = [5, 8, 10];
const smBonuses = [0, 5, 10, 15];

let totalCombinations = entryThresholds.length * lateFollowerMcapThresholds.length * 
    lateFollowerSmThresholds.length * lateFollowerPenalties.length *
    riskyMcapThresholds.length * riskyPenalties.length *
    smBonusThresholds.length * smBonuses.length;

console.log(`总参数组合: ${totalCombinations}`);
console.log(`搜索中...\n`);

let count = 0;
for (const entryThreshold of entryThresholds) {
    for (const lateFollowerMcapThreshold of lateFollowerMcapThresholds) {
        for (const lateFollowerSmThreshold of lateFollowerSmThresholds) {
            for (const lateFollowerPenalty of lateFollowerPenalties) {
                for (const riskyMcapThreshold of riskyMcapThresholds) {
                    for (const riskyPenalty of riskyPenalties) {
                        for (const smBonusThreshold of smBonusThresholds) {
                            for (const smBonus of smBonuses) {
                                const params = {
                                    entryThreshold,
                                    lateFollowerMcapThreshold,
                                    lateFollowerSmThreshold,
                                    lateFollowerPenalty,
                                    riskyMcapThreshold,
                                    riskyPenalty,
                                    smBonusThreshold,
                                    smBonus
                                };
                                
                                const result = runBacktest(params);
                                
                                // 只保存有意义的结果（金狗命中率>50% 且 噪音过滤率>50%）
                                if (result.goldHitRate >= 50 && result.noiseFilterRate >= 50) {
                                    results.push({ params, ...result });
                                }
                                
                                count++;
                            }
                        }
                    }
                }
            }
        }
    }
}

// 按综合得分排序
results.sort((a, b) => b.compositeScore - a.compositeScore);

console.log(`${'═'.repeat(70)}`);
console.log(`📊 回测结果 (Top 20)`);
console.log(`${'═'.repeat(70)}\n`);

console.log(`找到 ${results.length} 个有效参数组合\n`);

console.log('排名 | 金狗命中 | 噪音过滤 | 综合分 | 关键参数');
console.log('─'.repeat(70));

results.slice(0, 20).forEach((r, i) => {
    const p = r.params;
    console.log(
        `${(i+1).toString().padStart(2)} | ` +
        `${r.goldHitRate.toFixed(1).padStart(5)}% (${r.goldHit}/${r.goldTotal}) | ` +
        `${r.noiseFilterRate.toFixed(1).padStart(5)}% (${r.noiseFilter}/${r.noiseTotal}) | ` +
        `${r.compositeScore.toFixed(1).padStart(5)} | ` +
        `阈值=${p.entryThreshold} 接盘MCAP=${p.lateFollowerMcapThreshold/1000}K SM<${p.lateFollowerSmThreshold} 扣${-p.lateFollowerPenalty}分`
    );
});

// 最优参数
if (results.length > 0) {
    const best = results[0];
    console.log(`\n${'═'.repeat(70)}`);
    console.log(`🏆 最优参数组合`);
    console.log(`${'═'.repeat(70)}\n`);
    
    console.log(`金狗命中率: ${best.goldHitRate.toFixed(1)}% (${best.goldHit}/${best.goldTotal})`);
    console.log(`噪音过滤率: ${best.noiseFilterRate.toFixed(1)}% (${best.noiseFilter}/${best.noiseTotal})`);
    console.log(`综合得分: ${best.compositeScore.toFixed(1)}`);
    console.log(`\n参数配置:`);
    console.log(JSON.stringify(best.params, null, 2));
    
    // 保存结果
    fs.writeFileSync(
        path.join(__dirname, '..', 'data', 'parameter-backtest-results.json'),
        JSON.stringify({ best: best, top20: results.slice(0, 20) }, null, 2)
    );
}

// 当前参数对比
console.log(`\n${'═'.repeat(70)}`);
console.log(`📊 当前参数 vs 最优参数 对比`);
console.log(`${'═'.repeat(70)}\n`);

const currentParams = {
    entryThreshold: 30,
    lateFollowerMcapThreshold: 500000,
    lateFollowerSmThreshold: 5,
    lateFollowerPenalty: -30,
    riskyMcapThreshold: 500000,
    riskyPenalty: -15,
    smBonusThreshold: 10,
    smBonus: 0
};

const currentResult = runBacktest(currentParams);
console.log(`当前参数:`);
console.log(`  金狗命中率: ${currentResult.goldHitRate.toFixed(1)}%`);
console.log(`  噪音过滤率: ${currentResult.noiseFilterRate.toFixed(1)}%`);
console.log(`  综合得分: ${currentResult.compositeScore.toFixed(1)}`);

if (results.length > 0) {
    const best = results[0];
    console.log(`\n最优参数:`);
    console.log(`  金狗命中率: ${best.goldHitRate.toFixed(1)}% (提升 ${(best.goldHitRate - currentResult.goldHitRate).toFixed(1)}%)`);
    console.log(`  噪音过滤率: ${best.noiseFilterRate.toFixed(1)}% (变化 ${(best.noiseFilterRate - currentResult.noiseFilterRate).toFixed(1)}%)`);
    console.log(`  综合得分: ${best.compositeScore.toFixed(1)} (提升 ${(best.compositeScore - currentResult.compositeScore).toFixed(1)})`);
}
