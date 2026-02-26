#!/usr/bin/env node
/**
 * 完整回测分析 - 使用DeBot官方金狗银狗数据
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const logPath = path.join(__dirname, '..', 'logs', 'restart_v75_20260113_002033.log');
const content = fs.readFileSync(logPath, 'utf-8');

console.log(`\n${'═'.repeat(70)}`);
console.log(`📊 DeBot 金狗银狗完整回测分析`);
console.log(`${'═'.repeat(70)}\n`);

// 1. 提取所有DeBot标记的金狗银狗
const goldDogs = [];
const silverDogs = [];
const allTokens = [];

// 匹配 AI SIGNAL 块
const signalRegex = /\[DeBot Scout\] (🥇|🥈) AI SIGNAL #\d+: ([A-Za-z0-9]+)[\s\S]*?等级:\s*(gold|silver)[\s\S]*?最大涨幅:\s*([0-9.]+)x/g;

let match;
while ((match = signalRegex.exec(content)) !== null) {
    const emoji = match[1];
    const address = match[2];
    const level = match[3];
    const gain = parseFloat(match[4]);
    
    const token = { address, level, gain, emoji };
    
    // 避免重复
    if (!allTokens.find(t => t.address === address)) {
        allTokens.push(token);
        if (level === 'gold' || gain >= 10) {
            goldDogs.push(token);
        } else if (level === 'silver' || gain >= 2) {
            silverDogs.push(token);
        }
    }
}

console.log(`发现 DeBot 官方金狗: ${goldDogs.length} 个`);
console.log(`发现 DeBot 官方银狗: ${silverDogs.length} 个`);

// 2. 从日志中提取处理结果
// 提取毕业的token
const graduatedTokens = new Set();
const graduateMatches = content.matchAll(/\[GRADUATE\]\s+([^\s]+)\s+毕业/g);
for (const m of graduateMatches) {
    graduatedTokens.add(m[1]);
}

// 提取被过滤的token及其七维分
const filteredTokens = new Map();
const filterMatches = content.matchAll(/\[Filter\]\s+([^\s]+)\s+七维分\s+(\d+)\s*[<≤]\s*30/g);
for (const m of filterMatches) {
    if (!filteredTokens.has(m[1])) {
        filteredTokens.set(m[1], parseInt(m[2]));
    }
}

console.log(`\n系统处理统计:`);
console.log(`  毕业(买入): ${graduatedTokens.size} 个`);
console.log(`  过滤(不入池): ${filteredTokens.size} 个`);

// 3. 检查每个金狗银狗的处理结果
console.log(`\n${'═'.repeat(70)}`);
console.log(`🥇 金狗处理结果 (涨幅>=10x 或 gold级)`);
console.log(`${'═'.repeat(70)}`);

let goldBought = 0, goldFiltered = 0, goldUnknown = 0;
const goldFilteredList = [];

// 我们需要用symbol来匹配，因为地址格式可能不同
// 从日志提取 address -> symbol 映射
const addrToSymbol = new Map();
const symbolMatches = content.matchAll(/"symbol":"([^"]+)"[\s\S]*?"address":"([A-Za-z0-9]+)"/g);
for (const m of symbolMatches) {
    addrToSymbol.set(m[2], m[1]);
}

// 反向映射
const symbolToAddr = new Map();
for (const [addr, sym] of addrToSymbol) {
    symbolToAddr.set(sym, addr);
}

for (const dog of goldDogs.sort((a, b) => b.gain - a.gain)) {
    const symbol = addrToSymbol.get(dog.address) || dog.address.substring(0, 8);
    
    let status = '❓未知';
    let score = null;
    
    // 检查是否毕业
    if (graduatedTokens.has(symbol) || graduatedTokens.has(dog.address.substring(0, 8))) {
        status = '✅买入';
        goldBought++;
    }
    // 检查是否被过滤
    else if (filteredTokens.has(symbol)) {
        status = '❌过滤';
        score = filteredTokens.get(symbol);
        goldFiltered++;
        goldFilteredList.push({ ...dog, symbol, score });
    } else {
        goldUnknown++;
    }
    
    console.log(`  ${symbol.padEnd(15)} | ${dog.level.padEnd(6)} | 涨幅:${dog.gain.toFixed(1).padStart(5)}x | ${status}${score ? ` (七维分:${score})` : ''}`);
}

console.log(`\n${'═'.repeat(70)}`);
console.log(`🥈 银狗处理结果 (涨幅2-10x 或 silver级)`);
console.log(`${'═'.repeat(70)}`);

let silverBought = 0, silverFiltered = 0, silverUnknown = 0;

for (const dog of silverDogs.sort((a, b) => b.gain - a.gain).slice(0, 20)) {
    const symbol = addrToSymbol.get(dog.address) || dog.address.substring(0, 8);
    
    let status = '❓未知';
    let score = null;
    
    if (graduatedTokens.has(symbol) || graduatedTokens.has(dog.address.substring(0, 8))) {
        status = '✅买入';
        silverBought++;
    } else if (filteredTokens.has(symbol)) {
        status = '❌过滤';
        score = filteredTokens.get(symbol);
        silverFiltered++;
    } else {
        silverUnknown++;
    }
    
    console.log(`  ${symbol.padEnd(15)} | ${dog.level.padEnd(6)} | 涨幅:${dog.gain.toFixed(1).padStart(5)}x | ${status}${score ? ` (七维分:${score})` : ''}`);
}

// 4. 计算噪音过滤率
// 统计所有被过滤的token中，有多少是真正的噪音
const allFilteredSymbols = Array.from(filteredTokens.keys());
const goldSilverSymbols = new Set([
    ...goldDogs.map(d => addrToSymbol.get(d.address) || d.address.substring(0, 8)),
    ...silverDogs.map(d => addrToSymbol.get(d.address) || d.address.substring(0, 8))
]);

let noiseFiltered = 0;
let goodFiltered = 0;
for (const sym of allFilteredSymbols) {
    if (goldSilverSymbols.has(sym)) {
        goodFiltered++;
    } else {
        noiseFiltered++;
    }
}

// 5. 汇总报告
console.log(`\n${'═'.repeat(70)}`);
console.log(`📊 回测汇总报告`);
console.log(`${'═'.repeat(70)}`);

console.log(`\n🥇 金狗命中率:`);
console.log(`   总数: ${goldDogs.length}`);
console.log(`   ✅ 买入: ${goldBought} (${(goldBought/goldDogs.length*100).toFixed(1)}%)`);
console.log(`   ❌ 过滤: ${goldFiltered} (${(goldFiltered/goldDogs.length*100).toFixed(1)}%) ← 漏杀!`);
console.log(`   ❓ 未处理: ${goldUnknown} (${(goldUnknown/goldDogs.length*100).toFixed(1)}%)`);

console.log(`\n🥈 银狗命中率:`);
console.log(`   总数: ${silverDogs.length}`);
console.log(`   ✅ 买入: ${silverBought} (${(silverBought/silverDogs.length*100).toFixed(1)}%)`);
console.log(`   ❌ 过滤: ${silverFiltered} (${(silverFiltered/silverDogs.length*100).toFixed(1)}%) ← 漏杀!`);

console.log(`\n🔇 噪音过滤分析:`);
console.log(`   总过滤: ${filteredTokens.size}`);
console.log(`   ✅ 正确过滤(噪音): ${noiseFiltered} (${(noiseFiltered/filteredTokens.size*100).toFixed(1)}%)`);
console.log(`   ❌ 错误过滤(金银狗): ${goodFiltered} (${(goodFiltered/filteredTokens.size*100).toFixed(1)}%)`);

console.log(`\n🎯 核心问题:`);
console.log(`   被过滤金狗的平均七维分: ${(goldFilteredList.reduce((s, d) => s + (d.score || 0), 0) / goldFilteredList.length).toFixed(1)}`);
console.log(`   当前七维分阈值: 30`);
console.log(`   建议调整阈值: 15-20`);

// 6. 测试不同阈值的效果
console.log(`\n${'═'.repeat(70)}`);
console.log(`🧪 不同七维分阈值的效果模拟`);
console.log(`${'═'.repeat(70)}`);

for (const threshold of [15, 18, 20, 25, 30]) {
    let goldWouldPass = 0;
    let noiseWouldPass = 0;
    
    for (const [symbol, score] of filteredTokens) {
        if (score >= threshold) {
            if (goldSilverSymbols.has(symbol)) {
                goldWouldPass++;
            } else {
                noiseWouldPass++;
            }
        }
    }
    
    const goldPassRate = ((goldFiltered - (goldFiltered - goldWouldPass)) / goldDogs.length * 100);
    const noiseFilterRate = ((filteredTokens.size - noiseWouldPass) / filteredTokens.size * 100);
    
    console.log(`   阈值=${threshold}: 金狗通过率=${goldPassRate.toFixed(0)}% | 噪音过滤率=${noiseFilterRate.toFixed(0)}%`);
}

// 保存结果
const results = {
    analyzed_at: new Date().toISOString(),
    goldDogs: goldDogs.length,
    silverDogs: silverDogs.length,
    goldBought, goldFiltered, goldUnknown,
    silverBought, silverFiltered, silverUnknown,
    noiseFiltered, goodFiltered,
    goldFilteredList
};

fs.writeFileSync(
    path.join(__dirname, '..', 'data', 'complete-backtest-results.json'),
    JSON.stringify(results, null, 2)
);
