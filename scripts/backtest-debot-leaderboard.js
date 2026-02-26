#!/usr/bin/env node
/**
 * DeBot 金狗银狗榜单回测分析
 * 
 * 1. 从日志提取DeBot标记的金狗银狗 (gold/silver等级 + 涨幅>2x)
 * 2. 验证这些token在我们系统中是被买入还是被过滤
 * 3. 计算命中率和噪音过滤率
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const logsDir = path.join(__dirname, '..', 'logs');

// 查找最近3天的日志文件
const logFiles = fs.readdirSync(logsDir)
    .filter(f => f.endsWith('.log') && (f.includes('restart') || f.includes('debug')))
    .map(f => ({
        name: f,
        path: path.join(logsDir, f),
        mtime: fs.statSync(path.join(logsDir, f)).mtime
    }))
    .sort((a, b) => b.mtime - a.mtime)
    .slice(0, 5);

console.log(`\n${'═'.repeat(70)}`);
console.log(`📊 DeBot 金狗银狗榜单回测分析`);
console.log(`${'═'.repeat(70)}`);
console.log(`分析最近的日志文件:`);
logFiles.forEach(f => console.log(`  - ${f.name} (${(fs.statSync(f.path).size / 1024 / 1024).toFixed(1)}MB)`));

// 数据结构
const goldDogs = new Map();   // 官方金狗 (gold级 或 涨幅>10x)
const silverDogs = new Map(); // 官方银狗 (silver级 或 涨幅>2x)
const allSignals = new Map(); // 所有DeBot信号

const systemBought = new Set();  // 系统买入的
const systemFiltered = new Set(); // 系统过滤的

// 解析日志
for (const logFile of logFiles) {
    console.log(`\n解析 ${logFile.name}...`);
    const content = fs.readFileSync(logFile.path, 'utf-8');
    const lines = content.split('\n');
    
    let currentToken = null;
    let currentData = {};
    
    for (let i = 0; i < lines.length; i++) {
        const line = lines[i];
        
        // 1. 提取DeBot AI SIGNAL (金狗银狗)
        const signalMatch = line.match(/\[DeBot Scout\] (🥇|🥈|🔥) AI SIGNAL.*: ([A-Za-z0-9]+)/);
        if (signalMatch) {
            currentToken = signalMatch[2];
            currentData = { address: currentToken, emoji: signalMatch[1] };
        }
        
        // 2. 提取等级
        if (currentToken && line.includes('[DeBot Scout]') && line.includes('等级:')) {
            const levelMatch = line.match(/等级:\s*(gold|silver|bronze)/i);
            if (levelMatch) {
                currentData.level = levelMatch[1].toLowerCase();
            }
        }
        
        // 3. 提取涨幅
        if (currentToken && line.includes('最大涨幅:')) {
            const gainMatch = line.match(/最大涨幅:\s*([0-9.]+)x/);
            if (gainMatch) {
                currentData.maxGain = parseFloat(gainMatch[1]);
                
                // 保存到对应分类
                if (currentData.level === 'gold' || currentData.maxGain >= 10) {
                    if (!goldDogs.has(currentToken)) {
                        goldDogs.set(currentToken, currentData);
                    }
                } else if (currentData.level === 'silver' || currentData.maxGain >= 2) {
                    if (!silverDogs.has(currentToken)) {
                        silverDogs.set(currentToken, currentData);
                    }
                }
                
                allSignals.set(currentToken, currentData);
                currentToken = null;
                currentData = {};
            }
        }
        
        // 4. 检测系统买入 (毕业)
        const graduateMatch = line.match(/\[GRADUATE\]\s+(.+?)\s+毕业了/);
        if (graduateMatch) {
            // 需要找到对应的地址
        }
        
        // 也检测地址形式的毕业
        const graduateAddrMatch = line.match(/\[GRADUATE\]\s+([A-Za-z0-9]{8,})/);
        if (graduateAddrMatch) {
            systemBought.add(graduateAddrMatch[1].substring(0, 20));
        }
        
        // 5. 检测系统过滤 (不入池)
        if (line.includes('不入池')) {
            // 往上找地址
            for (let j = i - 1; j >= Math.max(0, i - 30); j--) {
                const prevLine = lines[j];
                if (prevLine.includes('market/metrics') || prevLine.includes('验证完成')) {
                    const addrMatch = prevLine.match(/"address":"([A-Za-z0-9]+)"|验证完成:\s*([A-Za-z0-9]+)/);
                    if (addrMatch) {
                        const addr = addrMatch[1] || addrMatch[2];
                        systemFiltered.add(addr.substring(0, 20));
                        break;
                    }
                }
            }
        }
    }
}

// 输出金狗银狗榜单
console.log(`\n${'═'.repeat(70)}`);
console.log(`🥇 DeBot 官方金狗榜单 (${goldDogs.size}个)`);
console.log(`${'═'.repeat(70)}`);

const goldList = Array.from(goldDogs.values()).sort((a, b) => (b.maxGain || 0) - (a.maxGain || 0));
goldList.slice(0, 15).forEach((dog, i) => {
    const addr = dog.address.substring(0, 12);
    const bought = systemBought.has(dog.address.substring(0, 20)) ? '✅买入' : 
                   systemFiltered.has(dog.address.substring(0, 20)) ? '❌过滤' : '❓未知';
    console.log(`  ${i+1}. ${addr}... | ${dog.level || 'gold'} | 涨幅:${(dog.maxGain || 0).toFixed(1)}x | ${bought}`);
});

console.log(`\n${'═'.repeat(70)}`);
console.log(`🥈 DeBot 官方银狗榜单 (${silverDogs.size}个)`);
console.log(`${'═'.repeat(70)}`);

const silverList = Array.from(silverDogs.values()).sort((a, b) => (b.maxGain || 0) - (a.maxGain || 0));
silverList.slice(0, 15).forEach((dog, i) => {
    const addr = dog.address.substring(0, 12);
    const bought = systemBought.has(dog.address.substring(0, 20)) ? '✅买入' : 
                   systemFiltered.has(dog.address.substring(0, 20)) ? '❌过滤' : '❓未知';
    console.log(`  ${i+1}. ${addr}... | ${dog.level || 'silver'} | 涨幅:${(dog.maxGain || 0).toFixed(1)}x | ${bought}`);
});

// 统计命中率
console.log(`\n${'═'.repeat(70)}`);
console.log(`📊 命中率分析`);
console.log(`${'═'.repeat(70)}`);

let goldBought = 0, goldFiltered = 0, goldUnknown = 0;
for (const dog of goldDogs.values()) {
    const prefix = dog.address.substring(0, 20);
    if (systemBought.has(prefix)) goldBought++;
    else if (systemFiltered.has(prefix)) goldFiltered++;
    else goldUnknown++;
}

let silverBought = 0, silverFiltered = 0, silverUnknown = 0;
for (const dog of silverDogs.values()) {
    const prefix = dog.address.substring(0, 20);
    if (systemBought.has(prefix)) silverBought++;
    else if (systemFiltered.has(prefix)) silverFiltered++;
    else silverUnknown++;
}

console.log(`\n🥇 金狗命中率:`);
console.log(`   总数: ${goldDogs.size}`);
console.log(`   ✅ 买入: ${goldBought} (${(goldBought/goldDogs.size*100).toFixed(1)}%)`);
console.log(`   ❌ 过滤: ${goldFiltered} (${(goldFiltered/goldDogs.size*100).toFixed(1)}%)`);
console.log(`   ❓ 未知: ${goldUnknown} (${(goldUnknown/goldDogs.size*100).toFixed(1)}%)`);

console.log(`\n🥈 银狗命中率:`);
console.log(`   总数: ${silverDogs.size}`);
console.log(`   ✅ 买入: ${silverBought} (${(silverBought/silverDogs.size*100).toFixed(1)}%)`);
console.log(`   ❌ 过滤: ${silverFiltered} (${(silverFiltered/silverDogs.size*100).toFixed(1)}%)`);
console.log(`   ❓ 未知: ${silverUnknown} (${(silverUnknown/silverDogs.size*100).toFixed(1)}%)`);

// 噪音过滤统计
console.log(`\n${'═'.repeat(70)}`);
console.log(`🔇 噪音过滤分析`);
console.log(`${'═'.repeat(70)}`);

const totalSignals = allSignals.size;
const goodSignals = goldDogs.size + silverDogs.size;
const noiseSignals = totalSignals - goodSignals;

console.log(`   总DeBot信号: ${totalSignals}`);
console.log(`   金狗+银狗: ${goodSignals} (${(goodSignals/totalSignals*100).toFixed(1)}%)`);
console.log(`   噪音信号: ${noiseSignals} (${(noiseSignals/totalSignals*100).toFixed(1)}%)`);
console.log(`   系统买入: ${systemBought.size}`);
console.log(`   系统过滤: ${systemFiltered.size}`);

// 保存结果
const results = {
    analyzed_at: new Date().toISOString(),
    goldDogs: goldList,
    silverDogs: silverList,
    stats: {
        gold: { total: goldDogs.size, bought: goldBought, filtered: goldFiltered, unknown: goldUnknown },
        silver: { total: silverDogs.size, bought: silverBought, filtered: silverFiltered, unknown: silverUnknown },
        noise: { total: noiseSignals }
    }
};

const outputPath = path.join(__dirname, '..', 'data', 'debot-leaderboard-backtest.json');
fs.writeFileSync(outputPath, JSON.stringify(results, null, 2));
console.log(`\n结果已保存到: ${outputPath}`);
