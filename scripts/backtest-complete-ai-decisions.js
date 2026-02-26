#!/usr/bin/env node
/**
 * 完整历史AI决策回测
 *
 * 分析logs/ai/batch_*.log中的所有AI决策与金狗列表的匹配情况
 */

import 'dotenv/config';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// 加载金狗数据
const backtestPath = path.join(__dirname, '..', 'data', 'extended-backtest-results.json');
const backtestData = JSON.parse(fs.readFileSync(backtestPath, 'utf-8'));
const goldDogList = backtestData.goldDogList || [];

console.log(`\n${'═'.repeat(70)}`);
console.log(`🔬 完整历史AI决策回测 (分析所有batch日志)`);
console.log(`${'═'.repeat(70)}`);

// 创建金狗查找表
const goldDogSymbols = new Set();
goldDogList.forEach(dog => {
    if (dog.symbol) goldDogSymbols.add(dog.symbol.toLowerCase());
});

console.log(`\n📊 金狗数据:`);
console.log(`   金狗总数: ${goldDogList.length}`);
console.log(`   金狗Symbol数: ${goldDogSymbols.size}`);

// 读取所有batch日志
const aiLogDir = path.join(__dirname, '..', 'logs', 'ai');
const logFiles = fs.readdirSync(aiLogDir)
    .filter(f => f.startsWith('batch_') && f.endsWith('.log'))
    .sort();

console.log(`   批量日志文件数: ${logFiles.length}`);

// 解析日志中的AI建议
function parseBatchLog(content) {
    const decisions = [];

    // 匹配 [BUY] 建议入手:
    const buyBlockMatch = content.match(/🟢\s*\[BUY\]\s*建议入手:\s*([^\n]+)/g);
    if (buyBlockMatch) {
        for (const block of buyBlockMatch) {
            // 提取币名列表
            const symbolsMatch = block.match(/建议入手:\s*(.+)/);
            if (symbolsMatch) {
                const symbols = symbolsMatch[1].split(/[,，]/);
                for (const s of symbols) {
                    const clean = s.trim().replace(/^\$/, '').replace(/\s+.*$/, '');
                    if (clean && clean.length > 0) {
                        decisions.push({ action: 'BUY', symbol: clean });
                    }
                }
            }
        }
    }

    // 匹配 [WATCH] 继续观察:
    const watchBlockMatch = content.match(/🟡\s*\[WATCH\]\s*继续观察:\s*([^\n]+)/g);
    if (watchBlockMatch) {
        for (const block of watchBlockMatch) {
            const symbolsMatch = block.match(/继续观察:\s*(.+)/);
            if (symbolsMatch) {
                const symbols = symbolsMatch[1].split(/[,，]/);
                for (const s of symbols) {
                    const clean = s.trim().replace(/^\$/, '').replace(/\s+.*$/, '');
                    if (clean && clean.length > 0) {
                        decisions.push({ action: 'WATCH', symbol: clean });
                    }
                }
            }
        }
    }

    // 匹配 DISCARD 在分析文本中
    const discardMatches = content.matchAll(/DISCARD[:\s]+\$?([A-Za-z0-9\u4e00-\u9fa5]+)/gi);
    for (const match of discardMatches) {
        decisions.push({ action: 'DISCARD', symbol: match[1] });
    }

    // 匹配毕业的token
    const graduateMatches = content.matchAll(/🚀\s*\[GRADUATE\]\s*([^\s]+)\s*毕业/g);
    for (const match of graduateMatches) {
        decisions.push({ action: 'GRADUATE', symbol: match[1] });
    }

    return decisions;
}

// 检查是否为金狗
function isGoldDog(symbol) {
    if (!symbol) return false;
    const cleanSymbol = symbol.replace(/\.\.\.$/, '').toLowerCase();
    if (goldDogSymbols.has(cleanSymbol)) return true;
    for (const gs of goldDogSymbols) {
        if (gs.startsWith(cleanSymbol) || cleanSymbol.startsWith(gs)) {
            return true;
        }
    }
    return false;
}

// 统计
const stats = {
    totalFiles: logFiles.length,
    buyDecisions: [],
    watchDecisions: [],
    discardDecisions: [],
    graduateDecisions: [],
};

// 分析每个日志文件
for (const file of logFiles) {
    const content = fs.readFileSync(path.join(aiLogDir, file), 'utf-8');
    const timestamp = file.replace('batch_', '').replace('.log', '');
    const decisions = parseBatchLog(content);

    for (const d of decisions) {
        const isGold = isGoldDog(d.symbol);
        const record = { timestamp, symbol: d.symbol, isGold };

        switch (d.action) {
            case 'BUY':
                stats.buyDecisions.push(record);
                break;
            case 'WATCH':
                stats.watchDecisions.push(record);
                break;
            case 'DISCARD':
                stats.discardDecisions.push(record);
                break;
            case 'GRADUATE':
                stats.graduateDecisions.push(record);
                break;
        }
    }
}

// 去重统计 (同一个symbol可能多次出现)
const uniqueBuy = [...new Set(stats.buyDecisions.map(d => d.symbol))];
const uniqueWatch = [...new Set(stats.watchDecisions.map(d => d.symbol))];
const uniqueGraduate = [...new Set(stats.graduateDecisions.map(d => d.symbol))];

// 输出结果
console.log(`\n${'═'.repeat(70)}`);
console.log(`📊 AI决策统计 (全部日志)`);
console.log(`${'═'.repeat(70)}`);

console.log(`\n【决策分布】`);
console.log(`  BUY建议: ${stats.buyDecisions.length} 次 (${uniqueBuy.length} 唯一币)`);
console.log(`  WATCH建议: ${stats.watchDecisions.length} 次 (${uniqueWatch.length} 唯一币)`);
console.log(`  GRADUATE毕业: ${stats.graduateDecisions.length} 次 (${uniqueGraduate.length} 唯一币)`);
console.log(`  DISCARD: ${stats.discardDecisions.length} 次`);

// 命中分析
const buyHits = stats.buyDecisions.filter(d => d.isGold);
const watchHits = stats.watchDecisions.filter(d => d.isGold);
const graduateHits = stats.graduateDecisions.filter(d => d.isGold);
const discardHits = stats.discardDecisions.filter(d => d.isGold);

// 唯一命中
const uniqueBuyHits = [...new Set(buyHits.map(d => d.symbol))];
const uniqueGraduateHits = [...new Set(graduateHits.map(d => d.symbol))];

const buyPrecision = uniqueBuy.length > 0
    ? (uniqueBuyHits.length / uniqueBuy.length * 100).toFixed(1)
    : 0;
const graduatePrecision = uniqueGraduate.length > 0
    ? (uniqueGraduateHits.length / uniqueGraduate.length * 100).toFixed(1)
    : 0;

console.log(`\n【命中分析 (唯一币)】`);
console.log(`  BUY命中金狗: ${uniqueBuyHits.length}/${uniqueBuy.length} = ${buyPrecision}%`);
console.log(`  GRADUATE命中金狗: ${uniqueGraduateHits.length}/${uniqueGraduate.length} = ${graduatePrecision}%`);

// 金狗覆盖率
const allHitSymbols = new Set([
    ...buyHits.map(d => d.symbol.toLowerCase()),
    ...watchHits.map(d => d.symbol.toLowerCase()),
    ...graduateHits.map(d => d.symbol.toLowerCase())
]);
const recall = goldDogSymbols.size > 0
    ? (allHitSymbols.size / goldDogSymbols.size * 100).toFixed(1)
    : 0;

console.log(`\n【金狗覆盖率】`);
console.log(`  AI发现的金狗: ${allHitSymbols.size}/${goldDogSymbols.size} = ${recall}%`);

// 详细列表
console.log(`\n${'═'.repeat(70)}`);
console.log(`📋 BUY/GRADUATE命中的金狗`);
console.log(`${'═'.repeat(70)}`);

if (uniqueBuyHits.length > 0 || uniqueGraduateHits.length > 0) {
    const allGoldHits = [...new Set([...uniqueBuyHits, ...uniqueGraduateHits])];
    allGoldHits.forEach(s => {
        console.log(`  ✅ $${s}`);
    });
} else {
    console.log(`  (无)`);
}

console.log(`\n【DISCARD但实际是金狗（漏掉的）】`);
const uniqueDiscardHits = [...new Set(discardHits.map(d => d.symbol))];
if (uniqueDiscardHits.length > 0) {
    uniqueDiscardHits.forEach(s => {
        console.log(`  ❌ $${s}`);
    });
} else {
    console.log(`  (无)`);
}

// 关键指标
console.log(`\n${'═'.repeat(70)}`);
console.log(`📈 关键指标总结`);
console.log(`${'═'.repeat(70)}`);

console.log(`\n  🎯 AI BUY决策胜率: ${buyPrecision}%`);
console.log(`  🚀 AI GRADUATE胜率: ${graduatePrecision}%`);
console.log(`  📈 AI金狗覆盖率: ${recall}%`);

const targetWinRate = 70;
console.log(`\n  目标胜率: ${targetWinRate}%`);
if (parseFloat(graduatePrecision) >= targetWinRate) {
    console.log(`  ✅ GRADUATE决策已达到${targetWinRate}%胜率目标!`);
} else if (parseFloat(buyPrecision) >= targetWinRate) {
    console.log(`  ✅ BUY决策已达到${targetWinRate}%胜率目标!`);
} else {
    const gap = targetWinRate - Math.max(parseFloat(buyPrecision), parseFloat(graduatePrecision));
    console.log(`  ❌ 未达到${targetWinRate}%目标 (差距: ${gap.toFixed(1)}%)`);
}

// 保存结果
const output = {
    timestamp: new Date().toISOString(),
    dataSource: 'logs/ai/batch_*.log',
    logFiles: logFiles.length,
    stats: {
        goldDogCount: goldDogList.length,
        uniqueBuyDecisions: uniqueBuy.length,
        uniqueWatchDecisions: uniqueWatch.length,
        uniqueGraduateDecisions: uniqueGraduate.length,
        uniqueDiscardDecisions: [...new Set(stats.discardDecisions.map(d => d.symbol))].length,
        buyHits: uniqueBuyHits.length,
        graduateHits: uniqueGraduateHits.length,
        buyPrecision: parseFloat(buyPrecision),
        graduatePrecision: parseFloat(graduatePrecision),
        recall: parseFloat(recall)
    },
    goldHits: {
        buy: uniqueBuyHits,
        graduate: uniqueGraduateHits,
        discardMissed: uniqueDiscardHits
    }
};

fs.writeFileSync(
    path.join(__dirname, '..', 'data', 'complete-historical-ai-backtest.json'),
    JSON.stringify(output, null, 2)
);

console.log(`\n✅ 结果已保存到 data/complete-historical-ai-backtest.json`);
