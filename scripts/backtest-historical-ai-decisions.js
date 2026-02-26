#!/usr/bin/env node
/**
 * 历史AI决策回测
 *
 * 分析ai_analysis_log.json中的AI决策与金狗列表的匹配情况
 * 这使用的是当时的AI判断，而非现在的实时数据
 */

import 'dotenv/config';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// 加载数据
const aiLogPath = path.join(__dirname, '..', 'data', 'ai_analysis_log.json');
const backtestPath = path.join(__dirname, '..', 'data', 'extended-backtest-results.json');

const aiLogs = JSON.parse(fs.readFileSync(aiLogPath, 'utf-8'));
const backtestData = JSON.parse(fs.readFileSync(backtestPath, 'utf-8'));
const goldDogList = backtestData.goldDogList || [];

console.log(`\n${'═'.repeat(70)}`);
console.log(`🔬 历史AI决策回测 (基于信号时刻的AI分析)`);
console.log(`${'═'.repeat(70)}`);

console.log(`\n📊 数据规模:`);
console.log(`   AI分析日志: ${aiLogs.length} 条`);
console.log(`   金狗列表: ${goldDogList.length} 个`);

// 创建金狗查找表 (使用合约地址)
const goldDogSet = new Set();
const goldDogSymbols = new Set();

goldDogList.forEach(dog => {
    if (dog.ca) goldDogSet.add(dog.ca.toLowerCase());
    if (dog.tokenAddress) goldDogSet.add(dog.tokenAddress.toLowerCase());
    if (dog.symbol) goldDogSymbols.add(dog.symbol.toLowerCase());
});

console.log(`   金狗地址数: ${goldDogSet.size}`);
console.log(`   金狗Symbol数: ${goldDogSymbols.size}`);

// 解析AI响应，提取决策
function parseAIDecision(response) {
    const decisions = [];

    // 匹配 BUY 决策
    const buyMatches = response.matchAll(/\*?\*?BUY:?\s*\$?([^\s\(\*]+)\s*(?:\(CA:\s*([^\)]+)\))?/gi);
    for (const match of buyMatches) {
        decisions.push({
            action: 'BUY',
            symbol: match[1],
            ca: match[2] || null
        });
    }

    // 匹配 WATCH 决策
    const watchMatches = response.matchAll(/\*?\*?WATCH:?\s*\$?([^\s\(\*]+)/gi);
    for (const match of watchMatches) {
        decisions.push({
            action: 'WATCH',
            symbol: match[1],
            ca: null
        });
    }

    // 匹配 DISCARD 决策
    const discardMatches = response.matchAll(/\*?\*?DISCARD:?\s*\$?([^\s\(\*]+)\s*(?:\(CA:\s*([^\)]+)\))?/gi);
    for (const match of discardMatches) {
        decisions.push({
            action: 'DISCARD',
            symbol: match[1],
            ca: match[2] || null
        });
    }

    return decisions;
}

// 检查是否为金狗
function isGoldDog(symbol, ca) {
    // 检查地址
    if (ca && goldDogSet.has(ca.toLowerCase())) {
        return true;
    }
    // 检查symbol (处理截断的symbol)
    if (symbol) {
        const cleanSymbol = symbol.replace(/\.\.\.$/, '').toLowerCase();
        if (goldDogSymbols.has(cleanSymbol)) {
            return true;
        }
        // 部分匹配
        for (const gs of goldDogSymbols) {
            if (gs.startsWith(cleanSymbol) || cleanSymbol.startsWith(gs)) {
                return true;
            }
        }
    }
    return false;
}

// 分析所有AI决策
const stats = {
    totalDecisions: 0,
    buyDecisions: [],
    watchDecisions: [],
    discardDecisions: [],
    buyHits: 0, // BUY决策命中金狗
    watchHits: 0, // WATCH决策命中金狗
    discardHits: 0, // DISCARD决策却是金狗 (漏掉)
};

for (const log of aiLogs) {
    if (!log.response) continue;

    const decisions = parseAIDecision(log.response);

    for (const decision of decisions) {
        stats.totalDecisions++;
        const isGold = isGoldDog(decision.symbol, decision.ca);

        const record = {
            timestamp: log.timestamp,
            symbol: decision.symbol,
            ca: decision.ca,
            isGold
        };

        switch (decision.action) {
            case 'BUY':
                stats.buyDecisions.push(record);
                if (isGold) stats.buyHits++;
                break;
            case 'WATCH':
                stats.watchDecisions.push(record);
                if (isGold) stats.watchHits++;
                break;
            case 'DISCARD':
                stats.discardDecisions.push(record);
                if (isGold) stats.discardHits++;
                break;
        }
    }
}

// 输出结果
console.log(`\n${'═'.repeat(70)}`);
console.log(`📊 AI决策统计`);
console.log(`${'═'.repeat(70)}`);

console.log(`\n【决策分布】`);
console.log(`  BUY决策: ${stats.buyDecisions.length}`);
console.log(`  WATCH决策: ${stats.watchDecisions.length}`);
console.log(`  DISCARD决策: ${stats.discardDecisions.length}`);
console.log(`  总决策: ${stats.totalDecisions}`);

console.log(`\n【命中分析】`);
const buyPrecision = stats.buyDecisions.length > 0
    ? (stats.buyHits / stats.buyDecisions.length * 100).toFixed(1)
    : 0;
const watchPrecision = stats.watchDecisions.length > 0
    ? (stats.watchHits / stats.watchDecisions.length * 100).toFixed(1)
    : 0;
const discardMissRate = stats.discardDecisions.length > 0
    ? (stats.discardHits / stats.discardDecisions.length * 100).toFixed(1)
    : 0;

console.log(`  BUY命中金狗: ${stats.buyHits}/${stats.buyDecisions.length} = ${buyPrecision}%`);
console.log(`  WATCH命中金狗: ${stats.watchHits}/${stats.watchDecisions.length} = ${watchPrecision}%`);
console.log(`  DISCARD漏掉金狗: ${stats.discardHits}/${stats.discardDecisions.length} = ${discardMissRate}%`);

// 金狗覆盖率
const allHits = stats.buyHits + stats.watchHits;
const recall = goldDogList.length > 0
    ? (allHits / goldDogList.length * 100).toFixed(1)
    : 0;
console.log(`\n【金狗覆盖率】`);
console.log(`  BUY+WATCH发现金狗: ${allHits}/${goldDogList.length} = ${recall}%`);

// 详细列出命中的金狗
console.log(`\n${'═'.repeat(70)}`);
console.log(`📋 BUY决策命中的金狗`);
console.log(`${'═'.repeat(70)}`);

const buyGolds = stats.buyDecisions.filter(d => d.isGold);
if (buyGolds.length > 0) {
    buyGolds.forEach(d => {
        console.log(`  ✅ $${d.symbol} | ${d.timestamp.slice(0, 16)}`);
    });
} else {
    console.log(`  (无)`);
}

console.log(`\n【WATCH决策命中的金狗】`);
const watchGolds = stats.watchDecisions.filter(d => d.isGold);
if (watchGolds.length > 0) {
    watchGolds.forEach(d => {
        console.log(`  🟡 $${d.symbol} | ${d.timestamp.slice(0, 16)}`);
    });
} else {
    console.log(`  (无)`);
}

console.log(`\n【DISCARD但实际是金狗（漏掉的）】`);
const discardGolds = stats.discardDecisions.filter(d => d.isGold);
if (discardGolds.length > 0) {
    discardGolds.forEach(d => {
        console.log(`  ❌ $${d.symbol} | ${d.timestamp.slice(0, 16)}`);
    });
} else {
    console.log(`  (无)`);
}

// 计算综合指标
console.log(`\n${'═'.repeat(70)}`);
console.log(`📈 关键指标总结`);
console.log(`${'═'.repeat(70)}`);

console.log(`\n  🎯 AI BUY决策胜率: ${buyPrecision}%`);
console.log(`  📈 AI覆盖率(BUY+WATCH): ${recall}%`);
console.log(`  ⚠️ DISCARD误杀率: ${discardMissRate}%`);

const targetWinRate = 70;
console.log(`\n  目标胜率: ${targetWinRate}%`);
if (parseFloat(buyPrecision) >= targetWinRate) {
    console.log(`  ✅ AI BUY决策已达到${targetWinRate}%胜率目标!`);
} else {
    console.log(`  ❌ AI BUY决策未达到${targetWinRate}%目标 (差距: ${(targetWinRate - parseFloat(buyPrecision)).toFixed(1)}%)`);
}

// 保存结果
const output = {
    timestamp: new Date().toISOString(),
    dataSource: 'ai_analysis_log.json',
    stats: {
        totalAILogs: aiLogs.length,
        goldDogCount: goldDogList.length,
        totalDecisions: stats.totalDecisions,
        buyDecisions: stats.buyDecisions.length,
        watchDecisions: stats.watchDecisions.length,
        discardDecisions: stats.discardDecisions.length,
        buyHits: stats.buyHits,
        watchHits: stats.watchHits,
        discardHits: stats.discardHits,
        buyPrecision: parseFloat(buyPrecision),
        recall: parseFloat(recall),
        discardMissRate: parseFloat(discardMissRate)
    },
    goldHits: {
        buy: buyGolds.map(d => ({ symbol: d.symbol, timestamp: d.timestamp })),
        watch: watchGolds.map(d => ({ symbol: d.symbol, timestamp: d.timestamp })),
        discard: discardGolds.map(d => ({ symbol: d.symbol, timestamp: d.timestamp }))
    }
};

fs.writeFileSync(
    path.join(__dirname, '..', 'data', 'historical-ai-backtest-result.json'),
    JSON.stringify(output, null, 2)
);

console.log(`\n✅ 结果已保存到 data/historical-ai-backtest-result.json`);
