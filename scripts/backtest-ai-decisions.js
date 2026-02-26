#!/usr/bin/env node
/**
 * AI决策回测脚本
 *
 * 分析历史AI分析日志，对比AI的买入建议与实际代币表现
 * 目标: 了解AI判断的准确率、优势和问题
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const aiLogsDir = path.join(__dirname, '..', 'logs', 'ai');
const backtestDataPath = path.join(__dirname, '..', 'data', 'extended-backtest-results.json');

console.log(`\n${'═'.repeat(70)}`);
console.log(`🤖 AI决策回测分析`);
console.log(`${'═'.repeat(70)}`);

// 加载金狗数据（用于对比）
let goldDogList = [];
try {
    const backtestData = JSON.parse(fs.readFileSync(backtestDataPath, 'utf-8'));
    goldDogList = backtestData.goldDogList || [];
    console.log(`\n加载金狗对照表: ${goldDogList.length} 个金狗`);
} catch (e) {
    console.log(`\n⚠️ 无法加载金狗对照表: ${e.message}`);
}

// 解析AI日志
function parseAILog(content) {
    const results = [];

    // 提取时间
    const timeMatch = content.match(/Time: ([\d\/: ]+)/);
    const timestamp = timeMatch ? timeMatch[1] : null;

    // 提取提到的代币
    const tokensMatch = content.match(/Tokens?: ([\$\w\., ]+)/);
    const tokensLine = tokensMatch ? tokensMatch[1] : '';

    // 解析 BUY/WATCH/DISCARD 建议
    const buyMatches = content.matchAll(/\*\*BUY\*\*:\s*\$?([\w\.\-]+)/gi);
    const watchMatches = content.matchAll(/\*\*WATCH\*\*:\s*\$?([\w\.\-]+)/gi);
    const discardMatches = content.matchAll(/\*\*DISCARD\*\*:\s*\$?([\w\.\-]+)/gi);

    // 也解析简单的 BUY: $TOKEN 格式
    const simpleBuyMatches = content.matchAll(/(?:^|\n)\s*(?:\*\*)?BUY(?:\*\*)?:\s*\$?([\w\.\-]+)/gim);

    for (const match of buyMatches) {
        results.push({ symbol: match[1].toUpperCase(), action: 'BUY', timestamp });
    }
    for (const match of simpleBuyMatches) {
        const symbol = match[1].toUpperCase();
        if (!results.find(r => r.symbol === symbol && r.action === 'BUY')) {
            results.push({ symbol, action: 'BUY', timestamp });
        }
    }
    for (const match of watchMatches) {
        results.push({ symbol: match[1].toUpperCase(), action: 'WATCH', timestamp });
    }
    for (const match of discardMatches) {
        results.push({ symbol: match[1].toUpperCase(), action: 'DISCARD', timestamp });
    }

    // 提取叙事评分
    const narrativeScoreMatch = content.match(/叙事(?:评分)?[：:]\s*\*?\*?(\d+(?:\.\d+)?)\s*[\/分]/i);
    const narrativeScore = narrativeScoreMatch ? parseFloat(narrativeScoreMatch[1]) : null;

    // 提取 Tier 等级
    const tierMatch = content.match(/(?:叙事等级|TIER)[：:\s]*\*?\*?(TIER_?[SABCDF]|S|A|B|C|D|F)\*?\*?/i);
    const tier = tierMatch ? tierMatch[1].toUpperCase().replace('_', '') : null;

    // 提取目标市值
    const targetMcapMatch = content.match(/目标市值[：:]\s*\$?([\d,\.]+)([KMkm])?/i);
    let targetMcap = null;
    if (targetMcapMatch) {
        targetMcap = parseFloat(targetMcapMatch[1].replace(/,/g, ''));
        const suffix = targetMcapMatch[2]?.toUpperCase();
        if (suffix === 'K') targetMcap *= 1000;
        if (suffix === 'M') targetMcap *= 1000000;
    }

    // 给每个结果添加元数据
    return results.map(r => ({
        ...r,
        narrativeScore,
        tier,
        targetMcap
    }));
}

// 读取所有AI日志
const logFiles = fs.readdirSync(aiLogsDir).filter(f => f.endsWith('.log'));
console.log(`\n扫描AI日志: ${logFiles.length} 个文件`);

const allDecisions = [];

for (const logFile of logFiles) {
    try {
        const content = fs.readFileSync(path.join(aiLogsDir, logFile), 'utf-8');
        const decisions = parseAILog(content);
        decisions.forEach(d => {
            d.logFile = logFile;
        });
        allDecisions.push(...decisions);
    } catch (e) {
        // 忽略读取错误
    }
}

console.log(`\n解析到 ${allDecisions.length} 个AI决策`);

// 统计
const buyDecisions = allDecisions.filter(d => d.action === 'BUY');
const watchDecisions = allDecisions.filter(d => d.action === 'WATCH');
const discardDecisions = allDecisions.filter(d => d.action === 'DISCARD');

console.log(`\n【决策分布】`);
console.log(`  BUY: ${buyDecisions.length}`);
console.log(`  WATCH: ${watchDecisions.length}`);
console.log(`  DISCARD: ${discardDecisions.length}`);

// 与金狗对照
console.log(`\n${'═'.repeat(70)}`);
console.log(`📊 AI决策 vs 实际金狗表现`);
console.log(`${'═'.repeat(70)}`);

// 创建金狗符号集合（标准化）
const goldDogSymbols = new Set(goldDogList.map(g => g.symbol.toUpperCase().replace(/^\$/, '')));

// 检查BUY建议中有多少是金狗
const buyHits = buyDecisions.filter(d => {
    const symbol = d.symbol.replace(/^\$/, '').toUpperCase();
    return goldDogSymbols.has(symbol);
});

// 检查DISCARD建议中有多少是金狗（错误丢弃）
const discardMisses = discardDecisions.filter(d => {
    const symbol = d.symbol.replace(/^\$/, '').toUpperCase();
    return goldDogSymbols.has(symbol);
});

// 检查金狗有多少被AI发现
const goldDogsFound = goldDogList.filter(g => {
    const symbol = g.symbol.toUpperCase().replace(/^\$/, '');
    return buyDecisions.some(d => d.symbol.replace(/^\$/, '').toUpperCase() === symbol);
});

console.log(`\n【AI BUY建议命中率】`);
console.log(`  BUY建议总数: ${buyDecisions.length}`);
console.log(`  命中金狗: ${buyHits.length}`);
console.log(`  命中率: ${buyDecisions.length > 0 ? (buyHits.length / buyDecisions.length * 100).toFixed(1) : 0}%`);

console.log(`\n【金狗覆盖率】`);
console.log(`  金狗总数: ${goldDogList.length}`);
console.log(`  被AI发现: ${goldDogsFound.length}`);
console.log(`  覆盖率: ${goldDogList.length > 0 ? (goldDogsFound.length / goldDogList.length * 100).toFixed(1) : 0}%`);

console.log(`\n【错误丢弃】`);
console.log(`  DISCARD建议总数: ${discardDecisions.length}`);
console.log(`  错误丢弃金狗: ${discardMisses.length}`);
if (discardMisses.length > 0) {
    console.log(`  错误丢弃的金狗:`);
    discardMisses.forEach(d => {
        const goldDog = goldDogList.find(g => g.symbol.toUpperCase() === d.symbol.toUpperCase());
        console.log(`    - ${d.symbol} (涨幅: ${goldDog?.gain || '?'}x)`);
    });
}

// 详细分析命中的金狗
console.log(`\n${'═'.repeat(70)}`);
console.log(`🏆 AI成功命中的金狗`);
console.log(`${'═'.repeat(70)}`);

if (buyHits.length > 0) {
    buyHits.forEach(d => {
        const goldDog = goldDogList.find(g => g.symbol.toUpperCase().replace(/^\$/, '') === d.symbol.replace(/^\$/, '').toUpperCase());
        const gain = goldDog?.gain || '?';
        console.log(`  ${d.symbol.padEnd(20)} | 涨幅: ${gain}x | Tier: ${d.tier || '?'} | 日志: ${d.logFile}`);
    });
} else {
    console.log(`  (无)`);
}

// 分析漏掉的金狗
console.log(`\n${'═'.repeat(70)}`);
console.log(`⚠️ AI漏掉的金狗 (未给出BUY建议)`);
console.log(`${'═'.repeat(70)}`);

const missedGoldDogs = goldDogList.filter(g => {
    const symbol = g.symbol.toUpperCase().replace(/^\$/, '');
    return !buyDecisions.some(d => d.symbol.replace(/^\$/, '').toUpperCase() === symbol);
});

if (missedGoldDogs.length > 0) {
    // 只显示前20个
    missedGoldDogs.slice(0, 20).forEach(g => {
        console.log(`  ${g.symbol.padEnd(20)} | 涨幅: ${g.gain}x`);
    });
    if (missedGoldDogs.length > 20) {
        console.log(`  ... 还有 ${missedGoldDogs.length - 20} 个`);
    }
} else {
    console.log(`  (无)`);
}

// 分析Tier等级与实际表现的关系
console.log(`\n${'═'.repeat(70)}`);
console.log(`📈 Tier等级与金狗命中关系`);
console.log(`${'═'.repeat(70)}`);

const tierStats = {};
buyDecisions.forEach(d => {
    const tier = d.tier || 'UNKNOWN';
    if (!tierStats[tier]) {
        tierStats[tier] = { total: 0, hits: 0 };
    }
    tierStats[tier].total++;

    const isGold = goldDogSymbols.has(d.symbol.replace(/^\$/, '').toUpperCase());
    if (isGold) {
        tierStats[tier].hits++;
    }
});

console.log(`\n| Tier | BUY建议 | 命中金狗 | 命中率 |`);
console.log(`|------|---------|----------|--------|`);
Object.entries(tierStats).sort((a, b) => a[0].localeCompare(b[0])).forEach(([tier, stats]) => {
    const hitRate = stats.total > 0 ? (stats.hits / stats.total * 100).toFixed(1) : 0;
    console.log(`| ${tier.padEnd(4)} | ${String(stats.total).padStart(7)} | ${String(stats.hits).padStart(8)} | ${String(hitRate).padStart(5)}% |`);
});

// AI问题分析
console.log(`\n${'═'.repeat(70)}`);
console.log(`🔍 AI能力分析总结`);
console.log(`${'═'.repeat(70)}`);

const hitRate = buyDecisions.length > 0 ? (buyHits.length / buyDecisions.length * 100) : 0;
const coverageRate = goldDogList.length > 0 ? (goldDogsFound.length / goldDogList.length * 100) : 0;

console.log(`\n【AI优势】`);
if (hitRate > 10) {
    console.log(`  ✅ BUY命中率 ${hitRate.toFixed(1)}% 高于随机(~6%)`);
}
if (coverageRate > 30) {
    console.log(`  ✅ 金狗覆盖率 ${coverageRate.toFixed(1)}% 能发现部分金狗`);
}

console.log(`\n【AI问题】`);
if (hitRate < 10) {
    console.log(`  ⚠️ BUY命中率仅 ${hitRate.toFixed(1)}%，精确度需提升`);
}
if (coverageRate < 50) {
    console.log(`  ⚠️ 金狗覆盖率仅 ${coverageRate.toFixed(1)}%，漏掉太多机会`);
}
if (discardMisses.length > 0) {
    console.log(`  ⚠️ 错误丢弃了 ${discardMisses.length} 个金狗`);
}

console.log(`\n【优化建议】`);
console.log(`  1. 提高对 STABLE 信号代币的关注（目前可能过度筛选）`);
console.log(`  2. 调整 Tier 判断标准，减少对高涨幅代币的错误判断`);
console.log(`  3. 结合链上数据（SM数量、趋势类型）作为硬约束`);
console.log(`  4. AI用于辅助判断，不应作为唯一决策依据`);

// 保存分析结果
const analysisResult = {
    timestamp: new Date().toISOString(),
    summary: {
        totalAILogs: logFiles.length,
        totalDecisions: allDecisions.length,
        buyDecisions: buyDecisions.length,
        watchDecisions: watchDecisions.length,
        discardDecisions: discardDecisions.length,
        goldDogCount: goldDogList.length
    },
    performance: {
        buyHitRate: hitRate,
        goldDogCoverage: coverageRate,
        wrongDiscards: discardMisses.length
    },
    tierAnalysis: tierStats,
    hitDetails: buyHits.map(d => ({
        symbol: d.symbol,
        tier: d.tier,
        gain: goldDogList.find(g => g.symbol.toUpperCase().replace(/^\$/, '') === d.symbol.replace(/^\$/, '').toUpperCase())?.gain
    })),
    missedGoldDogs: missedGoldDogs.map(g => ({
        symbol: g.symbol,
        gain: g.gain
    }))
};

const outputPath = path.join(__dirname, '..', 'data', 'ai-backtest-analysis.json');
fs.writeFileSync(outputPath, JSON.stringify(analysisResult, null, 2));
console.log(`\n✅ 分析结果已保存到 data/ai-backtest-analysis.json`);
