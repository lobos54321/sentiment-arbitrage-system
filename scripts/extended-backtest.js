#!/usr/bin/env node
/**
 * 扩展回测脚本 v2.0
 * 从多个日志文件收集更多数据，提高统计意义
 *
 * 功能：
 * 1. 扫描所有日志文件，提取金狗/噪音样本
 * 2. 支持多种金狗识别方式（DeBot榜单、涨幅追踪）
 * 3. 生成更全面的特征分析数据
 * 4. 运行参数网格搜索
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const logsDir = path.join(__dirname, '..', 'logs');
const dataDir = path.join(__dirname, '..', 'data');

// 确保 data 目录存在
if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
}

console.log(`\n${'═'.repeat(70)}`);
console.log(`📊 扩展回测数据收集 v2.0`);
console.log(`${'═'.repeat(70)}\n`);

// ═══════════════════════════════════════════════════════════════
// 配置
// ═══════════════════════════════════════════════════════════════

const CONFIG = {
    // 金狗判定标准
    GOLD_MIN_GAIN: 10,      // 10x+ 涨幅为金狗
    SILVER_MIN_GAIN: 3,     // 3x+ 涨幅为银狗

    // 日志文件过滤
    MIN_LOG_SIZE: 100000,   // 至少 100KB 的日志才处理

    // 样本去重
    DEDUPE_BY_SYMBOL: true  // 同一个 symbol 只保留一个样本
};

// ═══════════════════════════════════════════════════════════════
// 数据收集
// ═══════════════════════════════════════════════════════════════

const allGoldDogs = new Map();   // symbol -> { gain, source, features }
const allNoise = new Map();      // symbol -> { features }
const allFilteredTokens = [];    // 所有被过滤的代币特征

/**
 * 从单个日志文件提取数据
 */
function extractFromLog(logPath) {
    const filename = path.basename(logPath);
    console.log(`📄 处理: ${filename}`);

    let content;
    try {
        content = fs.readFileSync(logPath, 'utf-8');
    } catch (e) {
        console.log(`   ⚠️ 读取失败: ${e.message}`);
        return { gold: 0, noise: 0, filtered: 0 };
    }

    const lines = content.split('\n');
    let goldCount = 0, noiseCount = 0, filteredCount = 0;

    // 1. 提取 DeBot 金狗/银狗信号
    const debotSignalRegex = /\[DeBot Scout\] (🥇|🥈) AI SIGNAL.*?: ([A-Za-z0-9]+)/g;
    let match;
    while ((match = debotSignalRegex.exec(content)) !== null) {
        const level = match[1] === '🥇' ? 'gold' : 'silver';
        const address = match[2];

        // 查找后续的涨幅信息
        const gainRegex = new RegExp(`${address}[\\s\\S]{0,500}最大涨幅:\\s*([0-9.]+)x`, 'g');
        const gainMatch = gainRegex.exec(content);
        if (gainMatch) {
            const gain = parseFloat(gainMatch[1]);
            if (gain >= CONFIG.GOLD_MIN_GAIN) {
                // 查找 symbol
                const symbolRegex = new RegExp(`"address":"${address}"[^}]*"symbol":"([^"]+)"`, 'g');
                const symbolMatch = symbolRegex.exec(content);
                const symbol = symbolMatch ? symbolMatch[1] : address.slice(0, 8);

                if (!allGoldDogs.has(symbol)) {
                    allGoldDogs.set(symbol, { gain, level, source: filename, address });
                    goldCount++;
                }
            }
        }
    }

    // 2. 提取涨幅追踪数据
    const gainTrackRegex = /\[涨幅追踪\].*?([A-Za-z0-9\u4e00-\u9fa5]+).*?涨幅.*?([0-9.]+)x/g;
    while ((match = gainTrackRegex.exec(content)) !== null) {
        const symbol = match[1];
        const gain = parseFloat(match[2]);

        if (gain >= CONFIG.GOLD_MIN_GAIN && !allGoldDogs.has(symbol)) {
            allGoldDogs.set(symbol, { gain, level: 'tracked', source: filename });
            goldCount++;
        }
    }

    // 3. 提取评分明细 (用于特征分析)
    for (let i = 0; i < lines.length; i++) {
        const line = lines[i];

        if (!line.includes('评分明细')) continue;

        const scoreMatch = line.match(/\[(\d+)分\]/);
        const smMatch = line.match(/聪明钱:\s*(\d+)\/\d+\s*\((\d+)个/);
        const trendMatch = line.match(/趋势:\s*(\d+)\/\d+\s*\((\w+)\)/);
        const densityMatch = line.match(/密度:\s*(\d+)\/\d+/);
        const signalTrendMatch = line.match(/信号趋势:\s*(\d+)\/\d+\s*\((\w+)\)/);
        const safetyMatch = line.match(/安全:\s*(\d+)\/\d+/);
        const aiMatch = line.match(/AI:\s*(\d+)\/\d+/);

        if (!scoreMatch) continue;

        // 查找 symbol
        let symbol = null;
        for (let j = i; j < Math.min(i + 30, lines.length); j++) {
            const verifyMatch = lines[j].match(/验证完成:\s*([^\n\s]+)/);
            if (verifyMatch) {
                symbol = verifyMatch[1];
                break;
            }
            const symbolMatch = lines[j].match(/验证中:\s*([^\s(]+)/);
            if (symbolMatch) {
                symbol = symbolMatch[1];
                break;
            }
        }

        if (!symbol) continue;

        // 查找市值
        let mcap = null;
        for (let j = i; j < Math.min(i + 15, lines.length); j++) {
            const mcapMatch = lines[j].match(/市值\$([0-9.]+)K/);
            if (mcapMatch) mcap = parseFloat(mcapMatch[1]) * 1000;
        }

        // 检查是否被过滤
        let isFiltered = false;
        let filterReason = null;
        for (let j = i; j < Math.min(i + 20, lines.length); j++) {
            if (lines[j].includes('[Filter]') || lines[j].includes('未通过')) {
                isFiltered = true;
                const reasonMatch = lines[j].match(/:\s*(.+)$/);
                if (reasonMatch) filterReason = reasonMatch[1];
                break;
            }
        }

        const feature = {
            symbol,
            baseScore: parseInt(scoreMatch[1]),
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
            isFiltered,
            filterReason,
            source: filename
        };

        allFilteredTokens.push(feature);
        filteredCount++;
    }

    console.log(`   ✅ 金狗: +${goldCount} | 噪音: +${noiseCount} | 特征: +${filteredCount}`);
    return { gold: goldCount, noise: noiseCount, filtered: filteredCount };
}

/**
 * 扫描所有日志文件
 */
function scanAllLogs() {
    const logFiles = fs.readdirSync(logsDir)
        .filter(f => f.endsWith('.log'))
        .map(f => path.join(logsDir, f))
        .filter(f => {
            const stats = fs.statSync(f);
            return stats.size >= CONFIG.MIN_LOG_SIZE;
        })
        .sort((a, b) => {
            const statsA = fs.statSync(a);
            const statsB = fs.statSync(b);
            return statsB.mtime - statsA.mtime; // 最新的优先
        });

    console.log(`找到 ${logFiles.length} 个有效日志文件 (>100KB)\n`);

    let totalGold = 0, totalNoise = 0, totalFiltered = 0;

    for (const logFile of logFiles) {
        const result = extractFromLog(logFile);
        totalGold += result.gold;
        totalNoise += result.noise;
        totalFiltered += result.filtered;
    }

    console.log(`\n${'─'.repeat(50)}`);
    console.log(`📊 总计: 金狗 ${allGoldDogs.size} | 特征样本 ${allFilteredTokens.length}`);
}

/**
 * 标记金狗特征
 */
function labelFeatures() {
    const goldSymbols = new Set(allGoldDogs.keys());

    const goldFeatures = [];
    const noiseFeatures = [];

    // 去重：同一个 symbol 只保留一个样本
    const seenSymbols = new Set();

    for (const feature of allFilteredTokens) {
        if (CONFIG.DEDUPE_BY_SYMBOL && seenSymbols.has(feature.symbol)) {
            continue;
        }
        seenSymbols.add(feature.symbol);

        if (goldSymbols.has(feature.symbol)) {
            feature.isGold = true;
            goldFeatures.push(feature);
        } else {
            feature.isGold = false;
            noiseFeatures.push(feature);
        }
    }

    return { goldFeatures, noiseFeatures };
}

// ═══════════════════════════════════════════════════════════════
// 参数网格搜索
// ═══════════════════════════════════════════════════════════════

const parameterSpace = {
    sevenDimThreshold: [15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25],
    smCountThreshold: [1, 2, 3, 4, 5],
    requireTrendIncreasing: [true, false],
    requireSignalAccelerating: [true, false],
    allowLateFollower: [true, false],
    baseScoreThreshold: [45, 50, 55, 60, 65],
    maxMcap: [null, 300000, 500000, 1000000, 1500000, 2000000, 3000000]
};

function evaluateStrategy(params, goldFeatures, noiseFeatures) {
    const shouldPass = (f) => {
        if (f.baseScore < params.baseScoreThreshold) return false;
        if (f.smCount !== null && f.smCount < params.smCountThreshold) return false;
        if (params.requireTrendIncreasing && f.trendType !== 'INCREASING') return false;
        if (params.requireSignalAccelerating && f.signalTrendType !== 'ACCELERATING') return false;
        if (!params.allowLateFollower && f.lateFollower) return false;
        if (params.maxMcap !== null && f.mcap !== null && f.mcap > params.maxMcap) return false;
        return true;
    };

    const goldPass = goldFeatures.filter(shouldPass).length;
    const goldPassRate = goldFeatures.length > 0 ? goldPass / goldFeatures.length * 100 : 0;

    const noiseFiltered = noiseFeatures.filter(f => !shouldPass(f)).length;
    const noiseFilterRate = noiseFeatures.length > 0 ? noiseFiltered / noiseFeatures.length * 100 : 0;

    const compositeScore = goldPassRate * 0.6 + noiseFilterRate * 0.4;

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

function runGridSearch(goldFeatures, noiseFeatures) {
    console.log(`\n${'═'.repeat(70)}`);
    console.log(`🔍 参数网格搜索`);
    console.log(`${'═'.repeat(70)}\n`);

    let totalCombinations = 1;
    Object.values(parameterSpace).forEach(values => {
        totalCombinations *= values.length;
    });

    console.log(`总共 ${totalCombinations} 种参数组合...`);
    console.log(`金狗样本: ${goldFeatures.length} | 噪音样本: ${noiseFeatures.length}\n`);

    if (goldFeatures.length < 10) {
        console.log(`⚠️ 警告: 金狗样本太少 (${goldFeatures.length})，结果可能不可靠`);
    }

    const results = [];

    for (const sevenDimThreshold of parameterSpace.sevenDimThreshold) {
        for (const smCountThreshold of parameterSpace.smCountThreshold) {
            for (const requireTrendIncreasing of parameterSpace.requireTrendIncreasing) {
                for (const requireSignalAccelerating of parameterSpace.requireSignalAccelerating) {
                    for (const allowLateFollower of parameterSpace.allowLateFollower) {
                        for (const baseScoreThreshold of parameterSpace.baseScoreThreshold) {
                            for (const maxMcap of parameterSpace.maxMcap) {
                                const result = evaluateStrategy({
                                    sevenDimThreshold,
                                    smCountThreshold,
                                    requireTrendIncreasing,
                                    requireSignalAccelerating,
                                    allowLateFollower,
                                    baseScoreThreshold,
                                    maxMcap
                                }, goldFeatures, noiseFeatures);

                                results.push(result);
                            }
                        }
                    }
                }
            }
        }
    }

    results.sort((a, b) => b.compositeScore - a.compositeScore);

    return results;
}

// ═══════════════════════════════════════════════════════════════
// 主流程
// ═══════════════════════════════════════════════════════════════

console.log('📂 扫描日志文件...\n');
scanAllLogs();

console.log('\n📊 标记金狗特征...');
const { goldFeatures, noiseFeatures } = labelFeatures();
console.log(`   金狗特征: ${goldFeatures.length}`);
console.log(`   噪音特征: ${noiseFeatures.length}`);

// 运行网格搜索
const results = runGridSearch(goldFeatures, noiseFeatures);

// 输出 TOP 20
console.log(`\n${'═'.repeat(70)}`);
console.log(`🏆 综合得分 TOP 20`);
console.log(`${'═'.repeat(70)}\n`);

console.log('排名 | 金狗通过 | 噪音过滤 | 精确率 | 综合分 | 参数');
console.log('─'.repeat(100));

results.slice(0, 20).forEach((r, i) => {
    const params = [];
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

// 保存结果
const outputData = {
    timestamp: new Date().toISOString(),
    sampleSize: {
        goldDogs: goldFeatures.length,
        noise: noiseFeatures.length,
        total: goldFeatures.length + noiseFeatures.length
    },
    goldDogList: Array.from(allGoldDogs.entries()).map(([symbol, data]) => ({
        symbol,
        gain: data.gain,
        level: data.level,
        source: data.source
    })),
    totalCombinations: results.length,
    top20: results.slice(0, 20),
    bestOverall: results[0],
    goldFeatures: goldFeatures,  // 保存全部样本
    noiseFeatures: noiseFeatures  // 保存全部样本
};

fs.writeFileSync(
    path.join(dataDir, 'extended-backtest-results.json'),
    JSON.stringify(outputData, null, 2)
);

console.log(`\n${'═'.repeat(70)}`);
console.log(`📋 统计摘要`);
console.log(`${'═'.repeat(70)}\n`);

console.log(`样本数量:`);
console.log(`  金狗: ${goldFeatures.length} 个`);
console.log(`  噪音: ${noiseFeatures.length} 个`);
console.log(`  总计: ${goldFeatures.length + noiseFeatures.length} 个`);

console.log(`\n最佳策略 (综合得分=${results[0].compositeScore.toFixed(1)}):`);
console.log(`  金狗通过: ${results[0].goldPassRate.toFixed(1)}%`);
console.log(`  噪音过滤: ${results[0].noiseFilterRate.toFixed(1)}%`);
console.log(`  精确率: ${results[0].precision.toFixed(1)}%`);
console.log(`  参数: ${JSON.stringify(results[0].params, null, 2)}`);

console.log(`\n结果已保存到 data/extended-backtest-results.json`);
