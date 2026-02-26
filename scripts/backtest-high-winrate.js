#!/usr/bin/env node
/**
 * 高胜率系统回测 - 使用Grok实时查询
 *
 * 方法:
 * 1. 从历史金狗和噪音中抽样
 * 2. 对每个代币调用Grok查询X/Twitter数据
 * 3. 应用高胜率筛选系统
 * 4. 计算实际命中率
 *
 * 注意: 这会调用真实的Grok API，有成本
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import GrokTwitterClient from '../src/social/grok-twitter-client.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const dataPath = path.join(__dirname, '..', 'data', 'extended-backtest-results.json');

// 加载数据
const data = JSON.parse(fs.readFileSync(dataPath, 'utf-8'));
const goldDogList = data.goldDogList || [];
const goldFeatures = data.goldFeatures || [];
const noiseFeatures = data.noiseFeatures || [];

console.log(`\n${'═'.repeat(70)}`);
console.log(`🔬 高胜率系统回测 (Grok实时查询)`);
console.log(`${'═'.repeat(70)}`);

// 配置
const SAMPLE_SIZE = 10; // 每类抽样数量（控制API成本）
const DRY_RUN = process.argv.includes('--dry-run'); // 干跑模式，不调用API

if (DRY_RUN) {
    console.log(`\n⚠️ 干跑模式 - 不调用Grok API`);
}

// BALANCED过滤
function balancedFilter(f) {
    return f.signalTrendType !== 'DECAYING' &&
           f.smCount >= 1 &&
           f.baseScore >= 50;
}

// 筛选通过BALANCED的样本
const balancedGold = goldFeatures.filter(balancedFilter);
const balancedNoise = noiseFeatures.filter(balancedFilter);

console.log(`\n可用样本: ${balancedGold.length}金狗 + ${balancedNoise.length}噪音`);

// 随机抽样
function sample(arr, n) {
    const shuffled = [...arr].sort(() => 0.5 - Math.random());
    return shuffled.slice(0, n);
}

const goldSample = sample(balancedGold, Math.min(SAMPLE_SIZE, balancedGold.length));
const noiseSample = sample(balancedNoise, Math.min(SAMPLE_SIZE, balancedNoise.length));

console.log(`\n抽样: ${goldSample.length}金狗 + ${noiseSample.length}噪音`);

// 链上评分函数
function calculateChainScore(token) {
    let score = 0;

    if (token.signalTrendType === 'ACCELERATING') {
        score += 30;
    } else if (token.signalTrendType === 'STABLE') {
        score += 10;
    }

    if (token.smCount === 2) {
        score += 25;
    } else if (token.smCount === 1) {
        score += 15;
    } else if (token.smCount >= 3 && token.smCount <= 4) {
        score += 10;
    } else if (token.smCount >= 5) {
        score += 5;
    }

    if (token.baseScore >= 50 && token.baseScore < 55) {
        score += 15;
    } else if (token.baseScore >= 55 && token.baseScore < 60) {
        score += 12;
    } else if (token.baseScore >= 60) {
        score += 8;
    }

    return score;
}

// AI评分函数
function calculateAIScore(xData) {
    if (!xData) return 0;

    let score = 0;

    if (xData.mention_count >= 50) score += 20;
    else if (xData.mention_count >= 20) score += 15;
    else if (xData.mention_count >= 10) score += 10;
    else if (xData.mention_count >= 5) score += 5;

    const realKolCount = xData.kol_involvement?.real_kol_count || 0;
    if (realKolCount >= 3) score += 30;
    else if (realKolCount >= 2) score += 25;
    else if (realKolCount >= 1) score += 15;

    const organicRatio = xData.bot_detection?.organic_tweet_ratio || 0;
    if (organicRatio >= 0.8) score += 15;
    else if (organicRatio >= 0.6) score += 10;
    else if (organicRatio >= 0.4) score += 5;

    if (xData.sentiment === 'positive') score += 10;
    else if (xData.sentiment === 'neutral') score += 5;

    const narrativeScore = xData.narrative_score?.total || 0;
    score += Math.min(narrativeScore * 0.25, 25);

    if (xData.origin_source?.is_authentic === false) {
        score -= 20;
    }
    if ((xData.kol_involvement?.fake_kol_mentions || 0) > 2) {
        score -= 15;
    }

    return Math.max(0, score);
}

// 预测类别
function predictCategory(chainScore, aiScore) {
    const finalScore = chainScore * 0.4 + aiScore * 0.6;

    if (finalScore >= 70 && aiScore >= 50 && chainScore >= 40) {
        return 'GOLD';
    }
    if (finalScore >= 55 && (aiScore >= 40 || chainScore >= 50)) {
        return 'SILVER';
    }
    if (finalScore >= 40) {
        return 'BRONZE';
    }
    return 'NOISE';
}

// 执行回测
async function runBacktest() {
    const grokClient = new GrokTwitterClient();

    const results = [];

    // 处理所有样本
    const allSamples = [
        ...goldSample.map(s => ({ ...s, isGold: true })),
        ...noiseSample.map(s => ({ ...s, isGold: false }))
    ];

    console.log(`\n开始分析 ${allSamples.length} 个代币...\n`);

    for (let i = 0; i < allSamples.length; i++) {
        const token = allSamples[i];
        const symbol = token.symbol || token.name || 'UNKNOWN';

        console.log(`[${i + 1}/${allSamples.length}] 分析 ${symbol}...`);

        const chainScore = calculateChainScore(token);
        let aiScore = 0;
        let xData = null;

        if (!DRY_RUN) {
            try {
                xData = await grokClient.searchToken(symbol, token.tokenAddress || '', 60);
                aiScore = calculateAIScore(xData);
                console.log(`   链上: ${chainScore} | AI: ${aiScore} | X提及: ${xData.mention_count || 0}`);
            } catch (error) {
                console.log(`   ⚠️ Grok查询失败: ${error.message}`);
            }

            // 避免API限流
            await new Promise(resolve => setTimeout(resolve, 2000));
        } else {
            // 干跑模式：随机模拟AI评分
            aiScore = token.isGold ? Math.random() * 50 + 30 : Math.random() * 40 + 10;
            console.log(`   [模拟] 链上: ${chainScore} | AI: ${aiScore.toFixed(0)}`);
        }

        const prediction = predictCategory(chainScore, aiScore);
        const isCorrect = (prediction === 'GOLD' || prediction === 'SILVER') === token.isGold;

        results.push({
            symbol,
            isGold: token.isGold,
            chainScore,
            aiScore: Math.round(aiScore),
            finalScore: Math.round(chainScore * 0.4 + aiScore * 0.6),
            prediction,
            isCorrect,
            xData: xData ? {
                mentions: xData.mention_count,
                kols: xData.kol_involvement?.real_kol_count || 0,
                sentiment: xData.sentiment
            } : null
        });
    }

    // 分析结果
    console.log(`\n${'═'.repeat(70)}`);
    console.log(`📊 回测结果分析`);
    console.log(`${'═'.repeat(70)}`);

    // 预测为GOLD/SILVER的
    const positives = results.filter(r => r.prediction === 'GOLD' || r.prediction === 'SILVER');
    const truePositives = positives.filter(r => r.isGold);
    const falsePositives = positives.filter(r => !r.isGold);

    // 预测为BRONZE/NOISE的
    const negatives = results.filter(r => r.prediction === 'BRONZE' || r.prediction === 'NOISE');
    const trueNegatives = negatives.filter(r => !r.isGold);
    const falseNegatives = negatives.filter(r => r.isGold);

    console.log(`\n【预测结果】`);
    console.log(`  预测为 GOLD/SILVER: ${positives.length}`);
    console.log(`    - 正确 (真金狗): ${truePositives.length}`);
    console.log(`    - 错误 (假阳性): ${falsePositives.length}`);
    console.log(`  预测为 BRONZE/NOISE: ${negatives.length}`);
    console.log(`    - 正确 (真噪音): ${trueNegatives.length}`);
    console.log(`    - 错误 (漏金狗): ${falseNegatives.length}`);

    const precision = positives.length > 0 ? (truePositives.length / positives.length * 100) : 0;
    const recall = goldSample.length > 0 ? (truePositives.length / goldSample.length * 100) : 0;

    console.log(`\n【关键指标】`);
    console.log(`  精确率 (胜率): ${precision.toFixed(1)}%`);
    console.log(`  召回率 (金狗覆盖): ${recall.toFixed(1)}%`);
    console.log(`  目标胜率: 70%`);
    console.log(`  达标: ${precision >= 70 ? '✅ 是' : '❌ 否'}`);

    // 详细列表
    console.log(`\n${'═'.repeat(70)}`);
    console.log(`📋 详细预测结果`);
    console.log(`${'═'.repeat(70)}`);

    console.log(`\n【预测为 GOLD/SILVER】`);
    positives.forEach(r => {
        const mark = r.isGold ? '✅' : '❌';
        console.log(`  ${mark} ${r.symbol.padEnd(15)} | 预测: ${r.prediction.padEnd(6)} | 实际: ${r.isGold ? '金狗' : '噪音'} | 评分: ${r.finalScore}`);
    });

    console.log(`\n【预测为 BRONZE/NOISE 但实际是金狗】`);
    falseNegatives.forEach(r => {
        console.log(`  ⚠️ ${r.symbol.padEnd(15)} | 预测: ${r.prediction.padEnd(6)} | 评分: ${r.finalScore}`);
    });

    // 保存结果
    const outputPath = path.join(__dirname, '..', 'data', 'high-winrate-backtest-result.json');
    fs.writeFileSync(outputPath, JSON.stringify({
        timestamp: new Date().toISOString(),
        config: { sampleSize: SAMPLE_SIZE, dryRun: DRY_RUN },
        summary: {
            totalSamples: results.length,
            goldSamples: goldSample.length,
            noiseSamples: noiseSample.length,
            positives: positives.length,
            truePositives: truePositives.length,
            falsePositives: falsePositives.length,
            precision,
            recall
        },
        results
    }, null, 2));

    console.log(`\n✅ 结果已保存到 data/high-winrate-backtest-result.json`);

    // 优化建议
    console.log(`\n${'═'.repeat(70)}`);
    console.log(`💡 优化建议`);
    console.log(`${'═'.repeat(70)}`);

    if (precision < 70) {
        console.log(`\n当前精确率 ${precision.toFixed(1)}% 未达70%目标，建议:`);
        console.log(`  1. 提高 GOLD 预测阈值 (当前 finalScore≥70)`);
        console.log(`  2. 增加 AI 权重 (当前 60%)`);
        console.log(`  3. 更严格的 KOL 参与要求`);
        console.log(`  4. 添加更多负面信号检测`);
    }

    if (recall < 50) {
        console.log(`\n召回率 ${recall.toFixed(1)}% 较低，可能漏掉金狗，建议:`);
        console.log(`  1. 适当放宽 SILVER 预测条件`);
        console.log(`  2. 对链上强信号给予更高权重`);
    }
}

// 运行
runBacktest().catch(console.error);
