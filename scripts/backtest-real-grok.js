#!/usr/bin/env node
/**
 * 高胜率系统真实回测 - 使用Grok API
 *
 * 小规模测试：5个金狗 + 5个噪音
 * 验证系统是否能达到70%胜率
 */

import 'dotenv/config';
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
console.log(`🔬 高胜率系统真实回测 (Grok API)`);
console.log(`${'═'.repeat(70)}`);

// 配置
const TEST_SIZE = 5; // 每类测试数量

// BALANCED过滤
function balancedFilter(f) {
    return f.signalTrendType !== 'DECAYING' &&
           f.smCount >= 1 &&
           f.baseScore >= 50;
}

const balancedGold = goldFeatures.filter(balancedFilter);
const balancedNoise = noiseFeatures.filter(balancedFilter);

// 选择有明确symbol的样本
const goldWithSymbol = balancedGold.filter(g => g.symbol && g.symbol.length <= 15);
const noiseWithSymbol = balancedNoise.filter(n => n.symbol && n.symbol.length <= 15);

// 随机抽样
function sample(arr, n) {
    const shuffled = [...arr].sort(() => 0.5 - Math.random());
    return shuffled.slice(0, n);
}

const goldSample = sample(goldWithSymbol, Math.min(TEST_SIZE, goldWithSymbol.length));
const noiseSample = sample(noiseWithSymbol, Math.min(TEST_SIZE, noiseWithSymbol.length));

console.log(`\n测试样本: ${goldSample.length}金狗 + ${noiseSample.length}噪音`);

// 评分函数
function calculateChainScore(token) {
    let score = 0;
    if (token.signalTrendType === 'ACCELERATING') score += 35;
    else if (token.signalTrendType === 'STABLE') score += 10;

    const sm = token.smCount || 0;
    if (sm === 2) score += 25;
    else if (sm === 1) score += 18;
    else if (sm === 3) score += 15;
    else if (sm >= 4) score += 10;

    const baseScore = token.baseScore || 0;
    if (baseScore >= 50 && baseScore < 55) score += 15;
    else if (baseScore >= 55 && baseScore < 60) score += 12;
    else if (baseScore >= 60) score += 10;

    return score;
}

function calculateAIScore(xData) {
    if (!xData) return 0;
    let score = 0;

    const mentions = xData.mention_count || 0;
    if (mentions >= 50) score += 20;
    else if (mentions >= 20) score += 15;
    else if (mentions >= 10) score += 10;
    else if (mentions >= 5) score += 5;

    const realKolCount = xData.kol_involvement?.real_kol_count || 0;
    if (realKolCount >= 3) score += 35;
    else if (realKolCount >= 2) score += 28;
    else if (realKolCount >= 1) score += 18;

    const organicRatio = xData.bot_detection?.organic_tweet_ratio || 0.5;
    if (organicRatio >= 0.8) score += 15;
    else if (organicRatio >= 0.6) score += 10;
    else if (organicRatio >= 0.4) score += 5;

    if (xData.sentiment === 'positive') score += 10;
    else if (xData.sentiment === 'neutral') score += 5;

    const narrativeTotal = xData.narrative_score?.total || 0;
    score += Math.min(narrativeTotal * 0.2, 20);

    if (xData.origin_source?.is_authentic === false) score -= 25;
    if ((xData.kol_involvement?.fake_kol_mentions || 0) > 2) score -= 15;

    return Math.max(0, Math.min(100, score));
}

function predictCategory(token, chainScore, aiScore) {
    let chainWeight = token.signalTrendType === 'ACCELERATING' ? 0.5 : 0.4;
    let aiWeight = 1 - chainWeight;

    const chainNorm = Math.min(chainScore / 75 * 100, 100);
    const aiNorm = Math.min(aiScore, 100);
    const finalScore = chainNorm * chainWeight + aiNorm * aiWeight;

    // 优化后的阈值
    if (finalScore >= 65 && aiScore >= 45 && chainScore >= 35) return 'GOLD';
    if (finalScore >= 50 && (aiScore >= 35 || chainScore >= 45)) return 'SILVER';
    if (finalScore >= 42 && (token.signalTrendType === 'ACCELERATING' || token.smCount === 2)) return 'BRONZE_HIGH';
    if (finalScore >= 35) return 'BRONZE';
    return 'NOISE';
}

// 执行回测
async function runRealBacktest() {
    const grokClient = new GrokTwitterClient();
    const results = [];

    const allSamples = [
        ...goldSample.map(s => ({ ...s, isGold: true })),
        ...noiseSample.map(s => ({ ...s, isGold: false }))
    ];

    console.log(`\n开始Grok API测试...\n`);

    for (let i = 0; i < allSamples.length; i++) {
        const token = allSamples[i];
        const symbol = token.symbol;

        console.log(`[${i + 1}/${allSamples.length}] 查询 $${symbol}...`);

        const chainScore = calculateChainScore(token);
        let aiScore = 0;
        let xData = null;
        let error = null;

        try {
            xData = await grokClient.searchToken(symbol, token.tokenAddress || '', 60);
            aiScore = calculateAIScore(xData);

            console.log(`   ✅ 链上: ${chainScore} | AI: ${aiScore}`);
            console.log(`      X提及: ${xData.mention_count || 0} | KOL: ${xData.kol_involvement?.real_kol_count || 0} | 情绪: ${xData.sentiment || 'N/A'}`);
        } catch (err) {
            error = err.message;
            console.log(`   ❌ 查询失败: ${error}`);
        }

        const prediction = predictCategory(token, chainScore, aiScore);
        const isBuy = ['GOLD', 'SILVER', 'BRONZE_HIGH'].includes(prediction);
        const isCorrect = isBuy === token.isGold;

        results.push({
            symbol,
            isGold: token.isGold,
            chainScore,
            aiScore,
            prediction,
            isBuy,
            isCorrect,
            xData: xData ? {
                mentions: xData.mention_count,
                kols: xData.kol_involvement?.real_kol_count || 0,
                sentiment: xData.sentiment,
                narrativeScore: xData.narrative_score?.total
            } : null,
            error
        });

        // API限流
        if (i < allSamples.length - 1) {
            await new Promise(resolve => setTimeout(resolve, 2500));
        }
    }

    // 分析结果
    console.log(`\n${'═'.repeat(70)}`);
    console.log(`📊 真实回测结果`);
    console.log(`${'═'.repeat(70)}`);

    const buyDecisions = results.filter(r => r.isBuy);
    const truePositives = buyDecisions.filter(r => r.isGold);
    const falsePositives = buyDecisions.filter(r => !r.isGold);

    const skipDecisions = results.filter(r => !r.isBuy);
    const trueNegatives = skipDecisions.filter(r => !r.isGold);
    const falseNegatives = skipDecisions.filter(r => r.isGold);

    const precision = buyDecisions.length > 0 ? (truePositives.length / buyDecisions.length * 100) : 0;
    const recall = goldSample.length > 0 ? (truePositives.length / goldSample.length * 100) : 0;

    console.log(`\n【决策统计】`);
    console.log(`  买入决策: ${buyDecisions.length}`);
    console.log(`    - 正确 (真金狗): ${truePositives.length}`);
    console.log(`    - 错误 (假阳性): ${falsePositives.length}`);
    console.log(`  跳过决策: ${skipDecisions.length}`);
    console.log(`    - 正确 (真噪音): ${trueNegatives.length}`);
    console.log(`    - 漏掉 (假阴性): ${falseNegatives.length}`);

    console.log(`\n【核心指标】`);
    console.log(`  🎯 精确率 (胜率): ${precision.toFixed(1)}%`);
    console.log(`  📈 召回率 (覆盖): ${recall.toFixed(1)}%`);
    console.log(`  🏆 目标: ≥70%`);
    console.log(`  ${precision >= 70 ? '✅ 达标!' : '❌ 未达标'}`);

    // 详细结果
    console.log(`\n【详细结果】`);
    results.forEach(r => {
        const mark = r.isCorrect ? '✅' : '❌';
        const actual = r.isGold ? '金狗' : '噪音';
        const decision = r.isBuy ? '买入' : '跳过';
        console.log(`  ${mark} $${r.symbol.padEnd(12)} | ${r.prediction.padEnd(11)} → ${decision} | 实际: ${actual} | 分数: ${r.chainScore}+${r.aiScore}`);
    });

    // 保存结果
    const output = {
        timestamp: new Date().toISOString(),
        testSize: TEST_SIZE,
        results,
        summary: {
            precision,
            recall,
            buyDecisions: buyDecisions.length,
            truePositives: truePositives.length,
            falsePositives: falsePositives.length
        }
    };

    fs.writeFileSync(
        path.join(__dirname, '..', 'data', 'real-grok-backtest-result.json'),
        JSON.stringify(output, null, 2)
    );

    console.log(`\n✅ 结果已保存到 data/real-grok-backtest-result.json`);
}

runRealBacktest().catch(console.error);
