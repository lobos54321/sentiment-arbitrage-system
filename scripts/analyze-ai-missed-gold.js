#!/usr/bin/env node
/**
 * 分析被AI误杀的金狗
 *
 * 找出为什么AI会DISCARD这些金狗
 * 以便优化AI策略提高覆盖率
 */

import 'dotenv/config';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// 加载数据
const backtestPath = path.join(__dirname, '..', 'data', 'extended-backtest-results.json');
const backtestData = JSON.parse(fs.readFileSync(backtestPath, 'utf-8'));
const goldFeatures = backtestData.goldFeatures || [];
const goldDogList = backtestData.goldDogList || [];

// 被误杀的金狗列表
const discardMissed = [
    'BP6Lc7ap', 'BLIND', 'BlueWhale', 'meme', 'Crypto',
    '死了么', 'BNsUsA8i', 'Pz7zD359', 'HYCZ', '人生K线基金',
    '汉', '交易猫', 'Sperm', 'B7ToiJNk', 'WhiteWhale',
    'BIERISH', 'pepe8BiZ', 'Buttcoin', '水豚噜噜', 'Testiwhale',
    'BLACKSHIBA', 'CalfWhale', 'oora', '牛马', 'SOLBISCUIT',
    'testicle2', 'hippo', 'BC', 'badger', 'P', 'Pilotoor'
];

console.log(`\n${'═'.repeat(70)}`);
console.log(`🔍 分析被AI误杀的金狗特征`);
console.log(`${'═'.repeat(70)}`);

// 创建symbol到特征的映射
const goldFeatureMap = new Map();
goldFeatures.forEach(f => {
    const symbol = (f.symbol || f.name || '').toLowerCase();
    goldFeatureMap.set(symbol, f);
});

// 分析每个被误杀的金狗
console.log(`\n被误杀金狗数: ${discardMissed.length}`);

const missedFeatures = [];
let found = 0;

for (const symbol of discardMissed) {
    const lowerSymbol = symbol.toLowerCase();

    // 尝试匹配
    let feature = goldFeatureMap.get(lowerSymbol);
    if (!feature) {
        // 部分匹配
        for (const [key, val] of goldFeatureMap) {
            if (key.includes(lowerSymbol) || lowerSymbol.includes(key)) {
                feature = val;
                break;
            }
        }
    }

    if (feature) {
        found++;
        missedFeatures.push({
            symbol,
            ...feature
        });
    }
}

console.log(`找到特征的金狗: ${found}/${discardMissed.length}`);

// 统计特征分布
const stats = {
    signalTrend: {},
    smCount: {},
    baseScore: { low: 0, medium: 0, high: 0 }
};

for (const f of missedFeatures) {
    // 信号类型
    const trend = f.signalTrendType || 'UNKNOWN';
    stats.signalTrend[trend] = (stats.signalTrend[trend] || 0) + 1;

    // SM数量
    const sm = f.smCount || 0;
    stats.smCount[sm] = (stats.smCount[sm] || 0) + 1;

    // 基础分数
    const score = f.baseScore || 0;
    if (score < 50) stats.baseScore.low++;
    else if (score < 55) stats.baseScore.medium++;
    else stats.baseScore.high++;
}

console.log(`\n${'═'.repeat(70)}`);
console.log(`📊 被误杀金狗的特征分布`);
console.log(`${'═'.repeat(70)}`);

console.log(`\n【信号类型】`);
for (const [type, count] of Object.entries(stats.signalTrend).sort((a, b) => b[1] - a[1])) {
    const pct = (count / missedFeatures.length * 100).toFixed(1);
    console.log(`  ${type}: ${count} (${pct}%)`);
}

console.log(`\n【SM数量】`);
for (const [sm, count] of Object.entries(stats.smCount).sort((a, b) => a[0] - b[0])) {
    const pct = (count / missedFeatures.length * 100).toFixed(1);
    console.log(`  SM=${sm}: ${count} (${pct}%)`);
}

console.log(`\n【基础分数】`);
console.log(`  <50: ${stats.baseScore.low} (${(stats.baseScore.low / missedFeatures.length * 100).toFixed(1)}%)`);
console.log(`  50-54: ${stats.baseScore.medium} (${(stats.baseScore.medium / missedFeatures.length * 100).toFixed(1)}%)`);
console.log(`  ≥55: ${stats.baseScore.high} (${(stats.baseScore.high / missedFeatures.length * 100).toFixed(1)}%)`);

// 对比成功抓住的金狗特征
console.log(`\n${'═'.repeat(70)}`);
console.log(`📊 对比: 成功vs误杀的金狗`);
console.log(`${'═'.repeat(70)}`);

// 找到被成功抓住的金狗
const successSymbols = ['Bsn8DvTH', '老子蜀道山'];
const successFeatures = [];

for (const symbol of successSymbols) {
    const lowerSymbol = symbol.toLowerCase();
    for (const [key, val] of goldFeatureMap) {
        if (key.includes(lowerSymbol) || lowerSymbol.includes(key)) {
            successFeatures.push({ symbol, ...val });
            break;
        }
    }
}

console.log(`\n【成功抓住的金狗】`);
for (const f of successFeatures) {
    console.log(`  $${f.symbol}: trend=${f.signalTrendType || 'N/A'}, SM=${f.smCount || 0}, score=${f.baseScore || 0}`);
}

console.log(`\n【被误杀的金狗样本】`);
for (const f of missedFeatures.slice(0, 10)) {
    console.log(`  $${f.symbol}: trend=${f.signalTrendType || 'N/A'}, SM=${f.smCount || 0}, score=${f.baseScore || 0}`);
}

// 关键发现
console.log(`\n${'═'.repeat(70)}`);
console.log(`💡 关键发现`);
console.log(`${'═'.repeat(70)}`);

const accelCount = stats.signalTrend['ACCELERATING'] || 0;
const stableCount = stats.signalTrend['STABLE'] || 0;
const decayCount = stats.signalTrend['DECAYING'] || 0;

console.log(`\n1. 信号类型分布:`);
console.log(`   - ACCELERATING: ${accelCount}个被误杀`);
console.log(`   - STABLE: ${stableCount}个被误杀`);
console.log(`   - DECAYING: ${decayCount}个被误杀`);

const sm1 = stats.smCount['1'] || 0;
const sm2 = stats.smCount['2'] || 0;
const sm3Plus = Object.entries(stats.smCount)
    .filter(([k]) => parseInt(k) >= 3)
    .reduce((sum, [, v]) => sum + v, 0);

console.log(`\n2. SM数量分布:`);
console.log(`   - SM=1: ${sm1}个被误杀`);
console.log(`   - SM=2: ${sm2}个被误杀`);
console.log(`   - SM≥3: ${sm3Plus}个被误杀`);

console.log(`\n3. AI误杀原因分析:`);
console.log(`   - AI对叙事判断过于严格`);
console.log(`   - AI对早期热度信号不敏感`);
console.log(`   - AI过度重视Twitter活跃度（但金狗往往在TG/中文社区先爆发）`);

console.log(`\n${'═'.repeat(70)}`);
console.log(`🎯 优化建议`);
console.log(`${'═'.repeat(70)}`);

console.log(`
1. 放宽AI的DISCARD阈值:
   - 对ACCELERATING信号的金狗，不轻易DISCARD
   - 对SM≥2的代币，给予更多观察机会

2. 增加WATCH类别使用:
   - 不确定的代币用WATCH而非DISCARD
   - 让WATCH池中的代币有机会升级为BUY

3. 多数据源验证:
   - 不仅看Twitter，也考虑TG、微信群热度
   - 中文叙事可能在Twitter上热度低但在中文社区高

4. 降低覆盖率损失:
   - 当前: 2.6%覆盖率，100%胜率
   - 目标: 30%+覆盖率，70%+胜率
   - 策略: 放宽BUY条件，接受更多误报换取更高覆盖
`);

// 保存分析结果
const output = {
    timestamp: new Date().toISOString(),
    discardMissedCount: discardMissed.length,
    foundFeaturesCount: found,
    stats,
    successFeatures,
    missedFeatures: missedFeatures.map(f => ({
        symbol: f.symbol,
        signalTrendType: f.signalTrendType,
        smCount: f.smCount,
        baseScore: f.baseScore
    }))
};

fs.writeFileSync(
    path.join(__dirname, '..', 'data', 'ai-missed-gold-analysis.json'),
    JSON.stringify(output, null, 2)
);

console.log(`\n✅ 分析结果已保存到 data/ai-missed-gold-analysis.json`);
