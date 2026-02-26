#!/usr/bin/env node
/**
 * X数据快照分析工具
 *
 * 分析data/x-snapshots/目录下的快照数据
 * 用于未来回测AI评分公式
 */

import 'dotenv/config';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const snapshotDir = path.join(__dirname, '..', 'data', 'x-snapshots');

console.log(`\n${'═'.repeat(70)}`);
console.log(`📊 X数据快照分析`);
console.log(`${'═'.repeat(70)}`);

// 检查目录是否存在
if (!fs.existsSync(snapshotDir)) {
    console.log(`\n⚠️ 快照目录不存在: ${snapshotDir}`);
    console.log(`   系统运行后会自动创建并开始收集数据`);
    process.exit(0);
}

// 读取所有快照文件
const files = fs.readdirSync(snapshotDir).filter(f => f.endsWith('.json'));

if (files.length === 0) {
    console.log(`\n⚠️ 暂无快照数据`);
    console.log(`   系统需要运行一段时间来收集X/Twitter数据`);
    console.log(`   建议等待2-4周后再运行此分析`);
    process.exit(0);
}

console.log(`\n📁 快照文件数: ${files.length}`);

// 加载并分析所有快照
const snapshots = [];
for (const file of files) {
    try {
        const content = fs.readFileSync(path.join(snapshotDir, file), 'utf-8');
        snapshots.push(JSON.parse(content));
    } catch (e) {
        console.warn(`⚠️ 无法解析 ${file}: ${e.message}`);
    }
}

console.log(`✅ 成功加载: ${snapshots.length} 条快照`);

// 统计分析
const stats = {
    timeRange: {
        earliest: null,
        latest: null
    },
    mentionDistribution: {
        zero: 0,
        low: 0,      // 1-10
        medium: 0,   // 11-50
        high: 0      // 50+
    },
    kolDistribution: {
        zero: 0,
        low: 0,      // 1
        medium: 0,   // 2
        high: 0      // 3+
    },
    sentimentDistribution: {
        positive: 0,
        neutral: 0,
        negative: 0,
        unknown: 0
    },
    avgMetrics: {
        mentions: 0,
        kols: 0,
        organic: 0
    }
};

// 分析每个快照
let totalMentions = 0;
let totalKols = 0;
let totalOrganic = 0;
let organicCount = 0;

for (const s of snapshots) {
    // 时间范围
    if (s.timestamp) {
        if (!stats.timeRange.earliest || s.timestamp < stats.timeRange.earliest) {
            stats.timeRange.earliest = s.timestamp;
        }
        if (!stats.timeRange.latest || s.timestamp > stats.timeRange.latest) {
            stats.timeRange.latest = s.timestamp;
        }
    }

    // 提及分布
    const mentions = s.mention_count || 0;
    totalMentions += mentions;
    if (mentions === 0) stats.mentionDistribution.zero++;
    else if (mentions <= 10) stats.mentionDistribution.low++;
    else if (mentions <= 50) stats.mentionDistribution.medium++;
    else stats.mentionDistribution.high++;

    // KOL分布
    const kols = s.kol_involvement?.real_kol_count || 0;
    totalKols += kols;
    if (kols === 0) stats.kolDistribution.zero++;
    else if (kols === 1) stats.kolDistribution.low++;
    else if (kols === 2) stats.kolDistribution.medium++;
    else stats.kolDistribution.high++;

    // 情绪分布
    const sentiment = s.sentiment || 'unknown';
    stats.sentimentDistribution[sentiment] = (stats.sentimentDistribution[sentiment] || 0) + 1;

    // 有机比例
    const organic = s.bot_detection?.organic_tweet_ratio;
    if (organic !== undefined) {
        totalOrganic += organic;
        organicCount++;
    }
}

// 计算平均值
stats.avgMetrics.mentions = (totalMentions / snapshots.length).toFixed(1);
stats.avgMetrics.kols = (totalKols / snapshots.length).toFixed(2);
stats.avgMetrics.organic = organicCount > 0 ? (totalOrganic / organicCount * 100).toFixed(1) + '%' : 'N/A';

// 输出分析结果
console.log(`\n${'═'.repeat(70)}`);
console.log(`📈 统计分析结果`);
console.log(`${'═'.repeat(70)}`);

console.log(`\n【时间范围】`);
console.log(`   最早: ${stats.timeRange.earliest || 'N/A'}`);
console.log(`   最晚: ${stats.timeRange.latest || 'N/A'}`);

console.log(`\n【提及数分布】`);
console.log(`   0提及: ${stats.mentionDistribution.zero} (${(stats.mentionDistribution.zero / snapshots.length * 100).toFixed(1)}%)`);
console.log(`   1-10: ${stats.mentionDistribution.low} (${(stats.mentionDistribution.low / snapshots.length * 100).toFixed(1)}%)`);
console.log(`   11-50: ${stats.mentionDistribution.medium} (${(stats.mentionDistribution.medium / snapshots.length * 100).toFixed(1)}%)`);
console.log(`   50+: ${stats.mentionDistribution.high} (${(stats.mentionDistribution.high / snapshots.length * 100).toFixed(1)}%)`);

console.log(`\n【KOL参与分布】`);
console.log(`   0 KOL: ${stats.kolDistribution.zero} (${(stats.kolDistribution.zero / snapshots.length * 100).toFixed(1)}%)`);
console.log(`   1 KOL: ${stats.kolDistribution.low} (${(stats.kolDistribution.low / snapshots.length * 100).toFixed(1)}%)`);
console.log(`   2 KOL: ${stats.kolDistribution.medium} (${(stats.kolDistribution.medium / snapshots.length * 100).toFixed(1)}%)`);
console.log(`   3+ KOL: ${stats.kolDistribution.high} (${(stats.kolDistribution.high / snapshots.length * 100).toFixed(1)}%)`);

console.log(`\n【情绪分布】`);
for (const [sentiment, count] of Object.entries(stats.sentimentDistribution)) {
    if (count > 0) {
        console.log(`   ${sentiment}: ${count} (${(count / snapshots.length * 100).toFixed(1)}%)`);
    }
}

console.log(`\n【平均指标】`);
console.log(`   平均提及: ${stats.avgMetrics.mentions}`);
console.log(`   平均KOL: ${stats.avgMetrics.kols}`);
console.log(`   平均有机率: ${stats.avgMetrics.organic}`);

// 找出高质量快照 (有KOL参与或高提及)
const highQuality = snapshots.filter(s =>
    (s.mention_count || 0) >= 10 ||
    (s.kol_involvement?.real_kol_count || 0) >= 1
);

console.log(`\n【高质量快照】`);
console.log(`   符合条件: ${highQuality.length}/${snapshots.length} (${(highQuality.length / snapshots.length * 100).toFixed(1)}%)`);

if (highQuality.length > 0) {
    console.log(`\n   样本:`);
    highQuality.slice(0, 5).forEach(s => {
        console.log(`   - $${s.tokenSymbol}: ${s.mention_count || 0}提及, ${s.kol_involvement?.real_kol_count || 0}KOL, ${s.sentiment || 'N/A'}情绪`);
    });
}

// 保存分析结果
const output = {
    timestamp: new Date().toISOString(),
    totalSnapshots: snapshots.length,
    stats,
    highQualityCount: highQuality.length,
    highQualitySamples: highQuality.slice(0, 10).map(s => ({
        symbol: s.tokenSymbol,
        ca: s.token_ca,
        mentions: s.mention_count,
        kols: s.kol_involvement?.real_kol_count,
        sentiment: s.sentiment,
        timestamp: s.timestamp
    }))
};

fs.writeFileSync(
    path.join(__dirname, '..', 'data', 'x-snapshot-analysis.json'),
    JSON.stringify(output, null, 2)
);

console.log(`\n✅ 分析结果已保存到 data/x-snapshot-analysis.json`);

// 回测建议
console.log(`\n${'═'.repeat(70)}`);
console.log(`💡 回测建议`);
console.log(`${'═'.repeat(70)}`);

if (snapshots.length < 100) {
    console.log(`
   ⚠️ 数据量不足 (${snapshots.length}条)
   建议继续收集至少100-200条数据后再进行回测
   预计需要1-2周运行时间
`);
} else {
    console.log(`
   ✅ 数据量足够进行初步回测
   可以运行 backtest-x-scoring.js 来验证AI评分公式
`);
}
