/**
 * 测试 AI 叙事判断能力 - 使用真实后台数据
 * 
 * 从数据库读取最近的真实代币，调用 DeBot API + LLM 分析
 * 
 * 运行方式：
 *   node scripts/test-real-narrative.js
 */

import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path'
import Database from 'better-sqlite3';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const projectRoot = join(__dirname, '..');

const dbPath = process.env.DB_PATH || './data/sentiment_arb.db';
const db = new Database(join(projectRoot, dbPath));

async function main() {
    console.log('\n' + '═'.repeat(70));
    console.log('🧪 AI 叙事判断测试 - 真实后台数据');
    console.log('═'.repeat(70));

    // 从数据库获取最近的真实代币
    const recentSignals = db.prepare(`
        SELECT DISTINCT 
            ts.token_ca,
            ts.chain,
            ts.channel_name,
            ts.message_text,
            ts.timestamp
        FROM telegram_signals ts
        WHERE ts.chain = 'SOL'
        ORDER BY ts.created_at DESC
        LIMIT 5
    `).all();

    if (recentSignals.length === 0) {
        console.log('❌ 数据库无数据，请先运行系统收集信号');
        process.exit(1);
    }

    console.log(`\n📊 找到 ${recentSignals.length} 个最近的真实代币:\n`);

    // 动态导入模块
    const { AIAnalyst } = await import('../src/utils/ai-analyst.js');
    const debotScoutModule = await import('../src/inputs/debot-playwright-scout.js');
    const debotScout = debotScoutModule.default;

    const analyst = new AIAnalyst();

    if (!analyst.enabled) {
        console.log('❌ AI 分析未启用！请检查配置');
        process.exit(1);
    }

    console.log(`🔧 AI 配置: ${analyst.model}`);
    console.log('─'.repeat(70));

    const results = [];

    for (let i = 0; i < recentSignals.length; i++) {
        const signal = recentSignals[i];
        const tokenCA = signal.token_ca;

        console.log(`\n[${i + 1}/${recentSignals.length}] 测试代币: ${tokenCA.slice(0, 12)}...`);
        console.log(`   来源频道: ${signal.channel_name}`);
        console.log(`   时间: ${signal.timestamp}`);

        // 从消息中提取一些信息
        const message = signal.message_text || '';
        const smartMoneyMatch = message.match(/(\d+)个聪明钱/);
        const smartWallets = smartMoneyMatch ? parseInt(smartMoneyMatch[1]) : 0;

        const mcMatch = message.match(/市值[:：]\s*\$?([\d.]+)K?/i);
        const marketCap = mcMatch ? parseFloat(mcMatch[1]) * (message.includes('K') ? 1000 : 1) : 0;

        const symbolMatch = message.match(/\$(\w+)/);
        const symbol = symbolMatch ? symbolMatch[1] : tokenCA.slice(0, 8);

        console.log(`   解析信息: Symbol=$${symbol}, 聪明钱=${smartWallets}, 市值=$${marketCap.toLocaleString()}`);

        try {
            // 1. 调用 DeBot API 获取 AI Report
            console.log(`   📡 获取 DeBot AI Report...`);
            let debotReport = null;
            try {
                debotReport = await debotScout.fetchAIReport(tokenCA);
                if (debotReport) {
                    debotReport = debotScout.parseAIReport(debotReport);
                    console.log(`   ✅ DeBot 评分: ${debotReport?.rating?.score || 0}/10`);
                    console.log(`   ✅ 叙事类型: ${debotReport?.narrativeType || 'N/A'}`);
                    if (debotReport?.origin) {
                        console.log(`   ✅ 背景: ${debotReport.origin.slice(0, 80)}...`);
                    }
                } else {
                    console.log(`   ⚠️ DeBot 无数据 (404)`);
                }
            } catch (e) {
                console.log(`   ⚠️ DeBot API 错误: ${e.message}`);
            }

            // 2. 准备分析数据
            const analysisData = {
                symbol: symbol,
                tokenAddress: tokenCA,
                smartWalletOnline: smartWallets,
                liquidity: marketCap * 0.3, // 估算流动性
                signalCount: 1,
                tokenLevel: 'UNKNOWN',
                maxPriceGain: 1.0,
                debotScore: debotReport?.rating?.score || 0,
                narrativeType: debotReport?.narrativeType || 'UNKNOWN',
                narrative: debotReport?.origin || '',
                negativeIncidents: debotReport?.distribution?.negativeIncidents || '',
                tgChannelCount: 1,
                hasTier1: false
            };

            // 3. 调用 LLM 分析
            console.log(`   🧠 调用 LLM 分析...`);
            const startTime = Date.now();
            const llmResult = await analyst.evaluate(analysisData);
            const elapsed = Date.now() - startTime;

            if (llmResult) {
                console.log(`   ✅ LLM 评分: ${llmResult.score}/100 (${elapsed}ms)`);
                console.log(`   ✅ 判断: ${llmResult.reason}`);
                console.log(`   ✅ 风险: ${llmResult.risk_level}`);

                // 计算叙事总分
                const debotBase = Math.min((debotReport?.rating?.score || 0) * 2, 10);
                const llmBonus = Math.min(llmResult.score * 0.15, 15);
                const narrativeScore = Math.min(debotBase + llmBonus, 25);

                console.log(`\n   📈 叙事评分: DeBot(${debotBase}/10) + LLM(${llmBonus.toFixed(1)}/15) = ${narrativeScore.toFixed(1)}/25`);

                results.push({
                    token: tokenCA.slice(0, 12),
                    symbol,
                    debotScore: debotReport?.rating?.score || 0,
                    llmScore: llmResult.score,
                    llmReason: llmResult.reason,
                    riskLevel: llmResult.risk_level,
                    narrativeScore: narrativeScore.toFixed(1),
                    elapsed
                });
            } else {
                console.log(`   ❌ LLM 返回空`);
            }

        } catch (error) {
            console.log(`   ❌ 错误: ${error.message}`);
        }

        console.log('─'.repeat(70));
    }

    // 汇总结果
    console.log('\n' + '═'.repeat(70));
    console.log('📊 测试结果汇总');
    console.log('═'.repeat(70));

    if (results.length > 0) {
        console.log('\n| Token | Symbol | DeBot | LLM | 叙事总分 | 风险 | 判断 |');
        console.log('|-------|--------|-------|-----|----------|------|------|');

        for (const r of results) {
            console.log(`| ${r.token}... | $${r.symbol} | ${r.debotScore}/10 | ${r.llmScore}/100 | ${r.narrativeScore}/25 | ${r.riskLevel} | ${r.llmReason.slice(0, 30)}... |`);
        }

        const avgLlm = results.reduce((sum, r) => sum + r.llmScore, 0) / results.length;
        const avgNarrative = results.reduce((sum, r) => sum + parseFloat(r.narrativeScore), 0) / results.length;
        const avgTime = results.reduce((sum, r) => sum + r.elapsed, 0) / results.length;

        console.log(`\n📈 统计:`);
        console.log(`   平均 LLM 评分: ${avgLlm.toFixed(1)}/100`);
        console.log(`   平均叙事总分: ${avgNarrative.toFixed(1)}/25`);
        console.log(`   平均响应时间: ${(avgTime / 1000).toFixed(1)}秒`);
        console.log(`   测试通过率: ${results.length}/${recentSignals.length}`);
    } else {
        console.log('❌ 无有效测试结果');
    }

    console.log('\n' + '═'.repeat(70));
    console.log('🎯 AI 叙事判断测试完成');
    console.log('═'.repeat(70) + '\n');
}

main().catch(error => {
    console.error('❌ 测试失败:', error);
    process.exit(1);
});
