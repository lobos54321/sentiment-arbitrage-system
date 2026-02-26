/**
 * 测试 AI 叙事判断能力
 * 
 * 验证 DeBot + LLM 叙事评分系统
 * 
 * 运行方式：
 *   node scripts/test-narrative-ai.js [token_ca]
 * 
 * 示例：
 *   node scripts/test-narrative-ai.js 7GCihgDB8fe6LNa32gXF5Ae7HBKomvM5bpNpTtVYpump
 */

import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const projectRoot = join(__dirname, '..');

// 动态导入模块
async function main() {
    console.log('\n' + '═'.repeat(70));
    console.log('🧪 AI 叙事判断测试');
    console.log('═'.repeat(70));
    
    // 获取测试 token 地址
    const tokenCA = process.argv[2] || null;
    
    if (!tokenCA) {
        console.log('\n📋 使用示例数据测试 AI 评分能力...\n');
        await testWithMockData();
    } else {
        console.log(`\n📋 测试真实 Token: ${tokenCA}\n`);
        await testWithRealToken(tokenCA);
    }
}

/**
 * 使用模拟数据测试
 */
async function testWithMockData() {
    const { AIAnalyst } = await import('../src/utils/ai-analyst.js');
    const { generateNarrativePrompt } = await import('../src/prompts/narrative-analyst.js');
    
    // 模拟场景：不同类型的代币叙事
    const testCases = [
        {
            name: '高质量AI Agent叙事',
            data: {
                symbol: 'AIBOT',
                tokenAddress: 'AI1234567890123456789012345678901234567890pump',
                smartWalletOnline: 8,
                liquidity: 150000,
                signalCount: 8,
                tokenLevel: 'HOT',
                maxPriceGain: 5.2,
                debotScore: 8,
                narrativeType: 'AI_AGENT',
                narrative: 'AI Agent自动交易机器人，支持多链操作，集成GPT-4，首发创新概念',
                negativeIncidents: '',
                tgChannelCount: 5,
                hasTier1: true,
                xData: {
                    mention_count: 150,
                    unique_authors: 45,
                    sentiment: 'positive',
                    engagement: { total_likes: 2500, total_retweets: 800 },
                    origin_source: { type: 'KOL', is_authentic: true },
                    kol_involvement: { real_kol_count: 3, fake_kol_mentions: 0 },
                    bot_detection: { suspected_bot_tweets: 5, organic_tweet_ratio: 0.95 },
                    narrative_score: { total: 85, grade: 'A', recommendation: '强烈推荐' }
                }
            },
            expected: { min_score: 70, max_risk: 'LOW' }
        },
        {
            name: '垃圾蹭热点叙事',
            data: {
                symbol: 'ELONMUSK9999',
                tokenAddress: 'ELON123456789012345678901234567890123456pump',
                smartWalletOnline: 1,
                liquidity: 15000,
                signalCount: 55,
                tokenLevel: 'WATCH',
                maxPriceGain: 1.5,
                debotScore: 3,
                narrativeType: 'UNKNOWN',
                narrative: '模仿马斯克，抄袭老项目概念',
                negativeIncidents: '疑似scam项目，团队匿名',
                tgChannelCount: 1,
                hasTier1: false,
                xData: {
                    mention_count: 10,
                    unique_authors: 3,
                    sentiment: 'mixed',
                    engagement: { total_likes: 50, total_retweets: 10 },
                    origin_source: { type: 'unknown', is_authentic: false },
                    kol_involvement: { real_kol_count: 0, fake_kol_mentions: 5 },
                    bot_detection: { suspected_bot_tweets: 8, organic_tweet_ratio: 0.2 },
                    narrative_score: { total: 25, grade: 'D', recommendation: '远离' }
                }
            },
            expected: { max_score: 40, risk: 'HIGH' }
        },
        {
            name: '潜力股DeSci叙事',
            data: {
                symbol: 'BIOFI',
                tokenAddress: 'BIO123456789012345678901234567890123456pump',
                smartWalletOnline: 4,
                liquidity: 80000,
                signalCount: 5,
                tokenLevel: 'RISING',
                maxPriceGain: 3.0,
                debotScore: 7,
                narrativeType: 'DESCI',
                narrative: '去中心化科学研究资金平台，与顶尖大学合作',
                negativeIncidents: '',
                tgChannelCount: 3,
                hasTier1: true,
                xData: {
                    mention_count: 80,
                    unique_authors: 25,
                    sentiment: 'positive',
                    engagement: { total_likes: 1200, total_retweets: 400 },
                    origin_source: { type: 'project', is_authentic: true },
                    kol_involvement: { real_kol_count: 2, fake_kol_mentions: 0 },
                    bot_detection: { suspected_bot_tweets: 2, organic_tweet_ratio: 0.9 }
                }
            },
            expected: { min_score: 60, max_risk: 'MEDIUM' }
        }
    ];
    
    const analyst = new AIAnalyst();
    
    if (!analyst.enabled) {
        console.log('❌ AI 分析未启用！请检查以下配置：');
        console.log('   1. 确保设置了 AI_ANALYSIS_ENABLED=true');
        console.log('   2. 确保设置了 XAI_API_KEY 或 OPENAI_API_KEY');
        process.exit(1);
    }
    
    console.log('🔧 AI 配置：');
    console.log(`   模型: ${analyst.model}`);
    console.log(`   超时: ${analyst.timeoutMs}ms`);
    console.log(`   X搜索: ${analyst.xSearchEnabled ? '✅' : '❌'}`);
    console.log('');
    
    console.log('═'.repeat(70));
    
    for (const testCase of testCases) {
        console.log(`\n📊 测试场景: ${testCase.name}`);
        console.log('─'.repeat(50));
        
        // 打印输入数据摘要
        console.log('输入数据:');
        console.log(`   Symbol: ${testCase.data.symbol}`);
        console.log(`   聪明钱: ${testCase.data.smartWalletOnline}个`);
        console.log(`   流动性: $${testCase.data.liquidity.toLocaleString()}`);
        console.log(`   DeBot评分: ${testCase.data.debotScore}/10`);
        console.log(`   叙事类型: ${testCase.data.narrativeType}`);
        console.log(`   负面信息: ${testCase.data.negativeIncidents || '无'}`);
        if (testCase.data.xData) {
            console.log(`   X提及: ${testCase.data.xData.mention_count}条`);
            console.log(`   X情绪: ${testCase.data.xData.sentiment}`);
        }
        console.log('');
        
        // 打印 Prompt
        console.log('📝 生成的 Prompt:');
        const prompt = generateNarrativePrompt(testCase.data);
        console.log('─'.repeat(40));
        console.log(prompt.slice(0, 500) + '...\n');
        
        // 调用 AI 分析
        console.log('🧠 调用 AI 分析...');
        const startTime = Date.now();
        
        try {
            const result = await analyst.evaluate(testCase.data);
            const elapsed = Date.now() - startTime;
            
            console.log(`\n✅ AI 返回 (${elapsed}ms):`);
            console.log(`   评分: ${result.score}/100`);
            console.log(`   判断: ${result.reason}`);
            console.log(`   风险: ${result.risk_level}`);
            
            // 验证预期
            let passed = true;
            if (testCase.expected.min_score && result.score < testCase.expected.min_score) {
                console.log(`   ⚠️ 低于预期最低分 ${testCase.expected.min_score}`);
                passed = false;
            }
            if (testCase.expected.max_score && result.score > testCase.expected.max_score) {
                console.log(`   ⚠️ 高于预期最高分 ${testCase.expected.max_score}`);
                passed = false;
            }
            if (testCase.expected.risk && result.risk_level !== testCase.expected.risk) {
                console.log(`   ⚠️ 风险等级不符预期: ${testCase.expected.risk}`);
                passed = false;
            }
            
            // 计算叙事分数（按照 cross-validator 逻辑）
            const debotBase = Math.min(testCase.data.debotScore * 2, 10);
            const llmBonus = Math.min(result.score * 0.15, 15);
            const narrativeScore = Math.min(debotBase + llmBonus, 25);
            
            console.log(`\n📈 叙事评分计算:`);
            console.log(`   DeBot基础: ${testCase.data.debotScore}/10 × 2 = ${debotBase}/10`);
            console.log(`   LLM加成: ${result.score}/100 × 0.15 = ${llmBonus.toFixed(1)}/15`);
            console.log(`   叙事总分: ${narrativeScore.toFixed(1)}/25`);
            
            console.log(`\n结论: ${passed ? '✅ 通过' : '❌ 需检查'}`);
            
        } catch (error) {
            console.log(`   ❌ 错误: ${error.message}`);
        }
        
        console.log('─'.repeat(50));
    }
    
    console.log('\n' + '═'.repeat(70));
    console.log('🎯 测试完成');
    console.log('═'.repeat(70) + '\n');
}

/**
 * 测试真实 Token
 */
async function testWithRealToken(tokenCA) {
    // 动态导入 DeBot Scout
    const debotScoutModule = await import('../src/inputs/debot-playwright-scout.js');
    const debotScout = debotScoutModule.default;
    const { AIAnalyst } = await import('../src/utils/ai-analyst.js');
    
    const analyst = new AIAnalyst();
    
    if (!analyst.enabled) {
        console.log('❌ AI 分析未启用！');
        process.exit(1);
    }
    
    console.log('🔍 从 DeBot 获取数据...');
    
    try {
        // 获取 AI Report
        const aiReport = await debotScout.fetchAIReport(tokenCA);
        
        if (!aiReport) {
            console.log('⚠️ 未找到 DeBot AI Report，使用模拟数据');
            return;
        }
        
        // 解析 AI Report
        const parsedReport = debotScout.parseAIReport(aiReport);
        
        console.log('\n📊 DeBot 数据:');
        console.log(`   评分: ${parsedReport?.rating?.score || 0}/10`);
        console.log(`   叙事类型: ${parsedReport?.narrativeType || 'Unknown'}`);
        console.log(`   背景: ${parsedReport?.origin?.slice(0, 100) || 'N/A'}...`);
        
        // 准备分析数据
        const data = analyst.prepareData(
            { tokenAddress: tokenCA, symbol: tokenCA.slice(0, 8) },
            parsedReport,
            { channelCount: 0, tier1Count: 0 }
        );
        
        // 调用 AI 分析
        console.log('\n🧠 调用 AI 分析...');
        const result = await analyst.evaluate(data);
        
        if (result) {
            console.log(`\n✅ AI 分析结果:`);
            console.log(`   评分: ${result.score}/100`);
            console.log(`   判断: ${result.reason}`);
            console.log(`   风险: ${result.risk_level}`);
            
            // 计算叙事评分
            const debotBase = Math.min((parsedReport?.rating?.score || 0) * 2, 10);
            const llmBonus = Math.min(result.score * 0.15, 15);
            const narrativeScore = Math.min(debotBase + llmBonus, 25);
            
            console.log(`\n📈 叙事评分计算:`);
            console.log(`   DeBot基础: ${parsedReport?.rating?.score || 0}/10 × 2 = ${debotBase}/10`);
            console.log(`   LLM加成: ${result.score}/100 × 0.15 = ${llmBonus.toFixed(1)}/15`);
            console.log(`   叙事总分: ${narrativeScore.toFixed(1)}/25`);
        }
        
    } catch (error) {
        console.error('❌ 测试失败:', error.message);
    }
}

// 运行测试
main().catch(error => {
    console.error('❌ 测试失败:', error);
    process.exit(1);
});
