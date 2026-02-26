/**
 * 发现高胜率交易员的信息源
 * 
 * 策略：跟踪高手的关注列表，找到他们的 alpha 来源
 */

import GrokTwitterClient from '../src/social/grok-twitter-client.js';
import dotenv from 'dotenv';

dotenv.config();

async function discoverAlphaSources() {
    console.log('🔍 开始发现高胜率交易员的信息源...\n');

    const grok = new GrokTwitterClient();

    // 用户发现的高胜率交易员 / 信号卖家
    const alphaTraders = [
        'aaalyonbtc',   // 用户观察到胜率很高
        'waveking1314'  // 信号卖家 - 挖掘他的信息来源
    ];

    for (const trader of alphaTraders) {
        console.log(`\n${'='.repeat(60)}`);
        console.log(`📊 分析 @${trader} 的信息源...`);
        console.log(`${'='.repeat(60)}\n`);

        try {
            const result = await grok.discoverAlphaSources(trader);

            if (result.analysis_summary) {
                console.log(`📝 交易风格分析: ${result.analysis_summary}`);
            }

            if (result.alpha_sources && result.alpha_sources.length > 0) {
                console.log(`\n🎯 发现 ${result.alpha_sources.length} 个潜在 Alpha 源:\n`);

                for (const source of result.alpha_sources) {
                    console.log(`  ${source.handle}`);
                    console.log(`     类型: ${source.category}`);
                    console.log(`     影响力: ${source.influence_score}/10`);
                    console.log(`     原因: ${source.reason}`);
                    console.log('');
                }
            }

            if (result.recommended_priority && result.recommended_priority.length > 0) {
                console.log(`\n⭐ 推荐优先监控的账号:`);
                result.recommended_priority.forEach((handle, i) => {
                    console.log(`  ${i + 1}. ${handle}`);
                });
            }

            console.log(`\n📋 完整数据已保存`);

        } catch (error) {
            console.error(`❌ 分析 @${trader} 失败:`, error.message);
        }
    }

    console.log('\n✅ Alpha 源发现完成');
}

discoverAlphaSources().catch(console.error);
