#!/usr/bin/env node
/**
 * 检查被过滤token的后续表现
 * 使用DexScreener API查询
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// 读取被过滤的token列表
const dataPath = path.join(__dirname, '..', 'data', 'filtered-tokens-to-check.json');
const data = JSON.parse(fs.readFileSync(dataPath, 'utf-8'));

async function fetchDexScreener(tokenCA) {
    try {
        const response = await fetch(`https://api.dexscreener.com/latest/dex/tokens/${tokenCA}`, {
            headers: { 'Accept': 'application/json' },
            signal: AbortSignal.timeout(10000)
        });
        if (!response.ok) return null;
        return await response.json();
    } catch (e) {
        return null;
    }
}

async function checkTokens() {
    console.log(`\n${'═'.repeat(70)}`);
    console.log(`📊 被过滤Token后续表现检查`);
    console.log(`${'═'.repeat(70)}`);
    console.log(`检查 ${data.tokens.length} 个token\n`);

    const results = {
        alive: [],
        dead: [],
        goldDogs: [],    // 涨幅 > 100%
        silverDogs: [],  // 涨幅 > 50%
        missed: []       // 涨幅 > 30%
    };

    for (const token of data.tokens) {
        const result = await fetchDexScreener(token.address);
        
        // 防止限流
        await new Promise(r => setTimeout(r, 300));
        
        if (!result || !result.pairs || result.pairs.length === 0) {
            results.dead.push({ ...token, status: 'DEAD' });
            console.log(`💀 ${token.symbol} (分数:${token.score}) -> 已死亡/下架`);
            continue;
        }

        const pair = result.pairs[0];
        const priceChange24h = parseFloat(pair.priceChange?.h24) || 0;
        const priceChange6h = parseFloat(pair.priceChange?.h6) || 0;
        const liquidity = parseFloat(pair.liquidity?.usd) || 0;
        const volume24h = parseFloat(pair.volume?.h24) || 0;
        const mcap = parseFloat(pair.marketCap) || parseFloat(pair.fdv) || 0;

        const tokenResult = {
            ...token,
            status: 'ALIVE',
            priceChange24h,
            priceChange6h,
            liquidity,
            volume24h,
            mcap
        };

        results.alive.push(tokenResult);

        // 分类
        if (priceChange24h > 100 || priceChange6h > 100) {
            results.goldDogs.push(tokenResult);
            console.log(`🥇 ${token.symbol} (分数:${token.score}) -> 24h:+${priceChange24h.toFixed(0)}% 6h:+${priceChange6h.toFixed(0)}% 💰 MCAP:$${(mcap/1000).toFixed(0)}K`);
        } else if (priceChange24h > 50 || priceChange6h > 50) {
            results.silverDogs.push(tokenResult);
            console.log(`🥈 ${token.symbol} (分数:${token.score}) -> 24h:+${priceChange24h.toFixed(0)}% 6h:+${priceChange6h.toFixed(0)}%`);
        } else if (priceChange24h > 30 || priceChange6h > 30) {
            results.missed.push(tokenResult);
            console.log(`⚠️ ${token.symbol} (分数:${token.score}) -> 24h:+${priceChange24h.toFixed(0)}% 6h:+${priceChange6h.toFixed(0)}%`);
        } else if (priceChange24h < -30) {
            console.log(`✅ ${token.symbol} (分数:${token.score}) -> 24h:${priceChange24h.toFixed(0)}% (正确过滤)`);
        } else {
            console.log(`➖ ${token.symbol} (分数:${token.score}) -> 24h:${priceChange24h.toFixed(0)}% (中性)`);
        }
    }

    // 汇总
    console.log(`\n${'═'.repeat(70)}`);
    console.log(`📊 汇总结果`);
    console.log(`${'═'.repeat(70)}`);
    console.log(`总检查: ${data.tokens.length}`);
    console.log(`存活: ${results.alive.length} (${(results.alive.length/data.tokens.length*100).toFixed(1)}%)`);
    console.log(`死亡: ${results.dead.length} (${(results.dead.length/data.tokens.length*100).toFixed(1)}%)`);
    console.log(`\n🎯 错过的机会:`);
    console.log(`  🥇 金狗 (>100%): ${results.goldDogs.length} 个`);
    console.log(`  🥈 银狗 (>50%): ${results.silverDogs.length} 个`);
    console.log(`  ⚠️ 潜力 (>30%): ${results.missed.length} 个`);
    
    const totalMissed = results.goldDogs.length + results.silverDogs.length;
    const missRate = (totalMissed / data.tokens.length * 100).toFixed(1);
    console.log(`\n📈 漏杀率: ${missRate}% (${totalMissed}/${data.tokens.length})`);
    
    // 保存详细结果
    const outputPath = path.join(__dirname, '..', 'data', 'filtered-tokens-performance.json');
    fs.writeFileSync(outputPath, JSON.stringify(results, null, 2));
    console.log(`\n详细结果已保存到 ${outputPath}`);
}

checkTokens().catch(console.error);
