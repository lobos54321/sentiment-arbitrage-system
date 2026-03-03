/**
 * Jito Bundle 测试脚本
 * 测试是否能连接到Jito Block Engine
 */

import { Connection, Keypair, PublicKey } from '@solana/web3.js';
import bs58 from 'bs58';
import { JitoBundleSender } from './src/execution/jito-bundle-sender.js';

const JITO_ENDPOINTS = [
  'https://mainnet.block-engine.jito.wtf',
  'https://amsterdam.mainnet.block-engine.jito.wtf',
  'https://frankfurt.mainnet.block-engine.jito.wtf',
  'https://ny.mainnet.block-engine.jito.wtf',
  'https://tokyo.mainnet.block-engine.jito.wtf',
];

async function testJitoConnection() {
  console.log('\n🧪 测试 Jito Block Engine 连接...\n');
  
  for (const endpoint of JITO_ENDPOINTS) {
    try {
      const start = Date.now();
      const response = await fetch(`${endpoint}/api/v1/bundles`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          jsonrpc: '2.0',
          id: 1,
          method: 'getTipAccounts',
          params: []
        }),
      });
      
      const latency = Date.now() - start;
      const data = await response.json();
      
      if (data.result || data.error) {
        console.log(`✅ ${endpoint}`);
        console.log(`   延迟: ${latency}ms`);
        if (data.result) {
          console.log(`   Tip账户数: ${data.result.length}`);
        }
      } else {
        console.log(`⚠️  ${endpoint} - 未知响应`);
      }
    } catch (error) {
      console.log(`❌ ${endpoint}`);
      console.log(`   错误: ${error.message}`);
    }
    console.log();
  }
}

async function testJupiterQuote() {
  console.log('\n🧪 测试 Jupiter 报价...\n');
  
  const SOL_MINT = 'So11111111111111111111111111111111111111112';
  // 使用一个流动性好的token测试
  const USDC_MINT = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
  
  try {
    const response = await fetch(
      `https://api.jup.ag/swap/v1/quote?inputMint=${SOL_MINT}&outputMint=${USDC_MINT}&amount=100000000&slippageBps=100`
    );
    const quote = await response.json();
    
    if (quote.outAmount) {
      const solIn = 0.1;
      const usdcOut = parseInt(quote.outAmount) / 1e6;
      console.log(`✅ Jupiter 报价成功`);
      console.log(`   ${solIn} SOL → ${usdcOut.toFixed(2)} USDC`);
      console.log(`   路由: ${quote.routePlan?.map(r => r.swapInfo?.label).join(' → ') || '直接'}`);
    } else {
      console.log(`❌ 报价失败: ${JSON.stringify(quote)}`);
    }
  } catch (error) {
    console.log(`❌ Jupiter 错误: ${error.message}`);
  }
}

async function main() {
  console.log('=' .repeat(60));
  console.log('🚀 Jito + Jupiter 集成测试');
  console.log('=' .repeat(60));
  
  await testJitoConnection();
  await testJupiterQuote();
  
  console.log('\n' + '=' .repeat(60));
  console.log('✅ 测试完成');
  console.log('=' .repeat(60));
}

main().catch(console.error);
