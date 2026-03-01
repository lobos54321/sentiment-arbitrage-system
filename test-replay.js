/**
 * 信号重放测试 - 验证新 MC 阈值 + Opus 模型
 * 从 DB 取历史信号，重新跑 pipeline
 */
import Database from 'better-sqlite3';
import { PremiumSignalEngine } from './src/engines/premium-signal-engine.js';
import { LivePriceMonitor } from './src/tracking/live-price-monitor.js';

const db = new Database('./data/sentiment_arb.db');

// 取几条有代表性的信号
const testSignals = [
  // 之前被 MC>20K 拦掉的
  { token_ca: '6nDUuWpdTGiom4BRXtLMtWyT9UfNuKFLo86MYPGgpump', symbol: 'ESSENCE', market_cap: 40510 },
  // 之前通过的低 MC
  { token_ca: 'GqtnXK2QfXKDLqWjyphQf77ZpZxVrt19mZMQ7S9ipump', symbol: 'KREDO', market_cap: 225180 },
];

// 从 DB 补充 description
for (const sig of testSignals) {
  const row = db.prepare('SELECT description FROM premium_signals WHERE token_ca = ? ORDER BY id DESC LIMIT 1').get(sig.token_ca);
  sig.description = row?.description || '';
  sig.source = 'premium_channel_ath';
  sig.is_ath = true;
}

async function main() {
  console.log('=== 信号重放测试 ===\n');

  // 初始化 engine
  const engine = new PremiumSignalEngine();

  // 启动 LivePriceMonitor
  if (process.env.JUPITER_API_KEY) {
    const priceMonitor = new LivePriceMonitor();
    priceMonitor.start();
    engine.setLivePriceMonitor(priceMonitor);
  }

  await engine.initialize();

  for (const sig of testSignals) {
    console.log(`\n${'═'.repeat(60)}`);
    console.log(`重放: $${sig.symbol} | MC: $${(sig.market_cap/1000).toFixed(1)}K | CA: ${sig.token_ca.substring(0, 12)}...`);
    console.log('═'.repeat(60));

    try {
      const result = await engine.processSignal(sig);
      console.log(`\n结果: ${JSON.stringify(result, null, 2)}`);
    } catch (err) {
      console.error(`错误: ${err.message}`);
    }

    // 等 2 秒避免限速
    await new Promise(r => setTimeout(r, 2000));
  }

  console.log('\n=== 重放完成 ===');
  process.exit(0);
}

main().catch(console.error);
