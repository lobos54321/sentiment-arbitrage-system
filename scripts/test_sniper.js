import Database from 'better-sqlite3';
import { executeSniperWatchlistEntry } from '../src/strategies/MomentumResonance.js';
import PremiumSignalEngine from '../src/engines/premium-signal-engine.js';

async function runLocalTest() {
  console.log("=========================================");
  console.log("🚀 启动本地狙击者策略 (Momentum Resonance) 沙箱回测");
  console.log("=========================================");
  
  // 连接我们之前拉取的服务器真实信号库
  const db = new Database('./server_sentiment_arb.db');
  
  // 创建依赖的底层服务
  const engine = new PremiumSignalEngine({ db });

  // 筛选最近 10 条真实发生的 NEW_TRENDING 信号
  const signals = db.prepare(`
    SELECT * FROM premium_signals 
    ORDER BY timestamp DESC LIMIT 5
  `).all();

  console.log(`\n✅ 成功从本地加载了 ${signals.length} 个真实的近期趋势信号，准备进入狙击探测`);

  const systemContext = {
    livePositionMonitor: engine.livePositionMonitor || { positions: new Map() },
    shadowMode: true,
    shadowTracker: engine.shadowTracker || { hasOpenPosition: () => false },
    stats: engine.stats,
    _backfillPrebuyKlines: engine._backfillPrebuyKlines.bind(engine),
    _waitForFreshLocalKlines: engine._waitForFreshLocalKlines ? engine._waitForFreshLocalKlines.bind(engine) : async () => {},
    _checkKline: engine._checkKline.bind(engine),
    saveSignalRecord: () => {},
    signalHistory: engine.signalHistory,
    _saveAthCounts: () => {}
  };

  for (const raw of signals) {
    const signal = {
        symbol: raw.symbol,
        market_cap: raw.market_cap,
        volume_24h: raw.volume_24h,
        is_ath: raw.is_ath === 1,
        timestamp: raw.source_message_ts || raw.timestamp
    };
    const ca = raw.token_ca;
    
    console.log(`\n-----------------------------------------`);
    console.log(`▶️ 信号源灌入: $${signal.symbol} (CA: ${ca.substring(0,8)}...)`);
    console.log(`📊 MC: $${(signal.market_cap/1000).toFixed(1)}K  Vol: $${(signal.volume_24h/1000).toFixed(1)}K`);
    console.log(`-----------------------------------------`);

    // 为了加速测试，我们给 sandbox 函数挂载的 _checkKline 包装一下，只探测1次或跳过60s等待
    // 在真实环境 `executeSniperWatchlistEntry` 自身带有 60s 的等待循环
    try {
        const result = await executeSniperWatchlistEntry(ca, signal, systemContext);
        console.log(`🏁 最终决策结果:`, result);
    } catch(e) {
        console.log(`❌ 评估出错:`, e.message);
    }
  }
  
  console.log("\n✅ 测试结束。");
  process.exit(0);
}

runLocalTest();
