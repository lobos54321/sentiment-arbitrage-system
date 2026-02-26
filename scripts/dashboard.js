#!/usr/bin/env node
/**
 * 📊 Sentiment Arbitrage Dashboard
 * 
 * 查看系统运行状态和信号源表现
 * 
 * 使用方法:
 *   npm run dashboard          # 完整报告
 *   npm run dashboard sources  # 只看信号源
 *   npm run dashboard signals  # 只看最近信号
 */

import Database from 'better-sqlite3';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import dotenv from 'dotenv';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const projectRoot = join(__dirname, '..');
const dbPath = process.env.DB_PATH || './data/sentiment_arb.db';

const db = new Database(join(projectRoot, dbPath));

// 命令行参数
const command = process.argv[2] || 'all';

console.log('\n' + '═'.repeat(70));
console.log('📊 SENTIMENT ARBITRAGE DASHBOARD');
console.log('═'.repeat(70));
console.log(`📅 ${new Date().toLocaleString()}`);
console.log('');

// ========================================
// 1. 系统概览
// ========================================
if (command === 'all' || command === 'overview') {
  console.log('┌─────────────────────────────────────────────────────────────────────┐');
  console.log('│ 📈 SYSTEM OVERVIEW                                                  │');
  console.log('└─────────────────────────────────────────────────────────────────────┘\n');
  
  try {
    // 获取总体统计
    const totalSignals = db.prepare(`SELECT COUNT(*) as count FROM telegram_signals`).get();
    const todaySignals = db.prepare(`
      SELECT COUNT(*) as count FROM telegram_signals 
      WHERE created_at > strftime('%s', 'now', '-1 day')
    `).get();
    
    let trackedSignals = { count: 0 };
    let completedTracking = { count: 0 };
    
    try {
      trackedSignals = db.prepare(`
        SELECT COUNT(*) as count FROM shadow_price_tracking
      `).get();
      
      completedTracking = db.prepare(`
        SELECT COUNT(*) as count FROM shadow_price_tracking WHERE status = 'completed'
      `).get();
    } catch (e) {
      // Table might not exist yet
    }
    
    console.log(`   📡 Total Signals Received: ${totalSignals?.count || 0}`);
    console.log(`   📡 Signals Today (24h): ${todaySignals?.count || 0}`);
    console.log(`   📊 Signals Tracked: ${trackedSignals?.count || 0}`);
    console.log(`   ✅ Tracking Completed: ${completedTracking?.count || 0}`);
  } catch (e) {
    console.log('   ⏳ No signal data yet.');
  }
  console.log('');
}

// ========================================
// 2. 信号源排名 (核心)
// ========================================
if (command === 'all' || command === 'sources') {
  console.log('┌─────────────────────────────────────────────────────────────────────┐');
  console.log('│ 🏆 SIGNAL SOURCE RANKING                                            │');
  console.log('└─────────────────────────────────────────────────────────────────────┘\n');
  
  try {
    // 从 shadow_price_tracking 获取信号源统计
    const sourceStats = db.prepare(`
      SELECT 
        source_id,
        COUNT(*) as total_signals,
        SUM(CASE WHEN pnl_15m > 0 THEN 1 ELSE 0 END) as wins,
        SUM(CASE WHEN pnl_15m <= 0 THEN 1 ELSE 0 END) as losses,
        ROUND(AVG(pnl_15m), 2) as avg_pnl_15m,
        ROUND(AVG(pnl_5m), 2) as avg_pnl_5m,
        ROUND(MAX(max_pnl), 2) as best_pnl,
        ROUND(MIN(CASE WHEN pnl_15m < 0 THEN pnl_15m END), 2) as worst_pnl,
        ROUND(SUM(CASE WHEN pnl_15m > 0 THEN 1.0 ELSE 0 END) / COUNT(*) * 100, 1) as win_rate
      FROM shadow_price_tracking
      WHERE status = 'completed' AND source_id IS NOT NULL
      GROUP BY source_id
      HAVING total_signals >= 3
      ORDER BY avg_pnl_15m DESC
    `).all();
    
    if (sourceStats.length === 0) {
      console.log('   ⏳ No completed tracking data yet. Keep running in shadow mode!\n');
    } else {
      console.log('   Rank │ Source                    │ Signals │ Win Rate │ Avg PnL │ Best   │ Worst');
      console.log('   ─────┼───────────────────────────┼─────────┼──────────┼─────────┼────────┼───────');
      
      sourceStats.forEach((s, i) => {
        const rank = i + 1;
        const sourceName = (s.source_id || 'Unknown').substring(0, 23).padEnd(23);
        const signals = String(s.total_signals).padStart(5);
        const winRate = `${(s.win_rate || 0).toFixed(0)}%`.padStart(6);
        const avgPnl = `${s.avg_pnl_15m >= 0 ? '+' : ''}${(s.avg_pnl_15m || 0).toFixed(1)}%`.padStart(7);
      const bestPnl = `+${(s.best_pnl || 0).toFixed(0)}%`.padStart(5);
      const worstPnl = `${(s.worst_pnl || 0).toFixed(0)}%`.padStart(5);
      
      // 颜色标记
      const emoji = s.win_rate >= 50 ? '🟢' : s.win_rate >= 30 ? '🟡' : '🔴';
      
      console.log(`   ${emoji} ${String(rank).padStart(2)} │ ${sourceName} │ ${signals} │ ${winRate} │ ${avgPnl} │ ${bestPnl} │ ${worstPnl}`);
    });
    
    console.log('');
    console.log('   Legend: 🟢 Win Rate ≥50%  🟡 30-50%  🔴 <30%');
  }
  } catch (e) {
    console.log('   ⏳ Shadow tracking table not initialized. Run the system first!\n');
  }
  console.log('');
}

// ========================================
// 3. 频道活跃度
// ========================================
if (command === 'all' || command === 'channels') {
  console.log('┌─────────────────────────────────────────────────────────────────────┐');
  console.log('│ 📱 CHANNEL ACTIVITY (Last 24h)                                      │');
  console.log('└─────────────────────────────────────────────────────────────────────┘\n');
  
  try {
    const channelActivity = db.prepare(`
      SELECT 
        channel_name,
        COUNT(*) as signal_count,
        MAX(datetime(created_at, 'unixepoch')) as last_signal
      FROM telegram_signals
    WHERE created_at > strftime('%s', 'now', '-1 day')
    GROUP BY channel_name
    ORDER BY signal_count DESC
    LIMIT 15
  `).all();
  
  if (channelActivity.length === 0) {
    console.log('   ⏳ No signals in the last 24 hours.\n');
  } else {
    console.log('   Channel                         │ Signals │ Last Signal');
    console.log('   ────────────────────────────────┼─────────┼─────────────────────');
    
    channelActivity.forEach(c => {
      const name = (c.channel_name || 'Unknown').substring(0, 30).padEnd(30);
      const count = String(c.signal_count).padStart(5);
      const lastSignal = c.last_signal || 'N/A';
      
      console.log(`   ${name} │ ${count} │ ${lastSignal}`);
    });
  }
  } catch (e) {
    console.log('   ⏳ No channel data yet.\n');
  }
  console.log('');
}

// ========================================
// 4. 最近信号
// ========================================
if (command === 'all' || command === 'signals') {
  console.log('┌─────────────────────────────────────────────────────────────────────┐');
  console.log('│ 🔔 RECENT SIGNALS (Last 20)                                         │');
  console.log('└─────────────────────────────────────────────────────────────────────┘\n');
  
  try {
    const recentSignals = db.prepare(`
      SELECT 
        s.token_ca,
        s.chain,
        s.channel_name,
        datetime(s.created_at, 'unixepoch') as signal_time,
        t.pnl_5m,
        t.pnl_15m,
        t.max_pnl,
        t.status as track_status
      FROM telegram_signals s
      LEFT JOIN shadow_price_tracking t ON s.token_ca = t.token_ca AND s.chain = t.chain
      ORDER BY s.created_at DESC
      LIMIT 20
    `).all();
    
    if (recentSignals.length === 0) {
      console.log('   ⏳ No signals yet.\n');
    } else {
      console.log('   Token     │ Chain │ Source              │ 5min    │ 15min   │ Max     │ Status');
    console.log('   ──────────┼───────┼─────────────────────┼─────────┼─────────┼─────────┼────────');
    
    recentSignals.forEach(s => {
      const token = (s.token_ca || '').substring(0, 8).padEnd(8);
      const chain = (s.chain || '').padEnd(5);
      const source = (s.channel_name || 'Unknown').substring(0, 19).padEnd(19);
      
      const pnl5m = s.pnl_5m !== null ? `${s.pnl_5m >= 0 ? '+' : ''}${s.pnl_5m.toFixed(1)}%`.padStart(7) : '   -   ';
      const pnl15m = s.pnl_15m !== null ? `${s.pnl_15m >= 0 ? '+' : ''}${s.pnl_15m.toFixed(1)}%`.padStart(7) : '   -   ';
      const maxPnl = s.max_pnl !== null ? `+${s.max_pnl.toFixed(0)}%`.padStart(7) : '   -   ';
      
      let status = '⏳';
      if (s.track_status === 'completed') status = s.pnl_15m > 0 ? '✅' : '❌';
      else if (s.track_status === 'tracking') status = '📊';
      
      console.log(`   ${token} │ ${chain} │ ${source} │ ${pnl5m} │ ${pnl15m} │ ${maxPnl} │ ${status}`);
    });
  }
  } catch (e) {
    console.log('   ⏳ No signal data yet.\n');
  }
  console.log('');
}

// ========================================
// 5. 叙事热度
// ========================================
if (command === 'all' || command === 'narratives') {
  console.log('┌─────────────────────────────────────────────────────────────────────┐');
  console.log('│ 📖 NARRATIVE WEIGHTS                                                │');
  console.log('└─────────────────────────────────────────────────────────────────────┘\n');
  
  try {
    const narratives = db.prepare(`
      SELECT name, weight, stage, heat_score
      FROM ai_narratives
      ORDER BY weight DESC
      LIMIT 10
    `).all();
    
    if (narratives.length === 0) {
      console.log('   ⏳ No narrative data.\n');
    } else {
      console.log('   Narrative           │ Weight │ Stage            │ Heat');
      console.log('   ────────────────────┼────────┼──────────────────┼──────');
      
      narratives.forEach(n => {
        const name = (n.name || '').padEnd(18);
        const weight = (n.weight || 0).toFixed(1).padStart(5);
        const stage = (n.stage || 'unknown').padEnd(16);
        const heat = (n.heat_score || 0).toFixed(1).padStart(4);
        
        const emoji = n.weight >= 8 ? '🔥' : n.weight >= 5 ? '📈' : '📉';
        
        console.log(`   ${emoji} ${name} │ ${weight} │ ${stage} │ ${heat}`);
      });
    }
  } catch (e) {
    console.log('   ⏳ Narrative table not initialized yet.\n');
  }
  console.log('');
}

// ========================================
// 6. 推荐操作
// ========================================
if (command === 'all') {
  console.log('┌─────────────────────────────────────────────────────────────────────┐');
  console.log('│ 💡 RECOMMENDATIONS                                                  │');
  console.log('└─────────────────────────────────────────────────────────────────────┘\n');
  
  try {
    // 检查数据量
    const totalTracked = db.prepare(`SELECT COUNT(*) as c FROM shadow_price_tracking WHERE status = 'completed'`).get();
    
    if (!totalTracked || totalTracked.c < 50) {
      console.log(`   ⏳ Keep running! Only ${totalTracked?.c || 0}/50 signals tracked.`);
      console.log('      Need more data for reliable source evaluation.\n');
    } else {
      // 找出表现差的源
      const poorSources = db.prepare(`
        SELECT source_id, 
               ROUND(SUM(CASE WHEN pnl_15m > 0 THEN 1.0 ELSE 0 END) / COUNT(*) * 100, 1) as win_rate,
               ROUND(AVG(pnl_15m), 2) as avg_pnl
        FROM shadow_price_tracking
        WHERE status = 'completed' AND source_id IS NOT NULL
        GROUP BY source_id
        HAVING COUNT(*) >= 5 AND (win_rate < 30 OR avg_pnl < -10)
      `).all();
      
      if (poorSources.length > 0) {
        console.log('   ❌ Consider removing these poor-performing sources:\n');
        poorSources.forEach(s => {
          console.log(`      - ${s.source_id}: ${s.win_rate}% win rate, ${s.avg_pnl}% avg PnL`);
        });
        console.log('');
      }
      
      // 找出表现好的源
      const goodSources = db.prepare(`
        SELECT source_id, 
               ROUND(SUM(CASE WHEN pnl_15m > 0 THEN 1.0 ELSE 0 END) / COUNT(*) * 100, 1) as win_rate,
               ROUND(AVG(pnl_15m), 2) as avg_pnl
        FROM shadow_price_tracking
        WHERE status = 'completed' AND source_id IS NOT NULL
        GROUP BY source_id
        HAVING COUNT(*) >= 5 AND win_rate >= 50 AND avg_pnl > 0
        ORDER BY avg_pnl DESC
        LIMIT 5
      `).all();
      
      if (goodSources.length > 0) {
        console.log('   ✅ Top performing sources (ready for live trading):\n');
        goodSources.forEach(s => {
          console.log(`      - ${s.source_id}: ${s.win_rate}% win rate, +${s.avg_pnl}% avg PnL`);
        });
        console.log('');
      }
    }
  } catch (e) {
    console.log('   ⏳ No tracking data yet. Run the system in shadow mode first!\n');
  }
}

// ========================================
// 7. CrossValidator 验证信号
// ========================================
if (command === 'all' || command === 'validated') {
  console.log('┌─────────────────────────────────────────────────────────────────────┐');
  console.log('│ 🎯 CROSSVALIDATOR SIGNALS (DeBot验证通过)                           │');
  console.log('└─────────────────────────────────────────────────────────────────────┘\n');
  
  try {
    const validatedSignals = db.prepare(`
      SELECT 
        token_ca,
        chain,
        channel_name,
        message_text,
        datetime(created_at, 'unixepoch') as signal_time
      FROM telegram_signals
      WHERE channel_name LIKE 'DeBot%'
      ORDER BY created_at DESC
      LIMIT 15
    `).all();
    
    if (validatedSignals.length === 0) {
      console.log('   ⏳ No CrossValidator signals yet.\n');
    } else {
      console.log('   Token    │ Rating │ Position │ Smart$ │ AI │ TG │ Score │ Time');
      console.log('   ─────────┼────────┼──────────┼────────┼────┼────┼───────┼─────────────');
      
      validatedSignals.forEach(s => {
        const token = (s.token_ca || '').substring(0, 8);
        const msg = s.message_text || '';
        
        // 解析消息
        const ratingMatch = msg.match(/评级:\s*(\w+)/);
        const posMatch = msg.match(/仓位:\s*([\d.]+)\s*SOL/);
        const smartMatch = msg.match(/聪明钱:\s*(\d+)\/(\d+)/);
        const aiMatch = msg.match(/AI评分:\s*(\d+)\/10/);
        const tgMatch = msg.match(/TG热度:\s*(\d+)/);
        const scoreMatch = msg.match(/\((\d+)分\)/);
        
        const rating = ratingMatch ? ratingMatch[1].padEnd(6) : '?     ';
        const position = posMatch ? `${posMatch[1]} SOL`.padEnd(8) : '?       ';
        const smart = smartMatch ? `${smartMatch[1]}/${smartMatch[2]}`.padEnd(6) : '?     ';
        const ai = aiMatch ? aiMatch[1].padStart(2) : ' ?';
        const tg = tgMatch ? tgMatch[1].padStart(2) : ' ?';
        const score = scoreMatch ? scoreMatch[1].padStart(3) : '  ?';
        const time = s.signal_time ? s.signal_time.split(' ')[1] : '?';
        
        const emoji = s.channel_name.includes('A_Signal') ? '✅' : 
                      s.channel_name.includes('S_Signal') ? '🚀' : '🐦';
        
        console.log(`   ${emoji} ${token} │ ${rating} │ ${position} │ ${smart} │ ${ai} │ ${tg} │ ${score} │ ${time}`);
      });
      
      // 统计
      const stats = db.prepare(`
        SELECT 
          COUNT(*) as total,
          SUM(CASE WHEN channel_name LIKE '%S_Signal%' THEN 1 ELSE 0 END) as s_level,
          SUM(CASE WHEN channel_name LIKE '%A_Signal%' THEN 1 ELSE 0 END) as a_level,
          SUM(CASE WHEN channel_name LIKE '%Scout%' THEN 1 ELSE 0 END) as scout
        FROM telegram_signals
        WHERE channel_name LIKE 'DeBot%'
      `).get();
      
      console.log('');
      console.log(`   📊 总计: ${stats.total} 个验证信号 (🚀S级: ${stats.s_level} | ✅A级: ${stats.a_level} | 🐦Scout: ${stats.scout})`);
    }
  } catch (e) {
    console.log('   ⏳ No validated signal data yet.\n');
  }
  console.log('');
}

// ========================================
// 8. 模拟交易战绩
// ========================================
if (command === 'all' || command === 'trades') {
  console.log('┌─────────────────────────────────────────────────────────────────────┐');
  console.log('│ 💰 TRADING PERFORMANCE (模拟交易战绩)                               │');
  console.log('└─────────────────────────────────────────────────────────────────────┘\n');
  
  try {
    // 检查 trades 表
    const trades = db.prepare(`
      SELECT 
        token_ca,
        chain,
        action,
        entry_price,
        exit_price,
        position_size,
        pnl_percent,
        pnl_sol,
        status,
        datetime(created_at, 'unixepoch') as trade_time
      FROM trades
      ORDER BY created_at DESC
      LIMIT 20
    `).all();
    
    if (trades.length === 0) {
      console.log('   ⏳ No trades yet. Enable AUTO_BUY_ENABLED=true to start simulation.\n');
    } else {
      // 计算总体统计
      const perfStats = db.prepare(`
        SELECT 
          COUNT(*) as total_trades,
          SUM(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END) as wins,
          SUM(CASE WHEN pnl_percent <= 0 THEN 1 ELSE 0 END) as losses,
          ROUND(AVG(pnl_percent), 2) as avg_pnl,
          ROUND(SUM(pnl_sol), 4) as total_pnl_sol,
          ROUND(MAX(pnl_percent), 2) as best_trade,
          ROUND(MIN(pnl_percent), 2) as worst_trade
        FROM trades
        WHERE status = 'closed'
      `).get();
      
      const winRate = perfStats.total_trades > 0 
        ? ((perfStats.wins / perfStats.total_trades) * 100).toFixed(1) 
        : 0;
      
      console.log('   📊 OVERALL STATS:');
      console.log(`      Total Trades: ${perfStats.total_trades}`);
      console.log(`      Win Rate: ${winRate}% (${perfStats.wins}W / ${perfStats.losses}L)`);
      console.log(`      Avg PnL: ${perfStats.avg_pnl >= 0 ? '+' : ''}${perfStats.avg_pnl || 0}%`);
      console.log(`      Total PnL: ${perfStats.total_pnl_sol >= 0 ? '+' : ''}${perfStats.total_pnl_sol || 0} SOL`);
      console.log(`      Best Trade: +${perfStats.best_trade || 0}%`);
      console.log(`      Worst Trade: ${perfStats.worst_trade || 0}%`);
      console.log('');
      
      console.log('   📜 RECENT TRADES:');
      console.log('   Token    │ Chain │ Size     │ Entry      │ Exit       │ PnL %   │ PnL SOL │ Status');
      console.log('   ─────────┼───────┼──────────┼────────────┼────────────┼─────────┼─────────┼────────');
      
      trades.forEach(t => {
        const token = (t.token_ca || '').substring(0, 8);
        const chain = (t.chain || '').padEnd(5);
        const size = `${t.position_size || 0} SOL`.padEnd(8);
        const entry = t.entry_price ? `$${t.entry_price.toFixed(8)}`.substring(0, 10).padEnd(10) : '?         ';
        const exit = t.exit_price ? `$${t.exit_price.toFixed(8)}`.substring(0, 10).padEnd(10) : '?         ';
        const pnlPct = t.pnl_percent !== null ? `${t.pnl_percent >= 0 ? '+' : ''}${t.pnl_percent.toFixed(1)}%`.padStart(7) : '   ?   ';
        const pnlSol = t.pnl_sol !== null ? `${t.pnl_sol >= 0 ? '+' : ''}${t.pnl_sol.toFixed(3)}`.padStart(7) : '   ?   ';
        
        const emoji = t.status === 'closed' ? (t.pnl_percent > 0 ? '✅' : '❌') : '⏳';
        
        console.log(`   ${emoji} ${token} │ ${chain} │ ${size} │ ${entry} │ ${exit} │ ${pnlPct} │ ${pnlSol} │ ${t.status || '?'}`);
      });
    }
  } catch (e) {
    console.log('   ⏳ Trades table not initialized or error: ' + e.message + '\n');
  }
  console.log('');
}

// ========================================
// 总结
// ========================================
console.log('═'.repeat(70));
console.log('Commands: npm run dashboard [all|sources|signals|channels|narratives|validated|trades]');
console.log('═'.repeat(70) + '\n');

db.close();
