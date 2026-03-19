/**
 * 从 Zeabur 部署实例导出信号数据库
 * 
 * 用法:
 *   DASHBOARD_TOKEN=your_token node scripts/export-zeabur-signals.mjs
 *
 * 需要 DASHBOARD_TOKEN（在 Zeabur 环境变量中设置）
 */

import https from 'https';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const BASE_URL = process.env.ZEABUR_URL || 'https://sentiment-arbitrage.zeabur.app';
const TOKEN = process.env.DASHBOARD_TOKEN || '';

function fetchJson(url) {
  return new Promise((resolve) => {
    const fullUrl = `${url}${url.includes('?') ? '&' : '?'}token=${TOKEN}`;
    https.get(fullUrl, { timeout: 30000, headers: {'User-Agent': 'Mozilla/5.0'} }, (res) => {
      let data = '';
      res.on('data', d => data += d);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); } catch { resolve(null); }
      });
    }).on('error', () => resolve(null)).on('timeout', () => resolve(null));
  });
}

async function main() {
  if (!TOKEN) {
    console.error('❌ 请设置 DASHBOARD_TOKEN 环境变量');
    console.error('   export DASHBOARD_TOKEN=your_token');
    console.error('   node scripts/export-zeabur-signals.mjs');
    process.exit(1);
  }

  console.log(`🔌 连接 Zeabur: ${BASE_URL}`);
  
  // 1. 测试连接
  const health = await fetchJson(`${BASE_URL}/health`);
  if (!health?.status) {
    console.error('❌ 无法连接到 Zeabur 实例');
    process.exit(1);
  }
  console.log(`✅ 连接成功. DB: ${health.db?.shadow_pnl?.total} shadow_pnl 记录`);
  
  // 2. 导出完整数据库
  console.log('\n📥 导出数据库...');
  const exportData = await fetchJson(`${BASE_URL}/api/export`);
  if (!exportData) {
    console.error('❌ 导出失败，检查 DASHBOARD_TOKEN 是否正确');
    process.exit(1);
  }
  
  const outputFile = path.join(__dirname, '../data/zeabur-export.json');
  fs.writeFileSync(outputFile, JSON.stringify(exportData, null, 2));
  console.log(`✅ 数据已保存到 ${outputFile}`);
  
  // 3. 解析shadow_pnl记录（含NOT_ATH信号）
  const records = exportData.shadow_pnl || exportData.tables?.shadow_pnl || [];
  console.log(`\n📊 shadow_pnl 记录: ${records.length}个`);
  
  if (records.length > 0) {
    const ath = records.filter(r => r.is_ath === 1 || r.is_ath === true);
    const notath = records.filter(r => r.is_ath === 0 || r.is_ath === false || !r.is_ath);
    console.log(`   ATH信号: ${ath.length}个`);
    console.log(`   NOT_ATH信号: ${notath.length}个`);
    
    // 保存信号列表
    const signals = records.map(r => ({
      ts: new Date(r.entry_time || r.created_at).getTime(),
      type: (r.is_ath) ? 'ATH' : 'NEW_TRENDING',
      is_ath: !!(r.is_ath),
      symbol: r.symbol,
      token_ca: r.token_ca,
      market_cap: r.entry_mc,
      pnl: r.exit_pnl,
      exit_reason: r.exit_reason,
    }));
    
    const channelHistFile = path.join(__dirname, '../data/channel-history.json');
    fs.writeFileSync(channelHistFile, JSON.stringify({
      fetched_at: new Date().toISOString(),
      source: 'zeabur_export',
      total: records.length,
      ath_signals: ath.length,
      trending_signals: notath.length,
      signals,
    }, null, 2));
    
    console.log(`\n💾 信号数据已保存到 data/channel-history.json`);
    console.log(`🎯 下一步: python3 scripts/backtest-not-ath.py`);
  }
}

main().catch(console.error);
