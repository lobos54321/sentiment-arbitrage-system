# 虚拟盘数据完整地址清单

## 一、数据库文件

### 主数据库
```
路径：/Users/boliu/sentiment-arbitrage-system/data/sentiment_arb.db
大小：5.9MB
最后修改：2026-02-26
```

### 关键表结构

#### 1. shadow_pnl（虚拟盘交易记录）
```sql
-- 表路径
data/sentiment_arb.db -> shadow_pnl

-- 字段
id, token_ca, symbol, score, entry_mc, entry_time, 
exit_pnl, high_pnl, low_pnl, exit_reason, closed, closed_at

-- 数据量
总记录：318条
已关闭：304条
未关闭：14条
```

#### 2. trades（交易记录）
```sql
-- 表路径
data/sentiment_arb.db -> trades

-- 字段
id, token_ca, chain, symbol, name, narrative, rating, action,
position_tier, position_size, executed_price, tokens_received,
actual_slippage, tx_hash, timestamp, status, is_simulation,
exit_timestamp, exit_tx_hash, exit_reason, exit_price,
realized_pnl, max_up_2h, max_dd_2h, hold_duration_minutes

-- 数据量
总记录：416条（全部is_simulation=1）
状态：全部OPEN（未关闭）
```

#### 3. premium_signals（信号记录）
```sql
-- 表路径
data/sentiment_arb.db -> premium_signals

-- 字段
id, token_ca, symbol, market_cap, holders, volume_24h, top10_pct,
hard_gate_status, ai_action, ai_confidence, ai_narrative_tier, executed
```

---

## 二、日志文件

### 主日志目录
```
路径：/Users/boliu/sentiment-arbitrage-system/logs/
```

### 关键日志文件
```bash
# Shadow模式测试日志
logs/test-shadow-v2-fix.log.gz

# 系统日志
logs/system.log.gz

# 调试日志
logs/debug_v76.log.gz
logs/debug_v81_positions.log.gz
logs/debug_tighter_stoploss.log.gz
logs/debug_dual_track.log.gz

# Shadow共识缓存
logs/shadow-consensus-cache.json

# 输出日志
logs/out.log
logs/error.log
logs/nohup.log
```

---

## 三、导出的CSV文件

### 临时导出文件
```bash
# 最近50笔交易
/tmp/shadow_trades_recent.csv

# 最近100笔交易
/tmp/shadow_trades_100.csv
```

---

## 四、分析报告

### 生成的文档
```bash
# 虚拟盘完整分析报告
/Users/boliu/sentiment-arbitrage-system/docs/shadow-mode-analysis.md

# 系统检查报告
/Users/boliu/sentiment-arbitrage-system/docs/system-check-report.md

# 策略对比报告
/Users/boliu/sentiment-arbitrage-system/docs/strategy-comparison.md

# 新策略配置
/Users/boliu/sentiment-arbitrage-system/docs/strategy-new-config.md

# 最优策略设计
/Users/boliu/sentiment-arbitrage-system/docs/strategy-optimal-70pct.md
```

---

## 五、数据访问命令

### 5.1 查看虚拟盘总体统计
```bash
cd /Users/boliu/sentiment-arbitrage-system

sqlite3 data/sentiment_arb.db "SELECT 
  COUNT(*) as total,
  SUM(CASE WHEN closed=1 THEN 1 ELSE 0 END) as closed_trades,
  SUM(CASE WHEN closed=1 AND exit_pnl > 0 THEN 1 ELSE 0 END) as winners,
  ROUND(100.0 * SUM(CASE WHEN closed=1 AND exit_pnl > 0 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN closed=1 THEN 1 ELSE 0 END), 0), 1) as win_rate,
  ROUND(AVG(CASE WHEN closed=1 THEN exit_pnl END), 2) as avg_pnl,
  ROUND(SUM(CASE WHEN closed=1 THEN exit_pnl END), 2) as total_pnl
FROM shadow_pnl;"
```

### 5.2 导出所有已关闭交易
```bash
sqlite3 -header -csv data/sentiment_arb.db "SELECT 
  symbol,
  score,
  ROUND(entry_mc/1000, 1) as entry_mc_k,
  ROUND(exit_pnl, 1) as exit_pnl,
  ROUND(high_pnl, 1) as high_pnl,
  ROUND(low_pnl, 1) as low_pnl,
  exit_reason,
  datetime(entry_time, 'unixepoch') as entry_time,
  datetime(closed_at, 'unixepoch') as closed_at,
  ROUND((closed_at - entry_time) / 60.0, 1) as hold_minutes
FROM shadow_pnl 
WHERE closed=1 
ORDER BY id;" > shadow_all_trades.csv
```

### 5.3 查看退出原因分布
```bash
sqlite3 data/sentiment_arb.db "SELECT 
  exit_reason,
  COUNT(*) as count,
  ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM shadow_pnl WHERE closed=1), 1) as pct,
  ROUND(AVG(exit_pnl), 1) as avg_pnl,
  SUM(CASE WHEN exit_pnl > 0 THEN 1 ELSE 0 END) as winners
FROM shadow_pnl 
WHERE closed=1 
GROUP BY exit_reason 
ORDER BY count DESC;"
```

### 5.4 查看MC分层表现
```bash
sqlite3 data/sentiment_arb.db "SELECT 
  CASE 
    WHEN entry_mc < 10000 THEN '0-10K'
    WHEN entry_mc < 20000 THEN '10-20K'
    WHEN entry_mc < 30000 THEN '20-30K'
    WHEN entry_mc < 50000 THEN '30-50K'
    ELSE '50K+'
  END as mc_range,
  COUNT(*) as trades,
  SUM(CASE WHEN exit_pnl > 0 THEN 1 ELSE 0 END) as winners,
  ROUND(100.0 * SUM(CASE WHEN exit_pnl > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as win_rate,
  ROUND(AVG(exit_pnl), 1) as avg_pnl
FROM shadow_pnl 
WHERE closed=1 
GROUP BY mc_range
ORDER BY mc_range;"
```

### 5.5 查看最佳交易（Top 20）
```bash
sqlite3 -header -csv data/sentiment_arb.db "SELECT 
  symbol,
  score,
  ROUND(entry_mc/1000, 1) as entry_mc_k,
  ROUND(exit_pnl, 1) as exit_pnl,
  ROUND(high_pnl, 1) as high_pnl,
  exit_reason
FROM shadow_pnl 
WHERE closed=1 
ORDER BY exit_pnl DESC 
LIMIT 20;"
```

### 5.6 查看最差交易（Bottom 20）
```bash
sqlite3 -header -csv data/sentiment_arb.db "SELECT 
  symbol,
  score,
  ROUND(entry_mc/1000, 1) as entry_mc_k,
  ROUND(exit_pnl, 1) as exit_pnl,
  ROUND(high_pnl, 1) as high_pnl,
  exit_reason
FROM shadow_pnl 
WHERE closed=1 
ORDER BY exit_pnl ASC 
LIMIT 20;"
```

### 5.7 查看MOON_STOP交易
```bash
sqlite3 -header -csv data/sentiment_arb.db "SELECT 
  symbol,
  score,
  ROUND(entry_mc/1000, 1) as entry_mc_k,
  ROUND(exit_pnl, 1) as exit_pnl,
  ROUND(high_pnl, 1) as high_pnl,
  exit_reason
FROM shadow_pnl 
WHERE closed=1 AND exit_reason LIKE 'MOON_STOP%'
ORDER BY high_pnl DESC;"
```

### 5.8 查看评分系统表现
```bash
sqlite3 data/sentiment_arb.db "SELECT 
  CASE 
    WHEN score >= 90 THEN '90-100'
    WHEN score >= 80 THEN '80-89'
    WHEN score >= 70 THEN '70-79'
    WHEN score >= 60 THEN '60-69'
    ELSE '<60'
  END as score_range,
  COUNT(*) as trades,
  SUM(CASE WHEN exit_pnl > 0 THEN 1 ELSE 0 END) as winners,
  ROUND(100.0 * SUM(CASE WHEN exit_pnl > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as win_rate,
  ROUND(AVG(exit_pnl), 1) as avg_pnl
FROM shadow_pnl 
WHERE closed=1 
GROUP BY score_range
ORDER BY score_range DESC;"
```

---

## 六、数据库备份

### 创建备份
```bash
cd /Users/boliu/sentiment-arbitrage-system

# 备份主数据库
cp data/sentiment_arb.db data/sentiment_arb.db.backup-$(date +%Y%m%d)

# 导出shadow_pnl表为SQL
sqlite3 data/sentiment_arb.db ".dump shadow_pnl" > shadow_pnl_backup.sql

# 导出为CSV
sqlite3 -header -csv data/sentiment_arb.db "SELECT * FROM shadow_pnl;" > shadow_pnl_full.csv
```

---

## 七、快速访问脚本

### 创建快速查询脚本
```bash
cat > /Users/boliu/sentiment-arbitrage-system/scripts/query-shadow.sh << 'SCRIPT'
#!/bin/bash
# 虚拟盘数据快速查询脚本

DB_PATH="/Users/boliu/sentiment-arbitrage-system/data/sentiment_arb.db"

case "$1" in
  stats)
    echo "=== 虚拟盘总体统计 ==="
    sqlite3 "$DB_PATH" "SELECT 
      COUNT(*) as total,
      SUM(CASE WHEN closed=1 AND exit_pnl > 0 THEN 1 ELSE 0 END) as winners,
      ROUND(100.0 * SUM(CASE WHEN closed=1 AND exit_pnl > 0 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN closed=1 THEN 1 ELSE 0 END), 0), 1) as win_rate,
      ROUND(AVG(CASE WHEN closed=1 THEN exit_pnl END), 2) as avg_pnl,
      ROUND(SUM(CASE WHEN closed=1 THEN exit_pnl END), 2) as total_pnl
    FROM shadow_pnl;"
    ;;
  
  exits)
    echo "=== 退出原因分布 ==="
    sqlite3 "$DB_PATH" "SELECT 
      exit_reason,
      COUNT(*) as count,
      ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM shadow_pnl WHERE closed=1), 1) as pct
    FROM shadow_pnl 
    WHERE closed=1 
    GROUP BY exit_reason 
    ORDER BY count DESC 
    LIMIT 10;"
    ;;
  
  mc)
    echo "=== MC分层表现 ==="
    sqlite3 "$DB_PATH" "SELECT 
      CASE 
        WHEN entry_mc < 10000 THEN '0-10K'
        WHEN entry_mc < 20000 THEN '10-20K'
        WHEN entry_mc < 30000 THEN '20-30K'
        ELSE '30K+'
      END as mc_range,
      COUNT(*) as trades,
      ROUND(100.0 * SUM(CASE WHEN exit_pnl > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as win_rate,
      ROUND(AVG(exit_pnl), 1) as avg_pnl
    FROM shadow_pnl 
    WHERE closed=1 
    GROUP BY mc_range;"
    ;;
  
  top)
    echo "=== Top 10 最佳交易 ==="
    sqlite3 -header "$DB_PATH" "SELECT 
      symbol, score, ROUND(exit_pnl, 1) as pnl, exit_reason
    FROM shadow_pnl 
    WHERE closed=1 
    ORDER BY exit_pnl DESC 
    LIMIT 10;"
    ;;
  
  moon)
    echo "=== MOON_STOP交易 ==="
    sqlite3 -header "$DB_PATH" "SELECT 
      symbol, ROUND(exit_pnl, 1) as pnl, ROUND(high_pnl, 1) as peak, exit_reason
    FROM shadow_pnl 
    WHERE closed=1 AND exit_reason LIKE 'MOON_STOP%'
    ORDER BY high_pnl DESC;"
    ;;
  
  *)
    echo "用法: $0 {stats|exits|mc|top|moon}"
    echo ""
    echo "  stats  - 总体统计"
    echo "  exits  - 退出原因分布"
    echo "  mc     - MC分层表现"
    echo "  top    - Top 10最佳交易"
    echo "  moon   - MOON_STOP交易"
    ;;
esac
SCRIPT

chmod +x /Users/boliu/sentiment-arbitrage-system/scripts/query-shadow.sh
```

### 使用快速查询脚本
```bash
cd /Users/boliu/sentiment-arbitrage-system

# 查看总体统计
./scripts/query-shadow.sh stats

# 查看退出原因
./scripts/query-shadow.sh exits

# 查看MC分层
./scripts/query-shadow.sh mc

# 查看最佳交易
./scripts/query-shadow.sh top

# 查看MOON_STOP
./scripts/query-shadow.sh moon
```

---

## 八、数据摘要

### 核心数据
```
总交易数：304笔（已关闭）
胜率：53.3%（162胜/142负）
平均PnL：+19.89%
总PnL：+6045.95%
```

### MC分层表现
```
MC 0-10K:   61笔, 75.4%胜率, +50.5%平均PnL ⭐⭐⭐
MC 10-20K: 131笔, 53.4%胜率, +17.8%平均PnL ⭐⭐
MC 20-30K:  49笔, 42.9%胜率, +10.3%平均PnL ⭐
MC 50K+:    48笔, 33.3%胜率,  -2.7%平均PnL ❌
```

### 退出原因Top 5
```
1. STOP_LOSS:        71笔 (23.4%), -20.0%平均
2. TAKE_PROFIT_50:   58笔 (19.1%), +93.2%平均
3. FAST_STOP:        53笔 (17.4%), -10.6%平均
4. MID_STOP:         18笔 ( 5.9%), -13.8%平均
5. TRAIL_STOP系列:   70笔 (23.0%), +13.1%平均
```

### MOON_STOP明星交易
```
峰值+1336% → 实际收益+128.6%
峰值+297%  → 实际收益+196.4%
峰值+147%  → 实际收益+39.0%
峰值+140%  → 实际收益+54.1%
```
