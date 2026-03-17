# Sentiment Arbitrage System — 统一审计报告（最终版 v3）

> 日期: 2026-03-17
> 来源: 手动审查 + 3个并行审计 Agent + 用户独立核查 + 竞争对手交叉验证
> 经过多轮验证去噪后的最终清单

---

## 🔴🔴 CRITICAL-0 — 架构级缺陷（比所有 bug 都严重）

这三个问题说明：**风控系统不是"有 bug"，是压根没接上生产流程。**

### CR-0A: RiskManager 完全未接入生产流程 — 孤岛代码
- **文件**: `risk-manager.js` (导出) vs 整个 `src/` (无 import)
- **证据**: `grep -rn "import.*RiskManager" src/` → 零结果。`grep -rn "new RiskManager" src/` → 零结果。`canTrade()`、`recordTradeResult()`、`checkRiskLimits()` 从未被任何代码调用。
- **影响**: CR-1/CR-2/CR-3 三个风控 bug 的修复**毫无意义** — 即使全修好，调用路径根本不存在。系统在完全没有风控的状态下运行。
- **修复**: 在 `premium-signal-engine.js` 买入判断前加 `riskManager.canTrade()` 调用，在 `live-position-monitor.js` 的 `_closePosition` 后调用 `riskManager.recordTradeResult()`。

### CR-0B: RiskManager 查的是错误的表 — 新旧系统脱节
- **文件**: `risk-manager.js:262` → 查 `positions` 表 vs `live-position-monitor.js` → 写 `live_positions` 表
- **证据**: 旧管道 `index.js` 的 `processSignal` 写入 `positions` 表。生产管道 `premium-signal-engine.js` → `live-position-monitor.js` 写入 `live_positions` 表。`risk-manager.js` 查的是旧表 `positions`。字段也不同：`positions` 有 `exit_time`，`live_positions` 用 `closed_at`。
- **影响**: 即使接入 RiskManager，它读到的也是旧系统的过期数据，所有基于 DB 的风控判断（日亏损、胜率、持仓数）都指向错误数据源。
- **修复**: RiskManager 所有 SQL 改为查 `live_positions` 表，字段对齐（`exit_time` → `closed_at`）。

### CR-0C: async EventEmitter + 无全局兜底 = 进程崩溃
- **文件**: `live-position-monitor.js:105, 359`
- **证据**: `_onPriceUpdate` 是 `async` 函数，通过 `this.priceMonitor.on('price-update', this._onPriceUpdate)` 绑定。Node.js EventEmitter 的 `emit()` 是同步的，不 await async listener 返回的 Promise。异常变成 unhandled promise rejection。全局无 `process.on('unhandledRejection')` 处理器。Node.js 15+ 默认行为：`process.exitCode = 1` → **整个进程退出**。
- **影响**: 不是"停止监控"（原 CR-5 评估），而是**整个交易系统进程崩溃**，所有仓位同时完全失控。
- **修复**: ① `_onPriceUpdate` 加顶层 try-catch；② `src/index.js` 加全局 `process.on('unhandledRejection')` 兜底。

---

## 🔴 CRITICAL — 代码级 bug（接入后需修复）

> 以下 bug 在 RiskManager 接入（CR-0A）并查正确的表（CR-0B）后才有实际意义。

### CR-1: Circuit Breaker 被注释，连亏 8 笔不暂停
- **文件**: `risk-manager.js:536`
- **证据**: `// pauseTrading();` 被注释。
- **前提**: 需先完成 CR-0A（接入）才生效。
- **修复**: 用 `process.env.SHADOW_MODE` 判断，替代注释。

### CR-2: 日亏损上限读 undefined，限额永远不触发
- **文件**: `risk-manager.js:237` vs `strategy.js:473`
- **证据**: config 定义 `DAILY_LOSS_LIMIT: { SOL: 0.5 }`，代码读 `this.params.DAILY_LOSS_LIMIT_SOL`（不存在）→ `undefined`。
- **前提**: 需先完成 CR-0A + CR-0B。
- **修复**: 展平 config — `DAILY_LOSS_LIMIT_SOL: 0.5`。

### CR-3: 日亏损 SQL 类型错误，统计永远为 0
- **文件**: `risk-manager.js:264`
- **证据**: `exit_time` 是 Unix ms 整数，SQL 用 `date()` 返回字符串。SQLite 类型比较失败。
- **前提**: 需先完成 CR-0B（改查 `live_positions` 表，用 `closed_at` 字段）。
- **修复**: 与 CR-0B 合并修复 — 改查 `live_positions`，用正确的时间字段和类型。

### CR-4: SOL 余额竞态 — 多仓并发退出时 PnL 记录错误
- **文件**: `jupiter-ultra-executor.js:163, 185`
- **证据**: `solAfter - solBefore` 在多仓同时退出时互相污染。
- **修复**: 优先使用 Jupiter Ultra API 返回的 `outAmount`。

---

## 🟠 HIGH — 确认属实

### HI-1: TP4 卖出失败后仓位永远无法退出
- **文件**: `live-position-monitor.js:541-586`
- **问题**: TP4 卖出失败 → `pos.tp4=false` → moonbag 不激活。所有超时保护都要求 `!pos.tp1`，TP1+ 仓位没有超时兜底。
- **影响**: 仓位永远无法关闭，资金永久套牢。
- **修复**: TP3+ 仓位加超时兜底（如 2h 无成交则强制退出）。

### HI-2: `getRecentStats()` 双重 bug — 字段错 + 类型错
- **文件**: `risk-manager.js:662`
- **证据**: `created_at > strftime('%s', 'now', '-7 days')` — `created_at` 是 ISO 字符串，`strftime('%s')` 是整数。SQLite 字符串恒大于整数 → 全历史记录都匹配。且用 `created_at`（开仓时间）而非 `exit_time`（平仓时间），包含未平仓记录。
- **影响**: 7天胜率实际是全历史胜率。`checkRiskLimits()` 用此数据触发 defensive mode，可能误开或误关。
- **前提**: 同样需要 CR-0B 改查 `live_positions` 表。

### HI-3: 链上卖出成功但 DB 未标记关闭 → 重启后 ghost position
- **文件**: `live-position-monitor.js:857-984, 993-1015`
- **问题**: 卖出成功后崩溃，DB 里 `status='open'`，重启后再次尝试卖出已卖掉的 token。
- **影响**: 钱包余额为 0 时标记 `MANUAL_SOLD`，PnL 不记录。

### HI-4: 钱包扫描 SOL 估算公式错误
- **文件**: `live-position-monitor.js:211`
- **问题**: `entry_sol / token_amount`，`token_amount` 带 decimals（如 `1e9`），结果≈0，fallback `0.001` SOL。
- **修复**: 用 `entry_sol / (token_amount / 10^decimals)` 或链上 SOL 余额差。

### HI-5: `checkDangerousFunctions` null → [] 误判安全
- **文件**: `hard-gates.js:394`
- **问题**: `null` 被 `|| []` 转为空数组，当作"无危险函数"通过。
- **修复**: null 时返回 `unknown: true`。

### HI-6: signalHistory Map 无限增长 — 内存泄漏
- **文件**: `premium-signal-engine.js:59`
- **修复**: 每小时清理 >24h 且未产生交易的条目。

### HI-7: JSON 持久化非原子写入 + 同步阻塞事件循环
- **文件**: `dead-dog-pool.js:363`, `cooldown-manager.js:199`, `premium-signal-engine.js:681`
- **问题**: 原子性 — `writeFileSync` 直接覆写，崩溃时半写。阻塞性 — 同步写入阻塞事件循环。
- **修复**: `await writeFile(path + '.tmp', data)` → `await rename(tmp, path)`。

### HI-8: WatchEntry 触发即删 — 买入失败无法重试
- **文件**: `watch-entry-monitor.js:193`
- **修复**: 买入成功后再 remove。

### HI-9: Dashboard API 多端点无鉴权
- **文件**: `dashboard-server.js` 多处
- **修复**: 所有 API 路由统一加 `checkAuth`。

### HI-10: ATH 信号 mcTo=0 时 fallback 到 mcFrom
- **文件**: `premium-channel-listener.js:252-266`
- **影响**: 可能在 MC 已超出范围时仍通过过滤。

---

## 🟡 MEDIUM — 确认属实

### ME-1: `stop_loss_percent: 40` 硬编码（实际 SL 是 35%）
- **文件**: `premium-signal-engine.js:454`
- **影响**: Shadow 回测数据 SL 标注错误。

### ME-2: ATH Count 在 anti-chase 拒绝时不递增
- **文件**: `premium-signal-engine.js:352-360, 436-444`
- **影响**: 同一 token 被 anti-chase 拒绝后，下次仍被视为 ATH#1。

### ME-3: ATH 计数异步写入未 await — 崩溃时状态丢失
- **文件**: `premium-signal-engine.js:681`
- **问题**: `fs.writeFile(callback)` 未 await，内存已更新，磁盘未写完。与 HI-7 原子性是不同维度 — 这是 async/sync 混用的逻辑竞态。
- **修复**: `_saveAthCounts()` 改为 async/await。

### ME-4: Defensive mode ALLOWED_TIERS 不挡 TIER_C
- **文件**: `risk-manager.js:346-354`
- **影响**: 配置 `ALLOWED_TIERS: ['TIER_A']` 但 TIER_C 被显式放行。

### ME-5: 信号去重 `recentSymbols.set()` 在 `addToken()` 失败时仍标记已处理
- **文件**: `premium-signal-engine.js:524-525`
- **影响**: 有限 — 异常路径窄，但一旦发生该 token 永久被跳过。

### ME-6: retryCounter 纯内存，重启后退避丢失
- **文件**: `live-position-monitor.js:32`
- **影响**: 有限 — DB `scan_retry_count` 限制总次数。

### ME-7: Circuit Breaker 标志位纯内存，重启后丢失
- **文件**: `risk-manager.js:163-175`
- **问题**: `_lastCircuitBreakerTime` 丢失导致 4 小时恢复期从零计时。
- **修复**: 持久化到 DB。

### ME-8: `_onPriceUpdate` 异常后脏状态
- **文件**: `live-position-monitor.js:359+`
- **问题**: CR-0C 修复加 try-catch 后的衍生问题 — 异常中断的操作可能留下 `partialSellInProgress = true`。
- **修复**: catch 里清理标志位。

### ME-9: 钱包扫描 SOL 估算公式错误（同 HI-4）
- **文件**: `live-position-monitor.js:211`

### ME-10: soldPct 浮点精度累积误差
- **文件**: `live-position-monitor.js:766-770`
- **影响**: cosmetic — 不影响实际交易。

---

## 🟢 LOW — 代码卫生

### LO-1: 无 WAL mode — 多连接同一 DB 时写阻塞读
### LO-2: `tokenData` 未定义（旧管道 `index.js:490`，生产不走该路径）
### LO-3: `SignalSnapshotRecorder.recordPassed()` 每次调用都执行 CREATE TABLE
### LO-4: Graceful shutdown 不等待 in-flight 卖出
### LO-5: `exit-cooldown.js:81-87` 模板字符串拼接 SQL（当前安全但脆弱）

---

## ❌ 已排除（误报）

| 编号 | 描述 | 排除原因 |
|------|------|---------|
| C4（旧） | Partial Sell 竞态（2秒窗口重复 TP1） | JS 单线程，`partialSellInProgress = true` 在第一个 `await` 之前同步设置 |
| C5（旧） | 崩溃后 TP 重复触发 | TP 卖出成功后立即持久化，窗口极小 |
| H1（旧） | 仓位槽位计算 `!p.tp1` 反了 | 逻辑正确 — `!p.tp1` 过滤未触发 TP1 的在险仓位 |
| H11（旧） | emergencySell 用过时 tokenAmount | 调用方传入的是 `getTokenBalance()` 的实时值 |
| H15（旧） | 买入前不查余额 | `premium-signal-engine.js:478` 有余额检查 |
| N3 | `_getCachedMC` priceCache 无空值保护 | `priceCache` 在构造函数中初始化为 `new Map()` |
| M1-用户版 | Regex 7指标全部失败 | 每个指标独立匹配，返回部分结果 |

---

## 统计

| 级别 | 数量 |
|------|------|
| 🔴🔴 CRITICAL-0（架构级） | 3 |
| 🔴 CRITICAL（代码级） | 4 |
| 🟠 HIGH | 10 |
| 🟡 MEDIUM | 10 |
| 🟢 LOW | 5 |
| ❌ 误报排除 | 7 |
| **总计（有效）** | **32** |

---

## 修复计划（按依赖关系排序）

### 第零步（地基）— 接线 + 防崩溃
> 不做这一步，后面所有修复都是空中楼阁。

| 编号 | 改动 | 行数 |
|------|------|------|
| CR-0C | `_onPriceUpdate` 加顶层 try-catch + `index.js` 加 `process.on('unhandledRejection')` | ~15 行 |
| CR-0A | `premium-signal-engine.js` 买入前调 `riskManager.canTrade()`，`live-position-monitor.js` 平仓后调 `riskManager.recordTradeResult()` | ~20 行 |
| CR-0B | RiskManager 所有 SQL 改查 `live_positions` 表，字段对齐 | ~30 行 |

### 第一步（风控生效）— 接线完成后修 bug
| 编号 | 改动 | 行数 |
|------|------|------|
| CR-1 | `pauseTrading()` 用 `SHADOW_MODE` 环境变量判断 | ~5 行 |
| CR-2 | 展平 config key `DAILY_LOSS_LIMIT_SOL: 0.5` | ~3 行 |
| CR-3 | SQL 时间字段和类型修复（合并到 CR-0B） | 已含 |
| HI-2 | `getRecentStats` 改用 `closed_at` + 类型修复（合并到 CR-0B） | 已含 |

### 第二步（数据正确性）
| 编号 | 改动 | 行数 |
|------|------|------|
| CR-4 | sell() 优先用 Jupiter `outAmount` | ~5 行 |
| HI-4 | 估算公式修正 | ~3 行 |
| HI-5 | null guard 返回 `unknown: true` | ~3 行 |

### 第三步（防止仓位卡死）
| 编号 | 改动 | 行数 |
|------|------|------|
| HI-1 | TP3+ 仓位加超时兜底 | ~10 行 |
| HI-3 | 卖出前写中间状态 `status='selling'` | ~8 行 |
| ME-8 | try-catch 里清理脏状态标志位 | ~5 行 |

### 第四步（稳定性改进）
| 编号 | 改动 | 行数 |
|------|------|------|
| HI-6 | signalHistory 定期清理 | ~10 行 |
| HI-7 + ME-3 | JSON 原子异步写入 + ATH await | ~20 行 |
| HI-8 | WatchEntry 延迟删除 | ~5 行 |

### 第五步（安全和防御）
| 编号 | 改动 | 行数 |
|------|------|------|
| HI-9 | Dashboard API 统一加认证 | ~10 行 |
| ME-7 | CB 标志位持久化到 DB | ~15 行 |
| ME-4 | TIER_C 处理逻辑对齐配置 | ~3 行 |

---

## 一句话总结

**系统的风控模块是一座孤岛** — 写好了但从未接入生产交易流程。所有信号分析、买卖操作都绕过了 RiskManager。即使 RiskManager 内部的 bug（连亏不暂停、日限额失效、SQL 类型错误）全部修好，也不会有任何效果，因为没有代码调用它。修复计划的第一步不是修 bug，而是**接线**。
