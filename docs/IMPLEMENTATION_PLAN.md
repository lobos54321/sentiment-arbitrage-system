# 实施计划 - Sentiment Arbitrage System MVP 2.0

## 已完成 ✅

### Phase 0: 项目基础架构
- [x] 创建项目目录结构
- [x] 创建核心配置文件 `system.config.json`
- [x] 创建频道管理文件 `channels.csv`
- [x] 创建 package.json 与依赖定义
- [x] 设计完整数据库schema（8张表）
- [x] 实现数据库初始化脚本 `init-db.js`
- [x] 创建 .env.example 模板
- [x] 编写完整README文档

### Phase 1A: 数据输入层
- [x] 实现 TelegramSignalParser
  - [x] 消息解析（CA/chain/symbol/MC提取）
  - [x] 推广频道提取与去重
  - [x] 派生指标计算（tg_ch_5m/15m/60m, velocity, accel）
  - [x] Cluster估算（反Matrix检测基础）
  - [x] 数据库持久化

## 进行中 🚧

### Phase 1B: 链上数据获取 (当前任务)

需要实现两个chain-specific模块：

#### src/inputs/chain-snapshot-sol.js
```javascript
功能清单:
□ 连接Solana RPC / Helius API
□ 获取Token Mint信息
  □ freeze_authority (Enabled/Disabled/Unknown)
  □ mint_authority (Enabled/Disabled/Unknown)
□ 获取LP状态
  □ Raydium Pool查询
  □ LP Token是否Burned
  □ LP Lock信息（平台/时长/proof）
□ 获取池子流动性（SOL或USD）
□ Top10持仓分析
  □ 获取所有holder
  □ 剔除Raydium/曲线/交易所地址
  □ 计算Top10百分比
□ 滑点测试
  □ 按仓位大小模拟卖出
  □ 使用Jupiter Quote API
  □ 记录sell_20pct滑点
□ Wash Trading检测（启发式）
  □ 高频对倒识别
  □ 返回 LOW/MEDIUM/HIGH
□ Key Risk Wallets识别
  □ 早期大额买入
  □ 新钱包持仓>3%
```

#### src/inputs/chain-snapshot-bsc.js
```javascript
功能清单:
□ 连接BSC RPC / BscScan API
□ Honeypot检测
  □ 调用GoPlus API或自建模拟
  □ 返回 PASS/FAIL/Unknown
□ Tax检测
  □ buy_tax, sell_tax, is_mutable
  □ 检查setTax/setFee等函数
□ Owner分析
  □ owner地址类型判断
    □ 0x00000...000 → Renounced
    □ MultiSig合约 → MultiSig
    □ TimeLock合约 → TimeLock
    □ 普通EOA → EOA
  □ Owner权限检查
□ Dangerous Functions检测
  □ blacklist/whitelist
  □ setMarketingFee/setTax（无上限）
  □ tradingEnabled可控
  □ cooldown/maxSell/maxWallet
□ LP Lock验证
  □ PinkSale/Unicrypt/DxSale查询
  □ 锁定时长/比例/proof
□ 获取流动性（BNB或USD）
□ 获取24h交易量
□ Top10持仓（剔除池子/死地址）
□ 卖出限制检测
```

#### 数据聚合器 src/inputs/chain-snapshot.js
```javascript
功能:
□ 统一接口，根据chain调用对应模块
□ 返回标准化数据结构
□ 处理Unknown情况（API失败/超时）
□ 缓存机制（避免重复查询）
```

## 待完成 📋

### Phase 1C: X轻量验证模块
- [ ] src/inputs/x-validator.js
  - [ ] Twitter API集成（Bearer Token）
  - [ ] 搜索最早提及时间
  - [ ] 统计15分钟内独立作者数
  - [ ] 检测Tier1 KOL提及
  - [ ] 数据写入social_snapshots表

### Phase 2: Gate过滤层

#### Phase 2A: Hard Gate
- [ ] src/gates/hard-gate.js
  - [ ] SOL Hard Gate实现
    - [ ] Freeze/Mint检查
    - [ ] LP状态验证
    - [ ] 返回 PASS/GREYLIST/REJECT + reasons
  - [ ] BSC Hard Gate实现
    - [ ] Honeypot检查
    - [ ] Tax验证（≤5% + 不可变）
    - [ ] Owner安全等价条件
    - [ ] LP Lock验证
    - [ ] Dangerous Functions检查
  - [ ] Unknown处理逻辑
  - [ ] 数据写入gates表

#### Phase 2B: Exit Gate
- [ ] src/gates/exit-gate.js
  - [ ] 按仓位测滑点核心逻辑
    - [ ] 从决策矩阵获取planned_position
    - [ ] 计算sell_test_amount = 20% * position
    - [ ] 执行滑点测试
  - [ ] SOL Exit Gate
    - [ ] 流动性检查（≥50 SOL）
    - [ ] Top10检查（<30%）
    - [ ] Wash Flag组合判断
  - [ ] BSC Exit Gate
    - [ ] 流动性检查（≥100 BNB）
    - [ ] 交易量检查（≥$500k）
    - [ ] Top10检查（<40%）
  - [ ] 数据写入gates表

### Phase 3: Soft Alpha Score引擎

#### Phase 3A: 分项计算模块
- [ ] src/scoring/narrative.js (0-25分)
  - [ ] 热点关键词库与匹配
  - [ ] X事件锚点检测
  - [ ] 拥挤度惩罚计算
  - [ ] 返回分数 + reasons

- [ ] src/scoring/influence.js (0-25分)
  - [ ] TG频道Tier加权（TierA加分）
  - [ ] X Tier1 KOL检测
  - [ ] 黑名单频道扣分
  - [ ] 返回分数 + reasons

- [ ] src/scoring/tg-spread.js (0-30分) **核心**
  - [ ] 数量分计算（tg_ch_15m阈值）
  - [ ] 独立性分（Tier加权 + cluster去重）
  - [ ] Matrix Penalty检测（强制）
    - [ ] tg_ch_15m ≥8 但 tg_clusters_15m ≤2 → -20
    - [ ] 1-2分钟内同步投放 → -10~-20
  - [ ] 返回分数 + reasons + penalty

- [ ] src/scoring/graph.js (0-10分)
  - [ ] 上游性分析（lead_time历史）
  - [ ] TG与X同步升温检测
  - [ ] 返回分数 + reasons

- [ ] src/scoring/source.js (0-10分)
  - [ ] 计算time_lag（取TG/X最小值）
  - [ ] 按阈值打分（<5min→10, 5-15→5, >20→0）
  - [ ] 返回分数 + reasons

#### Phase 3B: 总分聚合
- [ ] src/scoring/soft-score.js
  - [ ] 调用各分项模块
  - [ ] 按权重计算总分
  - [ ] 应用X验证调节（<2 authors → ×0.8）
  - [ ] 应用Matrix Penalty
  - [ ] 数据写入score_details表
  - [ ] 返回总分 + breakdown

### Phase 4: 决策与执行

#### Phase 4A: 决策矩阵
- [ ] src/decision/decision-matrix.js
  - [ ] 读取Hard/Exit Gate状态
  - [ ] 读取Soft Score
  - [ ] 匹配决策规则表
  - [ ] 返回：rating + action + position_tier
  - [ ] GREYLIST强制禁止Auto Buy

#### Phase 4B: 仓位计算
- [ ] src/decision/position-sizer.js
  - [ ] 根据position_tier查询config
  - [ ] 应用Score调节（如A级×0.7）
  - [ ] 检查全局限额（max_concurrent等）
  - [ ] 返回最终仓位大小

#### Phase 4C: 冷却与重复控制
- [ ] src/utils/state-manager.js
  - [ ] 同Token 30分钟冷却检查
  - [ ] 同叙事并发数检查（1h内≤3）
  - [ ] 同来源24h Reject比检查
  - [ ] 使用system_state表

#### Phase 4D: GMGN执行器
- [ ] src/execution/gmgn-executor.js
  - [ ] GMGN API集成
  - [ ] SOL买入（Anti-MEV + Smart Priority）
  - [ ] BSC买入（滑点10-20%）
  - [ ] 追高检查（5分钟内+50% → 不市价追）
  - [ ] 交易记录写入trades表

### Phase 5: 持仓监控与退出

- [ ] src/execution/monitor.js
  - [ ] 每1-3分钟轮询所有持仓
  - [ ] 监控指标：
    - [ ] TG: tg_ch_15m, tg_accel
    - [ ] 链上: Key Risk Wallet变化, Top10变化
    - [ ] 退出可行性: 滑点是否恶化
  - [ ] 三类退出触发（优先级）：
    1. [ ] 风控撤退（Key Wallet dump/滑点恶化）
    2. [ ] 情绪衰减（tg_accel转负/15分钟无新增）
    3. [ ] 标准SOP（+50%/+100%/120分钟）
  - [ ] 调用GMGN执行卖出
  - [ ] 更新trades表（exit_times/prices/pnl）

### Phase 6: 复盘与优化

#### Phase 6A: 每周优化脚本
- [ ] scripts/weekly-optimize.js
  - [ ] 频道Tier动态更新
    - [ ] 计算30-120min期望值
    - [ ] 升级/降级Tier
    - [ ] 更新channel_performance表
  - [ ] Matrix黑名单更新
    - [ ] 检测高同步投放且胜率差
    - [ ] 加入黑名单或降权
  - [ ] 阈值校准
    - [ ] 用历史分布调整tg_ch_15m阈值
    - [ ] Score分界线优化

#### Phase 6B: 回测框架
- [ ] scripts/backtest.js
  - [ ] 读取历史signal数据
  - [ ] 模拟完整决策流程
  - [ ] 计算假设PnL
  - [ ] 生成回测报告
  - [ ] 写入backtest_runs表

#### Phase 6C: 影子模式
- [ ] 24小时影子运行
  - [ ] shadow_mode flag检查
  - [ ] 记录所有决策但不执行
  - [ ] 验证Score与2h表现相关性
  - [ ] 生成验证报告

### Phase 7: 测试与上线

#### Phase 7A: 单元测试
- [ ] tests/telegram-parser.test.js
- [ ] tests/hard-gate.test.js
- [ ] tests/exit-gate.test.js
- [ ] tests/soft-score.test.js
- [ ] tests/decision-matrix.test.js

#### Phase 7B: 集成测试
- [ ] tests/integration/full-pipeline.test.js
  - [ ] 端到端流程测试
  - [ ] 边界情况测试
  - [ ] Unknown处理测试
  - [ ] GREYLIST行为测试

#### Phase 7C: 小资金上线
- [ ] 仅Small/Normal档
- [ ] 人工确认模式（Auto Buy=false）
- [ ] 累计50+样本
- [ ] 分析实际表现vs预测

#### Phase 7D: 全功能上线
- [ ] Auto Buy开启
- [ ] Max仓位启用
- [ ] 每周优化启用
- [ ] 监控告警系统

## 开发优先级

### P0 - 核心路径（可交付最小系统）
1. ✅ 数据库初始化
2. ✅ TG Parser
3. **Chain Snapshot（SOL + BSC）** ← 当前任务
4. Hard Gate（SOL + BSC）
5. Exit Gate（按仓位测滑点）
6. Soft Score（至少TG_Spread核心）
7. Decision Matrix
8. GMGN Executor（买入）
9. Position Monitor（基础卖出）

完成P0可进入**影子模式验证**。

### P1 - 完整功能
10. X Validator
11. 完整Soft Score（所有5个分项）
12. Matrix Penalty完整检测
13. 冷却与重复控制
14. 完整退出策略（三类）
15. 每周优化脚本

### P2 - 增强与优化
16. 回测框架
17. 单元测试覆盖
18. 性能优化（缓存/并发）
19. 监控告警系统
20. Web Dashboard（可选）

## 时间估算（单人开发）

- **P0核心路径**: 5-7天（假设每天6小时投入）
  - Chain Snapshot: 1.5天
  - Hard/Exit Gate: 1.5天
  - Soft Score核心: 1天
  - Decision + GMGN: 1天
  - Monitor: 1天
  - 集成调试: 0.5-1天

- **P1完整功能**: +3-4天
- **P2增强**: +2-3天

**总计**: 2-3周可完成可交付版本。

## 下一步行动

### 立即开始（今天）
1. 实现 `chain-snapshot-sol.js`
   - 优先: Freeze/Mint/LP检查
   - 然后: 流动性/Top10/滑点
2. 实现 `chain-snapshot-bsc.js`
   - 优先: Honeypot/Tax/Owner
   - 然后: LP Lock/Dangerous Functions

### 本周目标
- 完成P0核心路径
- 进入24小时影子模式
- 验证基础逻辑正确性

### 下周目标
- 补齐P1功能
- 开始小资金测试
- 收集真实样本

## 需要的外部资源

### API Keys（按优先级）
1. **必需**:
   - Telegram Bot Token
   - Solana RPC（免费可用）
   - BSC RPC（免费可用）

2. **强烈推荐**:
   - DexScreener API（提高可靠性）
   - Helius API（Solana高级功能）
   - GoPlus Security API（BSC安全检测）

3. **可选**:
   - Twitter Bearer Token（X验证）
   - BscScan API（合约验证）
   - GMGN API（如有交易量可申请）

### 开发工具
- Node.js ≥18
- SQLite3（better-sqlite3）
- 代码编辑器（VSCode推荐）
- Git（版本控制）

## 风险与缓解

### 技术风险
- **API不稳定**: 多源fallback + 缓存
- **数据缺失**: Unknown处理策略 + GREYLIST
- **性能问题**: 缓存 + 限流 + 并发控制

### 业务风险
- **Matrix矩阵盘**: Matrix Penalty强制检测
- **Rug Pull**: Hard Gate + Exit Gate双重保险
- **流动性陷阱**: 按仓位测滑点 + Top10检查

### 执行风险
- **GMGN延迟**: 追高检查 + 冷却控制
- **卖不出去**: 实时滑点监控 + 风控撤退优先

## 成功指标

### 影子模式验证（24小时）
- Score ≥80的信号，30-120min内PnL > 0的比例 ≥60%
- Score <40的信号，避免率 ≥90%
- GREYLIST误判率 <10%

### 小资金测试（1周，50单）
- 总PnL > 0
- 最大单笔亏损 < 30%
- Rug/无法退出率 < 5%

### 全功能运行（1个月）
- 月度ROI > 20%
- 胜率 > 55%
- Sharpe Ratio > 1.5
- 最大回撤 < 25%
