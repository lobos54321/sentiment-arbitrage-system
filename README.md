# Sentiment Arbitrage System MVP 2.0

机构级链上情绪套利系统 - 捕捉30-120分钟情绪主升浪窗口

## 系统架构

```
Telegram信号源
      ↓
[TG Parser] → 提取token_ca + promoted_channels + 时间戳
      ↓
[Chain Snapshot] → 获取链上数据 (SOL/BSC)
      ↓
[X Validator] → 轻量验证 (可选)
      ↓
[Hard Gate] → 安全过滤 (PASS/GREYLIST/REJECT)
      ↓
[Exit Gate] → 可退出性过滤 (按仓位测滑点)
      ↓
[Soft Score] → 0-100分综合评分
      ↓
[Decision Matrix] → Score → 动作 + 仓位
      ↓
[GMGN Executor] → 快速买入/卖出
      ↓
[Position Monitor] → 持仓监控 + 风控撤退
      ↓
[Database] → 落库复盘 + 每周优化
```

## 快速开始

### 1. 安装

```bash
cd sentiment-arbitrage-system
npm install
```

### 2. 配置

复制环境变量模板：

```bash
cp .env.example .env
```

编辑 `.env` 填入必要配置：

```env
# Telegram
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_ADMIN_CHAT_ID=your_chat_id

# GMGN (if using API)
GMGN_API_KEY=your_gmgn_api_key
GMGN_WALLET_ADDRESS=your_wallet_address

# RPC Endpoints
SOLANA_RPC_URL=https://api.mainnet-beta.solana.com
BSC_RPC_URL=https://bsc-dataseed.binance.org

# API Keys (optional but recommended)
DEXSCREENER_API_KEY=
BIRDEYE_API_KEY=
HELIUS_API_KEY=
TWITTER_BEARER_TOKEN=

# Database
DB_PATH=./data/sentiment_arb.db
```

### 3. 初始化数据库

```bash
npm run db:init
```

### 4. 运行影子模式（24小时不下单，仅记录）

```bash
npm run shadow
```

### 5. 正式运行

```bash
npm start
```

## 核心功能

### Hard Gate（安全硬过滤）

**SOL:**
- ✅ Freeze Authority Disabled
- ✅ Mint Authority Disabled
- ✅ LP Burned 或 Locked >1个月

**BSC:**
- ✅ Honeypot Pass
- ✅ Tax ≤5% 且不可变
- ✅ Owner Renounced/MultiSig/TimeLock
- ✅ LP Locked >1个月
- ✅ 无危险函数 (blacklist/tradingEnabled等)

**结果**: PASS / GREYLIST / REJECT

### Exit Gate（可退出性过滤）

**按仓位测滑点**（关键）：
- 使用 `sell_test_amount = 20% * planned_position`
- SOL: 滑点 <2% Pass, >5% Reject
- BSC: liq≥100 BNB + vol≥$500k

**Top10持仓检查**：
- SOL: <30% Pass
- BSC: <40% Pass

**Wash Trading检测**：
- Low/Med: Pass
- High + 其他风险: Reject

### Soft Alpha Score (0-100)

```
Score = 0.25×Narrative + 0.25×Influence + 0.30×TG_Spread + 0.10×Graph + 0.10×Source
```

**TG_Spread（核心30分）**：
- 数量分 (0-15): tg_ch_15m ≥8 → 15分
- 独立性分 (0-15): TierA频道 + cluster去重
- **Matrix Penalty**: cluster≤2 且 ch≥8 → -20分

**Narrative (0-25)**：
- 热点关键词 (0-15)
- X事件锚点 (0-10)
- 拥挤度惩罚

**Influence (0-25)**：
- TG频道质量 (0-15): TierA加分
- X Tier1命中 (0-10)

**Graph (0-10)**: 上游性 + TG/X同步

**Source (0-10)**: 最早提及时间

**调节**：
- X验证: 若 x_authors<2 → Score×0.8
- Matrix Penalty: 最高可倒扣-20分

### 决策矩阵

| Gate状态 | Score | 评级 | 动作 | 仓位 |
|---|---:|---|---|---|
| PASS+PASS | ≥80 | S | Auto Buy | Max |
| PASS+PASS | 60-79 | A | Buy (确认) | Normal |
| PASS或GREYLIST | 40-59 | B | Watch/试错 | Small |
| ANY GREYLIST | 任意 | - | Watch only | Small |
| Score<40或REJECT | - | REJECT | Ignore | 0 |

**仓位档位**：
- SOL: Small 0.08 / Normal 0.15 / Max 0.5
- BSC: Small 0.008 / Normal 0.015 / Max 0.05

### 退出策略（优先级从高到低）

**1. 风控撤退（最高优先）**：
- Key Wallet持仓下降>10% → 清仓80-100%
- 滑点从<2%恶化到>5% → 立即清仓

**2. 情绪衰减止盈**：
- tg_accel由正转负 → 卖50%
- 连续15分钟无新增提及 → 清仓

**3. 标准SOP**：
- +50% → 卖30%
- +100% → 卖50%
- 持仓120分钟未达20% → 清仓

## Unknown处理策略

**Hard Gate关键字段Unknown**：
- SOL: Freeze/Mint/LP → GREYLIST
- BSC: Honeypot/Tax/Owner/LP → GREYLIST

**Exit Gate关键字段Unknown**：
- 池深度/滑点/Top10 → GREYLIST + 最多Small仓

**Soft Score辅助字段Unknown**：
- 对应分项封顶50%

**GREYLIST行为**：
- 禁止Auto Buy
- 只能人工确认后Small仓试错
- 必须补齐数据后才能重新评估

## 冷却与重复交易控制

- **同一Token**: 30分钟内禁止重复Auto Buy
- **同叙事**: 1小时内最多持仓3个同叙事token
- **同来源频道**: 24h内Reject比>50% → 降权/拉黑

## 数据库结构

### 核心表

**tokens**: token基础信息
**gates**: Hard/Exit Gate评估结果
**social_snapshots**: TG/X社交数据快照
**trades**: 交易记录与PnL跟踪
**score_details**: 评分明细（调试用）
**channel_performance**: 频道表现跟踪
**system_state**: 冷却时间等全局状态
**backtest_runs**: 回测/影子模式运行记录

## 复盘与优化

### 每周自动调参

```bash
npm run optimize
```

**优化内容**：
1. **频道Tier动态更新**: 按30-120min期望值升/降级
2. **Matrix黑名单**: 高同步投放且胜率差 → 降权/拉黑
3. **阈值校准**: 用历史分布调整tg_ch_15m阈值

### 回测

```bash
npm run backtest -- --start=2024-01-01 --end=2024-01-31
```

## 项目结构

```
sentiment-arbitrage-system/
├── src/
│   ├── index.js                 # 主入口
│   ├── inputs/
│   │   ├── telegram-parser.js   # TG信号解析
│   │   ├── chain-snapshot.js    # 链上数据获取
│   │   └── x-validator.js       # X轻量验证
│   ├── gates/
│   │   ├── hard-gate.js         # 安全硬过滤
│   │   └── exit-gate.js         # 退出性过滤
│   ├── scoring/
│   │   ├── soft-score.js        # Alpha评分引擎
│   │   ├── narrative.js         # 叙事分析
│   │   ├── influence.js         # 影响力分析
│   │   ├── tg-spread.js         # TG扩散分析
│   │   ├── graph.js             # 图谱分析
│   │   └── source.js            # 源头分析
│   ├── decision/
│   │   ├── decision-matrix.js   # 决策矩阵
│   │   └── position-sizer.js    # 仓位计算
│   ├── execution/
│   │   ├── gmgn-executor.js     # GMGN执行器
│   │   └── monitor.js           # 持仓监控
│   ├── database/
│   │   └── db.js                # 数据库操作
│   └── utils/
│       ├── config-loader.js     # 配置加载
│       ├── logger.js            # 日志系统
│       └── state-manager.js     # 状态管理（冷却等）
├── scripts/
│   ├── init-db.js               # 数据库初始化
│   ├── backtest.js              # 回测脚本
│   └── weekly-optimize.js       # 每周优化
├── config/
│   ├── system.config.json       # 系统配置
│   └── channels.csv             # 频道白/黑名单
├── data/
│   └── sentiment_arb.db         # SQLite数据库
├── tests/
│   └── *.test.js                # 单元测试
├── docs/
│   └── *.md                     # 详细文档
├── package.json
├── .env.example
└── README.md
```

## 上线检查清单

### 阶段0: 准备（已完成）
- [x] 项目结构创建
- [x] 配置文件创建
- [x] 数据库schema设计
- [x] 频道白名单初始化

### 阶段1: 核心开发（✅ 已完成）
- [x] TG Parser实现
- [x] Chain Snapshot模块 (SOL + BSC)
- [x] Hard Gate逻辑
- [x] Exit Gate逻辑
- [x] Soft Score引擎
- [x] Decision Matrix
- [x] GMGN Executor (Telegram Bot模式)
- [x] Position Monitor
- [x] 主程序集成和事件循环

### 阶段2: 测试验证
- [ ] 单元测试覆盖
- [ ] 24小时影子模式运行
- [ ] Score与2h内表现相关性验证
- [ ] 边界情况测试

### 阶段3: 小资金上线
- [ ] 仅Small/Normal档
- [ ] 人工确认模式
- [ ] 累计至少50个样本

### 阶段4: 全功能上线
- [ ] Auto Buy开启
- [ ] Max仓位启用
- [ ] 每周优化启用

## 风险警告

⚠️ **这是高风险交易系统**

- Meme币90%会归零
- 再高的评分也可能失败
- 市场会快速变化
- 只投闲钱

**建议**：
1. 从Small仓位开始
2. 严格执行止损
3. 定期复盘调整
4. 保持情绪稳定

## 技术支持

遇到问题：
1. 检查系统日志
2. 查看数据库记录
3. 验证配置文件
4. 确认API密钥有效

## 许可证

MIT License - 仅供学习研究使用
