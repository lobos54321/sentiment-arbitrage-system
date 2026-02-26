# GMGN 自动化交易配置指南

本系统支持两种 GMGN 交易执行方案，推荐使用 **Telegram Bot 模式**（方案 A）。

---

## 🚀 方案 A：Telegram Bot 模式（推荐）

### 优势
- ✅ **无需 API 白名单**：不需要申请 GMGN API 权限
- ✅ **Anti-MEV 保护**：GMGN Bot 内置防夹功能
- ✅ **私钥托管**：由 GMGN 管理私钥，避免本地风险
- ✅ **实现简单**：配置快速，易于维护
- ✅ **即时生效**：无需等待审核

### 缺点
- ⚠️ **中心化托管**：需要信任 GMGN 平台
- ⚠️ **资金安全**：不建议在 Bot 钱包存放大量资金（建议 < 10 SOL 或 1 BNB）
- ⚠️ **有限控制**：依赖 GMGN Bot 的功能和限制

---

## 📋 配置步骤（方案 A）

### 步骤 1：配置 GMGN Telegram Bot

#### 1.1 启动 GMGN Bot
在 Telegram 中搜索并启动：
- **Solana**: `@GMGN_sol_bot`
- **BSC**: `@GMGN_bsc_bot`

发送 `/start` 开始使用。

#### 1.2 充值钱包
1. 点击 Bot 中的 "💰 Wallet" 按钮
2. 复制你的专属充值地址
3. 转入资金（建议测试阶段 0.5 SOL 或 0.05 BNB）

#### 1.3 开启 Auto Buy（自动买入）
1. 点击 "⚙️ Settings"
2. 找到 "Auto Buy" 选项并开启
3. 设置单笔买入金额：
   - **Solana**: 建议 0.1 - 0.5 SOL
   - **BSC**: 建议 0.01 - 0.05 BNB
4. 配置 Anti-MEV 保护（推荐开启）
5. 设置滑点容忍度：
   - **普通代币**: 1-2%
   - **新币/高波动**: 5-15%

#### 1.4 开启 Auto Sell（自动卖出，可选）
1. 在 Settings 中找到 "Auto Sell"
2. 设置止盈目标：
   - **保守**: +30% ~ +50%
   - **激进**: +100% ~ +200%
3. 设置止损线：
   - **建议**: -15% ~ -25%
4. 开启后，买入成功的代币会自动挂单，24 小时监控

---

### 步骤 2：获取 Telegram API Credentials

#### 2.1 申请 API ID 和 Hash
1. 访问 https://my.telegram.org/apps
2. 使用你的 Telegram 账号登录
3. 点击 "Create new application"
4. 填写应用信息：
   - **App title**: `GMGN Auto Trader`
   - **Short name**: `gmgn_bot`
   - **Platform**: `Other`
5. 提交后获得：
   - `api_id`（数字，如 12345678）
   - `api_hash`（字符串，如 `abcdef1234567890abcdef1234567890`）

#### 2.2 配置到 .env 文件
在项目根目录的 `.env` 文件中添加：

```bash
# GMGN Telegram Bot 配置
TELEGRAM_API_ID=12345678
TELEGRAM_API_HASH=abcdef1234567890abcdef1234567890
TELEGRAM_SESSION=  # 首次运行后会自动生成
```

---

### 步骤 3：首次运行和认证

#### 3.1 启动系统
```bash
npm start
```

#### 3.2 完成 Telegram 认证
首次运行时，系统会提示：
1. 输入你的 Telegram 手机号（带国家代码，如 `+8613800138000`）
2. 输入收到的验证码
3. 如果开启了两步验证，输入密码

#### 3.3 保存 Session
认证成功后，终端会显示一个 session string：
```
💾 Save this session string to .env as TELEGRAM_SESSION:
1BVtsOIQBu7cR... (很长的字符串)
```

将这个字符串复制到 `.env` 文件中：
```bash
TELEGRAM_SESSION=1BVtsOIQBu7cR...
```

**保存后，下次运行不需要再次输入手机号和验证码。**

---

### 步骤 4：测试交易流程

#### 4.1 影子模式测试（推荐先运行）
确保 `.env` 中：
```bash
SHADOW_MODE=true
```

这样系统会模拟交易，不会实际发送指令给 GMGN Bot。

#### 4.2 观察日志
系统会输出：
```
🎭 [GMGN Telegram] SHADOW MODE - Simulating buy
✅ [GMGN Telegram] Trade persisted: ID 1
```

#### 4.3 真实模式测试
确认逻辑无误后，修改 `.env`：
```bash
SHADOW_MODE=false
```

重新启动，系统将真实发送交易指令。

---

## 🔧 高级配置（可选）

### 优先级费用（Priority Fees）
在 GMGN Bot Settings 中调整：
- **正常行情**: Medium（默认）
- **抢新币/高波动**: High 或 Very High
- **成本**: 额外 0.001 - 0.01 SOL/tx

### MEV 保护
- **Anti-MEV**: 必须开启（防止 sandwich attack）
- **Jito Bundles**（Solana 专属）: 可选开启，进一步提升抗 MEV 能力

### 滑点策略
| 场景 | 建议滑点 |
|------|---------|
| 流动性好的代币 | 0.5% - 1% |
| 普通 Meme 币 | 2% - 5% |
| 新币抢跑 | 10% - 20% |
| 极端波动 | 最高 50%（谨慎！） |

---

## ⚠️ 安全建议

### 资金管理
1. **分散存储**：
   - Bot 钱包：仅存放交易用资金（< 10 SOL 或 1 BNB）
   - 冷钱包：存放大额资金和利润
2. **定期提现**：每天或每周将利润提取到个人钱包
3. **止损设置**：必须设置 Auto Sell 止损线（建议 -20%）

### 风险控制
1. **不要存放大额资金**：GMGN Bot 是中心化托管
2. **监控异常**：定期检查 Bot 交易记录
3. **备份 Session**：保存好 `TELEGRAM_SESSION` 字符串
4. **API ID 保密**：不要分享 `api_id` 和 `api_hash`

---

## 🆚 方案 B：GMGN API 直接调用（高级）

### 适用场景
- 需要高频交易（> 100 次/天）
- 需要完全自定义策略
- 愿意自行管理私钥
- 有 GMGN 平台交易历史

### 权限要求
1. **Access Token**: 在 GMGN 官网生成
2. **IP 白名单**: 提交服务器 IP、交易地址、邀请码
3. **交易活跃度**: 钱包需有真实交易记录
4. **速率限制**: 约 2 req/s

### 申请流程
1. 登录 https://gmgn.ai
2. 进入 Account Settings
3. 生成 API Token
4. 联系 GMGN 官方（Discord/Telegram）申请白名单
5. 提供：钱包地址、服务器 IP、用途说明

### 配置（方案 B 已实现，需要白名单后启用）
在 `.env` 中：
```bash
GMGN_API_KEY=your_api_key_here
GMGN_WALLET_ADDRESS=your_wallet_address
USE_GMGN_API=true  # 启用 API 模式
```

**注意**：方案 B 需要你自行管理私钥，风险更高。建议普通用户使用方案 A。

---

## 📊 监控和维护

### 查看交易记录
在 GMGN Bot 中：
1. 点击 "📊 Positions"：查看持仓
2. 点击 "📜 History"：查看历史交易

### 系统日志
系统会记录所有操作到控制台：
```
✅ [GMGN Telegram] Buy signal sent to @GMGN_sol_bot
✅ [GMGN Telegram] Trade persisted: ID 12
```

### 数据库查询
```bash
sqlite3 data/sentiment_arb.db "SELECT * FROM trades WHERE status='OPEN';"
```

---

## 🐛 常见问题

### Q1: 首次运行提示 "Missing TELEGRAM_API_ID"
**A**: 确保已在 https://my.telegram.org/apps 申请，并正确配置到 `.env`。

### Q2: 收不到 Telegram 验证码
**A**:
1. 检查手机号格式（需要带国家代码，如 `+86`）
2. 确保 Telegram 账号正常
3. 可能被限流，等待几分钟再试

### Q3: Bot 显示"余额不足"
**A**:
1. 检查 GMGN Bot 钱包余额
2. 调整 Auto Buy 金额设置
3. 充值更多资金到 Bot 钱包

### Q4: 交易没有执行
**A**:
1. 确认 Auto Buy 已开启
2. 检查 Bot 设置的最小流动性要求
3. 查看 GMGN Bot 返回的错误消息

### Q5: Session 过期
**A**: 重新运行系统，完成手机号验证，更新 `.env` 中的 `TELEGRAM_SESSION`。

---

## 📞 技术支持

- **GMGN 官方 Discord**: https://discord.gg/gmgn
- **Telegram 社群**: 搜索 GMGN 相关群组
- **文档**: https://docs.gmgn.ai/

---

## ✅ 配置检查清单

部署前请确认：

- [ ] 已在 GMGN Bot 中充值资金
- [ ] 已开启 Auto Buy 功能
- [ ] 已设置合理的买入金额
- [ ] 已开启 Anti-MEV 保护
- [ ] 已获取 Telegram API ID 和 Hash
- [ ] 已配置 `.env` 文件
- [ ] 已完成首次 Telegram 认证
- [ ] 已保存 Session String
- [ ] 已在影子模式测试
- [ ] 理解资金安全风险

完成以上步骤后，你的 GMGN 自动化交易系统即可投入使用！🎉
