# API 配置指南

本文档详细说明系统使用的所有 API，包括获取方式、配置方法和作用。

---

## 📊 API 优先级分类

### ✅ **免费且无需配置（已可用）**
1. DexScreener API
2. GoPlus Security API
3. Jupiter Quote API
4. Solana RPC (公共节点)
5. BSC RPC (Binance 官方节点)

### 🟢 **已配置（增强功能）**
1. ✅ Helius API - Solana 数据增强
2. ✅ Telegram Bot Token - 信号接收

### 🟡 **推荐配置（提升体验）**
1. Telegram API (for GMGN Bot) - 自动交易必需
2. BscScan API - BSC 合约验证（可选）

### 🔴 **可选配置（高级功能）**
1. Twitter Bearer Token - X 验证模块（P1 功能）
2. GMGN API Key - API 直接调用模式（需白名单）

---

## 📋 详细配置说明

### 1. **DexScreener API** ✅ 已可用

**用途：**
- 获取代币价格、流动性、交易量
- 计算价格变化（5min, 1h, 24h）
- 获取代币基础信息（symbol, name）

**端点：**
```
https://api.dexscreener.com/latest/dex/tokens/{tokenCA}
```

**配置：**
- ❌ **无需 API Key**（完全免费）
- ✅ 已在代码中集成

**速率限制：**
- 300 请求/分钟（token pair 端点）
- 我们的使用量：约 10-20 次/分钟

**数据返回示例：**
```json
{
  "pairs": [{
    "priceUsd": "0.000123",
    "liquidity": {"usd": 50000, "base": 100, "quote": 500},
    "volume": {"h24": 120000},
    "priceChange": {"m5": 2.5, "h1": 10.2, "h24": -5.3},
    "txns": {"h24": {"buys": 150, "sells": 120}}
  }]
}
```

---

### 2. **GoPlus Security API** ✅ 已可用

**用途（仅 BSC）：**
- Honeypot 检测
- 买卖税分析（buy_tax, sell_tax）
- 税率可修改性检测
- Owner 类型分析
- LP 锁定状态
- Top10 持仓分布

**端点：**
```
https://api.gopluslabs.io/api/v1/token_security/56?contract_addresses={tokenCA}
```
（56 = BSC Chain ID）

**配置：**
- ❌ **无需 API Key**（公开 API）
- ✅ 已在 `chain-snapshot-bsc.js` 中集成

**速率限制：**
- 未明确限制（建议 < 60 req/min）

**数据返回示例：**
```json
{
  "result": {
    "0x...": {
      "is_honeypot": "0",
      "buy_tax": "0.02",
      "sell_tax": "0.03",
      "slippage_modifiable": "1",
      "owner_address": "0x...",
      "lp_holders": [...],
      "holders": [...]
    }
  }
}
```

**重要字段说明：**
- `is_honeypot`: "0" = Pass, "1" = Fail
- `buy_tax` / `sell_tax`: 十进制格式（0.05 = 5%）
- `slippage_modifiable`: "1" = 税率可修改
- `lp_holders`: LP 锁定详情

---

### 3. **Jupiter Quote API** ✅ 已可用

**用途（仅 Solana）：**
- 测试卖出滑点（基于真实流动性）
- 获取最优交易路径
- 计算价格影响

**端点：**
```
https://quote-api.jup.ag/v6/quote
  ?inputMint={tokenCA}
  &outputMint=So11111111111111111111111111111111111111112
  &amount={token_amount}
  &slippageBps=100
```

**配置：**
- ❌ **无需 API Key**
- ✅ 已在 `chain-snapshot-sol.js` 中集成

**我们的使用：**
```javascript
// 测试卖出 20% 的计划仓位
const sellTestAmount = plannedPosition * 0.20; // SOL
const tokenAmount = (sellTestAmount / price) * 1e9; // 转为 lamports
```

**数据返回示例：**
```json
{
  "outAmount": "150000000",
  "otherAmountThreshold": "148500000",
  "priceImpactPct": 1.2,
  "routePlan": [...]
}
```

**滑点计算：**
```javascript
slippage = ((outAmount - minAmount) / outAmount) * 100
```

---

### 4. **Helius API** ✅ 已配置

**用途（仅 Solana）：**
- ✅ **Enhanced RPC** - 更快的区块链数据查询
- ✅ **Transaction History** - 解析交易记录
- ✅ **Risk Wallet Detection** - 识别风险地址

**RPC 端点：**
```
https://mainnet.helius-rpc.com/?api-key={YOUR_KEY}
```

**Enhanced API 端点：**
```
https://api-mainnet.helius-rpc.com/v0/addresses/{address}/transactions/?api-key={YOUR_KEY}
```

**配置：**
```bash
# .env 文件
HELIUS_API_KEY=fc942b56-923c-4a62-b786-38035d8a8e08
```

**✅ 当前状态：**
- API Key 已配置
- 系统自动使用 Helius RPC（更快）
- Risk Wallet Detection 已实现

**免费限额：**
- 每月 100,000 请求
- 适合中等交易量（< 50 次/天）

**升级选项：**
- 如果超过限额，可升级到付费 tier
- 或者回退到免费 RPC（系统自动 fallback）

---

### 5. **Telegram Bot Token** ✅ 已配置

**用途：**
- 接收 Telegram 信号（监听聚合频道）
- 发送交易通知（买入/卖出提醒）

**获取方式：**
1. 与 @BotFather 对话
2. 发送 `/newbot`
3. 按提示创建 bot
4. 获得 token（格式：`123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11`）

**配置：**
```bash
# .env 文件
TELEGRAM_BOT_TOKEN=8468934005:AAFU6hqihZOPqqYkonRE84dsmhyGHcQD5Nc
TELEGRAM_ADMIN_CHAT_ID=  # 你的 Chat ID（用于接收通知）
```

**✅ 当前状态：**
- Bot Token 已提供
- 需要配置 `TELEGRAM_ADMIN_CHAT_ID`

**获取 Chat ID：**
1. 向你的 bot 发送任意消息
2. 访问：`https://api.telegram.org/bot{YOUR_TOKEN}/getUpdates`
3. 找到 `"chat":{"id":123456789}`
4. 将 `123456789` 填入 `.env`

---

### 6. **Telegram API (for GMGN)** 🟡 推荐配置

**用途：**
- 控制 GMGN Bot 执行交易
- 自动发送代币合约地址到 GMGN Bot
- 接收交易确认消息

**获取方式：**
1. 访问 https://my.telegram.org/apps
2. 使用你的 Telegram 账号登录
3. 点击 "Create new application"
4. 填写信息：
   - App title: `GMGN Auto Trader`
   - Short name: `gmgn_bot`
   - Platform: `Other`
5. 获得 `api_id` 和 `api_hash`

**配置：**
```bash
# .env 文件
TELEGRAM_API_ID=12345678
TELEGRAM_API_HASH=abcdef1234567890abcdef1234567890
TELEGRAM_SESSION=  # 首次运行后自动生成
```

**首次运行：**
- 系统会提示输入手机号和验证码
- 完成后会生成 `TELEGRAM_SESSION` 字符串
- 保存到 `.env`，下次无需再验证

**详细配置指南：**
参考 `docs/GMGN_SETUP_GUIDE.md`

---

### 7. **BscScan API** 🟡 推荐配置

**用途：**
- 读取 BSC 合约源代码
- 验证税率是否有硬编码上限
- 增强 BSC 代币安全检测

**为什么需要：**
GoPlus 只能告诉你 "税率可修改"，但不能告诉你源码中是否有限制。

**示例场景：**
```solidity
// 合约源码：
uint256 public constant MAX_TAX = 5; // 5%

function setTax(uint256 newTax) external onlyOwner {
    require(newTax <= MAX_TAX, "Tax too high");
    sellTax = newTax;
}
```

如果有这种限制，即使 `slippage_modifiable: 1`，也是安全的。

**获取方式：**
1. 访问 https://bscscan.com/myapikey
2. 注册/登录 BscScan 账号
3. 创建免费 API Key

**配置：**
```bash
# .env 文件
BSCSCAN_API_KEY=YOUR_FREE_API_KEY
```

**免费限额：**
- 5 请求/秒
- 100,000 请求/天
- 完全够用

**端点：**
```
https://api.bscscan.com/api
  ?module=contract
  &action=getsourcecode
  &address={tokenCA}
  &apikey={YOUR_KEY}
```

**❌ 没有 BscScan 的影响：**
- 系统仍能工作
- 对于 "税率可修改" 的代币，无法验证硬编码上限
- 这类代币会被标记为 GREYLIST（需要人工审核）

---

### 8. **Twitter Bearer Token** 🔴 可选

**用途：**
- X (Twitter) 验证模块（P1 功能）
- 检测代币最早 Twitter 提及时间
- 统计 15 分钟内独立作者数
- 识别 Tier1 KOL 提及

**获取方式：**
1. 申请 Twitter Developer 账号
2. 创建 App
3. 生成 Bearer Token

**配置：**
```bash
# .env 文件
TWITTER_BEARER_TOKEN=YOUR_BEARER_TOKEN
```

**⚠️ 注意：**
- 这是 P1（完整功能）阶段的需求
- MVP 阶段不是必需的
- 可以先跳过，后续再配置

---

### 9. **GMGN API Key** 🔴 需要白名单

**用途：**
- GMGN API 直接调用模式（方案 B）
- 高频交易场景（> 100 次/天）
- 完全自定义交易策略

**权限要求：**
1. Access Token（基础）
2. IP 白名单（交易权限）
3. 交易活跃度（需要有历史记录）

**申请流程：**
1. 登录 https://gmgn.ai
2. 生成 Access Token (Account Settings)
3. 联系 GMGN 团队申请白名单：
   - 提供钱包地址（有交易历史）
   - 提供服务器 IP
   - 说明用途
4. 等待审批（1-3 天）

**配置：**
```bash
# .env 文件
GMGN_API_KEY=YOUR_API_KEY
GMGN_WALLET_ADDRESS=YOUR_WALLET
USE_GMGN_API=true
```

**⚠️ 推荐策略：**
- **MVP 阶段**：使用 Telegram Bot 模式（无需白名单）
- **交易量大后**：再申请 API 白名单

---

## 🎯 MVP 配置检查清单

部署前请确认以下 API 已配置：

### ✅ 必需（MVP 可运行）
- [x] DexScreener API - 免费，无需配置
- [x] GoPlus Security API - 免费，无需配置
- [x] Jupiter Quote API - 免费，无需配置
- [x] Solana RPC - 免费，无需配置
- [x] BSC RPC - 免费，无需配置
- [x] Helius API - ✅ 已配置
- [x] Telegram Bot Token - ✅ 已配置

### 🟡 推荐配置
- [ ] Telegram Admin Chat ID - 需要手动获取
- [ ] Telegram API (for GMGN) - 从 my.telegram.org 获取
- [ ] BscScan API - 可选，建议配置

### 🔴 暂不需要
- [ ] Twitter Bearer Token - P1 阶段
- [ ] GMGN API Key - 可选，Telegram 模式更简单

---

## 📊 当前系统状态总结

### ✅ 已完全配置且可用：
1. ✅ DexScreener - 市场数据
2. ✅ GoPlus - BSC 安全检测
3. ✅ Jupiter - Solana 滑点测试
4. ✅ Helius - Solana 增强功能（已有 API Key）
5. ✅ Telegram Bot - 信号接收（已有 Token）

### 🟡 需要你补充：
1. 🟡 Telegram Admin Chat ID - 接收通知用
2. 🟡 Telegram API credentials - GMGN 自动交易用
3. 🟡 BscScan API Key - BSC 源码验证（可选）

### 系统可运行状态：
- ✅ **影子模式**：可以立即运行（模拟交易）
- ✅ **数据采集**：所有链上数据功能完整
- ✅ **评分系统**：完整的 Gate + Soft Score
- 🟡 **自动交易**：需要配置 Telegram API

---

## 🚀 快速启动建议

### 第 1 步：测试数据采集（现在可以）
```bash
npm install
npm run db:init
SHADOW_MODE=true npm start
```

### 第 2 步：配置通知（15分钟）
1. 获取 Telegram Chat ID
2. 填入 `.env` 的 `TELEGRAM_ADMIN_CHAT_ID`
3. 重启系统

### 第 3 步：配置自动交易（30分钟）
1. 在 GMGN Bot 中开启 Auto Buy
2. 获取 Telegram API credentials
3. 填入 `.env`
4. 首次运行完成认证
5. 切换到真实模式（`SHADOW_MODE=false`）

### 第 4 步：可选优化
- 申请 BscScan API（提升 BSC 检测）
- 等待交易量大后再申请 GMGN API 白名单

---

## 📞 获取帮助

**API 申请问题：**
- Helius: https://docs.helius.dev
- BscScan: https://docs.bscscan.com
- Telegram: https://core.telegram.org/api
- GMGN: Discord https://discord.gg/gmgn

**系统配置问题：**
- 参考 `docs/GMGN_SETUP_GUIDE.md`
- 参考 `README.md`
