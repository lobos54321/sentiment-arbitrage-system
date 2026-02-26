# 🚀 快速启动指南（5 步完成配置）

本指南将帮助你在 **30 分钟内**完成系统配置并开始运行。

---

## ✅ 已完成的配置

你已经配置好了：
- ✅ Telegram Bot Token
- ✅ Helius API Key
- ✅ BscScan API Key

还需要完成 **3 个配置**：

---

## 📋 待完成配置（3 步）

### **步骤 1：获取你的 Telegram Chat ID**（5 分钟）

#### 1.1 向你的 Bot 发消息
1. 打开 Telegram
2. 搜索你的 Bot（创建时 BotFather 告诉你的用户名）
3. 点击 "Start" 或发送 "Hello"

#### 1.2 获取 Chat ID
在浏览器中打开这个链接：
```
https://api.telegram.org/bot8468934005:AAFU6hqihZOPqqYkonRE84dsmhyGHcQD5Nc/getUpdates
```

#### 1.3 查找你的 Chat ID
在返回的 JSON 中找到：
```json
{
  "result": [{
    "message": {
      "chat": {
        "id": 123456789  // ← 这就是你的 Chat ID
      }
    }
  }]
}
```

#### 1.4 填入配置文件
打开 `.env` 文件，找到第 3 行：
```bash
TELEGRAM_ADMIN_CHAT_ID=123456789  # 替换成你的 Chat ID
```

---

### **步骤 2：配置 GMGN Telegram Bot**（10 分钟）

#### 2.1 启动 GMGN Bot
在 Telegram 中搜索：
- Solana: `@GMGN_sol_bot`
- BSC: `@GMGN_bsc_bot`

发送 `/start` 开始

#### 2.2 充值钱包（测试用）
1. 点击 Bot 中的 "💰 Wallet" 按钮
2. 复制充值地址
3. 转入小额测试资金：
   - **Solana**: 0.5 - 1 SOL
   - **BSC**: 0.05 - 0.1 BNB

#### 2.3 开启自动买入
1. 点击 "⚙️ Settings"
2. 找到 "Auto Buy" → 点击开启
3. 设置参数：
   - **Amount per trade** (每笔金额):
     - SOL: `0.1` - `0.2`
     - BSC: `0.01` - `0.02`
   - **Anti-MEV**: 开启 ✅
   - **Slippage**: 设为 `5-10%`（测试阶段）

#### 2.4 设置止盈止损（可选）
1. 在 Settings 中找到 "Auto Sell"
2. 开启后设置：
   - **Take Profit** (止盈): `+30%` - `+50%`
   - **Stop Loss** (止损): `-20%` - `-30%`

---

### **步骤 3：获取 Telegram API Credentials**（10 分钟）

#### 3.1 申请 API 访问
1. 访问：https://my.telegram.org/apps
2. 用你的 Telegram 账号登录
3. 点击 "Create new application"

#### 3.2 填写应用信息
- **App title**: `GMGN Auto Trader`
- **Short name**: `gmgn_bot`
- **Platform**: 选 `Other` 或 `Desktop`
- **Description**: `Auto trading bot`（可选）

#### 3.3 获取凭证
提交后会显示：
```
App api_id: 12345678
App api_hash: abcdef1234567890abcdef1234567890
```

**重要：** 记下这两个值！

#### 3.4 填入配置文件
打开 `.env` 文件，找到第 17-19 行：
```bash
TELEGRAM_API_ID=12345678  # 你的 api_id
TELEGRAM_API_HASH=abcdef1234567890abcdef1234567890  # 你的 api_hash
TELEGRAM_SESSION=  # 暂时留空，首次运行后自动生成
```

---

## 🎯 首次运行和认证

### 1. 安装依赖
```bash
cd sentiment-arbitrage-system
npm install
```

### 2. 初始化数据库
```bash
npm run db:init
```

### 3. 首次启动（会进行 Telegram 认证）
```bash
npm start
```

### 4. 完成 Telegram 认证

**提示 1 - 输入手机号：**
```
Please enter your phone number:
```
输入格式：`+86` + 手机号（如 `+8613800138000`）

**提示 2 - 输入验证码：**
```
Please enter the code you received:
```
Telegram 会发给你 5 位验证码，输入即可

**提示 3 - 输入密码（如果开启了两步验证）：**
```
Please enter your password:
```
输入你的 Telegram 密码

**提示 4 - 保存 Session String：**
```
💾 Save this session string to .env as TELEGRAM_SESSION:
1BVtsOIQBu7cR4QaW5toTMQjh0N... (很长的字符串)
```

### 5. 保存 Session String
1. 复制整个 Session String（从 `1` 开始到最后）
2. 打开 `.env` 文件
3. 找到第 19 行：
```bash
TELEGRAM_SESSION=1BVtsOIQBu7cR4QaW5toTMQjh0N...  # 粘贴完整字符串
```
4. 保存文件

### 6. 重新启动
```bash
npm start
```

这次不会再要求认证，系统会直接启动！

---

## 🎭 影子模式测试（推荐先运行）

首次运行建议保持影子模式（不会真实交易）：

`.env` 文件中确认：
```bash
SHADOW_MODE=true  # 影子模式开启
AUTO_BUY_ENABLED=false  # 自动买入关闭
```

观察系统日志，确认一切正常后再切换到真实模式。

---

## 🔄 切换到真实模式

测试无误后，修改 `.env`：
```bash
SHADOW_MODE=false  # 关闭影子模式
AUTO_BUY_ENABLED=true  # 开启自动买入
```

重启系统：
```bash
npm start
```

---

## 📊 配置检查清单

部署前确认：

### 基础配置
- [ ] `TELEGRAM_BOT_TOKEN` - ✅ 已配置
- [ ] `HELIUS_API_KEY` - ✅ 已配置
- [ ] `BSCSCAN_API_KEY` - ✅ 已配置
- [ ] `TELEGRAM_ADMIN_CHAT_ID` - 你需要填写
- [ ] `TELEGRAM_API_ID` - 你需要填写
- [ ] `TELEGRAM_API_HASH` - 你需要填写
- [ ] `TELEGRAM_SESSION` - 首次运行后生成

### GMGN Bot 配置
- [ ] 已在 GMGN Bot 充值
- [ ] 已开启 Auto Buy
- [ ] 已设置单笔金额
- [ ] 已开启 Anti-MEV
- [ ] 已设置滑点容忍度

### 系统配置
- [ ] 已运行 `npm install`
- [ ] 已运行 `npm run db:init`
- [ ] 已完成 Telegram 认证
- [ ] 已保存 Session String

---

## 🐛 常见问题

### Q1: "Missing TELEGRAM_API_ID" 错误
**A:** 确保已在 `.env` 文件中填写 `TELEGRAM_API_ID` 和 `TELEGRAM_API_HASH`。

### Q2: 收不到 Telegram 验证码
**A:**
1. 检查手机号格式（需要带 `+86` 等国际区号）
2. 确保 Telegram 账号正常
3. 等待几分钟后重试

### Q3: Session 过期
**A:** 重新运行系统，再次完成手机号验证，更新 `.env` 中的 `TELEGRAM_SESSION`。

### Q4: Bot 显示 "余额不足"
**A:**
1. 检查 GMGN Bot 钱包余额
2. 确认充值成功
3. 调整 Auto Buy 的单笔金额

---

## 📞 获取帮助

**Telegram 认证问题：**
- Telegram 官方文档：https://core.telegram.org/api

**GMGN Bot 问题：**
- GMGN Discord: https://discord.gg/gmgn
- 在 Bot 中发送 `/help` 查看帮助

**系统问题：**
- 检查 `README.md`
- 检查 `docs/API_CONFIGURATION.md`

---

## 🎉 完成！

配置完成后，系统会：
1. 监听 Telegram 信号
2. 分析代币质量（Gate + Score）
3. 自动决策是否买入
4. 发送指令给 GMGN Bot
5. 监控持仓并自动退出

祝交易顺利！🚀
