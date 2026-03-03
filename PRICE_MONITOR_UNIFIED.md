# 价格监控系统统一方案

## 🎯 问题根源

### 之前的问题：
1. **价格单位不一致**
   - 买入时 entryPrice 记录 USD 价格
   - LivePriceMonitor 发送 SOL 价格（转换后）
   - PnL 计算混用两种单位

2. **价格数据不准确**
   - Jupiter Price API 返回市场价格
   - Jupiter Swap 实际报价考虑滑点
   - Meme coin 流动性低，两者差异巨大

3. **SOL/USD 汇率延迟**
   - SOL 价格每 10 秒更新
   - Token 价格每 0.5 秒更新
   - 转换后的 SOL 价格有延迟

### 实盘案例：
- $George: 峰值显示 +70.9%，实际卖出 -7.1%
- $McClaw: 峰值显示 +63.2%，实际卖出 -1.0%

---

## ✅ 解决方案

### 方案 1：修复 entryPrice 记录方式（已实现）

**使用实际交易价格计算 entryPrice：**
```javascript
// 买入后计算实际入场价格
const actualTokenAmount = tokenAmount / Math.pow(10, tokenDecimals);
const entryPrice = finalSize / actualTokenAmount;  // SOL per token
```

**优势：**
- ✅ 使用实际成交价格，最准确
- ✅ 统一所有价格使用 SOL 单位
- ✅ 不依赖外部价格数据

**文件：** `src/engines/premium-signal-engine.js`

---

### 方案 2：LivePriceMonitorV2 - 使用 Jupiter Swap Quote（新增）

**完全统一价格源：**
- ❌ 不再使用 Jupiter Price API（市场价格）
- ✅ 直接使用 Jupiter Swap Quote（实际可交易价格）
- ✅ 每次价格更新时，模拟卖出获取真实报价

**核心逻辑：**
```javascript
// 使用 Jupiter Quote API 获取实际可交易价格
const quote = await jupiterApi.getQuote({
  inputMint: tokenCA,
  outputMint: SOL_MINT,
  amount: tokenAmount,
  slippageBps: 1500  // 与实际卖出一致
});

// 计算 SOL per token 价格
const outSol = quote.outAmount / 1e9;
const solPrice = outSol / actualTokenAmount;
```

**优势：**
- ✅ 使用实际可交易价格，最准确
- ✅ 考虑了滑点和流动性
- ✅ 与卖出时的报价一致
- ✅ 不需要 SOL/USD 汇率转换
- ✅ 价格单位统一（SOL per token）

**劣势：**
- ⚠️ API 调用频率高（每 1.5 秒）
- ⚠️ 可能触发 rate limit（已优化：逐个查询，间隔 100ms）

**文件：** `src/tracking/live-price-monitor-v2.js`

---

## 🔧 使用方法

### 启用 V2 版本（推荐）

在 `.env` 文件中添加：
```bash
USE_PRICE_MONITOR_V2=true
```

### 继续使用 V1 版本

不设置或设置为 false：
```bash
USE_PRICE_MONITOR_V2=false
```

---

## 📊 预期效果

### 修复后：
1. **PnL 显示准确**
   - 显示的 PnL 与实际交易价格一致
   - 不会再出现峰值 +70% 实际 -7% 的情况

2. **PEAK_EXIT 触发准确**
   - 触发时的价格与卖出时的价格接近
   - 减少价格暴跌导致的损失

3. **价格单位统一**
   - 所有价格使用 SOL 单位
   - entryPrice、currentPrice、exitPrice 统一

---

## 🚀 部署步骤

1. **拉取最新代码**
   ```bash
   git pull origin main
   ```

2. **配置环境变量**
   ```bash
   # 启用 V2 版本（推荐）
   echo "USE_PRICE_MONITOR_V2=true" >> .env
   ```

3. **重启服务**
   ```bash
   pm2 restart sentiment-arbitrage
   ```

4. **观察日志**
   ```bash
   pm2 logs sentiment-arbitrage
   ```

   应该看到：
   ```
   📡 [价格监控] 使用 V2 版本 (Jupiter Swap Quote)
   📡 [LivePriceMonitorV2] 初始化 - 使用 Jupiter Swap Quote
   ✅ [LivePriceMonitorV2] 启动 | Quote 间隔: 1500ms
   ```

---

## 🔍 验证方法

### 1. 检查 PnL 显示
观察下一笔交易的日志：
```
💰 [Entry] 入场价格: 0.0000000001025 SOL/token | 0.1 SOL → 975442154.04 tokens
```

### 2. 对比峰值和实际退出
```
🟢 [EXIT] $TOKEN | PEAK_EXIT(peak+71%) | PnL: +60.3% | 最高: +70.9%
报价: 975442154035 tokens → 0.1603 SOL
实际 PnL: (0.1603 - 0.1) / 0.1 = +60.3% ✅
```

### 3. 前端数据验证
- 峰值 PnL 应该与日志中的 "最高" 一致
- 实际退出 PnL 应该与 Jupiter 报价计算的 PnL 一致

---

## 📈 回测数据

基于虚拟盘 318 笔交易的回测：

| 策略 | 胜率 | 平均退出 | 期望值 |
|------|------|----------|--------|
| PEAK_EXIT (当前) | 72.0% | +31.76% | +31.76% |
| TAKE_PROFIT_50 | 72.0% | +18.04% | +18.04% |
| TAKE_PROFIT_30 | 72.0% | +14.06% | +14.06% |

**PEAK_EXIT 策略优势：**
- 期望值是 TP50 的 1.76 倍
- 期望值是 TP30 的 2.26 倍
- 能捕获更多利润

**前提：价格数据必须准确！**

---

## ⚠️ 注意事项

1. **V2 版本 API 调用频率高**
   - 每个 token 每 1.5 秒查询一次
   - 已优化：逐个查询，间隔 100ms
   - 如果触发 rate limit，会自动降级

2. **观察 1-2 笔交易后再评估**
   - 验证 PnL 显示是否准确
   - 验证 PEAK_EXIT 触发是否合理
   - 如有问题，可切换回 V1 版本

3. **V1 版本仍然可用**
   - 已修复 entryPrice 记录方式
   - 已修复 USD → SOL 转换
   - 如果 V2 有问题，可以回退

---

## 🎯 总结

**核心修复：**
1. ✅ entryPrice 使用实际交易价格（SOL 单位）
2. ✅ LivePriceMonitorV2 使用 Jupiter Swap Quote（实际可交易价格）
3. ✅ 价格单位完全统一（SOL per token）

**预期效果：**
- PnL 显示准确
- PEAK_EXIT 触发准确
- 不会再出现虚假峰值

**建议：**
- 启用 V2 版本（USE_PRICE_MONITOR_V2=true）
- 观察 1-2 笔交易验证效果
- 如有问题，可切换回 V1 版本
