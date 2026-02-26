# SOP优化计划实施指南

**日期**: 2025-12-20
**状态**: Phase 3.5 完成,开始实施用户优化建议

---

## 用户优化建议清单 (来自上一条消息)

### 1. 永久黑名单机制 ✅ 开始实施

**原始需求**:
> "Exit Gate 触发的 Token (流动性崩溃、Key Risk Wallet 抛售) 应该被永久拉黑,不再是 30 分钟冷却"

**实施计划**:

#### 触发条件 (需要永久黑名单的情况):
1. **流动性崩溃** (`liquidity_check` 失败且 liquidity_usd < 初始liquidity的50%)
2. **Key Risk Wallet Exit** (Top10 holder 抛售超过持仓的80%)
3. **Rug Pull检测** (合约所有权突然转移且流动性骤降)
4. **Honeypot确认** (buy成功但sell失败,且gas > 1M)

#### 实施步骤:
1. **数据库扩展** (添加 `permanent_blacklist` 表)
   ```sql
   CREATE TABLE IF NOT EXISTS permanent_blacklist (
     token_ca TEXT PRIMARY KEY,
     chain TEXT NOT NULL,
     blacklist_reason TEXT NOT NULL,
     blacklist_timestamp INTEGER NOT NULL,
     initial_liquidity REAL,
     final_liquidity REAL,
     exit_tx_hash TEXT,
     deployer_address TEXT
   );
   ```

2. **Exit Gate逻辑修改** (`src/gates/exit-gate.js`)
   - 检测到 critical failure → 调用 `addToPermanentBlacklist()`
   - Critical conditions:
     - Liquidity drop > 50%
     - Key wallet exit > 80% holdings
     - Rug pull signature detected

3. **Signal Processing修改** (`src/index.js`)
   - Step 1之前: Check permanent blacklist
   - 如果命中 → 立即 REJECT,跳过所有处理

4. **日志增强**
   - 记录 `initial_liquidity` 和 `deployer_balance` 到数据库
   - Exit Gate 触发时记录详细原因和交易哈希

---

### 2. BSC Gas Limit 检查 (防Honeypot)

**原始需求**:
> "BSC 可以用 estimateGas 检查, 如果 > 1,000,000 gas 说明是 Honeypot, 直接 REJECT"

**实施计划**:

#### 检查位置:
- **Hard Gate阶段** (`src/gates/hard-gate.js`)
- **BSC链专用检查**

#### 实施步骤:
1. **添加 Gas Limit 检查方法** (`src/chain/chain-snapshot-bsc.js`)
   ```javascript
   async estimateSellGas(tokenCA, amount = '1000000000000000000') {
     // Simulate sell transaction using ethers.js
     // Return estimated gas
   }
   ```

2. **Hard Gate集成**
   ```javascript
   // In hard-gate.js
   if (chain === 'BSC') {
     const sellGas = await bscService.estimateSellGas(token_ca);
     if (sellGas > 1_000_000) {
       return {
         status: 'REJECT',
         reasons: ['Honeypot detected: sell gas > 1M (likely trap contract)']
       };
     }
   }
   ```

3. **配置参数** (`config/config.json`)
   ```json
   "bsc_honeypot_detection": {
     "max_sell_gas": 1000000,
     "enabled": true
   }
   ```

---

### 3. Tier 1 豁免 Matrix Penalty

**原始需求**:
> "如果有 Tier 1 频道参与, 即使 tg_ch_15m >= 8 且 tg_clusters_15m <= 2, 也不触发 Matrix Penalty"

**实施计划**:

#### 修改位置:
- **TG Spread Scoring** (`src/scoring/tg-spread.js`)

#### 实施步骤:
1. **Matrix Penalty逻辑修改**
   ```javascript
   // In calculateMatrixPenalty() method

   // Check if any Tier 1 channels are involved
   const tier1Channels = channels.filter(ch => ch.tier === 'A');

   if (tier1Channels.length > 0) {
     console.log(`   ✨ Tier 1 exemption: ${tier1Channels.length} Tier A channels present`);
     return { penalty: 0, reason: 'Tier 1 exemption - high quality source' };
   }

   // Otherwise apply normal Matrix Penalty logic
   ```

2. **配置参数**
   ```json
   "matrix_penalty": {
     "tier1_exemption": true,
     "high_channel_threshold": 8,
     "low_cluster_threshold": 2
   }
   ```

---

### 4. 退出滑点保护机制

**原始需求**:
> "Exit 时设置最大允许滑点 (比如 15%), 超过则分批退出或等待"

**实施计划**:

#### 实施位置:
- **Exit Logic** (`src/execution/exit-handler.js` - 需要创建)

#### 实施步骤:
1. **创建 Exit Handler 模块**
   ```javascript
   // src/execution/exit-handler.js
   class ExitHandler {
     async executeExit(tokenCA, amount, maxSlippage = 0.15) {
       const currentSlippage = await this.estimateSlippage(tokenCA, amount);

       if (currentSlippage > maxSlippage) {
         console.log(`   ⚠️  High slippage detected: ${(currentSlippage*100).toFixed(1)}%`);
         return await this.batchExit(tokenCA, amount, maxSlippage);
       }

       return await this.singleExit(tokenCA, amount, maxSlippage);
     }

     async batchExit(tokenCA, totalAmount, maxSlippage) {
       // Split into smaller batches
       const batchSize = totalAmount * 0.3; // 30% per batch
       // Execute multiple sells with delay
     }
   }
   ```

2. **配置参数**
   ```json
   "exit_protection": {
     "max_slippage": 0.15,
     "batch_size_ratio": 0.3,
     "batch_delay_ms": 5000
   }
   ```

---

### 5. 数据日志增强 (initial_liquidity + deployer_balance)

**原始需求**:
> "数据库添加 initial_liquidity 和 deployer_balance 字段, 用于回溯分析"

**实施计划**:

#### 数据库修改:
1. **扩展 `chain_snapshots` 表**
   ```sql
   ALTER TABLE chain_snapshots ADD COLUMN initial_liquidity REAL;
   ALTER TABLE chain_snapshots ADD COLUMN deployer_balance REAL;
   ALTER TABLE chain_snapshots ADD COLUMN deployer_address TEXT;
   ```

2. **Snapshot获取逻辑修改** (`src/chain/chain-snapshot-sol.js`)
   ```javascript
   // In getSnapshot()
   const deployerAddress = await this.getDeployerAddress(tokenCA);
   const deployerBalance = await this.getTokenBalance(deployerAddress, tokenCA);

   snapshot.deployer_address = deployerAddress;
   snapshot.deployer_balance = deployerBalance;
   snapshot.initial_liquidity = snapshot.liquidity_usd; // Store as initial
   ```

3. **持久化修改** (`src/index.js`)
   ```javascript
   // When persisting snapshot
   INSERT INTO chain_snapshots (
     ...,
     initial_liquidity,
     deployer_balance,
     deployer_address
   ) VALUES (...);
   ```

---

### 6. 更新付费群信号源

**原始需求**:
> "CXO 监控信号很快会被关闭, 准备更新一些付费群信号替换"

**实施计划**:

#### 等待用户提供:
- 新付费群的 Telegram 链接或 ID
- 新群的 Tier 等级 (A/B/C)
- 是否需要移除 CXO (@CXOStrategyBot)

#### 实施步骤:
1. **数据库更新** (`signal_channels` 表)
2. **代码中移除/添加频道** (`src/signals/telegram-user-listener.js`)
3. **更新配置文件** (如果有)

**状态**: ⏸️ 等待用户提供新群信息

---

## 实施优先级

### 🔴 高优先级 (立即实施):
1. ✅ **Permanent Blacklist** - 防止重复交易失败Token
2. ✅ **BSC Gas Limit检查** - 防Honeypot,保护资金安全
3. ✅ **Tier 1豁免Matrix Penalty** - 提升高质量信号准确性

### 🟡 中优先级 (下一批):
4. **数据日志增强** - 为回溯分析提供数据
5. **退出滑点保护** - 优化退出策略

### 🟢 低优先级 (等待用户输入):
6. **更新付费群信号源** - 需要用户提供新群信息

---

## 当前实施进度

**Phase 3.5**: Token元数据集成 ✅ 代码完成,等待重启测试

**Optimization 1**: Permanent Blacklist 🔄 正在实施

---

**文档版本**: v1.0
**最后更新**: 2025-12-20
**负责人**: Claude
