# Grok API 集成实施方案

**创建时间**: 2025-12-19
**目标**: 使用 Grok API 提升 Soft Score 到 70-80+

---

## ✅ 已完成

1. **Grok API 配置**
   - API Key 添加到 `.env`
   - 测试成功 ($BONK: 247提及, 12,456互动)

2. **Grok Twitter Client 创建**
   - 文件: `src/social/grok-twitter-client.js`
   - 功能: 搜索Twitter, 验证信号, 计算可信度

---

## 📋 实施步骤 (按优先级)

### Phase 1: 增强 TG Spread (30分 → 25分)

**当前问题**: 只计算单次提及 → 0分

**改进策略**:
```javascript
TG Spread Score (30分):
├─ Telegram 15分钟窗口 (10分)
│  ├─ 统计唯一频道数
│  ├─ 消息速率
│  └─ 传播模式
│
├─ Twitter 验证 (15分) ← Grok API
│  ├─ 提及数量 (mention_count)
│  ├─ 独立作者数 (unique_authors)
│  ├─ KOL 参与度 (kol_count)
│  └─ 情绪分析 (sentiment)
│
└─ 链上社交信号 (5分)
   └─ DexScreener boosts/watchlist
```

**实施文件**:
- 修改: `src/social/soft-score.js` 中的 `_calculateTGSpread()`
- 集成: Grok Twitter Client

**预期提升**: 0分 → 20-25分

---

### Phase 2: Influence 动态评级 (25分 → 20分)

**当前问题**: 所有频道权重相同 → 得分低

**改进策略**:
```javascript
Influence Score (25分):
├─ 频道历史表现 (15分)
│  ├─ 统计30天信号数量
│  ├─ 计算准确率
│  └─ 动态调整 tier
│
├─ 频道影响力指标 (5分)
│  ├─ Tier A: 3.0x 权重
│  ├─ Tier B: 2.0x 权重
│  └─ Tier C: 1.0x 权重
│
└─ Twitter验证加成 (5分)
   └─ 如果Twitter也提及,加成5-10分
```

**实施文件**:
- 修改: `src/social/soft-score.js` 中的 `_calculateInfluence()`
- 新增: 频道历史追踪数据库表

**预期提升**: 当前分数 → +15-20分

---

### Phase 3: Narrative 智能检测 (25分 → 20分)

**当前问题**: 无叙事检测 → 0分

**改进策略**:
```javascript
Narrative Score (25分):
├─ 热门叙事匹配 (20分)
│  ├─ AI/Agent (权重 10/10)
│  ├─ MEME (权重 10/10)
│  ├─ RWA (权重 7/10)
│  ├─ DePIN (权重 7/10)
│  ├─ DeFi (权重 6/10)
│  └─ PolitiFi (权重 5/10)
│
└─ 叙事时效性 (5分)
   └─ 匹配热点 = 高分
```

**实施文件**:
- 新增: `src/social/narrative-detector.js`
- 数据: 叙事关键词库 (基于12月研究)

**预期提升**: 0分 → 15-20分

---

### Phase 4: Source 可信度 (10分 → 8分)

**当前问题**: 无可信度计算 → 0分

**改进策略**:
```javascript
Source Score (10分):
├─ 频道历史统计 (5分)
│  ├─ 信号数量
│  ├─ 信号质量
│  └─ 频道年龄
│
└─ 社区验证 (5分)
   └─ Twitter上也在讨论
```

**实施文件**:
- 修改: `src/social/soft-score.js` 中的 `_calculateSource()`

**预期提升**: 0分 → +5-8分

---

### Phase 5: Graph 社区关系 (10分 → 8分)

**当前**: 固定5分

**改进策略**:
```javascript
Graph Score (10分):
├─ 跨频道传播 (5分)
│  └─ 多个频道同时提及
│
└─ KOL 提及 (5分)
   └─ Twitter KOL 参与
```

**实施文件**:
- 修改: `src/social/soft-score.js` 中的 `_calculateGraph()`

**预期提升**: 5分 → +3-5分 = 8分

---

## 📊 预期得分提升路径

```
当前得分: 1/100

Phase 1 完成: 1 → 25 (TG Spread + Twitter)
Phase 2 完成: 25 → 45 (+ Influence)
Phase 3 完成: 45 → 65 (+ Narrative)
Phase 4 完成: 65 → 73 (+ Source)
Phase 5 完成: 73 → 76 (+ Graph)

最终目标: 75-80/100 ✅
```

---

## 🔧 实施优先级

**立即实施** (关键得分提升):
1. ✅ Grok Client 创建
2. ⏳ TG Spread + Twitter 验证 (Phase 1) - **最大提升**
3. ⏳ Narrative 检测 (Phase 3) - **容易实现**

**第二轮** (稳固分数):
4. ⏳ Influence 动态评级 (Phase 2)
5. ⏳ Source 可信度 (Phase 4)
6. ⏳ Graph 优化 (Phase 5)

---

## 💡 关键技术要点

### Grok API 调用模式

```javascript
// src/index.js 中的集成
const GrokTwitterClient = require('./social/grok-twitter-client');
const grokClient = new GrokTwitterClient();

// Step 3: 采集社交数据时
const twitterData = await grokClient.searchToken(
  snapshot.symbol,
  signal.token_ca,
  15  // 15分钟窗口
);

// 将Twitter数据添加到socialData
socialData.twitter_mentions = twitterData.mention_count;
socialData.twitter_unique_authors = twitterData.unique_authors;
socialData.twitter_sentiment = twitterData.sentiment;
socialData.twitter_kol_count = twitterData.kol_count;
socialData.twitter_engagement = twitterData.engagement;
```

### 叙事关键词库

基于 2025年12月市场研究:

```javascript
const NARRATIVES = {
  'AI/Agent': {
    weight: 10,
    keywords: ['ai', 'agent', 'autonomous', 'llm', 'gpt', 'neural', 'ml', 'machine learning']
  },
  'MEME': {
    weight: 10,
    keywords: ['meme', 'pepe', 'doge', 'shib', 'wojak', 'community', 'viral']
  },
  'RWA': {
    weight: 7,
    keywords: ['rwa', 'real world asset', 'tokenization', 'asset', 'property']
  },
  'DePIN': {
    weight: 7,
    keywords: ['depin', 'infrastructure', 'network', 'node', 'bandwidth']
  },
  'DeFi': {
    weight: 6,
    keywords: ['defi', 'yield', 'lending', 'swap', 'liquidity', 'farming']
  },
  'PolitiFi': {
    weight: 5,
    keywords: ['trump', 'political', 'election', 'vote', 'politics']
  }
};
```

---

## 🎯 成功指标

### 短期 (本周内)
- [ ] Phase 1 完成 → 得分 20-25
- [ ] Phase 3 完成 → 得分 40-45
- [ ] 至少1个信号得分 > 60

### 中期 (本月内)
- [ ] 全部 Phase 完成 → 得分 75-80
- [ ] 至少5个信号得分 > 70
- [ ] 至少1个信号通过 AUTO_BUY

### 长期 (3个月内)
- [ ] 稳定得分 80+
- [ ] 实际执行交易
- [ ] 收集反馈优化参数

---

## 📝 下一步行动

**现在开始**:
1. 实现 Phase 1 (TG Spread + Twitter)
2. 测试单个信号的得分变化
3. 验证 Twitter 数据正确集成

**文档更新**: 2025-12-19
**下次更新**: Phase 1 完成后
