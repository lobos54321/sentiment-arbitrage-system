/**
 * AI-Powered Narrative System v2
 * 
 * 核心理念：不用旧世界的框架判断新事物
 * 
 * 架构：
 * 1. 实时 Twitter 搜索验证叙事（不是猜测，是验证）
 * 2. 自动发现新叙事（AI 学习市场）
 * 3. 动态更新叙事热度（每小时）
 * 
 * AI 作用：
 * - 搜索 Twitter 了解当前讨论主题
 * - 分析讨论内容判断叙事类型
 * - 发现新兴叙事并自动添加
 * - 实时评估叙事生命周期
 */

import GrokTwitterClient from '../social/grok-twitter-client.js';

export class AINarrativeSystem {
  constructor(config, db) {
    this.config = config;
    this.db = db;
    this.grokClient = new GrokTwitterClient();
    
    // Initialize database table
    this.initializeDatabase();
    
    // Load narratives into memory for fast lookup
    this.narrativesCache = new Map();
    this.loadNarrativesCache();
    
    // 新叙事发现缓存（防止重复查询）
    this.recentDiscoveries = new Map(); // token -> discovery result
    this.discoveryExpiry = 30 * 60 * 1000; // 30 分钟过期
  }

  /**
   * Initialize narratives table in database
   */
  initializeDatabase() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS ai_narratives (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        narrative_name TEXT UNIQUE NOT NULL,
        
        -- Market metrics (0-10 scale)
        market_heat REAL DEFAULT 5,
        sustainability REAL DEFAULT 5,
        competition_level TEXT DEFAULT 'medium',
        
        -- Lifecycle stage
        lifecycle_stage TEXT DEFAULT 'unknown',
        lifecycle_multiplier REAL DEFAULT 1.0,
        
        -- Calculated weight (0-10)
        weight REAL DEFAULT 5,
        
        -- AI reasoning
        ai_reasoning TEXT,
        keywords TEXT,  -- JSON array of keywords
        
        -- Metadata
        last_updated INTEGER,
        update_source TEXT,  -- 'weekly_ai', 'manual', 'initial'
        
        created_at INTEGER DEFAULT (strftime('%s', 'now'))
      );
      
      CREATE INDEX IF NOT EXISTS idx_narrative_name ON ai_narratives(narrative_name);
      CREATE INDEX IF NOT EXISTS idx_narrative_weight ON ai_narratives(weight DESC);
    `);
    
    // Seed initial narratives if empty
    const count = this.db.prepare('SELECT COUNT(*) as count FROM ai_narratives').get();
    if (count.count === 0) {
      this.seedInitialNarratives();
    }
  }

  /**
   * Seed initial narratives based on December 2024 market data
   */
  seedInitialNarratives() {
    const initialNarratives = [
      {
        name: 'AI_Agents',
        market_heat: 9.2,
        sustainability: 7.5,
        competition_level: 'medium',
        lifecycle_stage: 'early_explosion',
        lifecycle_multiplier: 1.3,
        keywords: ['ai', 'agent', 'autonomous', 'llm', 'gpt', 'neural', 'bot', 'machine learning', 'artificial intelligence'],
        ai_reasoning: 'AI agents are the hottest narrative of late 2024. Projects like VIRTUAL, ai16z showing massive gains. Early stage with high growth potential.'
      },
      {
        name: 'Meme_Coins',
        market_heat: 9.8,
        sustainability: 3.9,
        competition_level: 'high',
        lifecycle_stage: 'evergreen',
        lifecycle_multiplier: 1.0,
        keywords: ['meme', 'pepe', 'doge', 'shib', 'wojak', 'frog', 'dog', 'cat', 'bonk', 'wif', 'popcat', 'goat', 'pnut', 'degen'],
        ai_reasoning: 'Meme coins remain the highest traffic category (25% of CoinGecko). Low sustainability but consistently attracts retail attention.'
      },
      {
        name: 'DeSci',
        market_heat: 8.5,
        sustainability: 8.0,
        competition_level: 'low',
        lifecycle_stage: 'early_growth',
        lifecycle_multiplier: 1.2,
        keywords: ['desci', 'science', 'research', 'biotech', 'longevity', 'health', 'medicine', 'bio', 'vita'],
        ai_reasoning: 'DeSci emerging as major narrative. RIF, VITA showing strength. Real utility in funding research, backed by serious institutions.'
      },
      {
        name: 'RWA',
        market_heat: 7.9,
        sustainability: 9.1,
        competition_level: 'medium',
        lifecycle_stage: 'mature_growth',
        lifecycle_multiplier: 1.1,
        keywords: ['rwa', 'real world asset', 'tokenized', 'tokenization', 'property', 'real estate', 'treasury', 'bond', 'blackrock', 'ondo'],
        ai_reasoning: 'RWA has strong institutional backing (BlackRock $589M fund). High sustainability due to real asset backing. Mature but still growing.'
      },
      {
        name: 'DeFi',
        market_heat: 6.8,
        sustainability: 8.2,
        competition_level: 'high',
        lifecycle_stage: 'mature',
        lifecycle_multiplier: 0.9,
        keywords: ['defi', 'decentralized finance', 'yield', 'farming', 'liquidity', 'amm', 'dex', 'swap', 'lending', 'staking'],
        ai_reasoning: 'DeFi is core infrastructure, proven utility. Mature market with established players. Growth is moderate but stable.'
      },
      {
        name: 'Gaming_Metaverse',
        market_heat: 1.8,
        sustainability: 2.5,
        competition_level: 'medium',
        lifecycle_stage: 'decline',
        lifecycle_multiplier: 0.4,
        keywords: ['gaming', 'game', 'metaverse', 'play to earn', 'p2e', 'nft game', 'gamefi', 'virtual world'],
        ai_reasoning: 'Gaming/Metaverse narrative has collapsed. -93% funding decline per Messari. Avoid this narrative - negative signal.'
      },
      {
        name: 'Layer2_Scaling',
        market_heat: 5.9,
        sustainability: 8.5,
        competition_level: 'high',
        lifecycle_stage: 'mature',
        lifecycle_multiplier: 0.9,
        keywords: ['layer 2', 'l2', 'rollup', 'optimistic', 'zk', 'zero knowledge', 'scaling', 'arbitrum', 'optimism', 'base'],
        ai_reasoning: 'L2s are critical infrastructure but market is saturated (50+ L2s). Difficult for new projects to stand out.'
      },
      {
        name: 'Pump_Fun_Meta',
        market_heat: 8.0,
        sustainability: 4.0,
        competition_level: 'high',
        lifecycle_stage: 'peak',
        lifecycle_multiplier: 1.0,
        keywords: ['pump', 'pumpfun', 'bonding curve', 'fair launch', 'no presale', 'stealth', 'organic'],
        ai_reasoning: 'Pump.fun has dominated Solana meme coin launches. High activity but many rugs. Peak attention, may decline soon.'
      },
      {
        name: 'Celebrity_KOL',
        market_heat: 7.0,
        sustainability: 3.0,
        competition_level: 'medium',
        lifecycle_stage: 'growth',
        lifecycle_multiplier: 1.1,
        keywords: ['celebrity', 'influencer', 'ansem', 'murad', 'hsaka', 'kol', 'paid', 'call', 'alpha'],
        ai_reasoning: 'KOL-backed tokens can pump hard but often dump after. High risk, high reward. Watch for rug pulls.'
      },
      {
        name: 'Christmas_Seasonal',
        market_heat: 6.5,
        sustainability: 1.0,
        competition_level: 'low',
        lifecycle_stage: 'peak',
        lifecycle_multiplier: 0.8,
        keywords: ['christmas', 'santa', 'xmas', 'holiday', 'new year', 'winter', 'gift', 'noel'],
        ai_reasoning: 'Seasonal narrative. Will peak around Dec 25 then crash. Very short window, trade carefully.'
      }
    ];

    const insertStmt = this.db.prepare(`
      INSERT INTO ai_narratives (
        narrative_name, market_heat, sustainability, competition_level,
        lifecycle_stage, lifecycle_multiplier, keywords, ai_reasoning,
        weight, last_updated, update_source
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'initial')
    `);

    for (const narrative of initialNarratives) {
      // Calculate weight: (heat * 0.4 + sustainability * 0.3 + (10 - competition) * 0.2 + historical * 0.1) * lifecycle
      const competitionScore = narrative.competition_level === 'low' ? 8 : 
                               narrative.competition_level === 'medium' ? 5 : 2;
      const weight = (
        narrative.market_heat * 0.4 +
        narrative.sustainability * 0.3 +
        competitionScore * 0.2 +
        5 * 0.1  // Default historical score
      ) * narrative.lifecycle_multiplier;

      insertStmt.run(
        narrative.name,
        narrative.market_heat,
        narrative.sustainability,
        narrative.competition_level,
        narrative.lifecycle_stage,
        narrative.lifecycle_multiplier,
        JSON.stringify(narrative.keywords),
        narrative.ai_reasoning,
        Math.min(10, weight),
        Date.now()
      );
    }

    console.log(`✅ [AI Narrative] Seeded ${initialNarratives.length} initial narratives`);
  }

  /**
   * Load narratives into memory cache
   */
  loadNarrativesCache() {
    const narratives = this.db.prepare('SELECT * FROM ai_narratives ORDER BY weight DESC').all();
    
    this.narrativesCache.clear();
    for (const n of narratives) {
      this.narrativesCache.set(n.narrative_name, {
        ...n,
        keywords: JSON.parse(n.keywords || '[]')
      });
    }
    
    console.log(`📚 [AI Narrative] Loaded ${narratives.length} narratives into cache`);
  }

  /**
   * 每小时热度更新 - 基于实时 Twitter 数据
   * 
   * 这个比 weekly update 更轻量，只更新热度
   */
  async hourlyHeatUpdate() {
    console.log('🔄 [AI Narrative] Hourly heat update...');
    
    const prompt = `你是加密货币市场分析师。基于你对 Twitter/X 当前讨论的了解，评估以下叙事的实时热度。

当前叙事列表：
${Array.from(this.narrativesCache.keys()).join('\n')}

任务：
1. 评估每个叙事的当前热度 (0-10)
2. 识别任何新兴热点叙事
3. 标记正在衰退的叙事

用 JSON 回复：
{
  "heat_updates": [
    {"name": "AI_Agents", "heat": 9.5, "trend": "rising"},
    {"name": "Gaming_Metaverse", "heat": 2.0, "trend": "declining"}
  ],
  "emerging_narratives": [
    {"name": "新叙事名称", "heat": 8.0, "keywords": ["关键词1", "关键词2"], "reasoning": "原因"}
  ]
}`;

    try {
      const response = await this.grokClient.askGrok(prompt);
      const jsonMatch = response.match(/\{[\s\S]*\}/);
      
      if (jsonMatch) {
        const data = JSON.parse(jsonMatch[0]);
        
        // 更新热度
        for (const update of data.heat_updates || []) {
          if (this.narrativesCache.has(update.name)) {
            this.db.prepare(`
              UPDATE ai_narratives 
              SET market_heat = ?, last_updated = ?
              WHERE narrative_name = ?
            `).run(update.heat, Date.now(), update.name);
          }
        }
        
        // 添加新兴叙事
        for (const emerging of data.emerging_narratives || []) {
          await this.discoverNewNarrative(emerging.name, {
            keywords: emerging.keywords || [],
            market_heat: emerging.heat || 7,
            reasoning: emerging.reasoning,
            source_token: 'hourly_scan'
          });
        }
        
        this.loadNarrativesCache();
        console.log(`✅ [AI Narrative] Hourly update complete`);
      }
    } catch (error) {
      console.error('❌ [AI Narrative] Hourly update failed:', error.message);
    }
  }

  /**
   * Weekly AI Update - Ask Grok to reassess all narratives
   * 
   * This should be called by a cron job weekly
   */
  async weeklyNarrativeUpdate() {
    console.log('🔄 [AI Narrative] Starting weekly narrative update...');
    
    const prompt = `You are a crypto market analyst. Analyze the current narrative landscape in crypto for December 2024.

For each of these narratives, provide updated metrics:
1. AI_Agents
2. Meme_Coins
3. DeSci
4. RWA
5. DeFi
6. Gaming_Metaverse
7. Layer2_Scaling
8. Pump_Fun_Meta
9. Celebrity_KOL

Also identify any NEW hot narratives that should be added.

For each narrative, rate:
- market_heat (0-10): Current market attention and trading volume
- sustainability (0-10): Long-term viability and real utility
- lifecycle_stage: early_explosion / early_growth / growth / peak / mature / decline
- competition_level: low / medium / high
- brief reasoning (1-2 sentences)

Respond in JSON format:
{
  "narratives": [
    {
      "name": "AI_Agents",
      "market_heat": 9.5,
      "sustainability": 7.5,
      "lifecycle_stage": "early_explosion",
      "competition_level": "medium",
      "reasoning": "..."
    }
  ],
  "new_narratives": [
    {
      "name": "...",
      "keywords": ["...", "..."],
      ...
    }
  ]
}`;

    try {
      const response = await this.grokClient.askGrok(prompt);
      
      // Parse response
      const jsonMatch = response.match(/\{[\s\S]*\}/);
      if (!jsonMatch) {
        console.error('❌ [AI Narrative] Failed to parse Grok response');
        return;
      }
      
      const data = JSON.parse(jsonMatch[0]);
      
      // Update existing narratives
      const updateStmt = this.db.prepare(`
        UPDATE ai_narratives SET
          market_heat = ?,
          sustainability = ?,
          lifecycle_stage = ?,
          lifecycle_multiplier = ?,
          competition_level = ?,
          ai_reasoning = ?,
          weight = ?,
          last_updated = ?,
          update_source = 'weekly_ai'
        WHERE narrative_name = ?
      `);

      for (const n of data.narratives || []) {
        const lifecycleMultiplier = this.getLifecycleMultiplier(n.lifecycle_stage);
        const competitionScore = n.competition_level === 'low' ? 8 : 
                                 n.competition_level === 'medium' ? 5 : 2;
        const weight = (
          n.market_heat * 0.4 +
          n.sustainability * 0.3 +
          competitionScore * 0.2 +
          5 * 0.1
        ) * lifecycleMultiplier;

        updateStmt.run(
          n.market_heat,
          n.sustainability,
          n.lifecycle_stage,
          lifecycleMultiplier,
          n.competition_level,
          n.reasoning,
          Math.min(10, weight),
          Date.now(),
          n.name
        );
      }

      // Add new narratives
      if (data.new_narratives && data.new_narratives.length > 0) {
        const insertStmt = this.db.prepare(`
          INSERT OR IGNORE INTO ai_narratives (
            narrative_name, market_heat, sustainability, competition_level,
            lifecycle_stage, lifecycle_multiplier, keywords, ai_reasoning,
            weight, last_updated, update_source
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'weekly_ai')
        `);

        for (const n of data.new_narratives) {
          const lifecycleMultiplier = this.getLifecycleMultiplier(n.lifecycle_stage);
          const competitionScore = n.competition_level === 'low' ? 8 : 
                                   n.competition_level === 'medium' ? 5 : 2;
          const weight = (
            n.market_heat * 0.4 +
            n.sustainability * 0.3 +
            competitionScore * 0.2 +
            5 * 0.1
          ) * lifecycleMultiplier;

          insertStmt.run(
            n.name,
            n.market_heat,
            n.sustainability,
            n.competition_level,
            n.lifecycle_stage,
            lifecycleMultiplier,
            JSON.stringify(n.keywords || []),
            n.reasoning,
            Math.min(10, weight),
            Date.now()
          );
        }
      }

      // Reload cache
      this.loadNarrativesCache();
      
      console.log(`✅ [AI Narrative] Weekly update complete. Updated ${data.narratives?.length || 0} narratives, added ${data.new_narratives?.length || 0} new`);
      
    } catch (error) {
      console.error('❌ [AI Narrative] Weekly update failed:', error.message);
    }
  }

  /**
   * 智能叙事识别 v2
   * 
   * 流程：
   * 1. 先搜索 Twitter 获取真实讨论内容
   * 2. AI 分析讨论主题，判断叙事类型
   * 3. 如果是新叙事，自动学习并添加
   * 
   * @param {string} tokenSymbol - Token symbol
   * @param {string} tokenName - Token name
   * @param {Object} twitterData - Twitter data from Grok search
   * @returns {Object} { narrative, confidence, reasoning, is_new_narrative }
   */
  async identifyTokenNarrative(tokenSymbol, tokenName, twitterData = null) {
    // 检查缓存，避免重复查询
    const cacheKey = `${tokenSymbol}_${tokenName}`.toLowerCase();
    const cached = this.recentDiscoveries.get(cacheKey);
    if (cached && Date.now() - cached.timestamp < this.discoveryExpiry) {
      return cached.result;
    }

    // 第一步：快速关键词匹配（对已知叙事有效）
    const keywordMatch = this.matchNarrativeByKeywords(tokenSymbol, tokenName);
    
    // 第二步：通过 Twitter 搜索验证
    let twitterContext = '';
    let twitterTopics = [];
    
    if (twitterData) {
      // 已有 Twitter 数据
      twitterContext = `
Twitter 数据：
- 提及数：${twitterData.mention_count || 0}
- 互动数：${twitterData.engagement || 0}
- 推文样本：${twitterData.sample_tweets?.slice(0, 3).join(' | ') || '无'}
`;
      twitterTopics = twitterData.topics || [];
    } else {
      // 需要搜索 Twitter
      try {
        const searchResult = await this.grokClient.searchTwitter(`$${tokenSymbol}`, 24);
        if (searchResult && searchResult.mention_count > 0) {
          twitterContext = `
Twitter 数据：
- 提及数：${searchResult.mention_count}
- 互动数：${searchResult.engagement}
- 推文样本：${searchResult.sample_tweets?.slice(0, 3).join(' | ') || '无'}
`;
          twitterTopics = searchResult.topics || [];
        }
      } catch (error) {
        console.log(`   ⚠️ Twitter search failed: ${error.message}`);
      }
    }

    // 第三步：AI 分析（不是猜测，是基于真实数据分析）
    const existingNarratives = Array.from(this.narrativesCache.keys()).join(', ');
    
    const prompt = `你是一个加密货币市场分析师。基于以下真实数据，分析这个 token 的叙事类型。

Token 信息：
- Symbol: $${tokenSymbol}
- Name: ${tokenName || '未知'}
${twitterContext}

现有叙事类型：${existingNarratives}

关键任务：
1. 分析 Twitter 讨论的主题和情绪
2. 判断属于哪个现有叙事，还是一个新叙事
3. 如果是新叙事，描述它的特征

注意：
- 如果 Twitter 数据显示这是一个新兴热点，不要硬套到现有叙事
- 加密市场变化很快，新叙事随时可能出现
- 例如：如果发现大量讨论"量子计算"或"RWA 2.0"等新概念，这可能是新叙事

用 JSON 回复：
{
  "narrative": "现有叙事名称 或 NEW:新叙事名称",
  "confidence": 0.85,
  "is_new_narrative": false,
  "new_narrative_keywords": [],
  "market_heat": 8,
  "reasoning": "基于 Twitter 讨论分析..."
}`;

    try {
      const response = await this.grokClient.askGrok(prompt);
      const jsonMatch = response.match(/\{[\s\S]*?\}/);
      
      if (jsonMatch) {
        const result = JSON.parse(jsonMatch[0]);
        
        // 如果发现新叙事，自动添加到系统
        if (result.is_new_narrative && result.narrative.startsWith('NEW:')) {
          const newNarrativeName = result.narrative.replace('NEW:', '').trim();
          await this.discoverNewNarrative(newNarrativeName, {
            keywords: result.new_narrative_keywords || [],
            market_heat: result.market_heat || 7,
            reasoning: result.reasoning,
            source_token: tokenSymbol
          });
          result.narrative = newNarrativeName;
        }
        
        const identification = {
          narrative: result.narrative,
          confidence: result.confidence || 0.7,
          reasoning: result.reasoning || 'AI 基于 Twitter 数据分析',
          is_new_narrative: result.is_new_narrative || false,
          market_heat: result.market_heat,
          source: 'grok_twitter_verified'
        };
        
        // 缓存结果
        this.recentDiscoveries.set(cacheKey, {
          result: identification,
          timestamp: Date.now()
        });
        
        return identification;
      }
    } catch (error) {
      console.log(`   ⚠️ Grok narrative analysis failed: ${error.message}`);
    }

    // 回退到关键词匹配
    const fallbackResult = keywordMatch || {
      narrative: 'Unknown',
      confidence: 0.3,
      reasoning: '无法确定叙事类型',
      source: 'fallback'
    };
    
    this.recentDiscoveries.set(cacheKey, {
      result: fallbackResult,
      timestamp: Date.now()
    });
    
    return fallbackResult;
  }

  /**
   * 自动发现并添加新叙事
   */
  async discoverNewNarrative(narrativeName, data) {
    console.log(`🆕 [AI Narrative] Discovering new narrative: ${narrativeName}`);
    
    // 检查是否已存在
    if (this.narrativesCache.has(narrativeName)) {
      console.log(`   Already exists, skipping...`);
      return;
    }
    
    // Step 1: 评估潜在影响力
    console.log(`   📊 Evaluating potential impact...`);
    const impactAnalysis = await this.evaluateNarrativeImpact(narrativeName, data);
    
    // Step 2: 如果影响力太低，不添加
    if (impactAnalysis.impact_score < 3) {
      console.log(`   ⚠️ Low impact score (${impactAnalysis.impact_score}/10), skipping...`);
      return;
    }
    
    // Step 3: 添加新叙事到数据库
    const lifecycleMultiplier = impactAnalysis.lifecycle_multiplier || 1.3;
    const weight = Math.min(10, impactAnalysis.impact_score * lifecycleMultiplier);
    
    try {
      this.db.prepare(`
        INSERT INTO ai_narratives (
          narrative_name, market_heat, sustainability, competition_level,
          lifecycle_stage, lifecycle_multiplier, keywords, ai_reasoning,
          weight, last_updated, update_source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'auto_discovered')
      `).run(
        narrativeName,
        impactAnalysis.market_heat || 7,
        impactAnalysis.sustainability || 5,
        impactAnalysis.competition_level || 'low',
        impactAnalysis.lifecycle_stage || 'early_explosion',
        lifecycleMultiplier,
        JSON.stringify(data.keywords || []),
        impactAnalysis.reasoning || `Auto-discovered from $${data.source_token}`,
        weight,
        Date.now()
      );
      
      // 重新加载缓存
      this.loadNarrativesCache();
      
      console.log(`   ✅ New narrative added: ${narrativeName}`);
      console.log(`      Impact: ${impactAnalysis.impact_score}/10`);
      console.log(`      Weight: ${weight.toFixed(1)}`);
      console.log(`      Reasoning: ${impactAnalysis.reasoning}`);
      
      // Step 4: 启动后续验证（30分钟后检查市场反应）
      this.scheduleNarrativeVerification(narrativeName);
      
    } catch (error) {
      console.log(`   ❌ Failed to add narrative: ${error.message}`);
    }
  }

  /**
   * 评估新叙事的潜在影响力
   * 
   * 分析维度：
   * 1. 市场时机 - 是否符合当前市场情绪
   * 2. 受众规模 - 潜在用户/投资者数量
   * 3. 病毒传播性 - 是否容易传播
   * 4. 资金吸引力 - 是否能吸引资金流入
   * 5. 持续性 - 是否能持续热度
   */
  async evaluateNarrativeImpact(narrativeName, data) {
    const prompt = `你是加密货币市场分析专家。评估以下新叙事的潜在影响力。

新叙事名称：${narrativeName}
初始数据：
- 关键词：${(data.keywords || []).join(', ')}
- 来源 Token：$${data.source_token || 'unknown'}
- 初始热度：${data.market_heat || 'unknown'}
- 发现原因：${data.reasoning || 'unknown'}

请从以下 5 个维度评估（每项 0-10 分）：

1. 市场时机 (market_timing)
   - 当前市场情绪是否适合这个叙事？
   - 是否有重大事件/催化剂支持？

2. 受众规模 (audience_size)
   - 潜在投资者/用户有多少？
   - 是否能吸引圈外用户？

3. 病毒传播性 (virality)
   - 是否容易在社交媒体传播？
   - 是否有 meme 潜力？

4. 资金吸引力 (capital_attraction)
   - VC/大户是否会感兴趣？
   - 是否有明确的价值主张？

5. 持续性 (sustainability)
   - 热度能持续多久？
   - 是短期炒作还是长期趋势？

同时判断：
- lifecycle_stage: early_explosion / early_growth / growth / peak / mature / decline
- competition_level: low / medium / high

用 JSON 格式回复：
{
  "impact_score": 7.5,
  "market_timing": 8,
  "audience_size": 6,
  "virality": 9,
  "capital_attraction": 7,
  "sustainability": 5,
  "lifecycle_stage": "early_explosion",
  "lifecycle_multiplier": 1.3,
  "competition_level": "low",
  "market_heat": 8,
  "reasoning": "简短的分析理由（2-3句话）",
  "risk_factors": ["风险1", "风险2"],
  "catalysts": ["催化剂1", "催化剂2"]
}`;

    try {
      const response = await this.grokClient.askGrok(prompt);
      const jsonMatch = response.match(/\{[\s\S]*\}/);
      
      if (jsonMatch) {
        const analysis = JSON.parse(jsonMatch[0]);
        
        // 计算综合影响力分数
        if (!analysis.impact_score) {
          analysis.impact_score = (
            (analysis.market_timing || 5) * 0.25 +
            (analysis.audience_size || 5) * 0.15 +
            (analysis.virality || 5) * 0.25 +
            (analysis.capital_attraction || 5) * 0.20 +
            (analysis.sustainability || 5) * 0.15
          );
        }
        
        // 设置生命周期乘数
        analysis.lifecycle_multiplier = this.getLifecycleMultiplier(analysis.lifecycle_stage);
        
        return analysis;
      }
    } catch (error) {
      console.log(`   ⚠️ Impact evaluation failed: ${error.message}`);
    }
    
    // 默认值（保守估计）
    return {
      impact_score: 5,
      market_heat: data.market_heat || 5,
      sustainability: 4,
      lifecycle_stage: 'early_growth',
      lifecycle_multiplier: 1.2,
      competition_level: 'medium',
      reasoning: 'Default evaluation - AI analysis unavailable'
    };
  }

  /**
   * 安排叙事验证（30分钟后检查市场反应）
   */
  scheduleNarrativeVerification(narrativeName) {
    console.log(`   ⏰ Scheduling verification for ${narrativeName} in 30 minutes...`);
    
    setTimeout(async () => {
      await this.verifyNarrativeReaction(narrativeName);
    }, 30 * 60 * 1000); // 30 分钟后
  }

  /**
   * 验证市场对新叙事的反应
   */
  async verifyNarrativeReaction(narrativeName) {
    console.log(`🔍 [AI Narrative] Verifying market reaction to: ${narrativeName}`);
    
    const narrative = this.narrativesCache.get(narrativeName);
    if (!narrative) {
      console.log(`   Narrative not found in cache, skipping...`);
      return;
    }
    
    // 搜索这个叙事相关的 Twitter 讨论
    const keywords = narrative.keywords || [];
    const searchQuery = keywords.slice(0, 3).join(' OR ');
    
    try {
      const twitterData = await this.grokClient.searchTwitter(searchQuery, 50);
      
      const prompt = `你是加密货币市场分析师。验证新叙事的市场反应。

叙事名称：${narrativeName}
30分钟前预测的影响力：${narrative.weight}/10

当前 Twitter 数据：
- 搜索词：${searchQuery}
- 提及数：${twitterData?.mentions || 0}
- 互动数：${twitterData?.engagement || 0}

问题：
1. 市场反应是否符合预期？
2. 热度是上升还是下降？
3. 是否需要调整影响力评分？

用 JSON 回复：
{
  "reaction": "positive" | "neutral" | "negative",
  "heat_trend": "rising" | "stable" | "declining",
  "adjusted_weight": 7.5,
  "reasoning": "简短分析"
}`;

      const response = await this.grokClient.askGrok(prompt);
      const jsonMatch = response.match(/\{[\s\S]*\}/);
      
      if (jsonMatch) {
        const verification = JSON.parse(jsonMatch[0]);
        
        // 更新数据库
        this.db.prepare(`
          UPDATE ai_narratives 
          SET weight = ?, 
              ai_reasoning = ai_reasoning || ' | 30min验证: ' || ?,
              last_updated = ?
          WHERE narrative_name = ?
        `).run(
          verification.adjusted_weight,
          verification.reasoning,
          Date.now(),
          narrativeName
        );
        
        this.loadNarrativesCache();
        
        console.log(`   ✅ Verification complete for ${narrativeName}`);
        console.log(`      Reaction: ${verification.reaction}`);
        console.log(`      Trend: ${verification.heat_trend}`);
        console.log(`      Adjusted weight: ${verification.adjusted_weight}`);
      }
    } catch (error) {
      console.log(`   ❌ Verification failed: ${error.message}`);
    }
  }

  /**
   * Match narrative by keywords (fast local matching)
   */
  matchNarrativeByKeywords(tokenSymbol, tokenName) {
    const searchText = `${tokenSymbol} ${tokenName || ''}`.toLowerCase();
    
    let bestMatch = null;
    let bestScore = 0;

    for (const [name, narrative] of this.narrativesCache) {
      const keywords = narrative.keywords || [];
      let matchCount = 0;
      const matchedKeywords = [];

      for (const keyword of keywords) {
        if (searchText.includes(keyword.toLowerCase())) {
          matchCount++;
          matchedKeywords.push(keyword);
        }
      }

      if (matchCount > 0) {
        // Score based on match density and narrative weight
        const matchDensity = matchCount / keywords.length;
        const score = matchDensity * narrative.weight;

        if (score > bestScore) {
          bestScore = score;
          bestMatch = {
            narrative: name,
            confidence: Math.min(1.0, 0.5 + matchDensity * 0.5),
            reasoning: `Matched keywords: ${matchedKeywords.join(', ')}`,
            matchedKeywords,
            source: 'keyword_match'
          };
        }
      }
    }

    return bestMatch;
  }

  /**
   * Get narrative score for a token
   * 
   * @param {string} tokenSymbol 
   * @param {string} tokenName 
   * @param {Object} twitterData 
   * @returns {Object} { score, narrative, breakdown }
   */
  async scoreNarrative(tokenSymbol, tokenName, twitterData = null) {
    // Identify narrative
    const identification = await this.identifyTokenNarrative(tokenSymbol, tokenName, twitterData);
    
    if (!identification || identification.narrative === 'Unknown') {
      // No narrative identified - give base score for pump.fun tokens
      const isPumpFun = tokenSymbol?.toLowerCase().includes('pump') || 
                        tokenName?.toLowerCase().includes('pump');
      return {
        score: isPumpFun ? 5 : 2,
        narrative: null,
        breakdown: {
          identified_narrative: 'Unknown',
          confidence: 0,
          reasoning: 'No narrative match found',
          is_pump_fun: isPumpFun
        }
      };
    }

    // Get narrative metrics from database
    const narrative = this.narrativesCache.get(identification.narrative);
    
    if (!narrative) {
      return {
        score: 3,
        narrative: identification.narrative,
        breakdown: {
          identified_narrative: identification.narrative,
          confidence: identification.confidence,
          reasoning: 'Narrative not in database',
          source: identification.source
        }
      };
    }

    // Calculate score (0-25 points for Narrative component)
    // Formula: (weight / 10) * 20 * confidence * twitter_bonus
    let score = (narrative.weight / 10) * 20 * identification.confidence;

    // Twitter validation bonus
    let twitterBonus = 1.0;
    if (twitterData && twitterData.mention_count >= 10) {
      twitterBonus = 1.2;  // 20% bonus
      score *= twitterBonus;
    }

    // Cap at 25
    score = Math.min(25, Math.round(score));

    return {
      score,
      narrative: identification.narrative,
      breakdown: {
        identified_narrative: identification.narrative,
        narrative_weight: narrative.weight,
        market_heat: narrative.market_heat,
        lifecycle_stage: narrative.lifecycle_stage,
        sustainability: narrative.sustainability,
        confidence: identification.confidence,
        twitter_bonus: twitterBonus,
        reasoning: identification.reasoning,
        ai_reasoning: narrative.ai_reasoning,
        source: identification.source
      }
    };
  }

  /**
   * Get lifecycle multiplier based on stage
   */
  getLifecycleMultiplier(stage) {
    const multipliers = {
      'early_explosion': 1.3,
      'early_growth': 1.2,
      'growth': 1.1,
      'peak': 1.0,
      'mature': 0.9,
      'decline': 0.5,
      'evergreen': 1.0,
      'unknown': 0.8
    };
    return multipliers[stage] || 0.8;
  }

  /**
   * Get all narratives ranked by weight
   */
  getRankedNarratives() {
    return Array.from(this.narrativesCache.values())
      .sort((a, b) => b.weight - a.weight)
      .map(n => ({
        name: n.narrative_name,
        weight: n.weight,
        market_heat: n.market_heat,
        lifecycle_stage: n.lifecycle_stage,
        sustainability: n.sustainability,
        last_updated: new Date(n.last_updated).toISOString()
      }));
  }

  /**
   * Manually add/update a narrative
   */
  addOrUpdateNarrative(narrativeData) {
    const lifecycleMultiplier = this.getLifecycleMultiplier(narrativeData.lifecycle_stage);
    const competitionScore = narrativeData.competition_level === 'low' ? 8 : 
                             narrativeData.competition_level === 'medium' ? 5 : 2;
    const weight = (
      narrativeData.market_heat * 0.4 +
      narrativeData.sustainability * 0.3 +
      competitionScore * 0.2 +
      5 * 0.1
    ) * lifecycleMultiplier;

    this.db.prepare(`
      INSERT OR REPLACE INTO ai_narratives (
        narrative_name, market_heat, sustainability, competition_level,
        lifecycle_stage, lifecycle_multiplier, keywords, ai_reasoning,
        weight, last_updated, update_source
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'manual')
    `).run(
      narrativeData.name,
      narrativeData.market_heat,
      narrativeData.sustainability,
      narrativeData.competition_level,
      narrativeData.lifecycle_stage,
      lifecycleMultiplier,
      JSON.stringify(narrativeData.keywords || []),
      narrativeData.reasoning || '',
      Math.min(10, weight),
      Date.now()
    );

    // Reload cache
    this.loadNarrativesCache();
  }
}

export default AINarrativeSystem;
