/**
 * Narrative Detection Module
 *
 * Data-driven narrative detection using quantifiable market metrics
 * Weights derived from real market data (CoinGecko, DeFi Llama, Messari)
 *
 * Methodology: NARRATIVE-SCORING-FRAMEWORK.md
 * Formula: Market Heat (40%) + Sustainability (30%) + Competition (20%) + Historical (10%) × Lifecycle
 */

export class NarrativeDetector {
  constructor() {
    // Current narrative weights as of December 2025
    // Derived from real market data - see NARRATIVE-SCORING-FRAMEWORK.md for methodology
    this.narratives = {
      'AI_Agents': {
        weight: 10,
        // Data justification:
        // - Market Heat: 9.2/10 (22.39% web traffic share - CoinGecko)
        // - Growth: +245% (FET, RNDR, TAO validated)
        // - Lifecycle: 1.3x (Early explosion phase, 0-3 months)
        keywords: [
          'ai', 'agent', 'autonomous', 'llm', 'gpt', 'neural', 'bot', 'machine learning',
          'artificial intelligence', 'ml', 'deep learning', 'chatbot', 'assistant'
        ],
        patterns: [
          /\bai\s+agent/i,
          /autonomous\s+ai/i,
          /llm\s+powered/i,
          /gpt\s*-?\s*\d+/i,
          /neural\s+network/i
        ]
      },

      'Meme_Coins': {
        weight: 10,
        // Data justification:
        // - Market Heat: 9.8/10 (25.02% web traffic share - highest)
        // - Historical: +33.08% average PnL (verified 2024 data)
        // - Lifecycle: 1.0x (Evergreen theme)
        // - Note: Low sustainability (3.9) but high competition tolerance
        keywords: [
          'meme', 'pepe', 'doge', 'shib', 'wojak', 'community', 'viral', 'frog',
          'dog', 'cat', 'shiba', 'inu', 'floki', 'elon', 'dogecoin', 'shitcoin',
          'fun', 'moon', 'ape', 'chad', 'based', 'giga'
        ],
        patterns: [
          /\b(pepe|doge|shib|wojak)\b/i,
          /meme\s+coin/i,
          /community\s+driven/i,
          /\b(moon|ape|based)\b/i
        ]
      },

      'Prediction_Markets': {
        weight: 9,
        // Data justification:
        // - Market Heat: 8.5/10 (Polymarket reached $3.2B volume)
        // - Sustainability: 7.8/10 (Real utility, Trump admin support)
        // - Historical: 8.1/10 (+120% OMEN, +95% AZUR in 2024)
        // - Lifecycle: 1.2x (Growth phase, 3-12 months)
        keywords: [
          'prediction', 'betting', 'forecast', 'polymarket', 'augur', 'gnosis',
          'market', 'odds', 'outcome', 'event', 'oracle', 'consensus', 'wisdom of crowds'
        ],
        patterns: [
          /prediction\s+market/i,
          /betting\s+protocol/i,
          /forecast\s+platform/i
        ]
      },

      'RWA': {
        weight: 8,
        // Data justification:
        // - Market Heat: 7.9/10 (11% web traffic, +85% YoY growth)
        // - Sustainability: 9.1/10 (BlackRock $589M tokenization fund)
        // - Competition: 6.8/10 (Growing but not saturated)
        // - Lifecycle: 1.1x (Mature growth, 12-18 months)
        keywords: [
          'rwa', 'real world asset', 'tokenized', 'tokenization', 'property', 'real estate',
          'commodities', 'treasury', 'bond', 'securities', 'asset backed', 'blackrock', 'ondo'
        ],
        patterns: [
          /real\s+world\s+asset/i,
          /rwa/i,
          /tokenized\s+(real\s+estate|bond|treasury)/i,
          /asset\s+backed/i
        ]
      },

      'DeFi': {
        weight: 7,
        // Data justification:
        // - Market Heat: 6.8/10 (Mature but stable)
        // - Sustainability: 8.2/10 (Core infrastructure, proven utility)
        // - Historical: 5.5/10 (Moderate growth, +15-25%)
        // - Lifecycle: 0.9x (Mature phase, >18 months)
        keywords: [
          'defi', 'decentralized finance', 'yield', 'farming', 'liquidity', 'amm',
          'dex', 'swap', 'lending', 'borrowing', 'staking', 'pool', 'vault', 'protocol'
        ],
        patterns: [
          /defi/i,
          /decentralized\s+finance/i,
          /yield\s+farming/i,
          /liquidity\s+pool/i,
          /automated\s+market\s+maker/i
        ]
      },

      'Layer2_Scaling': {
        weight: 6,
        // Data justification:
        // - Market Heat: 5.9/10 (Established tech, less hype)
        // - Sustainability: 8.5/10 (Critical infrastructure)
        // - Competition: 4.2/10 (Saturated - 50+ L2s)
        // - Historical: 4.8/10 (Mixed performance, fragmentation)
        keywords: [
          'layer 2', 'l2', 'rollup', 'optimistic', 'zk', 'zero knowledge', 'scaling',
          'arbitrum', 'optimism', 'base', 'polygon', 'zksync', 'starknet', 'throughput'
        ],
        patterns: [
          /layer\s*2/i,
          /l2/i,
          /rollup/i,
          /zk\s*(rollup|sync)?/i,
          /zero\s+knowledge/i
        ]
      },

      'SocialFi': {
        weight: 4,
        // Data justification:
        // - Market Heat: 3.2/10 (Friend.tech collapse, low interest)
        // - Sustainability: 2.8/10 (No lasting adoption)
        // - Historical: 1.5/10 (Most projects failed in 2024)
        // - Lifecycle: 0.5x (Declining phase)
        keywords: [
          'socialfi', 'social', 'friend.tech', 'social token', 'creator', 'influencer',
          'social network', 'web3 social'
        ],
        patterns: [
          /socialfi/i,
          /social\s+(token|network|platform)/i,
          /creator\s+economy/i
        ]
      },

      'Gaming_Metaverse': {
        weight: 1,
        // Data justification:
        // - Market Heat: 1.8/10 (-93% funding decline per Messari)
        // - Sustainability: 2.5/10 (No real adoption)
        // - Historical: 0.9/10 (Failed in 2024)
        // - Lifecycle: 0.4x (Death phase)
        // AVOID: This is a NEGATIVE signal
        keywords: [
          'gaming', 'game', 'metaverse', 'play to earn', 'p2e', 'nft game', 'gamefi',
          'virtual world', 'avatar', 'play2earn', 'game token'
        ],
        patterns: [
          /gaming/i,
          /metaverse/i,
          /play\s*to\s*earn/i,
          /p2e/i,
          /gamefi/i
        ]
      }
    };

    // Special bonus multipliers for Twitter validation
    this.twitterBonus = {
      // If Twitter shows narrative confirmation, add bonus
      minMentions: 10,      // Need at least 10 mentions for validation
      bonusMultiplier: 1.2  // 20% bonus if Twitter confirms narrative
    };
  }

  /**
   * Detect narratives in token data
   *
   * @param {Object} tokenData - Token metadata
   * @param {string} tokenData.name - Token name
   * @param {string} tokenData.symbol - Token symbol
   * @param {string} tokenData.description - Token description (if available)
   * @param {string} tokenData.token_ca - Token contract address (for pump.fun detection)
   * @param {Object} twitterData - Twitter data from Grok API (optional)
   * @returns {Object} { narratives: [], topNarrative: {}, score: 0-25 }
   */
  detect(tokenData, twitterData = null) {
    const detectedNarratives = [];

    // 🚀 PUMP.FUN DETECTION: Check if this is a Bonding Curve token
    const isPumpFun = tokenData.token_ca && tokenData.token_ca.toLowerCase().endsWith('pump');

    // 🚀 PUMP.FUN TOLERANCE: If metadata is missing but it's a pump.fun token, use symbol/name only
    // Metadata lag is expected for fresh pump.fun tokens due to RPC indexing delay
    const hasMetadata = tokenData.name || tokenData.symbol || tokenData.description;

    if (isPumpFun && !hasMetadata) {
      console.log(`🚀 [Pump.fun] Metadata lag detected - using baseline score`);
      // Return baseline score for fresh pump.fun tokens
      return {
        narratives: [],
        topNarrative: null,
        score: 5,  // Baseline 5 points (out of 25) for unknown narrative
        breakdown: {
          base_weight: 0,
          confidence: 0,
          twitter_validated: false,
          pump_fun_baseline: true,
          reason: 'Metadata not yet indexed (expected for fresh pump.fun tokens)'
        }
      };
    }

    // Combine all searchable text
    const searchText = [
      tokenData.name || '',
      tokenData.symbol || '',
      tokenData.description || ''
    ].join(' ').toLowerCase();

    // Check each narrative
    for (const [narrativeName, narrativeConfig] of Object.entries(this.narratives)) {
      const matches = this._matchNarrative(searchText, narrativeConfig);

      if (matches.found) {
        detectedNarratives.push({
          name: narrativeName,
          weight: narrativeConfig.weight,
          confidence: matches.confidence,
          matchedKeywords: matches.keywords,
          matchedPatterns: matches.patterns
        });
      }
    }

    // Sort by weight * confidence
    detectedNarratives.sort((a, b) =>
      (b.weight * b.confidence) - (a.weight * a.confidence)
    );

    // Get top narrative
    const topNarrative = detectedNarratives.length > 0 ? detectedNarratives[0] : null;

    // Calculate score (0-25 points for Narrative component)
    let score = 0;

    if (topNarrative) {
      // Base score from narrative weight (0-10 maps to 0-20 points)
      score = (topNarrative.weight / 10) * 20;

      // Confidence adjustment (0.5-1.0 confidence)
      score = score * topNarrative.confidence;

      // Twitter validation bonus
      if (twitterData && this._twitterValidatesNarrative(twitterData, topNarrative)) {
        score = score * this.twitterBonus.bonusMultiplier;
        console.log(`   ✨ Twitter validates ${topNarrative.name} narrative (+20% bonus)`);
      }

      // Cap at 25 points (max for Narrative component)
      score = Math.min(25, score);
    }

    return {
      narratives: detectedNarratives,
      topNarrative,
      score: Math.round(score),
      breakdown: {
        base_weight: topNarrative ? topNarrative.weight : 0,
        confidence: topNarrative ? topNarrative.confidence : 0,
        twitter_validated: twitterData && topNarrative ?
          this._twitterValidatesNarrative(twitterData, topNarrative) : false
      }
    };
  }

  /**
   * Match narrative against token text
   *
   * @private
   */
  _matchNarrative(searchText, narrativeConfig) {
    const matchedKeywords = [];
    const matchedPatterns = [];

    // Keyword matching
    for (const keyword of narrativeConfig.keywords) {
      if (searchText.includes(keyword.toLowerCase())) {
        matchedKeywords.push(keyword);
      }
    }

    // Pattern matching (regex)
    for (const pattern of narrativeConfig.patterns) {
      if (pattern.test(searchText)) {
        matchedPatterns.push(pattern.source);
      }
    }

    const totalMatches = matchedKeywords.length + matchedPatterns.length;
    const found = totalMatches > 0;

    // Confidence based on match density
    // More matches = higher confidence
    const confidence = found ?
      Math.min(1.0, 0.5 + (totalMatches * 0.1)) : 0;

    return {
      found,
      confidence,
      keywords: matchedKeywords,
      patterns: matchedPatterns
    };
  }

  /**
   * Check if Twitter data validates the narrative
   *
   * @private
   */
  _twitterValidatesNarrative(twitterData, narrative) {
    if (!twitterData || !twitterData.mention_count) {
      return false;
    }

    // Need minimum mentions for validation
    if (twitterData.mention_count < this.twitterBonus.minMentions) {
      return false;
    }

    // Check if Twitter mentions include narrative keywords
    if (!twitterData.top_tweets || twitterData.top_tweets.length === 0) {
      return false;
    }

    const narrativeConfig = this.narratives[narrative.name];
    if (!narrativeConfig) {
      return false;
    }

    // Check if any top tweets mention narrative keywords
    const tweetTexts = twitterData.top_tweets
      .map(tweet => tweet.text || '')
      .join(' ')
      .toLowerCase();

    for (const keyword of narrativeConfig.keywords) {
      if (tweetTexts.includes(keyword.toLowerCase())) {
        return true;
      }
    }

    return false;
  }

  /**
   * Get current narrative rankings
   *
   * @returns {Array} Sorted list of narratives by weight
   */
  getCurrentRankings() {
    return Object.entries(this.narratives)
      .map(([name, config]) => ({
        name,
        weight: config.weight,
        keywords: config.keywords.slice(0, 5) // Top 5 keywords
      }))
      .sort((a, b) => b.weight - a.weight);
  }
}

export default NarrativeDetector;
