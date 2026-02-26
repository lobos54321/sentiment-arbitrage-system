/**
 * AI-Powered Influencer Discovery System
 * 
 * Uses Grok AI to:
 * 1. Discover and rank Twitter KOLs (Key Opinion Leaders)
 * 2. Discover and rank Telegram channels
 * 3. Real-time detection of influencer mentions for tokens
 * 
 * Data is stored in database and updated weekly
 */

import GrokTwitterClient from '../social/grok-twitter-client.js';

export class AIInfluencerSystem {
  constructor(config, db) {
    this.config = config;
    this.db = db;
    this.grokClient = new GrokTwitterClient();
    
    // Initialize database tables
    this.initializeDatabase();
    
    // Load into memory cache
    this.kolCache = new Map();
    this.channelCache = new Map();
    this.loadCaches();
  }

  /**
   * Initialize influencer tables in database
   */
  initializeDatabase() {
    // Twitter KOLs table
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS ai_twitter_kols (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        handle TEXT UNIQUE NOT NULL,
        display_name TEXT,
        
        -- Metrics
        followers INTEGER DEFAULT 0,
        influence_score REAL DEFAULT 5,
        tier TEXT DEFAULT 'Tier3',
        
        -- Focus areas
        focus_areas TEXT,  -- JSON array: ['meme', 'defi', 'ai']
        chains TEXT,       -- JSON array: ['SOL', 'ETH', 'BSC']
        
        -- Track record
        avg_call_performance REAL,  -- Average % gain of called tokens
        total_calls INTEGER DEFAULT 0,
        win_rate REAL,
        
        -- AI assessment
        ai_notes TEXT,
        reliability_score REAL DEFAULT 5,
        
        -- Status
        is_active INTEGER DEFAULT 1,
        last_updated INTEGER,
        update_source TEXT,
        
        created_at INTEGER DEFAULT (strftime('%s', 'now'))
      );
      
      CREATE INDEX IF NOT EXISTS idx_kol_handle ON ai_twitter_kols(handle);
      CREATE INDEX IF NOT EXISTS idx_kol_tier ON ai_twitter_kols(tier);
      CREATE INDEX IF NOT EXISTS idx_kol_influence ON ai_twitter_kols(influence_score DESC);
    `);

    // Telegram channels table (enhanced version)
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS ai_telegram_channels (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        display_name TEXT,
        
        -- Metrics
        subscriber_count INTEGER DEFAULT 0,
        influence_score REAL DEFAULT 5,
        tier TEXT DEFAULT 'C',
        
        -- Focus areas
        focus_areas TEXT,  -- JSON array
        chains TEXT,       -- JSON array
        
        -- Track record
        avg_signal_performance REAL,
        total_signals INTEGER DEFAULT 0,
        win_rate REAL,
        avg_time_advantage_min REAL,  -- How early compared to others
        
        -- Quality metrics
        signal_frequency TEXT,  -- 'high', 'medium', 'low'
        noise_ratio REAL,       -- % of messages that are NOT signals
        
        -- AI assessment
        ai_notes TEXT,
        reliability_score REAL DEFAULT 5,
        
        -- Status
        is_monitored INTEGER DEFAULT 0,
        is_active INTEGER DEFAULT 1,
        last_updated INTEGER,
        update_source TEXT,
        
        created_at INTEGER DEFAULT (strftime('%s', 'now'))
      );
      
      CREATE INDEX IF NOT EXISTS idx_channel_username ON ai_telegram_channels(username);
      CREATE INDEX IF NOT EXISTS idx_channel_tier ON ai_telegram_channels(tier);
      CREATE INDEX IF NOT EXISTS idx_channel_monitored ON ai_telegram_channels(is_monitored);
    `);

    // Check if we need to seed initial data
    const kolCount = this.db.prepare('SELECT COUNT(*) as count FROM ai_twitter_kols').get();
    const channelCount = this.db.prepare('SELECT COUNT(*) as count FROM ai_telegram_channels').get();
    
    if (kolCount.count === 0) {
      this.seedInitialKOLs();
    }
    if (channelCount.count === 0) {
      this.seedInitialChannels();
    }
  }

  /**
   * Seed initial known KOLs
   */
  seedInitialKOLs() {
    const initialKOLs = [
      // ===== Tier 1 - Major influencers (>300k followers, massive market impact) =====
      {
        handle: 'blknoiz06',
        display_name: 'Ansem',
        followers: 650000,
        tier: 'Tier1',
        influence_score: 9.5,
        focus_areas: ['meme', 'solana'],
        chains: ['SOL'],
        reliability_score: 7.5,
        ai_notes: 'Top Solana meme coin caller. Massive following, single tweet can 10x a coin.'
      },
      {
        handle: 'MustStopMurad',
        display_name: 'Murad',
        followers: 450000,
        tier: 'Tier1',
        influence_score: 9.0,
        focus_areas: ['meme', 'culture'],
        chains: ['SOL', 'ETH'],
        reliability_score: 8.0,
        ai_notes: 'Meme coin maximalist. Memecoin supercycle thesis. Strong cult following.'
      },
      {
        handle: 'AshCryptoReal',
        display_name: 'Ash Crypto',
        followers: 400000,
        tier: 'Tier1',
        influence_score: 8.5,
        focus_areas: ['trading', 'meme', 'macro'],
        chains: ['SOL', 'ETH', 'BTC'],
        reliability_score: 7.5,
        ai_notes: 'Veteran trader with deep market wisdom. Covers meme coins and broader market.'
      },
      {
        handle: 'LarpVonTrier',
        display_name: 'Larp Von Trier',
        followers: 350000,
        tier: 'Tier1',
        influence_score: 8.5,
        focus_areas: ['meme', 'degen'],
        chains: ['SOL'],
        reliability_score: 7.0,
        ai_notes: 'Sharp humor with market analysis. Frequent meme coin commentary.'
      },
      
      // ===== Tier 2 - Significant influencers (100k-300k followers) =====
      {
        handle: 'Poe_Ether',
        display_name: 'POE',
        followers: 280000,
        tier: 'Tier2',
        influence_score: 8.0,
        focus_areas: ['meme', 'early_calls'],
        chains: ['SOL', 'ETH'],
        reliability_score: 7.5,
        ai_notes: 'Bold early calls on emerging meme coins before mainstream.'
      },
      {
        handle: 'HsakaTrades',
        display_name: 'Hsaka',
        followers: 250000,
        tier: 'Tier2',
        influence_score: 7.5,
        focus_areas: ['trading', 'analysis'],
        chains: ['SOL', 'ETH'],
        reliability_score: 8.0,
        ai_notes: 'Technical analysis focused. More selective with calls.'
      },
      {
        handle: 'artsch00lreject',
        display_name: 'Artschool Reject',
        followers: 220000,
        tier: 'Tier2',
        influence_score: 7.5,
        focus_areas: ['meme', 'culture'],
        chains: ['SOL'],
        reliability_score: 7.0,
        ai_notes: 'Blends art and market sentiment. Spots viral meme potential.'
      },
      {
        handle: 'thecexoffender',
        display_name: 'CEX Offender',
        followers: 200000,
        tier: 'Tier2',
        influence_score: 7.0,
        focus_areas: ['degen', 'meme'],
        chains: ['SOL', 'ETH'],
        reliability_score: 6.5,
        ai_notes: 'Edgy humor, unfiltered commentary on degen launches.'
      },
      {
        handle: 'DegenSpartan',
        display_name: 'Degen Spartan',
        followers: 200000,
        tier: 'Tier2',
        influence_score: 7.0,
        focus_areas: ['defi', 'yield'],
        chains: ['ETH', 'SOL'],
        reliability_score: 7.5,
        ai_notes: 'DeFi focused, known for finding yield opportunities.'
      },
      {
        handle: 'CryptoWizardd',
        display_name: 'Wizard',
        followers: 180000,
        tier: 'Tier2',
        influence_score: 7.0,
        focus_areas: ['meme', 'ai'],
        chains: ['SOL'],
        reliability_score: 6.5,
        ai_notes: 'Active in AI agent and meme narratives.'
      },
      {
        handle: 'inversebrah',
        display_name: 'Inversebrah',
        followers: 150000,
        tier: 'Tier2',
        influence_score: 6.5,
        focus_areas: ['trading', 'macro'],
        chains: ['SOL', 'ETH', 'BTC'],
        reliability_score: 7.0,
        ai_notes: 'Contrarian views, macro analysis.'
      },
      {
        handle: 'arrogantfrfr',
        display_name: 'Modest',
        followers: 140000,
        tier: 'Tier2',
        influence_score: 6.5,
        focus_areas: ['meme', 'degen'],
        chains: ['SOL'],
        reliability_score: 6.5,
        ai_notes: 'Transparent meme coin calls, strong community engagement.'
      },
      {
        handle: 'iambroots',
        display_name: 'Broots',
        followers: 130000,
        tier: 'Tier2',
        influence_score: 6.5,
        focus_areas: ['meme', 'trading'],
        chains: ['SOL'],
        reliability_score: 7.0,
        ai_notes: 'Consistent meme coin analysis and community engagement.'
      },
      
      // ===== Tier 3 - Notable influencers (50k-100k followers) =====
      {
        handle: 'UniswapVillain',
        display_name: 'Uniswap Villain',
        followers: 95000,
        tier: 'Tier3',
        influence_score: 6.0,
        focus_areas: ['defi', 'meme'],
        chains: ['ETH', 'SOL'],
        reliability_score: 6.0,
        ai_notes: 'DeFi and meme coin plays.'
      },
      {
        handle: 'CrashiusClay69',
        display_name: 'Crash',
        followers: 85000,
        tier: 'Tier3',
        influence_score: 5.5,
        focus_areas: ['meme', 'degen'],
        chains: ['SOL'],
        reliability_score: 5.5,
        ai_notes: 'High risk degen plays.'
      },
      {
        handle: 'CryptoWendyO',
        display_name: 'Wendy O',
        followers: 80000,
        tier: 'Tier3',
        influence_score: 5.5,
        focus_areas: ['trading', 'education'],
        chains: ['SOL', 'ETH'],
        reliability_score: 7.0,
        ai_notes: 'Active trader, educational content on risk management.'
      },
      {
        handle: 'gameaboris',
        display_name: 'Gameaon',
        followers: 80000,
        tier: 'Tier3',
        influence_score: 5.5,
        focus_areas: ['meme', 'gaming'],
        chains: ['SOL'],
        reliability_score: 6.0,
        ai_notes: 'Gaming and meme focus.'
      },
      {
        handle: 'CryptoGodJohn',
        display_name: 'John',
        followers: 70000,
        tier: 'Tier3',
        influence_score: 5.0,
        focus_areas: ['meme'],
        chains: ['SOL', 'BSC'],
        reliability_score: 5.5,
        ai_notes: 'Frequent caller, high volume.'
      },
      {
        handle: 'larpalt',
        display_name: 'o.o',
        followers: 65000,
        tier: 'Tier3',
        influence_score: 5.0,
        focus_areas: ['meme', 'degen'],
        chains: ['SOL'],
        reliability_score: 5.5,
        ai_notes: 'Community engagement focused.'
      }
    ];

    const insertStmt = this.db.prepare(`
      INSERT INTO ai_twitter_kols (
        handle, display_name, followers, tier, influence_score,
        focus_areas, chains, reliability_score, ai_notes,
        last_updated, update_source
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'initial')
    `);

    for (const kol of initialKOLs) {
      insertStmt.run(
        kol.handle,
        kol.display_name,
        kol.followers,
        kol.tier,
        kol.influence_score,
        JSON.stringify(kol.focus_areas),
        JSON.stringify(kol.chains),
        kol.reliability_score,
        kol.ai_notes,
        Date.now()
      );
    }

    console.log(`✅ [AI Influencer] Seeded ${initialKOLs.length} initial Twitter KOLs`);
  }

  /**
   * Seed initial known Telegram channels
   */
  seedInitialChannels() {
    const initialChannels = [
      // Tier A - Top quality channels
      {
        username: '@MomentumTrackerCN',
        display_name: 'Momentum Tracker CN',
        subscriber_count: 50000,
        tier: 'A',
        influence_score: 9.0,
        focus_areas: ['aggregator', 'smart_money'],
        chains: ['SOL'],
        reliability_score: 8.5,
        signal_frequency: 'high',
        avg_time_advantage_min: 5,
        ai_notes: 'Top aggregator channel. Fast signal delivery. Tracks smart money.'
      },
      {
        username: '@SOLSmartAlert',
        display_name: 'SOL Smart Alert',
        subscriber_count: 30000,
        tier: 'A',
        influence_score: 8.5,
        focus_areas: ['smart_money', 'whale'],
        chains: ['SOL'],
        reliability_score: 8.0,
        signal_frequency: 'medium',
        avg_time_advantage_min: 8,
        ai_notes: 'Whale wallet tracking. Quality over quantity.'
      },
      {
        username: '@CXOStrategyBot',
        display_name: 'CXO Strategy Bot',
        subscriber_count: 25000,
        tier: 'A',
        influence_score: 8.0,
        focus_areas: ['strategy', 'analysis'],
        chains: ['SOL', 'ETH'],
        reliability_score: 8.5,
        signal_frequency: 'low',
        avg_time_advantage_min: 10,
        ai_notes: 'Strategy focused. Lower frequency but higher quality.'
      },
      
      // Tier B - Good quality channels
      {
        username: '@DexscreenerBoostAlerts',
        display_name: 'DexScreener Boost Alerts',
        subscriber_count: 40000,
        tier: 'B',
        influence_score: 7.0,
        focus_areas: ['dexscreener', 'boost'],
        chains: ['SOL', 'ETH', 'BSC'],
        reliability_score: 6.5,
        signal_frequency: 'high',
        avg_time_advantage_min: 2,
        ai_notes: 'Automated DexScreener boost alerts. High volume, need filtering.'
      },
      {
        username: '@gem1000xpump',
        display_name: 'Gem 1000x Pump',
        subscriber_count: 20000,
        tier: 'B',
        influence_score: 6.0,
        focus_areas: ['gems', 'pump'],
        chains: ['SOL'],
        reliability_score: 5.5,
        signal_frequency: 'high',
        avg_time_advantage_min: 3,
        ai_notes: 'High volume gem calls. Many misses but some hits.'
      },
      {
        username: '@Xiao_Trading',
        display_name: 'Xiao Trading',
        subscriber_count: 15000,
        tier: 'B',
        influence_score: 6.5,
        focus_areas: ['trading', 'analysis'],
        chains: ['SOL'],
        reliability_score: 7.0,
        signal_frequency: 'medium',
        avg_time_advantage_min: 5,
        ai_notes: 'Chinese trader. Good analysis, medium frequency.'
      },
      {
        username: '@Picgemscalls',
        display_name: 'Pic Gems Calls',
        subscriber_count: 12000,
        tier: 'B',
        influence_score: 5.5,
        focus_areas: ['gems'],
        chains: ['SOL'],
        reliability_score: 5.0,
        signal_frequency: 'high',
        avg_time_advantage_min: 4,
        ai_notes: 'Gem hunting channel. Mixed results.'
      },
      
      // Tier C - Lower quality / Unknown
      {
        username: '@wedegentheyaped',
        display_name: 'We Degen They Aped',
        subscriber_count: 8000,
        tier: 'C',
        influence_score: 4.0,
        focus_areas: ['degen', 'meme'],
        chains: ['SOL'],
        reliability_score: 4.0,
        signal_frequency: 'high',
        avg_time_advantage_min: 1,
        ai_notes: 'Degen focused. High noise ratio.'
      },
      {
        username: '@nhn0x69420',
        display_name: 'NHN',
        subscriber_count: 5000,
        tier: 'C',
        influence_score: 3.5,
        focus_areas: ['meme'],
        chains: ['SOL'],
        reliability_score: 4.5,
        signal_frequency: 'medium',
        avg_time_advantage_min: 0,
        ai_notes: 'Small channel. Unverified track record.'
      }
    ];

    const insertStmt = this.db.prepare(`
      INSERT INTO ai_telegram_channels (
        username, display_name, subscriber_count, tier, influence_score,
        focus_areas, chains, reliability_score, signal_frequency,
        avg_time_advantage_min, ai_notes, is_monitored,
        last_updated, update_source
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, 'initial')
    `);

    for (const ch of initialChannels) {
      insertStmt.run(
        ch.username,
        ch.display_name,
        ch.subscriber_count,
        ch.tier,
        ch.influence_score,
        JSON.stringify(ch.focus_areas),
        JSON.stringify(ch.chains),
        ch.reliability_score,
        ch.signal_frequency,
        ch.avg_time_advantage_min,
        ch.ai_notes,
        Date.now()
      );
    }

    console.log(`✅ [AI Influencer] Seeded ${initialChannels.length} initial Telegram channels`);
  }

  /**
   * Load caches from database
   */
  loadCaches() {
    // Load KOLs
    const kols = this.db.prepare('SELECT * FROM ai_twitter_kols WHERE is_active = 1').all();
    this.kolCache.clear();
    for (const kol of kols) {
      this.kolCache.set(kol.handle.toLowerCase(), {
        ...kol,
        focus_areas: JSON.parse(kol.focus_areas || '[]'),
        chains: JSON.parse(kol.chains || '[]')
      });
    }

    // Load channels
    const channels = this.db.prepare('SELECT * FROM ai_telegram_channels WHERE is_active = 1').all();
    this.channelCache.clear();
    for (const ch of channels) {
      // Store by multiple keys for easier lookup
      const key = ch.username.toLowerCase().replace('@', '');
      this.channelCache.set(key, {
        ...ch,
        focus_areas: JSON.parse(ch.focus_areas || '[]'),
        chains: JSON.parse(ch.chains || '[]')
      });
      // Also store with @ prefix
      this.channelCache.set(ch.username.toLowerCase(), this.channelCache.get(key));
    }

    console.log(`📚 [AI Influencer] Loaded ${kols.length} KOLs and ${channels.length} channels`);
  }

  /**
   * Weekly discovery - Use Grok to find new KOLs and channels
   */
  async weeklyDiscovery() {
    console.log('🔍 [AI Influencer] Starting weekly discovery...');

    // Discover new KOLs
    await this.discoverNewKOLs();
    
    // Discover new channels
    await this.discoverNewChannels();
    
    // Update existing entries
    await this.updateExistingInfluencers();
    
    // Auto-sync high quality channels to monitoring
    this.autoSyncMonitoring();
    
    // Reload caches
    this.loadCaches();
    
    console.log('✅ [AI Influencer] Weekly discovery complete');
  }

  /**
   * Automatically sync high quality channels to monitoring list
   */
  autoSyncMonitoring() {
    // Mark high quality channels as monitored
    const updated = this.db.prepare(`
      UPDATE ai_telegram_channels 
      SET is_monitored = 1 
      WHERE (tier = 'A' OR (tier = 'B' AND influence_score >= 7))
        AND is_monitored = 0
    `).run();
    
    if (updated.changes > 0) {
      console.log(`   📱 Auto-enabled monitoring for ${updated.changes} high-quality channels`);
    }
    
    // Sync to telegram_channels table
    const monitoredChannels = this.db.prepare(`
      SELECT username, display_name, tier 
      FROM ai_telegram_channels 
      WHERE is_monitored = 1
    `).all();
    
    const insertStmt = this.db.prepare(`
      INSERT OR IGNORE INTO telegram_channels (channel_name, channel_username, tier, active)
      VALUES (?, ?, ?, 1)
    `);
    
    let synced = 0;
    for (const ch of monitoredChannels) {
      const result = insertStmt.run(
        ch.display_name || ch.username.replace('@', ''),
        ch.username,
        ch.tier
      );
      if (result.changes > 0) synced++;
    }
    
    if (synced > 0) {
      console.log(`   📱 Synced ${synced} channels to active monitoring list`);
    }
  }

  /**
   * Use Grok to discover new crypto Twitter KOLs
   */
  async discoverNewKOLs() {
    const prompt = `You are a crypto market analyst. Identify the top 20 most influential crypto Twitter/X accounts as of December 2024.

Focus on accounts that:
1. Actively call/recommend meme coins or new tokens
2. Have significant follower counts (>50k)
3. Are known for moving markets with their calls
4. Focus on Solana, Ethereum, or BSC ecosystems

For each account provide:
- handle (Twitter username without @)
- display_name
- estimated_followers
- focus_areas (array: 'meme', 'defi', 'ai', 'trading', 'macro', 'nft')
- primary_chains (array: 'SOL', 'ETH', 'BSC', 'BTC')
- tier ('Tier1' for >300k followers, 'Tier2' for 100k-300k, 'Tier3' for 50k-100k)
- reliability_notes (brief assessment of their track record)

Respond in JSON format:
{
  "kols": [
    {
      "handle": "ansaboris",
      "display_name": "Ansem",
      "estimated_followers": 600000,
      "focus_areas": ["meme", "solana"],
      "primary_chains": ["SOL"],
      "tier": "Tier1",
      "reliability_notes": "Major SOL meme coin influencer, moves markets"
    }
  ]
}`;

    try {
      const response = await this.grokClient.askGrok(prompt);
      const jsonMatch = response.match(/\{[\s\S]*\}/);
      
      if (!jsonMatch) {
        console.log('   ⚠️ Could not parse KOL discovery response');
        return;
      }

      const data = JSON.parse(jsonMatch[0]);
      let newCount = 0;

      const insertStmt = this.db.prepare(`
        INSERT OR IGNORE INTO ai_twitter_kols (
          handle, display_name, followers, tier, influence_score,
          focus_areas, chains, ai_notes, last_updated, update_source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'ai_discovery')
      `);

      for (const kol of data.kols || []) {
        // Calculate influence score
        const influenceScore = this.calculateKOLInfluenceScore(kol);
        
        const result = insertStmt.run(
          kol.handle,
          kol.display_name,
          kol.estimated_followers,
          kol.tier,
          influenceScore,
          JSON.stringify(kol.focus_areas || []),
          JSON.stringify(kol.primary_chains || []),
          kol.reliability_notes,
          Date.now()
        );

        if (result.changes > 0) {
          newCount++;
        }
      }

      console.log(`   ✅ Discovered ${newCount} new KOLs`);
      
    } catch (error) {
      console.error('   ❌ KOL discovery failed:', error.message);
    }
  }

  /**
   * Use Grok to discover new Telegram channels
   */
  async discoverNewChannels() {
    const prompt = `You are a crypto market analyst. Identify the top 15 most important Telegram channels for crypto signal discovery as of December 2024.

Focus on channels that:
1. Provide early signals for new token launches
2. Track smart money / whale wallets
3. Aggregate signals from multiple sources
4. Focus on Solana, BSC, or Ethereum meme coins

For each channel provide:
- username (Telegram handle with @)
- display_name
- estimated_subscribers
- focus_areas (array: 'aggregator', 'smart_money', 'whale', 'gems', 'degen', 'analysis')
- primary_chains (array: 'SOL', 'ETH', 'BSC')
- tier ('A' for best quality, 'B' for good, 'C' for risky)
- signal_frequency ('high', 'medium', 'low')
- notes (brief description)

Respond in JSON format:
{
  "channels": [
    {
      "username": "@MomentumTrackerCN",
      "display_name": "Momentum Tracker CN",
      "estimated_subscribers": 50000,
      "focus_areas": ["aggregator", "smart_money"],
      "primary_chains": ["SOL"],
      "tier": "A",
      "signal_frequency": "high",
      "notes": "Top signal aggregator for Solana"
    }
  ]
}`;

    try {
      const response = await this.grokClient.askGrok(prompt);
      const jsonMatch = response.match(/\{[\s\S]*\}/);
      
      if (!jsonMatch) {
        console.log('   ⚠️ Could not parse channel discovery response');
        return;
      }

      const data = JSON.parse(jsonMatch[0]);
      let newCount = 0;

      const insertStmt = this.db.prepare(`
        INSERT OR IGNORE INTO ai_telegram_channels (
          username, display_name, subscriber_count, tier, influence_score,
          focus_areas, chains, signal_frequency, ai_notes,
          last_updated, update_source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ai_discovery')
      `);

      for (const ch of data.channels || []) {
        const influenceScore = this.calculateChannelInfluenceScore(ch);
        
        const result = insertStmt.run(
          ch.username,
          ch.display_name,
          ch.estimated_subscribers,
          ch.tier,
          influenceScore,
          JSON.stringify(ch.focus_areas || []),
          JSON.stringify(ch.primary_chains || []),
          ch.signal_frequency,
          ch.notes,
          Date.now()
        );

        if (result.changes > 0) {
          newCount++;
        }
      }

      console.log(`   ✅ Discovered ${newCount} new channels`);
      
    } catch (error) {
      console.error('   ❌ Channel discovery failed:', error.message);
    }
  }

  /**
   * Update existing influencers with latest info
   */
  async updateExistingInfluencers() {
    // This would typically update follower counts, recent performance, etc.
    // For now, just update the timestamp
    this.db.prepare(`
      UPDATE ai_twitter_kols SET last_updated = ? WHERE is_active = 1
    `).run(Date.now());
    
    this.db.prepare(`
      UPDATE ai_telegram_channels SET last_updated = ? WHERE is_active = 1
    `).run(Date.now());
  }

  /**
   * Calculate influence score for a KOL
   */
  calculateKOLInfluenceScore(kol) {
    let score = 0;
    
    // Followers (0-5 points)
    if (kol.estimated_followers >= 500000) score += 5;
    else if (kol.estimated_followers >= 200000) score += 4;
    else if (kol.estimated_followers >= 100000) score += 3;
    else if (kol.estimated_followers >= 50000) score += 2;
    else score += 1;
    
    // Tier bonus (0-3 points)
    if (kol.tier === 'Tier1') score += 3;
    else if (kol.tier === 'Tier2') score += 2;
    else score += 1;
    
    // Focus area relevance (0-2 points)
    const relevantAreas = ['meme', 'trading', 'ai', 'defi'];
    const matchCount = (kol.focus_areas || []).filter(a => relevantAreas.includes(a)).length;
    score += Math.min(2, matchCount * 0.5);
    
    return Math.min(10, score);
  }

  /**
   * Calculate influence score for a channel
   */
  calculateChannelInfluenceScore(ch) {
    let score = 0;
    
    // Subscribers (0-4 points)
    if (ch.estimated_subscribers >= 50000) score += 4;
    else if (ch.estimated_subscribers >= 20000) score += 3;
    else if (ch.estimated_subscribers >= 10000) score += 2;
    else score += 1;
    
    // Tier (0-4 points)
    if (ch.tier === 'A') score += 4;
    else if (ch.tier === 'B') score += 2;
    else score += 1;
    
    // Signal frequency balance (0-2 points)
    if (ch.signal_frequency === 'medium') score += 2;  // Best balance
    else if (ch.signal_frequency === 'low') score += 1.5;  // Quality
    else score += 1;  // High frequency = more noise
    
    return Math.min(10, score);
  }

  /**
   * Real-time: Check if a token was mentioned by known KOLs
   * 
   * @param {Object} twitterData - Twitter data from Grok search
   * @returns {Object} { kol_mentions, influence_boost, details }
   */
  detectKOLMentions(twitterData) {
    if (!twitterData || !twitterData.top_tweets) {
      return { kol_mentions: 0, influence_boost: 0, details: [] };
    }

    const details = [];
    let totalInfluence = 0;

    for (const tweet of twitterData.top_tweets) {
      const author = (tweet.author || '').toLowerCase().replace('@', '');
      const kol = this.kolCache.get(author);
      
      if (kol) {
        details.push({
          handle: kol.handle,
          tier: kol.tier,
          influence: kol.influence_score
        });
        totalInfluence += kol.influence_score;
      }
    }

    return {
      kol_mentions: details.length,
      influence_boost: Math.min(15, totalInfluence),  // Cap at 15 points
      details
    };
  }

  /**
   * Get channel info by name (fuzzy matching)
   * 
   * @param {string} channelName - Channel name or username
   * @returns {Object|null} Channel info
   */
  getChannelInfo(channelName) {
    if (!channelName) return null;
    
    // Normalize the name
    const normalized = channelName.toLowerCase()
      .replace('@', '')
      .replace(/[^\w\d]/g, '');
    
    // Try exact match first
    for (const [key, channel] of this.channelCache) {
      const keyNormalized = key.replace('@', '').replace(/[^\w\d]/g, '');
      if (keyNormalized === normalized) {
        return channel;
      }
    }
    
    // Try partial match
    for (const [key, channel] of this.channelCache) {
      const keyNormalized = key.replace('@', '').replace(/[^\w\d]/g, '');
      if (keyNormalized.includes(normalized) || normalized.includes(keyNormalized)) {
        return channel;
      }
    }
    
    return null;
  }

  /**
   * Calculate influence score for a signal
   * 
   * @param {string} channelName - Source channel name
   * @param {Object} twitterData - Twitter data from Grok
   * @returns {Object} { score, breakdown }
   */
  calculateInfluenceScore(channelName, twitterData) {
    let score = 0;
    const breakdown = {
      channel_score: 0,
      kol_score: 0,
      channel_tier: 'Unknown',
      kol_mentions: []
    };

    // 1. Channel influence (0-15 points)
    const channel = this.getChannelInfo(channelName);
    if (channel) {
      breakdown.channel_tier = channel.tier;
      breakdown.channel_name = channel.display_name;
      
      // Tier-based scoring
      if (channel.tier === 'A') {
        breakdown.channel_score = 12;
      } else if (channel.tier === 'B') {
        breakdown.channel_score = 8;
      } else {
        breakdown.channel_score = 3;
      }
      
      // Reliability bonus
      if (channel.reliability_score >= 7) {
        breakdown.channel_score += 3;
      }
      
      breakdown.channel_score = Math.min(15, breakdown.channel_score);
      score += breakdown.channel_score;
    } else {
      // Unknown channel - give base score
      breakdown.channel_score = 2;
      breakdown.channel_tier = 'Unknown';
      score += 2;
    }

    // 2. KOL mentions (0-10 points)
    const kolDetection = this.detectKOLMentions(twitterData);
    breakdown.kol_score = kolDetection.influence_boost;
    breakdown.kol_mentions = kolDetection.details;
    score += Math.min(10, kolDetection.influence_boost);

    return {
      score: Math.min(25, score),  // Cap at 25 (Influence component max)
      breakdown
    };
  }

  /**
   * Get all KOLs ranked by influence
   */
  getRankedKOLs() {
    return this.db.prepare(`
      SELECT handle, display_name, followers, tier, influence_score, 
             reliability_score, ai_notes, last_updated
      FROM ai_twitter_kols 
      WHERE is_active = 1 
      ORDER BY influence_score DESC
    `).all();
  }

  /**
   * Get all channels ranked by influence
   */
  getRankedChannels() {
    return this.db.prepare(`
      SELECT username, display_name, subscriber_count, tier, influence_score,
             reliability_score, signal_frequency, ai_notes, is_monitored, last_updated
      FROM ai_telegram_channels 
      WHERE is_active = 1 
      ORDER BY influence_score DESC
    `).all();
  }

  /**
   * Add channel to monitoring list
   */
  addToMonitoring(username) {
    this.db.prepare(`
      UPDATE ai_telegram_channels SET is_monitored = 1 WHERE username = ?
    `).run(username);
    this.loadCaches();
  }

  /**
   * Remove channel from monitoring list
   */
  removeFromMonitoring(username) {
    this.db.prepare(`
      UPDATE ai_telegram_channels SET is_monitored = 0 WHERE username = ?
    `).run(username);
    this.loadCaches();
  }
}

export default AIInfluencerSystem;
