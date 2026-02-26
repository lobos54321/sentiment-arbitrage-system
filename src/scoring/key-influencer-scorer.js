/**
 * Key Influencer Scorer
 * 
 * 评估关键人物对代币的互动
 * 
 * 关键人物分级：
 * - Tier S: 市场巨鲸 (马斯克、CZ、何一、V神) - 权重 ×3
 * - Tier A: 交易所 + 顶级VC - 权重 ×2
 * - Tier B: 顶级 KOL - 权重 ×1.5
 * - Tier C: 普通 KOL - 权重 ×1
 * 
 * 互动类型：
 * - post_positive: 正面发帖 (+10)
 * - retweet: 转发 (+6)
 * - quote_tweet: 引用 (+8)
 * - reply_positive: 正面回复 (+4)
 * - like: 点赞 (+1)
 * - post_negative: 负面发帖 (-15)
 * - warning: 警告 (-10)
 */

export class KeyInfluencerScorer {
    constructor(config, db) {
        this.config = config;
        this.db = db;

        // 初始化关键账号列表
        this.initializeKeyAccounts();
    }

    /**
     * 初始化关键账号列表
     */
    initializeKeyAccounts() {
        // 👑 Tier S - 市场巨鲸 (任何互动都是超级信号)
        this.tierS = new Map([
            ['elonmusk', { name: 'Elon Musk', weight: 10, type: 'whale' }],
            ['cz_binance', { name: 'CZ', weight: 10, type: 'exchange_founder' }],
            ['heyibinance', { name: '何一 He Yi', weight: 9, type: 'exchange_founder' }],
            ['VitalikButerin', { name: 'Vitalik', weight: 9.5, type: 'founder' }],
            ['brian_armstrong', { name: 'Brian Armstrong', weight: 9, type: 'exchange_founder' }],
            ['aaborekeyakovenko', { name: 'Anatoly (Solana)', weight: 9, type: 'founder' }],
            ['rajgokal', { name: 'Raj Gokal (Solana)', weight: 8.5, type: 'founder' }],
            ['realDonaldTrump', { name: 'Donald Trump', weight: 10, type: 'whale' }],
            ['EricTrump', { name: 'Eric Trump', weight: 8, type: 'whale' }],
        ]);

        // 🏛️ Tier A - 交易所官方 + 顶级 VC
        this.tierA = new Map([
            // 交易所官方
            ['binance', { name: 'Binance', weight: 9, type: 'exchange' }],
            ['okx', { name: 'OKX', weight: 8.5, type: 'exchange' }],
            ['Bybit_Official', { name: 'Bybit', weight: 8, type: 'exchange' }],
            ['bitaboreket', { name: 'Bitget', weight: 7.5, type: 'exchange' }],
            ['coinbase', { name: 'Coinbase', weight: 8.5, type: 'exchange' }],
            ['kaborekaken', { name: 'Kraken', weight: 7.5, type: 'exchange' }],
            ['gate_io', { name: 'Gate.io', weight: 7, type: 'exchange' }],
            ['ABOREKEKUCOIN', { name: 'KuCoin', weight: 7, type: 'exchange' }],

            // 交易所高管
            ['star_xu', { name: 'Star Xu (OKX)', weight: 8, type: 'exchange_exec' }],
            ['benbybit', { name: 'Ben Zhou (Bybit)', weight: 7.5, type: 'exchange_exec' }],
            ['GracyBitget', { name: 'Gracy Chen (Bitget)', weight: 7.5, type: 'exchange_exec' }],

            // 顶级 VC
            ['a16zcrypto', { name: 'a16z Crypto', weight: 8, type: 'vc' }],
            ['paradigm', { name: 'Paradigm', weight: 8, type: 'vc' }],
            ['sequoia', { name: 'Sequoia', weight: 7.5, type: 'vc' }],
            ['polyaborekchain', { name: 'Polychain', weight: 7.5, type: 'vc' }],
            ['panaborekteracap', { name: 'Pantera', weight: 7.5, type: 'vc' }],
            ['dragonfly_xyz', { name: 'Dragonfly', weight: 7.5, type: 'vc' }],

            // 做市商
            ['wintermute_t', { name: 'Wintermute', weight: 7, type: 'mm' }],
            ['jump_', { name: 'Jump Crypto', weight: 7, type: 'mm' }],
        ]);

        // 🎯 Tier B - 顶级 KOL (从数据库加载)
        this.tierB = new Map([
            // 默认顶级 KOL
            ['blknoiz06', { name: 'Ansem', weight: 9.5, type: 'kol' }],
            ['MustStopMurad', { name: 'Murad', weight: 9, type: 'kol' }],
            ['HsakaTrades', { name: 'Hsaka', weight: 7.5, type: 'kol' }],
            ['ColdBloodShill', { name: 'ColdBloodShill', weight: 7, type: 'kol' }],
            ['coaborekeinfluence', { name: 'Cobie', weight: 8, type: 'kol' }],
            ['GCRClassic', { name: 'GCR', weight: 8, type: 'kol' }],
            ['punk6529', { name: 'Punk6529', weight: 7.5, type: 'kol' }],
            ['inversebrah', { name: 'Inversebrah', weight: 6.5, type: 'kol' }],

            // 🔥 用户发现的高胜率交易员 / 信号卖家
            ['aaalyonbtc', { name: 'Aaalyonbtc (高胜率)', weight: 8.5, type: 'alpha_trader' }],
            ['waveking1314', { name: 'WaveKing1314 (信号卖家)', weight: 7, type: 'signal_seller' }],  // 滞后但说明有热度

            // 中文 KOL
            ['phyrex_ni', { name: 'Phyrex 倪大', weight: 7, type: 'kol_cn' }],
            ['Wuaborekichain', { name: 'Wu Blockchain', weight: 7.5, type: 'kol_cn' }],
        ]);

        // 🔥 Trusted Alpha Sources - 从高胜率交易员跟踪到的信息源
        // 通过 Grok 分析 @aaalyonbtc 和 @waveking1314 的关注列表发现
        this.trustedAlphaSources = new Map([
            // === 链上数据分析 (最可靠的 alpha 来源) ===
            ['lookonchain', { name: 'Lookonchain', weight: 10, type: 'onchain_analyst', priority: 1 }],
            ['spotonchain', { name: 'Spot On Chain', weight: 9, type: 'onchain_analyst', priority: 1 }],
            ['zachxbt', { name: 'ZachXBT', weight: 9, type: 'onchain_analyst', priority: 1 }],
            ['bubblemaps', { name: 'Bubblemaps', weight: 7, type: 'onchain_analyst', priority: 1 }],  // @waveking1314 来源
            ['0xScope', { name: '0xScope', weight: 6, type: 'onchain_bot', priority: 2 }],

            // === 顶级 Solana KOL ===
            ['Ansem1', { name: 'Ansem (alt)', weight: 9, type: 'sol_kol', priority: 1 }],
            ['AnsemBull', { name: 'Ansem Bull', weight: 9, type: 'sol_kol', priority: 1 }],  // @waveking1314 来源
            ['GiganticRebirth', { name: 'Gigantic Rebirth', weight: 8, type: 'sol_kol', priority: 1 }],
            ['Bluntz_Capital', { name: 'Bluntz Capital', weight: 8.5, type: 'sol_kol', priority: 1 }],
            ['theunipcs', { name: 'The Unipcs', weight: 8.5, type: 'sol_kol', priority: 1 }],  // @waveking1314 来源
            ['wizardofsoho', { name: 'Wizard of Soho', weight: 8, type: 'sol_kol', priority: 1 }],  // @waveking1314 来源
            ['defi_mochi', { name: 'Defi Mochi', weight: 8, type: 'sol_kol', priority: 1 }],  // @waveking1314 来源
            ['0xPhilanthrop', { name: '0xPhilanthrop', weight: 7, type: 'sol_kol', priority: 2 }],  // @waveking1314 来源
            ['0xMert_', { name: 'Mert (Helius)', weight: 8.5, type: 'sol_ecosystem', priority: 1 }],
            ['supernovajack', { name: 'Supernova Jack', weight: 7, type: 'sol_kol', priority: 2 }],
            ['CryptoChase', { name: 'Crypto Chase', weight: 7, type: 'sol_kol', priority: 2 }],

            // === 宏观 + Meme 混合 ===
            ['CryptoKaleo', { name: 'Crypto Kaleo', weight: 8.5, type: 'macro_kol', priority: 1 }],
            ['TheCryptoDog', { name: 'The Crypto Dog', weight: 7.5, type: 'kol', priority: 2 }],
            ['CryptoTony__', { name: 'Crypto Tony', weight: 7.5, type: 'kol', priority: 2 }],
            ['onchaincollege', { name: 'OnChain College', weight: 7, type: 'analyst', priority: 2 }],

            // === DEX 和工具 ===
            ['dexscreener', { name: 'DexScreener', weight: 9, type: 'tool', priority: 1 }],
            ['birdeye_so', { name: 'Birdeye', weight: 7.5, type: 'tool', priority: 1 }],
            ['jupiterexchange', { name: 'Jupiter', weight: 7, type: 'dex', priority: 2 }],
        ]);

        // 互动类型权重
        // 注意：在 Meme 币世界，即使是负面互动也能带来热度
        // Tier S 的任何互动都视为正面 (流量为王)
        this.interactionWeights = {
            post_positive: 10,   // 正面发帖提及
            quote_tweet: 8,      // 引用推文
            retweet: 6,          // 转发
            reply_positive: 4,   // 正面回复
            like: 1,             // 点赞

            // 负面互动 - Tier S 不扣分，其他 Tier 轻微扣分
            // Meme 逻辑：巨鲸的任何关注都是利好
            post_negative: 5,    // 负面发帖 → 改为加分 (有热度)
            warning: 3,          // 警告推文 → 轻加分
            sell_announcement: 0, // 宣布卖出 → 中性
        };

        // 时效性衰减配置 (分钟)
        this.timeDecay = {
            hot: { max: 60, multiplier: 1.5 },      // <1小时: ×1.5
            fresh: { max: 360, multiplier: 1.0 },   // 1-6小时: ×1.0
            recent: { max: 1440, multiplier: 0.7 }, // 6-24小时: ×0.7
            old: { max: 4320, multiplier: 0.3 },    // 1-3天: ×0.3
            stale: { max: Infinity, multiplier: 0 }, // >3天: 忽略
        };
    }

    /**
     * 计算关键人物评分
     * 
     * @param {string} tokenSymbol - 代币符号
     * @param {Object} xData - X/Twitter 搜索数据
     * @returns {Object} { score, details, superSignal }
     */
    async score(tokenSymbol, xData) {
        const result = {
            score: 0,
            maxScore: 15,
            details: [],
            superSignal: null,
            tierSInteractions: [],
            tierAInteractions: [],
            tierBInteractions: [],
        };

        // 如果没有 X 数据或没有关键人物互动数据，返回 0
        if (!xData || !xData.key_influencer_interactions) {
            // 检查 top_tweets 中是否有关键账号
            if (xData?.top_tweets && xData.top_tweets.length > 0) {
                for (const tweet of xData.top_tweets) {
                    const author = tweet.author?.toLowerCase();
                    if (author) {
                        const tierInfo = this.getAccountTier(author);
                        if (tierInfo) {
                            result.details.push(`发现 ${tierInfo.tier} 账号 @${author} 的推文`);

                            // 计算互动分数
                            const interactionScore = this.calculateInteractionScore(
                                tierInfo,
                                'post_positive',
                                0 // 假设是新鲜的
                            );

                            result.score += interactionScore;

                            // 检查超级信号
                            if (tierInfo.tier === 'S') {
                                result.superSignal = {
                                    type: 'TIER_S_POST',
                                    account: author,
                                    action: 'CONSIDER_PREMIUM_BUY'
                                };
                                result.tierSInteractions.push({ account: author, type: 'post' });
                            } else if (tierInfo.tier === 'A') {
                                result.tierAInteractions.push({ account: author, type: 'post' });
                            }
                        }
                    }
                }
            }

            // 限制最大分数
            result.score = Math.min(result.score, result.maxScore);
            return result;
        }

        // 处理关键人物互动
        for (const interaction of xData.key_influencer_interactions) {
            const account = interaction.account?.toLowerCase();
            const interactionType = interaction.type || 'like';
            const ageMinutes = interaction.age_minutes || 0;

            const tierInfo = this.getAccountTier(account);
            if (!tierInfo) continue;

            // 计算互动分数
            const interactionScore = this.calculateInteractionScore(
                tierInfo,
                interactionType,
                ageMinutes
            );

            result.score += interactionScore;
            result.details.push(
                `${tierInfo.tier} @${account} ${interactionType} (+${interactionScore.toFixed(1)})`
            );

            // 分类记录
            if (tierInfo.tier === 'S') {
                result.tierSInteractions.push(interaction);
            } else if (tierInfo.tier === 'A') {
                result.tierAInteractions.push(interaction);
            } else if (tierInfo.tier === 'B') {
                result.tierBInteractions.push(interaction);
            }
        }

        // 检查超级信号
        result.superSignal = this.checkSuperSignals(result);

        // 多关键账号加成
        const totalKeyInteractions =
            result.tierSInteractions.length +
            result.tierAInteractions.length +
            result.tierBInteractions.length;

        if (totalKeyInteractions >= 3) {
            result.score *= 1.5;
            result.details.push(`多关键账号加成 (${totalKeyInteractions}个) ×1.5`);
        }

        // 限制最大分数
        result.score = Math.min(Math.max(result.score, 0), result.maxScore);

        return result;
    }

    /**
     * 获取账号的 Tier 信息
     */
    getAccountTier(account) {
        const normalized = account.toLowerCase().replace('@', '');

        if (this.tierS.has(normalized)) {
            return { tier: 'S', ...this.tierS.get(normalized), tierWeight: 3 };
        }
        if (this.tierA.has(normalized)) {
            return { tier: 'A', ...this.tierA.get(normalized), tierWeight: 2 };
        }
        if (this.tierB.has(normalized)) {
            return { tier: 'B', ...this.tierB.get(normalized), tierWeight: 1.5 };
        }

        // 🔥 Alpha Sources - 从高胜率交易员跟踪到的信息源
        if (this.trustedAlphaSources.has(normalized)) {
            const source = this.trustedAlphaSources.get(normalized);
            return {
                tier: 'ALPHA',
                ...source,
                tierWeight: source.priority === 1 ? 1.5 : 1.2
            };
        }

        // 从数据库查询 Tier C KOL
        if (this.db) {
            try {
                const kol = this.db.prepare(
                    'SELECT * FROM ai_twitter_kols WHERE LOWER(handle) = ? AND is_active = 1'
                ).get(normalized);

                if (kol) {
                    return {
                        tier: 'C',
                        name: kol.display_name,
                        weight: kol.influence_score || 5,
                        tierWeight: 1
                    };
                }
            } catch (e) {
                // 忽略数据库错误
            }
        }

        return null;
    }

    /**
     * 计算单次互动的分数
     */
    calculateInteractionScore(tierInfo, interactionType, ageMinutes) {
        // 基础互动分数
        const baseScore = this.interactionWeights[interactionType] || 1;

        // Tier 权重
        const tierWeight = tierInfo.tierWeight || 1;

        // 账号权重
        const accountWeight = (tierInfo.weight || 5) / 10;

        // 时效性衰减
        const timeMultiplier = this.getTimeDecayMultiplier(ageMinutes);

        // 最终分数
        return baseScore * tierWeight * accountWeight * timeMultiplier;
    }

    /**
     * 获取时效性衰减系数
     */
    getTimeDecayMultiplier(ageMinutes) {
        for (const [, config] of Object.entries(this.timeDecay)) {
            if (ageMinutes < config.max) {
                return config.multiplier;
            }
        }
        return 0;
    }

    /**
     * 检查超级信号
     */
    checkSuperSignals(result) {
        // Tier S 发帖 → 立即关注
        if (result.tierSInteractions.length > 0) {
            const account = result.tierSInteractions[0].account;
            return {
                type: 'TIER_S_INTERACTION',
                account: account,
                action: 'ALERT_AND_CONSIDER_BUY',
                message: `🚨 超级信号: Tier S 账号 @${account} 与代币有互动!`
            };
        }

        // 多个交易所账号互动 → 可能要上所
        const exchangeAccounts = result.tierAInteractions.filter(
            i => this.tierA.get(i.account?.toLowerCase())?.type === 'exchange'
        );
        if (exchangeAccounts.length >= 2) {
            return {
                type: 'MULTI_EXCHANGE_INTEREST',
                accounts: exchangeAccounts.map(i => i.account),
                action: 'POTENTIAL_LISTING_ALERT',
                message: `🏛️ 多交易所关注: ${exchangeAccounts.length} 个交易所账号有互动`
            };
        }

        // CZ + 何一 同时互动 → 币安可能要上
        const czInteraction = result.tierSInteractions.some(
            i => i.account?.toLowerCase() === 'cz_binance'
        );
        const heyiInteraction = result.tierSInteractions.some(
            i => i.account?.toLowerCase() === 'heyibinance'
        );
        if (czInteraction && heyiInteraction) {
            return {
                type: 'BINANCE_LISTING_SIGNAL',
                action: 'HIGH_PRIORITY_ALERT',
                message: '🔥 超级信号: CZ + 何一同时互动，可能币安要上!'
            };
        }

        return null;
    }

    /**
     * 获取所有关键账号列表 (用于 Grok 搜索 prompt)
     */
    getKeyAccountsForSearch() {
        const accounts = [];

        // Tier S
        for (const [handle] of this.tierS) {
            accounts.push(`@${handle}`);
        }

        // Tier A (只取交易所和顶级 VC)
        for (const [handle, info] of this.tierA) {
            if (['exchange', 'exchange_exec', 'vc'].includes(info.type)) {
                accounts.push(`@${handle}`);
            }
        }

        // Tier B (只取最顶级的)
        const topKOLs = ['blknoiz06', 'MustStopMurad', 'HsakaTrades', 'coaborekeinfluence'];
        for (const handle of topKOLs) {
            accounts.push(`@${handle}`);
        }

        return accounts;
    }
}

export default KeyInfluencerScorer;
