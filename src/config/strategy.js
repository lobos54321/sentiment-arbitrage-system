/**
 * Strategy Configuration v7.0
 *
 * 所有策略参数集中管理
 * 修改策略只需改这个文件
 */

const config = {
    // ═══════════════════════════════════════════════════════════════
    // 🛑 第 1 层: Hard Gates (硬门槛 - 一票否决)
    // ═══════════════════════════════════════════════════════════════
    HARD_GATES: {
        MIN_SMART_MONEY: 1,           // 最少聪明钱 (至少1个火种)
        MIN_LIQUIDITY: 5000,          // 最小流动性 ($5k)
        MAX_LIQUIDITY: 5000000,       // 最大流动性 ($5M)
        MCAP_RANGE: [5000, 5000000],  // 市值范围 ($5k - $5M)
        MIN_HOLDERS: 30,              // 最少持有人
        MAX_TOP10_RATE: 0.80,         // Top10 持仓占比上限 (80%)
        AGE_MINUTES: [1, 360],        // 代币年龄 (1分钟 - 6小时)
        REQUIRE_MINT_OFF: true,       // 必须丢权限 (SOL链)
        SCAM_KEYWORDS: ['TEST', 'DROPS', 'REWARD', 'GIFT', 'AIRDROP', 'FREE', 'SCAM', 'RUG', 'HONEYPOT'],
        // v9.5: 全局禁用的叙事等级 (无论防御模式是否开启)
        // 数据支撑: TIER_B 52笔交易，19.2%胜率，-$83.99总亏损
        FORBIDDEN_TIERS: ['TIER_B']   // 永久禁用TIER_B (普通叙事)
    },

    // ═══════════════════════════════════════════════════════════════
    // ⚖️ 第 2 层: 评分系统 (Scoring - 满分100)
    // ═══════════════════════════════════════════════════════════════
    SCORING: {
        // === 权重配置 ===
        WEIGHTS: {
            SMART_MONEY: 2.5,         // 1个聪明钱 = 2.5分 (10个满分25)
            SIGNAL: 0.5,              // 1个信号 = 0.5分
            SAFETY_BASE: 5,           // 安全基础分
            HIGH_LIQUIDITY_BONUS: 5,  // 流动性 > $20k 加分
            HIGH_HOLDERS_BONUS: 5     // 持有人 > 100 加分
        },

        // === 入场时机评分 (基于市值) ===
        TIMING: {
            EARLY: { maxMcap: 50000, score: 25 },      // < $50k: +25分
            NORMAL: { maxMcap: 100000, score: 15 },    // < $100k: +15分
            LATE: { maxMcap: 500000, score: 5 },       // < $500k: +5分
            RISKY: { maxMcap: Infinity, score: -15 }   // > $500k: -15分
        },

        // === 影响力评分 (Influence 0-25分) ===
        INFLUENCE: {
            MAX_SCORE: 25,
            // 频道质量得分 (0-15分)
            CHANNEL: {
                MAX_SCORE: 15,
                TIER_A: 3.0,           // Tier A 频道 = 3分/个
                TIER_B: 1.5,           // Tier B 频道 = 1.5分/个
                TIER_C: 0.5,           // Tier C (未知) = 0.5分/个
                UNKNOWN_BASE: 3,       // 未知频道基础分
                NO_INFO_BASE: 2        // 无频道信息基础分
            },
            // KOL 得分 (0-10分)
            KOL: {
                MAX_SCORE: 10,
                STRONG: { minCount: 3, score: 10 },    // >= 3 KOL = 满分
                MODERATE: { minCount: 1, score: 5 },   // >= 1 KOL = 5分
                TIER1_BONUS: 10                        // Tier1 KOL = 满分
            }
        },

        // === 链上数据评分 (Graph 0-10分) ===
        GRAPH: {
            MAX_SCORE: 10,
            // Pump.fun 项目评分
            PUMPFUN: {
                // 市值评分 (0-3分)
                MCAP: {
                    EXCELLENT: { min: 50000, score: 3 },
                    GOOD: { min: 30000, score: 2 },
                    BASIC: { min: 10000, score: 1 }
                },
                // 交易量评分 (0-2分)
                VOLUME: {
                    HIGH: { min: 10000, score: 2 },
                    MODERATE: { min: 5000, score: 1 }
                },
                // 交易次数评分 (0-2分)
                TXNS: {
                    HIGH: { min: 500, score: 2 },
                    MODERATE: { min: 100, score: 1 }
                },
                // 联合曲线进度评分 (0-2分)
                BONDING: {
                    NEAR_RAYDIUM: { min: 80, score: 2 },
                    PROGRESSING: { min: 50, score: 1 }
                },
                // 买卖比评分 (0-1分)
                BUY_SELL_RATIO: {
                    BULLISH: { min: 1.2, score: 1 }
                }
            },
            // 普通 Raydium 项目评分
            RAYDIUM: {
                // 流动性评分 (0-4分)
                LIQUIDITY: {
                    EXCELLENT: { min: 50000, score: 4 },
                    GOOD: { min: 20000, score: 3 },
                    FAIR: { min: 10000, score: 2 },
                    BASIC: { min: 5000, score: 1 }
                },
                // Top10 持仓评分 (0-3分)
                TOP10: {
                    EXCELLENT: { max: 0.3, score: 3 },
                    GOOD: { max: 0.5, score: 2 },
                    FAIR: { max: 0.7, score: 1 }
                },
                // 持仓人数评分 (0-2分)
                HOLDERS: {
                    HIGH: { min: 500, score: 2 },
                    MODERATE: { min: 100, score: 1 }
                }
            }
        },

        // === 信号源评分 (Source 0-10分) ===
        SOURCE: {
            MAX_SCORE: 10,
            // 频道等级得分 (0-6分)
            CHANNEL_TIER: {
                A: 6,
                B: 4,
                C: 2
            },
            // 信号聚合加成 (0-4分)
            AGGREGATION: {
                MANY: { minChannels: 3, score: 4 },
                MULTIPLE: { minChannels: 2, score: 2 }
            }
        },

        // === X 验证乘数 ===
        X_VALIDATION: {
            HIGH_MULTIPLIER: 1.3,      // 高 X 验证 = x1.3
            MEDIUM_MULTIPLIER: 1.1,    // 中 X 验证 = x1.1
            BASE_MULTIPLIER: 1.0,      // 无 X 验证 = x1.0
            HIGH_THRESHOLD: 5,         // >= 5 提及 = 高验证
            MEDIUM_THRESHOLD: 1        // >= 1 提及 = 中验证
        },

        // 动态因子阈值
        DYNAMIC_FACTORS: {
            // 金狗特征
            GOLDEN: {
                MIN_DIVERGENCE: 0.8,      // 背离度 > 0.8
                MIN_HEALTH_RATIO: 0.15,   // 健康度 > 0.15
                MIN_SMART_MONEY: 3        // 聪明钱 >= 3
            },
            // 银狗特征
            SILVER: {
                MIN_SMART_MONEY: 5,       // 聪明钱 >= 5
                MIN_SIGNAL_COUNT: 10,     // 信号次数 >= 10
                MIN_DIVERGENCE: 0.2       // 背离度 > 0.2
            },
            // 土狗特征 (熔断)
            TRAP: {
                MAX_DIVERGENCE: 0.1,      // 背离度 < 0.1
                MIN_SIGNAL_COUNT: 20      // 信号次数 > 20
            }
        }
    },

    // ═══════════════════════════════════════════════════════════════
    // 🔭 第 3 层: 观察室配置
    // ═══════════════════════════════════════════════════════════════
    WAITING_ROOM: {
        MAX_POOL_SIZE: 20,                    // 最大容量
        MIN_WAIT_TIME_MS: 5 * 60 * 1000,     // 最少等待 5 分钟
        MAX_WAIT_TIME_MS: 15 * 60 * 1000,    // 最大等待 15 分钟
        RANDOM_JITTER_MS: 3 * 60 * 1000,     // 随机抖动 0-3 分钟
        CHECK_INTERVAL_MS: 30 * 1000,         // 检查间隔 30 秒
        SM_ACCEL_TRIGGER: 2,                  // 聪明钱激增触发 (+2)
        USE_AI_DECISION: true,                // ✅ AI 决策已启用
        USE_TIERED_POOL: true                 // ✅ 使用三级动态观察池
    },

    // ═══════════════════════════════════════════════════════════════
    // 🚑 银池直通车 (Silver Emergency Exit)
    // 用于捕捉"马斯克发推"式的突发爆发行情
    // 满足所有条件的银池币可以跳过金池晋级，直接毕业买入
    // ═══════════════════════════════════════════════════════════════
    SILVER_EMERGENCY: {
        ENABLED: true,                        // 开关
        REQUIRE_TAG: 'GOLDEN',                // 必须被动态因子判定为金狗
        MIN_SM_DELTA: 4,                      // 聪明钱至少净流入 4 个 (确认抢筹)
        MIN_PRICE_GAIN: 20,                   // 价格至少拉升 20% (确认主升浪)
        IGNORE_MIN_TIME: true                 // 允许忽略观察时间限制
    },

    // 🏟️ 三级动态观察池配置 (优胜劣汰晋级赛机制)
    TIERED_POOL: {
        // 池子容量 (总共 50 个)
        CAPACITY: {
            GOLD: 5,     // 核心区: 高频监控 + AI 直通车
            SILVER: 15,  // 缓冲区: 中频监控
            BRONZE: 30   // 海选区: 低频监控
        },
        // 初始入池分数线
        ENTRY_THRESHOLD: {
            GOLD: 75,    // 聪明钱 ≥10 + GOLDEN 标签
            SILVER: 65,  // 评分正常
            BRONZE: 55   // 入门级
        },
        // 动态晋级条件
        PROMOTION: {
            TO_SILVER: { smDelta: 2, priceDelta: 15 },
            TO_GOLD: { smDelta: 3, priceDelta: 25 }
        },
        // 最小观察时间
        MIN_OBSERVE_TIME: {
            GOLD: 5 * 60 * 1000,     // 金池 5 分钟
            SILVER: 8 * 60 * 1000,   // 银池 8 分钟
            BRONZE: 10 * 60 * 1000   // 铜池 10 分钟
        },
        // 降级/淘汰条件
        EVICTION: {
            DROP_GOLD: { smDelta: -2, priceDelta: -15 },  // SM-2 或 价格跌15% 降级
            DROP_SILVER: { smDelta: -1, priceDelta: -10 }, // SM-1 且 价格跌10% 降级
            MAX_IDLE_TIME: 15 * 60 * 1000
        },
        // 🎓 v6.2 毕业答辩标准
        GRADUATION_REQ: {
            // 金池毕业要求
            GOLD: {
                MIN_TIME: 5 * 60 * 1000,    // 最短观察 5 分钟
                MIN_SM_DELTA: -1,           // 聪明钱允许微量流出 (22->21 OK)
                MAX_PRICE_DROP: -0.10,      // 价格不能跌超 10%
                REQUIRE_STABLE_TREND: true  // 不能正在急跌
            },
            // 银池毕业要求 (更严)
            SILVER: {
                MIN_TIME: 8 * 60 * 1000,
                MIN_SM_DELTA: 0,            // 聪明钱不能流出
                MIN_PRICE_GAIN: 0.05,       // 价格需要涨 5%
                REQUIRE_STABLE_TREND: true
            }
        }
    },

    // ═══════════════════════════════════════════════════════════════
    // 💰 交易决策配置
    // ═══════════════════════════════════════════════════════════════
    DECISION: {
        // 分级门槛
        TIERS: {
            S_TIER: 80,   // S级: 80分以上
            A_TIER: 70,   // A级: 70分以上
            B_TIER: 60    // B级: 60分以上
        },

        // 仓位配置 (SOL)
        POSITIONS: {
            SMALL: 0.05,      // 小仓
            NORMAL: 0.10,     // 标准仓
            PREMIUM: 0.15,    // 重仓
            MAX: 0.20         // 最大仓
        },

        // ═══════════════════════════════════════════════════════════════
        // 🏦 v6.5 仓位容量管理 (Position Capacity Management)
        // 只有 OPEN 状态计入限制，PARTIAL (已出本) 是零风险 Moon Bag 不占位
        // ═══════════════════════════════════════════════════════════════
        MAX_POSITIONS: {
            NORMAL: 6,               // 普通席位: 6 个 (任何评分都可入场)
            VIP: 2,                  // VIP 席位: 2 个 (仅限 GOLDEN 标签)
            TOTAL: 8,                // 硬上限: 最多 8 个 OPEN 仓位
            PER_CHAIN: {             // 按链分配 (防止单链过度集中)
                SOL: 5,
                BSC: 4
            }
        },

        // 特殊加成
        GOLDEN_MULTIPLIER: 1.5    // 金狗仓位 x1.5
    },

    // ═══════════════════════════════════════════════════════════════
    // 📉 退出策略配置 (钻石手版 v7.0)
    // ═══════════════════════════════════════════════════════════════
    EXIT_STRATEGY: {
        // 🛑 1. 分级止损 (根据仓位等级动态调整)
        STOP_LOSS: {
            SCOUT: -0.50,     // 统一 -50% 止损
            NORMAL: -0.50,
            PREMIUM: -0.50
        },

        // ═══════════════════════════════════════════════════════════════
        // 🎭 v9.0 叙事等级止损 (Narrative-Based Stop Loss)
        // 根据叙事质量动态调整止损线
        // - 大叙事 (S级): 允许更大回撤，持有更久等待爆发
        // - 普通叙事 (C级): 快速止损，减少损失
        // ═══════════════════════════════════════════════════════════════
        NARRATIVE_STOP_LOSS: {
            TIER_S: -0.70,    // S级大叙事: -70% 止损 (可死拿10x+)
            TIER_A: -0.60,    // A级优质叙事: -60% 止损 (可持3x+)
            TIER_B: -0.50,    // B级普通叙事: -50% 止损 (赚50%就跑)
            TIER_C: -0.40,    // C级垃圾叙事: -40% 止损 (快进快出)
            DEFAULT: -0.50    // 默认: -50% 止损
        },

        // 🛡️ 2. 聪明钱容忍度
        SM_EXIT_BUFFER: 2,
        SMART_MONEY_HOLD_THRESHOLD: 3,  // 聪明钱 >= 3 继续持有
        FOLLOW_SMART_MONEY: true,

        // 📈 3. 移动止损 / 利润回撤保护
        // v9.4: 提前触发，避免卖飞（今晚很多币涨了10-30%后完全吐回去）
        PROFIT_LOCK: {
            TRIGGER: 0.15,    // v9.4: 0.50→0.15，涨15%就启动保护
            CALLBACK: 0.40    // v9.4: 0.25→0.40，从最高点回撤40%才卖（更宽容）
        },

        // ═══════════════════════════════════════════════════════════════
        // 🔐 v9.4 利润保护阶梯 (Profit Protection Tiers)
        // 增加早期保护层级，避免小涨后完全吐回去
        // ═══════════════════════════════════════════════════════════════
        PROFIT_PROTECTION_TIERS: [
            { profit: 0.15, protect: 0.00, note: 'v9.4: 赚15%后启动监控，保底0%' },
            { profit: 0.30, protect: 0.10, note: 'v9.4: 赚30%后，保底锁利3%' },
            { profit: 0.50, protect: 0.20, note: '赚50%后，保底锁利10%' },
            { profit: 1.00, protect: 0.50, note: '翻倍后，保底锁利50%' },
            { profit: 2.00, protect: 0.70, note: '3倍后，保底锁利140%' },
            { profit: 5.00, protect: 0.80, note: '6倍后，保底锁利400%' }
        ],

        // ⏰ 4. 时间止损
        TIME_STOP: {
            SOL_MINUTES: 150,    // SOL: 2.5小时
            BSC_MINUTES: 180,    // BSC: 3小时
            SKIP_IF_SM_INFLOW: true
        },

        // 💰 5. 分批止盈 (阶梯出货)
        TAKE_PROFIT: {
            DOUBLE: { trigger: 1.00, sell: 50 },   // 翻倍出本 50%
            TRIPLE: { trigger: 3.00, sell: 15 },   // 3倍卖 15%
            PENTA: { trigger: 5.00, sell: 10 },    // 5倍卖 10%
            // 剩余 25%: 追踪止损
        },

        // 🎯 追踪止损 (Trailing Stop)
        TRAILING_STOP: {
            ENABLED: true,
            TRIGGER_AFTER_PROFIT: 3.00,  // 3倍以上启用
            CALLBACK_PERCENT: 0.30,      // 从最高点回撤 30%
            MIN_PROFIT_LOCK: 1.50        // 至少保住 1.5 倍利润
        },

        // 🚨 6. 紧急退出
        EMERGENCY: {
            LIQUIDITY_DROP: 0.50,    // 流动性腰斩
            SM_EXODUS: 3,            // 聪明钱跑了 3 个以上
            DEV_DUMP: true,          // Dev 大量卖出
            DEV_DUMP_THRESHOLD: 0.10 // Dev 卖出 10% 以上
        },

        // 🛡️ 7. K线趋势保护
        TREND_PROTECTION: {
            ENABLED: true,
            BOUNCE_THRESHOLD: 0.02   // 涨 2% 视为有承接
        },

        // 🔒 8. 金库模式 (翻倍出本后的免费筹码)
        VAULT_MODE: {
            ENABLED: true,
            STOP_LOSS: -0.80,         // 金库仓位止损 -80%
            MIN_PROFIT_TO_VAULT: 1.0  // 至少翻倍才进入金库
        },

        // 🎭 9. 叙事等级止盈策略
        NARRATIVE_TAKE_PROFIT: {
            TIER_S: {
                HALF: { trigger: 1.00, sell: 20 },
                DOUBLE: { trigger: 2.00, sell: 30 },
                TRIPLE: { trigger: 5.00, sell: 30 },
                MOON: { trigger: 10.00, sell: 50 },
                MEGA: { trigger: 20.00, sell: 100 }
            },
            TIER_A: {
                HALF: { trigger: 0.80, sell: 25 },
                DOUBLE: { trigger: 1.50, sell: 40 },
                TRIPLE: { trigger: 3.00, sell: 40 },
                MOON: { trigger: 5.00, sell: 100 }
            },
            TIER_B: {
                HALF: { trigger: 0.50, sell: 40 },
                DOUBLE: { trigger: 1.00, sell: 50 },
                TRIPLE: { trigger: 1.50, sell: 50 },
                MOON: { trigger: 2.00, sell: 100 }
            },
            TIER_C: {
                HALF: { trigger: 0.30, sell: 50 },
                DOUBLE: { trigger: 0.50, sell: 50 },
                TRIPLE: { trigger: 1.00, sell: 100 },
                MOON: null
            }
        },

        // 🤖 10. AI 目标价止盈
        AI_TARGET_PRICE: {
            ENABLED: true,
            SELL_RATIO: 0.80   // 达到 AI 预测价格时卖 80%
        }
    },

    // ═══════════════════════════════════════════════════════════════
    // 🛡️ 风险管理配置 (v7.0 统一)
    // ═══════════════════════════════════════════════════════════════
    RISK: {
        // 入场标准
        MIN_SCORE_TO_TRADE: 50,

        // 时间衰减
        TIME_DECAY: {
            FRESH_MINUTES: 5,
            STALE_MINUTES: 15,
            EXPIRED_MINUTES: 30,
            STALE_MULTIPLIER: 0.8,
            EXPIRED_MULTIPLIER: 0
        },

        // 资金管理
        MAX_POSITION_PERCENT: 0.02,
        MAX_CONCURRENT_POSITIONS: 6,
        GOLDEN_EXTRA_SLOTS: 2,
        MAX_TOTAL_POSITIONS: 8,

        // Swap 机制 (腾笼换鸟)
        SWAP_CONFIG: {
            ENABLED: true,
            MIN_SCORE_TO_SWAP: 80,
            REQUIRE_GOLDEN: true,
            MIN_HOLD_TIME_MINUTES: 30,
            REQUIRE_LOSS: true,
            REQUIRE_SM_EXIT: true
        },

        // 物理熔断
        CIRCUIT_BREAKER: {
            CONSECUTIVE_LOSS_PAUSE: 6,
            PAUSE_DURATION_HOURS: 4,
            WIN_RATE_THRESHOLD: 0.25,
            MIN_TRADES_FOR_STATS: 10,
            // v9.4: 熔断后恢复期配置
            RECOVERY_PERIOD_HOURS: 4,    // 恢复期时长 (熔断后4小时内)
            RECOVERY_POSITION_MULTIPLIER: 0.5  // 恢复期仓位乘数 (减半)
        },

        // ═══════════════════════════════════════════════════════════════
        // 🛡️ v9.5 防御模式 (Defensive Mode)
        // v9.5更新: TIER_B已在HARD_GATES中全局禁用
        // 防御模式额外禁止TIER_S (大热避险)
        // ═══════════════════════════════════════════════════════════════
        DEFENSIVE_MODE: {
            ENABLED: true,
            MIN_SCORE_BOOST: 15,
            // v9.5: TIER_B已全局禁用，防御模式只需额外禁止TIER_S
            FORBIDDEN_TIERS: ['TIER_S'],  // 防御模式额外禁止
            ALLOWED_TIERS: ['TIER_A'],    // v9.5: 只允许TIER_A (TIER_B已全局禁用)
            POSITION_MULTIPLIER: 0.5,
            SM_THRESHOLD_BOOST: 3
        },

        // 每日风控
        DAILY_LOSS_LIMIT: {
            SOL: 0.3,
            BNB: 0.1
        },

        // 危险信号权重
        DANGER_SIGNALS: {
            LP_UNLOCK_SOON: 10,
            OWNER_NOT_RENOUNCED: 5,
            HIGH_TAX: 8,
            HONEYPOT_RISK: 20,
            DEV_HOLDING_HIGH: 7,
            SMART_MONEY_EXITING: 15,
            LIQUIDITY_DROPPING: 12,
            SOCIAL_DELETED: 20
        },
        MAX_DANGER_SCORE: 15
    },

    // ═══════════════════════════════════════════════════════════════
    // 🤖 AI 配置
    // ═══════════════════════════════════════════════════════════════
    AI: {
        MODEL: 'grok-4-1-fast-reasoning',
        TEMPERATURE: 0.3,
        TIMEOUT_MS: 120000,
        MAX_RETRIES: 2
    }
};

// ES Module exports
export const HARD_GATES = config.HARD_GATES;
export const SCORING = config.SCORING;
export const WAITING_ROOM = config.WAITING_ROOM;
export const DECISION = config.DECISION;
export const EXIT_STRATEGY = config.EXIT_STRATEGY;
export const RISK = config.RISK;
export const AI = config.AI;
export const TIERED_POOL = config.TIERED_POOL;
export const SILVER_EMERGENCY = config.SILVER_EMERGENCY;
export default config;
