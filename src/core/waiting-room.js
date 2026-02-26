/**
 * Waiting Room v6.1 - AI 智能观察池
 * 
 * 核心逻辑：
 * 1. 入池时保存快照（价格、聪明钱、流动性等）
 * 2. 观察期内持续更新数据，记录 Delta
 * 3. 观察期满时，综合数据发给 AI 判断
 * 4. AI 说 BUY 才毕业，SKIP 则踢出
 * 
 * 使用 strategy.js 配置
 */

import { EventEmitter } from 'events';
import { WAITING_ROOM as WR_CONFIG } from '../config/strategy.js';
import dynamicCalculator from '../utils/dynamic-calculator.js';
import { buildEntryPrompt } from '../utils/prompt-builder.js';
import aiAnalyst from '../utils/ai-analyst.js';

export class WaitingRoom extends EventEmitter {
    constructor(config, db, klineValidator, services = {}) {
        super();
        this.config = config;
        this.db = db;
        this.klineValidator = klineValidator;
        this.solService = services.sol;
        this.bscService = services.bsc;

        this.signals = new Map();
        this.checkInterval = null;
        this.signalCountCache = new Map();

        // 从配置文件加载参数
        this.params = {
            MIN_WAIT_TIME_BASE_MS: WR_CONFIG.MIN_WAIT_TIME_MS,
            RANDOM_JITTER_MS: WR_CONFIG.RANDOM_JITTER_MS,
            MAX_WAIT_TIME_MS: WR_CONFIG.MAX_WAIT_TIME_MS,
            CHECK_INTERVAL_MS: WR_CONFIG.CHECK_INTERVAL_MS,
            MAX_GRADUATE_PER_CHECK: 1,
            SM_ACCEL_TRIGGER: WR_CONFIG.SM_ACCEL_TRIGGER,
            USE_AI_DECISION: WR_CONFIG.USE_AI_DECISION
        };

        console.log('🔭 Waiting Room v6.1 (Config-Driven) initialized');
    }

    start() {
        if (this.checkInterval) return;
        console.log('▶️  Waiting Room started');
        this.checkInterval = setInterval(() => {
            this.checkSignals();
        }, this.params.CHECK_INTERVAL_MS);
    }

    stop() {
        if (this.checkInterval) {
            clearInterval(this.checkInterval);
            this.checkInterval = null;
        }
        console.log('⏹️  Waiting Room stopped');
    }

    /**
     * 更新信号次数缓存（从 Heatmap 获取）
     */
    updateSignalCountCache(cache) {
        this.signalCountCache = cache;
    }

    /**
     * 新币入池
     */
    async addSignal(signal, analysis) {
        const tokenCA = signal.token_ca;
        const now = Date.now();

        if (this.signals.has(tokenCA)) {
            console.log(`   ⏭️ [Waiting Room] ${signal.symbol} 已在池中`);
            return;
        }

        const targetWait = this.params.MIN_WAIT_TIME_BASE_MS + Math.random() * this.params.RANDOM_JITTER_MS;

        // 🔥 记录入池快照
        const initialSnapshot = {
            price: signal.price || analysis?.token?.price || 0,
            smartMoney: signal.smart_wallet_online || analysis?.token?.smartWalletOnline || 0,
            liquidity: signal.liquidity || analysis?.token?.liquidity || 0,
            holders: signal.holders || analysis?.token?.holders || 0,
            marketCap: signal.marketCap || analysis?.token?.marketCap || 0,
            volume: signal.volume || analysis?.token?.volume || 0,
            top10Percent: signal.top10Percent || analysis?.token?.top10Percent || 0
        };

        console.log(`🔭 [Waiting Room] 入池: ${signal.symbol}`);
        console.log(`   📊 快照: 价格=$${initialSnapshot.price?.toFixed(8) || 'N/A'} | SM=${initialSnapshot.smartMoney} | 流动性=$${(initialSnapshot.liquidity / 1000).toFixed(1)}K`);
        console.log(`   ⏱️  目标等待: ${(targetWait / 1000 / 60).toFixed(1)} 分钟`);

        this.signals.set(tokenCA, {
            signal,
            analysis,
            startTime: now,
            entryTime: now,
            targetWaitTime: targetWait,
            status: 'WAITING',
            initial: initialSnapshot,
            priceSnapshots: [],       // 价格快照历史
            klineHistory: [],         // K 线历史
            aiReport: analysis?.aiReport || signal.aiReport || null,
            triggerReason: null
        });
    }

    /**
     * 检查所有信号，决定是否毕业
     */
    async checkSignals() {
        const now = Date.now();
        const tokens = Array.from(this.signals.keys());
        let graduatedThisCycle = 0;

        for (const tokenCA of tokens) {
            if (graduatedThisCycle >= this.params.MAX_GRADUATE_PER_CHECK) break;

            const item = this.signals.get(tokenCA);
            if (!item || !item.startTime) {
                this.signals.delete(tokenCA);
                continue;
            }

            const waitTime = now - item.startTime;
            const waitMinutes = (waitTime / 60000).toFixed(1);

            // 1. 超时处理
            if (waitTime > this.params.MAX_WAIT_TIME_MS) {
                console.log(`⌛ [Waiting Room] ${item.signal.symbol} 观察超时 (${waitMinutes}min)，踢出`);
                this.signals.delete(tokenCA);
                continue;
            }

            // 2. 获取最新数据
            let currentData = null;
            try {
                const service = item.signal.chain === 'SOL' ? this.solService : this.bscService;
                if (service) {
                    const snapshot = await service.getSnapshot(tokenCA);
                    if (snapshot) {
                        // 🔥 正确映射字段：liquidity_usd 是美元，liquidity 是 SOL/BNB
                        currentData = {
                            price: snapshot.current_price || snapshot.price,
                            smartWalletOnline: snapshot.smart_wallet_count || snapshot.smartWalletOnline,
                            liquidity: snapshot.liquidity_usd || snapshot.liquidity || 0,  // 优先用 USD
                            holders: snapshot.holder_count || snapshot.holders,
                            marketCap: snapshot.market_cap || snapshot.marketCap,
                            isMintAbandoned: snapshot.mint_authority === null || snapshot.mint_authority === 'None',
                            top10Percent: snapshot.top10_percent || snapshot.top10Percent,
                            dataSource: 'live_snapshot'
                        };
                    }
                }
            } catch (e) {
                console.error(`⚠️ [Waiting Room] 获取数据失败: ${item.signal.symbol} - ${e.message}`);
            }

            // 🔥 数据验证与后备：确保流动性是合理的美元值
            const MIN_VALID_LIQUIDITY_USD = 1000;  // 最小有效流动性 $1000

            if (!currentData || !currentData.liquidity || currentData.liquidity < MIN_VALID_LIQUIDITY_USD) {
                // 使用入池快照作为后备
                if (!currentData) {
                    console.log(`   ⚠️ [Waiting Room] 数据获取失败，使用入池快照`);
                } else {
                    console.log(`   ⚠️ [Waiting Room] 流动性异常 ($${currentData.liquidity?.toFixed(0) || 0})，使用入池快照`);
                }
                currentData = {
                    price: item.initial.price,
                    smartWalletOnline: item.initial.smartMoney,
                    liquidity: item.initial.liquidity,
                    holders: item.initial.holders,
                    marketCap: item.initial.marketCap,
                    isMintAbandoned: item.signal.isMintAbandoned,
                    top10Percent: item.initial.top10Percent,
                    dataSource: 'initial_snapshot'
                };
            } else {
                // 🔥 验证 holders 数据（Top10 API 被禁用时可能为 null）
                if (!currentData.holders || currentData.holders < 1) {
                    currentData.holders = item.initial.holders;  // 使用入池时的 holders
                }
                // 验证 marketCap
                if (!currentData.marketCap || currentData.marketCap < 1000) {
                    currentData.marketCap = item.initial.marketCap;
                }
            }

            // 记录快照
            item.priceSnapshots.push({
                t: now,
                p: currentData.price || currentData.current_price,
                sm: currentData.smartWalletOnline || currentData.smart_wallet_count,
                liq: currentData.liquidity
            });

            // 保存 K 线数据
            if (item.signal.klineData && item.signal.klineData.length > 0) {
                item.klineHistory = item.signal.klineData;
            }

            // 3. 检查触发条件
            let shouldTrigger = false;
            let triggerReason = '';

            // 条件 A: 观察期满
            if (waitTime >= item.targetWaitTime) {
                shouldTrigger = true;
                triggerReason = `观察期满 (${waitMinutes}min)`;
            }
            // 条件 B: 聪明钱激增（加速触发）
            else {
                const smDelta = (currentData.smartWalletOnline || 0) - (item.initial.smartMoney || 0);
                if (smDelta >= this.params.SM_ACCEL_TRIGGER) {
                    shouldTrigger = true;
                    triggerReason = `聪明钱激增 (+${smDelta})`;
                }
            }

            if (!shouldTrigger) continue;

            console.log(`\n🔔 [Waiting Room] ${item.signal.symbol} 触发毕业检查`);
            console.log(`   原因: ${triggerReason}`);

            // 4. 计算动态因子
            const signalCount = this.signalCountCache.get(tokenCA) || item.analysis?.alertCount || 1;
            const factors = dynamicCalculator.calculateFactors({
                smartMoney: currentData.smartWalletOnline || item.initial.smartMoney,
                signalCount: signalCount,
                liquidity: currentData.liquidity || item.initial.liquidity,
                marketCap: currentData.marketCap || item.initial.marketCap,
                avgBuyAmount: item.signal.avgBuyAmount || 0.5
            });

            console.log(`   🧮 特征: [${factors.tag}] ${factors.reason}`);
            console.log(`   📊 背离度=${factors.divergence} | 健康度=${factors.healthRatio}`);

            // 5. 土狗熔断（不调 AI，直接跳过）
            if (factors.tag === 'TRAP') {
                console.log(`   🚫 [熔断] 判定为土狗陷阱，直接踢出`);
                this.signals.delete(tokenCA);
                continue;
            }

            // 6. K 线健康检查
            const klineHealth = dynamicCalculator.analyzeKlineHealth(item.klineHistory);
            console.log(`   📈 K线: ${klineHealth.summary}`);

            if (klineHealth.health === 'BEARISH') {
                console.log(`   ⚠️ K线走势偏弱，降低信心`);
            }

            // 7. AI 决策
            if (this.params.USE_AI_DECISION) {
                const decision = await this.getAIDecision(item, currentData, factors, klineHealth);

                if (decision.action === 'BUY') {
                    console.log(`   ✅ AI 决策: 买入 (${decision.position})`);
                    console.log(`   💡 理由: ${decision.reason}`);
                    console.log(`   🎯 预计见顶: ${decision.exit_mcap || 'N/A'} (${decision.exit_multiplier || 'N/A'})`);

                    // 毕业
                    item.aiDecision = decision;
                    item.triggerReason = triggerReason;
                    await this.graduate(tokenCA, {
                        signal: item.signal,
                        analysis: item.analysis,
                        factors,
                        decision,
                        currentData
                    });
                    graduatedThisCycle++;
                } else {
                    console.log(`   ❌ AI 决策: 跳过`);
                    console.log(`   💡 理由: ${decision.reason}`);
                    this.signals.delete(tokenCA);
                }
            } else {
                // 不使用 AI，直接毕业（旧逻辑）
                if (factors.tag !== 'WEAK' && klineHealth.health !== 'BEARISH') {
                    await this.graduate(tokenCA, {
                        signal: item.signal,
                        analysis: item.analysis,
                        factors,
                        currentData
                    });
                    graduatedThisCycle++;
                } else {
                    this.signals.delete(tokenCA);
                }
            }
        }
    }

    /**
     * 获取 AI 决策
     */
    async getAIDecision(item, currentData, factors, klineHealth) {
        try {
            // 构建 Prompt
            const prompt = buildEntryPrompt({
                record: {
                    symbol: item.signal.symbol,
                    address: item.signal.token_ca,
                    chain: item.signal.chain,
                    startTime: item.startTime,
                    initial: item.initial
                },
                current: {
                    price: currentData.price || currentData.current_price,
                    smartWalletOnline: currentData.smartWalletOnline || currentData.smart_wallet_count,
                    liquidity: currentData.liquidity,
                    holders: currentData.holders,
                    marketCap: currentData.marketCap,
                    isMintAbandoned: currentData.isMintAbandoned,
                    top10Percent: currentData.top10Percent
                },
                factors,
                aiReport: item.aiReport,
                klines: item.klineHistory
            });

            // 调用 AI
            const decision = await aiAnalyst.analyze(prompt);
            return decision;

        } catch (error) {
            console.error(`   ❌ AI 决策失败: ${error.message}`);
            return {
                action: 'SKIP',
                reason: `AI故障: ${error.message}`,
                error: true
            };
        }
    }

    /**
     * 毕业
     */
    async graduate(tokenCA, data) {
        const item = this.signals.get(tokenCA);
        if (!item) return;

        console.log(`✅ [Waiting Room] ${item.signal.symbol} 毕业！即将买入`);

        // 发出毕业事件
        this.emit('graduate', {
            signal: data.signal,
            analysis: data.analysis,
            factors: data.factors,
            decision: data.decision,
            currentData: data.currentData,
            reason: item.triggerReason || 'TIME_MATURED'
        });

        this.signals.delete(tokenCA);
    }

    /**
     * 获取当前池中信号数量
     */
    getPoolSize() {
        return this.signals.size;
    }

    /**
     * 获取池中所有信号
     */
    getPoolSignals() {
        return Array.from(this.signals.values()).map(item => ({
            symbol: item.signal.symbol,
            address: item.signal.token_ca,
            waitTime: Math.floor((Date.now() - item.startTime) / 1000),
            status: item.status,
            initialSM: item.initial.smartMoney
        }));
    }
}
