/**
 * Batch AI Advisor v1.0
 * 
 * 批量查询 GOLD + SILVER 池代币，让 AI 横向对比选最优
 * 使用简化 Prompt，直接获取操作建议
 * 
 * 策略:
 * - 金池: 每 5 分钟批量查询一次
 * - 银池: 每 10 分钟批量查询一次
 * - 合并: 可以同时查 GOLD + SILVER
 */

import { EventEmitter } from 'events';
import OpenAI from 'openai';
import fs from 'fs';
import path from 'path';
import { AI as AI_CONFIG } from '../config/strategy.js';
import { applyProtectionRules } from '../decision/gold-dog-protection.js';
import snapshotRecorder from '../database/signal-snapshot-recorder.js';

class BatchAIAdvisor extends EventEmitter {
    constructor(tieredObserver, crossValidator = null) {
        super();
        this.tieredObserver = tieredObserver;
        this.crossValidator = crossValidator;  // 用于获取剩余仓位
        this.client = null;
        this.initialized = false;

        // v8.1 双轨制配置
        this.FAST_TRACK_INTERVAL = 2 * 60 * 1000;   // 快轨: 每 2 分钟检查紧急队列
        this.SLOW_TRACK_INTERVAL = 5 * 60 * 1000;   // 慢轨: 每 5 分钟分析全池

        this.fastTrackTimer = null;
        this.slowTrackTimer = null;

        // 紧急队列: 存放新晋升到 GOLD 的代币
        this.urgentQueue = new Map();  // address -> token

        // 已分析过的代币缓存 (防止重复分析)
        this.recentlyAnalyzed = new Map();  // address -> timestamp
        this.ANALYSIS_COOLDOWN = 10 * 60 * 1000;  // 10 分钟内不重复分析同一个币

        // v9.3: 分析锁 (防止快轨和慢轨同时运行)
        this.isAnalyzing = false;
        this.analyzingTokens = new Set();  // 正在分析的代币地址

        // 统计
        this.stats = {
            queries: 0,
            fastQueries: 0,
            slowQueries: 0,
            buyRecommendations: 0,
            watchRecommendations: 0,
            discardRecommendations: 0
        };

        console.log('🧠 [BatchAIAdvisor v8.1] 双轨制 AI 顾问初始化');
    }

    /**
     * 初始化 API 客户端
     */
    init() {
        if (this.initialized) return true;

        const apiKey = process.env.XAI_API_KEY;
        if (!apiKey) {
            console.error('❌ [BatchAIAdvisor] XAI_API_KEY 未配置');
            return false;
        }

        this.client = new OpenAI({
            apiKey: apiKey,
            baseURL: 'https://api.x.ai/v1'
        });

        this.initialized = true;
        console.log('🧠 [BatchAIAdvisor] Grok API 已初始化');
        return true;
    }

    /**
     * 启动定时查询 v8.1: 双轨制
     */
    start() {
        if (!this.init()) {
            console.error('❌ [BatchAIAdvisor] 启动失败: API 未初始化');
            return;
        }

        // 快轨定时器: 每 2 分钟检查紧急队列
        this.fastTrackTimer = setInterval(() => {
            this.queryUrgentQueue();
        }, this.FAST_TRACK_INTERVAL);

        // 慢轨定时器: 每 5 分钟分析全池
        this.slowTrackTimer = setInterval(() => {
            this.queryCombined();
        }, this.SLOW_TRACK_INTERVAL);

        // 监听 TieredObserver 的晋级事件
        if (this.tieredObserver) {
            this.tieredObserver.on('promotion', (data) => {
                this.onTokenPromotion(data);
            });
        }

        console.log(`🧠 [BatchAIAdvisor v8.1] 双轨制 AI 分析已启动`);
        console.log(`   🚄 快轨: 每 ${this.FAST_TRACK_INTERVAL / 60000} 分钟 (紧急队列)`);
        console.log(`   🚃 慢轨: 每 ${this.SLOW_TRACK_INTERVAL / 60000} 分钟 (全池覆盖)`);

        // 首次延迟 30 秒后启动慢轨查询
        setTimeout(() => {
            this.queryCombined();
        }, 30 * 1000);
    }

    /**
     * 停止定时查询
     */
    stop() {
        if (this.fastTrackTimer) {
            clearInterval(this.fastTrackTimer);
            this.fastTrackTimer = null;
        }
        if (this.slowTrackTimer) {
            clearInterval(this.slowTrackTimer);
            this.slowTrackTimer = null;
        }
        // 移除事件监听
        if (this.tieredObserver) {
            this.tieredObserver.removeAllListeners('promotion');
        }
        console.log('🧠 [BatchAIAdvisor v8.1] 已停止');
    }

    /**
     * v8.1: 代币晋升到 GOLD 时触发
     */
    onTokenPromotion(data) {
        const { token, fromTier, toTier } = data;
        if (toTier !== 'GOLD') return;

        // 检查是否在冷却期
        const lastAnalyzed = this.recentlyAnalyzed.get(token.address);
        if (lastAnalyzed && Date.now() - lastAnalyzed < this.ANALYSIS_COOLDOWN) {
            console.log(`   ⏸️ [紧急队列] ${token.symbol} 在 10 分钟冷却期内，跳过`);
            return;
        }

        // 加入紧急队列
        this.urgentQueue.set(token.address, token);
        console.log(`   📥 [紧急队列] ${token.symbol} 已入队，等待下次快轨分析`);
    }

    /**
     * v8.1: 快轨 - 查询紧急队列
     */
    async queryUrgentQueue() {
        if (this.urgentQueue.size === 0) {
            return;  // 队列为空，跳过
        }

        // v9.3: 检查分析锁
        if (this.isAnalyzing) {
            console.log(`🚄 [快轨] 慢轨分析中，跳过本轮`);
            return;
        }

        // 仓位预检查
        if (this.crossValidator) {
            const stats = this.crossValidator.getActivePositionStats();
            const maxPositions = 10;  // v8.1 扩容
            const remaining = maxPositions - stats.total;
            if (remaining <= 0) {
                console.log(`🚄 [快轨] 仓位已满 (${stats.total}/${maxPositions})，暂停紧急分析`);
                return;
            }
        }

        const tokens = Array.from(this.urgentQueue.values());
        this.urgentQueue.clear();

        // v9.3: 过滤掉正在分析的代币
        const filteredTokens = tokens.filter(t => !this.analyzingTokens.has(t.address));
        if (filteredTokens.length === 0) {
            console.log(`🚄 [快轨] 所有代币正在被分析，跳过`);
            return;
        }

        console.log(`\n🚄 [快轨] 紧急分析 ${filteredTokens.length} 个新晋 GOLD 代币`);
        this.stats.fastQueries++;

        // 标记为已分析
        filteredTokens.forEach(t => {
            this.recentlyAnalyzed.set(t.address, Date.now());
            this.analyzingTokens.add(t.address);
        });

        try {
            await this.batchQuery(filteredTokens, 'URGENT');
        } finally {
            // v9.3: 释放分析锁
            filteredTokens.forEach(t => this.analyzingTokens.delete(t.address));
        }
    }

    /**
     * 查询金池
     */
    async queryGoldPool() {
        const goldTokens = this.tieredObserver.getTokensByTier('GOLD');
        if (goldTokens.length === 0) {
            console.log('🧠 [BatchAIAdvisor] 金池为空，跳过查询');
            return;
        }

        console.log(`\n🧠 [BatchAIAdvisor] 金池批量查询 (${goldTokens.length} 个代币)`);
        await this.batchQuery(goldTokens.slice(0, 10), 'GOLD');
    }

    /**
     * 查询银池
     */
    async querySilverPool() {
        const silverTokens = this.tieredObserver.getTokensByTier('SILVER');
        if (silverTokens.length === 0) {
            console.log('🧠 [BatchAIAdvisor] 银池为空，跳过查询');
            return;
        }

        console.log(`\n🧠 [BatchAIAdvisor] 银池批量查询 (${silverTokens.length} 个代币)`);
        await this.batchQuery(silverTokens.slice(0, 10), 'SILVER');
    }

    /**
     * 合并查询 (金池 + 银池一起)
     * v8.1: 慢轨 - 全池覆盖，过滤已分析过的币
     */
    async queryCombined() {
        // v9.3: 设置分析锁
        if (this.isAnalyzing) {
            console.log(`🚃 [慢轨] 上一轮分析未完成，跳过`);
            return;
        }
        this.isAnalyzing = true;

        try {
            // 仓位预检查：如果满仓，跳过 AI 查询
            if (this.crossValidator) {
                const stats = this.crossValidator.getActivePositionStats();
                const maxPositions = 10;  // v8.1 扩容
                const remaining = maxPositions - stats.total;

                if (remaining <= 0) {
                    console.log(`🚃 [慢轨] 仓位已满 (${stats.total}/${maxPositions})，暂停 AI 查询`);
                    return;
                }
                console.log(`🚃 [慢轨] 剩余仓位: ${remaining}/${maxPositions}`);
            }

            // 读取 observation_pool.json
            let allPoolTokens = [];
            try {
                const poolPath = path.join(process.cwd(), 'data', 'observation_pool.json');
                if (fs.existsSync(poolPath)) {
                    const poolData = JSON.parse(fs.readFileSync(poolPath, 'utf8'));
                    allPoolTokens = poolData.tokens || [];
                }
            } catch (e) {
                console.error('❌ [慢轨] 读取观察池失败:', e.message);
            }

            // v8.1: 过滤掉刚被快轨分析过的币
            const now = Date.now();
            allPoolTokens = allPoolTokens.filter(t => {
                const lastAnalyzed = this.recentlyAnalyzed.get(t.address);
                // v9.3: 也过滤正在分析的代币
                if (this.analyzingTokens.has(t.address)) return false;
                return !lastAnalyzed || (now - lastAnalyzed >= this.ANALYSIS_COOLDOWN);
            });

        // 按 tier 分组
        const goldTokens = allPoolTokens.filter(t => t.tier === 'GOLD');
        const silverTokens = allPoolTokens.filter(t => t.tier === 'SILVER');

        // ═══════════════════════════════════════════════════════════════
        // v9.0 STABLE 高分优先队列 (Gold Dog Protection)
        // 79.3% 被误杀金狗是 STABLE 信号，优先分析这类代币
        // ═══════════════════════════════════════════════════════════════
        const stableHighScoreTokens = allPoolTokens.filter(t => {
            const signalType = t.signalTrendType || t.trendType || '';
            const baseScore = t.baseScore || t.score || 0;
            const sm = t.smCurrent || t.smartMoney || 0;
            return signalType === 'STABLE' && baseScore >= 55 && sm >= 2;
        }).map(t => ({ ...t, poolType: 'STABLE_PRIORITY', priorityReason: 'STABLE高分保护' }));

        // v9.0 ACCELERATING 信号优先队列
        const acceleratingTokens = allPoolTokens.filter(t => {
            const signalType = t.signalTrendType || t.trendType || '';
            const baseScore = t.baseScore || t.score || 0;
            return signalType === 'ACCELERATING' && baseScore >= 50;
        }).map(t => ({ ...t, poolType: 'ACCEL_PRIORITY', priorityReason: '链上加速信号' }));

        // v8.1 待审队列: 优先加入被阻塞的非 GOLDEN 代币
        let pendingTokens = [];
        if (this.crossValidator && this.crossValidator.getPendingTokens) {
            pendingTokens = this.crossValidator.getPendingTokens()
                .map(t => ({ ...t, poolType: 'PENDING' }));
        }

        // ═══════════════════════════════════════════════════════════════
        // v9.0 动态配额逻辑 - 优先级: ACCEL > STABLE高分 > 待审 > 金池 > 银池
        // ═══════════════════════════════════════════════════════════════
        const MAX_TOTAL = 10;

        // 1. ACCELERATING 最高优先级 (最多2个)
        const selectedAccel = acceleratingTokens.slice(0, 2);
        let remaining = MAX_TOTAL - selectedAccel.length;

        // 2. STABLE 高分次优先 (最多3个)
        const selectedStable = stableHighScoreTokens
            .filter(t => !selectedAccel.find(a => a.address === t.address))
            .slice(0, Math.min(3, remaining));
        remaining -= selectedStable.length;

        // 3. 待审队列 (最多2个)
        const selectedPending = pendingTokens
            .filter(t => !selectedAccel.find(a => a.address === t.address) &&
                        !selectedStable.find(s => s.address === t.address))
            .slice(0, Math.min(2, remaining));
        remaining -= selectedPending.length;

        // 4. 金池补充 (剩余配额)
        const selectedGold = goldTokens
            .filter(t => !selectedAccel.find(a => a.address === t.address) &&
                        !selectedStable.find(s => s.address === t.address) &&
                        !selectedPending.find(p => p.address === t.address))
            .slice(0, remaining)
            .map(t => ({ ...t, poolType: 'GOLD' }));
        remaining -= selectedGold.length;

        // 5. 银池补充 (剩余配额)
        const selectedSilver = remaining > 0
            ? silverTokens
                .filter(t => !selectedAccel.find(a => a.address === t.address) &&
                            !selectedStable.find(s => s.address === t.address) &&
                            !selectedPending.find(p => p.address === t.address) &&
                            !selectedGold.find(g => g.address === t.address))
                .slice(0, remaining)
                .map(t => ({ ...t, poolType: 'SILVER' }))
            : [];

        const allTokens = [...selectedAccel, ...selectedStable, ...selectedPending, ...selectedGold, ...selectedSilver];

        if (allTokens.length === 0) {
            console.log('🚃 [慢轨] 观察池为空或全已分析，跳过查询');
            return;
        }

        // v9.0 详细日志
        const accelCount = selectedAccel.length;
        const stableCount = selectedStable.length;
        const pendingCount = selectedPending.length;
        console.log(`\n🚃 [慢轨] 全池分析 ${allTokens.length} 个代币`);
        console.log(`   优先级配额: 🔥ACCEL=${accelCount} | 🛡️STABLE=${stableCount} | ⏳待审=${pendingCount} | 🥇金池=${selectedGold.length} | 🥈银池=${selectedSilver.length}`);
        this.stats.slowQueries++;

        // 标记为已分析
        allTokens.forEach(t => this.recentlyAnalyzed.set(t.address, Date.now()));

        await this.batchQuery(allTokens, 'COMBINED');
        } finally {
            // v9.3: 释放分析锁
            this.isAnalyzing = false;
        }
    }

    /**
     * 批量查询 AI
     */
    async batchQuery(tokens, poolType) {
        if (!this.client) {
            console.error('❌ [BatchAIAdvisor] API 未初始化');
            return;
        }

        if (tokens.length === 0) return;

        // 构建 Prompt
        const prompt = this.buildBatchPrompt(tokens, poolType);

        try {
            this.stats.queries++;
            console.log(`🤖 [BatchAIAdvisor] 正在分析 ${tokens.length} 个代币...`);

            const completion = await Promise.race([
                this.client.chat.completions.create({
                    model: AI_CONFIG.MODEL || 'grok-4-1-fast-reasoning',
                    messages: [
                        {
                            role: 'system',
                            content: '你是一个胜率和盈利极高的MEME猎手，对SOL和BSC链上的MEME币有深入研究。请根据提供的代币信息给出专业的交易建议。'
                        },
                        {
                            role: 'user',
                            content: prompt
                        }
                    ],
                    temperature: AI_CONFIG.TEMPERATURE || 0.3
                }),
                this.timeoutPromise(AI_CONFIG.TIMEOUT_MS || 60000)
            ]);

            const content = completion.choices[0]?.message?.content;
            if (!content) {
                console.error('❌ [BatchAIAdvisor] 无响应内容');
                return;
            }

            // v8.0 保存 AI 分析记录到文件
            this.saveAnalysisLog(tokens, prompt, content);

            // 解析并处理回复
            this.processAIResponse(content, tokens);

        } catch (error) {
            console.error(`❌ [BatchAIAdvisor] 查询失败: ${error.message}`);
        }
    }

    /**
     * 构建批量查询 Prompt v5.0
     * 让 Grok 自己查询链上数据、推特、TG，自己做判断
     */
    buildBatchPrompt(tokens, poolType) {
        // 获取剩余仓位数
        let remainingPositions = 'N/A';
        if (this.crossValidator) {
            const stats = this.crossValidator.getActivePositionStats();
            const maxPositions = 10;  // v8.1 扩容
            remainingPositions = maxPositions - stats.total;
        }

        // v5.0: 简化代币列表，只提供 CA 和基本信息
        // v9.0: 增加链上信号强度标记
        let tokenList = '';
        tokens.forEach((token, index) => {
            const chain = token.chain || 'SOL';
            const sm = token.smartMoney || token.smCurrent || 0;
            const mcap = token.marketCap || 0;
            const liq = token.liquidity || 0;
            const signalType = token.signalTrendType || token.trendType || '';
            const baseScore = token.baseScore || token.score || 0;

            // v9.0: 链上信号强度标记
            let chainSignal = '';
            if (signalType === 'ACCELERATING') {
                chainSignal = '🔥链上加速';
            } else if (signalType === 'STABLE' && baseScore >= 55) {
                chainSignal = '📈链上稳定高分';
            }
            if (sm === 2) {
                chainSignal += chainSignal ? '+SM=2黄金' : '⭐SM=2黄金组合';
            }

            tokenList += `${index + 1}. $${token.symbol} (${chain})${chainSignal ? ' ' + chainSignal : ''}\n`;
            tokenList += `   CA: ${token.address}\n`;
            tokenList += `   系统参考数据: SM=${sm}, MCAP=$${(mcap / 1000).toFixed(1)}K, Liq=$${(liq / 1000).toFixed(1)}K, Score=${baseScore}${signalType ? ', 信号=' + signalType : ''}\n\n`;
        });

        return `你是一个专业的 MEME 币猎手。你需要先理解我的卖出策略，然后在此基础上做出买入决策。

═══════════════════════════════════════════
� **我的卖出策略（你需要理解）**
═══════════════════════════════════════════

**核心退出维度**：
| 维度 | 触发条件 |
|------|----------|
| 动量退出 | 1分钟跌20%强制退出；放量下跌警报 |
| 聪明钱退出 | SM<2个考虑退出；减少50%强制退出 |
| 流动性退出 | 流动性<$5K退出；滑点>5%退出 |
| 社交热度 | 热度衰减到30%以下退出 |

**叙事层级止损**：
- TIER_S: -70% 止损
- TIER_A: -60% 止损
- TIER_B: -50% 止损
- TIER_C: -40% 止损

**利润保护阶梯**：
| 利润 | 保护比例 | 说明 |
|------|----------|------|
| +50% | 保护20% | 赚50%后，最少锁利10% |
| +100% | 保护50% | 翻倍后，最少锁利50% |
| +200% | 保护70% | 3倍后，最少锁利140% |
| +500% | 保护80% | 6倍后，最少锁利400% |

═══════════════════════════════════════════
📊 **我的配置**
═══════════════════════════════════════════
- 剩余仓位: ${remainingPositions} 个
- **每批可推荐 1-3 个**，优先推荐最有潜力的
- 宁可多推荐让系统过滤，也不要漏掉潜力币

📋 **待评估代币**:
${tokenList}

🔍 **请你自己查询**:
1. 去 Dexscreener/Birdeye/GMGN 查询实时链上数据
2. 去推特 (X) 和 Telegram 搜索社区热度和情绪
3. 如果查不到，参考系统参考数据

⚠️ **重要提示 - 链上信号优先**:
- 带有🔥🔥链上加速 或 ⭐SM=2黄金组合 标记的代币，链上数据已经很强
- 这类代币即使Twitter暂时没热度，也应该优先考虑BUY
- 金狗往往在Twitter火之前就有链上信号，不要因为Twitter冷就DISCARD

═══════════════════════════════════════════
📝 **分析步骤**
═══════════════════════════════════════════

**第一步：横向对比（本批内）**
快速评估所有代币，选出本批中**相对最优**的那个。

**第二步：叙事独特性 + 时效性判断**
- 叙事是独一无二的还是同类已经很多？
- 同类叙事有龙头币吗？这个还有机会吗？
- **🔥 是否绑定今天的新闻/热门话题/流行梗？**
- 这个热点是刚出来还是已经过气了？

**第三步：金狗潜力评估（与市场对比）**
- 对比近期金狗（10x+），这个币有类似潜力吗？
- 社区热度和资金流入是否在早期？
- 有成为板块龙头的潜力吗？

**第四步：深入分析** (如果有金狗潜力)
### $SYMBOL
1. **叙事分析**: 故事+热点+独特性
2. **链上数据**: 聪明钱、流动性、买卖比
3. **社区热度**: 推特/TG 讨论情况
4. **叙事等级**: TIER_S / TIER_A / TIER_B / TIER_C

**第五步：目标市值与卖出计划**
基于叙事等级和潜力，预估：
5. **目标市值**: 这个币能涨到多少市值？(如 $500K / $1M / $5M)
6. **分段止盈计划**: 
   - 第一止盈点: 市值 $XXX 时卖 XX%
   - 第二止盈点: 市值 $XXX 时卖 XX%
   - 最终止盈点: 市值 $XXX 时卖剩余

**第六步：入场时机判断**
- 当前市值 vs 目标市值，空间有多少倍？
- 现在是好的入场点吗？如果涨太多，等回调到什么价格？

**第七步：最终建议**
7. **操作建议** (必须是以下三种之一):
   - **BUY: $SYMBOL (CA: 代币合约地址) (叙事等级 TIER_X, 目标市值 $XXX)** — 现在买，有X倍空间
   - **WATCH_ENTRY: $SYMBOL (CA: 代币合约地址) (目标价格 $X.XX, 目标市值 $XXX)** — 等回调再买
   - **DISCARD: $SYMBOL (CA: 代币合约地址)** — 无金狗潜力，理由...

‼️ 重要：
- **必须在建议行写明合约地址 (CA)**，这是系统唯一识别代币的依据
- 买入前先想好怎么卖：目标市值、止盈点位
- 预期空间≥2x即可考虑（不需要非得3x+）
- **链上信号强（SM=2或ACCELERATING）时，即使Twitter没热度也可以BUY**
- **宁可多推荐让系统过滤，也不要因为保守漏掉金狗**
- 第7点必须明确写 "BUY:", "WATCH_ENTRY:", 或 "DISCARD:"`;
    }

    /**
     * 处理 AI 回复
     */
    processAIResponse(content, tokens) {
        // 保存到历史记录文件 (v1.2: 专门日志系统)
        try {
            const logDir = path.join(process.cwd(), 'logs', 'ai');
            if (!fs.existsSync(logDir)) fs.mkdirSync(logDir, { recursive: true });

            const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
            const logFile = path.join(logDir, `batch_${timestamp}.log`);

            const logContent = [
                `Time: ${new Date().toLocaleString('zh-CN')}`,
                `Tokens: ${tokens.map(t => `$${t.symbol}`).join(', ')}`,
                `═`.repeat(60),
                content,
                `═`.repeat(60)
            ].join('\n');

            fs.writeFileSync(logFile, logContent);
            console.log(`💾 [BatchAIAdvisor] 分析历史已保存至: logs/ai/batch_${timestamp}.log`);
        } catch (e) {
            console.error('❌ [BatchAIAdvisor] 保存历史日志失败:', e.message);
        }

        console.log('\n' + '═'.repeat(60));
        console.log('🧠 [BatchAIAdvisor] AI 分析结果:');
        console.log('═'.repeat(60));
        console.log(content);
        console.log('═'.repeat(60) + '\n');

        // 解析回复，提取操作建议
        const buyTokens = [];
        const watchTokens = [];
        const discardTokens = [];

        // v2: 基于章节解析（更准确）
        // 尝试匹配建议章节的标题（从后往前找，确保匹配最后一个"最终建议"等章节）
        // 简化后的正则：只要行内包含关键词即可
        const opSectionHeaderRegex = /(?:操作建议|最终建议|最终决策|决策建议|Final\s*Decision|Final\s*Recommendation)/gim;
        let lastHeaderMatch = null;
        let match;
        while ((match = opSectionHeaderRegex.exec(content)) !== null) {
            lastHeaderMatch = match;
        }

        if (lastHeaderMatch) {
            const opSection = content.substring(lastHeaderMatch.index);
            const opLines = opSection.split('\n');

            for (const line of opLines) {
                const upperLine = line.toUpperCase();

                // 遍历所有代币，查找该行是否提及 (优先匹配 CA，其次匹配 Symbol)
                for (const token of tokens) {
                    const symbol = token.symbol;
                    const address = token.address;

                    // 1. 优先通过地址匹配 (CA)
                    const addressRegex = new RegExp(address.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
                    // 2. 备选通过符号匹配
                    const symbolRegex = new RegExp(`\\$?${symbol.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}(?![a-zA-Z0-9])`, 'i');

                    if (addressRegex.test(line) || symbolRegex.test(line)) {
                        const lineClean = line.replace(/[*#]/g, '').trim();
                        // 构造更严格的动作指令正则：动作词 必须紧邻标点或位于行核心位置
                        const isActionLine = (
                            new RegExp(`(?:BUY|买入|入手|WATCH_ENTRY|再买|到位买|WATCH|观察|再看|DISCARD|丢弃|放弃|排除)[\\s:：]`, 'i').test(lineClean) ||
                            new RegExp(`[:：\\s-](?:BUY|买入|入手|WATCH_ENTRY|再买|到位买|WATCH|观察|再看|DISCARD|丢弃|放弃|排除)`, 'i').test(lineClean)
                        );

                        if (!isActionLine) continue;

                        // 1. 检查 BUY (支持 BUY: $SYM 或 $SYM: BUY)
                        if (upperLine.includes('BUY') || upperLine.includes('买入') || upperLine.includes('入手')) {
                            if (!buyTokens.includes(token)) {
                                // 提取 TIER
                                const tierMatch = line.match(/TIER[_\s]?(S|A|B|C)/i);
                                if (tierMatch) {
                                    token.intentionTier = `TIER_${tierMatch[1].toUpperCase()}`;
                                }
                                buyTokens.push(token);
                                this.stats.buyRecommendations++;
                                console.log(`🧠 [BatchAIAdvisor] 解析到买入: ${symbol}${token.intentionTier ? ` (${token.intentionTier})` : ''}`);
                            }
                        }
                        // 2. 检查 WATCH_ENTRY
                        else if (upperLine.includes('WATCH_ENTRY') || upperLine.includes('再买') || upperLine.includes('到位买')) {
                            if (!buyTokens.includes(token) && !watchTokens.includes(token)) {
                                const priceMatch = line.match(/目标价格[：:\s]*\$?([\d.]+)/i) ||
                                    line.match(/\$?([\d.]+)\s*再买/i) ||
                                    line.match(/\$([\d.]+)/);
                                if (priceMatch) {
                                    token.targetEntryPrice = parseFloat(priceMatch[1]);
                                    token.isWatchEntry = true;
                                }
                                const tierMatch = line.match(/TIER[_\s]?(S|A|B|C)/i);
                                if (tierMatch) {
                                    token.intentionTier = `TIER_${tierMatch[1].toUpperCase()}`;
                                }
                                watchTokens.push(token);
                                this.stats.watchRecommendations++;
                                console.log(`🎯 [BatchAIAdvisor] 解析到盯盘: ${symbol}${token.targetEntryPrice ? ` @$${token.targetEntryPrice}` : ''}`);
                            }
                        }
                        // 3. 检查 WATCH
                        else if (upperLine.includes('WATCH') || upperLine.includes('观察') || upperLine.includes('再看')) {
                            if (!buyTokens.includes(token) && !watchTokens.includes(token)) {
                                watchTokens.push(token);
                                this.stats.watchRecommendations++;
                            }
                        }
                        // 4. 检查 DISCARD
                        else if (upperLine.includes('DISCARD') || upperLine.includes('丢弃') || upperLine.includes('放弃') || upperLine.includes('排除')) {
                            if (!buyTokens.includes(token) && !watchTokens.includes(token) && !discardTokens.includes(token)) {
                                discardTokens.push(token);
                                this.stats.discardRecommendations++;
                            }
                        }
                    }
                }
            }
        } else {
            // Fallback: 旧逻辑（以防AI不按格式输出）
            console.warn('⚠️ [BatchAIAdvisor] 未找到"操作建议"章节，使用兜底解析');
            // 简单解析：查找 BUY/WATCH/DISCARD 关键词
            const contentUpper = content.toUpperCase();

            tokens.forEach(token => {
                const symbolUpper = token.symbol.toUpperCase();
                const address = token.address.toUpperCase();

                // 在回复中查找该代币所在的行或上下文
                // 1. 查找包含 CA 或 Symbol 的那一块文本（前后各 30 字符，缩小窗口减少误报）
                const caEscaped = address.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
                const symEscaped = symbolUpper.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
                const contextRegex = new RegExp(`(?:.{0,30})(?:${caEscaped}|\\$?${symEscaped})(?:.{0,30})`, 'i');
                const contextMatch = content.match(contextRegex);

                if (contextMatch) {
                    const contextText = contextMatch[0].toUpperCase();
                    // 必须包含 BUY/WATCH 等明确动作词，且排除描述性文字
                    if (contextText.includes('BUY') || contextText.includes('买入') || contextText.includes('入手')) {
                        buyTokens.push(token);
                        this.stats.buyRecommendations++;
                    } else if (contextText.includes('DISCARD') || contextText.includes('丢弃') || contextText.includes('排除')) {
                        discardTokens.push(token);
                        this.stats.discardRecommendations++;
                    } else if (contextText.includes('WATCH') || contextText.includes('观察') || contextText.includes('再看')) {
                        watchTokens.push(token);
                        this.stats.watchRecommendations++;
                    }
                }
            });
        }

        // 未被分类的代币默认为WATCH
        tokens.forEach(token => {
            if (!buyTokens.includes(token) && !watchTokens.includes(token) && !discardTokens.includes(token)) {
                watchTokens.push(token);
            }
        });

        // ═══════════════════════════════════════════════════════════════
        // v9.0 金狗保护规则应用 (Gold Dog Protection)
        // 对于 AI 判定为 DISCARD 的代币，检查是否满足保护条件
        // 79.3% 被误杀金狗是 STABLE 信号，86.2% 基础分 ≥55
        // ═══════════════════════════════════════════════════════════════
        const protectedTokens = [];
        const upgradedToBuy = [];

        discardTokens.forEach(token => {
            // 构建保护规则需要的数据结构
            const protectionData = {
                baseScore: token.baseScore || token.score || 0,
                smCount: token.smCurrent || token.smartMoney || 0,
                signalTrendType: token.signalTrendType || token.trendType || 'UNKNOWN'
            };

            const protection = applyProtectionRules(protectionData, 'DISCARD');

            if (protection.protectionApplied) {
                console.log(`   🛡️ [金狗保护] ${token.symbol}: ${protection.reason}`);

                if (protection.finalDecision === 'BUY_CANDIDATE') {
                    // 升级为 BUY 候选
                    upgradedToBuy.push(token);
                    protectedTokens.push({ token, protection });
                } else if (protection.finalDecision === 'WATCH') {
                    // 降级为 WATCH，从 DISCARD 移除
                    if (!watchTokens.includes(token)) {
                        watchTokens.push(token);
                    }
                    protectedTokens.push({ token, protection });
                }
            }
        });

        // 从 DISCARD 中移除被保护的代币
        protectedTokens.forEach(({ token }) => {
            const idx = discardTokens.indexOf(token);
            if (idx > -1) {
                discardTokens.splice(idx, 1);
            }
        });

        // 将升级为 BUY 的代币加入 buyTokens
        upgradedToBuy.forEach(token => {
            if (!buyTokens.includes(token)) {
                buyTokens.push(token);
                this.stats.buyRecommendations++;
                console.log(`   🚀 [保护升级] ${token.symbol} 从 DISCARD 升级为 BUY 候选`);
            }
        });

        if (protectedTokens.length > 0) {
            console.log(`\n🛡️ [金狗保护统计] 本批次保护了 ${protectedTokens.length} 个被AI误杀的代币`);
            console.log(`   升级为BUY: ${upgradedToBuy.length} | 降级为WATCH: ${protectedTokens.length - upgradedToBuy.length}`);
        }

        // 输出解析验证表
        console.log('\n📊 [解析验证] AI建议 vs 系统理解:');
        console.log('─'.repeat(60));
        console.log(`🟢 BUY    : ${buyTokens.map(t => t.symbol).join(', ') || '(无)'}`);
        console.log(`🟡 WATCH  : ${watchTokens.map(t => t.symbol).join(', ') || '(无)'}`);
        console.log(`🔴 DISCARD: ${discardTokens.map(t => t.symbol).join(', ') || '(无)'}`);
        console.log('─'.repeat(60) + '\n');

        // ═══════════════════════════════════════════════════════════════
        // v9.1 智能硬门槛检查 (与叙事等级联动)
        //
        // 问题: 固定 -30% 阈值与 NARRATIVE_STOP_LOSS 冲突
        //       TIER_S 允许 -70% 止损，但硬门槛在 -30% 就拒绝了
        //
        // 解决: 根据 AI 给出的叙事等级动态调整跌幅阈值
        //       同时保护 STABLE/ACCELERATING 信号 (79.3% 金狗是 STABLE)
        // ═══════════════════════════════════════════════════════════════
        const qualifiedBuyTokens = buyTokens.filter(token => {
            const sm = token.smartMoney || token.smCurrent || 0;
            const smInitial = token.smInitial || sm;
            const smDelta = sm - smInitial;
            const priceChange = parseFloat(token.priceChange || 0);
            const signalType = token.signalTrendType || token.trendType || '';
            const intentionTier = token.intentionTier || 'TIER_C';  // AI给出的叙事等级
            const smCount = token.smartMoney || token.smCurrent || 0;
            const isProtectedSignal = signalType === 'STABLE' || signalType === 'ACCELERATING';

            // ═══════════════════════════════════════════════════════════════
            // 规则 1: SM 净流出检查 (保持严格)
            // ═══════════════════════════════════════════════════════════════
            if (smDelta < 0) {
                // v9.1: STABLE/ACCELERATING 信号允许轻微流出 (-1)
                if (!isProtectedSignal || smDelta < -1) {
                    console.log(`   ❌ [硬门槛] ${token.symbol}: SM 净流出 (${smDelta})，拒绝买入`);
                    // v7.4: 记录拒绝快照用于回测
                    snapshotRecorder.recordRejection(token, 'sm_flow', `SM净流出 ${smDelta}`, {
                        smDelta, priceChange, signalType, intentionTier, smCount
                    });
                    return false;
                }
                console.log(`   ⚠️ [硬门槛] ${token.symbol}: SM 轻微流出 (${smDelta})，但 ${signalType} 信号保护，放行`);
            }

            // ═══════════════════════════════════════════════════════════════
            // 规则 2: 涨幅过高检查 (v9.2 智能阈值 - 叙事/信号联动)
            //
            // 问题: 固定 80% 阈值杀死了 MEME (+733.4%) 等金狗
            //       高阶叙事 + ACCELERATING 信号的爆发性增长应该被允许
            //
            // 解决: 根据叙事等级和信号类型动态调整涨幅阈值
            // ═══════════════════════════════════════════════════════════════
            const isEarlyGem = (token.marketCap || 0) < 50000;

            // v9.2: 涨幅阈值根据叙事等级和信号类型调整
            const riseThresholds = {
                'TIER_S': { base: 500, accel: 2000, early: 10000 },  // S级允许爆发
                'TIER_A': { base: 300, accel: 1000, early: 8000 },
                'TIER_B': { base: 150, accel: 500, early: 6000 },
                'TIER_C': { base: 80, accel: 200, early: 5000 },
                'default': { base: 80, accel: 200, early: 5000 }
            };

            const tierThresholds = riseThresholds[intentionTier] || riseThresholds.default;

            let maxPriceChange;
            if (isEarlyGem) {
                maxPriceChange = tierThresholds.early;
            } else if (isProtectedSignal) {
                maxPriceChange = tierThresholds.accel;
            } else {
                maxPriceChange = tierThresholds.base;
            }

            // v9.2: SM=2 黄金组合额外放宽 50%
            if (smCount >= 2 && isProtectedSignal) {
                maxPriceChange *= 1.5;
                console.log(`   🛡️ [SM=2黄金组合] ${token.symbol}: 涨幅阈值放宽到 ${maxPriceChange.toFixed(0)}%`);
            }

            if (priceChange > maxPriceChange) {
                console.log(`   ❌ [硬门槛] ${token.symbol}: 涨幅过高 (+${priceChange.toFixed(1)}% > ${maxPriceChange.toFixed(0)}%)，${intentionTier}${isProtectedSignal ? '+' + signalType : ''} 阈值`);
                // v7.4: 记录拒绝快照用于回测
                snapshotRecorder.recordRejection(token, 'price_rise', `涨幅过高 +${priceChange.toFixed(1)}% > ${maxPriceChange.toFixed(0)}%`, {
                    priceChange, maxPriceChange, intentionTier, signalType, smCount, isProtectedSignal, isEarlyGem
                });
                return false;
            }

            // ═══════════════════════════════════════════════════════════════
            // 规则 3: 跌幅检查 (v9.2 Phoenix/PVP 幸存者保护)
            //
            // 问题: 固定 -30% 阈值杀死了 BITLORD(-61.7%, 42x)、BOAR(-45.3%, 20x) 等金狗
            //       40% 的金狗有 DECREASING 趋势，但配合 ACCELERATING 信号意味着正在反弹
            //
            // 解决:
            // 1. 与 NARRATIVE_STOP_LOSS 对齐 (留 10% 缓冲)
            // 2. STABLE/ACCELERATING 信号额外放宽 15%
            // 3. Phoenix 检测: ACCELERATING + 大跌幅 = 反弹中的凤凰
            // 4. SM=2 黄金组合额外放宽 10%
            // ═══════════════════════════════════════════════════════════════
            const dropThresholds = {
                'TIER_S': -60,  // S级大叙事允许深度回调
                'TIER_A': -50,
                'TIER_B': -40,
                'TIER_C': -30,
                'default': -30
            };

            let maxDrop = dropThresholds[intentionTier] || dropThresholds.default;

            // v9.2: STABLE/ACCELERATING 信号额外放宽 15% (原来是10%)
            if (isProtectedSignal) {
                maxDrop -= 15;  // 例如 TIER_B 从 -40% 放宽到 -55%
            }

            // v9.2: SM=2 黄金组合额外放宽 10%
            if (smCount >= 2 && isProtectedSignal) {
                maxDrop -= 10;  // 例如 TIER_B + ACCELERATING + SM=2 = -65%
                console.log(`   🛡️ [SM=2黄金组合] ${token.symbol}: 跌幅阈值放宽到 ${maxDrop}%`);
            }

            // v9.2: Phoenix/PVP 幸存者检测
            // 如果是 ACCELERATING 信号 + 大跌幅，说明是从低点反弹的凤凰
            // 这种情况应该更宽容，因为 ACCELERATING 意味着链上资金在加速流入
            const isPhoenix = signalType === 'ACCELERATING' && priceChange < -40;
            if (isPhoenix) {
                maxDrop -= 20;  // Phoenix 额外放宽 20%，例如 -55% -> -75%
                console.log(`   🔥 [Phoenix检测] ${token.symbol}: ACCELERATING + 深跌 = 凤凰反弹，阈值放宽到 ${maxDrop}%`);
            }

            if (priceChange < maxDrop) {
                console.log(`   ❌ [硬门槛] ${token.symbol}: 跌幅过大 (${priceChange.toFixed(1)}% < ${maxDrop}%)，${intentionTier}${isProtectedSignal ? '+' + signalType : ''}${isPhoenix ? '+Phoenix' : ''} 阈值`);
                // v7.4: 记录拒绝快照用于回测
                snapshotRecorder.recordRejection(token, 'price_drop', `跌幅过大 ${priceChange.toFixed(1)}% < ${maxDrop}%`, {
                    priceChange, maxDrop, intentionTier, signalType, smCount, isProtectedSignal, isPhoenix
                });
                return false;
            }

            console.log(`   ✅ [硬门槛] ${token.symbol}: 通过 (SM${smDelta >= 0 ? '+' : ''}${smDelta}, 涨幅${priceChange >= 0 ? '+' : ''}${priceChange.toFixed(1)}%, ${intentionTier}${isProtectedSignal ? '+' + signalType : ''}${isPhoenix ? '+Phoenix' : ''} 阈值${maxDrop}%)`);
            // v7.4: 记录通过快照用于回测
            snapshotRecorder.recordPassed(token, {
                smDelta, priceChange, intentionTier, signalType, smCount, isProtectedSignal, isPhoenix, maxDrop, maxPriceChange
            });
            return true;

        });

        // 输出分类结果
        if (qualifiedBuyTokens.length > 0) {
            console.log(`🟢 [BUY] 通过硬门槛: ${qualifiedBuyTokens.map(t => t.symbol).join(', ')}`);
        } else if (buyTokens.length > 0) {
            console.log(`⚠️ [BUY] AI推荐了 ${buyTokens.length} 个币，但全部未通过硬门槛检查`);
        }
        if (watchTokens.length > 0) {
            console.log(`🟡 [WATCH] 继续观察: ${watchTokens.map(t => t.symbol).join(', ')}`);
            // v7.4: 记录 WATCH 决策用于回测
            watchTokens.forEach(t => {
                snapshotRecorder.recordWatch(t, { reason: 'AI建议继续观察', aiDecision: 'WATCH' });
            });
        }
        if (discardTokens.length > 0) {
            console.log(`🔴 [DISCARD] 建议丢弃: ${discardTokens.map(t => t.symbol).join(', ')}`);
        }

        // 发出事件 (只发送通过硬门槛的币)
        qualifiedBuyTokens.forEach(token => {
            this.emit('buy', { token, aiResponse: content });
        });

        // v7.4: 处理被硬门槛拒绝的 BUY 信号
        const rejectedByHardgate = buyTokens.filter(t => !qualifiedBuyTokens.includes(t));
        if (rejectedByHardgate.length > 0) {
            rejectedByHardgate.forEach(token => {
                this.emit('hardgate_reject', { token, aiResponse: content });
            });
        }

        discardTokens.forEach(token => {
            this.emit('discard', { token, aiResponse: content });
        });

        // v5.0: 发出 WATCH_ENTRY 事件（等待价格到达目标点位再买入）
        const watchEntryTokens = watchTokens.filter(t => t.isWatchEntry && t.targetEntryPrice);
        if (watchEntryTokens.length > 0) {
            console.log(`🎯 [WATCH_ENTRY] 等待入场: ${watchEntryTokens.map(t => `${t.symbol}@$${t.targetEntryPrice}`).join(', ')}`);
            watchEntryTokens.forEach(token => {
                this.emit('watch_entry', {
                    token,
                    targetPrice: token.targetEntryPrice,
                    intentionTier: token.intentionTier,
                    aiResponse: content
                });
            });
        }
    }

    /**
     * 超时 Promise
     */
    timeoutPromise(ms) {
        return new Promise((_, reject) => {
            setTimeout(() => reject(new Error('请求超时')), ms);
        });
    }

    /**
     * v8.0 保存 AI 分析记录到文件
     */
    saveAnalysisLog(tokens, prompt, response) {
        try {
            const logPath = path.join(process.cwd(), 'data/ai_analysis_log.json');

            // 读取现有记录
            let logs = [];
            if (fs.existsSync(logPath)) {
                try {
                    logs = JSON.parse(fs.readFileSync(logPath, 'utf8'));
                } catch (e) {
                    logs = [];
                }
            }

            // 添加新记录
            const record = {
                timestamp: new Date().toISOString(),
                tokenCount: tokens.length,
                tokens: tokens.map(t => ({
                    symbol: t.symbol,
                    address: t.address,
                    chain: t.chain,
                    score: t.score,
                    marketCap: t.marketCap,
                    smartMoney: t.smartMoney
                })),
                prompt: prompt.substring(0, 500) + '...',  // 截断 prompt
                response: response,
                stats: { ...this.stats }
            };

            logs.push(record);  // 新记录放后面，方便 tail 查看

            // 只保留最近 50 条（由于是 push，截取最后 50 条）
            if (logs.length > 50) {
                logs = logs.slice(-50);
            }

            fs.writeFileSync(logPath, JSON.stringify(logs, null, 2));
            console.log(`📝 [BatchAIAdvisor] 分析记录已保存到 data/ai_analysis_log.json`);
        } catch (e) {
            console.error(`❌ [BatchAIAdvisor] 保存分析记录失败:`, e.message);
        }
    }

    /**
     * 获取统计
     */
    getStats() {
        return {
            ...this.stats,
            initialized: this.initialized
        };
    }
}

export default BatchAIAdvisor;
