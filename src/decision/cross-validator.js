/**
 * Cross Validator - 交叉验证系统
 * 
 * 核心逻辑：
 * - DeBot (主力) = "事实" - 链上真金白银
 * - Telegram (辅助) = "情绪" - 社区热度
 * - 交叉验证 = "共识" - 既有钱也有人，才是主升浪
 * 
 * 决策矩阵：
 * - 早鸟局: DeBot有 + TG无 → 小仓 0.05 SOL (潜伏模式)
 * - 共识局: DeBot有 + TG有 → 中仓 0.15 SOL (双验证)
 * - 顶级局: 聪明钱多 + 叙事好 + TG热 → 大仓 0.2 SOL (S级共振)
 * - 纯土狗: 有聪明钱但无叙事 → 忽略
 * - 喊单盘: TG热但DeBot无信号 → 观察 (等DeBot确认)
 */

import { EventEmitter } from 'events';

export class CrossValidator extends EventEmitter {
    constructor(db) {
        super();
        this.db = db;
        
        // 配置
        this.config = {
            // DeBot 门槛
            minSmartWalletOnline: 2,    // 至少2个聪明钱在线
            minLiquidity: 10000,         // 最低流动性 $10,000
            minAiScore: 4,               // AI评分 >= 4 (10分制)
            
            // TG 热度门槛
            tgHotThreshold: 2,           // 至少2个频道提及
            tgTimeWindow: 30 * 60 * 1000, // 30分钟时间窗口
            
            // 仓位配置 (SOL)
            positions: {
                scout: 0.05,    // 早鸟局 - 小仓潜伏
                normal: 0.15,   // 共识局 - 中仓
                max: 0.2        // 顶级局 - 大仓
            }
        };
        
        // 决策缓存
        this.recentDecisions = new Map();
    }
    
    /**
     * 入口：验证 DeBot 信号
     * @param {Object} debotSignal - 来自 DeBot Scout 的信号
     * @returns {Object} 决策结果
     */
    async validate(debotSignal) {
        const tokenAddress = debotSignal.tokenAddress;
        const chain = debotSignal.chain;
        
        console.log(`\n🔍 [CrossValidator] 开始验证: ${debotSignal.symbol || tokenAddress.slice(0, 8)}...`);
        
        // 1. 获取 AI 叙事评分 (从 debotSignal 中提取，或已缓存)
        const aiScore = this.extractAiScore(debotSignal);
        
        // 2. 获取 TG 热度 (查询本地数据库)
        const tgHeat = await this.getTgHeat(tokenAddress, chain);
        
        // 3. 提取聪明钱数据
        const smartMoney = this.extractSmartMoney(debotSignal);
        
        // 4. 检查负面事件
        const hasNegative = this.checkNegativeEvents(debotSignal);
        
        // 5. 做出决策
        const decision = this.makeDecision({
            tokenAddress,
            chain,
            symbol: debotSignal.symbol || debotSignal.name || 'Unknown',
            smartMoney,
            aiScore,
            tgHeat,
            hasNegative,
            liquidity: debotSignal.liquidity || 0,
            marketCap: debotSignal.marketCap || 0,
            debotSignal
        });
        
        // 6. 缓存决策
        this.cacheDecision(tokenAddress, decision);
        
        // 7. 发射事件
        if (decision.action !== 'IGNORE') {
            this.emit('validated-signal', decision);
        }
        
        return decision;
    }
    
    /**
     * 提取 AI 评分
     */
    extractAiScore(signal) {
        // 从 AI 报告中提取评分
        if (signal.aiReport?.rating?.score) {
            return signal.aiReport.rating.score;
        }
        
        // 根据代币等级估算分数
        const tierScores = {
            'gold': 8,
            'silver': 6,
            'bronze': 4
        };
        
        if (signal.tokenTier && tierScores[signal.tokenTier]) {
            return tierScores[signal.tokenTier];
        }
        
        if (signal.tokenLevel && tierScores[signal.tokenLevel]) {
            return tierScores[signal.tokenLevel];
        }
        
        // 默认中等分数
        return 5;
    }
    
    /**
     * 提取聪明钱数据
     */
    extractSmartMoney(signal) {
        return {
            online: signal.smartWalletOnline || 0,
            total: signal.smartWalletTotal || 0,
            signalCount: signal.signalCount || 0,
            maxPriceGain: signal.maxPriceGain || 0,
            activityScore: signal.activityScore || 0
        };
    }
    
    /**
     * 检查负面事件
     */
    checkNegativeEvents(signal) {
        // 检查 AI 报告中的负面事件
        if (signal.aiReport?.distribution?.negativeIncidents) {
            const negative = signal.aiReport.distribution.negativeIncidents.toLowerCase();
            if (negative.includes('scam') || negative.includes('fraud') || negative.includes('rug')) {
                return true;
            }
        }
        
        // 检查安全信息
        if (signal.isMintAbandoned === false) {
            // Mint 权限未放弃，可能有风险
            return true;
        }
        
        return false;
    }
    
    /**
     * 获取 TG 热度 (查询本地数据库)
     */
    async getTgHeat(tokenAddress, chain) {
        if (!this.db) {
            return { count: 0, channels: [], tier1Count: 0 };
        }
        
        try {
            const timeWindow = Date.now() - this.config.tgTimeWindow;
            
            // 查询近30分钟的 TG 提及
            const stmt = this.db.prepare(`
                SELECT 
                    channel_name,
                    channel_tier,
                    COUNT(*) as mention_count
                FROM telegram_signals
                WHERE token_ca = ?
                  AND chain = ?
                  AND timestamp > ?
                GROUP BY channel_name, channel_tier
            `);
            
            const mentions = stmt.all(tokenAddress, chain, timeWindow);
            
            const totalCount = mentions.reduce((sum, m) => sum + m.mention_count, 0);
            const tier1Count = mentions.filter(m => m.channel_tier === 1).length;
            const channels = mentions.map(m => m.channel_name);
            
            return {
                count: totalCount,
                channels: channels,
                tier1Count: tier1Count,
                uniqueChannels: mentions.length
            };
            
        } catch (error) {
            console.error('[CrossValidator] TG heat query error:', error.message);
            return { count: 0, channels: [], tier1Count: 0 };
        }
    }
    
    /**
     * 核心决策逻辑
     */
    makeDecision(data) {
        const {
            tokenAddress,
            chain,
            symbol,
            smartMoney,
            aiScore,
            tgHeat,
            hasNegative,
            liquidity,
            marketCap,
            debotSignal
        } = data;
        
        const reasons = [];
        
        // === 硬性过滤 ===
        
        // 负面事件 → 直接拒绝
        if (hasNegative) {
            reasons.push('🚫 检测到负面事件/SCAM警告');
            return this.createDecision('IGNORE', null, 0, reasons, data);
        }
        
        // AI 评分太低 → 拒绝 (无叙事)
        if (aiScore < this.config.minAiScore) {
            reasons.push(`🚫 AI评分太低: ${aiScore}/10 (需要>=${this.config.minAiScore})`);
            return this.createDecision('IGNORE', null, 0, reasons, data);
        }
        
        // 流动性不足 → 拒绝
        if (liquidity < this.config.minLiquidity) {
            reasons.push(`🚫 流动性不足: $${liquidity.toLocaleString()} (需要>=$${this.config.minLiquidity.toLocaleString()})`);
            return this.createDecision('IGNORE', null, 0, reasons, data);
        }
        
        // === 软性评分 ===
        
        const hasSmartMoney = smartMoney.online >= this.config.minSmartWalletOnline;
        const hasTgHeat = tgHeat.count >= this.config.tgHotThreshold;
        const hasStrongAi = aiScore >= 7;
        const hasVeryHighSmartMoney = smartMoney.online >= 3;
        
        // === 决策矩阵 ===
        
        // 场景 A: 顶级局 (聪明钱多 + 叙事好 + TG热)
        if (hasVeryHighSmartMoney && hasStrongAi && hasTgHeat) {
            reasons.push(`🔥 S级共振: ${smartMoney.online}个聪明钱在线`);
            reasons.push(`✅ AI评分优秀: ${aiScore}/10`);
            reasons.push(`✅ TG热度: ${tgHeat.count}次提及, ${tgHeat.uniqueChannels || 0}个频道`);
            return this.createDecision('BUY_MAX', 'S', this.config.positions.max, reasons, data);
        }
        
        // 场景 B: 共识局 (聪明钱 + TG热)
        if (hasSmartMoney && hasTgHeat) {
            reasons.push(`✅ 链上+社交双验证`);
            reasons.push(`✅ ${smartMoney.online}个聪明钱在线`);
            reasons.push(`✅ TG: ${tgHeat.count}次提及`);
            return this.createDecision('BUY_NORMAL', 'A', this.config.positions.normal, reasons, data);
        }
        
        // 场景 C: 早鸟局 (只有聪明钱 + 叙事及格，TG还没反应)
        if (hasSmartMoney && aiScore >= this.config.minAiScore) {
            reasons.push(`🐦 潜伏模式: 聪明钱先知`);
            reasons.push(`✅ ${smartMoney.online}个聪明钱在线`);
            reasons.push(`✅ AI评分: ${aiScore}/10`);
            reasons.push(`⏳ TG尚未反应 (${tgHeat.count}次提及)`);
            return this.createDecision('BUY_SMALL', 'B', this.config.positions.scout, reasons, data);
        }
        
        // 场景 D: 观察 (聪明钱不足但有热度)
        if (!hasSmartMoney && hasTgHeat) {
            reasons.push(`👀 等待DeBot确认: TG热但聪明钱不足`);
            reasons.push(`⚠️ 聪明钱: ${smartMoney.online}/${this.config.minSmartWalletOnline}不足`);
            reasons.push(`✅ TG热度: ${tgHeat.count}次提及`);
            return this.createDecision('WATCH', 'C', 0, reasons, data);
        }
        
        // 场景 E: 信号不足
        reasons.push(`⚠️ 信号强度不足`);
        reasons.push(`   聪明钱: ${smartMoney.online}/${this.config.minSmartWalletOnline}`);
        reasons.push(`   TG热度: ${tgHeat.count}/${this.config.tgHotThreshold}`);
        return this.createDecision('IGNORE', 'D', 0, reasons, data);
    }
    
    /**
     * 创建标准决策对象
     */
    createDecision(action, rating, positionSize, reasons, data) {
        // v7.4 提取信号血统 (从原始信号传递或构造)
        const originalLineage = data.debotSignal?.signalLineage || {};
        const signalLineage = {
            source: originalLineage.source || data.debotSignal?.source || 'debot',
            hunterType: originalLineage.hunterType || null,
            hunterAddr: originalLineage.hunterAddr || data.debotSignal?.shadow_wallet || null,
            hunterScore: originalLineage.hunterScore || data.debotSignal?.shadow_score || null,
            route: 'cross_validator',  // CrossValidator 是验证路由
            entryReason: originalLineage.entryReason || this.getEntryReason(rating, data),
            confidence: originalLineage.confidence || 'direct'
        };

        const decision = {
            action,          // BUY_MAX | BUY_NORMAL | BUY_SMALL | WATCH | IGNORE
            rating,          // S | A | B | C | D | null
            positionSize,    // SOL 数量

            token: {
                address: data.tokenAddress,
                chain: data.chain,
                symbol: data.symbol,
                liquidity: data.liquidity,
                marketCap: data.marketCap
            },

            validation: {
                smartMoney: data.smartMoney,
                aiScore: data.aiScore,
                tgHeat: data.tgHeat,
                hasNegative: data.hasNegative
            },

            reasons,
            timestamp: Date.now(),

            // 原始信号引用
            debotSignal: data.debotSignal,

            // v7.4 信号血统追踪
            signalLineage
        };

        // 打印决策
        this.logDecision(decision);

        return decision;
    }

    /**
     * v7.4 根据评级推断入场原因
     */
    getEntryReason(rating, data) {
        switch (rating) {
            case 'S':
                return 's_grade_consensus';  // S级共振
            case 'A':
                return 'dual_validation';    // 双验证共识
            case 'B':
                return 'early_bird';         // 早鸟潜伏
            case 'C':
                return 'watch_pending';      // 观察等待
            default:
                return 'cross_validated';
        }
    }
    
    /**
     * 打印决策日志
     */
    logDecision(decision) {
        const actionEmojis = {
            'BUY_MAX': '🚀',
            'BUY_NORMAL': '✅',
            'BUY_SMALL': '🐦',
            'WATCH': '👀',
            'IGNORE': '⏭️'
        };
        
        const emoji = actionEmojis[decision.action] || '❓';
        
        console.log(`\n${emoji} [CrossValidator] 决策: ${decision.action}`);
        console.log(`   代币: ${decision.token.symbol} (${decision.token.address.slice(0, 8)}...)`);
        console.log(`   评级: ${decision.rating || 'N/A'}`);
        
        if (decision.positionSize > 0) {
            console.log(`   仓位: ${decision.positionSize} SOL`);
        }
        
        console.log(`   理由:`);
        decision.reasons.forEach(r => console.log(`     ${r}`));
    }
    
    /**
     * 缓存决策 (防止重复处理)
     */
    cacheDecision(tokenAddress, decision) {
        this.recentDecisions.set(tokenAddress, {
            decision,
            timestamp: Date.now()
        });
        
        // 清理1小时前的缓存
        const oneHourAgo = Date.now() - 60 * 60 * 1000;
        for (const [key, value] of this.recentDecisions) {
            if (value.timestamp < oneHourAgo) {
                this.recentDecisions.delete(key);
            }
        }
    }
    
    /**
     * 检查是否最近已决策
     */
    hasRecentDecision(tokenAddress) {
        const cached = this.recentDecisions.get(tokenAddress);
        if (!cached) return false;
        
        // 15分钟内的决策视为有效
        return Date.now() - cached.timestamp < 15 * 60 * 1000;
    }
    
    /**
     * 获取缓存的决策
     */
    getCachedDecision(tokenAddress) {
        const cached = this.recentDecisions.get(tokenAddress);
        return cached?.decision || null;
    }
    
    /**
     * 验证 TG 信号 (反向验证)
     * 当 TG 检测到新代币时，检查 DeBot 是否有信号
     */
    async validateTgSignal(tgSignal, debotHotTokens) {
        const tokenAddress = tgSignal.token_ca;
        
        // 在 DeBot 热门代币中查找
        const debotToken = debotHotTokens.find(t => 
            t.tokenAddress === tokenAddress
        );
        
        if (debotToken) {
            // DeBot 也有这个代币 → 交叉验证成功
            console.log(`[CrossValidator] TG信号得到DeBot确认: ${tokenAddress.slice(0, 8)}...`);
            return this.validate(debotToken);
        }
        
        // DeBot 没有 → 观察名单
        return {
            action: 'WATCH',
            rating: null,
            positionSize: 0,
            reasons: ['TG信号待DeBot确认'],
            token: {
                address: tokenAddress,
                chain: tgSignal.chain
            },
            timestamp: Date.now()
        };
    }
    
    /**
     * 获取决策统计
     */
    getStats() {
        const stats = {
            total: 0,
            byAction: {},
            byRating: {}
        };
        
        for (const [_, cached] of this.recentDecisions) {
            stats.total++;
            
            const action = cached.decision.action;
            const rating = cached.decision.rating;
            
            stats.byAction[action] = (stats.byAction[action] || 0) + 1;
            
            if (rating) {
                stats.byRating[rating] = (stats.byRating[rating] || 0) + 1;
            }
        }
        
        return stats;
    }
}

export default CrossValidator;
