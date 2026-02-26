/**
 * Dead Dog Pool v1.0 - 失败学习库 (P0 核心模块)
 *
 * 系统的"免疫系统" - 从失败中学习，自动进化
 *
 * 核心逻辑:
 * 1. 每次交易失败（止损/超时），提取特征模式
 * 2. 存入内存缓存 + 持久化到文件
 * 3. 下次开单前查库，命中则拒绝
 *
 * 特征类型:
 * - keywords: 名字关键词 (如 ELON, SAFE, INU)
 * - devs: 开发者地址
 * - narratives: 叙事类型 (如 AI, CAT, POLITI)
 * - timeSlots: 失败高发时段
 * - mcapRanges: 市值区间
 *
 * 衰减机制:
 * - 24小时自动衰减
 * - 连续失败 3 次 → 72小时临时黑名单
 */

import fs from 'fs';
import path from 'path';

class DeadDogPool {
    constructor(config = {}) {
        // 配置
        this.config = {
            // 衰减周期: 24小时
            DECAY_MS: config.decayMs || 24 * 60 * 60 * 1000,
            // 阈值: 连续死 3 次就拉黑
            BAN_THRESHOLD: config.banThreshold || 3,
            // 黑名单持续时间: 72小时
            BLACKLIST_DURATION_MS: config.blacklistDurationMs || 72 * 60 * 60 * 1000,
            // 数据文件路径
            DATA_FILE: config.dataFile || path.join(process.cwd(), 'data', 'dead-dog-pool.json')
        };

        // 模式缓存
        this.patterns = {
            keywords: new Map(),      // keyword -> { failCount, lastFail, totalLoss }
            devs: new Map(),          // devAddress -> { failCount, lastFail, totalLoss }
            narratives: new Map(),    // narrative -> { failCount, lastFail, totalLoss }
            timeSlots: new Map(),     // hourSlot (0-23) -> { failCount, lastFail }
            mcapRanges: new Map()     // mcapRange -> { failCount, lastFail, avgLoss }
        };

        // 临时黑名单 (精确匹配)
        this.blacklist = new Map();   // tokenCA -> { reason, until }

        // 统计
        this.stats = {
            totalDeaths: 0,
            blockedCount: 0,
            lastDeathTime: null
        };

        // 加载持久化数据
        this.loadFromFile();

        console.log('💀 [DeadDogPool] 失败学习库初始化完成');
        console.log(`   📊 已记录 ${this.stats.totalDeaths} 次失败`);
        console.log(`   🚫 已拦截 ${this.stats.blockedCount} 次`);
    }

    /**
     * 📝 记录一次死亡
     * @param {Object} token - 代币信息 { symbol, name, tokenCA, creator, narrative, marketCap, chain }
     * @param {string} reason - 死亡原因 (STOP_LOSS, TIME_STOP, RUG, EMERGENCY_EXIT)
     * @param {number} pnlPercent - 盈亏百分比 (负数)
     */
    async recordDeath(token, reason, pnlPercent = 0) {
        const now = Date.now();
        this.stats.totalDeaths++;
        this.stats.lastDeathTime = now;

        console.log(`\n💀 [DeadDogPool] 记录死亡: ${token.symbol}`);
        console.log(`   原因: ${reason} | 亏损: ${pnlPercent.toFixed(1)}%`);

        // 1. 提取关键词
        const keywords = this.extractKeywords(token.symbol, token.name);
        for (const word of keywords) {
            this.updatePattern('keywords', word, pnlPercent);
        }

        // 2. 记录开发者 (如果有)
        if (token.creator) {
            this.updatePattern('devs', token.creator, pnlPercent);
        }

        // 3. 记录叙事类型
        if (token.narrative) {
            this.updatePattern('narratives', token.narrative.toUpperCase(), pnlPercent);
        }

        // 4. 记录时段
        const hour = new Date(now).getHours();
        this.updatePattern('timeSlots', hour.toString(), pnlPercent);

        // 5. 记录市值区间
        if (token.marketCap > 0) {
            const mcapRange = this.getMcapRange(token.marketCap);
            this.updatePattern('mcapRanges', mcapRange, pnlPercent);
        }

        // 6. 持久化
        await this.saveToFile();

        console.log(`   📊 关键词: [${keywords.join(', ')}]`);
        console.log(`   🕐 时段: ${hour}:00`);
    }

    /**
     * 🛡️ 检查是否命中黑名单
     * @param {Object} token - 代币信息
     * @returns {{ blocked: boolean, reason?: string }}
     */
    isBlacklisted(token) {
        const now = Date.now();

        // 1. 检查精确黑名单
        if (token.tokenCA && this.blacklist.has(token.tokenCA)) {
            const entry = this.blacklist.get(token.tokenCA);
            if (now < entry.until) {
                this.stats.blockedCount++;
                return { blocked: true, reason: `精确黑名单: ${entry.reason}` };
            } else {
                this.blacklist.delete(token.tokenCA);
            }
        }

        // 2. 检查关键词模式
        const keywords = this.extractKeywords(token.symbol, token.name);
        for (const word of keywords) {
            const record = this.patterns.keywords.get(word);
            if (record && this.isPatternActive(record)) {
                if (record.failCount >= this.config.BAN_THRESHOLD) {
                    this.stats.blockedCount++;
                    return {
                        blocked: true,
                        reason: `🚫关键词[${word}]近期死亡${record.failCount}次`
                    };
                }
            }
        }

        // 3. 检查开发者
        if (token.creator) {
            const record = this.patterns.devs.get(token.creator);
            if (record && this.isPatternActive(record)) {
                if (record.failCount >= 2) {  // Dev 阈值更低
                    this.stats.blockedCount++;
                    return {
                        blocked: true,
                        reason: `🚫Dev[${token.creator.slice(0, 8)}...]死亡${record.failCount}次`
                    };
                }
            }
        }

        // 4. 检查叙事类型
        if (token.narrative) {
            const record = this.patterns.narratives.get(token.narrative.toUpperCase());
            if (record && this.isPatternActive(record)) {
                if (record.failCount >= 5) {  // 叙事阈值更高
                    // 不完全拦截，只是警告
                    console.log(`   ⚠️ [DeadDogPool] 叙事[${token.narrative}]近期死亡${record.failCount}次，谨慎`);
                }
            }
        }

        return { blocked: false };
    }

    /**
     * 更新模式记录
     */
    updatePattern(type, key, pnlPercent = 0) {
        const patterns = this.patterns[type];
        if (!patterns) return;

        const now = Date.now();
        if (!patterns.has(key)) {
            patterns.set(key, {
                failCount: 1,
                lastFail: now,
                totalLoss: Math.abs(pnlPercent),
                firstSeen: now
            });
        } else {
            const record = patterns.get(key);
            record.failCount++;
            record.lastFail = now;
            record.totalLoss += Math.abs(pnlPercent);
        }
    }

    /**
     * 检查模式是否还在活跃期
     */
    isPatternActive(record) {
        return Date.now() - record.lastFail < this.config.DECAY_MS;
    }

    /**
     * 提取关键词
     * 从 symbol 和 name 中提取有意义的词
     */
    extractKeywords(symbol = '', name = '') {
        const keywords = new Set();

        // 常见的无意义词 (停用词)
        const stopWords = new Set([
            'THE', 'A', 'AN', 'OF', 'IN', 'ON', 'FOR', 'TO', 'AND', 'OR',
            'TOKEN', 'COIN', 'MEME', 'SOL', 'BSC', 'ETH'
        ]);

        // 高风险关键词 (重点关注)
        const riskWords = new Set([
            'SAFE', 'MOON', 'ELON', 'PEPE', 'DOGE', 'SHIB', 'INU', 'BABY',
            'FLOKI', 'BONK', '100X', '1000X', 'GEM', 'APE', 'PUMP',
            'TRUMP', 'BIDEN', 'AI', 'GPT', 'AGI', 'CAT', 'DOG', 'FROG',
            'FRAUD', 'TEST', 'AIRDROP'
        ]);

        // 处理 symbol
        const symbolUpper = (symbol || '').toUpperCase();
        if (symbolUpper.length >= 2 && symbolUpper.length <= 10) {
            keywords.add(symbolUpper);
        }

        // 从 name 中提取词
        const nameWords = (name || '').toUpperCase()
            .replace(/[^A-Z0-9\s]/g, ' ')
            .split(/\s+/)
            .filter(w => w.length >= 2 && w.length <= 15);

        for (const word of nameWords) {
            if (!stopWords.has(word)) {
                keywords.add(word);
            }
            // 高风险词单独标记
            if (riskWords.has(word)) {
                keywords.add(`RISK:${word}`);
            }
        }

        return Array.from(keywords);
    }

    /**
     * 获取市值区间标签
     */
    getMcapRange(marketCap) {
        if (marketCap < 20000) return 'MICRO_<20K';
        if (marketCap < 50000) return 'TINY_20K-50K';
        if (marketCap < 100000) return 'SMALL_50K-100K';
        if (marketCap < 500000) return 'MID_100K-500K';
        if (marketCap < 1000000) return 'LARGE_500K-1M';
        return 'MEGA_>1M';
    }

    /**
     * 添加到精确黑名单
     */
    addToBlacklist(tokenCA, reason, durationMs = null) {
        this.blacklist.set(tokenCA, {
            reason,
            until: Date.now() + (durationMs || this.config.BLACKLIST_DURATION_MS)
        });
        console.log(`   🚫 [DeadDogPool] ${tokenCA.slice(0, 8)}... 加入黑名单: ${reason}`);
    }

    /**
     * 清理过期记录
     */
    cleanup() {
        const now = Date.now();
        let cleaned = 0;

        // 清理各类模式
        for (const [type, patterns] of Object.entries(this.patterns)) {
            for (const [key, record] of patterns) {
                if (!this.isPatternActive(record)) {
                    patterns.delete(key);
                    cleaned++;
                }
            }
        }

        // 清理黑名单
        for (const [tokenCA, entry] of this.blacklist) {
            if (now >= entry.until) {
                this.blacklist.delete(tokenCA);
                cleaned++;
            }
        }

        if (cleaned > 0) {
            console.log(`   🧹 [DeadDogPool] 清理了 ${cleaned} 条过期记录`);
        }
    }

    /**
     * 获取统计信息
     */
    getStats() {
        return {
            ...this.stats,
            keywordCount: this.patterns.keywords.size,
            devCount: this.patterns.devs.size,
            narrativeCount: this.patterns.narratives.size,
            blacklistCount: this.blacklist.size,
            topFailKeywords: this.getTopPatterns('keywords', 5),
            topFailDevs: this.getTopPatterns('devs', 3),
            topFailNarratives: this.getTopPatterns('narratives', 5)
        };
    }

    /**
     * 获取失败最多的模式
     */
    getTopPatterns(type, limit = 5) {
        const patterns = this.patterns[type];
        if (!patterns) return [];

        return Array.from(patterns.entries())
            .filter(([_, record]) => this.isPatternActive(record))
            .sort((a, b) => b[1].failCount - a[1].failCount)
            .slice(0, limit)
            .map(([key, record]) => ({
                pattern: key,
                failCount: record.failCount,
                totalLoss: record.totalLoss.toFixed(1)
            }));
    }

    /**
     * 持久化到文件
     */
    async saveToFile() {
        try {
            const data = {
                stats: this.stats,
                patterns: {
                    keywords: Array.from(this.patterns.keywords.entries()),
                    devs: Array.from(this.patterns.devs.entries()),
                    narratives: Array.from(this.patterns.narratives.entries()),
                    timeSlots: Array.from(this.patterns.timeSlots.entries()),
                    mcapRanges: Array.from(this.patterns.mcapRanges.entries())
                },
                blacklist: Array.from(this.blacklist.entries()),
                savedAt: new Date().toISOString()
            };

            // 确保目录存在
            const dir = path.dirname(this.config.DATA_FILE);
            if (!fs.existsSync(dir)) {
                fs.mkdirSync(dir, { recursive: true });
            }

            fs.writeFileSync(this.config.DATA_FILE, JSON.stringify(data, null, 2));
        } catch (error) {
            console.error('❌ [DeadDogPool] 保存失败:', error.message);
        }
    }

    /**
     * 从文件加载
     */
    loadFromFile() {
        try {
            if (!fs.existsSync(this.config.DATA_FILE)) {
                console.log('   📁 [DeadDogPool] 数据文件不存在，使用空白状态');
                return;
            }

            const data = JSON.parse(fs.readFileSync(this.config.DATA_FILE, 'utf8'));

            // 恢复统计
            this.stats = data.stats || this.stats;

            // 恢复模式
            if (data.patterns) {
                for (const [type, entries] of Object.entries(data.patterns)) {
                    if (this.patterns[type] && Array.isArray(entries)) {
                        this.patterns[type] = new Map(entries);
                    }
                }
            }

            // 恢复黑名单
            if (data.blacklist) {
                this.blacklist = new Map(data.blacklist);
            }

            // 清理过期记录
            this.cleanup();

            console.log('   ✅ [DeadDogPool] 数据加载成功');
        } catch (error) {
            console.error('❌ [DeadDogPool] 加载失败:', error.message);
        }
    }
}

// 单例导出
const deadDogPool = new DeadDogPool();
export { DeadDogPool };
export default deadDogPool;
