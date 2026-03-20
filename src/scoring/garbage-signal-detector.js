/**
 * Garbage Signal Detector v1.0
 *
 * 信号层面垃圾识别器 - 银狗/铜狗比例提升核心模块
 * 目标: 银狗/铜狗比例从 45% → 60%+，WR 和 EV 同步提升
 *
 * 检测 5 大垃圾模式 (每项独立评分，累加后得垃圾总分):
 *
 *   G1. 信号喷发 (Signal Spam)     0-25分 - 短时高密度低质量信号
 *   G2. 聪明钱背离 (SM Divergence) 0-25分 - 信号热但 SM 不跟/已撤离
 *   G3. 渠道降质 (Channel Decay)   0-20分 - 所有信号来自低层矩阵渠道
 *   G4. 时机衰退 (Timing Decay)    0-15分 - 晚入场 + 信号已过峰值
 *   G5. 社交注水 (Social Inflation) 0-15分 - 推特水军/假KOL/低有机度
 *
 * 垃圾分决策 (0-100):
 *   0-39  → CLEAN   → 正常通过，不修改原有逻辑
 *   40-59 → SUSPECT → 降级入铜池，仓位缩减至 0.5x
 *   60+   → GARBAGE → 直接拒绝，不进入观察室
 *
 * 数据来源:
 *   - token 对象 (signalTrendType, smCount, baseScore, marketCap 等)
 *   - socialData 对象 (tg_*, twitter_*, promoted_channels 等)
 *   - dynamicFactors (divergence, signalCount 等，可选)
 */

export class GarbageSignalDetector {
    constructor(config = {}) {
        // 阈值配置，允许外部覆盖
        this.thresholds = {
            // G1: 信号喷发
            spam: {
                signalPerMinHigh: 3.0,       // 每分钟 >3 个信号 = 明显喷发
                signalPerMinMedium: 1.5,     // 每分钟 >1.5 个 = 轻度喷发
                burstRatioHigh: 0.75,        // 75%信号在2min内发出 = 高度同步
                burstRatioMedium: 0.55,      // 55% = 中度同步
                burstMinChannels: 4,         // 至少4个渠道才判断burst
                accelWithoutSM: 2.0,         // 加速系数 > 2 但 SM=0 = 假加速
            },

            // G2: 聪明钱背离
            smDivergence: {
                earlyWarningDivergence: 0.15,  // divergence < 0.15 且信号多 = 早期预警
                earlyWarningSignalCount: 15,   // 信号数阈值
                highSignalLowSM: 20,           // signalCount > 20 且 SM < 2 = 危险
                smDecaying: -1,                // smDelta < -1 = SM正在撤离
            },

            // G3: 渠道降质
            channelDecay: {
                allTierCThreshold: 0.95,       // 95%+ Tier C = 全矩阵
                highTierCThreshold: 0.80,      // 80%+ Tier C = 高度矩阵
                minChannelsForCheck: 4,        // 至少4个渠道才进行tier检查
                extremeMatrixRatio: 0.9,       // 9:1 渠道/簇 比例 = 极端矩阵
                highMatrixRatio: 5.0,          // 5:1 = 高度矩阵
            },

            // G4: 时机衰退
            timingDecay: {
                lateAgeMinutes: 20,            // token > 20分钟 = 晚入场
                veryLateAgeMinutes: 35,        // token > 35分钟 = 非常晚
                decayingTimeLag: 25,           // timeLag > 25min = 严重滞后
                lateWithLowSM: 2,              // 晚入场 + SM < 2 = 危险
                tgVelocityNegative: -0.1,      // tg_velocity < -0.1 = 信号在退潮
            },

            // G5: 社交注水
            socialInflation: {
                highMentionsNoKOL: 25,         // mentions > 25 但 0 KOL = 水军
                mediumMentionsNoKOL: 12,       // mentions > 12 但 0 KOL = 轻度水军
                lowUniqueAuthors: 2,           // unique_authors < 2 但 mentions > 8
                fakeKolRatioHigh: 0.5,         // 假KOL占比 > 50%
                fakeKolRatioMedium: 0.3,       // 假KOL占比 > 30%
                lowOrganicRatio: 0.35,         // 有机推文比例 < 35%
            },

            // 决策阈值
            decision: {
                garbage: 60,    // >= 60 → 拒绝
                suspect: 40,    // 40-59 → 降级
            },

            ...config.garbageDetector
        };
    }

    /**
     * 主入口: 计算垃圾分
     *
     * @param {Object} token      - 代币聚合数据 (signalTrendType, smCount, baseScore, ...)
     * @param {Object} socialData - 社交快照 (tg_*, twitter_*, promoted_channels, ...)
     * @param {Object} dynamicFactors - 动态因子 (divergence, signalCount, ...) 可选
     * @returns {Object} { garbageScore, verdict, breakdown, reasons, positionMultiplier }
     */
    detect(token = {}, socialData = {}, dynamicFactors = {}) {
        const breakdown = {};

        // 计算各维度垃圾分
        const g1 = this._detectSignalSpam(token, socialData);
        const g2 = this._detectSMDivergence(token, socialData, dynamicFactors);
        const g3 = this._detectChannelDecay(socialData);
        const g4 = this._detectTimingDecay(token, socialData);
        const g5 = this._detectSocialInflation(socialData);

        breakdown.signal_spam      = g1;
        breakdown.sm_divergence    = g2;
        breakdown.channel_decay    = g3;
        breakdown.timing_decay     = g4;
        breakdown.social_inflation = g5;

        const garbageScore = Math.min(100,
            g1.score + g2.score + g3.score + g4.score + g5.score
        );

        const { verdict, positionMultiplier } = this._makeVerdict(garbageScore, token);

        const allReasons = [
            ...g1.reasons,
            ...g2.reasons,
            ...g3.reasons,
            ...g4.reasons,
            ...g5.reasons,
        ].filter(r => r);

        console.log(`🗑️  [GarbageDetector] ${token.symbol || token.ca || '?'} ` +
            `垃圾分=${garbageScore} → ${verdict} ` +
            `(G1:${g1.score} G2:${g2.score} G3:${g3.score} G4:${g4.score} G5:${g5.score})`);

        return {
            garbageScore,
            verdict,           // 'CLEAN' | 'SUSPECT' | 'GARBAGE'
            breakdown,
            reasons: allReasons,
            positionMultiplier // 1.0=正常, 0.5=降级, 0.0=拒绝
        };
    }

    // ─────────────────────────────────────────────
    // G1: 信号喷发检测 (0-25分)
    // ─────────────────────────────────────────────
    _detectSignalSpam(token, socialData) {
        let score = 0;
        const reasons = [];
        const t = this.thresholds.spam;

        const signalCount = socialData.N_total || token.signalCount || 0;
        const tokenAgeMins = token.tokenAge || token.ageMinutes || 1;
        const tgAcel = socialData.tg_accel || 0;
        const smCount = token.smCount || 0;
        const tgCh = socialData.tg_ch_15m || 0;
        const tgClusters = socialData.tg_clusters_15m;

        // G1a: 信号密度异常 (每分钟信号数)
        const signalPerMin = signalCount / Math.max(tokenAgeMins, 1);
        if (signalPerMin > t.signalPerMinHigh) {
            score += 15;
            reasons.push(`⚡ 信号喷发: ${signalPerMin.toFixed(1)}条/分钟 (阈值 ${t.signalPerMinHigh})`);
        } else if (signalPerMin > t.signalPerMinMedium) {
            score += 7;
            reasons.push(`⚡ 信号密集: ${signalPerMin.toFixed(1)}条/分钟`);
        }

        // G1b: 加速但无聪明钱跟随 (假加速信号)
        if (tgAcel > t.accelWithoutSM && smCount === 0) {
            score += 10;
            reasons.push(`📢 假加速: tg_accel=${tgAcel.toFixed(1)} 但 SM=0，无聪明钱跟随`);
        } else if (tgAcel > t.accelWithoutSM && smCount < 2) {
            score += 5;
            reasons.push(`📢 弱加速: tg_accel=${tgAcel.toFixed(1)} 但 SM=${smCount}，跟随力弱`);
        }

        // G1c: 极端渠道/簇比例 (高量低多样性)
        if (tgClusters !== null && tgClusters !== undefined && tgCh > 0) {
            const chPerCluster = tgCh / Math.max(tgClusters, 1);
            if (chPerCluster >= t.extremeMatrixRatio * 10 && tgCh >= 6) {
                // 注: 这里直接用数值比较，e.g. 12渠道/1簇 = 12 per cluster
                if (chPerCluster >= 10 && tgCh >= 8) {
                    score += 10;
                    reasons.push(`🤖 极端矩阵: ${tgCh}渠道/${tgClusters}簇 (${chPerCluster.toFixed(1)}渠道/簇)`);
                } else if (chPerCluster >= 6 && tgCh >= 6) {
                    score += 5;
                    reasons.push(`🤖 高度矩阵: ${tgCh}渠道/${tgClusters}簇 (${chPerCluster.toFixed(1)}渠道/簇)`);
                }
            }
        }

        return { score: Math.min(25, score), reasons };
    }

    // ─────────────────────────────────────────────
    // G2: 聪明钱背离检测 (0-25分)
    // ─────────────────────────────────────────────
    _detectSMDivergence(token, socialData, dynamicFactors) {
        let score = 0;
        const reasons = [];
        const t = this.thresholds.smDivergence;

        const smCount = token.smCount || dynamicFactors.smartMoney || 0;
        const signalCount = socialData.N_total || dynamicFactors.signalCount || token.signalCount || 0;
        const divergence = dynamicFactors.divergence ?? (smCount / (signalCount + 1));
        const smDelta = token.smDelta ?? token.trends?.smartMoney ?? null;

        // G2a: 早期预警 - 低背离 + 信号已多
        if (divergence < t.earlyWarningDivergence && signalCount >= t.earlyWarningSignalCount) {
            score += 12;
            reasons.push(`⚠️  SM早期背离: divergence=${divergence.toFixed(3)} 而 signalCount=${signalCount}`);
        }

        // G2b: 高信号低SM危险区 (比TRAP更早触发)
        if (signalCount > t.highSignalLowSM && smCount < 2) {
            score += 10;
            reasons.push(`🚨 高信号低聪明钱: signalCount=${signalCount} 但 SM=${smCount}`);
        }

        // G2c: SM 正在撤离 (smDelta 负值)
        if (smDelta !== null && smDelta <= t.smDecaying) {
            score += 10;
            reasons.push(`📉 聪明钱撤离中: smDelta=${smDelta}`);
        } else if (smDelta !== null && smDelta < 0) {
            score += 5;
            reasons.push(`📉 聪明钱轻微减少: smDelta=${smDelta}`);
        }

        // G2d: 综合强力背离 - signalTrendType=ACCELERATING 但 SM 极少
        if (token.signalTrendType === 'ACCELERATING' && smCount < 2 && signalCount > 10) {
            score += 8;
            reasons.push(`⚡ 信号加速但 SM 不跟: ACCELERATING + SM=${smCount} + 信号${signalCount}`);
        }

        return { score: Math.min(25, score), reasons };
    }

    // ─────────────────────────────────────────────
    // G3: 渠道降质检测 (0-20分)
    // ─────────────────────────────────────────────
    _detectChannelDecay(socialData) {
        let score = 0;
        const reasons = [];
        const t = this.thresholds.channelDecay;

        const channels = typeof socialData.promoted_channels === 'string'
            ? JSON.parse(socialData.promoted_channels)
            : socialData.promoted_channels || [];

        if (channels.length < t.minChannelsForCheck) {
            return { score: 0, reasons: [] };
        }

        const tierA = channels.filter(ch => ch.tier === 'A').length;
        const tierB = channels.filter(ch => ch.tier === 'B').length;
        const tierC = channels.filter(ch => ch.tier === 'C').length;
        const tierCRatio = tierC / channels.length;
        const hasAny = tierA + tierB + tierC;

        // G3a: 几乎全是Tier C (矩阵批发渠道)
        if (tierCRatio >= t.allTierCThreshold && tierA === 0) {
            score += 15;
            reasons.push(`📡 全矩阵渠道: ${tierC}/${channels.length} Tier C (${(tierCRatio * 100).toFixed(0)}%)`);
        } else if (tierCRatio >= t.highTierCThreshold && tierA === 0) {
            score += 8;
            reasons.push(`📡 高比例矩阵: ${tierC}/${channels.length} Tier C (${(tierCRatio * 100).toFixed(0)}%)，无Tier A`);
        }

        // G3b: 渠道/簇比例极端 (额外检测，与G1c互补)
        const tgClusters = socialData.tg_clusters_15m;
        const tgCh = socialData.tg_ch_15m || channels.length;
        if (tgClusters !== null && tgClusters !== undefined && tgClusters <= 1 && tgCh >= 5) {
            score += 10;
            reasons.push(`🎯 单簇多渠道: ${tgCh}渠道全来自${tgClusters}个簇，高度协同`);
        }

        // G3c: 高度Tier C + 无Tier A 且有较多渠道 (次级警告)
        if (tierCRatio >= 0.7 && tierA === 0 && channels.length >= 6) {
            if (score < 8) { // 避免与G3a重复叠加
                score += 5;
                reasons.push(`📡 渠道质量偏低: TierA=0, TierC=${tierCRatio.toFixed(2) * 100}%`);
            }
        }

        return { score: Math.min(20, score), reasons };
    }

    // ─────────────────────────────────────────────
    // G4: 时机衰退检测 (0-15分)
    // ─────────────────────────────────────────────
    _detectTimingDecay(token, socialData) {
        let score = 0;
        const reasons = [];
        const t = this.thresholds.timingDecay;

        const tokenAge = token.tokenAge || token.ageMinutes || 0;
        const signalTrend = token.signalTrendType || 'STABLE';
        const smCount = token.smCount || 0;
        const timeLag = socialData.tg_time_lag ?? null;
        const tgVelocity = socialData.tg_velocity ?? null;

        // G4a: 非常晚入场
        if (tokenAge > t.veryLateAgeMinutes) {
            score += 8;
            reasons.push(`⏰ 严重滞后入场: token年龄 ${tokenAge}分钟`);
        } else if (tokenAge > t.lateAgeMinutes) {
            score += 4;
            reasons.push(`⏰ 晚入场: token年龄 ${tokenAge}分钟`);
        }

        // G4b: 晚入场 + 信号非加速 + SM弱
        if (tokenAge > t.lateAgeMinutes && signalTrend !== 'ACCELERATING' && smCount < t.lateWithLowSM) {
            score += 6;
            reasons.push(`⏰ 晚入场叠加弱信号: ${tokenAge}min + ${signalTrend} + SM=${smCount}`);
        }

        // G4c: 信号衰退中 (DECAYING - 作为额外护城河)
        if (signalTrend === 'DECAYING') {
            score += 8;
            reasons.push(`📉 信号衰退: signalTrendType=DECAYING，信号热度消退`);
        }

        // G4d: 时间滞后严重
        if (timeLag !== null && timeLag > t.decayingTimeLag) {
            score += 5;
            reasons.push(`⌛ 发现时机严重滞后: 首次信号${timeLag}分钟前已出现`);
        }

        // G4e: TG信号速度为负 (信号在退潮)
        if (tgVelocity !== null && tgVelocity < t.tgVelocityNegative) {
            score += 4;
            reasons.push(`📉 TG信号退潮: tg_velocity=${tgVelocity.toFixed(2)}`);
        }

        return { score: Math.min(15, score), reasons };
    }

    // ─────────────────────────────────────────────
    // G5: 社交注水检测 (0-15分)
    // ─────────────────────────────────────────────
    _detectSocialInflation(socialData) {
        let score = 0;
        const reasons = [];
        const t = this.thresholds.socialInflation;

        const mentions = socialData.twitter_mentions || 0;
        const uniqueAuthors = socialData.twitter_unique_authors || 0;
        const kolCount = socialData.twitter_kol_count || 0;
        const xUniqueAuthors15m = socialData.x_unique_authors_15m || 0;
        // 从 Grok 原始数据中提取 (如果有)
        const organicRatio = socialData.twitter_organic_ratio ?? null;
        const fakeKolCount = socialData.twitter_fake_kol_count ?? 0;
        const realKolCount = socialData.twitter_real_kol_count ?? kolCount;

        // G5a: 高提及但零真实KOL (水军炒作)
        if (mentions >= t.highMentionsNoKOL && realKolCount === 0) {
            score += 10;
            reasons.push(`🤖 高推特量零KOL: ${mentions}条提及 但 real_KOL=0，疑似水军`);
        } else if (mentions >= t.mediumMentionsNoKOL && realKolCount === 0) {
            score += 5;
            reasons.push(`🤖 中等推特量零KOL: ${mentions}条提及 但 KOL=0`);
        }

        // G5b: 极低唯一作者 (少数账号反复发)
        if (mentions > 8 && uniqueAuthors < t.lowUniqueAuthors) {
            score += 7;
            reasons.push(`🔄 低唯一作者: ${mentions}条提及仅来自${uniqueAuthors}个账号，重复发布`);
        } else if (xUniqueAuthors15m < t.lowUniqueAuthors && mentions > 5) {
            score += 3;
            reasons.push(`🔄 X唯一作者偏少: x_unique_authors_15m=${xUniqueAuthors15m}`);
        }

        // G5c: 高假KOL比例
        if (fakeKolCount > 0 && realKolCount > 0) {
            const fakeRatio = fakeKolCount / (fakeKolCount + realKolCount);
            if (fakeRatio >= t.fakeKolRatioHigh) {
                score += 8;
                reasons.push(`🎭 高假KOL: ${(fakeRatio * 100).toFixed(0)}% KOL为假账号`);
            } else if (fakeRatio >= t.fakeKolRatioMedium) {
                score += 4;
                reasons.push(`🎭 中度假KOL: ${(fakeRatio * 100).toFixed(0)}% KOL可疑`);
            }
        }

        // G5d: 低有机推文比例
        if (organicRatio !== null && organicRatio < t.lowOrganicRatio) {
            score += 6;
            reasons.push(`🤖 低有机推文: organic_ratio=${(organicRatio * 100).toFixed(0)}% (阈值 ${t.lowOrganicRatio * 100}%)`);
        }

        return { score: Math.min(15, score), reasons };
    }

    // ─────────────────────────────────────────────
    // 最终决策
    // ─────────────────────────────────────────────
    _makeVerdict(garbageScore, token) {
        const t = this.thresholds.decision;

        // 特殊豁免: GOLDEN tag 对轻度垃圾有一定免疫力
        const isGolden = token.dynamicTag === 'GOLDEN';
        const immunityBonus = isGolden ? 10 : 0;
        const effectiveScore = garbageScore - immunityBonus;

        if (effectiveScore >= t.garbage) {
            return { verdict: 'GARBAGE', positionMultiplier: 0.0 };
        } else if (effectiveScore >= t.suspect) {
            return { verdict: 'SUSPECT', positionMultiplier: 0.5 };
        } else {
            return { verdict: 'CLEAN', positionMultiplier: 1.0 };
        }
    }

    /**
     * 快速判断：只返回是否需要拒绝
     * 用于在 applyFilter 之前的极速预筛
     *
     * @param {Object} token
     * @param {Object} socialData
     * @returns {boolean} true = 应该拒绝
     */
    shouldReject(token, socialData) {
        const result = this.detect(token, socialData);
        return result.verdict === 'GARBAGE';
    }

    /**
     * 获取仓位乘数 (用于下游仓位计算)
     *
     * @param {Object} token
     * @param {Object} socialData
     * @param {Object} dynamicFactors
     * @returns {number} 0.0 | 0.5 | 1.0
     */
    getPositionMultiplier(token, socialData, dynamicFactors = {}) {
        const result = this.detect(token, socialData, dynamicFactors);
        return result.positionMultiplier;
    }
}

// 单例模式，供全局使用
export default new GarbageSignalDetector();
