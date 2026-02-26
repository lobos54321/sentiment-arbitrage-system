/**
 * Dynamic Calculator v1.1
 * 
 * 计算动态特征因子，识别金狗/银狗/土狗
 * 使用 strategy.js 配置
 */

import { SCORING } from '../config/strategy.js';

class DynamicCalculator {
    constructor() {
        // 从配置文件加载阈值
        this.thresholds = SCORING.DYNAMIC_FACTORS;
    }

    /**
     * 计算动态特征因子
     * @param {Object} data 聚合数据
     * @returns {Object} { divergence, healthRatio, tag, reason, scoreBonus }
     */
    calculateFactors(data) {
        const {
            smartMoney = 0,
            signalCount = 0,
            liquidity = 0,
            marketCap = 0,
            avgBuyAmount = 0
        } = data;

        // 1. 舆论-资金背离度 (Hype-Money Divergence)
        const divergence = smartMoney / (signalCount + 1);

        // 2. 健康度 (Health Ratio)
        const mc = marketCap || (liquidity * 5); // 如果没有市值，用流动性估算
        const healthRatio = mc > 0 ? liquidity / mc : 0;

        // 3. 确信度 (Conviction) - 聪明钱平均买入量
        const conviction = avgBuyAmount > 1 ? 'HIGH' : avgBuyAmount > 0.5 ? 'MID' : 'LOW';

        // --- 特征识别逻辑 (使用配置阈值) ---
        let tag = "NORMAL";
        let reason = "数据平庸，观望";
        let scoreBonus = 0;

        const { GOLDEN, SILVER, TRAP } = this.thresholds;

        // 🥇 金狗特征 (Golden): 钱多 + 话少 + 池子厚
        if (divergence > GOLDEN.MIN_DIVERGENCE &&
            healthRatio > GOLDEN.MIN_HEALTH_RATIO &&
            smartMoney >= GOLDEN.MIN_SMART_MONEY) {
            tag = "GOLDEN";
            reason = "🥇 潜伏金狗: 资金先行+舆论未动+池厚";
            scoreBonus = 20;
        }
        // 🥈 银狗特征 (Silver): 钱多 + 话多 + 共振
        else if (smartMoney >= SILVER.MIN_SMART_MONEY &&
            signalCount >= SILVER.MIN_SIGNAL_COUNT &&
            divergence > SILVER.MIN_DIVERGENCE) {
            tag = "SILVER";
            reason = "🥈 主升浪: 资金舆论共振";
            scoreBonus = 10;
        }
        // ☠️ 土狗特征 (Trap): 钱少 + 话多 (严重背离)
        else if (divergence < TRAP.MAX_DIVERGENCE &&
            signalCount > TRAP.MIN_SIGNAL_COUNT) {
            tag = "TRAP";
            reason = "☠️ 陷阱: 严重背离(话多钱少)，疑似出货";
            scoreBonus = -100;
        }
        // 正常情况
        else if (smartMoney >= 2 && divergence > 0.2) {
            tag = "NORMAL";
            reason = "📊 普通信号: 需AI验证";
            scoreBonus = 0;
        }
        // 低质量
        else {
            tag = "WEAK";
            reason = "⚠️ 信号弱: 聪明钱或背离度不足";
            scoreBonus = -20;
        }

        return {
            divergence: parseFloat(divergence.toFixed(3)),
            healthRatio: parseFloat(healthRatio.toFixed(3)),
            conviction,
            tag,
            reason,
            scoreBonus,
            raw: { smartMoney, signalCount, liquidity, marketCap, avgBuyAmount }
        };
    }

    /**
     * 分析 K 线健康度
     * @param {Array} klines K 线数组
     * @returns {Object} { health, summary, signals }
     */
    analyzeKlineHealth(klines) {
        if (!klines || klines.length < 3) {
            return { health: 'UNKNOWN', summary: '数据不足', signals: [] };
        }

        const signals = [];
        let bullishCount = 0;
        let bearishCount = 0;
        let hasLongUpperShadow = false;
        let hasPinBar = false;
        let hasBigDrop = false;

        for (let i = 0; i < klines.length; i++) {
            const k = klines[i];
            const open = k.open || k.o || 0;
            const high = k.high || k.h || 0;
            const low = k.low || k.l || 0;
            const close = k.close || k.c || 0;

            if (open === 0 || close === 0) continue;

            const body = Math.abs(close - open);
            const range = high - low;
            const change = (close - open) / open;

            // 阳线 vs 阴线
            if (close > open) bullishCount++;
            else bearishCount++;

            // 长上影线（抛压信号）
            if (range > 0 && (high - Math.max(open, close)) / range > 0.5) {
                hasLongUpperShadow = true;
                signals.push('长上影线(抛压)');
            }

            // 插针（大阴线后快速拉回）
            if (range > 0 && body / range < 0.2 && (low < open * 0.9 || low < close * 0.9)) {
                hasPinBar = true;
                signals.push('插针K线');
            }

            // 大阴线（单根跌幅 > 10%）
            if (change < -0.1) {
                hasBigDrop = true;
                signals.push(`大阴线(${(change * 100).toFixed(1)}%)`);
            }
        }

        // 综合判断
        let health = 'NEUTRAL';
        let summary = '';

        if (hasBigDrop || (bearishCount > bullishCount * 2)) {
            health = 'BEARISH';
            summary = `⚠️ 走势偏弱: ${bullishCount}阳/${bearishCount}阴`;
        } else if (hasLongUpperShadow && bearishCount >= bullishCount) {
            health = 'BEARISH';
            summary = '⚠️ 有抛压信号';
        } else if (bullishCount > bearishCount && !hasLongUpperShadow) {
            health = 'BULLISH';
            summary = `✅ 走势健康: ${bullishCount}阳/${bearishCount}阴`;
        } else {
            health = 'NEUTRAL';
            summary = `📊 走势中性: ${bullishCount}阳/${bearishCount}阴`;
        }

        return {
            health,
            summary,
            signals: [...new Set(signals)], // 去重
            stats: { bullishCount, bearishCount, hasLongUpperShadow, hasPinBar, hasBigDrop }
        };
    }
}

export default new DynamicCalculator();
