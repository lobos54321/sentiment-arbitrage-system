/**
 * Kline Validator - K线健康度检查器
 * 
 * 核心逻辑：
 * 1. 检查最近 N 根 K 线的总体涨跌幅
 * 2. 识别 "断头铡" (单根巨大阴线)
 * 3. 识别 "阴跌" (连续阴线)
 */

export class KlineValidator {
    constructor() {
        this.params = {
            CHECK_WINDOW: 5,         // 检查最近 5 根
            MAX_DROP_PERCENT: -15,   // 总体最大跌幅 -15%
            SINGLE_CANDLE_DROP: -12, // 单根最大跌幅 -12% (防暴力砸盘)
            CONSECUTIVE_RED: 4,      // 连续 4 根阴线 (阴跌)
        };
    }

    /**
     * 检查 K 线健康度
     * @param {Array} klineData - K 线数组 [{time, open, high, low, close, volume}]
     * @returns {Object} { ok: boolean, reason: string, pnl: number }
     */
    checkHealth(klineData) {
        // 1. 数据不足，默认通过 (但在 Waiting Room 里会继续等)
        if (!klineData || !Array.isArray(klineData) || klineData.length < 3) {
            return { ok: true, reason: '数据不足', pnl: 0 };
        }

        // 取最近 N 根
        const recent = klineData.slice(-this.params.CHECK_WINDOW);
        if (recent.length === 0) return { ok: true, reason: '无数据', pnl: 0 };

        // 2. 检查总体涨跌幅
        const startPrice = recent[0].open;
        const endPrice = recent[recent.length - 1].close;
        const totalChange = ((endPrice - startPrice) / startPrice) * 100;

        if (totalChange < this.params.MAX_DROP_PERCENT) {
            return {
                ok: false,
                reason: `总体跌幅过大 (${totalChange.toFixed(1)}%)`,
                pnl: totalChange
            };
        }

        // 3. 检查单根 K 线
        let consecutiveRed = 0;

        for (const candle of recent) {
            const change = ((candle.close - candle.open) / candle.open) * 100;

            // 检查暴跌针
            if (change < this.params.SINGLE_CANDLE_DROP) {
                return {
                    ok: false,
                    reason: `发现砸盘针 (${change.toFixed(1)}%)`,
                    pnl: totalChange
                };
            }

            // 统计连阴
            if (change < -1) { // 跌幅超过 1% 算阴线
                consecutiveRed++;
            } else {
                consecutiveRed = 0;
            }

            if (consecutiveRed >= this.params.CONSECUTIVE_RED) {
                return {
                    ok: false,
                    reason: `连续阴跌 (${consecutiveRed}根)`,
                    pnl: totalChange
                };
            }
        }

        return {
            ok: true,
            reason: '走势健康',
            pnl: totalChange
        };
    }
}
