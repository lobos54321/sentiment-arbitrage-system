#!/usr/bin/env python3
"""
v19 ASYMMETRIC 策略回测（使用真实 1 分钟 K 线数据）

回测场景：
1. ATH#1 (PASS, MC 30-300K) — 当前策略
2. ATH#2 增量 (V17_NOT_ATH1, MC 30-300K) — 排除 ATH#1 已有
3. ATH#2 低MC (V17_NOT_ATH1, MC < 50K) — 你想测试的场景
4. ATH#1 低MC (PASS, MC < 50K) — 对照组
"""
import sqlite3
import json
import sys
import os
from collections import defaultdict
from datetime import datetime, timezone

SIGNAL_DB = '/tmp/sentiment.db'
KLINE_DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'kline_cache.db')

def simulate_v19_asymmetric(klines, entry_price, entry_ts):
    """
    v19 ASYMMETRIC 退出策略，用真实 1 分钟 K 线

    klines: [(timestamp, open, high, low, close, volume), ...]
    entry_price: 入场价格
    entry_ts: 入场时间戳（秒）

    返回: {pnl, exit_reason, exit_time_min, high_pnl, tp1_hit, tp2_hit, tp3_hit, tp4_hit}
    """
    if not klines or entry_price <= 0:
        return {'pnl': -100, 'exit_reason': 'NO_DATA', 'exit_time_min': 0,
                'high_pnl': 0, 'tp1_hit': False, 'tp2_hit': False, 'tp3_hit': False, 'tp4_hit': False}

    tp1 = tp2 = tp3 = tp4 = moonbag = False
    moonbag_high_pnl = 0
    sold_pct = 0
    realized_value = 0  # 以 entry_price=1.0 为基准
    high_pnl = 0

    for ts, o, h, l, c, vol in klines:
        t_min = (ts - entry_ts) / 60.0
        if t_min < 0:
            continue

        # 用 high 和 low 来模拟逐笔触发
        # 先用 high 检查 TP，再用 low 检查 SL（保守估计）

        price_high = h
        price_low = l
        price_close = c

        pnl_high = (price_high / entry_price - 1) * 100
        pnl_low = (price_low / entry_price - 1) * 100
        pnl_close = (price_close / entry_price - 1) * 100

        # 更新高水位（用 high）
        if pnl_high > high_pnl:
            high_pnl = pnl_high

        remaining = 100 - sold_pct
        if remaining <= 0:
            break

        # ========== TP 检查（用 high 价）==========

        # TP1: ≥+50% → 卖60%
        if not tp1 and pnl_high >= 50:
            sell = min(60, remaining)
            # 保守：用 50% PnL 对应的价格而非 high
            tp1_price = entry_price * 1.50
            realized_value += sell / 100 * tp1_price
            sold_pct += sell
            tp1 = True
            remaining = 100 - sold_pct

        # TP2: +100% → 卖15%
        if tp1 and not tp2 and pnl_high >= 100:
            sell = min(15, remaining)
            tp2_price = entry_price * 2.00
            realized_value += sell / 100 * tp2_price
            sold_pct += sell
            tp2 = True
            remaining = 100 - sold_pct

        # TP3: +200% → 卖15%
        if tp2 and not tp3 and pnl_high >= 200:
            sell = min(15, remaining)
            tp3_price = entry_price * 3.00
            realized_value += sell / 100 * tp3_price
            sold_pct += sell
            tp3 = True
            remaining = 100 - sold_pct

        # TP4: +500% → 卖5%，剩余进 Moonbag
        if tp3 and not tp4 and pnl_high >= 500:
            sell = min(5, remaining)
            tp4_price = entry_price * 6.00
            realized_value += sell / 100 * tp4_price
            sold_pct += sell
            tp4 = True
            moonbag = True
            moonbag_high_pnl = pnl_high
            remaining = 100 - sold_pct

        # ========== Moonbag 回撤检查 ==========
        if moonbag:
            if pnl_high > moonbag_high_pnl:
                moonbag_high_pnl = pnl_high
            # 从最高点回撤 35% 则平仓
            if moonbag_high_pnl > 0:
                # 回撤比例 = (最高PnL - 当前PnL) / (1 + 最高PnL/100) / entry * 100
                # 简化: 价格从峰值跌了多少
                peak_price = entry_price * (1 + moonbag_high_pnl / 100)
                drop_pct = (peak_price - price_low) / peak_price * 100
                if drop_pct >= 35:
                    remaining = 100 - sold_pct
                    exit_price = peak_price * 0.65  # 峰值 * (1-0.35)
                    realized_value += remaining / 100 * exit_price
                    sold_pct = 100
                    total_pnl = (realized_value / entry_price - 1) * 100
                    return {
                        'pnl': total_pnl, 'exit_reason': f'MOONBAG_EXIT(peak+{moonbag_high_pnl:.0f}%)',
                        'exit_time_min': t_min, 'high_pnl': high_pnl,
                        'tp1_hit': tp1, 'tp2_hit': tp2, 'tp3_hit': tp3, 'tp4_hit': tp4
                    }
            continue  # Moonbag 不受时间限制

        # ========== SL 检查（用 low 价）==========
        if not moonbag:
            current_sl = 0 if tp1 else -35
            if pnl_low <= current_sl:
                remaining = 100 - sold_pct
                sl_price = entry_price * (1 + current_sl / 100)
                realized_value += remaining / 100 * sl_price
                sold_pct = 100
                reason = f'BREAKEVEN_SL(PnL≤0%)' if tp1 else f'HARD_SL_35(PnL{pnl_low:.0f}%)'
                total_pnl = (realized_value / entry_price - 1) * 100
                return {
                    'pnl': total_pnl, 'exit_reason': reason,
                    'exit_time_min': t_min, 'high_pnl': high_pnl,
                    'tp1_hit': tp1, 'tp2_hit': tp2, 'tp3_hit': tp3, 'tp4_hit': tp4
                }

        # ========== 时间条件（用 close 价）==========

        # 15分钟死水: 未碰TP1 && peak<50% && -15%~+15%
        if not tp1 and t_min >= 15 and high_pnl < 50 and -15 <= pnl_close <= 15:
            remaining = 100 - sold_pct
            realized_value += remaining / 100 * price_close
            sold_pct = 100
            total_pnl = (realized_value / entry_price - 1) * 100
            return {
                'pnl': total_pnl, 'exit_reason': f'DEAD_WATER_15M(PnL{pnl_close:.0f}%)',
                'exit_time_min': t_min, 'high_pnl': high_pnl,
                'tp1_hit': tp1, 'tp2_hit': tp2, 'tp3_hit': tp3, 'tp4_hit': tp4
            }

        # 30分钟大限: 未碰TP1
        if not tp1 and t_min >= 30:
            remaining = 100 - sold_pct
            realized_value += remaining / 100 * price_close
            sold_pct = 100
            total_pnl = (realized_value / entry_price - 1) * 100
            return {
                'pnl': total_pnl, 'exit_reason': f'TIMEOUT_30M(PnL{pnl_close:.0f}%)',
                'exit_time_min': t_min, 'high_pnl': high_pnl,
                'tp1_hit': tp1, 'tp2_hit': tp2, 'tp3_hit': tp3, 'tp4_hit': tp4
            }

        # TP1+超时 2小时
        if tp1 and not moonbag and t_min >= 120:
            remaining = 100 - sold_pct
            realized_value += remaining / 100 * price_close
            sold_pct = 100
            total_pnl = (realized_value / entry_price - 1) * 100
            return {
                'pnl': total_pnl, 'exit_reason': f'TP1_PLUS_TIMEOUT_2H(PnL{pnl_close:.0f}%)',
                'exit_time_min': t_min, 'high_pnl': high_pnl,
                'tp1_hit': tp1, 'tp2_hit': tp2, 'tp3_hit': tp3, 'tp4_hit': tp4
            }

    # 轨迹遍历完还没退出
    remaining = 100 - sold_pct
    if remaining > 0 and klines:
        last_close = klines[-1][4]
        realized_value += remaining / 100 * last_close

    total_pnl = (realized_value / entry_price - 1) * 100 if entry_price > 0 else -100
    return {
        'pnl': total_pnl, 'exit_reason': 'KLINE_END',
        'exit_time_min': (klines[-1][0] - entry_ts) / 60 if klines else 0,
        'high_pnl': high_pnl,
        'tp1_hit': tp1, 'tp2_hit': tp2, 'tp3_hit': tp3, 'tp4_hit': tp4
    }


def run_backtest():
    signal_db = sqlite3.connect(SIGNAL_DB)
    signal_db.row_factory = sqlite3.Row
    kline_db = sqlite3.connect(KLINE_DB)
    kline_db.row_factory = sqlite3.Row

    # 统计 K 线覆盖情况
    kline_tokens = set(r['token_ca'] for r in kline_db.execute("SELECT DISTINCT token_ca FROM kline_1m"))
    print(f"K线缓存中有 {len(kline_tokens)} 个 token 的数据")

    # ============================================================
    # 定义回测组
    # ============================================================
    groups = {
        'ATH#1 (PASS, MC 30-300K)': {
            'query': """
                SELECT token_ca, symbol, MIN(market_cap) as entry_mc, MIN(timestamp) as entry_ts
                FROM premium_signals
                WHERE hard_gate_status = 'PASS' AND market_cap BETWEEN 30000 AND 300000
                GROUP BY token_ca
            """,
            'exclude': set(),
        },
        'ATH#1 (PASS, MC < 50K)': {
            'query': """
                SELECT token_ca, symbol, MIN(market_cap) as entry_mc, MIN(timestamp) as entry_ts
                FROM premium_signals
                WHERE hard_gate_status = 'PASS' AND market_cap < 50000
                GROUP BY token_ca
            """,
            'exclude': set(),
        },
        'ATH#2 (V17, MC 30-300K)': {
            'query': """
                SELECT token_ca, symbol, MIN(market_cap) as entry_mc, MIN(timestamp) as entry_ts
                FROM premium_signals
                WHERE hard_gate_status = 'V17_NOT_ATH1' AND market_cap BETWEEN 30000 AND 300000
                GROUP BY token_ca
            """,
            'exclude': 'ath1_30_300k',
        },
        'ATH#2 (V17, MC < 50K)': {
            'query': """
                SELECT token_ca, symbol, MIN(market_cap) as entry_mc, MIN(timestamp) as entry_ts
                FROM premium_signals
                WHERE hard_gate_status = 'V17_NOT_ATH1' AND market_cap < 50000
                GROUP BY token_ca
            """,
            'exclude': 'ath1_lt50k',
        },
    }

    # 先取 ATH#1 的 token 列表用于排除
    ath1_30_300k_tokens = set(r[0] for r in signal_db.execute("""
        SELECT DISTINCT token_ca FROM premium_signals
        WHERE hard_gate_status = 'PASS' AND market_cap BETWEEN 30000 AND 300000
    """))
    ath1_lt50k_tokens = set(r[0] for r in signal_db.execute("""
        SELECT DISTINCT token_ca FROM premium_signals
        WHERE hard_gate_status = 'PASS' AND market_cap < 50000
    """))

    exclude_map = {
        'ath1_30_300k': ath1_30_300k_tokens,
        'ath1_lt50k': ath1_lt50k_tokens,
    }

    all_results = {}

    for group_name, config in groups.items():
        rows = signal_db.execute(config['query']).fetchall()

        # 排除
        exclude_set = set()
        if isinstance(config['exclude'], str) and config['exclude']:
            exclude_set = exclude_map.get(config['exclude'], set())

        tokens = [r for r in rows if r['token_ca'] not in exclude_set]

        # 检查 K 线覆盖
        has_kline = [t for t in tokens if t['token_ca'] in kline_tokens]
        no_kline = [t for t in tokens if t['token_ca'] not in kline_tokens]

        print(f"\n{'='*60}")
        print(f"  {group_name}")
        print(f"  总计: {len(tokens)} | 有K线: {len(has_kline)} | 无K线: {no_kline.__len__()}")
        print(f"  覆盖率: {len(has_kline)/max(1,len(tokens))*100:.1f}%")
        print(f"{'='*60}")

        results = []
        for t in has_kline:
            ca = t['token_ca']
            entry_mc = t['entry_mc']
            entry_ts = t['entry_ts'] / 1000  # ms -> s

            # 从 K 线缓存获取入场时的价格
            # entry_ts 是信号时间，找最近的 K 线
            klines = kline_db.execute("""
                SELECT timestamp, open, high, low, close, volume
                FROM kline_1m
                WHERE token_ca = ? AND timestamp >= ? - 120
                ORDER BY timestamp ASC
            """, (ca, int(entry_ts))).fetchall()

            if not klines:
                continue

            # 入场价 = 信号时间最近的 K 线的 close
            first_kline = klines[0]
            entry_price = first_kline[4]  # close

            if entry_price <= 0:
                continue

            result = simulate_v19_asymmetric(
                [(k[0], k[1], k[2], k[3], k[4], k[5]) for k in klines],
                entry_price, int(entry_ts)
            )

            results.append({
                'symbol': t['symbol'],
                'ca': ca,
                'entry_mc': entry_mc,
                'entry_price': entry_price,
                **result,
            })

        all_results[group_name] = results
        print_group_results(results, group_name)

    # 最终对比表
    print_comparison(all_results)

    signal_db.close()
    kline_db.close()


def print_group_results(results, name):
    total = len(results)
    if total == 0:
        print(f"  无有效回测结果")
        return

    wins = sum(1 for r in results if r['pnl'] > 0)
    avg_pnl = sum(r['pnl'] for r in results) / total
    median_pnl = sorted(r['pnl'] for r in results)[total // 2]
    avg_peak = sum(r['high_pnl'] for r in results) / total

    tp1_rate = sum(1 for r in results if r['tp1_hit']) / total * 100
    tp2_rate = sum(1 for r in results if r['tp2_hit']) / total * 100
    tp3_rate = sum(1 for r in results if r['tp3_hit']) / total * 100
    tp4_rate = sum(1 for r in results if r['tp4_hit']) / total * 100

    # SOL 计算（假设每笔 0.06 SOL）
    pos_size = 0.06
    total_invested = pos_size * total
    total_returned = sum(pos_size * max(0, 1 + r['pnl'] / 100) for r in results)
    net_sol = total_returned - total_invested
    roi = net_sol / total_invested * 100

    print(f"\n  回测结果 ({total} tokens with K线):")
    print(f"  胜率: {wins/total*100:.1f}% ({wins}W / {total-wins}L)")
    print(f"  平均 PnL: {avg_pnl:+.1f}% | 中位 PnL: {median_pnl:+.1f}%")
    print(f"  平均峰值 PnL: +{avg_peak:.1f}%")
    print(f"  TP触达率: TP1={tp1_rate:.1f}% TP2={tp2_rate:.1f}% TP3={tp3_rate:.1f}% TP4={tp4_rate:.1f}%")
    print(f"  ROI: {roi:+.1f}% | 每笔{pos_size}SOL × {total}笔 = 投入{total_invested:.2f} → 净利{net_sol:+.3f} SOL")

    # 退出原因分布
    reasons = defaultdict(int)
    for r in results:
        reason = r['exit_reason']
        if 'HARD_SL' in reason: reasons['硬止损-35%'] += 1
        elif 'BREAKEVEN' in reason: reasons['保本止损0%'] += 1
        elif 'DEAD_WATER' in reason: reasons['15分死水'] += 1
        elif 'TIMEOUT_30M' in reason: reasons['30分超时'] += 1
        elif 'TIMEOUT_2H' in reason: reasons['TP1+2h超时'] += 1
        elif 'MOONBAG' in reason: reasons['Moonbag回撤'] += 1
        elif 'KLINE_END' in reason: reasons['K线结束'] += 1
        elif 'NO_DATA' in reason: reasons['无数据'] += 1
        else: reasons[reason[:15]] += 1

    print(f"\n  退出原因:")
    for reason, count in sorted(reasons.items(), key=lambda x: -x[1]):
        bar = '█' * int(count / total * 25)
        avg_r = sum(r['pnl'] for r in results
                    if (reason == '硬止损-35%' and 'HARD_SL' in r['exit_reason']) or
                       (reason == '保本止损0%' and 'BREAKEVEN' in r['exit_reason']) or
                       (reason == '15分死水' and 'DEAD_WATER' in r['exit_reason']) or
                       (reason == '30分超时' and 'TIMEOUT_30M' in r['exit_reason']) or
                       (reason == 'TP1+2h超时' and 'TIMEOUT_2H' in r['exit_reason']) or
                       (reason == 'Moonbag回撤' and 'MOONBAG' in r['exit_reason']) or
                       (reason == 'K线结束' and 'KLINE_END' in r['exit_reason'])
                   ) / max(1, count)
        print(f"    {reason:>14s}: {count:3d} ({count/total*100:5.1f}%) avg={avg_r:+.0f}% {bar}")

    # PnL 分布
    buckets = [('亏>50%', -999, -50), ('亏35%', -50, -34), ('亏20-35%', -34, -20),
               ('亏0-20%', -20, 0), ('赚0-50%', 0, 50), ('赚50-100%', 50, 100),
               ('赚100-200%', 100, 200), ('赚200%+', 200, 99999)]
    print(f"\n  PnL 分布:")
    for label, lo, hi in buckets:
        c = sum(1 for r in results if lo < r['pnl'] <= hi)
        bar = '█' * int(c / max(1, total) * 25)
        print(f"    {label:>12s}: {c:3d} ({c/total*100:5.1f}%) {bar}")


def print_comparison(all_results):
    print(f"\n{'═' * 80}")
    print(f"  最终对比表")
    print(f"{'═' * 80}")

    header = f"  {'指标':>14s}"
    sep = f"  {'-'*14}"
    for name in all_results:
        short = name.split('(')[1].rstrip(')') if '(' in name else name
        header += f" | {short:>16s}"
        sep += f"-+-{'-'*16}"
    print(header)
    print(sep)

    metrics = ['样本数', '胜率', '平均PnL', '中位PnL', '平均峰值', 'TP1率', 'TP2率', 'ROI']

    for metric in metrics:
        line = f"  {metric:>14s}"
        for name, results in all_results.items():
            t = len(results)
            if t == 0:
                line += f" | {'N/A':>16s}"
                continue

            if metric == '样本数':
                line += f" | {t:>16d}"
            elif metric == '胜率':
                wr = sum(1 for r in results if r['pnl'] > 0) / t * 100
                line += f" | {wr:>15.1f}%"
            elif metric == '平均PnL':
                avg = sum(r['pnl'] for r in results) / t
                line += f" | {avg:>+15.1f}%"
            elif metric == '中位PnL':
                med = sorted(r['pnl'] for r in results)[t // 2]
                line += f" | {med:>+15.1f}%"
            elif metric == '平均峰值':
                peak = sum(r['high_pnl'] for r in results) / t
                line += f" | {peak:>+15.1f}%"
            elif metric == 'TP1率':
                tp1 = sum(1 for r in results if r['tp1_hit']) / t * 100
                line += f" | {tp1:>15.1f}%"
            elif metric == 'TP2率':
                tp2 = sum(1 for r in results if r['tp2_hit']) / t * 100
                line += f" | {tp2:>15.1f}%"
            elif metric == 'ROI':
                pos_size = 0.06
                total_invested = pos_size * t
                total_returned = sum(pos_size * max(0, 1 + r['pnl'] / 100) for r in results)
                roi = (total_returned - total_invested) / total_invested * 100
                line += f" | {roi:>+15.1f}%"

        print(line)


if __name__ == '__main__':
    run_backtest()
