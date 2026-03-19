#!/usr/bin/env python3
"""
NOT_ATH 策略参数优化器
========================
目标: 找到满足以下条件的最优参数组合
  - 每日交易 10-20 笔
  - 胜率 ≥ 65%
  - 盈利率 ≥ 200% (总盈利/总投入)

方法: 先对所有 NOT_ATH 信号模拟交易（无过滤），再用网格搜索找最优过滤组合
"""

import json, os, sys, itertools
from datetime import datetime, timezone
from collections import defaultdict

os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

SLIP = 0.004
POS  = 0.06

# ─── 工具函数 ─────────────────────────────────────────────────────────────

def get_idx(sig, key):
    idx = sig.get('indices') or {}
    val = idx.get(key)
    if val is None:
        val = sig.get(key, 0)
    if isinstance(val, dict):
        return val.get('signal', 0) or 0
    return val if val is not None else 0

def find_entry_candle(candles, sig_ts_ms):
    eb = (sig_ts_ms // 1000 // 60) * 60
    return next((c for c in candles if c['ts'] <= eb and c['ts'] > eb - 120), None)

def calc_velocity(candles, entry_bar_ts):
    window_start = entry_bar_ts - 300
    bars = [c for c in candles if window_start <= c['ts'] < entry_bar_ts]
    if not bars:
        return None
    p0, p1 = bars[0]['o'], bars[-1]['c']
    return (p1 - p0) / p0 * 100 if p0 > 0 else None

def calc_pre_momentum(candles, entry_bar_ts):
    bars = [c for c in candles if c['ts'] < entry_bar_ts][-3:]
    if len(bars) < 2:
        return 0.0
    return (bars[-1]['c'] - bars[0]['o']) / bars[0]['o'] * 100

def simulate(candles, ec, sl, tp1_r, tp1_sell, tp2_r, tp2_sell, tp3_r, tp3_sell, tp4_r, tp4_sell, dw, mh):
    after = [c for c in candles if c['ts'] > ec['ts']]
    if not after:
        return None

    ep  = ec['c'] * (1 + SLIP)
    rem = POS / ep
    sol_out = 0.0
    tp1_hit = False
    peak    = 0.0
    hold    = 0
    ex      = None

    tp1_p = ep * (1 + tp1_r)
    tp2_p = ep * (1 + tp2_r)
    tp3_p = ep * (1 + tp3_r)
    tp4_p = ep * (1 + tp4_r)
    sl_p  = ep * (1 + sl)

    for c in after:
        if rem <= 1e-10:
            break
        hold += 1
        lo = c['l']; hi = c['h']

        cur_sl = ep if tp1_hit else sl_p
        if lo <= cur_sl:
            sol_out += rem * cur_sl * (1 - SLIP)
            rem = 0
            ex = 'SL_BE' if tp1_hit else 'SL'
            break

        if rem > 1e-10 and hi >= tp4_p:
            sell = rem * tp4_sell
            sol_out += sell * tp4_p * (1 - SLIP)
            rem -= sell
            ex = ex or 'TP4'

        if rem > 1e-10 and hi >= tp3_p:
            sell = rem * tp3_sell
            sol_out += sell * tp3_p * (1 - SLIP)
            rem -= sell
            ex = ex or 'TP3'

        if rem > 1e-10 and hi >= tp2_p:
            sell = rem * tp2_sell
            sol_out += sell * tp2_p * (1 - SLIP)
            rem -= sell
            ex = ex or 'TP2'

        if not tp1_hit and rem > 1e-10 and hi >= tp1_p:
            sell = rem * tp1_sell
            sol_out += sell * tp1_p * (1 - SLIP)
            rem -= sell
            tp1_hit = True
            ex = ex or 'TP1'

        if hi > peak:
            peak = hi
        if not tp1_hit:
            peak_pct = (peak - ep) / ep if ep > 0 else 0
            if hold >= dw and peak_pct < tp1_r and (c['c'] - ep) / ep <= 0.20:
                sol_out += rem * c['c'] * (1 - SLIP)
                rem = 0
                ex = ex or 'DEAD'
                break
            if hold >= mh:
                sol_out += rem * c['c'] * (1 - SLIP)
                rem = 0
                ex = ex or 'TIMEOUT'
                break

    if rem > 1e-10:
        sol_out += rem * after[-1]['c'] * (1 - SLIP)
        ex = ex or 'END'

    peak_pct = (peak - ep) / ep * 100 if ep > 0 else 0
    return {
        'pnl':  sol_out - POS,
        'pct':  (sol_out - POS) / POS * 100,
        'ex':   ex,
        'tp1':  tp1_hit,
        'peak': peak_pct,
        'hold': hold,
    }

# ─── 加载数据 ─────────────────────────────────────────────────────────────

def load_all():
    hist = json.load(open('data/channel-history.json'))
    signals = list(hist['signals'])

    for aux_path in ['/tmp/mar1316_signals_filtered.json', '/tmp/mar1618_signals.json']:
        if os.path.exists(aux_path):
            aux = json.load(open(aux_path))
            seen = {(s.get('token_ca',''), s['ts']) for s in signals}
            for s in aux:
                ca = s.get('token_ca') or s.get('ca', '')
                if (ca, s['ts']) not in seen and ca:
                    if 'indices' not in s:
                        s['indices'] = {
                            'super_index': s.get('super_index', 0),
                            'ai_index': s.get('ai_index', 0),
                            'trade_index': s.get('trade_index', 0),
                            'address_index': s.get('address_index', 0),
                            'sentiment_index': s.get('sentiment_index', 0),
                            'media_index': s.get('media_index', 0),
                        }
                    s['token_ca'] = ca
                    signals.append(s)

    cache = json.load(open('data/ohlcv-cache.json'))
    return signals, cache


def precompute_all_trades(signals, cache):
    """对所有 NOT_ATH 信号预计算多套退出参数下的结果"""
    # 多组退出参数
    exit_configs = [
        # (name, sl, tp1_r, tp1_sell, tp2_r, tp2_sell, tp3_r, tp3_sell, tp4_r, tp4_sell, dw, mh)
        ('A_SL15_TP80',  -0.15, 0.80, 0.60, 1.00, 0.50, 2.00, 0.50, 5.00, 0.80, 8, 15),
        ('B_SL20_TP60',  -0.20, 0.60, 0.60, 1.00, 0.50, 2.00, 0.50, 5.00, 0.80, 8, 15),
        ('C_SL10_TP50',  -0.10, 0.50, 0.60, 0.80, 0.50, 1.50, 0.50, 3.00, 0.80, 6, 12),
        ('D_SL15_TP40',  -0.15, 0.40, 0.50, 0.80, 0.50, 1.50, 0.50, 3.00, 0.80, 6, 12),
        ('E_SL10_TP30',  -0.10, 0.30, 0.50, 0.60, 0.40, 1.00, 0.40, 2.00, 0.80, 5, 10),
        ('F_SL08_TP25',  -0.08, 0.25, 0.50, 0.50, 0.40, 1.00, 0.40, 2.00, 0.80, 5, 10),
        ('G_SL12_TP35',  -0.12, 0.35, 0.50, 0.70, 0.40, 1.20, 0.40, 2.50, 0.80, 6, 12),
        ('H_SL15_TP50',  -0.15, 0.50, 0.60, 1.00, 0.50, 2.00, 0.50, 5.00, 0.80, 8, 15),
        ('I_SL10_TP40',  -0.10, 0.40, 0.50, 0.80, 0.50, 1.50, 0.50, 3.00, 0.80, 6, 12),
        ('J_SL20_TP80',  -0.20, 0.80, 0.60, 1.50, 0.50, 3.00, 0.50, 5.00, 0.80, 10, 20),
    ]

    all_trades = {}  # { exit_name: [ {sig_features + result} ] }

    for name, sl, tp1r, tp1s, tp2r, tp2s, tp3r, tp3s, tp4r, tp4s, dw, mh in exit_configs:
        trades = []
        for sig in signals:
            if sig.get('is_ath'):
                continue
            ca = sig.get('token_ca') or sig.get('ca', '')
            if not ca:
                continue
            cd = cache.get(ca)
            if not cd or not cd.get('candles'):
                continue
            candles = cd['candles']
            ec = find_entry_candle(candles, sig['ts'])
            if not ec:
                continue

            entry_bar_ts = ec['ts']
            sig_dt = datetime.fromtimestamp(sig['ts'] / 1000, tz=timezone.utc)

            r = simulate(candles, ec, sl, tp1r, tp1s, tp2r, tp2s, tp3r, tp3s, tp4r, tp4s, dw, mh)
            if not r:
                continue

            mc = sig.get('market_cap') or 0
            mc_k = mc / 1000 if mc else 0
            vel = calc_velocity(candles, entry_bar_ts)
            pre = calc_pre_momentum(candles, entry_bar_ts)

            trades.append({
                **r,
                'ca': ca,
                'sym': sig.get('symbol', ca[:8]),
                'ts': sig['ts'],
                'date': sig_dt.strftime('%m/%d'),
                'hour': sig_dt.hour,
                'mc_k': mc_k,
                'si': get_idx(sig, 'super_index'),
                'ai': get_idx(sig, 'ai_index'),
                'ti': get_idx(sig, 'trade_index'),
                'addi': get_idx(sig, 'address_index'),
                'senti': get_idx(sig, 'sentiment_index'),
                'mi': get_idx(sig, 'media_index'),
                'vel': vel,
                'pre': pre,
                'has_mc': mc > 0,
            })

        all_trades[name] = trades
        print(f'  退出策略 {name}: {len(trades)} 笔原始交易')

    return all_trades, exit_configs


def grid_search(all_trades, exit_configs):
    """网格搜索过滤参数"""

    # 过滤参数搜索空间
    si_mins = [0, 50, 80, 100, 120, 150, 200]
    ai_mins = [0, 40, 50, 60, 70]
    ai_maxes = [0, 80, 90, 100, 150, 999]  # 0=禁用
    mc_maxes = [15, 20, 25, 30, 50, 999]  # 999=不限
    mc_mins = [0, 3, 5, 8]
    ti_mins = [0, 1, 2, 3]
    mi_mins = [0, 30, 60]
    peak_mins = [0]  # 0=不用 peak filter (后过滤)
    skip_utc_20_22 = [True, False]

    # 确定有效天数（用于每日交易数计算）
    # 03/13-18 = 6天，但03/13-15数据少，更真实的是 03/16-18 = 3天
    # 我们用所有可用天计算，但分开报告

    best_results = []
    total_combos = 0

    for exit_name, sl, tp1r, tp1s, tp2r, tp2s, tp3r, tp3s, tp4r, tp4s, dw, mh in exit_configs:
        trades = all_trades[exit_name]
        if not trades:
            continue

        for si_min, ai_min, ai_max, mc_max, mc_min, ti_min, mi_min, skip_utc in itertools.product(
            si_mins, ai_mins, ai_maxes, mc_maxes, mc_mins, ti_mins, mi_mins, skip_utc_20_22
        ):
            total_combos += 1
            if ai_max > 0 and ai_max <= ai_min:
                continue

            filtered = []
            for t in trades:
                if t['si'] < si_min:
                    continue
                if t['ai'] < ai_min:
                    continue
                if ai_max > 0 and t['ai'] >= ai_max:
                    continue
                if t['has_mc']:
                    if mc_max < 999 and t['mc_k'] > mc_max:
                        continue
                    if t['mc_k'] < mc_min:
                        continue
                if t['ti'] < ti_min:
                    continue
                if t['mi'] < mi_min:
                    continue
                if skip_utc and 20 <= t['hour'] < 22:
                    continue
                filtered.append(t)

            n = len(filtered)
            if n < 6:  # 至少6笔才有统计意义
                continue

            wins = sum(1 for t in filtered if t['pnl'] > 0)
            wr = wins / n

            # 计算每日交易数 (用 03/16-18 的3天, 数据最完整)
            by_date = defaultdict(int)
            for t in filtered:
                by_date[t['date']] += 1
            core_dates = [d for d in by_date if d in ('03/16', '03/17', '03/18')]
            if core_dates:
                daily_avg = sum(by_date[d] for d in core_dates) / len(core_dates)
            else:
                daily_avg = n / 6.0

            total_pnl = sum(t['pnl'] for t in filtered)
            total_invested = POS * n
            profit_rate = total_pnl / total_invested * 100 if total_invested > 0 else 0

            # 检查是否满足目标
            if wr >= 0.50 and 5 <= daily_avg <= 30:
                ev_pct = total_pnl / n / POS * 100

                best_results.append({
                    'exit': exit_name,
                    'si_min': si_min,
                    'ai_min': ai_min,
                    'ai_max': ai_max,
                    'mc_max': mc_max,
                    'mc_min': mc_min,
                    'ti_min': ti_min,
                    'mi_min': mi_min,
                    'skip_utc': skip_utc,
                    'n': n,
                    'wr': wr,
                    'daily_avg': daily_avg,
                    'ev_pct': ev_pct,
                    'total_pnl': total_pnl,
                    'profit_rate': profit_rate,
                    'by_date': dict(by_date),
                })

    print(f'\n总搜索组合: {total_combos}')
    return best_results


def main():
    print('=' * 70)
    print('NOT_ATH 策略参数优化器')
    print('目标: 10-20笔/天 | 65%胜率 | 200%盈利率')
    print('=' * 70)

    print('\n加载数据...')
    signals, cache = load_all()
    print(f'  信号: {len(signals)}个  K线: {len(cache)}个token')

    print('\n预计算交易...')
    all_trades, exit_configs = precompute_all_trades(signals, cache)

    print('\n网格搜索...')
    results = grid_search(all_trades, exit_configs)

    # 排序: 优先胜率 > EV > 日交易量接近15
    results.sort(key=lambda r: (
        r['wr'],
        r['ev_pct'],
        -abs(r['daily_avg'] - 15),
    ), reverse=True)

    # ─── 输出 TOP 结果 ──────────────────────────────────────────────────
    print(f'\n{"=" * 100}')
    print(f'找到 {len(results)} 个满足条件的参数组合 (WR≥50%, 5-30笔/天)')
    print(f'{"=" * 100}')

    # WR >= 65% 的
    elite = [r for r in results if r['wr'] >= 0.65 and 8 <= r['daily_avg'] <= 25]
    print(f'\n精英组 (WR≥65%, 8-25笔/天): {len(elite)}个')

    # WR >= 60%
    good = [r for r in results if r['wr'] >= 0.60 and 8 <= r['daily_avg'] <= 25]
    print(f'优秀组 (WR≥60%, 8-25笔/天): {len(good)}个')

    # WR >= 55%
    decent = [r for r in results if r['wr'] >= 0.55 and 8 <= r['daily_avg'] <= 25]
    print(f'良好组 (WR≥55%, 8-25笔/天): {len(decent)}个')

    # 分层展示
    for tier_name, tier, limit in [
        ('精英 (WR≥65%)', elite, 20),
        ('优秀 (WR≥60%)', good, 15),
        ('良好 (WR≥55%)', decent, 10),
    ]:
        if not tier:
            continue
        print(f'\n{"─" * 100}')
        print(f'TOP {min(limit, len(tier))} — {tier_name}')
        print(f'{"─" * 100}')
        header = (f'{"#":>3} {"Exit":<16} {"SI":>4} {"AI":>6} {"MC":>8} {"TI":>3} {"MI":>3} '
                  f'{"UTC":>4} {"N":>4} {"WR":>6} {"D/天":>5} {"EV%":>7} {"PnL":>8} {"PR%":>6}')
        print(header)

        for i, r in enumerate(tier[:limit]):
            ai_str = f'{r["ai_min"]}+' if r['ai_max'] == 0 or r['ai_max'] >= 999 else f'{r["ai_min"]}-{r["ai_max"]}'
            mc_str = f'{r["mc_min"]}-{"∞" if r["mc_max"] >= 999 else str(r["mc_max"])}K'
            utc_str = 'skip' if r['skip_utc'] else 'all'
            print(f'{i+1:>3} {r["exit"]:<16} {r["si_min"]:>4} {ai_str:>6} {mc_str:>8} {r["ti_min"]:>3} {r["mi_min"]:>3} '
                  f'{utc_str:>4} {r["n"]:>4} {r["wr"]*100:>5.1f}% {r["daily_avg"]:>5.1f} '
                  f'{r["ev_pct"]:>+6.1f}% {r["total_pnl"]:>+7.4f} {r["profit_rate"]:>+5.0f}%')

            # 按日期明细
            dates_str = ' '.join(f'{d}:{r["by_date"].get(d,0)}' for d in sorted(r["by_date"]))
            print(f'     日期: {dates_str}')

    # ─── 最佳策略详细分析 ──────────────────────────────────────────────
    if elite:
        best = elite[0]
        print(f'\n{"=" * 100}')
        print('最优策略详细分析')
        print(f'{"=" * 100}')
        print(f'退出策略: {best["exit"]}')
        print(f'过滤: SI≥{best["si_min"]} | AI {best["ai_min"]}-{best["ai_max"] if best["ai_max"]<999 else "∞"} | '
              f'MC {best["mc_min"]}-{best["mc_max"] if best["mc_max"]<999 else "∞"}K | '
              f'TI≥{best["ti_min"]} | MI≥{best["mi_min"]} | UTC20-22:{"skip" if best["skip_utc"] else "allow"}')
        print(f'结果: {best["n"]}笔 | WR={best["wr"]*100:.1f}% | '
              f'EV={best["ev_pct"]:+.1f}% | PnL={best["total_pnl"]:+.4f} SOL | '
              f'日均={best["daily_avg"]:.1f}笔')

        # 重新获取完整交易列表
        exit_cfg = next(e for e in exit_configs if e[0] == best['exit'])
        trades = all_trades[best['exit']]
        filtered = []
        for t in trades:
            if t['si'] < best['si_min']: continue
            if t['ai'] < best['ai_min']: continue
            if best['ai_max'] > 0 and best['ai_max'] < 999 and t['ai'] >= best['ai_max']: continue
            if t['has_mc']:
                if best['mc_max'] < 999 and t['mc_k'] > best['mc_max']: continue
                if t['mc_k'] < best['mc_min']: continue
            if t['ti'] < best['ti_min']: continue
            if t['mi'] < best['mi_min']: continue
            if best['skip_utc'] and 20 <= t['hour'] < 22: continue
            filtered.append(t)

        wins = [t for t in filtered if t['pnl'] > 0]
        losses = [t for t in filtered if t['pnl'] <= 0]
        avg_win = sum(t['pct'] for t in wins) / len(wins) if wins else 0
        avg_loss = sum(t['pct'] for t in losses) / len(losses) if losses else 0
        rr = abs(avg_win / avg_loss) if avg_loss else 0

        print(f'平均盈利: {avg_win:+.1f}% | 平均亏损: {avg_loss:+.1f}% | 盈亏比: {rr:.2f}x')

        # 退出分布
        exits = defaultdict(int)
        for t in filtered:
            exits[t['ex']] += 1
        print(f'退出分布: {" | ".join(f"{k}:{v}" for k,v in sorted(exits.items(), key=lambda x:-x[1]))}')

        # 按日期
        print(f'\n按日期:')
        by_date = defaultdict(list)
        for t in filtered:
            by_date[t['date']].append(t)
        for d in sorted(by_date):
            g = by_date[d]
            g_wr = len([t for t in g if t['pnl']>0]) / len(g) * 100
            g_pnl = sum(t['pnl'] for t in g)
            print(f'  {d}: {len(g)}笔  WR={g_wr:.0f}%  PnL={g_pnl:+.4f} SOL')

        # 明细
        print(f'\n交易明细 (按PnL排序):')
        print(f'  {"SYM":<12} {"Date":>5} {"H":>3} {"MC":>5} {"SI":>4} {"AI":>4} {"TI":>3} {"MI":>3} {"Peak":>6} {"PnL%":>7} {"Exit"}')
        for t in sorted(filtered, key=lambda x: x['pnl'], reverse=True):
            flag = '✅' if t['pnl'] > 0 else '❌'
            print(f'  {flag} {t["sym"]:<10} {t["date"]:>5} {t["hour"]:02d}  '
                  f'{t["mc_k"]:>4.0f}K {t["si"]:>4.0f} {t["ai"]:>4.0f} {t["ti"]:>3.0f} {t["mi"]:>3.0f} '
                  f'{t["peak"]:>+5.0f}% {t["pct"]:>+6.1f}%  {t["ex"]}')

    # ─── 保存结果 ────────────────────────────────────────────────────────
    output = {
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'target': {'daily_trades': '10-20', 'win_rate': '65%+', 'profit_rate': '200%+'},
        'elite_count': len(elite),
        'good_count': len(good),
        'decent_count': len(decent),
        'top_strategies': (elite or good or decent)[:30],
    }
    with open('data/strategy-optimization-results.json', 'w') as f:
        json.dump(output, f, indent=2, default=str)
    print(f'\n结果已保存: data/strategy-optimization-results.json')


if __name__ == '__main__':
    main()
