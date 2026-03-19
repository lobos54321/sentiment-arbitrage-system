#!/usr/bin/env python3
"""
NOT_ATH 策略参数优化器 v2
========================
更精细的退出参数网格 + ATH 信号联合分析 + 真实月回报计算
"""

import json, os, sys, itertools
from datetime import datetime, timezone
from collections import defaultdict

os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

SLIP = 0.004
POS  = 0.06

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

def simulate(candles, ec, sl, tp1_r, tp1_sell, dw, mh):
    """简化模拟: SL → TP1(卖部分) → SL移至成本 → DW/MH"""
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

        if not tp1_hit and hi >= tp1_p:
            sell = rem * tp1_sell
            sol_out += sell * tp1_p * (1 - SLIP)
            rem -= sell
            tp1_hit = True
            ex = 'TP1'

        if hi > peak:
            peak = hi

        # TP1 未命中的 dead water / timeout
        if not tp1_hit:
            peak_pct = (peak - ep) / ep if ep > 0 else 0
            if hold >= dw and peak_pct < tp1_r and (c['c'] - ep) / ep <= 0.20:
                sol_out += rem * c['c'] * (1 - SLIP)
                rem = 0
                ex = 'DEAD'
                break
            if hold >= mh:
                sol_out += rem * c['c'] * (1 - SLIP)
                rem = 0
                ex = 'TIMEOUT'
                break
        # TP1 命中后，给更多时间跑
        elif tp1_hit and hold >= mh * 2:
            sol_out += rem * c['c'] * (1 - SLIP)
            rem = 0
            ex = 'TIMEOUT_POST'
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
                        s['indices'] = {k: s.get(k, 0) for k in ['super_index','ai_index','trade_index','address_index','sentiment_index','media_index']}
                    s['token_ca'] = ca
                    signals.append(s)
    cache = json.load(open('data/ohlcv-cache.json'))
    return signals, cache


def main():
    print('=' * 80)
    print('NOT_ATH 策略参数优化器 v2 — 精细搜索')
    print('=' * 80)

    signals, cache = load_all()
    print(f'信号: {len(signals)}  K线: {len(cache)} tokens')

    # ─── 退出参数网格 ────────────────────────────────────────────────────
    exit_grid = [
        # (sl, tp1_r, tp1_sell, dw, mh)
        (-0.08, 0.25, 0.50, 5, 10),
        (-0.08, 0.30, 0.50, 5, 10),
        (-0.08, 0.35, 0.50, 5, 10),
        (-0.10, 0.25, 0.50, 5, 10),
        (-0.10, 0.30, 0.50, 5, 10),
        (-0.10, 0.35, 0.50, 6, 12),
        (-0.10, 0.40, 0.50, 6, 12),
        (-0.10, 0.50, 0.60, 6, 12),
        (-0.12, 0.30, 0.50, 5, 10),
        (-0.12, 0.35, 0.50, 6, 12),
        (-0.12, 0.40, 0.50, 6, 12),
        (-0.12, 0.50, 0.60, 6, 12),
        (-0.15, 0.35, 0.50, 6, 12),
        (-0.15, 0.40, 0.50, 6, 12),
        (-0.15, 0.50, 0.60, 8, 15),
        (-0.15, 0.60, 0.60, 8, 15),
        (-0.15, 0.80, 0.60, 8, 15),
        (-0.20, 0.40, 0.50, 8, 15),
        (-0.20, 0.50, 0.60, 8, 15),
        (-0.20, 0.60, 0.60, 8, 15),
        (-0.20, 0.80, 0.60, 10, 20),
        (-0.25, 0.60, 0.60, 8, 15),
        (-0.25, 0.80, 0.60, 10, 20),
        (-0.25, 1.00, 0.60, 10, 20),
    ]

    # ─── 对所有 NOT_ATH 信号预计算特征 ────────────────────────────────────
    print('\n预处理信号...')
    prepared = []
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
        sig_dt = datetime.fromtimestamp(sig['ts'] / 1000, tz=timezone.utc)
        mc = sig.get('market_cap') or 0
        vel = calc_velocity(candles, ec['ts'])
        pre = calc_pre_momentum(candles, ec['ts'])

        prepared.append({
            'sig': sig,
            'candles': candles,
            'ec': ec,
            'ca': ca,
            'sym': sig.get('symbol', ca[:8]),
            'ts': sig['ts'],
            'date': sig_dt.strftime('%m/%d'),
            'hour': sig_dt.hour,
            'mc_k': mc / 1000 if mc else 0,
            'has_mc': mc > 0,
            'si': get_idx(sig, 'super_index'),
            'ai': get_idx(sig, 'ai_index'),
            'ti': get_idx(sig, 'trade_index'),
            'addi': get_idx(sig, 'address_index'),
            'senti': get_idx(sig, 'sentiment_index'),
            'mi': get_idx(sig, 'media_index'),
            'vel': vel,
            'pre': pre,
        })
    print(f'可回测信号: {len(prepared)}')

    # ─── 过滤参数搜索空间 ─────────────────────────────────────────────────
    si_mins   = [0, 50, 80, 100, 120, 150]
    ai_mins   = [0, 40, 50, 60]
    ai_maxes  = [0, 80, 90, 100]  # 0=禁用
    mc_maxes  = [15, 20, 25, 30, 50, 999]
    mc_mins   = [0, 5, 8]
    ti_mins   = [0, 1, 2]
    mi_mins   = [0, 30, 60]

    best_results = []
    combos = 0

    for ex_idx, (sl, tp1_r, tp1_sell, dw, mh) in enumerate(exit_grid):
        # 模拟所有交易
        sim_cache = {}
        for i, p in enumerate(prepared):
            r = simulate(p['candles'], p['ec'], sl, tp1_r, tp1_sell, dw, mh)
            sim_cache[i] = r

        ex_label = f'SL{int(sl*100)}_TP{int(tp1_r*100)}'

        for si_min, ai_min, ai_max, mc_max, mc_min, ti_min, mi_min in itertools.product(
            si_mins, ai_mins, ai_maxes, mc_maxes, mc_mins, ti_mins, mi_mins
        ):
            combos += 1
            if ai_max > 0 and ai_max <= ai_min:
                continue

            filtered_indices = []
            for i, p in enumerate(prepared):
                if p['si'] < si_min: continue
                if p['ai'] < ai_min: continue
                if ai_max > 0 and p['ai'] >= ai_max: continue
                if p['has_mc']:
                    if mc_max < 999 and p['mc_k'] > mc_max: continue
                    if p['mc_k'] < mc_min: continue
                if p['ti'] < ti_min: continue
                if p['mi'] < mi_min: continue
                r = sim_cache.get(i)
                if r is None: continue
                filtered_indices.append((i, r))

            n = len(filtered_indices)
            if n < 8:
                continue

            wins = sum(1 for _, r in filtered_indices if r['pnl'] > 0)
            wr = wins / n

            # 按日期统计 (只用03/17-18，数据最完整)
            by_date = defaultdict(int)
            for idx, _ in filtered_indices:
                by_date[prepared[idx]['date']] += 1

            core_dates = [d for d in by_date if d in ('03/17', '03/18')]
            if core_dates:
                daily_avg = sum(by_date[d] for d in core_dates) / len(core_dates)
            else:
                continue

            total_pnl = sum(r['pnl'] for _, r in filtered_indices)
            ev_pct = total_pnl / n / POS * 100

            # 计算月回报率
            # 假设基础资本 = 3 SOL, 每笔 0.06 SOL
            # 月交易数 = daily_avg * 30
            capital = 3.0
            monthly_trades = daily_avg * 30
            monthly_pnl = monthly_trades * (total_pnl / n)  # 按均值推算
            monthly_return_pct = monthly_pnl / capital * 100

            if wr >= 0.50 and 5 <= daily_avg <= 30:
                best_results.append({
                    'ex': ex_label,
                    'sl': sl, 'tp1_r': tp1_r, 'tp1_sell': tp1_sell, 'dw': dw, 'mh': mh,
                    'si_min': si_min, 'ai_min': ai_min, 'ai_max': ai_max,
                    'mc_max': mc_max, 'mc_min': mc_min, 'ti_min': ti_min, 'mi_min': mi_min,
                    'n': n, 'wr': wr, 'daily_avg': daily_avg,
                    'ev_pct': ev_pct, 'total_pnl': total_pnl,
                    'monthly_return': monthly_return_pct,
                    'by_date': dict(by_date),
                    'indices': filtered_indices,
                })

        if (ex_idx + 1) % 5 == 0:
            print(f'  完成 {ex_idx+1}/{len(exit_grid)} 退出策略...')

    print(f'\n总搜索: {combos} 组合')

    # ─── 多目标排序: 综合得分 ────────────────────────────────────────────
    for r in best_results:
        # 综合得分: WR权重40% + EV权重30% + 日均接近15权重15% + 月回报15%
        wr_score = min(r['wr'] / 0.70, 1.0) * 40  # 70% WR = 满分
        ev_score = min(max(r['ev_pct'], 0) / 50, 1.0) * 30  # 50% EV = 满分
        daily_score = max(0, 1 - abs(r['daily_avg'] - 15) / 15) * 15
        mr_score = min(max(r['monthly_return'], 0) / 300, 1.0) * 15
        r['score'] = wr_score + ev_score + daily_score + mr_score

    best_results.sort(key=lambda r: r['score'], reverse=True)

    # ─── 输出 ────────────────────────────────────────────────────────────
    elite = [r for r in best_results if r['wr'] >= 0.65 and 8 <= r['daily_avg'] <= 25]
    good = [r for r in best_results if r['wr'] >= 0.60 and 8 <= r['daily_avg'] <= 25]
    balanced = [r for r in best_results if r['wr'] >= 0.55 and r['ev_pct'] >= 20 and 10 <= r['daily_avg'] <= 25]

    print(f'\n{"=" * 110}')
    print(f'精英 WR≥65% 8-25/天: {len(elite)}  |  优秀 WR≥60%: {len(good)}  |  均衡 WR≥55%+EV≥20%: {len(balanced)}')
    print(f'{"=" * 110}')

    # ─── 按综合得分 TOP 30 ───────────────────────────────────────────────
    top = best_results[:50]
    print(f'\nTOP 50 (综合得分排序)')
    print(f'{"─" * 130}')
    hdr = (f'{"#":>3} {"Score":>5} {"Exit":>10} {"SI":>4} {"AI":>7} {"MC":>8} {"TI":>3} {"MI":>3} '
           f'{"N":>4} {"WR":>6} {"D/天":>5} {"EV%":>7} {"PnL":>8} {"月回%":>6} {"日期分布"}')
    print(hdr)

    for i, r in enumerate(top):
        ai_str = f'{r["ai_min"]}+' if r['ai_max'] == 0 else f'{r["ai_min"]}-{r["ai_max"]}'
        mc_str = f'{r["mc_min"]}-{"∞" if r["mc_max"]>=999 else str(r["mc_max"])}K'
        dates_str = ' '.join(f'{d}:{r["by_date"].get(d,0)}' for d in sorted(r["by_date"]))
        tag = ''
        if r['wr'] >= 0.65: tag = '★'
        elif r['wr'] >= 0.60: tag = '●'
        elif r['ev_pct'] >= 30: tag = '◆'
        print(f'{tag}{i+1:>2} {r["score"]:>5.1f} {r["ex"]:>10} {r["si_min"]:>4} {ai_str:>7} {mc_str:>8} '
              f'{r["ti_min"]:>3} {r["mi_min"]:>3} {r["n"]:>4} {r["wr"]*100:>5.1f}% {r["daily_avg"]:>5.1f} '
              f'{r["ev_pct"]:>+6.1f}% {r["total_pnl"]:>+7.4f} {r["monthly_return"]:>+5.0f}% {dates_str}')

    # ─── 最优策略深度分析 ─────────────────────────────────────────────────
    if not top:
        print('\n没有找到满足条件的策略')
        return

    best = top[0]
    print(f'\n{"=" * 110}')
    print(f'最优策略深度分析 (综合得分: {best["score"]:.1f})')
    print(f'{"=" * 110}')
    print(f'退出: SL={best["sl"]*100:.0f}% | TP1={best["tp1_r"]*100:.0f}%(卖{best["tp1_sell"]*100:.0f}%) | DW={best["dw"]} | MH={best["mh"]}')
    print(f'过滤: SI≥{best["si_min"]} | AI {best["ai_min"]}-{best["ai_max"] if best["ai_max"]>0 else "∞"} | '
          f'MC {best["mc_min"]}-{best["mc_max"] if best["mc_max"]<999 else "∞"}K | TI≥{best["ti_min"]} | MI≥{best["mi_min"]}')

    # 获取详细交易数据
    trades = []
    for idx, r in best['indices']:
        p = prepared[idx]
        trades.append({**r, **{k: p[k] for k in ['sym','ca','ts','date','hour','mc_k','si','ai','ti','mi','vel','pre','has_mc']}})

    wins = [t for t in trades if t['pnl'] > 0]
    losses = [t for t in trades if t['pnl'] <= 0]
    avg_win = sum(t['pct'] for t in wins) / len(wins) if wins else 0
    avg_loss = sum(t['pct'] for t in losses) / len(losses) if losses else 0
    rr = abs(avg_win / avg_loss) if avg_loss else 0

    print(f'\n总笔数: {len(trades)} | WR: {best["wr"]*100:.1f}% ({len(wins)}W/{len(losses)}L)')
    print(f'平均盈利: {avg_win:+.1f}% | 平均亏损: {avg_loss:+.1f}% | 盈亏比: {rr:.2f}x')
    print(f'EV/笔: {best["ev_pct"]:+.1f}% | 总PnL: {best["total_pnl"]:+.4f} SOL')
    print(f'日均 (03/17-18): {best["daily_avg"]:.1f}笔')
    print(f'月预估PnL (3 SOL资本): {best["monthly_return"]:+.0f}% = {best["monthly_return"]*3/100:+.2f} SOL')

    exits = defaultdict(int)
    for t in trades:
        exits[t['ex']] += 1
    print(f'退出分布: {" | ".join(f"{k}:{v}" for k,v in sorted(exits.items(), key=lambda x:-x[1]))}')

    # 日期稳定性
    print(f'\n日期稳定性:')
    by_date = defaultdict(list)
    for t in trades:
        by_date[t['date']].append(t)
    for d in sorted(by_date):
        g = by_date[d]
        g_wr = len([t for t in g if t['pnl']>0]) / len(g) * 100
        g_pnl = sum(t['pnl'] for t in g)
        g_ev = sum(t['pct'] for t in g) / len(g)
        print(f'  {d}: {len(g):>3}笔  WR={g_wr:>4.0f}%  PnL={g_pnl:>+7.4f} SOL  EV={g_ev:>+6.1f}%')

    # 交易明细
    print(f'\n交易明细:')
    print(f'  {"SYM":<12} {"Date":>5} {"H":>3} {"MC":>5} {"SI":>4} {"AI":>4} {"TI":>3} {"MI":>3} {"Peak":>6} {"PnL%":>7} {"Exit"}')
    for t in sorted(trades, key=lambda x: x['pnl'], reverse=True):
        flag = '✅' if t['pnl'] > 0 else '❌'
        print(f'  {flag} {t["sym"]:<10} {t["date"]:>5} {t["hour"]:02d}  '
              f'{t["mc_k"]:>4.0f}K {t["si"]:>4.0f} {t["ai"]:>4.0f} {t["ti"]:>3.0f} {t["mi"]:>3.0f} '
              f'{t["peak"]:>+5.0f}% {t["pct"]:>+6.1f}%  {t["ex"]}')

    # ─── 对比分析: 当前 v19 vs 最优 ─────────────────────────────────────
    print(f'\n{"=" * 110}')
    print(f'对比: 当前v19 vs 最优策略')
    print(f'{"=" * 110}')
    sl_str = f"{best['sl']*100:.0f}%"
    tp_str = f"+{best['tp1_r']*100:.0f}% sell{best['tp1_sell']*100:.0f}%"
    si_str = f">={best['si_min']}"
    ai_hi = best['ai_max'] if best['ai_max'] > 0 else 'inf'
    ai_str = f"{best['ai_min']}-{ai_hi}"
    mc_hi = best['mc_max'] if best['mc_max'] < 999 else 'inf'
    mc_str = f"{best['mc_min']}-{mc_hi}K"
    print(f"  SL:        -15%          vs  {sl_str}")
    print(f"  TP1:       +80% sell60%  vs  {tp_str}")
    print(f"  SI:        >=100         vs  {si_str}")
    print(f"  AI:        >=40          vs  {ai_str}")
    print(f"  MC:        <30K          vs  {mc_str}")

    # 保存
    output = {
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'best_strategy': {
            'sl': best['sl'], 'tp1_r': best['tp1_r'], 'tp1_sell': best['tp1_sell'],
            'dw': best['dw'], 'mh': best['mh'],
            'si_min': best['si_min'], 'ai_min': best['ai_min'], 'ai_max': best['ai_max'],
            'mc_max': best['mc_max'], 'mc_min': best['mc_min'],
            'ti_min': best['ti_min'], 'mi_min': best['mi_min'],
            'n': best['n'], 'wr': best['wr'], 'daily_avg': best['daily_avg'],
            'ev_pct': best['ev_pct'], 'monthly_return': best['monthly_return'],
        },
        'top_30': [{k: v for k, v in r.items() if k != 'indices'} for r in top[:30]],
    }
    with open('data/strategy-optimization-v2.json', 'w') as f:
        json.dump(output, f, indent=2, default=str)
    print(f'\n结果已保存: data/strategy-optimization-v2.json')


if __name__ == '__main__':
    main()
