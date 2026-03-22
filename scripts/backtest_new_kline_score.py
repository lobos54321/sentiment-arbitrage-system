#!/usr/bin/env python3
"""
Full Backtest: NOT_ATH K-Line Scoring
======================================
New scoring: RED(+2) + lowVol(+1) + active(+1) >= 3
Exit: SL=-3%, trail@+3%, factor=0.90, timeout=120min

数据：
- 信号：zeabur_sentiment.db (NOT_ATH_V17, Mar 7-22)
- K线：kline_cache_backup_20260319.db (Mar 4-19)

注意：super_index 不在信号DB中，无法过滤。
回测使用所有 NOT_ATH_V17 信号。
"""

import sqlite3
from datetime import datetime, timezone
from collections import defaultdict

# ─── Config ───────────────────────────────────────────────────────────────────
KLINE_DB = '/Users/boliu/sentiment-arbitrage-system/data/kline_cache_backup_20260319.db'
SENTIMENT_DB = '/tmp/zeabur_sentiment.db'

SL = -0.03
TRAIL_START = 0.03
TRAIL_FACTOR = 0.90
TIMEOUT = 120  # bars

# ─── Exit Simulation ─────────────────────────────────────────────────────────

def simulate(entry_price, post_bars):
    if not post_bars or entry_price <= 0:
        return None
    peak = 0
    trailing = False
    exit_reason = 'timeout'
    exit_pnl = 0.0
    hold = 0

    for i, bar in enumerate(post_bars):
        price = bar[4] if bar[4] > 0 else bar[1]
        if price <= 0:
            continue
        pnl = (price - entry_price) / entry_price
        peak = max(peak, pnl)
        hold = i + 1

        if not trailing and peak >= TRAIL_START:
            trailing = True

        if not trailing and pnl <= SL:
            return {'pnl': SL, 'reason': 'sl', 'peak': peak, 'hold': hold}

        if trailing:
            trail_level = peak * TRAIL_FACTOR
            if pnl <= trail_level:
                return {'pnl': max(pnl, trail_level), 'reason': 'trail', 'peak': peak, 'hold': hold}

        exit_pnl = pnl

    return {'pnl': exit_pnl, 'reason': exit_reason, 'peak': peak, 'hold': hold}


def compute_score(current, prev3):
    """
    New scoring logic:
    +2 = RED bar (close < open)
    +1 = lowVolume (current.vol <= avg(prev3 vol))
    +1 = active (|mom_from_lag1| > 30%)
    """
    is_red = current[4] < current[1]

    # RED is mandatory
    if not is_red:
        return {'score': None, 'is_red': False, 'low_volume': False, 'is_active': False,
                'mom': 0, 'avg_vol': 0, 'passed': False, 'reason': 'not_red_bar'}

    avg_vol = sum(b[5] for b in prev3) / 3
    low_volume = current[5] <= avg_vol

    mom = ((current[4] - prev3[0][4]) / prev3[0][4] * 100) if prev3[0][4] > 0 else 0
    is_active = abs(mom) > 30

    score = 2 + (1 if low_volume else 0) + (1 if is_active else 0)
    passed = score >= 3

    return {
        'score': score,
        'is_red': True,
        'low_volume': low_volume,
        'is_active': is_active,
        'mom': mom,
        'avg_vol': avg_vol,
        'passed': passed,
        'reason': 'pass' if passed else 'low_score'
    }


def main():
    sentiment_conn = sqlite3.connect(SENTIMENT_DB)
    kline_conn = sqlite3.connect(KLINE_DB)
    sentiment_conn.row_factory = sqlite3.Row
    kline_conn.row_factory = sqlite3.Row

    # Get all NOT_ATH signals
    signals = sentiment_conn.execute("""
        SELECT id, token_ca, symbol, timestamp, market_cap, hard_gate_status
        FROM premium_signals
        WHERE hard_gate_status LIKE 'NOT_ATH%'
          AND timestamp >= 1741305600000  -- Mar 7 2026
        ORDER BY timestamp
    """).fetchall()

    print(f"Total NOT_ATH signals: {len(signals)}")

    # Count by gate status
    from collections import Counter
    gate_counts = Counter(s['hard_gate_status'] for s in signals)
    print(f"By gate status: {dict(gate_counts)}")

    all_results = []
    no_kline = 0
    short_hist = 0
    not_red = 0

    for sig in signals:
        token_ca = sig['token_ca']
        sig_ts_sec = sig['timestamp'] // 1000

        # Get K-lines (descending order)
        klines = kline_conn.execute("""
            SELECT timestamp, open, high, low, close, volume
            FROM kline_1m WHERE token_ca = ? ORDER BY timestamp DESC
        """, (token_ca,)).fetchall()

        if not klines:
            no_kline += 1
            continue
        klines = [list(row) for row in klines]

        # Find entry bar (first bar where timestamp >= sig_ts_sec)
        entry_idx = None
        for i in range(len(klines) - 1, -1, -1):
            if klines[i][0] >= sig_ts_sec:
                entry_idx = i
                break

        if entry_idx is None or entry_idx + 3 >= len(klines):
            short_hist += 1
            continue

        bars_desc = klines[entry_idx:]  # [current, lag1, lag2, ...]
        current = bars_desc[0]
        prev3 = bars_desc[1:4]

        if len(prev3) < 3:
            short_hist += 1
            continue

        # Compute score
        scoring = compute_score(current, prev3)

        # Skip non-RED bars
        if not scoring['is_red']:
            not_red += 1
            continue

        # Simulate exit
        post_bars = list(reversed(bars_desc[1:]))  # ascending time
        if len(post_bars) < 2:
            short_hist += 1
            continue

        entry_price = current[4]
        if entry_price <= 0:
            short_hist += 1
            continue

        sim = simulate(entry_price, post_bars)
        if sim is None:
            short_hist += 1
            continue

        sig_dt = datetime.fromtimestamp(sig_ts_sec, tz=timezone.utc)
        all_results.append({
            'symbol': sig['symbol'] or token_ca[:8],
            'token_ca': token_ca,
            'date': sig_dt.strftime('%m/%d'),
            'hour': sig_dt.hour,
            'gate': sig['hard_gate_status'],
            **scoring,
            **sim
        })

    sentiment_conn.close()
    kline_conn.close()

    print(f"\n过滤统计:")
    print(f"  总信号: {len(signals)}")
    print(f"  无K线: {no_kline}")
    print(f"  历史不足: {short_hist}")
    print(f"  非红K: {not_red}")
    print(f"  有效RED样本: {len(all_results)}")

    if not all_results:
        print("\n无可用样本！")
        return

    # ─── OVERALL ──────────────────────────────────────────────────────────────
    pnls = [r['pnl'] for r in all_results]
    n = len(pnls)
    wins = sum(1 for p in pnls if p > 0)
    total = sum(pnls)
    ev = total / n * 100
    wr = wins / n * 100
    avg_win = sum(p for p in pnls if p > 0) / wins * 100 if wins else 0
    avg_loss = sum(p for p in pnls if p <= 0) / (n - wins) * 100 if n > wins else 0

    exits = Counter(r['reason'] for r in all_results)
    peaks = [r['peak'] for r in all_results]

    print(f"\n{'='*60}")
    print(f"OVERALL (all NOT_ATH + RED bar)")
    print(f"{'='*60}")
    print(f"  N={n} | WR={wr:.1f}% | EV={ev:+.2f}%")
    print(f"  总PnL: {total*100:+.1f}%")
    print(f"  AvgWin={avg_win:+.2f}% | AvgLoss={avg_loss:+.2f}%")
    print(f"  平均峰值: {sum(peaks)/n*100:+.1f}%")
    print(f"  平仓: {' '.join(f'{k}:{v}' for k,v in sorted(exits.items(), key=lambda x:-x[1]))}")

    # ─── BY SCORE ──────────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"BY SCORE (新评分)")
    print(f"{'='*60}")
    for s in [2, 3, 4]:
        g = [r for r in all_results if r['score'] == s]
        if not g:
            continue
        w = sum(1 for r in g if r['pnl'] > 0) / len(g) * 100
        ev_s = sum(r['pnl'] for r in g) / len(g) * 100
        sl = sum(1 for r in g if r['reason'] == 'sl') / len(g) * 100
        exits_s = Counter(r['reason'] for r in g)
        mark = '✅' if ev_s > 15 else ('⚠️' if ev_s > 0 else '❌')
        print(f"  {mark} score={s}: n={len(g):3d} WR={w:5.1f}% EV={ev_s:+7.2f}% SL%={sl:4.0f}% | {' '.join(f'{k}:{v}' for k,v in exits_s.most_common(3))}")

    # ─── SCORE >= 3 (新逻辑通过) vs SCORE < 3 (拒绝) ─────────────────────────
    passed = [r for r in all_results if r['passed']]
    rejected = [r for r in all_results if not r['passed']]

    print(f"\n{'='*60}")
    print(f"NEW LOGIC FILTER (score >= 3)")
    print(f"{'='*60}")
    for label, g in [('通过 (score>=3)', passed), ('拒绝 (score<3)', rejected)]:
        if not g:
            print(f"\n  {label}: n=0")
            continue
        pnls_g = [r['pnl'] for r in g]
        wins_g = sum(1 for p in pnls_g if p > 0)
        wr_g = wins_g / len(g) * 100
        ev_g = sum(pnls_g) / len(g) * 100
        avg_win_g = sum(p for p in pnls_g if p > 0) / wins_g * 100 if wins_g else 0
        avg_loss_g = sum(p for p in pnls_g if p <= 0) / (len(g)-wins_g) * 100 if len(g) > wins_g else 0
        sl_g = sum(1 for r in g if r['reason'] == 'sl') / len(g) * 100
        exits_g = Counter(r['reason'] for r in g)
        top5 = sorted(g, key=lambda x: -x['pnl'])[:5]
        print(f"\n  {label}: n={len(g)} WR={wr_g:.1f}% EV={ev_g:+.2f}% SL%={sl_g:.0f}%")
        print(f"    AvgWin={avg_win_g:+.2f}% | AvgLoss={avg_loss_g:+.2f}%")
        print(f"    平仓: {' '.join(f'{k}:{v}' for k,v in sorted(exits_g.items(), key=lambda x:-x[1]))}")
        print(f"    Top5: " + ' | '.join(f"{r['symbol']}({r['pnl']*100:+.0f}%)" for r in top5))

    # ─── BY RED BAR MOMENTUM (FBR%) ──────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"BY FBR% (RED bar强度)")
    print(f"{'='*60}")
    # RED bar's return %
    for r in all_results:
        r['fbr'] = ((r.get('mom', 0) + 0) * 0 + 0)  # placeholder
        # Actually compute FBR from the bar itself
    # We need fbr from current bar
    # Let me recompute properly

    # ─── Friction Sensitivity ──────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"FRICTION SENSITIVITY (round-trip)")
    print(f"{'='*60}")
    print(f"  {'Friction':>10} {'EV%':>8} {'WR%':>6}")
    for fric in [0, 0.01, 0.02, 0.035, 0.05, 0.07]:
        adj = [(p - fric) for p in pnls]
        ev_f = sum(adj) / len(adj) * 100
        wr_f = sum(1 for p in adj if p > 0) / len(adj) * 100
        print(f"  {fric*100:>10.1f}% {ev_f:>+8.2f}% {wr_f:>6.1f}%")

    # ─── Daily Breakdown ───────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"DAILY BREAKDOWN")
    print(f"{'='*60}")
    by_date = defaultdict(list)
    for r in all_results:
        by_date[r['date']].append(r)
    print(f"  {'Date':>6} {'N':>3} {'WR':>6} {'EV%':>8} {'SL%':>5}")
    for d in sorted(by_date):
        g = by_date[d]
        w = sum(1 for r in g if r['pnl'] > 0) / len(g) * 100
        ev_d = sum(r['pnl'] for r in g) / len(g) * 100
        sl_d = sum(1 for r in g if r['reason'] == 'sl') / len(g) * 100
        print(f"  {d:>6} {len(g):>3} {w:>5.1f}% {ev_d:>+8.2f}% {sl_d:>5.0f}%")

    # ─── Walk-Forward IS/OOS ─────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"WALK-FORWARD (IS vs OOS)")
    print(f"{'='*60}")
    is_res = [r for r in all_results if datetime.strptime(r['date'], '%m/%d').day <= 14]
    oos_res = [r for r in all_results if datetime.strptime(r['date'], '%m/%d').day >= 15]
    for label, g in [('IS (Mar7-14)', is_res), ('OOS (Mar15-19)', oos_res)]:
        if not g:
            continue
        w = sum(1 for r in g if r['pnl'] > 0) / len(g) * 100
        ev = sum(r['pnl'] for r in g) / len(g) * 100
        print(f"  {label}: n={len(g):3d} WR={w:5.1f}% EV={ev:+7.2f}%")

    # ─── Key Findings ─────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"KEY FINDINGS")
    print(f"{'='*60}")
    if passed:
        ev_pass = sum(r['pnl'] for r in passed) / len(passed) * 100
        wr_pass = sum(1 for r in passed if r['pnl'] > 0) / len(passed) * 100
        sl_pass = sum(1 for r in passed if r['reason'] == 'sl') / len(passed) * 100
        print(f"  1. 新逻辑通过 (score>=3): n={len(passed)} WR={wr_pass:.1f}% EV={ev_pass:+.2f}% SL%={sl_pass:.0f}%")
    if rejected:
        ev_rej = sum(r['pnl'] for r in rejected) / len(rejected) * 100
        wr_rej = sum(1 for r in rejected if r['pnl'] > 0) / len(rejected) * 100
        sl_rej = sum(1 for r in rejected if r['reason'] == 'sl') / len(rejected) * 100
        print(f"  2. 新逻辑拒绝 (score<3):  n={len(rejected)} WR={wr_rej:.1f}% EV={ev_rej:+.2f}% SL%={sl_rej:.0f}%")
    if passed and rejected:
        diff = ev_pass - ev_rej
        print(f"  3. 过滤改善: {diff:+.2f}% ({'更好' if diff > 0 else '更差'})")


if __name__ == '__main__':
    main()