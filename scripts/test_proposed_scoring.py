#!/usr/bin/env python3
"""
Proposed New Scoring for NOT_ATH
================================
Based on cross-validation findings:

Current filter HURTS: score≥2 → WR=17.4%, EV=+14.29%
Current REJECTED are BEST: score<2 → WR=53.5%, EV=+54.72%

Key insight: the scoring is selecting coins that immediately reverse (82.6% SL rate!)
The best performers (JACK +773%, 柯基 +284%) have:
  - RED bar at entry (price pullback after initial pump)
  - MILD prior momentum (not extended rallies)
  - LOW or DECLINING volume (smart money accumulation, not distribution)
  - Mostly broke support (sup=False is better!)

Proposed new scoring:
  +2 = RED bar (close < open) — pullback confirmation
  +1 = mom_5bar < 30% (not an extended rally)
  +1 = vol <= avg_vol_3 (NOT increasing = accumulation, not FOMO)
  +1 = (OPTIONAL) broke_support = True (retest of support = entry)
  Threshold: ≥2 to pass

This is the OPPOSITE of the current logic!
"""

import sqlite3
from datetime import datetime, timezone

KLINE_DB = '/Users/boliu/sentiment-arbitrage-system/data/kline_cache_backup_20260319.db'
SENTIMENT_DB = '/tmp/zeabur_sentiment.db'

def simulate_exit(entry_price, post_bars, SL=-0.03, TRAIL_START=0.03, TRAIL_FACTOR=0.90):
    if not post_bars or entry_price <= 0:
        return None
    peak_pnl = 0.0
    trailing_active = False
    exit_reason = 'timeout'
    exit_pnl = 0.0
    for i, bar in enumerate(post_bars):
        price = bar[4]
        if price <= 0:
            price = bar[1]
        if price <= 0:
            continue
        pnl = (price - entry_price) / entry_price
        peak_pnl = max(peak_pnl, pnl)
        if not trailing_active and peak_pnl >= TRAIL_START:
            trailing_active = True
        if not trailing_active and pnl <= SL:
            return {'pnl': SL, 'reason': 'sl', 'peak': peak_pnl}
        if trailing_active:
            trail_level = peak_pnl * TRAIL_FACTOR
            if pnl <= trail_level:
                return {'pnl': max(pnl, trail_level), 'reason': 'trail', 'peak': peak_pnl}
        exit_pnl = pnl
    return {'pnl': exit_pnl, 'reason': exit_reason, 'peak': peak_pnl}


def compute_proposed_score(current, prev3, bars_desc):
    """Proposed new scoring logic for NOT_ATH entry."""
    # +2 RED bar (close < open = pullback confirmation)
    is_red = current[4] < current[1]

    # Momentum: return from lag1 to current
    mom = ((current[4] - prev3[0][4]) / prev3[0][4] * 100) if prev3[0][4] > 0 else 0
    # +1 if mom < 30% (not extended)
    not_extended = abs(mom) < 30

    # Volume: current vs avg of prev3
    avg_vol = sum(b[5] for b in prev3) / 3
    vol_not_increasing = current[5] <= avg_vol  # +1 if NOT increasing

    # Support: did it break below prev3 min low? (retest entry)
    min_low = min(b[3] for b in prev3)
    broke_support = current[4] < min_low  # +1 if broke support (retest entry)

    # FBR for reference
    fbr = ((current[4] - current[1]) / current[1] * 100) if current[1] > 0 else 0

    # Proposed score
    score = 0
    if is_red: score += 2
    if not_extended: score += 1
    if vol_not_increasing: score += 1

    return {
        'score': score,
        'is_red': is_red,
        'mom': mom,
        'not_extended': not_extended,
        'avg_vol': avg_vol,
        'vol_not_increasing': vol_not_increasing,
        'broke_support': broke_support,
        'fbr': fbr,
        'current_vol': current[5],
    }


def main():
    sentiment_conn = sqlite3.connect(SENTIMENT_DB)
    kline_conn = sqlite3.connect(KLINE_DB)
    sentiment_conn.row_factory = sqlite3.Row
    kline_conn.row_factory = sqlite3.Row

    signals = sentiment_conn.execute("""
        SELECT id, token_ca, symbol, timestamp, market_cap, hard_gate_status
        FROM premium_signals
        WHERE hard_gate_status LIKE 'NOT_ATH%'
          AND timestamp >= 1741305600000
        ORDER BY timestamp
    """).fetchall()

    all_results = []

    for sig in signals:
        token_ca = sig['token_ca']
        sig_ts_ms = sig['timestamp']
        sig_ts_sec = sig_ts_ms // 1000

        klines = kline_conn.execute("""
            SELECT timestamp, open, high, low, close, volume
            FROM kline_1m WHERE token_ca = ? ORDER BY timestamp DESC
        """, (token_ca,)).fetchall()
        if not klines:
            continue
        klines = [list(row) for row in klines]

        entry_bar_idx = None
        for i in range(len(klines) - 1, -1, -1):
            if klines[i][0] >= sig_ts_sec:
                entry_bar_idx = i
                break
        if entry_bar_idx is None or entry_bar_idx + 3 >= len(klines):
            continue

        bars_desc = klines[entry_bar_idx:]
        if len(bars_desc) < 4:
            continue

        current = bars_desc[0]
        prev3 = bars_desc[1:4]

        post_bars = list(reversed(bars_desc[1:]))
        entry_price = current[4]
        if entry_price <= 0 or len(post_bars) < 2:
            continue

        sim = simulate_exit(entry_price, post_bars)
        if sim is None:
            continue

        proposed = compute_proposed_score(current, prev3, bars_desc)

        all_results.append({
            'token_ca': token_ca,
            'symbol': sig['symbol'] or token_ca[:8],
            'sig_date': datetime.fromtimestamp(sig_ts_sec, tz=timezone.utc).strftime('%m/%d'),
            **proposed,
            **sim
        })

    sentiment_conn.close()
    kline_conn.close()

    print(f"Total samples: {len(all_results)}")

    # ─── Test proposed scoring at different thresholds ────────────────────────
    print(f"\n{'='*60}")
    print("PROPOSED NEW SCORING: RED(+2) + NOT_EXTENDED(+1) + LOW_VOL(+1)")
    print(f"{'='*60}")

    for threshold in [2, 3, 4]:
        passed = [r for r in all_results if r['score'] >= threshold]
        if not passed:
            continue
        pnls = [r['pnl'] for r in passed]
        wins = sum(1 for p in pnls if p > 0)
        wr = wins / len(pnls) * 100
        ev = sum(pnls) / len(pnls) * 100
        exits = {}
        for r in passed:
            exits[r['reason']] = exits.get(r['reason'], 0) + 1
        print(f"\n  threshold >= {threshold}: n={len(passed)} WR={wr:.1f}% EV={ev:+.2f}%")
        print(f"    Exits: {' '.join(f'{k}:{v}' for k,v in sorted(exits.items(), key=lambda x:-x[1]))}")

    # ─── Detailed breakdown of proposed scoring ───────────────────────────────
    print(f"\n{'='*60}")
    print("Dimension Breakdown (PROPOSED)")
    print(f"{'='*60}")

    dims = [('is_red', 'RED bar(+2)'),
            ('not_extended', 'Not extended mom<30%'),
            ('vol_not_increasing', 'Vol NOT increasing')]
    for dim_key, label in dims:
        on = [r for r in all_results if r.get(dim_key)]
        off = [r for r in all_results if not r.get(dim_key)]
        if on:
            w = sum(1 for r in on if r['pnl'] > 0) / len(on) * 100
            ev = sum(r['pnl'] for r in on) / len(on) * 100
            sl_rate = sum(1 for r in on if r['reason'] == 'sl') / len(on) * 100
            print(f"  {label}: ON  n={len(on):3d} WR={w:5.1f}% EV={ev:+6.2f}% SL%={sl_rate:.0f}%")
        if off:
            w = sum(1 for r in off if r['pnl'] > 0) / len(off) * 100
            ev = sum(r['pnl'] for r in off) / len(off) * 100
            sl_rate = sum(1 for r in off if r['reason'] == 'sl') / len(off) * 100
            print(f"  {label}: OFF n={len(off):3d} WR={w:5.1f}% EV={ev:+6.2f}% SL%={sl_rate:.0f}%")

    # ─── All combinations of proposed dimensions ───────────────────────────────
    print(f"\n{'='*60}")
    print("All 8 Combinations (PROPOSED)")
    print(f"{'='*60}")
    print(f"  {'Combo':30s} {'N':>4} {'WR':>6} {'EV%':>7} {'SL%':>5} {'Peak%':>7}")
    print(f"  {'-'*65}")

    for red in [False, True]:
        for extended in [False, True]:
            for vol in [False, True]:
                g = [r for r in all_results
                     if r['is_red'] == red
                     and r['not_extended'] == extended
                     and r['vol_not_increasing'] == vol]
                if not g:
                    continue
                w = sum(1 for r in g if r['pnl'] > 0) / len(g) * 100
                ev = sum(r['pnl'] for r in g) / len(g) * 100
                sl = sum(1 for r in g if r['reason'] == 'sl') / len(g) * 100
                peak = sum(r['peak'] for r in g) / len(g) * 100
                tag = f"{'R' if red else 'r'}/{'E' if extended else 'e'}/{'V' if vol else 'v'}"
                mark = '✅' if ev > 20 else ('⚠️' if ev > 0 else '❌')
                print(f"  {mark} {tag:30s} {len(g):>4} {w:>5.1f}% {ev:>+7.2f}% {sl:>5.0f}% {peak:>+7.1f}%")

    # ─── Compare: Current vs Proposed at threshold=2 ─────────────────────────
    print(f"\n{'='*60}")
    print("COMPARISON: Current vs Proposed (threshold=2)")
    print(f"{'='*60}")

    # Current: trend_ok(+1) + holds_support(+1) + vol_increasing(+1) >= 2
    current_passed = [r for r in all_results if
                       (r.get('green_count', 0) >= 2) +  # using prev results
                       int(r.get('holds_support', False)) +
                       int(r.get('vol_increasing', False)) >= 2]

    # Proposed: is_red(+2) + not_extended(+1) + vol_not_increasing(+1) >= 2
    proposed_passed = [r for r in all_results if r['score'] >= 2]

    for label, results in [('CURRENT (trend+sup+vol_inc)', current_passed),
                            ('PROPOSED (red+not_ext+low_vol)', proposed_passed)]:
        if not results:
            print(f"\n  {label}: No samples")
            continue
        pnls = [r['pnl'] for r in results]
        wins = sum(1 for p in pnls if p > 0)
        wr = wins / len(pnls) * 100
        ev = sum(pnls) / len(pnls) * 100
        sl_rate = sum(1 for r in results if r['reason'] == 'sl') / len(results) * 100
        exits = {}
        for r in results:
            exits[r['reason']] = exits.get(r['reason'], 0) + 1
        print(f"\n  {label}: n={len(results)} WR={wr:.1f}% EV={ev:+.2f}% SL%={sl_rate:.0f}%")
        print(f"    Exits: {' '.join(f'{k}:{v}' for k,v in sorted(exits.items(), key=lambda x:-x[1]))}")

    # ─── Most important: RED bar analysis ───────────────────────────────────
    print(f"\n{'='*60}")
    print("RED BAR = Entry confirmation (most important signal)")
    print(f"{'='*60}")
    red_bar = [r for r in all_results if r['is_red']]
    green_bar = [r for r in all_results if not r['is_red']]
    for label, g in [('RED bars', red_bar), ('GREEN bars', green_bar)]:
        if not g:
            continue
        w = sum(1 for r in g if r['pnl'] > 0) / len(g) * 100
        ev = sum(r['pnl'] for r in g) / len(g) * 100
        sl = sum(1 for r in g if r['reason'] == 'sl') / len(g) * 100
        top5 = sorted(g, key=lambda x: -x['pnl'])[:5]
        print(f"\n  {label}: n={len(g)} WR={w:.1f}% EV={ev:+.2f}% SL%={sl:.0f}%")
        print(f"    Top 5: " + ' | '.join(f"{r['symbol']}({r['pnl']*100:+.0f}%)" for r in top5))


if __name__ == '__main__':
    main()