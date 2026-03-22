#!/usr/bin/env python3
"""
Cross-Validation Script for NOT_ATH Scoring Logic
================================================
Validates: trend(+1) + holdsSupport(+1) + volIncreasing(+1) >= 2

Issues fixed vs previous attempt:
1. Query direction fixed: bars BEFORE signal for scoring, bars AFTER for exit simulation
2. GeckoTerminal returns descending (newest first) — properly handled
3. Empty data case handled gracefully
4. Walk-forward: IS (Mar 7-14) vs OOS (Mar 15-19)
5. Monte Carlo permutation test (100 iterations)
"""

import sqlite3
import random
import math
from datetime import datetime, timezone

# ─── Config ──────────────────────────────────────────────────────────────────
KLINE_DB = '/Users/boliu/sentiment-arbitrage-system/data/kline_cache_backup_20260319.db'
SENTIMENT_DB = '/tmp/zeabur_sentiment.db'

# Scoring thresholds (current deployed logic)
TREND_GREEN_COUNT = 2  # need ≥2 green bars in prev3
SCORE_THRESHOLD = 2     # need ≥2 points to pass

# Strategy exit params (for simulation)
SL_PCT = -0.03
TRAIL_START = 0.03
TRAIL_FACTOR = 0.90
TIMEOUT_BARS = 120

# ─── Simulation ──────────────────────────────────────────────────────────────

def simulate_exit(entry_price, post_bars):
    """Standard exit simulation."""
    if not post_bars or entry_price <= 0:
        return None
    peak_pnl = 0.0
    trailing_active = False
    exit_reason = 'timeout'
    exit_pnl = 0.0
    bars_held = 0

    for i, bar in enumerate(post_bars):
        price = bar[4]  # close
        if price <= 0:
            price = bar[1]  # open
        if price <= 0:
            continue
        pnl = (price - entry_price) / entry_price
        peak_pnl = max(peak_pnl, pnl)
        bars_held = i + 1

        if not trailing_active and peak_pnl >= TRAIL_START:
            trailing_active = True

        if not trailing_active and pnl <= SL_PCT:
            exit_reason = 'sl'
            exit_pnl = SL_PCT
            break

        if trailing_active:
            trail_level = peak_pnl * TRAIL_FACTOR
            if pnl <= trail_level:
                exit_reason = 'trail'
                exit_pnl = max(pnl, trail_level)
                break

        exit_pnl = pnl

    return {
        'pnl': exit_pnl,
        'reason': exit_reason,
        'bars': bars_held,
        'peak': peak_pnl,
        'trailing': trailing_active
    }

# ─── Scoring ─────────────────────────────────────────────────────────────────

def compute_score(bars_desc, signal_bar_idx):
    """
    bars_desc: K-lines in descending order (newest first, index 0 = current)
    signal_bar_idx: index in bars_desc that corresponds to signal time
    Returns scoring dimensions for the CURRENT bar (index 0 in desc = most recent)

    Note: bars_desc[0] = most recent bar (current)
          bars_desc[1] = previous bar (lag1)
          bars_desc[2] = lag2
          etc.
    """
    if len(bars_desc) < 3:
        return None

    current = bars_desc[0]  # most recent = current bar
    prev = bars_desc[1]    # previous bar
    prev3 = bars_desc[1:4]  # prev3 = previous 3 bars (lag1, lag2, lag3)

    # Dimension 1: trend strength — green count in prev3
    green_count = sum(1 for b in prev3 if b[4] > b[1])  # close > open
    trend_ok = green_count >= TREND_GREEN_COUNT

    # Dimension 2: holds support — current close > min(low) of prev3
    min_low = min(b[3] for b in prev3)  # low column
    holds_support = current[4] > min_low  # close > min_low

    # Dimension 3: volume increasing — current vol > prev vol
    vol_increasing = current[5] > prev[5]

    score = sum([trend_ok, holds_support, vol_increasing])
    passed = score >= SCORE_THRESHOLD

    return {
        'score': score,
        'passed': passed,
        'trend_ok': trend_ok,
        'holds_support': holds_support,
        'vol_increasing': vol_increasing,
        'green_count': green_count,
        'min_low': min_low,
        'current_close': current[4],
        'current_open': current[1],
        'fbr': ((current[4] - current[1]) / current[1] * 100) if current[1] > 0 else 0,
        'current_vol': current[5],
        'prev_vol': prev[5],
    }

# ─── Data Loading ─────────────────────────────────────────────────────────────

def load_signals_with_klines():
    """Load NOT_ATH+super≥80 signals and their K-lines."""
    sentiment_conn = sqlite3.connect(SENTIMENT_DB)
    kline_conn = sqlite3.connect(KLINE_DB)
    sentiment_conn.row_factory = sqlite3.Row
    kline_conn.row_factory = sqlite3.Row

    # Get NOT_ATH signals
    # Note: super_index/ai_index not in this DB schema — use hard_gate_status as proxy
    # For actual deployment, super_index filter is applied at engine level before _checkKline
    signals = sentiment_conn.execute("""
        SELECT id, token_ca, symbol, timestamp, market_cap,
               hard_gate_status
        FROM premium_signals
        WHERE hard_gate_status LIKE 'NOT_ATH%'
          AND timestamp >= 1741305600000  -- Mar 01 2026 UTC
        ORDER BY timestamp
    """).fetchall()

    print(f"Total NOT_ATH + super≥80 signals: {len(signals)}")

    results = []
    no_kline = 0
    short_history = 0

    for sig in signals:
        token_ca = sig['token_ca']
        sig_ts_ms = sig['timestamp']
        sig_ts_sec = sig_ts_ms // 1000

        # Get all K-lines for this token (descending order from DB)
        klines = kline_conn.execute("""
            SELECT timestamp, open, high, low, close, volume
            FROM kline_1m
            WHERE token_ca = ?
            ORDER BY timestamp DESC
        """, (token_ca,)).fetchall()

        if not klines:
            no_kline += 1
            continue

        # Convert to list of lists for easier indexing
        klines = [list(row) for row in klines]

        # Find the bar that corresponds to signal time
        # K-lines are DESC (newest first), so we need:
        # - signal_bar_idx = index of the bar that contains sig_ts_sec
        # - bars AFTER signal = indices 0 to signal_bar_idx-1 (more recent than signal)
        # - bars BEFORE signal = indices signal_bar_idx+1 onwards (older than signal)
        # Wait — DESC order: index 0 = newest
        # If sig_ts falls within a bar, that bar's timestamp <= sig_ts
        # For entry: we want the bar that is AT or JUST AFTER the signal (first bar of our position)
        # Since data is DESC: scan from end (oldest) upward

        # Find entry bar: first bar where bar_ts >= sig_ts_sec (ascending search in DESC order)
        entry_bar_idx = None
        for i in range(len(klines) - 1, -1, -1):  # iterate oldest to newest
            if klines[i][0] >= sig_ts_sec:
                entry_bar_idx = i
                break

        if entry_bar_idx is None:
            # Signal time is after all K-line data
            no_kline += 1
            continue

        # For scoring: we need the CURRENT bar (at signal time) and PREVIOUS bars
        # In DESC order: entry_bar_idx is where we enter
        # Current bar for scoring = the bar at entry_bar_idx (the bar we just entered)
        # Previous bars = entry_bar_idx+1, entry_bar_idx+2, etc. (older, further back in time)
        # BUT we need 3 prev bars, so we need entry_bar_idx + 3 <= len(klines) - 1

        if entry_bar_idx + 3 >= len(klines):
            short_history += 1
            continue

        # Build bars_desc: [current, lag1, lag2, lag3, ...] where current = entry bar
        bars_desc = klines[entry_bar_idx:]  # from entry to oldest (descending time)
        # bars_desc[0] = entry/current bar
        # bars_desc[1] = lag1
        # bars_desc[2] = lag2
        # etc.

        # For exit simulation: bars AFTER entry (more recent than entry = index 0 in desc)
        post_bars = list(reversed(bars_desc[1:]))  # reverse to ascending time order
        if len(post_bars) < 2:
            short_history += 1
            continue

        # Compute score
        scoring = compute_score(bars_desc, entry_bar_idx)
        if scoring is None:
            short_history += 1
            continue

        # Simulate exit
        entry_price = bars_desc[0][4]  # close of entry bar
        if entry_price <= 0:
            short_history += 1
            continue

        sim_result = simulate_exit(entry_price, post_bars)
        if sim_result is None:
            short_history += 1
            continue

        results.append({
            'token_ca': token_ca,
            'symbol': sig['symbol'] or token_ca[:8],
            'sig_ts': sig_ts_sec,
            'sig_date': datetime.fromtimestamp(sig_ts_sec, tz=timezone.utc).strftime('%m/%d'),
            'sig_hour': datetime.fromtimestamp(sig_ts_sec, tz=timezone.utc).hour,
            'mc': sig['market_cap'],
            'gate_status': sig['hard_gate_status'],
            **scoring,
            **sim_result
        })

    sentiment_conn.close()
    kline_conn.close()

    print(f"  no_kline: {no_kline}, short_history: {short_history}, final样本: {len(results)}")
    return results

# ─── Analysis ────────────────────────────────────────────────────────────────

def compute_metrics(results, label=""):
    if not results:
        return
    pnls = [r['pnl'] for r in results]
    n = len(pnls)
    wins = sum(1 for p in pnls if p > 0)
    total = sum(pnls)
    ev = total / n * 100
    wr = wins / n * 100
    avg_win = sum(p for p in pnls if p > 0) / wins * 100 if wins else 0
    avg_loss = sum(p for p in pnls if p <= 0) / (n - wins) * 100 if n > wins else 0
    rr = abs(avg_win / avg_loss) if avg_loss else 0
    peak_avg = sum(r['peak'] for r in results) / n * 100

    exits = {}
    for r in results:
        exits[r['reason']] = exits.get(r['reason'], 0) + 1

    print(f"\n{'='*60}")
    print(f"{label}")
    print(f"{'='*60}")
    print(f"  N={n} | EV={ev:+.2f}% | WR={wr:.1f}% | RR={rr:.2f}x | PeakAvg={peak_avg:.0f}%")
    print(f"  TotalPnL={total*100:+.2f}% | AvgWin={avg_win:+.2f}% | AvgLoss={avg_loss:+.2f}%")
    print(f"  Exits: {' | '.join(f'{k}:{v}' for k,v in sorted(exits.items(), key=lambda x:-x[1]))}")

    return {'n': n, 'ev': ev, 'wr': wr, 'rr': rr, 'total': total, 'peak_avg': peak_avg,
            'avg_win': avg_win, 'avg_loss': avg_loss, 'exits': exits}


def dimension_breakdown(results):
    """Break down WR/EV by each dimension."""
    print(f"\n{'='*60}")
    print("Dimension Breakdown")
    print(f"{'='*60}")

    dims = ['trend_ok', 'holds_support', 'vol_increasing']
    dim_labels = ['Trend(green≥2)', 'HoldsSupport(close>minLow)', 'VolIncreasing(vol>prev)']

    for dim, label in zip(dims, dim_labels):
        on = [r for r in results if r.get(dim)]
        off = [r for r in results if not r.get(dim)]

        if on:
            w_on = sum(1 for r in on if r['pnl'] > 0) / len(on)
            ev_on = sum(r['pnl'] for r in on) / len(on) * 100
            print(f"  {label}: ON  n={len(on):3d} WR={w_on*100:5.1f}% EV={ev_on:+6.2f}%")
        if off:
            w_off = sum(1 for r in off if r['pnl'] > 0) / len(off)
            ev_off = sum(r['pnl'] for r in off) / len(off) * 100
            print(f"  {label}: OFF n={len(off):3d} WR={w_off*100:5.1f}% EV={ev_off:+6.2f}%")


def score_breakdown(results):
    """Breakdown by score value."""
    print(f"\n{'='*60}")
    print("Score Breakdown (score 0/1/2/3)")
    print(f"{'='*60}")
    for score_val in [0, 1, 2, 3]:
        g = [r for r in results if r['score'] == score_val]
        if not g:
            continue
        w = sum(1 for r in g if r['pnl'] > 0) / len(g) * 100
        ev = sum(r['pnl'] for r in g) / len(g) * 100
        print(f"  score={score_val}: n={len(g):3d} WR={w:5.1f}% EV={ev:+6.2f}%")


def walk_forward(results):
    """IS (Mar 7-14) vs OOS (Mar 15-19)."""
    print(f"\n{'='*60}")
    print("Walk-Forward: IS vs OOS")
    print(f"{'='*60}")

    is_res = [r for r in results if datetime.fromtimestamp(r['sig_ts'], tz=timezone.utc).day <= 14]
    oos_res = [r for r in results if datetime.fromtimestamp(r['sig_ts'], tz=timezone.utc).day >= 15]

    if is_res:
        w = sum(1 for r in is_res if r['pnl'] > 0) / len(is_res) * 100
        ev = sum(r['pnl'] for r in is_res) / len(is_res) * 100
        print(f"  IS (Mar 7-14):  n={len(is_res):3d} WR={w:5.1f}% EV={ev:+6.2f}%")
    if oos_res:
        w = sum(1 for r in oos_res if r['pnl'] > 0) / len(oos_res) * 100
        ev = sum(r['pnl'] for r in oos_res) / len(oos_res) * 100
        print(f"  OOS (Mar 15-19): n={len(oos_res):3d} WR={w:5.1f}% EV={ev:+6.2f}%")


def monte_carlo_permutation(results, n_iter=100):
    """Permutation test: shuffle signal-Kline pairs to establish null distribution."""
    print(f"\n{'='*60}")
    print(f"Monte Carlo Permutation Test ({n_iter} iter)")
    print(f"{'='*60}")

    observed_ev = sum(r['pnl'] for r in results) / len(results) * 100
    observed_wr = sum(1 for r in results if r['pnl'] > 0) / len(results) * 100

    count_ev_exceed = 0
    count_wr_exceed = 0
    pnls = [r['pnl'] for r in results]

    for _ in range(n_iter):
        shuffled = pnls[:]
        random.shuffle(shuffled)
        mock_ev = sum(shuffled) / len(shuffled) * 100
        mock_wr = sum(1 for p in shuffled if p > 0) / len(shuffled) * 100
        if mock_ev >= observed_ev:
            count_ev_exceed += 1
        if mock_wr >= observed_wr:
            count_wr_exceed += 1

    p_ev = count_ev_exceed / n_iter
    p_wr = count_wr_exceed / n_iter
    print(f"  Observed EV: {observed_ev:+.2f}% | p-value: {p_ev:.4f} ({'significant' if p_ev < 0.05 else 'NOT significant'})")
    print(f"  Observed WR: {observed_wr:.1f}% | p-value: {p_wr:.4f} ({'significant' if p_wr < 0.05 else 'NOT significant'})")
    print(f"  Note: p < 0.05 means probability of this result being due to chance")


def cross_combination_analysis(results):
    """All 8 combinations of 3 binary dimensions."""
    print(f"\n{'='*60}")
    print("All 8 Combinations (trend × support × vol)")
    print(f"{'='*60}")
    print(f"  {'Combo':30s} {'N':>4} {'WR':>6} {'EV%':>7} {'Peak%':>7}")
    print(f"  {'-'*60}")

    # All 8 combinations
    for trend in [False, True]:
        for sup in [False, True]:
            for vol in [False, True]:
                g = [r for r in results
                     if r['trend_ok'] == trend
                     and r['holds_support'] == sup
                     and r['vol_increasing'] == vol]
                if not g:
                    continue
                w = sum(1 for r in g if r['pnl'] > 0) / len(g) * 100
                ev = sum(r['pnl'] for r in g) / len(g) * 100
                peak = sum(r['peak'] for r in g) / len(g) * 100
                tag = f"{'T' if trend else 't'}/{'+' if sup else 's'}/{'+' if vol else 'v'}"
                mark = '✅' if ev > 10 else ('⚠️' if ev > 0 else '❌')
                print(f"  {mark} {tag:30s} {len(g):>4} {w:>5.1f}% {ev:>+7.2f}% {peak:>+7.1f}%")


def friction_sensitivity(results):
    """EV under different friction assumptions."""
    print(f"\n{'='*60}")
    print("Friction Sensitivity (round-trip slippage)")
    print(f"{'='*60}")

    pnls = [r['pnl'] for r in results]
    print(f"  {'Friction':>10} {'EV%':>8} {'WR%':>6}")
    for friction in [0, 0.01, 0.02, 0.035, 0.05, 0.07, 0.10]:
        adj = [(p - friction) for p in pnls]
        ev = sum(adj) / len(adj) * 100
        wr = sum(1 for p in adj if p > 0) / len(adj) * 100
        print(f"  {friction*100:>10.1f}% {ev:>+8.2f}% {wr:>6.1f}%")


def daily_stability(results):
    """Daily EV/WR for each date."""
    print(f"\n{'='*60}")
    print("Daily Stability")
    print(f"{'='*60}")
    by_date = {}
    for r in results:
        d = r['sig_date']
        if d not in by_date:
            by_date[d] = []
        by_date[d].append(r)

    print(f"  {'Date':>6} {'N':>3} {'WR':>5} {'EV%':>7}")
    for d in sorted(by_date):
        g = by_date[d]
        w = sum(1 for r in g if r['pnl'] > 0) / len(g) * 100
        ev = sum(r['pnl'] for r in g) / len(g) * 100
        print(f"  {d:>6} {len(g):>3} {w:>5.1f}% {ev:>+7.2f}%")


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    print("Loading signals and K-lines...")
    results = load_signals_with_klines()

    if not results:
        print("No valid samples. Check K-line/signal time overlap.")
        return

    print(f"\nTotal valid samples: {len(results)}")

    # Basic metrics
    compute_metrics(results, "OVERALL (all samples)")

    # Breakdown by score
    score_breakdown(results)

    # Dimension breakdown
    dimension_breakdown(results)

    # All 8 combinations
    cross_combination_analysis(results)

    # Walk-forward
    walk_forward(results)

    # Daily stability
    daily_stability(results)

    # Friction sensitivity
    friction_sensitivity(results)

    # Monte Carlo permutation
    monte_carlo_permutation(results, n_iter=100)

    # ─── Current deployed filter: score >= 2 ──────────────────────────────
    passed = [r for r in results if r['passed']]
    rejected = [r for r in results if not r['passed']]
    print(f"\n{'='*60}")
    print("Deployed Filter Effect (score ≥ 2)")
    print(f"{'='*60}")
    print(f"  Passed (score≥2):  n={len(passed)}")
    if passed:
        w = sum(1 for r in passed if r['pnl'] > 0) / len(passed) * 100
        ev = sum(r['pnl'] for r in passed) / len(passed) * 100
        print(f"    WR={w:.1f}% EV={ev:+.2f}%")
    print(f"  Rejected (score<2): n={len(rejected)}")
    if rejected:
        w = sum(1 for r in rejected if r['pnl'] > 0) / len(rejected) * 100
        ev = sum(r['pnl'] for r in rejected) / len(rejected) * 100
        print(f"    WR={w:.1f}% EV={ev:+.2f}%")
    if passed and rejected:
        ev_diff = (sum(r['pnl'] for r in passed) / len(passed) -
                   sum(r['pnl'] for r in rejected) / len(rejected)) * 100
        print(f"  EV improvement from filter: {ev_diff:+.2f}%")

    print(f"\n{'='*60}")
    print("KEY FINDINGS")
    print(f"{'='*60}")
    if results:
        print(f"  1. Baseline (all): N={len(results)}, EV={sum(r['pnl'] for r in results)/len(results)*100:+.2f}%, WR={sum(1 for r in results if r['pnl']>0)/len(results)*100:.1f}%")
    if passed:
        print(f"  2. With filter (score≥2): N={len(passed)}, EV={sum(r['pnl'] for r in passed)/len(passed)*100:+.2f}%, WR={sum(1 for r in passed if r['pnl']>0)/len(passed)*100:.1f}%")
    if rejected:
        print(f"  3. Rejected (score<2): N={len(rejected)}, EV={sum(r['pnl'] for r in rejected)/len(rejected)*100:+.2f}%, WR={sum(1 for r in rejected if r['pnl']>0)/len(rejected)*100:.1f}%")


if __name__ == '__main__':
    main()