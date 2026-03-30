#!/usr/bin/env python3
"""
Backtest Stage3 entry with additional super_index_growth >= 30 filter.

For each Stage3 entry in the time window, looks up:
  - signal-time super index (from New Trending signal in premium_signals)
  - ATH signals for the same token BEFORE stage3 entry time
  - super_index_current at stage3 entry
  - checks if (current - signal) >= threshold

Usage:
  python3 scripts/backtest_stage3_superindex.py
  python3 scripts/backtest_stage3_superindex.py --threshold 20 --hours 24
"""

import sqlite3
import json
import re
import os
import sys
import argparse
from pathlib import Path
from datetime import datetime, timezone

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
SENTIMENT_DB = os.environ.get('SENTIMENT_DB', str(DATA_DIR / 'sentiment_arb.db'))
PAPER_DB     = os.environ.get('PAPER_DB',     str(DATA_DIR / 'paper_trades.db'))


# ── helpers ──────────────────────────────────────────────────────────────────

def parse_super_index_signal(description):
    """Extract signal-time super index from New Trending description."""
    if not description:
        return None
    text = description.replace('**', '').replace('\r', '')
    text = re.sub(r'[\u200B-\u200D\uFEFF]', '', text)
    # "Super Index： 119🔮" or "✡ x 82"
    m = re.search(r'Super\s+Index[：:]\s*(\d+)\s*🔮?', text)
    if m:
        return int(m.group(1))
    m = re.search(r'Super\s+Index[：:]\s*✡?\s*x\s*(\d+)', text, re.IGNORECASE)
    if m:
        return int(m.group(1))
    return None

def parse_super_index_ath(description):
    """
    Extract (signal_val, current_val) from ATH description.
    Format: "Super Index：(signal)116🔮 --> (current)124🔮"
    Returns (signal_val, current_val) or (None, None).
    """
    if not description:
        return None, None
    text = description.replace('**', '').replace('\r', '')
    text = re.sub(r'[\u200B-\u200D\uFEFF]', '', text)
    m = re.search(
        r'Super\s+Index[：:]\s*[(\uff08]signal[)\uff09]\s*x?(\d+).*?[(\uff08]current[)\uff09]\s*x?(\d+)',
        text, re.IGNORECASE | re.DOTALL
    )
    if m:
        return int(m.group(1)), int(m.group(2))
    # ATH single value (no delta): "Super Index： 124🔮"
    m = re.search(r'Super\s+Index[：:]\s*(\d+)\s*🔮', text)
    if m:
        val = int(m.group(1))
        return None, val   # only current known
    return None, None

def ts_str(ts_sec):
    if not ts_sec:
        return '—'
    return datetime.fromtimestamp(ts_sec, tz=timezone.utc).strftime('%m-%d %H:%M UTC')


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--threshold', type=int, default=30,
                    help='super_index_current - super_index_signal threshold (default 30)')
    ap.add_argument('--hours', type=float, default=3,
                    help='look-back window in hours (default 3)')
    ap.add_argument('--verbose', action='store_true')
    args = ap.parse_args()

    import time
    now = int(time.time())
    window_start = now - int(args.hours * 3600)
    threshold = args.threshold

    pdb = sqlite3.connect(PAPER_DB)
    pdb.row_factory = sqlite3.Row
    sdb = sqlite3.connect(SENTIMENT_DB)
    sdb.row_factory = sqlite3.Row

    # ── 1. Load Stage3 trades in window ──────────────────────────────────────
    stage3_rows = pdb.execute("""
        SELECT id, token_ca, symbol, signal_ts, entry_ts, exit_ts,
               exit_reason, pnl_pct, peak_pnl, first_peak_pct, lifecycle_id
        FROM paper_trades
        WHERE strategy_stage = 'stage3'
          AND entry_ts >= ?
        ORDER BY entry_ts ASC
    """, (window_start,)).fetchall()

    # ── 2. Load all Stage1 entries in wider window (need signal super index) ─
    # signal_ts is in ms for live trades
    wider_start = window_start - 4 * 3600   # stage1 could be up to 4h before stage3
    stage1_rows = pdb.execute("""
        SELECT token_ca, signal_ts, entry_ts, peak_pnl
        FROM paper_trades
        WHERE strategy_stage = 'stage1'
          AND entry_ts >= ?
        ORDER BY entry_ts ASC
    """, (wider_start,)).fetchall()

    # Build lookup: token_ca → list of stage1 {signal_ts, peak_pnl}
    stage1_by_ca = {}
    for r in stage1_rows:
        ca = r['token_ca']
        sig_sec = r['signal_ts'] // 1000 if r['signal_ts'] > 1e12 else r['signal_ts']
        stage1_by_ca.setdefault(ca, []).append({
            'signal_ts_sec': sig_sec,
            'peak_pnl': float(r['peak_pnl'] or 0),
        })

    # ── 3. Load premium_signals for New Trending (super index at signal time) ─
    sig_rows = sdb.execute("""
        SELECT token_ca, timestamp, description, hard_gate_status
        FROM premium_signals
        WHERE timestamp >= ?
          AND description LIKE '%New Trending%'
        ORDER BY timestamp ASC
    """, (wider_start * 1000,)).fetchall()

    # token_ca → super_index at signal time
    signal_super = {}
    for r in sig_rows:
        ca = r['token_ca']
        si = parse_super_index_signal(r['description'])
        if si and ca not in signal_super:
            signal_super[ca] = si

    # ── 4. Load ATH signals (current super index) ────────────────────────────
    ath_rows = sdb.execute("""
        SELECT token_ca, timestamp, description
        FROM premium_signals
        WHERE timestamp >= ?
          AND (description LIKE '%ATH%' OR source = 'premium_channel_ath')
        ORDER BY timestamp ASC
    """, (wider_start * 1000,)).fetchall()

    # token_ca → list of {ts_sec, signal_val, current_val}
    ath_by_ca = {}
    for r in ath_rows:
        ca = r['token_ca']
        ts_sec = r['timestamp'] // 1000 if r['timestamp'] > 1e12 else r['timestamp']
        sv, cv = parse_super_index_ath(r['description'])
        if cv is not None:
            ath_by_ca.setdefault(ca, []).append({
                'ts_sec': ts_sec,
                'signal_val': sv,
                'current_val': cv,
            })

    # ── 5. Backtest each Stage3 entry ─────────────────────────────────────────
    print(f"\n{'='*80}")
    print(f"Stage3 backtest: last {args.hours}h  |  super_index_growth threshold = +{threshold}")
    print(f"{'='*80}\n")
    print(f"{'Symbol':<12} {'Entry':>15} {'super_sig':>9} {'super_cur':>9} {'delta':>6} {'pass?':>6}  {'PnL':>7}  {'exit_reason'}")
    print(f"{'-'*12} {'-'*15} {'-'*9} {'-'*9} {'-'*6} {'-'*6}  {'-'*7}  {'-'*15}")

    results_all   = []   # without filter
    results_pass  = []   # with filter

    for row in stage3_rows:
        ca        = row['token_ca']
        symbol    = row['symbol']
        entry_ts  = row['entry_ts']
        pnl       = float(row['pnl_pct'] or 0)
        exit_r    = row['exit_reason'] or '?'
        fp        = float(row['first_peak_pct'] or 0)

        results_all.append(pnl)

        # Get signal-time super index
        sig_super = signal_super.get(ca)
        if sig_super is None:
            # Try from stage1 lifecycle
            for s1 in stage1_by_ca.get(ca, []):
                if s1['signal_ts_sec'] < entry_ts:
                    # Lookup in premium_signals directly
                    pass   # already covered above

        # Get best ATH super index BEFORE stage3 entry
        cur_super = None
        ath_sig_from_ath = None
        best_ts_diff = None
        for ath in ath_by_ca.get(ca, []):
            if ath['ts_sec'] <= entry_ts:
                # Use the closest ATH signal before entry
                diff = entry_ts - ath['ts_sec']
                if best_ts_diff is None or diff < best_ts_diff:
                    best_ts_diff = diff
                    cur_super = ath['current_val']
                    if ath['signal_val']:
                        ath_sig_from_ath = ath['signal_val']

        # If ATH signal has its own signal_val, prefer it over New Trending
        if ath_sig_from_ath and sig_super is None:
            sig_super = ath_sig_from_ath

        # Compute delta
        if sig_super is not None and cur_super is not None:
            delta = cur_super - sig_super
            passes = delta >= threshold
        elif cur_super is None:
            delta = None
            passes = None  # unknown — no ATH signals for this token
        else:
            delta = None
            passes = None

        sig_str = str(sig_super) if sig_super else '?'
        cur_str = str(cur_super) if cur_super else '?'
        delta_str = f'+{delta}' if delta is not None and delta >= 0 else (str(delta) if delta is not None else '?')
        pass_str = ('✅ YES' if passes else ('❌ NO' if passes is False else '❓ ?'))

        pnl_str = f'{pnl*100:+.1f}%' if exit_r != 'stage3_entered' else 'open'
        print(f"{symbol:<12} {ts_str(entry_ts):>15} {sig_str:>9} {cur_str:>9} {delta_str:>6} {pass_str:>8}  {pnl_str:>7}  {exit_r}")

        if passes is True:
            results_pass.append(pnl)
        elif passes is None:
            results_pass.append(pnl)   # unknown: include conservatively

    # ── 6. Summary ───────────────────────────────────────────────────────────
    print(f"\n{'─'*80}")

    def stats(label, pnls):
        if not pnls:
            print(f"  {label}: 0 trades")
            return
        wins = [p for p in pnls if p > 0]
        wr   = len(wins) / len(pnls) * 100
        ev   = sum(pnls) / len(pnls) * 100
        print(f"  {label}: {len(pnls)} trades | WR {wr:.0f}% | EV {ev:+.1f}%/trade | total {sum(pnls)*100:+.1f}%")

    print()
    stats("WITHOUT filter (current)", results_all)
    stats(f"WITH super_index +{threshold} filter", results_pass)

    # ── 7. Stage1 eligible tokens that never became Stage3 ───────────────────
    print(f"\n{'─'*80}")
    print(f"\nStage1 tokens with first_peak ≥ 10% in window (Stage3 candidates):\n")
    seen = set()
    for r in pdb.execute("""
        SELECT symbol, token_ca, entry_ts, peak_pnl, exit_reason
        FROM paper_trades
        WHERE strategy_stage = 'stage1'
          AND entry_ts >= ?
          AND peak_pnl >= 0.10
        ORDER BY entry_ts ASC
    """, (window_start,)).fetchall():
        ca = r['token_ca']
        if ca in seen:
            continue
        seen.add(ca)
        sig_super = signal_super.get(ca)
        # latest ATH before signal_ts + 30min
        stage3_window = r['entry_ts'] + 30 * 60
        cur_s = None
        for ath in ath_by_ca.get(ca, []):
            if ath['ts_sec'] <= stage3_window + 300:  # 5min grace
                cur_s = ath['current_val']
        delta = (cur_s - sig_super) if (sig_super and cur_s) else None
        delta_str = f'+{delta}' if delta is not None and delta >= 0 else (str(delta) if delta is not None else '?')
        pass_str = ('✅' if delta is not None and delta >= threshold else ('❌' if delta is not None else '❓'))
        print(f"  {r['symbol']:<12} peak={r['peak_pnl']*100:+.1f}%  super_sig={sig_super or '?':>4}  super_cur={cur_s or '?':>4}  Δ={delta_str:>5}  {pass_str}")

    pdb.close()
    sdb.close()
    print()


if __name__ == '__main__':
    main()
