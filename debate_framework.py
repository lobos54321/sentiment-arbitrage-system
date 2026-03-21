#!/usr/bin/env python3
"""
Unified Debate Framework — shared infrastructure for three-AI strategy analysis.
Provides: data loading, canonical sim(), metrics, quintile analysis, output schema.
"""

import sqlite3
import re
import math
import json
import random
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

# === Canonical Paths ===
SENTIMENT_DB = '/tmp/sentiment.db'
KLINE_DB = str(Path(__file__).parent / 'data' / 'kline_cache.db')

# Canonical date window
START_MS = int(datetime(2026, 3, 13, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
END_MS   = int(datetime(2026, 3, 21, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)


def parse_signal_description(desc):
    """Extract all features from signal description text."""
    if not desc:
        return {}
    d = {}
    # MC
    mc_match = re.search(r'MC[：:]\*?\*?\s*\$?([\d,.]+)([KkMm]?)', desc)
    if mc_match:
        val = float(mc_match.group(1).replace(',', ''))
        s = mc_match.group(2).upper()
        if s == 'K': val *= 1000
        elif s == 'M': val *= 1_000_000
        d['mc'] = val
    # Indices
    for name, pat in [
        ('si', r'Super\s*Index[：:]\s*(\d+)'),
        ('ai', r'AI\s+Index[：:]\s*(\d+)'),
        ('trade', r'Trade\s+Index[：:]\s*(\d+)'),
        ('security', r'Security\s+Index[：:]\s*(\d+)'),
        ('address', r'Address\s+Index[：:]\s*(\d+)'),
        ('sentiment', r'Sentiment\s+Index[：:]\s*(\d+)'),
        ('media', r'Media\s+Index[：:]\s*(\d+)'),
    ]:
        m = re.search(pat, desc)
        if m: d[name] = int(m.group(1))
    # Holders
    m = re.search(r'Holders[：:]\s*([\d,]+)', desc)
    if m: d['holders'] = int(m.group(1).replace(',', ''))
    # Top10
    m = re.search(r'Top10[：:]\s*([\d.]+)%?', desc)
    if m: d['top10'] = float(m.group(1))
    # Vol24H
    m = re.search(r'Vol(?:ume)?\s*24[hH][：:]\s*\$?([\d,.]+)([KkMm]?)', desc)
    if m:
        val = float(m.group(1).replace(',', ''))
        s = m.group(2).upper()
        if s == 'K': val *= 1000
        elif s == 'M': val *= 1_000_000
        d['vol24h'] = val
    # Tx24H
    m = re.search(r'Tx\s*24[hH][：:]\s*([\d,]+)', desc)
    if m: d['tx24h'] = int(m.group(1).replace(',', ''))
    # Age
    m = re.search(r'Age[：:]\s*(\d+)\s*([mhd])', desc, re.IGNORECASE)
    if m:
        val = int(m.group(1))
        unit = m.group(2).lower()
        if unit == 'h': val *= 60
        elif unit == 'd': val *= 1440
        d['age_min'] = val
    # Flags
    d['no_mint'] = 'NoMint' in desc or '✅Mint' in desc
    d['no_blacklist'] = 'NoBlacklist' in desc or '✅Blacklist' in desc
    d['burnt'] = 'Burnt' in desc or '✅Burnt' in desc
    d['has_twitter'] = 'twitter.com' in desc.lower() or 'x.com' in desc.lower()
    d['has_website'] = 'Website' in desc and 'http' in desc
    return d


def load_dataset():
    """Load de-duplicated signals with matched klines.
    Returns list of dicts, one per token, with pre/post bars.
    """
    sdb = sqlite3.connect(SENTIMENT_DB)
    kdb = sqlite3.connect(KLINE_DB)

    # Get first signal per token (de-duplicated)
    signals = sdb.execute("""
        SELECT token_ca, symbol, MIN(timestamp) as ts, description
        FROM premium_signals
        WHERE hard_gate_status = 'NOT_ATH_V17'
          AND timestamp >= ? AND timestamp < ?
        GROUP BY token_ca
        ORDER BY ts
    """, (START_MS, END_MS)).fetchall()

    dataset = []
    for token_ca, symbol, signal_ts_ms, description in signals:
        signal_ts = signal_ts_ms / 1000  # convert to seconds

        # Get klines around signal time
        bars = kdb.execute("""
            SELECT timestamp, open, high, low, close, volume
            FROM kline_1m
            WHERE token_ca = ? AND timestamp >= ? AND timestamp <= ?
            ORDER BY timestamp
        """, (token_ca, signal_ts - 600, signal_ts + 7200 + 60)).fetchall()

        if len(bars) < 10:
            continue

        # Find entry bar (closest to signal time)
        entry_idx = None
        min_diff = float('inf')
        for i, bar in enumerate(bars):
            diff = abs(bar[0] - signal_ts)
            if diff < min_diff:
                min_diff = diff
                entry_idx = i

        if entry_idx is None or entry_idx < 1:
            continue

        entry_bar = bars[entry_idx]
        entry_price = entry_bar[4]  # close
        if entry_price <= 0:
            continue

        # Pre-signal bars (up to 10 before entry)
        bars_pre = bars[max(0, entry_idx - 10):entry_idx]
        # Post-signal bars (up to 120 after entry)
        bars_post = bars[entry_idx:entry_idx + 121]

        if len(bars_post) < 3:
            continue

        # Lag-1 volume
        lag1_vol = bars[entry_idx - 1][5] if entry_idx >= 1 else 0

        meta = parse_signal_description(description or '')

        dataset.append({
            'token_ca': token_ca,
            'symbol': symbol,
            'signal_ts': signal_ts,
            'signal_ts_ms': signal_ts_ms,
            'entry_price': entry_price,
            'entry_bar': entry_bar,  # (ts, o, h, l, c, v)
            'entry_vol': entry_bar[5],
            'lag1_vol': lag1_vol,
            'bars_pre': bars_pre,
            'bars_post': bars_post,
            'meta': meta,
        })

    sdb.close()
    kdb.close()
    return dataset


def sim(post_bars, entry_price, sl=-0.03, trail_start=0.03, trail_factor=0.90, timeout=120):
    """Canonical exit simulator — identical to analysis.py"""
    peak = 0
    trailing = False
    t = min(timeout, len(post_bars) - 1)
    if t < 0:
        return 0.0, 'timeout', 0

    exit_bar = t
    outcome = 'timeout'

    for i in range(1, t + 1):
        bar = post_bars[i]
        h = (bar[2] - entry_price) / entry_price
        l = (bar[3] - entry_price) / entry_price
        c = (bar[4] - entry_price) / entry_price
        peak = max(peak, h)

        if not trailing and peak >= trail_start:
            trailing = True

        if not trailing and l <= sl:
            return sl, 'stop_loss', i

        if trailing:
            trail_level = peak * trail_factor
            if c <= trail_level:
                return max(c, trail_level), 'trail_exit', i

    final_c = (post_bars[min(t, len(post_bars) - 1)][4] - entry_price) / entry_price
    return final_c, outcome, exit_bar


def compute_metrics(pnl_list):
    """Compute standard metrics from a list of PnL percentages."""
    if not pnl_list:
        return {'ev': 0, 'wr': 0, 'sharpe': 0, 'max_dd': 0, 'n': 0,
                'skew_top5_pct': 0, 'max_consec_loss': 0}

    n = len(pnl_list)
    ev = sum(pnl_list) / n
    wins = sum(1 for p in pnl_list if p > 0)
    wr = wins / n
    std = (sum((p - ev) ** 2 for p in pnl_list) / n) ** 0.5
    sharpe = ev / std if std > 0 else 0

    # Max drawdown (cumulative)
    cumsum = 0
    peak_cum = 0
    max_dd = 0
    for p in pnl_list:
        cumsum += p
        peak_cum = max(peak_cum, cumsum)
        dd = peak_cum - cumsum
        max_dd = max(max_dd, dd)

    # Skew: top 5% contribution
    sorted_pnl = sorted(pnl_list, reverse=True)
    top5_n = max(1, int(n * 0.05))
    total_profit = sum(p for p in pnl_list if p > 0)
    top5_profit = sum(sorted_pnl[:top5_n])
    skew_top5 = top5_profit / total_profit * 100 if total_profit > 0 else 0

    # Max consecutive loss
    streak = 0
    max_streak = 0
    for p in pnl_list:
        if p < 0:
            streak += 1
            max_streak = max(max_streak, streak)
        else:
            streak = 0

    return {
        'ev': ev, 'wr': wr, 'sharpe': sharpe, 'max_dd': max_dd,
        'n': n, 'skew_top5_pct': skew_top5, 'max_consec_loss': max_streak
    }


def quintile_analysis(records, factor_fn, pnl_fn, n_buckets=5):
    """Bin records into quintiles by factor_fn, compute metrics per bucket.
    Returns list of (bucket_label, n, ev, wr, sharpe) sorted by factor value.
    """
    # Filter records with valid factor values
    valid = [(factor_fn(r), pnl_fn(r), r) for r in records if factor_fn(r) is not None]
    if not valid:
        return []

    valid.sort(key=lambda x: x[0])
    bucket_size = max(1, len(valid) // n_buckets)

    results = []
    for i in range(n_buckets):
        start = i * bucket_size
        if i == n_buckets - 1:
            bucket = valid[start:]
        else:
            bucket = valid[start:start + bucket_size]

        if not bucket:
            continue

        pnls = [b[1] for b in bucket]
        factors = [b[0] for b in bucket]
        metrics = compute_metrics(pnls)

        results.append({
            'quintile': f'Q{i + 1}',
            'n': len(bucket),
            'factor_range': f'{min(factors):.4f} - {max(factors):.4f}',
            'ev': metrics['ev'],
            'wr': metrics['wr'],
            'sharpe': metrics['sharpe'],
        })

    return results


def run_baseline(dataset, sl=-0.03, trail_start=0.03, trail_factor=0.90, timeout=120, friction=0.0):
    """Run baseline backtest on entire dataset. Returns (pnl_list, details)."""
    pnls = []
    details = []
    for d in dataset:
        pnl, outcome, exit_bar = sim(d['bars_post'], d['entry_price'], sl, trail_start, trail_factor, timeout)
        pnl_adj = pnl - friction
        pnls.append(pnl_adj)
        details.append({
            'symbol': d['symbol'],
            'token_ca': d['token_ca'],
            'pnl': pnl,
            'pnl_adj': pnl_adj,
            'outcome': outcome,
            'exit_bar': exit_bar,
            'entry_vol': d['entry_vol'],
            'lag1_vol': d['lag1_vol'],
            'signal_ts': d['signal_ts'],
            'meta': d['meta'],
        })
    return pnls, details


def save_agent_result(agent_id, role, result_dict):
    """Save agent result as JSON."""
    output_path = Path(__file__).parent / f'debate_agent_{agent_id}.json'
    with open(output_path, 'w') as f:
        json.dump(result_dict, f, indent=2, default=str)
    print(f"Agent {agent_id} results saved to {output_path}")


if __name__ == '__main__':
    # Quick sanity check
    ds = load_dataset()
    print(f"Loaded {len(ds)} tokens")
    pnls, _ = run_baseline(ds)
    m = compute_metrics(pnls)
    print(f"Baseline: EV={m['ev']*100:+.2f}%, WR={m['wr']*100:.1f}%, "
          f"Sharpe={m['sharpe']:.3f}, n={m['n']}")
