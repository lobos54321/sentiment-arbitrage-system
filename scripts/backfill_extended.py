#!/usr/bin/env python3
"""
扩展补采 K 线 - Mar 4-12 的 premium_signals token
从 GeckoTerminal 拉 1m K 线，失败时 fallback DexScreener 查 pool 再回 GeckoTerminal 拉数据。
支持: --limit N (限制处理数量), 断点续传(自动跳过已有数据的token)
"""

import sqlite3
import subprocess
import json
import time
import sys
import argparse
from datetime import datetime, timezone
from pathlib import Path

SENTIMENT_DB = '/tmp/sentiment.db'
KLINE_DB = str(Path(__file__).parent.parent / 'data' / 'kline_cache.db')
GECKO_BASE = 'https://api.geckoterminal.com/api/v2/networks/solana'
DEXSCREENER_BASE = 'https://api.dexscreener.com/latest/dex'
GECKO_RATE_LIMIT = 1.3   # seconds between GeckoTerminal calls
DEX_RATE_LIMIT = 1.0     # seconds between DexScreener calls

# All historical (no date restriction) — fetch as much as possible for lifecycle + backtest
DATE_START_MS = 0  # no start limit
DATE_END_MS = int(datetime(2026, 3, 20, 23, 59, tzinfo=timezone.utc).timestamp() * 1000)

LOOKBACK_BARS = 5
LOOKBACK_SEC = LOOKBACK_BARS * 60
# Fetch up to 1500 bars = 25 hours (covers full 24h lifecycle + buffer)
FORWARD_BARS = 1500
FORWARD_SEC = FORWARD_BARS * 60


def curl_json(url, retries=2):
    """HTTP GET via curl, returns parsed JSON or None."""
    for attempt in range(retries):
        try:
            result = subprocess.run(
                ['curl', '-s', '-H', 'Accept: application/json', url],
                capture_output=True, text=True, timeout=20
            )
            if result.stdout:
                data = json.loads(result.stdout)
                # Rate limit error → wait and retry
                if 'error' in data and 'rate' in str(data.get('error', '')).lower():
                    time.sleep(5)
                    continue
                return data
        except Exception:
            if attempt < retries - 1:
                time.sleep(2)
    return None


def find_pool_gecko(token_ca):
    """GeckoTerminal: token → pool address (highest liquidity)."""
    url = f'{GECKO_BASE}/tokens/{token_ca}/pools?page=1'
    data = curl_json(url)
    if not data:
        return None
    pools = data.get('data', [])
    if not pools:
        return None
    return pools[0]['attributes']['address']


def find_pool_dexscreener(token_ca):
    """DexScreener fallback: token → Solana pair address."""
    url = f'{DEXSCREENER_BASE}/tokens/{token_ca}'
    data = curl_json(url)
    if not data:
        return None
    pairs = data.get('pairs', [])
    if not pairs:
        return None
    for p in pairs:
        if p.get('chainId') == 'solana':
            return p.get('pairAddress')
    return pairs[0].get('pairAddress') if pairs else None


def fetch_ohlcv(pool_address, before_ts=None):
    """GeckoTerminal: pool → 1min OHLCV bars, with pagination up to 5000 bars."""
    all_bars = []
    current_before = before_ts
    page_count = 0
    max_pages = 5  # 5 * 1000 = 5000 bars ≈ 3.5 days

    while page_count < max_pages:
        url = f'{GECKO_BASE}/pools/{pool_address}/ohlcv/minute?aggregate=1&limit=1000'
        if current_before:
            url += f'&before_timestamp={current_before}'
        data = curl_json(url)
        if not data:
            break
        bars = data.get('data', {}).get('attributes', {}).get('ohlcv_list', [])
        if not bars:
            break
        all_bars.extend(bars)
        page_count += 1
        # Next page: use timestamp of oldest bar - 1
        oldest_ts = bars[-1][0]
        current_before = oldest_ts - 1
        # If we got fewer than 1000 bars, we're done
        if len(bars) < 1000:
            break
        time.sleep(GECKO_RATE_LIMIT)

    return all_bars


def main():
    parser = argparse.ArgumentParser(description='Backfill Mar 4-12 kline data')
    parser.add_argument('--limit', type=int, default=0, help='Max tokens to process (0=all)')
    args = parser.parse_args()

    sdb = sqlite3.connect(SENTIMENT_DB)
    kdb = sqlite3.connect(KLINE_DB)
    kdb.execute("PRAGMA journal_mode=WAL")

    # Ensure table exists
    kdb.execute("""
        CREATE TABLE IF NOT EXISTS kline_1m (
            token_ca TEXT NOT NULL,
            pool_address TEXT NOT NULL DEFAULT '',
            timestamp INTEGER NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL DEFAULT 0,
            PRIMARY KEY (token_ca, timestamp)
        )
    """)

    # Tokens already in kline_cache
    kline_cas = set(r[0] for r in kdb.execute(
        "SELECT DISTINCT token_ca FROM kline_1m"
    ).fetchall())

    # All distinct tokens with relevant (new token / trending) signals
    # Exclude: ATH statuses, WASH, REJECT, RISK_BLOCKED
    rows = sdb.execute("""
        SELECT token_ca, symbol, MIN(timestamp) as first_signal_ts
        FROM premium_signals
        WHERE timestamp >= ? AND timestamp <= ?
        AND hard_gate_status IN (
            "PASS", "NOT_ATH_V14", "NOT_ATH_V17",
            "GREYLIST", "GREYLIST_LOW_CONF",
            "V17_NOT_ATH1", "V18_SUPERCUR_FILTER",
            "NOT_ATH_V16", "V17_MC_FILTER"
        )
        GROUP BY token_ca
        ORDER BY first_signal_ts
    """, (DATE_START_MS, DATE_END_MS)).fetchall()

    total_in_range = len(rows)

    # Filter out tokens that already have kline data (断点续传)
    missing = [(ca, sym, ts) for ca, sym, ts in rows if ca not in kline_cas]

    print(f"=== Backfill Extended: Historical K-line (all dates) ===")
    print(f"  Total eligible tokens: {total_in_range}")
    print(f"  Already have klines (≥100): {total_in_range - len(missing)}")
    print(f"  Missing (to process):  {len(missing)}")
    print(f"  Window: signal-5min → signal+25h ({LOOKBACK_BARS}+{FORWARD_BARS} bars)")
    print()

    if args.limit > 0:
        missing = missing[:args.limit]
        print(f"  --limit applied:       {len(missing)}")

    print()

    stats = {
        'gecko_ok': 0, 'dex_ok': 0, 'no_pool': 0,
        'no_ohlcv': 0, 'error': 0, 'total_bars': 0
    }

    for i, (ca, sym, sig_ts_ms) in enumerate(missing):
        sig_ts_sec = sig_ts_ms // 1000
        sym_display = (sym or '???')[:15]
        print(f"[{i+1}/{len(missing)}] ${sym_display:15s} ", end='', flush=True)

        # --- Step 1: Find pool ---
        pool = None
        source = None

        pool = find_pool_gecko(ca)
        if pool:
            source = 'gecko'
        else:
            time.sleep(GECKO_RATE_LIMIT)
            pool = find_pool_dexscreener(ca)
            if pool:
                source = 'dex->gecko'
            time.sleep(DEX_RATE_LIMIT)

        if not pool:
            print("x no pool")
            stats['no_pool'] += 1
            time.sleep(GECKO_RATE_LIMIT)
            continue

        time.sleep(GECKO_RATE_LIMIT)

        # --- Step 2: Fetch OHLCV ---
        # Target: signal time - 5min → signal + 5hours (for lifecycle + backtest)
        target_start = sig_ts_sec - LOOKBACK_SEC
        target_end = sig_ts_sec + FORWARD_SEC

        ohlcv = fetch_ohlcv(pool, before_ts=target_end + 60)

        if not ohlcv:
            print(f"x no ohlcv ({source}, pool={pool[:16]}...)")
            stats['no_ohlcv'] += 1
            time.sleep(GECKO_RATE_LIMIT)
            continue

        # Filter to target range
        filtered = [b for b in ohlcv if target_start <= b[0] <= target_end]

        # --- Step 3: Write to DB ---
        written = 0
        for bar in filtered:
            ts, o, h, l, c, v = bar[0], float(bar[1]), float(bar[2]), float(bar[3]), float(bar[4]), float(bar[5])
            try:
                kdb.execute(
                    "INSERT OR IGNORE INTO kline_1m (token_ca, pool_address, timestamp, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (ca, pool, ts, o, h, l, c, v)
                )
                written += 1
            except Exception:
                pass

        kdb.commit()

        if written > 0:
            if source == 'gecko':
                stats['gecko_ok'] += 1
            else:
                stats['dex_ok'] += 1
            stats['total_bars'] += written
            print(f"+ {written:3d} bars ({source}, pool={pool[:16]}...)")
        else:
            stats['no_ohlcv'] += 1
            print(f"- 0 bars in range ({source}, total_fetched={len(ohlcv)}, filtered={len(filtered)})")

        time.sleep(GECKO_RATE_LIMIT)

        # Progress summary every 50 tokens
        if (i + 1) % 50 == 0:
            elapsed_pct = (i + 1) / len(missing) * 100
            print(f"\n--- Progress: {i+1}/{len(missing)} ({elapsed_pct:.0f}%) | "
                  f"gecko={stats['gecko_ok']} dex={stats['dex_ok']} "
                  f"no_pool={stats['no_pool']} no_ohlcv={stats['no_ohlcv']} "
                  f"bars={stats['total_bars']} ---\n")

    # Final summary
    total_processed = stats['gecko_ok'] + stats['dex_ok'] + stats['no_pool'] + stats['no_ohlcv']
    success = stats['gecko_ok'] + stats['dex_ok']
    success_pct = (success / total_processed * 100) if total_processed > 0 else 0

    print(f"\n{'='*60}")
    print(f"Backfill Extended Complete:")
    print(f"  Processed:            {total_processed}")
    print(f"  GeckoTerminal OK:     {stats['gecko_ok']}")
    print(f"  DexScreener->Gecko OK:{stats['dex_ok']}")
    print(f"  No pool found:        {stats['no_pool']}")
    print(f"  No OHLCV data:        {stats['no_ohlcv']}")
    print(f"  Success rate:         {success_pct:.1f}%")
    print(f"  Total bars written:   {stats['total_bars']}")
    print(f"{'='*60}")

    sdb.close()
    kdb.close()


if __name__ == '__main__':
    main()
