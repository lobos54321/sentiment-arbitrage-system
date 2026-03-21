#!/usr/bin/env python3
"""
Focused backfill for Mar 13-18 tokens that are missing klines.
Only processes tokens with signals in Mar 13-18 range that have <100 bars.
"""

import sqlite3
import subprocess
import json
import time
import sys
from datetime import datetime, timezone
from pathlib import Path

SENTIMENT_DB = '/tmp/sentiment.db'
KLINE_DB = str(Path(__file__).parent.parent / 'data' / 'kline_cache.db')
GECKO_BASE = 'https://api.geckoterminal.com/api/v2/networks/solana'
DEXSCREENER_BASE = 'https://api.dexscreener.com/latest/dex'
GECKO_RATE_LIMIT = 1.3
DEX_RATE_LIMIT = 1.0

# Mar 13 00:00 → Mar 19 00:00
DATE_START_MS = int(datetime(2026, 3, 13, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
DATE_END_MS = int(datetime(2026, 3, 19, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)

LOOKBACK_SEC = 5 * 60
FORWARD_BARS = 1440  # 24 hours
FORWARD_SEC = FORWARD_BARS * 60


def curl_json(url, retries=2):
    for attempt in range(retries):
        try:
            result = subprocess.run(
                ['curl', '-s', '-m', str(20), url],
                capture_output=True, text=True, timeout=25
            )
            if result.stdout:
                data = json.loads(result.stdout)
                if 'error' in str(data).lower() and 'rate' in str(data.get('error', '')).lower():
                    time.sleep(5)
                    continue
                return data
        except Exception:
            if attempt < retries - 1:
                time.sleep(2)
    return None


def find_pool_gecko(token_ca):
    url = f'{GECKO_BASE}/tokens/{token_ca}/pools?page=1'
    data = curl_json(url)
    if not data:
        return None
    pools = data.get('data', [])
    if not pools:
        return None
    return pools[0]['attributes']['address']


def find_pool_dexscreener(token_ca):
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
    return pairs[0].get('pairAddress')


def fetch_ohlcv(pool_address, before_ts=None):
    """Fetch OHLCV with pagination, up to 5000 bars (~3.5 days)."""
    all_bars = []
    current_before = before_ts
    page_count = 0
    max_pages = 5

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
        oldest_ts = bars[-1][0]
        current_before = oldest_ts - 1
        if len(bars) < 1000:
            break
        time.sleep(GECKO_RATE_LIMIT)

    return all_bars


def main():
    sdb = sqlite3.connect(SENTIMENT_DB)
    kdb = sqlite3.connect(KLINE_DB)
    kdb.execute("PRAGMA journal_mode=WAL")

    # Ensure table
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

    # Get all Mar 13-18 signals
    signal_rows = sdb.execute("""
        SELECT token_ca, symbol, MIN(timestamp) as first_ts
        FROM premium_signals
        WHERE timestamp >= ? AND timestamp < ?
        AND hard_gate_status IN (
            "NOT_ATH_V17", "V17_NOT_ATH1", "V18_SUPERCUR_FILTER",
            "PASS", "NOT_ATH_V14", "GREYLIST"
        )
        GROUP BY token_ca
        ORDER BY first_ts
    """, (DATE_START_MS, DATE_END_MS)).fetchall()

    print(f"Mar 13-18 eligible signals: {len(signal_rows)} tokens")

    # Check which ones need data
    to_process = []
    for ca, sym, ts in signal_rows:
        bar_count = kdb.execute(
            'SELECT COUNT(*) FROM kline_1m WHERE token_ca = ?', (ca,)
        ).fetchone()[0]
        if bar_count < 100:
            to_process.append((ca, sym, ts))

    print(f"Need backfill: {len(to_process)} tokens")
    print()

    stats = {'ok': 0, 'no_pool': 0, 'no_ohlcv': 0, 'bars': 0}

    for i, (ca, sym, sig_ts_ms) in enumerate(to_process):
        sig_ts_sec = sig_ts_ms // 1000
        sym_d = (sym or '???')[:15]

        # Progress every 50
        if (i + 1) % 50 == 0:
            pct = (i + 1) / len(to_process) * 100
            print(f"\n--- Progress: {i+1}/{len(to_process)} ({pct:.0f}%) | "
                  f"ok={stats['ok']} no_pool={stats['no_pool']} no_ohlcv={stats['no_ohlcv']} bars={stats['bars']} ---\n")

        print(f"[{i+1}/{len(to_process)}] ${sym_d:15s} ", end='', flush=True)

        # Find pool
        pool = find_pool_gecko(ca)
        if pool:
            source = 'gecko'
        else:
            time.sleep(GECKO_RATE_LIMIT)
            pool = find_pool_dexscreener(ca)
            if pool:
                source = 'dex'
            else:
                print('x no pool')
                stats['no_pool'] += 1
                time.sleep(DEX_RATE_LIMIT)
                continue

        time.sleep(GECKO_RATE_LIMIT)

        # Fetch OHLCV (target: signal-5min → signal+24h)
        target_start = sig_ts_sec - LOOKBACK_SEC
        target_end = sig_ts_sec + FORWARD_SEC

        ohlcv = fetch_ohlcv(pool, before_ts=target_end + 60)

        if not ohlcv:
            print(f'x no ohlcv ({source})')
            stats['no_ohlcv'] += 1
            time.sleep(GECKO_RATE_LIMIT)
            continue

        # Filter to range
        filtered = [b for b in ohlcv if target_start <= b[0] <= target_end]

        written = 0
        for bar in filtered:
            ts, o, h, l, c, v = bar[0], float(bar[1]), float(bar[2]), float(bar[3]), float(bar[4]), float(bar[5])
            try:
                kdb.execute(
                    "INSERT OR IGNORE INTO kline_1m (token_ca, pool_address, timestamp, open, high, low, close, volume) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (ca, pool, ts, o, h, l, c, v)
                )
                written += 1
            except Exception:
                pass

        kdb.commit()
        stats['bars'] += written

        if written > 0:
            print(f'+ {written:4d} bars ({source})')
            stats['ok'] += 1
        else:
            print(f'- 0 bars in range ({source}, total={len(ohlcv)})')
            stats['no_ohlcv'] += 1

        time.sleep(GECKO_RATE_LIMIT)

    total = stats['ok'] + stats['no_pool'] + stats['no_ohlcv']
    ok_pct = stats['ok'] / total * 100 if total > 0 else 0

    print(f"\n{'='*60}")
    print(f"Mar 13-18 Backfill Complete:")
    print(f"  Processed:      {total}")
    print(f"  OK:            {stats['ok']} ({ok_pct:.1f}%)")
    print(f"  No pool:       {stats['no_pool']}")
    print(f"  No OHLCV:      {stats['no_ohlcv']}")
    print(f"  Total bars:    {stats['bars']}")
    print(f"{'='*60}")

    sdb.close()
    kdb.close()


if __name__ == '__main__':
    main()
