#!/usr/bin/env python3
"""
Batch OHLCV fetcher for NOT_ATH signals.
Pipeline: CA -> DexScreener (pairAddress) -> GeckoTerminal (OHLCV 1-min candles)
Saves to data/ohlcv-cache.json (keyed by CA).
"""

import json
import time
import os
import urllib.request
import urllib.error

CHANNEL_HISTORY = 'data/channel-history.json'
OHLCV_CACHE = 'data/ohlcv-cache.json'

def http_get(url, headers=None, timeout=15, retries=3):
    """HTTP GET with proxy support and retries."""
    req = urllib.request.Request(url)
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    req.add_header('User-Agent', 'Mozilla/5.0 (compatible; research-bot/1.0)')
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode('utf-8'))
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = 2 ** attempt * 2
                print(f'    429 rate limit, waiting {wait}s...')
                time.sleep(wait)
            else:
                return None
        except Exception:
            if attempt < retries - 1:
                time.sleep(1)
    return None

def fetch_pair_address(ca):
    """DexScreener: CA -> pairAddress on Solana (prefer pumpswap/raydium)."""
    url = f'https://api.dexscreener.com/latest/dex/tokens/{ca}'
    data = http_get(url)
    if not data or 'pairs' not in data or not data['pairs']:
        return None

    pairs = [p for p in data['pairs'] if p.get('chainId') == 'solana']
    if not pairs:
        return None

    def score(p):
        dex = p.get('dexId', '').lower()
        liq = p.get('liquidity', {}).get('usd', 0) or 0
        prio = 2 if 'pump' in dex else (1 if 'raydium' in dex else 0)
        return (prio, liq)

    pairs.sort(key=score, reverse=True)
    return pairs[0].get('pairAddress')

def fetch_ohlcv_geckoterm(pool_id, signal_ts_ms, limit=200):
    """GeckoTerminal: poolId -> 1-min OHLCV candles around signal time."""
    signal_ts = signal_ts_ms // 1000
    before_ts = signal_ts + 90 * 60

    url = (
        f'https://api.geckoterminal.com/api/v2/networks/solana/pools/{pool_id}/ohlcv/minute'
        f'?aggregate=1&limit={limit}&before_timestamp={before_ts}&token=base'
    )
    headers = {'Accept': 'application/json;version=20230302'}
    data = http_get(url, headers=headers)
    if not data:
        return None

    try:
        ohlcv_list = data['data']['attributes']['ohlcv_list']
        if not ohlcv_list:
            return None
        candles = [
            {'ts': c[0], 'o': float(c[1]), 'h': float(c[2]),
             'l': float(c[3]), 'c': float(c[4]), 'vol': float(c[5])}
            for c in ohlcv_list
        ]
        candles.sort(key=lambda x: x['ts'])
        entry_candles = [c for c in candles if c['ts'] >= signal_ts - 300]
        return entry_candles if entry_candles else candles
    except (KeyError, TypeError, IndexError):
        return None

def main():
    # Load signals
    with open(CHANNEL_HISTORY) as f:
        history = json.load(f)

    signals = history.get('signals', [])

    # Load existing cache (clean up nulls)
    if os.path.exists(OHLCV_CACHE):
        with open(OHLCV_CACHE) as f:
            cache = json.load(f)
        # Remove null entries so we retry them
        cache = {k: v for k, v in cache.items() if v is not None}
    else:
        cache = {}

    print(f'Total signals: {len(signals)}')
    print(f'Already cached (valid): {len(cache)}')

    # Filter to uncached; NOT_ATH first
    not_ath = [s for s in signals if not s.get('is_ath') and s.get('token_ca') and s['token_ca'] not in cache]
    ath = [s for s in signals if s.get('is_ath') and s.get('token_ca') and s['token_ca'] not in cache]

    to_fetch = not_ath + ath
    print(f'Need to fetch: {len(not_ath)} NOT_ATH + {len(ath)} ATH = {len(to_fetch)} total')

    if not to_fetch:
        print('Nothing to fetch.')
        _print_summary(cache)
        return

    success = 0
    fail_no_pair = 0
    fail_no_ohlcv = 0

    start = time.time()
    last_save = 0

    for i, sig in enumerate(to_fetch):
        ca = sig.get('token_ca')
        symbol = sig.get('symbol', '???')
        ts_ms = sig.get('ts', 0)
        is_ath = sig.get('is_ath', False)
        sig_type = 'ATH' if is_ath else 'NOT_ATH'

        # Step 1: DexScreener
        pair_addr = fetch_pair_address(ca)
        if not pair_addr:
            fail_no_pair += 1
            # Progress every 10
            if (i + 1) % 10 == 0:
                done = i + 1
                elapsed = time.time() - start
                rate = done / elapsed
                eta = (len(to_fetch) - done) / rate
                print(f'[{done}/{len(to_fetch)}] ok={success} no_pair={fail_no_pair} no_ohlcv={fail_no_ohlcv} | {rate:.1f}/s ETA:{eta:.0f}s')
            time.sleep(0.15)
            continue

        # Step 2: GeckoTerminal
        candles = fetch_ohlcv_geckoterm(pair_addr, ts_ms)
        if not candles:
            fail_no_ohlcv += 1
            if (i + 1) % 10 == 0:
                done = i + 1
                elapsed = time.time() - start
                rate = done / elapsed
                eta = (len(to_fetch) - done) / rate
                print(f'[{done}/{len(to_fetch)}] ok={success} no_pair={fail_no_pair} no_ohlcv={fail_no_ohlcv} | {rate:.1f}/s ETA:{eta:.0f}s')
            time.sleep(0.15)
            continue

        entry = {
            'ca': ca,
            'pairAddr': pair_addr,
            'symbol': symbol,
            'signal_ts': ts_ms // 1000,
            'is_ath': is_ath,
            'candles': candles
        }
        cache[ca] = entry
        success += 1

        # Progress every 10
        if (i + 1) % 10 == 0:
            done = i + 1
            elapsed = time.time() - start
            rate = done / elapsed
            eta = (len(to_fetch) - done) / rate
            print(f'[{done}/{len(to_fetch)}] ok={success} no_pair={fail_no_pair} no_ohlcv={fail_no_ohlcv} | {rate:.1f}/s ETA:{eta:.0f}s')

        # Save every 30 successful fetches or every 50 iterations
        if success - last_save >= 30 or (i + 1) % 50 == 0:
            with open(OHLCV_CACHE, 'w') as f:
                json.dump(cache, f, separators=(',', ':'))
            last_save = success

        time.sleep(0.3)  # polite rate limit

    # Final save
    with open(OHLCV_CACHE, 'w') as f:
        json.dump(cache, f, separators=(',', ':'))

    total_time = time.time() - start
    print(f'\nDone! Fetched {success}/{len(to_fetch)} in {total_time:.1f}s')
    print(f'  no_pair: {fail_no_pair}, no_ohlcv: {fail_no_ohlcv}')
    _print_summary(cache)

def _print_summary(cache):
    not_ath_cached = sum(1 for v in cache.values() if v and not v.get('is_ath'))
    ath_cached = sum(1 for v in cache.values() if v and v.get('is_ath'))
    print(f'Total cache: {len(cache)} (NOT_ATH:{not_ath_cached} ATH:{ath_cached})')

if __name__ == '__main__':
    main()
