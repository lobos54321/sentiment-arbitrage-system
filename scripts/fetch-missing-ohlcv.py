#!/usr/bin/env python3
"""
补充拉取缺失K线：遍历所有信号，对 ohlcv-cache 缺失的 CA 拉取并更新。
"""
import json, time, os, sys
import urllib.request, urllib.error

OHLCV_CACHE = 'data/ohlcv-cache.json'

def http_get(url, timeout=15, retries=3):
    req = urllib.request.Request(url)
    req.add_header('User-Agent', 'Mozilla/5.0 (compatible; research-bot/1.0)')
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode('utf-8'))
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = 2 ** attempt * 3
                print(f'    429 rate limit, sleep {wait}s')
                time.sleep(wait)
            else:
                return None
        except Exception:
            if attempt < retries - 1:
                time.sleep(1)
    return None

def fetch_pair_address(ca):
    url = f'https://api.dexscreener.com/latest/dex/tokens/{ca}'
    data = http_get(url)
    if not data or not data.get('pairs'):
        return None
    pairs = [p for p in data['pairs'] if p.get('chainId') == 'solana']
    if not pairs:
        return None
    def score(p):
        dex = p.get('dexId', '').lower()
        liq = (p.get('liquidity') or {}).get('usd', 0) or 0
        prio = 2 if 'pump' in dex else (1 if 'raydium' in dex else 0)
        return (prio, liq)
    pairs.sort(key=score, reverse=True)
    return pairs[0].get('pairAddress')

def fetch_ohlcv(pool_id, signal_ts, limit=200):
    """signal_ts: seconds"""
    before_ts = int(signal_ts) + 60 * limit
    url = (f'https://api.geckoterminal.com/api/v2/networks/solana/pools/{pool_id}'
           f'/ohlcv/minute?aggregate=1&before_timestamp={before_ts}&limit={limit}&currency=usd')
    data = http_get(url)
    if not data:
        return None
    raw = (data.get('data') or {}).get('attributes', {}).get('ohlcv_list', [])
    if not raw:
        return None
    candles = [{'ts': c[0], 'o': float(c[1]), 'h': float(c[2]),
                'l': float(c[3]), 'c': float(c[4]), 'vol': float(c[5])}
               for c in raw]
    return candles

def main():
    # 加载所有信号
    hist = json.load(open('data/channel-history.json'))
    all_sigs = list(hist['signals'])
    if os.path.exists('/tmp/mar1316_signals_filtered.json'):
        aux = json.load(open('/tmp/mar1316_signals_filtered.json'))
        seen = {(s.get('token_ca'), s['ts']) for s in all_sigs}
        for s in aux:
            ca = s.get('token_ca') or s.get('ca', '')
            if (ca, s['ts']) not in seen:
                all_sigs.append(s)

    cache = json.load(open(OHLCV_CACHE)) if os.path.exists(OHLCV_CACHE) else {}

    # 找出缺失的 CA → (ca, signal_ts_sec, name)
    missing = {}
    for s in all_sigs:
        ca = s.get('token_ca') or s.get('ca', '')
        if not ca or ca in cache:
            continue
        ts = s['ts']
        if ts > 1e12:
            ts /= 1000
        if ca not in missing:
            missing[ca] = {'ts': ts, 'name': s.get('token_name') or s.get('symbol', '?')}

    print(f'需要补充: {len(missing)} 个 CA')
    if not missing:
        print('全部已有K线，无需补充。')
        return

    success = fail_pair = fail_ohlcv = 0
    items = list(missing.items())

    for i, (ca, info) in enumerate(items):
        name = info['name']
        ts   = info['ts']

        pair = fetch_pair_address(ca)
        if not pair:
            fail_pair += 1
            print(f'  [{i+1}/{len(items)}] {name} NO_PAIR')
            time.sleep(0.2)
            continue

        candles = fetch_ohlcv(pair, ts)
        if not candles:
            fail_ohlcv += 1
            print(f'  [{i+1}/{len(items)}] {name} NO_OHLCV (pair={pair[:8]}...)')
            time.sleep(0.2)
            continue

        cache[ca] = {
            'ca': ca, 'pairAddr': pair, 'symbol': name,
            'signal_ts': int(ts), 'is_ath': False, 'candles': candles
        }
        success += 1
        print(f'  [{i+1}/{len(items)}] ✅ {name} ({len(candles)} candles)')

        if success % 20 == 0:
            with open(OHLCV_CACHE, 'w') as f:
                json.dump(cache, f, separators=(',', ':'))
            print(f'    [auto-save] cache={len(cache)}')

        time.sleep(0.4)

    with open(OHLCV_CACHE, 'w') as f:
        json.dump(cache, f, separators=(',', ':'))

    print(f'\n完成: 成功={success}  no_pair={fail_pair}  no_ohlcv={fail_ohlcv}')
    print(f'cache 总量: {len(cache)}')

if __name__ == '__main__':
    main()
