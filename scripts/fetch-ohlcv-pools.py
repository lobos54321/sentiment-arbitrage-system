#!/usr/bin/env python3
"""
OHLCV Fetcher — GeckoTerminal /tokens/{ca}/pools endpoint
用 /tokens/{ca}/pools 找 pool，再拉 1分钟 K线
"""
import json, time, urllib.request, urllib.error, sys

CACHE_PATH = 'data/ohlcv-cache.json'
HEADERS = {
    'Accept': 'application/json;version=20230302',
    'User-Agent': 'Mozilla/5.0 (sentiment-backtest/1.0)',
}

def gt_get(url, retries=3, delay=1.5):
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=12) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = delay * (2 ** i)
                print(f"    429 rate limit, wait {wait:.0f}s...")
                time.sleep(wait)
            elif e.code == 404:
                return None
            else:
                print(f"    HTTP {e.code}: {url}")
                return None
        except Exception as e:
            print(f"    Error: {e}")
            time.sleep(delay)
    return None

def get_pool_address(ca):
    """用 /tokens/{ca}/pools 获取最佳 pool 地址"""
    url = f"https://api.geckoterminal.com/api/v2/networks/solana/tokens/{ca}/pools?page=1"
    data = gt_get(url)
    if not data:
        return None
    pools = data.get('data', [])
    if not pools:
        return None
    # 选第一个 pool（流动性最高）
    pool = pools[0]
    return pool['id'].split('_')[-1]  # "solana_POOL_ADDRESS" → "POOL_ADDRESS"

def fetch_ohlcv(pool_addr, signal_ts_ms, window_minutes=120):
    """拉 pool 的 1分K，覆盖信号后2小时"""
    # GeckoTerminal OHLCV: 支持 before_timestamp 参数
    signal_ts_s = signal_ts_ms // 1000
    # 拉信号前5分钟到信号后120分钟
    before_ts = signal_ts_s + window_minutes * 60 + 60
    url = (f"https://api.geckoterminal.com/api/v2/networks/solana/pools/{pool_addr}"
           f"/ohlcv/minute?aggregate=1&limit=300&before_timestamp={before_ts}&currency=usd")
    data = gt_get(url)
    if not data:
        return None
    ohlcv_list = data.get('data', {}).get('attributes', {}).get('ohlcv_list', [])
    if not ohlcv_list:
        return None
    # GT格式: [ts, o, h, l, c, v]
    candles = []
    for bar in reversed(ohlcv_list):  # GT 返回降序，翻转为升序
        ts, o, h, l, c, v = bar
        candles.append({'ts': int(ts), 'o': float(o), 'h': float(h),
                        'l': float(l), 'c': float(c), 'v': float(v)})
    return candles

def main(tokens):
    # tokens = [(symbol, ca, signal_ts_ms), ...]
    cache = json.load(open(CACHE_PATH))
    updated = 0

    for sym, ca, ts_ms in tokens:
        print(f"\n  [{sym}] {ca[:20]}...")

        # Step 1: 找 pool
        pool_addr = get_pool_address(ca)
        if not pool_addr:
            print(f"    ❌ 找不到 pool")
            cache.setdefault(ca, {})['candles'] = []
            cache[ca]['pool'] = None
            cache[ca]['error'] = 'no_pool'
            continue
        print(f"    Pool: {pool_addr[:20]}...")
        time.sleep(0.8)  # 避免 rate limit

        # Step 2: 拉K线
        candles = fetch_ohlcv(pool_addr, ts_ms)
        if not candles:
            print(f"    ❌ 无K线数据")
            cache.setdefault(ca, {})['candles'] = []
            cache[ca]['pool'] = pool_addr
            cache[ca]['error'] = 'no_candles'
            continue

        # 只保留信号后的K线
        sig_ts = ts_ms // 1000
        after = [c for c in candles if c['ts'] >= sig_ts - 120]
        print(f"    ✅ {len(candles)} 根K线 → 信号后 {len(after)} 根")
        cache[ca] = {'candles': after, 'pool': pool_addr, 'fetched_at': int(time.time())}
        updated += 1
        time.sleep(0.8)

    json.dump(cache, open(CACHE_PATH, 'w'), ensure_ascii=False)
    print(f"\n完成: 更新 {updated}/{len(tokens)} 个 token")

if __name__ == '__main__':
    import sys
    # 从命令行接收 JSON 格式的 tokens 列表
    tokens_json = sys.argv[1] if len(sys.argv) > 1 else '[]'
    tokens = json.loads(tokens_json)
    main(tokens)
