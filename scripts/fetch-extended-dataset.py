#!/usr/bin/env python3
"""
扩展数据集获取脚本
---------------------------------
1. 获取3/17的13个新ATH信号的K线
2. 通过DexScreener找同期其他pump.fun代币 (NOT_ATH代理)
3. 获取所有代币的OHLCV
4. 运行完整回测对比
"""

import urllib.request, json, ssl, time, datetime, sys, os

ctx = ssl.create_default_context()
ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE

CACHE_FILE = 'data/ohlcv-cache.json'
NOT_ATH_CACHE_FILE = 'data/not-ath-signals.json'
SLIP = 0.004  # 0.4% 滑点

def fetch(url, delay=1.2, retries=3):
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={
                'User-Agent': 'Mozilla/5.0 (compatible)',
                'Accept': 'application/json'
            })
            time.sleep(delay if attempt == 0 else delay * (attempt + 1))
            with urllib.request.urlopen(req, timeout=20, context=ctx) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            if attempt == retries - 1:
                return None
    return None

def save_cache(cache, file=CACHE_FILE):
    with open(file, 'w') as f:
        json.dump(cache, f, indent=2)

# ──────────────────────────────────────────────────────
# Step 1: 获取K线数据
# ──────────────────────────────────────────────────────
def fetch_ohlcv(ca, entry_ts_sec):
    """获取代币的1分钟K线数据"""
    # 1. 找pair地址
    ds = fetch(f'https://api.dexscreener.com/latest/dex/tokens/{ca}', delay=0.8)
    if not ds or not ds.get('pairs'):
        return None
    
    # 选最接近entry时间的pair
    candidates = [(p, abs(p.get('pairCreatedAt',0)/1000 - entry_ts_sec))
                  for p in ds['pairs'] if p.get('chainId') == 'solana' and p.get('pairCreatedAt')]
    if not candidates:
        candidates = [(ds['pairs'][0], 0)]
    
    best_pair = min(candidates, key=lambda x: x[1])[0]
    pool_id = best_pair.get('pairAddress')
    if not pool_id:
        return None
    
    # 2. 获取K线 (双窗口)
    bars = {}
    for window_end in [entry_ts_sec + 600, entry_ts_sec + 3600]:
        d = fetch(
            f'https://api.geckoterminal.com/api/v2/networks/solana/pools/{pool_id}/ohlcv/minute'
            f'?aggregate=1&limit=200&before_timestamp={window_end}&token=base',
            delay=1.5
        )
        if d and d.get('data', {}).get('attributes', {}).get('ohlcv_list'):
            for bar in d['data']['attributes']['ohlcv_list']:
                ts = int(bar[0])
                if ts not in bars:
                    bars[ts] = {'ts': ts, 'o': bar[1], 'h': bar[2], 'l': bar[3], 'c': bar[4], 'vol': bar[5]}
    
    if not bars:
        return None
    
    candles = sorted(bars.values(), key=lambda x: x['ts'])
    
    # 过滤entry后的数据
    entry_bar = (entry_ts_sec // 60) * 60
    after_candles = [c for c in candles if c['ts'] >= entry_bar - 300]
    
    return {
        'ca': ca,
        'pairAddr': pool_id,
        'dex': best_pair.get('dexId', ''),
        'symbol': best_pair.get('baseToken', {}).get('symbol', ''),
        'candles': after_candles,
        'candle_count': len(after_candles)
    }

# ──────────────────────────────────────────────────────
# Step 2: 通过DexScreener找同期NOT_ATH代币
# ──────────────────────────────────────────────────────
def find_coexisting_pump_tokens(anchor_ca, anchor_entry_ts_ms, window_minutes=60):
    """
    找与anchor代币同时期的其他pump.fun代币作为NOT_ATH代理
    策略：找anchor pair创建时间 ±window_minutes 内的其他pumpswap pair
    """
    # 获取anchor的pair创建时间
    ds = fetch(f'https://api.dexscreener.com/latest/dex/tokens/{anchor_ca}', delay=0.6)
    if not ds or not ds.get('pairs'):
        return []
    
    anchor_pairs = [p for p in ds['pairs'] if p.get('chainId') == 'solana']
    if not anchor_pairs:
        return []
    
    # 找最接近entry时间的pair
    anchor_pair = min(anchor_pairs, key=lambda p: abs(p.get('pairCreatedAt',0) - anchor_entry_ts_ms))
    anchor_created = anchor_pair.get('pairCreatedAt', 0) / 1000  # 转为秒
    
    if not anchor_created:
        return []
    
    # 搜索同期代币: 在anchor_created ±window_minutes 内的pumpswap pair
    window_start = int(anchor_created - window_minutes * 60)
    window_end = int(anchor_created + window_minutes * 60)
    
    # 方法：用Solana RPC getSignaturesForAddress找pumpswap program的交易
    # 然后过滤创建时间在窗口内的pair
    # 但这太慢了。用DexScreener批量查询
    
    return []  # 暂时返回空，等待更好的方法

# ──────────────────────────────────────────────────────
# 信号数据
# ──────────────────────────────────────────────────────
# 3/15-3/16 ATH信号 (42个，已有K线)
SIGNALS_ATH = [
    # 已有缓存的42个信号
    {'symbol': 'Eclipse',    'ca': 'ApwtY1HWHgDLDY5unJ7awPrBeQo4UwstCM83A5zFpump', 'entry_ts': 1773576800963, 'mc': 115280, 'date': '3/15'},
    {'symbol': 'AGENTPUMPY', 'ca': '6xxKkqfd1nqstqhbHrhdCXsEFZ3Ge3SWhXV5bzNApump', 'entry_ts': 1773578922235, 'mc': 40750,  'date': '3/15'},
    {'symbol': 'LATENT',     'ca': 'GbNytkgN7eSKV1LECjr39omiW8JbJgNhT1tYN5Ubpump', 'entry_ts': 1773585339169, 'mc': 65520,  'date': '3/15'},
    {'symbol': 'Jeffrey',    'ca': 'BdsjNF4MzF2WSjokZwQiCpekcpMksUEz1piUYppCpump', 'entry_ts': 1773587617999, 'mc': 60350,  'date': '3/15'},
    {'symbol': 'Gany-1',     'ca': 'BgtczEGgf9mZMcGBLi4J5Pn8PtmFy6Xz5uBKkMfspump', 'entry_ts': 1773701454000, 'mc': 98200,  'date': '3/17'},
    {'symbol': 'Gany-2',     'ca': 'Dz4bX3snTDxqdKyZwdUgKoDvSjyvmoA23E6j5odZpump',  'entry_ts': 1773702026000, 'mc': 49900,  'date': '3/17'},
    {'symbol': '唐子',       'ca': 'ocB3t4czHwsueZk89YGBxEbisFLZ1tzvydpwbC9pump',    'entry_ts': 1773704965000, 'mc': 78100,  'date': '3/17'},
]

# 3/17 ATH信号 (需要获取K线)
SIGNALS_MAR17 = [
    {'symbol': 'Gany-1',    'ca': 'BgtczEGgf9mZMcGBLi4J5Pn8PtmFy6Xz5uBKkMfspump', 'entry_ts': 1773701454000, 'mc': 98200,  'v18_pass': True},
    {'symbol': 'Gany-2',    'ca': 'Dz4bX3snTDxqdKyZwdUgKoDvSjyvmoA23E6j5odZpump',  'entry_ts': 1773702026000, 'mc': 49900,  'v18_pass': True},
    {'symbol': '唐子',      'ca': 'ocB3t4czHwsueZk89YGBxEbisFLZ1tzvydpwbC9pump',    'entry_ts': 1773704965000, 'mc': 78100,  'v18_pass': True},
    {'symbol': 'HOSPICE',   'ca': '6uq3r5mMQL6tKkJd9JpuA3bPbrqitpFfhSQDVPCMpump',  'entry_ts': 1773696566000, 'mc': 124010, 'v18_pass': False},
    {'symbol': 'MINDLESS',  'ca': 'AJofCoVif3wj2Uy7mpzgxbqDyHPyn7xp6WzJBy7gpump',  'entry_ts': 1773705194000, 'mc': 32470,  'v18_pass': False},
    {'symbol': 'Clove',     'ca': 'EAXGeuMf8xxkNQzqpLE4PRuzmyXNKSDTPKMQvzoDpump',  'entry_ts': 1773706157000, 'mc': 25230,  'v18_pass': False},
    {'symbol': 'TITAN',     'ca': '4ay158ynQu4RfKe4oEjQFd3om2Fvyguz1UnQMm3rpump',  'entry_ts': 1773707077000, 'mc': 36360,  'v18_pass': False},
    {'symbol': 'pvpdog',    'ca': '9UT2T4XPYAtiUuSBdgkHTBbCTcfUtDVUSdSkTev1pump',  'entry_ts': 1773707374000, 'mc': 37760,  'v18_pass': False},
    {'symbol': 'Democrats', 'ca': 'Vnm731Pin6BHsvvoTkBpLWcNM78NknXYAF3oyQWpump',   'entry_ts': 1773703900000, 'mc': 351950, 'v18_pass': False},
    {'symbol': 'ARC',       'ca': '2oBe59KhZ8s7pzYGbHNWVDK5RGSA2Yx9ufAe85TGpump',  'entry_ts': 1773704940000, 'mc': 13380,  'v18_pass': False},
    {'symbol': 'Hyojo',     'ca': 'HfKNrf3VFYSzZfG3jS4pQEfjkHTxAwmD4wm1FUx8pump',  'entry_ts': 1773704561000, 'mc': 49470,  'v18_pass': False},
    {'symbol': 'TOKEN',     'ca': '8P1PRDiJjSK8xA3zvaARSo2uTTj1nRagCGL1kD9Dpump',  'entry_ts': 1773689508000, 'mc': 95500,  'v18_pass': False},
    {'symbol': '唐子-alt',  'ca': '85NLap7dACtf6APMerHcMDb4iBsYRziT7F8xecfYpump',  'entry_ts': 1773700558000, 'mc': 35760,  'v18_pass': False},
    {'symbol': 'Replacement','ca': '7STZgGYW7HsVpZGdaCYfikLu2FuebbpqC7gwaP3Apump', 'entry_ts': 1773700902000, 'mc': 43860,  'v18_pass': False},
]

if __name__ == '__main__':
    cache = json.load(open(CACHE_FILE))
    print(f'当前缓存: {len(cache)}个代币')
    
    # 获取3/17信号K线
    print('\n=== 获取3/17信号K线 ===')
    new_count = 0
    for sig in SIGNALS_MAR17:
        ca = sig['ca']
        if ca in cache:
            print(f'  {sig["symbol"]}: 已有缓存')
            continue
        
        print(f'  获取 {sig["symbol"]} ({ca[:12]}...) ...', end='', flush=True)
        entry_ts = sig['entry_ts'] // 1000
        data = fetch_ohlcv(ca, entry_ts)
        
        if data and data.get('candles'):
            cache[ca] = data
            save_cache(cache)
            new_count += 1
            print(f' ✅ {len(data["candles"])}根K线')
        else:
            cache[ca] = None  # 标记为无数据
            save_cache(cache)
            print(f' ❌ 无K线')
    
    print(f'\n获取完成: {new_count}个新代币有K线')
    print(f'总缓存: {len(cache)}个代币')

