#!/usr/bin/env python3
"""
K 线数据抓取器 + 本地缓存
- 从 DexScreener 获取 pool address
- 从 GeckoTerminal 拉 1 分钟 OHLCV K 线
- 存入 SQLite kline_cache 表，下次直接读
"""
import sqlite3
import json
import time
import subprocess
import sys
import os
from datetime import datetime, timezone

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'kline_cache.db')

def init_db(db_path=None):
    """初始化 K 线缓存数据库"""
    path = db_path or DB_PATH
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.execute("""
        CREATE TABLE IF NOT EXISTS kline_1m (
            token_ca TEXT NOT NULL,
            pool_address TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            PRIMARY KEY (token_ca, timestamp)
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS pool_mapping (
            token_ca TEXT PRIMARY KEY,
            pool_address TEXT NOT NULL,
            symbol TEXT,
            dex_id TEXT,
            chain TEXT DEFAULT 'solana',
            fetched_at INTEGER NOT NULL
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS fetch_log (
            token_ca TEXT NOT NULL,
            fetched_at INTEGER NOT NULL,
            candles_count INTEGER NOT NULL,
            earliest_ts INTEGER,
            latest_ts INTEGER,
            status TEXT DEFAULT 'ok'
        )
    """)
    db.execute("CREATE INDEX IF NOT EXISTS idx_kline_token ON kline_1m(token_ca)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_kline_ts ON kline_1m(token_ca, timestamp)")
    db.commit()
    return db


def curl_json(url, timeout=15):
    """用 curl 拉 JSON"""
    try:
        result = subprocess.run(
            ['curl', '-s', '-m', str(timeout), url],
            capture_output=True, text=True, timeout=timeout + 5
        )
        if result.returncode != 0:
            return None
        return json.loads(result.stdout)
    except Exception as e:
        return None


def get_pool_address(db, token_ca):
    """从 DexScreener 获取 pool address，带缓存"""
    # 先查缓存
    row = db.execute("SELECT pool_address FROM pool_mapping WHERE token_ca = ?", (token_ca,)).fetchone()
    if row:
        return row['pool_address']

    # 从 DexScreener 拉
    data = curl_json(f"https://api.dexscreener.com/latest/dex/tokens/{token_ca}")
    if not data:
        return None

    pairs = data.get('pairs', [])
    if not pairs:
        return None

    # 取 Solana 链上流动性最大的 pair
    sol_pairs = [p for p in pairs if p.get('chainId') == 'solana']
    if not sol_pairs:
        sol_pairs = pairs

    pair = max(sol_pairs, key=lambda p: (p.get('liquidity', {}).get('usd', 0) or 0))
    pool = pair.get('pairAddress', '')

    if pool:
        db.execute("""
            INSERT OR REPLACE INTO pool_mapping (token_ca, pool_address, symbol, dex_id, chain, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (token_ca, pool, pair.get('baseToken', {}).get('symbol', ''),
              pair.get('dexId', ''), pair.get('chainId', 'solana'), int(time.time())))
        db.commit()

    return pool


def fetch_klines(db, token_ca, pool_address, max_pages=5):
    """
    从 GeckoTerminal 拉 1 分钟 K 线
    每页最多 1000 根，用 before_timestamp 分页
    """
    all_candles = []
    before_ts = None

    for page in range(max_pages):
        url = f"https://api.geckoterminal.com/api/v2/networks/solana/pools/{pool_address}/ohlcv/minute?aggregate=1&limit=1000"
        if before_ts:
            url += f"&before_timestamp={before_ts}"

        data = curl_json(url)
        if not data:
            break

        ohlcv = data.get('data', {}).get('attributes', {}).get('ohlcv_list', [])
        if not ohlcv:
            break

        all_candles.extend(ohlcv)

        # 下一页从最早的 timestamp 往前
        before_ts = min(c[0] for c in ohlcv)

        # 如果返回的不到 1000 根，说明已经到底了
        if len(ohlcv) < 1000:
            break

        time.sleep(0.5)  # rate limit

    if not all_candles:
        return 0

    # 去重 + 排序
    seen = set()
    unique = []
    for c in all_candles:
        ts = c[0]
        if ts not in seen:
            seen.add(ts)
            unique.append(c)
    unique.sort(key=lambda c: c[0])

    # 批量写入 DB
    db.executemany("""
        INSERT OR IGNORE INTO kline_1m (token_ca, pool_address, timestamp, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, [(token_ca, pool_address, c[0], c[1], c[2], c[3], c[4], c[5]) for c in unique])

    # 记录 fetch log
    db.execute("""
        INSERT INTO fetch_log (token_ca, fetched_at, candles_count, earliest_ts, latest_ts, status)
        VALUES (?, ?, ?, ?, ?, 'ok')
    """, (token_ca, int(time.time()), len(unique), unique[0][0], unique[-1][0]))

    db.commit()
    return len(unique)


def get_cached_klines(db, token_ca, start_ts=None, end_ts=None):
    """从缓存读取 K 线数据"""
    query = "SELECT timestamp, open, high, low, close, volume FROM kline_1m WHERE token_ca = ?"
    params = [token_ca]

    if start_ts:
        query += " AND timestamp >= ?"
        params.append(start_ts)
    if end_ts:
        query += " AND timestamp <= ?"
        params.append(end_ts)

    query += " ORDER BY timestamp ASC"
    return db.execute(query, params).fetchall()


def fetch_token_klines(db, token_ca, force=False, max_pages=5):
    """
    完整流程：获取一个 token 的 K 线
    1. 查缓存是否已有
    2. 没有就从 API 拉
    """
    # 检查是否已有缓存
    if not force:
        count = db.execute("SELECT COUNT(*) as c FROM kline_1m WHERE token_ca = ?", (token_ca,)).fetchone()['c']
        if count > 0:
            return count

    # 获取 pool address
    pool = get_pool_address(db, token_ca)
    if not pool:
        return 0

    time.sleep(0.4)  # DexScreener rate limit

    # 拉 K 线
    count = fetch_klines(db, token_ca, pool, max_pages)
    return count


def batch_fetch(token_cas, db_path=None, max_pages=3, progress_interval=10):
    """批量拉取多个 token 的 K 线"""
    db = init_db(db_path)

    total = len(token_cas)
    fetched = 0
    cached = 0
    errors = 0
    total_candles = 0

    for i, ca in enumerate(token_cas):
        # 检查缓存
        existing = db.execute("SELECT COUNT(*) as c FROM kline_1m WHERE token_ca = ?", (ca,)).fetchone()['c']
        if existing > 0:
            cached += 1
            total_candles += existing
            if (i + 1) % progress_interval == 0:
                print(f"  [{i+1}/{total}] 已拉 {fetched} / 缓存命中 {cached} / 错误 {errors} / 总K线 {total_candles}")
            continue

        # 拉 pool address
        pool = get_pool_address(db, ca)
        if not pool:
            errors += 1
            if (i + 1) % progress_interval == 0:
                print(f"  [{i+1}/{total}] 已拉 {fetched} / 缓存命中 {cached} / 错误 {errors} / 总K线 {total_candles}")
            time.sleep(0.3)
            continue

        time.sleep(0.4)

        # 拉 K 线
        count = fetch_klines(db, ca, pool, max_pages)
        if count > 0:
            fetched += 1
            total_candles += count
        else:
            errors += 1

        if (i + 1) % progress_interval == 0:
            print(f"  [{i+1}/{total}] 已拉 {fetched} / 缓存命中 {cached} / 错误 {errors} / 总K线 {total_candles}")

        time.sleep(0.5)  # GeckoTerminal rate limit

    print(f"\n  完成: 新拉 {fetched} / 缓存命中 {cached} / 错误 {errors} / 总K线 {total_candles}")
    db.close()


# ============================================================
# CLI 入口
# ============================================================
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("用法:")
        print("  python3 kline-fetcher.py <token_ca>           # 拉单个 token")
        print("  python3 kline-fetcher.py --batch <file.txt>   # 批量拉取")
        print("  python3 kline-fetcher.py --stats              # 查看缓存统计")
        sys.exit(1)

    db = init_db()

    if sys.argv[1] == '--stats':
        total_tokens = db.execute("SELECT COUNT(DISTINCT token_ca) FROM kline_1m").fetchone()[0]
        total_candles = db.execute("SELECT COUNT(*) FROM kline_1m").fetchone()[0]
        total_pools = db.execute("SELECT COUNT(*) FROM pool_mapping").fetchone()[0]
        print(f"K线缓存统计:")
        print(f"  Tokens: {total_tokens}")
        print(f"  K线总数: {total_candles}")
        print(f"  Pool 映射: {total_pools}")

        if total_tokens > 0:
            row = db.execute("""
                SELECT datetime(MIN(timestamp), 'unixepoch') as earliest,
                       datetime(MAX(timestamp), 'unixepoch') as latest
                FROM kline_1m
            """).fetchone()
            print(f"  时间范围: {row['earliest']} ~ {row['latest']}")

    elif sys.argv[1] == '--batch':
        if len(sys.argv) < 3:
            print("需要指定文件路径")
            sys.exit(1)
        with open(sys.argv[2]) as f:
            cas = [line.strip() for line in f if line.strip()]
        batch_fetch(cas)

    else:
        token_ca = sys.argv[1]
        print(f"拉取 {token_ca} 的 K 线...")
        pool = get_pool_address(db, token_ca)
        if pool:
            print(f"  Pool: {pool}")
            time.sleep(0.5)
            count = fetch_klines(db, token_ca, pool, max_pages=5)
            print(f"  获取 {count} 根 K 线")

            if count > 0:
                earliest = db.execute("SELECT MIN(timestamp) FROM kline_1m WHERE token_ca = ?", (token_ca,)).fetchone()[0]
                latest = db.execute("SELECT MAX(timestamp) FROM kline_1m WHERE token_ca = ?", (token_ca,)).fetchone()[0]
                print(f"  范围: {datetime.utcfromtimestamp(earliest)} ~ {datetime.utcfromtimestamp(latest)}")
                print(f"  跨度: {(latest - earliest) / 3600:.1f} 小时")
        else:
            print(f"  找不到 pool address")

    db.close()
