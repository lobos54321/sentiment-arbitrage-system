#!/usr/bin/env python3
"""
24-Hour Lifecycle Tracker — tracks full lifecycle of all NOT_ATH signals.
======================================================================
Collects ground-truth data: what happens to these coins over 24h?

Two modes:
  --track   Live monitoring + data collection (runs indefinitely)
  --analyze Analyze collected data and print insights

Strategies simulated (all start with $100, friction=0.35%):
  Strategy A:  SL-only (-50%), no active exit, 24h hold
  Strategy B:  SL=-50% + Target exit (+100%) + 24h hard stop
  Strategy C:  SL=-30% + Trailing stop (start@+20%, factor=0.90) + 24h
  Strategy D:  No SL, hold to 24h (max loss = -100%)
  Strategy E:  Canonical v2: SL=-3% + Trailing stop (start@+3%, factor=0.90) + 120min timeout
              (Verified: EV=+16.6%, WR=76%, Bootstrap CI [14.8%, 18.4%])

Data collected per token:
  - 5-minute OHLCV samples from GeckoTerminal (or close price)
  - Peak price, volume trajectory
  - Final outcome: multiple, zero, or survived
  - P&L at h=1,3,6,12,24 for each strategy
"""

import sqlite3
import json
import time
import subprocess
import sys
import os
import signal
import logging
import random
import os
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

# === Configuration ===
SENTIMENT_DB = os.environ.get('SENTIMENT_DB', '/tmp/sentiment.db')
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
LIFECYCLE_DB = os.environ.get('LIFECYCLE_DB', str(DATA_DIR / 'lifecycle_tracks.db'))
KLINE_DB = os.environ.get('KLINE_DB', str(DATA_DIR / 'kline_cache.db'))

# GeckoTerminal API
GECKO_BASE = "https://api.geckoterminal.com/api/v2/networks/solana/pools"

# Tracking params
POLL_INTERVAL_SEC = 300        # 5 minutes between price samples
MAX_HOLD_HOURS = 24
DEAD_THRESHOLD = -0.80        # -80% from entry → considered "dead"
MAX_CONCURRENT = 200          # max tokens tracked simultaneously
SOL_TARGET = 100              # approximate USD entry per trade

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
log = logging.getLogger('lifecycle')


# === Database Setup ===

def init_db(db_path=None):
    path = db_path or LIFECYCLE_DB
    os.makedirs(os.path.dirname(path), exist_ok=True)
    
    def _setup_schema(conn):
        conn.execute("PRAGMA journal_mode=WAL")
        conn.row_factory = sqlite3.Row
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tracks (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                token_ca        TEXT NOT NULL,
                symbol          TEXT,
                signal_ts       INTEGER NOT NULL,
                entry_price     REAL NOT NULL,
                entry_ts        INTEGER NOT NULL,
                pool_address    TEXT,
                status          TEXT DEFAULT 'active',  -- active|completed|dead|expired
                complete_ts     INTEGER,
                complete_reason TEXT,
                created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(token_ca, signal_ts)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS price_samples (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                track_id    INTEGER NOT NULL,
                ts          INTEGER NOT NULL,
                price       REAL,
                volume      REAL,
                -- ohlcv if available
                open_       REAL,
                high        REAL,
                low         REAL,
                close       REAL,
                FOREIGN KEY (track_id) REFERENCES tracks(id),
                UNIQUE(track_id, ts)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS strategy_results (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                track_id        INTEGER NOT NULL,
                strategy        TEXT NOT NULL,  -- A|B|C|D
                entry_price     REAL NOT NULL,
                exit_price      REAL,
                exit_ts        INTEGER,
                exit_reason    TEXT,
                pnl_pct        REAL,
                peak_pnl       REAL,
                bars_held      INTEGER,
                FOREIGN KEY (track_id) REFERENCES tracks(id)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_track_token  ON tracks(token_ca)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_track_status ON tracks(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sample_track ON price_samples(track_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_strat_track  ON strategy_results(track_id)")
        conn.commit()

    try:
        db = sqlite3.connect(path)
        _setup_schema(db)
    except sqlite3.DatabaseError as e:
        if "file is not a database" in str(e).lower() or "disk image is malformed" in str(e).lower():
            logging.getLogger('lifecycle').warning(f"DB corrupted ({e}), recreating {path}")
            if db:
                db.close()
            if os.path.exists(path):
                os.remove(path)
            db = sqlite3.connect(path)
            _setup_schema(db)
        else:
            raise

    return db


# === API Helpers ===

def curl_json(url, timeout=15):
    try:
        r = subprocess.run(['curl', '-s', '-m', str(timeout), url],
                          capture_output=True, text=True, timeout=timeout + 5)
        if r.returncode != 0:
            return None
        return json.loads(r.stdout)
    except Exception:
        return None


def get_pool_address(token_ca, cache={}):
    if token_ca in cache:
        return cache[token_ca]
    data = curl_json(f"https://api.dexscreener.com/latest/dex/tokens/{token_ca}")
    if not data:
        return None
    pairs = data.get('pairs', [])
    if not pairs:
        return None
    sol_pairs = [p for p in pairs if p.get('chainId') == 'solana'] or pairs
    pair = max(sol_pairs, key=lambda p: p.get('liquidity', {}).get('usd', 0) or 0)
    pool = pair.get('pairAddress', '')
    if pool:
        cache[token_ca] = pool
    return pool or None


def get_ohlcv(pool_address, aggregate=5, limit=288, before_timestamp=None):
    """Get OHLCV from GeckoTerminal. aggregate=5 means 5-minute candles.
    If before_timestamp is set, returns candles before that timestamp.
    """
    if not pool_address:
        return None
    url = (f"{GECKO_BASE}/{pool_address}/ohlcv/minute"
           f"?aggregate={aggregate}&limit={limit}")
    if before_timestamp:
        url += f"&before_timestamp={before_timestamp}"
    data = curl_json(url)
    if not data:
        return None
    ohlcv_list = (data.get('data', {}).get('attributes', {})
                  .get('ohlcv_list', []))
    # Each entry: [timestamp, open, high, low, close, volume]
    result = []
    for row in ohlcv_list:
        if len(row) >= 6 and row[4] and row[4] > 0:
            result.append({
                'ts': int(row[0]),
                'open': float(row[1]),
                'high': float(row[2]),
                'low': float(row[3]),
                'close': float(row[4]),
                'volume': float(row[5]),
            })
    return result if result else None


def get_current_price_info(token_ca, pool_address):
    """Get latest price + volume from pool."""
    if not pool_address:
        pool_address = get_pool_address(token_ca)
    if not pool_address:
        return None, None
    # Try OHLCV first (most recent candle)
    ohlcv = get_ohlcv(pool_address, aggregate=1, limit=1)
    if ohlcv:
        latest = ohlcv[-1]
        return latest['close'], latest.get('volume', 0)

    # Fallback: pool info endpoint
    url = f"{GECKO_BASE}/{pool_address}"
    data = curl_json(url)
    if not data:
        return None, None
    attrs = data.get('data', {}).get('attributes', {})
    price_str = attrs.get('base_asset_price_usd') or attrs.get('price_usd')
    vol_str = attrs.get('volume_usd', {}).get('h24', 0)
    try:
        price = float(price_str) if price_str else None
        volume = float(vol_str) if vol_str else 0
    except (TypeError, ValueError):
        price, volume = None, None
    return price, volume


# === Strategies ===

def apply_strategies(track_id, entry_price, samples):
    """
    Apply all 4 strategies to a completed track.
    samples: list of {ts, close, high, low} sorted by ts (oldest first)
    Entry is at samples[0] (or index 1 if we skip the first bar).
    """
    results = []

    # Filter samples that are AFTER entry (entry is at signal time, samples are after)
    if not samples:
        return results

    entry_ts = samples[0]['ts']

    for strategy_label, strat in [
        ('A', {'sl': -0.50, 'target': None, 'trail': None, 'timeout': MAX_HOLD_HOURS * 3600}),
        ('B', {'sl': -0.50, 'target': 1.00, 'trail': None, 'timeout': MAX_HOLD_HOURS * 3600}),
        ('C', {'sl': -0.30, 'target': None, 'trail': {'start': 0.20, 'factor': 0.90}, 'timeout': MAX_HOLD_HOURS * 3600}),
        ('D', {'sl': None,  'target': None, 'trail': None, 'timeout': MAX_HOLD_HOURS * 3600}),
        ('E', {'sl': -0.03, 'target': None, 'trail': {'start': 0.03, 'factor': 0.90}, 'timeout': 120 * 60}),
    ]:
        pnl, reason, peak_pnl, bars = simulate_strategy(
            entry_price, entry_ts, samples, **strat
        )
        results.append({
            'track_id': track_id,
            'strategy': strategy_label,
            'entry_price': entry_price,
            'exit_price': entry_price * (1 + pnl) if pnl is not None else None,
            'exit_ts': samples[-1]['ts'] if reason else None,
            'exit_reason': reason,
            'pnl_pct': pnl,
            'peak_pnl': peak_pnl,
            'bars_held': bars,
        })

    return results


def simulate_strategy(entry_price, entry_ts, samples,
                      sl=None, target=None, trail=None, timeout=None):
    """
    Returns (pnl_pct, exit_reason, peak_pnl, bars_held)
    """
    if not samples:
        return None, 'no_data', 0.0, 0

    peak_pnl = 0.0
    trailing_active = False
    trail_peak = 0.0
    bars = 0
    first_ts = samples[0]['ts']

    for i, s in enumerate(samples):
        bar_ts = s['ts']
        price = s.get('close') or s.get('price')
        if not price or price <= 0:
            continue

        pnl = (price - entry_price) / entry_price
        high_pnl = (s.get('high', price) - entry_price) / entry_price
        low_pnl  = (s.get('low', price) - entry_price) / entry_price

        # Peak tracking (use high of bar for trailing)
        if high_pnl > peak_pnl:
            peak_pnl = high_pnl

        # Activate trailing
        if trail and not trailing_active and peak_pnl >= trail['start']:
            trailing_active = True
            trail_peak = peak_pnl

        bars = i + 1

        # Timeout
        elapsed = bar_ts - first_ts
        if timeout and elapsed >= timeout:
            final_pnl = (samples[min(i, len(samples)-1)].get('close', price) - entry_price) / entry_price
            return final_pnl, 'timeout', peak_pnl, bars

        # Dead (only for A/B/C)
        if sl and low_pnl <= sl:
            return sl, 'sl', peak_pnl, bars

        # Target hit (only for B)
        if target and high_pnl >= target:
            return target, 'target', peak_pnl, bars

        # Trailing stop (only for C)
        if trail and trailing_active:
            trail_level = trail_peak * trail['factor']
            if pnl <= trail_level:
                exit_pnl = max(pnl, trail_level)
                return exit_pnl, 'trail', peak_pnl, bars

    # End of data — use last close
    last = samples[-1].get('close', entry_price)
    final_pnl = (last - entry_price) / entry_price
    return final_pnl, 'expired', peak_pnl, bars


# === Lifecycle Track ===

class Tracker:
    def __init__(self, db):
        self.db = db  # lifecycle_tracks.db
        self.sdb = sqlite3.connect(SENTIMENT_DB)  # sentiment.db
        self.sdb.row_factory = sqlite3.Row
        self.pool_cache = {}
        self.active = {}  # token_ca -> track row

    def get_untracked_signals(self, limit=50):
        """Find signals not yet tracked (query sentiment.db, filter with lifecycle.db)."""
        rows = self.sdb.execute("""
            SELECT token_ca, symbol, timestamp, description
            FROM premium_signals
            WHERE hard_gate_status IN ('NOT_ATH_V17', 'V17_NOT_ATH1', 'V18_SUPERCUR_FILTER')
              AND timestamp >= ?
            ORDER BY timestamp DESC
            LIMIT ?
        """, (
            int(datetime(2026, 3, 10, tzinfo=timezone.utc).timestamp() * 1000),
            limit * 3  # over-fetch, filter in python
        )).fetchall()

        # Filter out already-tracked ones using lifecycle db
        result = []
        for r in rows:
            if len(result) >= limit:
                break
            exists = self.db.execute(
                "SELECT 1 FROM tracks WHERE token_ca=? AND signal_ts=?",
                (r['token_ca'], r['timestamp'] // 1000 if r['timestamp'] > 1e12 else r['timestamp'])
            ).fetchone()
            if not exists:
                result.append(r)
        return result

    def start_track(self, token_ca, symbol, signal_ts_ms):
        """Begin tracking a new token."""
        # Get pool address
        pool = get_pool_address(token_ca, self.pool_cache)
        if not pool:
            return None
        time.sleep(0.3)

        # Get entry price from latest OHLCV
        ohlcv = get_ohlcv(pool, aggregate=5, limit=12)  # last hour of 5m candles
        if not ohlcv:
            # Fallback to current price
            price, _ = get_current_price_info(token_ca, pool)
            if not price:
                return None
            entry_price = price
        else:
            # Use the most recent candle close as entry
            entry_price = ohlcv[-1]['close']

        entry_ts = int(time.time())
        signal_ts_sec = signal_ts_ms // 1000 if signal_ts_ms > 1e12 else signal_ts_ms

        # ── Insert track first to get track_id ─────────────────────────────
        cursor = self.db.execute("""
            INSERT OR IGNORE INTO tracks
                (token_ca, symbol, signal_ts, entry_price, entry_ts, pool_address, status)
            VALUES (?, ?, ?, ?, ?, ?, 'active')
        """, (token_ca, symbol, signal_ts_sec, entry_price, entry_ts, pool))
        self.db.commit()

        if cursor.rowcount == 0:
            return None

        track_id = cursor.lastrowid

        # ── Save pre-signal K-lines (signal之前的历史) ───────────────────
        # Try 1-minute bars first, then 5-minute
        for agg, lim in [(1, 30), (5, 50)]:
            hist = get_ohlcv(pool, aggregate=agg, limit=lim,
                             before_timestamp=signal_ts_sec)
            if hist:
                for bar in hist:
                    if bar['ts'] < signal_ts_sec:
                        self.db.execute("""
                            INSERT OR IGNORE INTO price_samples
                                (track_id, ts, price, open_, high, low, close, volume)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, (track_id, bar['ts'], bar['close'],
                              bar['open'], bar['high'], bar['low'], bar['close'], bar['volume']))

        self.db.commit()
        log.info(f"  [+T] {symbol} entry=${entry_price:.8f} track={track_id}")
        return track_id

    def sample_price(self, track_id, pool_address):
        """Get latest price sample and save."""
        ohlcv = get_ohlcv(pool_address, aggregate=5, limit=6)
        if not ohlcv:
            return None

        latest = ohlcv[-1]
        self.db.execute("""
            INSERT OR IGNORE INTO price_samples
                (track_id, ts, price, open_, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (track_id, latest['ts'], latest['close'],
              latest['open'], latest['high'], latest['low'], latest['close'], latest['volume']))
        self.db.commit()
        return latest['close']

    def check_completion(self, track_id, token_ca, entry_price, entry_ts, pool_address):
        """Check if a track should be marked complete."""
        now = int(time.time())
        elapsed_h = (now - entry_ts) / 3600

        # Get latest price
        ohlcv = get_ohlcv(pool_address, aggregate=5, limit=6)
        if not ohlcv:
            return False, None

        latest = ohlcv[-1]
        current_price = latest['close']
        if current_price <= 0:
            return False, None

        pnl = (current_price - entry_price) / entry_price

        reason = None

        # Dead: -80% from entry
        if pnl <= DEAD_THRESHOLD:
            reason = 'dead'

        # Timeout: 24h
        elif elapsed_h >= MAX_HOLD_HOURS:
            reason = 'expired'

        if reason:
            self._complete_track(track_id, entry_price, entry_ts, reason)
            return True, reason

        return False, None

    def _complete_track(self, track_id, entry_price, entry_ts, reason):
        """Mark track complete, apply strategies, save results."""
        now = int(time.time())

        self.db.execute("""
            UPDATE tracks SET status=?, complete_ts=?, complete_reason=?
            WHERE id=?
        """, ('completed' if reason != 'dead' else 'dead', now, reason, track_id))

        # Get all samples for strategy simulation
        samples = self.db.execute("""
            SELECT ts, close, high, low FROM price_samples
            WHERE track_id=? ORDER BY ts
        """, (track_id,)).fetchall()

        if not samples:
            self.db.commit()
            return

        sample_list = [
            {'ts': s['ts'], 'close': s['close'],
             'high': s['high'], 'low': s['low']}
            for s in samples
        ]

        # Apply strategies
        strategy_results = apply_strategies(track_id, entry_price, sample_list)
        for r in strategy_results:
            self.db.execute("""
                INSERT INTO strategy_results
                    (track_id, strategy, entry_price, exit_price, exit_ts,
                     exit_reason, pnl_pct, peak_pnl, bars_held)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (r['track_id'], r['strategy'], r['entry_price'],
                  r['exit_price'], r['exit_ts'], r['exit_reason'],
                  r['pnl_pct'], r['peak_pnl'], r['bars_held']))

        self.db.commit()
        log.info(f"  [-T] track={track_id} reason={reason} "
                 f"[A={self._fmt(self._get_strat(r['pnl_pct'], strategy_results, 'A'))} "
                 f"B={self._fmt(self._get_strat(r['pnl_pct'], strategy_results, 'B'))} "
                 f"C={self._fmt(self._get_strat(r['pnl_pct'], strategy_results, 'C'))} "
                 f"D={self._fmt(self._get_strat(r['pnl_pct'], strategy_results, 'D'))}]")

    def _get_strat(self, target_pnl, results, label):
        for r in results:
            if r['strategy'] == label:
                return r['pnl_pct']
        return None

    def _fmt(self, v):
        if v is None:
            return 'N/A'
        return f"{v*100:+.1f}%"

    def close(self):
        self.sdb.close()

    def process_batch(self):
        """Main loop: start new tracks, sample prices, check completions."""
        # 1. Start new tracks
        signals = self.get_untracked_signals(limit=20)
        started = 0
        for sig in signals:
            if len(self.active) >= MAX_CONCURRENT:
                break
            tid = self.start_track(sig['token_ca'], sig['symbol'], sig['timestamp'])
            if tid:
                self.active[sig['token_ca']] = tid
                started += 1

        if started:
            log.info(f"[NEW] Started {started} new tracks. Active: {len(self.active)}")

        # 2. Sample prices and check completions for active tracks
        to_remove = []
        rows = self.db.execute(
            "SELECT id, token_ca, symbol, entry_price, entry_ts, pool_address, status "
            "FROM tracks WHERE status='active' ORDER BY entry_ts"
        ).fetchall()

        for r in rows:
            tid = r['id']
            ca = r['token_ca']
            if ca in self.active and self.active[ca] != tid:
                continue

            price = self.sample_price(tid, r['pool_address'])
            if price is None:
                time.sleep(0.3)
                continue

            done, reason = self.check_completion(
                tid, ca, r['entry_price'], r['entry_ts'], r['pool_address']
            )
            if done:
                to_remove.append(ca)

        for ca in to_remove:
            self.active.pop(ca, None)

        if to_remove:
            log.info(f"[DONE] Completed {len(to_remove)} tracks. Active: {len(self.active)}")


# === Analysis ===

def analyze(db):
    """Print analysis of collected lifecycle data."""
    import statistics

    rows = db.execute("""
        SELECT t.token_ca, t.symbol, t.entry_price, t.complete_reason,
               t.entry_ts, t.complete_ts,
               sr.strategy, sr.pnl_pct, sr.peak_pnl, sr.bars_held
        FROM tracks t
        JOIN strategy_results sr ON sr.track_id = t.id
        WHERE t.status IN ('completed', 'dead')
        ORDER BY t.entry_ts
    """).fetchall()

    if not rows:
        log.info("No completed tracks yet.")
        return

    print("\n" + "=" * 70)
    print("  24H LIFECYCLE ANALYSIS")
    print("=" * 70)

    # Group by strategy
    by_strat = defaultdict(list)
    for r in rows:
        by_strat[r['strategy']].append(r)

    print(f"\n  Total tracks analyzed: {len(set(r['token_ca'] for r in rows))}")
    print(f"  By strategy:")

    strat_evs = {}
    for label in ['A', 'B', 'C', 'D']:
        data = by_strat.get(label, [])
        if not data:
            continue
        pnls = [r['pnl_pct'] for r in data if r['pnl_pct'] is not None]
        peaks = [r['peak_pnl'] for r in data if r['peak_pnl'] is not None]
        n = len(pnls)
        if n == 0:
            continue
        ev = sum(pnls) / n
        wr = sum(1 for p in pnls if p > 0) / n
        avg_peak = sum(peaks) / len(peaks) if peaks else 0
        strat_evs[label] = ev

        # By exit reason
        by_reason = defaultdict(list)
        for r in data:
            if r['pnl_pct'] is not None:
                by_reason[r['exit_reason'] or 'unknown'].append(r['pnl_pct'])

        print(f"\n  Strategy {label} (n={n}):")
        print(f"    EV:   {ev*100:+.2f}%")
        print(f"    WR:   {wr*100:.1f}%")
        print(f"    Avg peak: {avg_peak*100:+.1f}%")
        print(f"    Exit reasons:")
        for reason, ps in sorted(by_reason.items()):
            r_ev = sum(ps) / len(ps)
            r_wr = sum(1 for p in ps if p > 0) / len(ps)
            print(f"      {reason:10s}  n={len(ps):3d}  EV={r_ev*100:+.1f}%  WR={r_wr*100:.1f}%")

    # Find best strategy
    if strat_evs:
        best = max(strat_evs, key=strat_evs.get)
        print(f"\n  Best strategy: {best} (EV={strat_evs[best]*100:+.2f}%)")

    # By complete reason (pool survivorship)
    print(f"\n  Survivorship:")
    reason_dist = defaultdict(int)
    for r in rows:
        reason_dist[r['complete_reason'] or 'unknown'] += 1
    for reason, cnt in sorted(reason_dist.items()):
        print(f"    {reason:10s}  {cnt}")

    print("=" * 70)


# === Main ===

def main():
    db = init_db()
    tracker = Tracker(db)

    if '--analyze' in sys.argv:
        analyze(db)
        db.close()
        return

    log.info("=== 24H Lifecycle Tracker Started ===")
    log.info(f"  Poll interval: {POLL_INTERVAL_SEC}s")
    log.info(f"  Max hold: {MAX_HOLD_HOURS}h")
    log.info(f"  Dead threshold: {DEAD_THRESHOLD*100:.0f}%")
    log.info(f"  DB: {LIFECYCLE_DB}")

    # Count active
    active = db.execute("SELECT COUNT(*) as c FROM tracks WHERE status='active'").fetchone()['c']
    log.info(f"  Resuming with {active} active tracks")

    try:
        while True:
            tracker.process_batch()
            time.sleep(POLL_INTERVAL_SEC)
    except KeyboardInterrupt:
        log.info("Shutting down...")
    finally:
        tracker.close()
        db.close()


if __name__ == '__main__':
    main()
