#!/usr/bin/env python3
"""
Watchlist Store — SQLite persistence layer for the observation + 5-matrix strategy.

Manages token lifecycle:  watching → holding → moon_bag → expired
                                  ↑__________________________|  (re-entry)

Each entry tracks signal metadata, matrix scores, trade history, and dynamic
state (lowest/highest price, ATH flags, signal count progression).
"""

import sqlite3
import json
import time
import logging
import os
from pathlib import Path

log = logging.getLogger('watchlist')

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
WATCHLIST_DB = os.environ.get('WATCHLIST_DB', str(DATA_DIR / 'watchlist.db'))


# ─── Schema ────────────────────────────────────────────────────────────────

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS watchlist (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ca              TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    pool_address    TEXT,
    type            TEXT NOT NULL DEFAULT 'NOT_ATH',   -- NOT_ATH | ATH | ATH_FOLLOWUP

    -- Signal snapshot (recorded at first registration)
    signal_ts       INTEGER,
    premium_signal_id INTEGER,
    signal_price    REAL,
    signal_mc       REAL,
    signal_super    INTEGER DEFAULT 0,
    signal_holders  INTEGER DEFAULT 0,
    signal_vol24h   REAL DEFAULT 0,
    signal_tx24h    INTEGER DEFAULT 0,
    signal_top10    REAL DEFAULT 0,

    -- Dynamic state (updated as new signals arrive / prices change)
    added_at        REAL NOT NULL,
    signal_count    INTEGER DEFAULT 1,
    latest_super    INTEGER DEFAULT 0,
    has_ath         INTEGER DEFAULT 0,                -- boolean flag
    lowest_price    REAL,
    highest_price   REAL,
    last_eval_at    REAL DEFAULT 0,
    last_scores_json TEXT DEFAULT '{}',

    -- Trade history
    entry_count     INTEGER DEFAULT 0,                -- how many times we've entered
    last_exit_pnl   REAL,                             -- PnL of last exit
    last_exit_at    REAL,                              -- timestamp of last exit
    last_exit_price REAL,                              -- price at last exit (for re-entry validation)
    cooldown_until  REAL DEFAULT 0,                   -- re-entry cooldown

    -- Position info (when holding)
    entry_price     REAL,
    entry_time      REAL,
    peak_pnl        REAL DEFAULT 0,
    position_size_sol REAL,
    token_amount_raw TEXT,
    token_decimals  INTEGER DEFAULT 0,
    trade_id        INTEGER,                           -- reference to paper_trades.id
    has_locked_profit INTEGER DEFAULT 0,               -- whether 50% has been sold
    moon_peak_pnl   REAL DEFAULT 0,
    moon_start_time REAL,
    moon_trend_zero_count INTEGER DEFAULT 0,
    zero_vol_count  INTEGER DEFAULT 0,
    dynamic_sl      REAL DEFAULT -0.075,               -- current dynamic stop-loss (-7.5%)
    trailing_active INTEGER DEFAULT 0,
    last_matrix_check REAL DEFAULT 0,
    moon_trail_factor REAL DEFAULT 0.2,                -- P4: dynamic trail factor (velocity ratchet)
    consecutive_losses INTEGER DEFAULT 0,              -- P2: consecutive loss count for cooldown
    last_loss_time  REAL DEFAULT 0,                    -- P2: timestamp of last loss

    -- Lifecycle
    status          TEXT NOT NULL DEFAULT 'watching',  -- watching | pending_momentum | holding | moon_bag | expired
    expire_reason   TEXT,

    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(ca, status) -- only one active entry per token per status
);
"""

CREATE_INDEX_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_wl_status ON watchlist(status)",
    "CREATE INDEX IF NOT EXISTS idx_wl_ca ON watchlist(ca)",
    "CREATE INDEX IF NOT EXISTS idx_wl_added ON watchlist(added_at)",
]


# ─── Store Class ───────────────────────────────────────────────────────────

class WatchlistStore:
    """SQLite persistence for the observation watchlist.
    
    NOTE: Not internally thread-safe. Relies on CPython GIL for basic
    atomicity. Concurrent heavy writes from multiple threads may cause
    'database is locked' errors with SQLite.
    """

    def __init__(self, db_path=None):
        self.db_path = db_path or WATCHLIST_DB
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.db = sqlite3.connect(self.db_path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self._init_schema()
        log.info(f"[WatchlistStore] initialized: {self.db_path}")

    def _init_schema(self):
        self.db.execute(CREATE_TABLE_SQL)
        for idx_sql in CREATE_INDEX_SQL:
            self.db.execute(idx_sql)
        # Safe schema migration for new columns
        _migrate_columns = [
            ("ALTER TABLE watchlist ADD COLUMN entry_execution_json TEXT", None),
            ("ALTER TABLE watchlist ADD COLUMN exit_execution_json TEXT", None),
            # Fix 4: track ATH peaks separately to preserve original signal_price
            ("ALTER TABLE watchlist ADD COLUMN latest_ath_price REAL DEFAULT NULL", None),
            # P4: dynamic trail factor for moon bags
            ("ALTER TABLE watchlist ADD COLUMN moon_trail_factor REAL DEFAULT 0.2", None),
            # P2: loss cooldown tracking
            ("ALTER TABLE watchlist ADD COLUMN consecutive_losses INTEGER DEFAULT 0", None),
            ("ALTER TABLE watchlist ADD COLUMN last_loss_time REAL DEFAULT 0", None),
            ("ALTER TABLE watchlist ADD COLUMN last_exit_price REAL DEFAULT NULL", None),
        ]
        for col_sql, _ in _migrate_columns:
            try:
                self.db.execute(col_sql)
            except sqlite3.OperationalError:
                pass
        self.db.commit()

    # ─── Registration ──────────────────────────────────────────────────

    def register(self, ca, symbol, signal_type, pool_address=None,
                 signal_ts=None, premium_signal_id=None,
                 signal_price=None, signal_mc=None, signal_super=0,
                 signal_holders=0, signal_vol24h=0, signal_tx24h=0,
                 signal_top10=0):
        """
        Register a new token to the watchlist, or update an existing one.
        Returns the watchlist entry dict.
        """
        now = time.time()

        # Check if already watching/holding this token
        existing = self.get_by_ca(ca)
        if existing:
            # Update existing entry with new signal info
            updates = {'signal_count': existing['signal_count'] + 1}
            if signal_super > 0:
                updates['latest_super'] = signal_super
            if signal_type == 'ATH':
                updates['has_ath'] = 1
                # Fix 4: record ATH peak price separately — never overwrite original signal_price
                # so score_price always compares current price to the NOT_ATH anchor
                if signal_price and signal_price > 0:
                    updates['latest_ath_price'] = signal_price
                if existing['type'] == 'NOT_ATH':
                    updates['type'] = 'ATH'  # upgrade type
            self._update(existing['id'], **updates)
            log.info(
                f"[WL] Updated ${symbol} signal_count={updates.get('signal_count')} "
                f"has_ath={updates.get('has_ath', existing['has_ath'])} "
                f"type={updates.get('type', existing['type'])}"
            )
            return self.get_by_id(existing['id'])

        # Check if previously expired — allow re-registration for ATH upgrades
        expired = self.db.execute(
            "SELECT id FROM watchlist WHERE ca = ? AND status = 'expired' ORDER BY id DESC LIMIT 1",
            (ca,)
        ).fetchone()
        if expired and signal_type == 'ATH':
            # Re-activate expired entry
            self._update(expired['id'],
                         status='watching', type='ATH', has_ath=1,
                         signal_count=1, latest_super=signal_super,
                         added_at=now, last_eval_at=0, expire_reason=None,
                         entry_count=0, cooldown_until=0,
                         lowest_price=signal_price, highest_price=signal_price)
            log.info(f"[WL] Re-activated expired ${symbol} as ATH")
            return self.get_by_id(expired['id'])

        # New registration
        try:
            self.db.execute("""
                INSERT INTO watchlist
                    (ca, symbol, type, pool_address,
                     signal_ts, premium_signal_id,
                     signal_price, signal_mc, signal_super,
                     signal_holders, signal_vol24h, signal_tx24h, signal_top10,
                     added_at, latest_super, has_ath,
                     lowest_price, highest_price, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'watching')
            """, (
                ca, symbol, signal_type, pool_address,
                signal_ts, premium_signal_id,
                signal_price, signal_mc, signal_super,
                signal_holders, signal_vol24h, signal_tx24h, signal_top10,
                now, signal_super, 1 if signal_type == 'ATH' else 0,
                signal_price, signal_price,
            ))
            self.db.commit()
            entry_id = self.db.execute('SELECT last_insert_rowid()').fetchone()[0]
            log.info(
                f"[WL] Registered ${symbol} type={signal_type} "
                f"MC=${signal_mc or 0:.0f} Super={signal_super} "
                f"Holders={signal_holders}"
            )
            return self.get_by_id(entry_id)
        except sqlite3.IntegrityError:
            # Race condition: entry was created between our check and insert
            return self.get_by_ca(ca)

    # ─── Queries ───────────────────────────────────────────────────────

    def get_by_ca(self, ca):
        """Get the active (non-expired) watchlist entry for a token."""
        row = self.db.execute(
            "SELECT * FROM watchlist WHERE ca = ? AND status != 'expired' ORDER BY id DESC LIMIT 1",
            (ca,)
        ).fetchone()
        return dict(row) if row else None

    def get_by_id(self, entry_id):
        row = self.db.execute(
            "SELECT * FROM watchlist WHERE id = ?", (entry_id,)
        ).fetchone()
        return dict(row) if row else None

    def get_watching(self):
        """Get all entries in 'watching' status, ordered by added_at."""
        rows = self.db.execute(
            "SELECT * FROM watchlist WHERE status = 'watching' ORDER BY added_at"
        ).fetchall()
        return [dict(r) for r in rows]

    def get_holding(self):
        """Get all entries in 'holding' status."""
        rows = self.db.execute(
            "SELECT * FROM watchlist WHERE status = 'holding' ORDER BY entry_time"
        ).fetchall()
        return [dict(r) for r in rows]

    def get_moon_bags(self):
        """Get all entries in 'moon_bag' status."""
        rows = self.db.execute(
            "SELECT * FROM watchlist WHERE status = 'moon_bag' ORDER BY moon_start_time"
        ).fetchall()
        return [dict(r) for r in rows]

    def get_active_count(self):
        """Count of all non-expired, non-watching positions (holding + moon_bag)."""
        row = self.db.execute(
            "SELECT COUNT(*) as c FROM watchlist WHERE status IN ('holding', 'moon_bag')"
        ).fetchone()
        return row['c'] if row else 0

    def get_all_active(self):
        """Get all non-expired entries."""
        rows = self.db.execute(
            "SELECT * FROM watchlist WHERE status != 'expired' ORDER BY added_at"
        ).fetchall()
        return [dict(r) for r in rows]

    def get_recent_closed_trades(self, limit=50):
        """P3: Fetch recent closed trades with exit PnL for Kelly odds calculation.
        Returns list of dicts with 'exit_pnl' and 'closed_at' keys."""
        rows = self.db.execute('''
            SELECT last_exit_pnl as exit_pnl,
                   datetime(last_exit_at, 'unixepoch') as closed_at
            FROM watchlist
            WHERE status IN ('watching', 'expired') AND last_exit_pnl IS NOT NULL
            ORDER BY last_exit_at DESC LIMIT ?
        ''', (limit,)).fetchall()
        return [{'exit_pnl': r['exit_pnl'], 'closed_at': r['closed_at'] or ''} for r in rows]

    def get_recent_avg_peak_pnl(self, limit=20):
        """A2: Get average peak_pnl from recent trades for ATR-adaptive stop-loss.
        Uses paper_trades table — peak_pnl is recorded at close time.
        Returns float average (e.g., 0.15 = 15% avg peak), or None if insufficient data.
        """
        try:
            rows = self.db.execute('''
                SELECT peak_pnl FROM paper_trades
                WHERE peak_pnl IS NOT NULL AND peak_pnl > 0
                ORDER BY rowid DESC LIMIT ?
            ''', (limit,)).fetchall()
            if len(rows) < 5:  # Need at least 5 trades for meaningful average
                return None
            avg = sum(r['peak_pnl'] for r in rows) / len(rows)
            return avg
        except Exception:
            return None

    # ─── State Transitions ─────────────────────────────────────────────

    def mark_holding(self, entry_id, entry_price, position_size_sol,
                     token_amount_raw, token_decimals, trade_id,
                     initial_sl=-0.075):
        """Transition from watching → holding after buy execution.
        initial_sl: A2 adaptive stop-loss (default -7.5%, overridden by get_adaptive_stop_loss())
        """
        now = time.time()
        self._update(entry_id,
                     status='holding',
                     entry_price=entry_price,
                     entry_time=now,
                     peak_pnl=0,
                     position_size_sol=position_size_sol,
                     token_amount_raw=str(token_amount_raw),
                     token_decimals=token_decimals,
                     trade_id=trade_id,
                     has_locked_profit=0,
                     trailing_active=0,
                     dynamic_sl=initial_sl,
                     zero_vol_count=0,
                     moon_trail_factor=0.2,
                     last_matrix_check=now,
                     entry_count_delta=1)  # increment entry_count
        log.info(f"[WL] → holding (entry_id={entry_id} trade_id={trade_id} sl={initial_sl*100:.1f}%)")

    def mark_moon_bag(self, entry_id, moon_peak_pnl):
        """Transition from holding → moon_bag after 50% profit lock."""
        now = time.time()
        self._update(entry_id,
                     status='moon_bag',
                     has_locked_profit=1,
                     moon_peak_pnl=moon_peak_pnl,
                     moon_start_time=now,
                     moon_trend_zero_count=0,
                     last_matrix_check=now)
        log.info(f"[WL] → moon_bag (entry_id={entry_id})")

    def mark_watching(self, entry_id, exit_pnl, cooldown_sec=300):
        """Transition from holding/moon_bag → watching (re-observation after exit).
        P5: Default 5-min cooldown for ALL exits (win or loss) to prevent
        'win→instant re-buy→loss' pattern. Loss cooldown overrides to longer.
        """
        now = time.time()

        # Save current entry_price as last_exit_price before clearing
        # (fixes bug where entry_price=None broke re-entry price validation)
        existing = self.get_by_id(entry_id)
        last_exit_price_val = existing.get('entry_price') if existing else None

        # P2: Track consecutive losses for cooldown
        prev_consec = (existing.get('consecutive_losses', 0) or 0) if existing else 0

        if exit_pnl is not None and exit_pnl < 0:
            # Loss: increment consecutive loss counter
            new_consec = prev_consec + 1
            loss_time = now
            # Dynamic cooldown: 1 loss → 15min, 2+ losses → 2h
            if new_consec >= 2:
                cooldown_sec = 7200   # 2 hours
            else:
                cooldown_sec = 1800   # 30 minutes (was 15min — too short for meme dumps)
            log.info(f"[WL] Loss cooldown: consecutive_losses={new_consec} → cooldown={cooldown_sec}s")
        else:
            # Profit or breakeven: reset consecutive loss counter
            new_consec = 0
            loss_time = 0

        self._update(entry_id,
                     status='watching',
                     last_exit_pnl=exit_pnl,
                     last_exit_at=now,
                     last_exit_price=last_exit_price_val,
                     cooldown_until=now + cooldown_sec,
                     entry_price=None,
                     entry_time=None,
                     peak_pnl=0,
                     trade_id=None,
                     has_locked_profit=0,
                     trailing_active=0,
                     dynamic_sl=-0.075,
                     zero_vol_count=0,
                     moon_peak_pnl=0,
                     moon_start_time=None,
                     moon_trend_zero_count=0,
                     moon_trail_factor=0.2,
                     consecutive_losses=new_consec,
                     last_loss_time=loss_time,
                     last_eval_at=0)
        log.info(f"[WL] → watching (re-observation, exit_pnl={exit_pnl:.1%}, consec_losses={new_consec})")

    def mark_expired(self, entry_id, reason):
        """Remove token from active watchlist."""
        # Clean up any old expired records for the same ca to avoid UNIQUE(ca, status) conflict
        entry = self.get_by_id(entry_id)
        if entry:
            self.db.execute(
                "DELETE FROM watchlist WHERE ca = ? AND status = 'expired' AND id != ?",
                (entry['ca'], entry_id)
            )
        self._update(entry_id, status='expired', expire_reason=reason)
        log.info(f"[WL] → expired (entry_id={entry_id} reason={reason})")

    # ─── Updates ───────────────────────────────────────────────────────

    def update_scores(self, entry_id, scores_dict, eval_time=None):
        """Update the last matrix evaluation scores."""
        self._update(entry_id,
                     last_scores_json=json.dumps(scores_dict),
                     last_eval_at=eval_time or time.time())

    def update_price_bounds(self, entry_id, current_price):
        """Update lowest/highest price bounds during observation."""
        entry = self.get_by_id(entry_id)
        if not entry:
            return
        updates = {}
        if entry['lowest_price'] is None or current_price < entry['lowest_price']:
            updates['lowest_price'] = current_price
        if entry['highest_price'] is None or current_price > entry['highest_price']:
            updates['highest_price'] = current_price
        if not entry.get('signal_price') or entry.get('signal_price') <= 0:
            updates['signal_price'] = current_price
        if updates:
            self._update(entry_id, **updates)

    def update_position_state(self, entry_id, **kwargs):
        """Generic update for position-related fields during holding/moon_bag."""
        allowed = {
            'peak_pnl', 'trailing_active', 'dynamic_sl',
            'zero_vol_count', 'moon_peak_pnl', 'moon_trend_zero_count',
            'last_matrix_check',
        }
        filtered = {k: v for k, v in kwargs.items() if k in allowed}
        if filtered:
            self._update(entry_id, **filtered)

    # ─── Cleanup ───────────────────────────────────────────────────────

    def cleanup_old_expired(self, max_age_hours=48):
        """Remove expired entries older than max_age_hours."""
        cutoff = time.time() - max_age_hours * 3600
        self.db.execute(
            "DELETE FROM watchlist WHERE status = 'expired' AND added_at < ?",
            (cutoff,)
        )
        self.db.commit()

    # ─── Statistics ────────────────────────────────────────────────────

    def stats(self):
        """Return a summary dict of watchlist state."""
        rows = self.db.execute("""
            SELECT status, COUNT(*) as c FROM watchlist GROUP BY status
        """).fetchall()
        result = {r['status']: r['c'] for r in rows}
        result['total'] = sum(result.values())
        return result

    # ─── Internal ──────────────────────────────────────────────────────

    def _update(self, entry_id, entry_count_delta=0, **kwargs):
        """Generic update helper. Handles entry_count increment specially."""
        if entry_count_delta:
            kwargs_sql = ', '.join(f"{k} = ?" for k in kwargs)
            if kwargs_sql:
                kwargs_sql += ', '
            kwargs_sql += 'entry_count = entry_count + ?'
            values = list(kwargs.values()) + [entry_count_delta, entry_id]
            self.db.execute(
                f"UPDATE watchlist SET {kwargs_sql} WHERE id = ?",
                values
            )
        elif kwargs:
            set_clause = ', '.join(f"{k} = ?" for k in kwargs)
            values = list(kwargs.values()) + [entry_id]
            self.db.execute(
                f"UPDATE watchlist SET {set_clause} WHERE id = ?",
                values
            )
        self.db.commit()

    def close(self):
        if self.db:
            self.db.close()
