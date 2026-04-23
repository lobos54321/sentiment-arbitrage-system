#!/usr/bin/env python3
"""
Matrix Evaluator — Five-dimension scoring engine for watchlist entry decisions.

Matrices:
  ① Trend Direction     — K-line shape analysis (reuses check_multi_bar_trend)
  ② Volume Momentum     — Recent vs historical volume ratio + 24h tx count
  ③ Price Strength      — Growth from signal price + recovery from dip
  ④ Realtime Momentum   — 3×3-second ascending price snapshots (final trigger only)
  ⑤ Signal Evolution    — Signal count growth, ATH appearance, Super Index delta

Entry thresholds:
  NOT_ATH: 4/5 matrices >= 60, none = 0, max observation 2h
  ATH:     3/5 matrices >= 60, ① ③ required, max observation 30min
"""

import time
import logging

log = logging.getLogger('matrix')

# Import existing analysis functions from paper_trade_monitor
# These will be imported at runtime to avoid circular imports
_trend_fn = None
_bars_fn = None
_price_fn = None
_dex_volume_fn = None
_dex_trend_fn = None      # P1: DexScreener trend snapshot for volume scoring


def _lazy_import():
    """Lazy import to avoid circular dependency with paper_trade_monitor."""
    global _trend_fn, _bars_fn, _price_fn, _dex_volume_fn, _dex_trend_fn
    if _trend_fn is None:
        from paper_trade_monitor import (
            check_multi_bar_trend,
            get_notath_bars,
            fetch_realtime_price,
        )
        _trend_fn = check_multi_bar_trend
        _bars_fn = get_notath_bars
        _price_fn = fetch_realtime_price
        # DexScreener volume — may not exist yet, gracefully handle
        try:
            from paper_trade_monitor import fetch_dexscreener_volume
            _dex_volume_fn = fetch_dexscreener_volume
        except ImportError:
            _dex_volume_fn = None
        # P1: DexScreener trend snapshot for real-time volume scoring
        try:
            from entry_engine import fetch_dexscreener_trend_snapshot
            _dex_trend_fn = fetch_dexscreener_trend_snapshot
        except ImportError:
            try:
                from paper_trade_monitor import fetch_dexscreener_trend_snapshot
                _dex_trend_fn = fetch_dexscreener_trend_snapshot
            except ImportError:
                _dex_trend_fn = None


# ─── Individual Matrix Scorers ─────────────────────────────────────────────

def score_trend(bars, symbol, token_ca=None, pool_address=None):
    """
    Matrix ① — Trend Direction (reverted to 84% win rate period)
    
    Uses OLS linear regression on K-line close prices (check_multi_bar_trend).
    84% period scoring:
      norm_slope > +0.15 → T=100 (clear uptrend, passed_shape)
      norm_slope < -0.15 → T=0   (downtrend)
      else               → T=50  (sideways / insufficient)
    
    Data source: synthetic K-line bars built from price polling (~10s intervals).
    Each 1-minute bar gets ~6 price observations — enough for OHLC + regression.

    Returns: (score: int 0-100, reason: str, detail: str)
    """
    _lazy_import()

    if not bars or len(bars) < 3:
        return 50, 'insufficient_bars', 'not enough bars for regression (fail-open)'

    trend_ok, reason, detail = _trend_fn(bars, symbol)

    if trend_ok:
        if reason == 'passed_shape':
            return 100, reason, detail
        else:
            return 50, reason, detail
    else:
        return 0, reason, detail


def score_volume(bars, signal_tx24h=0, signal_vol24h=0, token_ca=None, pool_address=None):
    """
    Matrix ② — Volume Momentum
    
    Data priority:
    1. K-line bars volume (compare recent vs avg) — if kline_cache has data
    2. DexScreener real-time volume — fallback for new tokens
    3. Signal's initial volume_24h — last-resort baseline

    Returns: (score: int 0-100, reason: str)
    """
    _lazy_import()

    # --- Path 0 (P1): DexScreener 5-min real trade data (most reliable) ---
    # Uses the same proven data source as SmartEntry: real buys_m5/sells_m5/vol_m5/vol_h1
    # instead of synthetic observation counts from price polling.
    if token_ca and callable(_dex_trend_fn):
        try:
            trend_data = _dex_trend_fn(token_ca)
            if trend_data:
                buys = trend_data.get('buys_m5', 0) or 0
                sells = trend_data.get('sells_m5', 0) or 0
                vol_m5 = trend_data.get('vol_m5', 0) or 0
                vol_h1 = trend_data.get('vol_h1', 0) or 0
                total_txns = buys + sells
                h1_avg_m5 = vol_h1 / 12.0 if vol_h1 > 0 else 0
                vol_ratio = vol_m5 / h1_avg_m5 if h1_avg_m5 > 0 else 0

                if total_txns >= 300 and vol_ratio >= 2.0:
                    return 100, f'dex_trend_strong txns={total_txns} ratio={vol_ratio:.1f} buys={buys} sells={sells}'
                elif total_txns >= 100 and vol_ratio >= 1.2:
                    return 70, f'dex_trend_moderate txns={total_txns} ratio={vol_ratio:.1f} buys={buys} sells={sells}'
                elif total_txns >= 50 and vol_ratio >= 0.8:
                    return 40, f'dex_trend_flat txns={total_txns} ratio={vol_ratio:.1f} buys={buys} sells={sells}'
                else:
                    # m5 txns too low — try h1 as fallback before returning V=0.
                    # Data: SOLTARD m5=8 txns → V=0 for 30min, but h1=271 txns.
                    # New coins often have sparse m5 data but active h1.
                    buys_h1 = trend_data.get('buys_h1', 0) or 0
                    sells_h1 = trend_data.get('sells_h1', 0) or 0
                    total_h1 = buys_h1 + sells_h1
                    if total_h1 >= 250 and vol_h1 > 0:
                        return 70, f'dex_h1_strong h1_txns={total_h1} vol_h1=${vol_h1:.0f} (m5_txns={total_txns} too sparse)'
                    elif total_h1 >= 200 and vol_h1 > 0:
                        return 60, f'dex_h1_fallback h1_txns={total_h1} vol_h1=${vol_h1:.0f} (m5_txns={total_txns} too sparse)'
                    elif total_h1 >= 80 and vol_h1 > 0:
                        return 40, f'dex_h1_moderate h1_txns={total_h1} vol_h1=${vol_h1:.0f} (m5_txns={total_txns} too sparse)'
                    else:
                        return 0, f'dex_trend_weak txns={total_txns} h1_txns={total_h1} ratio={vol_ratio:.1f} buys={buys} sells={sells}'
        except Exception as e:
            logging.getLogger('matrix_evaluator').warning(f"dex_trend_weak parsing failed: {e}")
            pass  # fall through to existing paths

    # --- Path 1: DexScreener real-time volume (via fetch_dexscreener_volume) ---
    # NOTE: Path 1 (kline_cache bars volume) removed 2026-04-20.
    # kline_cache.db was stale for 18+ days. Path 0 (DexScreener trend) is the
    # primary source and always executes first. This path is the fallback.
    if token_ca:
        try:
            dex_data = _dex_volume_fn(token_ca) if callable(globals().get('_dex_volume_fn')) else None
            if dex_data and isinstance(dex_data, dict):
                vol_usd = dex_data.get('volume_usd', 0) or 0
                txns = dex_data.get('txns', 0) or 0
                if vol_usd > 50000 and txns >= 300:
                    return 100, f'dex_strong vol=${vol_usd:.0f} txns={txns}'
                elif vol_usd > 20000 and txns >= 100:
                    return 70, f'dex_moderate vol=${vol_usd:.0f} txns={txns}'
                elif vol_usd > 5000:
                    return 40, f'dex_flat vol=${vol_usd:.0f} txns={txns}'
                elif vol_usd > 0:
                    return 0, f'dex_weak vol=${vol_usd:.0f} txns={txns}'
        except Exception as e:
            logging.getLogger('matrix_evaluator').warning(f"dex_weak parsing failed: {e}")
            pass
    
    # --- Path 3: Use signal's initial volume ---
    vol24h = signal_vol24h or 0
    if vol24h > 50000:
        return 70, f'signal_vol24h=${vol24h:.0f} (strong initial)'
    elif vol24h > 20000:
        return 50, f'signal_vol24h=${vol24h:.0f} (moderate initial)'
    elif vol24h > 0:
        return 40, f'signal_vol24h=${vol24h:.0f} (weak initial)'
    
    return 50, 'no_volume_data (fail-open)'


def score_price_strength(current_price, signal_price, lowest_price, latest_ath_price=None):
    """
    Matrix ③ — Price Strength (reverted to 84% win rate period logic)
    
    84% period scoring:
      0% ≤ growth ≤ 50%  + recovery ≥ 5%   → P = 100 (healthy)
      0% ≤ growth ≤ 100% + recovery ≥ 3%   → P = 70  (fast)
      growth < 0%         + recovery ≥ 10%  → P = 80  (V-bounce)
      growth > 100%                         → P = 30  (overextended)
      growth < 0%         + recovery < 5%   → P = 0   (bottom)
      else                                  → P = 40  (marginal)

    Returns: (score: int 0-100, reason: str)
    """
    if not current_price or current_price <= 0:
        return 0, 'no_price'
    if not signal_price or signal_price <= 0:
        return 50, 'no_signal_price'

    growth_pct = ((current_price - signal_price) / signal_price) * 100

    recovery_pct = 0
    if lowest_price and lowest_price > 0:
        recovery_pct = ((current_price - lowest_price) / lowest_price) * 100

    # Healthy initial growth zone (0 - 50%)
    if 0 <= growth_pct <= 50 and recovery_pct >= 5:
        return 100, f'healthy growth={growth_pct:+.1f}% recovery={recovery_pct:.1f}%'

    # Fast growth (0 - 100%) — wider band than healthy
    if 0 <= growth_pct <= 100 and recovery_pct >= 3:
        return 70, f'fast_growth growth={growth_pct:+.1f}% recovery={recovery_pct:.1f}%'

    # V-bounce from below signal price
    if growth_pct < 0 and recovery_pct >= 10:
        return 80, f'v_bounce growth={growth_pct:+.1f}% recovery={recovery_pct:.1f}%'

    # Overextended (> 100%)
    if growth_pct > 100:
        return 30, f'overextended growth={growth_pct:+.1f}%'

    # Bottom / weak bounce
    if growth_pct < 0 and recovery_pct < 5:
        return 0, f'bottom growth={growth_pct:+.1f}% recovery={recovery_pct:.1f}%'

    # Default: marginal
    return 40, f'marginal growth={growth_pct:+.1f}% recovery={recovery_pct:.1f}%'


MIN_MOMENTUM_MOVE_PCT = 1.5  # 9s minimum move: 1.5%
# Data-driven: in 6h audit, all FIRE passes had <1% 6s move (noise), max observed
# meme coin 6s move was +3.69%. 5% would block ALL entries including Wifejak (+484%).
# 1.5% filters pure noise while allowing legitimate trend momentum through.
# Upgraded: 3×3s=9s window (accelerated from 5x3s to reduce system latency).


def score_realtime_momentum(token_ca, pool_address, interval_sec=3):
    """
    Matrix ④ — Realtime Momentum (3×3-second snapshots = 9s window)
    Only called when matrices ①②③⑤ are already passing.

    Requires: price must move UP by at least MIN_MOMENTUM_MOVE_PCT (1.5%) over 9 seconds.
    Uses 3 samples to dramatically cut entry latency.

    Returns: (score: int 0-100, reason: str, snapshots: list)
    """
    _lazy_import()

    snapshots = []
    for i in range(3):
        if i > 0:
            time.sleep(interval_sec)
        price, src, age_ms = _price_fn(token_ca, pool_address)
        if price and price > 0:
            snapshots.append(price)

    if len(snapshots) < 3:
        return 0, 'insufficient_snapshots', snapshots

    s_first = snapshots[0]
    s_last = snapshots[-1]
    s_max = max(snapshots)
    s_min = min(snapshots)

    if s_first <= 0:
        return 0, 'zero_base_price', snapshots

    pct_move = ((s_last - s_first) / s_first) * 100
    pct_max = ((s_max - s_first) / s_first) * 100

    # Count how many consecutive rises we see
    rises = sum(1 for i in range(1, len(snapshots)) if snapshots[i] > snapshots[i-1])
    snap_str = ' '.join(f'{s:.10f}' for s in snapshots)

    # Strong ascending: mostly rising, last > first by threshold
    if rises >= 2 and pct_move >= MIN_MOMENTUM_MOVE_PCT:
        return 100, f'ascending +{pct_move:.1f}% rises={rises}/2 [{snap_str}]', snapshots

    # Moderate: last > first by threshold, at least some rises
    if pct_move >= MIN_MOMENTUM_MOVE_PCT and rises >= 1:
        return 80, f'net_ascending +{pct_move:.1f}% rises={rises}/2 [{snap_str}]', snapshots

    # Weak but valid: overall up by threshold, even with dips mid-way
    if pct_move >= MIN_MOMENTUM_MOVE_PCT and s_last > s_first:
        return 60, f'choppy_up +{pct_move:.1f}% rises={rises}/2 [{snap_str}]', snapshots

    # Peak during window but ended lower (pump fading)
    if pct_max >= MIN_MOMENTUM_MOVE_PCT and pct_move < MIN_MOMENTUM_MOVE_PCT:
        return 0, f'fading peak={pct_max:+.1f}% end={pct_move:+.2f}% [{snap_str}]', snapshots

    # Below threshold
    if pct_move > 0:
        return 0, f'noise +{pct_move:.2f}% < {MIN_MOMENTUM_MOVE_PCT}% rises={rises}/2 [{snap_str}]', snapshots

    return 0, f'declining {pct_move:.2f}% rises={rises}/2 [{snap_str}]', snapshots



def score_signal_evolution(entry):
    """
    Matrix ⑤ — Signal Strength Evolution
    Evaluates signal heat progression over time.

    Returns: (score: int 0-100, reason: str)
    """
    signal_count = entry.get('signal_count', 1)
    has_ath = bool(entry.get('has_ath', 0))
    super_growth = (entry.get('latest_super', 0) or 0) - (entry.get('signal_super', 0) or 0)
    time_elapsed = time.time() - entry.get('added_at', time.time())
    time_minutes = time_elapsed / 60

    if has_ath:
        return 100, f'has_ath signal_count={signal_count}'

    if signal_count >= 3 and super_growth >= 20:
        return 80, f'hot signal_count={signal_count} super_growth={super_growth}'

    if signal_count >= 2 and super_growth >= 10:
        return 60, f'warm signal_count={signal_count} super_growth={super_growth}'

    if signal_count == 1 and time_minutes < 10:
        return 50, f'fresh signal_count=1 age={time_minutes:.0f}min'

    if signal_count == 1 and time_minutes > 30:
        return 20, f'cold signal_count=1 age={time_minutes:.0f}min'

    return 40, f'tepid signal_count={signal_count} age={time_minutes:.0f}min super_growth={super_growth}'


# ─── Composite Evaluator ──────────────────────────────────────────────────

class MatrixEvaluator:
    """
    Five-matrix composite evaluator for watchlist entries.

    Runs matrices ①②③⑤ first (cheap, no extra latency).
    If all pass thresholds → runs matrix ④ (costs 6 seconds for 3×3s snapshots).
    """

    # In-memory price history: {ca: [(timestamp, price), ...]}
    # Accumulated from each evaluate() call to provide synthetic bars
    # when kline_cache.db has no data for the token.
    _price_history = {}

    # K-line fetch cache: {ca: (bars, fetched_at)}
    # Caches GeckoTerminal K-line responses for 30s to avoid hitting rate limits.
    # kline_cache.db is the first choice but has been stale since 2026-04-02;
    # this ensures we always have real 1m K-line data from GeckoTerminal.
    _kline_cache = {}

    @classmethod
    def clear_kline_cache(cls):
        """Purges the in-memory kline cache to prevent indefinite memory growth."""
        cls._kline_cache.clear()

    # Thresholds for NOT_ATH entries (reverted to 84% win rate period)
    NOT_ATH_THRESHOLDS = {
        'trend_min': 50,    # 84% period: T≥50 (was 60)
        'volume_min': 60,   # V≥60 counts as passing
        'price_min': 70,    # P≥70 hard gate
        'signal_min': 60,   # S≥60 counts as passing
        'momentum_min': 60, # at least not declining
        'min_passing': 3,   # 84% period: 3 of 5 (was 4)
        'max_obs_minutes': 120,  # 2 hours max observation
    }

    # Thresholds for ATH entries (reverted to 84% win rate period)
    ATH_THRESHOLDS = {
        'trend_min': 50,    # 84% period: T=0 was soft warning, T≥50 passes
        'volume_min': 60,   # V≥60 counts as passing
        'price_min': 0,     # Skipped for ATH (ATH = price at highs)
        'signal_min': 0,    # ATH = auto 100
        'momentum_min': 60, # required
        'min_passing': 3,   # 84% period: 3 of 5
        'max_obs_minutes': 120,  # 2h
    }

    def evaluate(self, entry):
        """
        Full matrix evaluation for a watchlist entry.

        Returns: {
            'scores': {'trend': int, 'volume': int, 'price': int, 'signal': int, 'momentum': int|None},
            'reasons': {'trend': str, 'volume': str, 'price': str, 'signal': str, 'momentum': str|None},
            'ready_for_momentum': bool,
            'action': 'wait' | 'fire' | 'remove',
            'action_reason': str,
        }
        """
        _lazy_import()

        ca = entry['ca']
        symbol = entry['symbol']
        pool = entry.get('pool_address')
        signal_type = entry.get('type', 'NOT_ATH')
        thresholds = self.ATH_THRESHOLDS if signal_type == 'ATH' else self.NOT_ATH_THRESHOLDS

        scores = {}
        reasons = {}

        # --- Check removal conditions first ---
        removal = self._check_removal(entry, thresholds)
        if removal:
            return {
                'scores': {}, 'reasons': {},
                'ready_for_momentum': False,
                'action': 'remove',
                'action_reason': removal,
            }

        # --- Check cooldown ---
        if entry.get('cooldown_until', 0) > time.time():
            remain = entry['cooldown_until'] - time.time()
            return {
                'scores': {}, 'reasons': {},
                'ready_for_momentum': False,
                'action': 'wait',
                'action_reason': f'cooldown {remain:.0f}s remaining',
            }

        # --- Check max re-entry ---
        if entry.get('entry_count', 0) >= 3:
            return {
                'scores': {}, 'reasons': {},
                'ready_for_momentum': False,
                'action': 'remove',
                'action_reason': 'max_entries_reached (3)',
            }

        # --- Matrix ① Trend --- (uses real 1m K-lines from GeckoTerminal)
        bars = None

        # ① Prefer kline_cache.db if data is fresh (< 5 minutes old)
        if pool:
            db_bars = _bars_fn(pool, limit=100)  # fetch all available bars
            if db_bars and len(db_bars) >= 3:
                newest_ts = db_bars[0].get('ts', 0)
                if newest_ts >= time.time() - 300:
                    bars = db_bars

        # ② GeckoTerminal real 1m K-lines (with 30s in-memory cache)
        # limit=100 → fetch all available bars; linear regression benefits from longer history
        if not bars and pool:
            cached_kline = self.__class__._kline_cache.get(ca)
            if cached_kline and time.time() - cached_kline[1] < 30:
                bars = cached_kline[0]
            else:
                gt_bars = _bars_fn(pool, limit=100)
                if gt_bars:
                    self.__class__._kline_cache[ca] = (gt_bars, time.time())
                    bars = gt_bars

        # ③ Final fallback: synthetic bars from our own price observations
        if not bars or len(bars) < 3:
            bars = self._get_synthetic_bars(ca)

        scores['trend'], reasons['trend'], _ = score_trend(bars, symbol, token_ca=ca)

        # --- Matrix ② Volume ---
        scores['volume'], reasons['volume'] = score_volume(
            bars,
            signal_tx24h=entry.get('signal_tx24h', 0),
            signal_vol24h=entry.get('signal_vol24h', 0),
            token_ca=ca,
            pool_address=pool,
        )

        # --- Matrix ③ Price Strength ---
        current_price = None
        if pool:
            current_price, _, _ = _price_fn(ca, pool)

        scores['price'], reasons['price'] = score_price_strength(
            current_price,
            entry.get('signal_price'),
            entry.get('lowest_price'),
            latest_ath_price=entry.get('latest_ath_price'),  # Fix 4
        )

        # Update price bounds
        if current_price and current_price > 0:
            if entry.get('lowest_price') is None or current_price < entry['lowest_price']:
                entry['lowest_price'] = current_price
            if entry.get('highest_price') is None or current_price > entry['highest_price']:
                entry['highest_price'] = current_price
            # Accumulate price observation for synthetic bar construction
            history = self._price_history.setdefault(ca, [])
            history.append((int(time.time()), current_price))
            # Keep last 60 minutes of observations for synthetic K-line construction.
            # Synthetic bars are the FALLBACK T score data source when DexScreener is unavailable.
            # DexScreener is the PRIMARY source (see score_trend).
            cutoff = int(time.time()) - 3600
            self._price_history[ca] = [(t, p) for t, p in history if t >= cutoff]

        # --- Matrix ⑤ Signal Evolution ---
        scores['signal'], reasons['signal'] = score_signal_evolution(entry)

        # --- Check pre-momentum thresholds ---
        scores['momentum'] = None
        reasons['momentum'] = 'not_evaluated'

        ready = self._check_pre_momentum_pass(scores, thresholds, signal_type=signal_type)

        # Hard blocks: no zero scores allowed for critical matrices
        # Exception: ATH signals get T=0 as soft check (not block).
        # Data: $Rudi T=0 for 74% of evals. ATH K-lines oscillate wildly (big green→big red).
        # T has no discriminating power for ATH. Momentum check (M) is the real guard.
        hard_block = None
        if scores['trend'] == 0:
            if signal_type == 'ATH':
                # ATH: T=0 is just a warning, not a block
                hard_block = 'trend=0(soft)'
            else:
                ready = False
                hard_block = 'trend=0'
        if scores['price'] == 0:
            ready = False
            hard_block = (hard_block + '+' if hard_block else '') + 'price=0'
        if scores['volume'] == 0:
            # V=0 = no liquidity, hard block (data: GRASS V=0 lost -75.2%)
            ready = False
            hard_block = (hard_block + '+' if hard_block else '') + 'volume=0'
        elif scores['volume'] < thresholds['volume_min']:
            # Low volume (but not zero) — informational warning, not a blocker
            pass
        # Signal (S) is pure bonus — not a blocker, so no hard_block entry for it

        # Always log evaluation result so we can diagnose filtering
        log.info(
            f"[Matrix] ${symbol} eval: "
            f"T={scores['trend']} V={scores['volume']} P={scores['price']} S={scores['signal']} "
            f"ready={ready} block={hard_block or 'none'} "
            f"type={signal_type} age={int((time.time() - entry.get('added_at', time.time())) / 60)}min"
        )

        action = 'wait'
        action_reason = 'matrices not yet aligned'

        if ready:
            # DexScreener price_m5 trend check is now integrated into score_trend()
            # (T score). If K-line says uptrend but price_m5 < -3% → T=0 → hard block
            # for NOT_ATH. No need for a separate Trend Gate.

            log.info(
                f"[Matrix] ${symbol} pre-momentum PASS: "
                f"T={scores['trend']} V={scores['volume']} P={scores['price']} S={scores['signal']} "
                f"→ running 3×3s momentum check..."
            )

            # For re-entries: verify price > last exit price
            # BUG FIX: was checking entry_price which gets cleared to None on exit.
            # Now uses last_exit_price which is persisted in mark_watching().
            last_exit_px = entry.get('last_exit_price')
            if entry.get('entry_count', 0) > 0 and last_exit_px is not None:
                if current_price and current_price <= last_exit_px:
                    return {
                        'scores': scores, 'reasons': reasons,
                        'ready_for_momentum': False,
                        'action': 'wait',
                        'action_reason': f"reentry: price {current_price:.10f} <= last_exit {last_exit_px:.10f}, waiting for recovery",
                    }

            scores['momentum'], reasons['momentum'], snaps = score_realtime_momentum(
                ca, pool
            )
            # Fix 2: record the final snapshot price as the confirmed trigger price
            momentum_final_price = snaps[-1] if snaps else current_price
            # Compute momentum pct for SmartEntry to use (fresh, not DexScreener-lagged)
            momentum_pct = ((snaps[-1] - snaps[0]) / snaps[0] * 100) if len(snaps) >= 2 and snaps[0] > 0 else 0

            if scores['momentum'] >= thresholds['momentum_min']:
                action = 'fire'
                action_reason = (
                    f"ALL MATRICES PASS: T={scores['trend']} V={scores['volume']} "
                    f"P={scores['price']} S={scores['signal']} M={scores['momentum']}"
                )
                log.info(f"[Matrix] 🔫 ${symbol} {action_reason}")
            else:
                action_reason = f"momentum check failed: {reasons['momentum']}"
                log.info(f"[Matrix] ${symbol} momentum FAIL: {reasons['momentum']}")
                momentum_final_price = None
                momentum_pct = 0

        else:
            momentum_final_price = None
            snaps = []
            momentum_pct = 0

        return {
            'scores': scores,
            'reasons': reasons,
            'ready_for_momentum': ready,
            'action': action,
            'action_reason': action_reason,
            'current_price': current_price,
            'momentum_final_price': momentum_final_price,  # Fix 2: accurate trigger price
            'momentum_snapshots': snaps,  # 3×3s raw prices [p1, p2, p3]
            'momentum_pct': momentum_pct,  # 9s net change %
        }
    def _check_pre_momentum_pass(self, scores, thresholds, signal_type='NOT_ATH'):
        """Check if matrices ①③ meet thresholds for momentum trigger.
        Reverted to 84% win rate period logic:
        - T≥50 counts as passing for ALL signal types (was only ATH before)
        - V and S are pure bonuses — never block, only add passing count
        - Only T and P are structural hard-gates
        - ATH + T≥50 + V≥60 → P hard-gate is bypassed (momentum_direct)
        """
        # Structural hard-gates: trend and price
        hard_checks = [
            ('trend', scores.get('trend', 0), thresholds['trend_min']),
            ('price', scores.get('price', 0), thresholds['price_min']),
        ]

        # 84% period: T≥50 counts as passing for all signal types
        passing_count = sum(1 for name, val, _ in hard_checks
                            if val >= 60 or (name == 'trend' and val >= 50))
        hard_fails = any(val < mins for _, val, mins in hard_checks)

        if hard_fails:
            # ATH momentum-direct bypass: ATH + T≥50 + V≥60 → bypass P hard-gate
            t_score = scores.get('trend', 0)
            v_score = scores.get('volume', 0)
            if signal_type == 'ATH' and t_score >= 50 and v_score >= 60:
                log.info(
                    f"[Matrix] ATH momentum-direct bypass: T={t_score} V={v_score} "
                    f"P={scores.get('price', 0)} → bypassing P hard-gate, letting momentum decide"
                )
            else:
                return False

        # Volume and Signal are bonuses: if >= 60 they add to passing_count
        if scores.get('volume', 0) >= 60:
            passing_count += 1
        if scores.get('signal', 0) >= 60:
            passing_count += 1

        return passing_count >= thresholds['min_passing'] - 1  # -1 because momentum hasn't been checked

    def _get_synthetic_bars(self, ca, bar_count=30):
        """Build synthetic 1-minute bars from accumulated price observations.
        
        CRITICAL: This is the PRIMARY data source for T score!
        - kline_cache.db has been stale since 2026-04-02 (11+ days)
        - GeckoTerminal returns 403 (Cloudflare blocked) for pump.fun coins
        
        So synthetic bars from our own price observations are the only
        actual data feeding check_multi_bar_trend / linear regression.
        
        With adaptive evaluation frequency (10s for first 5 min),
        each 1-minute bar gets ~6 price observations — enough for
        meaningful OHLC values. Extended from 5 bars to 30 bars
        to give linear regression a wider window.
        
        Returns list of bar dicts (newest first), or None if insufficient data.
        """
        history = self._price_history.get(ca)
        if not history or len(history) < 3:
            return None

        # Bucket observations into 1-minute windows
        now = int(time.time())
        bars = []
        for i in range(bar_count):
            window_end = now - i * 60
            window_start = window_end - 60
            points = [(t, p) for t, p in history if window_start <= t < window_end]
            if not points:
                continue
            prices = [p for _, p in points]
            bars.append({
                'ts': window_start,
                'open': prices[0],
                'high': max(prices),
                'low': min(prices),
                'close': prices[-1],
                'volume': len(points),  # use observation count as a proxy for activity
            })

        if len(bars) < 3:
            return None

        return bars  # newest first (already in this order)

    def _check_removal(self, entry, thresholds):
        """Check if entry should be removed from watchlist."""
        now = time.time()
        age_minutes = (now - entry.get('added_at', now)) / 60

        # Timeout
        # SUSTAINED_ATH gets double timeout (240min) because it's a genuine multi-hour trend
        _max_obs = 240 if entry.get('is_sustained_ath') else thresholds['max_obs_minutes']
        if age_minutes >= _max_obs:
            return f'timeout ({age_minutes:.0f}min >= {_max_obs}min)'

        # Price collapse: current << signal
        # Data-validated: -70% kills all truly dead coins (REDBULL -99%, COOKED -95%,
        # LIB -100%, GOUT -80%) while preserving coins with recovery potential.
        # Was -50% which risked killing coins like BabyBull (-49% but had +295% run).
        signal_price = entry.get('signal_price')
        lowest_price = entry.get('lowest_price')
        if signal_price and lowest_price:
            if lowest_price < signal_price * 0.3:
                return f'price_collapse (lowest={lowest_price:.10f} < 70% drop from signal={signal_price:.10f})'

        return None


# ─── Holding Exit Matrix (for positions already bought) ────────────────────

class ExitMatrixEvaluator:
    """
    Matrix-driven dynamic exit evaluator for held positions.

    Runs trend + volume checks every 60 seconds.
    Manages trailing stops, dynamic SL tightening, and profit locking.
    """

    def evaluate_exit(self, entry, current_price):
        """
        Evaluate exit conditions for a held position.

        Returns: {
            'action': 'hold' | 'exit' | 'lock_profit' | 'tighten_sl',
            'reason': str,
            'current_pnl': float,
            'trail_floor': float|None,
        }
        """
        _lazy_import()

        if not current_price or current_price <= 0:
            return {'action': 'hold', 'reason': 'no_price', 'current_pnl': 0, 'trail_floor': None}

        entry_price = entry.get('entry_price', 0)
        if not entry_price or entry_price <= 0:
            return {'action': 'hold', 'reason': 'no_entry_price', 'current_pnl': 0, 'trail_floor': None}

        current_pnl = (current_price - entry_price) / entry_price
        peak_pnl = max(entry.get('peak_pnl', 0), current_pnl)

        # === COORDINATED THREAT SCORE (synced with ExitGuardian) ===
        # Prefer Guardian's full score (includes FLAT-TOP from price_ring).
        # Fall back to local TIME_DECAY + THIN_POOL if Guardian hasn't run yet.
        _guardian_threat = entry.get('_guardian_threat_tighten')
        if _guardian_threat is not None:
            _threat_tighten = _guardian_threat
        else:
            # Fallback: compute locally without FLAT-TOP
            _threat_score = 0
            _peak_ts = entry.get('_peak_ts', 0) or entry.get('entry_time', 0) or 0
            _time_since_peak = (time.time() - _peak_ts) if _peak_ts > 0 else 0
            if peak_pnl >= 0.05:
                if _time_since_peak > 60:
                    _threat_score += 2
                elif _time_since_peak > 30:
                    _threat_score += 1
            _liq_usd = entry.get('_dex_liquidity_usd', 0) or 0
            if 0 < _liq_usd < 10000:
                _threat_score += 2
            elif 0 < _liq_usd < 20000:
                _threat_score += 1
            _threat_tighten = min(_threat_score * 0.02, 0.06)
        _decay = 1.0  # kept for downstream compat

        # === Hard Stop-Loss ===
        # Fixed -15% — reverted from AdaptiveSL (-5%~-7.5%) to match 84% win rate period.
        # Audit (26 trades, 2026-04-22): Tight SL killed 22/26 trades at -7%~-10%,
        # many of which had peaks of +3-4% (would have survived with -15% SL).
        # Meme coin normal volatility is ±5-8%, so -5% SL = noise-level stop.
        hard_sl = entry.get('dynamic_sl', -0.15)
        if current_pnl <= hard_sl:
            return {
                'action': 'exit',
                'reason': f'hard_sl ({current_pnl:.1%} <= {hard_sl:.1%})',
                'current_pnl': current_pnl,
                'trail_floor': None,
            }

        # === ATH Fast Lane: Three-Phase Exit Strategy ===
        # Phase 1 (peak < 50%):  Free run — only Hard SL -7.5% (already checked above)
        # Phase 2 (50-100%):     Trail with absolute -20pp floor
        # Phase 3 (peak >= 100%): lock_profit → sell 50% (recover principal) → rest goes moon_bag
        _is_ath_entry = entry.get('type') == 'ATH' or entry.get('signal_type') == 'ATH'
        if _is_ath_entry:
            symbol = entry.get('symbol', '?')

            # Phase 3: peak >= 100% → trigger lock_profit (sell 50% to recover principal)
            if peak_pnl >= 1.0 and not entry.get('has_locked_profit'):
                log.info(
                    f"[ExitMatrix] {symbol} ATH PHASE3: peak={peak_pnl:.1%} >= 100% "
                    f"→ lock_profit (sell 50% to recover principal, rest → moon_bag)"
                )
                return {
                    'action': 'lock_profit',
                    'reason': f'ath_phase3_lock (peak={peak_pnl:.1%} >= 100%, sell 50% → recover principal)',
                    'current_pnl': current_pnl,
                    'trail_floor': None,
                }

            # Phase 2: peak 50-100% → trail with absolute -20pp floor (A3: time-decay applied)
            # Velocity factor: tighten margins when momentum is fading
            _vel = entry.get('_guardian_velocity', 0) or 0
            if _vel < -5.0:
                _vel_t = 0.70   # CRASH → 30% tighter
            elif _vel < -2.0:
                _vel_t = 0.85   # fading → 15% tighter
            else:
                _vel_t = 1.0

            if peak_pnl >= 0.50:
                trail_floor = peak_pnl - (0.20 * _vel_t) + _threat_tighten
                if current_pnl < trail_floor:
                    return {
                        'action': 'exit',
                        'reason': f'ath_phase2_trail (pnl={current_pnl:.1%} < floor={trail_floor:.1%}, peak={peak_pnl:.1%}, -20pp abs)',
                        'current_pnl': current_pnl,
                        'trail_floor': trail_floor,
                    }
                return {
                    'action': 'hold',
                    'reason': f'ath_phase2_hold (floor={trail_floor:.1%}, peak={peak_pnl:.1%})',
                    'current_pnl': current_pnl,
                    'trail_floor': trail_floor,
                }

            # Phase 1: peak < 50% — tiered protection (was unconditional free_run → DUCK bug)
            # Sub-phase 1c: peak >= 25% → trail with -15pp floor (A3: time-decay applied)
            if peak_pnl >= 0.25:
                trail_floor = peak_pnl - (0.15 * _vel_t) + _threat_tighten
                if current_pnl < trail_floor:
                    return {
                        'action': 'exit',
                        'reason': f'ath_phase1_trail_25 (pnl={current_pnl:.1%} < floor={trail_floor:.1%}, peak={peak_pnl:.1%}, -15pp)',
                        'current_pnl': current_pnl,
                        'trail_floor': trail_floor,
                    }
                return {
                    'action': 'hold',
                    'reason': f'ath_phase1_hold_25 (floor={trail_floor:.1%}, peak={peak_pnl:.1%})',
                    'current_pnl': current_pnl,
                    'trail_floor': trail_floor,
                }

            # Sub-phase 1b: peak >= 15% → trail with -10pp floor (A3: time-decay applied)
            if peak_pnl >= 0.15:
                trail_floor = peak_pnl - (0.10 * _vel_t) + _threat_tighten
                if current_pnl < trail_floor:
                    return {
                        'action': 'exit',
                        'reason': f'ath_phase1_trail_15 (pnl={current_pnl:.1%} < floor={trail_floor:.1%}, peak={peak_pnl:.1%}, -10pp)',
                        'current_pnl': current_pnl,
                        'trail_floor': trail_floor,
                    }
                return {
                    'action': 'hold',
                    'reason': f'ath_phase1_hold_15 (floor={trail_floor:.1%}, peak={peak_pnl:.1%})',
                    'current_pnl': current_pnl,
                    'trail_floor': trail_floor,
                }

            # Sub-phase 1a: peak < 15% — velocity+volume crash brake
            # Previously unconditional free_run. Now uses Guardian velocity signals.
            _vel = entry.get('_guardian_velocity', 0) or 0
            _tvol = entry.get('_guardian_tick_vol', 1) or 1
            if peak_pnl >= 0.05:  # had at least +5% peak
                _crash = False
                _crash_reason = ''
                if _vel < -5.0 and current_pnl < peak_pnl * 0.3:
                    _crash = True
                    _crash_reason = f'vel_crash (vel={_vel:.1f}, pnl={current_pnl:.1%} < 30% of peak={peak_pnl:.1%})'
                elif _vel < -3.0 and current_pnl <= 0:
                    _crash = True
                    _crash_reason = f'vel_fade (vel={_vel:.1f}, pnl={current_pnl:.1%}, peak was {peak_pnl:.1%})'
                elif _tvol < 0.001 and current_pnl < peak_pnl * 0.5:
                    _crash = True
                    _crash_reason = f'vol_death (tvol={_tvol:.4f}, pnl={current_pnl:.1%} < 50% of peak={peak_pnl:.1%})'
                if _crash:
                    return {
                        'action': 'exit',
                        'reason': f'ath_crash_brake ({_crash_reason})',
                        'current_pnl': current_pnl,
                        'trail_floor': None,
                    }

            return {
                'action': 'hold',
                'reason': f'ath_phase1_free_run (peak={peak_pnl:.1%} < 15%)',
                'current_pnl': current_pnl,
                'trail_floor': None,
            }

        # === Profit Lock at +50% (velocity-aware) ===
        # B+C covers -15% to +50% with velocity+volume trail.
        # Lock profit (sell 50% → moon bag) only at +50%+.
        # Safety cap: always lock at +70% regardless of velocity.
        if peak_pnl >= 0.50 and not entry.get('has_locked_profit'):

            velocity = entry.get('_guardian_velocity', 0)
            helius_tps = entry.get('_helius_tps', 0)
            symbol = entry.get('symbol', '?')

            # Safety cap: force lock at +70% no matter what
            if peak_pnl >= 0.70:
                log.info(
                    f"[ExitMatrix] {symbol} FORCE lock_profit: "
                    f"peak={peak_pnl:.1%} >= 70% safety cap"
                )
                return {
                    'action': 'lock_profit',
                    'reason': f'profit_lock_forced (peak={peak_pnl:.1%} >= 70%)',
                    'current_pnl': current_pnl,
                    'trail_floor': None,
                }

            # Rocket + real volume → delay lock, tighten trail
            if velocity > 10.0 and helius_tps > 3.0:
                entry['_trail_factor'] = max(entry.get('_trail_factor', 0) or 0, 0.70)
                log.info(
                    f"[ExitMatrix] {symbol} DELAY lock_profit: "
                    f"vel={velocity:.1f}%/min tps={helius_tps:.1f} → trail ratchet to 0.70 "
                    f"(peak={peak_pnl:.1%}, will lock when momentum fades)"
                )
                # Don't return lock_profit → fall through to trail logic below
            else:
                # Momentum has faded → normal lock
                return {
                    'action': 'lock_profit',
                    'reason': f'profit_lock (peak={peak_pnl:.1%} >= 50%, vel={velocity:.1f}, tps={helius_tps:.1f})',
                    'current_pnl': current_pnl,
                    'trail_floor': None,
                }

        # === Trailing Stop (velocity-aware, like Moon Bag) ===
        # Data: old fixed 0.5/0.6 factor only preserved 34% of peak profit.
        # New: velocity-based ratchet — fast moves lock more profit.
        trail_floor = None
        if peak_pnl >= 0.05:  # +5% trail activation
            # Use Guardian's velocity (3s price ring, 30s window) — more accurate
            # than computing our own from 2-min PnL history
            velocity = entry.get('_guardian_velocity', 0)

            # Tiered trail factor based on peak level + velocity
            # ATH tokens get wider trailing (0.50) because parabolic moves have ±6% swings in 6 seconds
            _is_ath_entry = entry.get('type') == 'ATH' or entry.get('signal_type') == 'ATH'
            if peak_pnl >= 0.20:
                base_factor = 0.50 if _is_ath_entry else 0.60   # ATH: 50% floor | non-ATH: 60% floor
            elif peak_pnl >= 0.10:
                base_factor = 0.50 if _is_ath_entry else 0.55   # ATH: wider room for continuation
            else:
                base_factor = 0.5    # >= +5% preserve at least 50%

            # Velocity-aware trail: momentum decides tightness
            # Strong momentum → let it run (looser trail)
            # Fading momentum → lock profit fast (tighter trail)
            if velocity < -5.0:
                vel_factor = 0.85    # dumping → lock hard, preserve 85%
            elif velocity < 0:
                vel_factor = 0.75    # fading → tighten
            elif velocity > 10.0:
                vel_factor = base_factor  # rocketing → use base, let it run
            else:
                vel_factor = 0.60    # neutral → moderate protection (was 0.70)

            # Ratchet: use whichever is higher, never lower the factor
            current_factor = entry.get('_trail_factor', base_factor)
            trail_factor = max(base_factor, vel_factor, current_factor)
            trail_floor = peak_pnl * trail_factor + _threat_tighten

            if current_pnl < trail_floor:
                return {
                    'action': 'exit',
                    'reason': f'trail_stop (pnl={current_pnl:.1%} < floor={trail_floor:.1%}, peak={peak_pnl:.1%}, factor={trail_factor:.2f}, vel={velocity:.1f}%/min)',
                    'current_pnl': current_pnl,
                    'trail_floor': trail_floor,
                    '_trail_factor': trail_factor,
                }

        # === Matrix-based soft exit (trend check) ===
        pool = entry.get('pool_address')
        should_check_matrix = (
            pool and
            time.time() - entry.get('last_matrix_check', 0) >= 60
        )

        if should_check_matrix:
            bars = _bars_fn(pool, limit=100)

            # Trend check
            trend_ok, reason, detail = _trend_fn(bars, entry.get('symbol', '?'))
            if not trend_ok:
                if current_pnl > 0:
                    return {
                        'action': 'exit',
                        'reason': f'matrix_tp (trend={reason}: {detail}, pnl={current_pnl:.1%})',
                        'current_pnl': current_pnl,
                        'trail_floor': trail_floor,
                    }
                elif current_pnl > -0.05:
                    return {
                        'action': 'exit',
                        'reason': f'matrix_sl (trend={reason}: {detail}, pnl={current_pnl:.1%})',
                        'current_pnl': current_pnl,
                        'trail_floor': trail_floor,
                    }

            # Volume check
            vol_score, vol_reason = score_volume(bars, entry.get('signal_tx24h', 0))
            if vol_score == 0:
                zero_count = entry.get('zero_vol_count', 0) + 1
                if zero_count >= 3:
                    return {
                        'action': 'tighten_sl',
                        'reason': f'volume_dead (zero_vol {zero_count} consecutive, tightening SL to -3%)',
                        'current_pnl': current_pnl,
                        'trail_floor': trail_floor,
                        'new_sl': -0.03,
                        'new_zero_vol_count': zero_count,
                    }
                return {
                    'action': 'hold',
                    'reason': f'volume_weak ({vol_reason})',
                    'current_pnl': current_pnl,
                    'trail_floor': trail_floor,
                    'new_zero_vol_count': zero_count,
                }

        # === Timeout (120 minutes) ===
        entry_time = entry.get('entry_time', 0)
        if entry_time and (time.time() - entry_time) / 60 >= 120:
            return {
                'action': 'exit',
                'reason': f'timeout (120min, pnl={current_pnl:.1%})',
                'current_pnl': current_pnl,
                'trail_floor': trail_floor,
            }

        return {
            'action': 'hold',
            'reason': 'all_clear',
            'current_pnl': current_pnl,
            'peak_pnl': peak_pnl,
            'trail_floor': trail_floor,
        }

    def evaluate_moon_bag(self, entry, current_price):
        """
        Evaluate exit for a Moon Bag position (50% remaining, no timeout).

        Returns: {
            'action': 'hold' | 'exit',
            'reason': str,
            'current_pnl': float,
        }
        """
        _lazy_import()

        entry_price = entry.get('entry_price', 0)
        if not entry_price or entry_price <= 0 or not current_price or current_price <= 0:
            return {'action': 'hold', 'reason': 'no_price', 'current_pnl': 0}

        current_pnl = (current_price - entry_price) / entry_price
        moon_peak = max(entry.get('moon_peak_pnl', 0), current_pnl)

        # === Breakeven stop (entry price) ===
        if current_price <= entry_price:
            return {
                'action': 'exit',
                'reason': f'moon_breakeven (price={current_price:.10f} <= entry={entry_price:.10f})',
                'current_pnl': current_pnl,
            }

        # === ATH Phase 3 Moon Bag: absolute -40pp trail (house money, let it moonshot) ===
        _is_ath_entry = entry.get('type') == 'ATH' or entry.get('signal_type') == 'ATH'
        if _is_ath_entry:
            # floor = peak - 40pp absolute. At peak=150% → floor=110%. At peak=300% → floor=260%.
            moon_floor = moon_peak - 0.40
            if moon_floor > 0 and current_pnl < moon_floor:
                return {
                    'action': 'exit',
                    'reason': f'ath_phase3_moon_trail (pnl={current_pnl:.1%} < floor={moon_floor:.1%}, peak={moon_peak:.1%}, -40pp abs)',
                    'current_pnl': current_pnl,
                }
            return {
                'action': 'hold',
                'reason': f'ath_phase3_moon_hold (floor={moon_floor:.1%}, peak={moon_peak:.1%})',
                'current_pnl': current_pnl,
            }

        # === Moon Trail (Dynamic Velocity-Based Factor) ===
        # P4: Factor adjusts based on how fast PnL is rising (velocity).
        # Velocity = PnL change per minute over 2-min sliding window.
        # Factor ratchets up only (never decreases).
        # Tiered: <5%/min → 0.2, 5-15%/min → 0.4, >15%/min → 0.6

        # Use Guardian's velocity directly (3s price ring, 30s window)
        velocity = entry.get('_guardian_velocity', 0)

        # Map velocity to target factor
        if velocity > 15.0:
            target_factor = 0.6
        elif velocity > 5.0:
            target_factor = 0.4
        else:
            target_factor = 0.2

        # Ratchet: only increase, never decrease
        current_factor = entry.get('moon_trail_factor', 0.2) or 0.2
        moon_trail_factor = max(target_factor, current_factor)

        moon_floor = moon_peak * moon_trail_factor

        if moon_floor > 0 and current_pnl < moon_floor:
            return {
                'action': 'exit',
                'reason': f'moon_trail (pnl={current_pnl:.1%} < floor={moon_floor:.1%}, peak={moon_peak:.1%}, factor={moon_trail_factor}, vel={velocity:.1f}%/min)',
                'current_pnl': current_pnl,
                'moon_trail_factor': moon_trail_factor,
            }

        # === 24h safety cap ===
        moon_start = entry.get('moon_start_time', 0)
        if moon_start and (time.time() - moon_start) / 3600 >= 24:
            return {
                'action': 'exit',
                'reason': f'moon_timeout_24h (pnl={current_pnl:.1%})',
                'current_pnl': current_pnl,
            }

        # === Trend death check (every 5 min) ===
        pool = entry.get('pool_address')
        if pool and time.time() - entry.get('last_matrix_check', 0) >= 300:
            bars = _bars_fn(pool, limit=100)
            trend_ok, reason, _ = _trend_fn(bars, entry.get('symbol', '?'))

            if not trend_ok:
                zero_count = entry.get('moon_trend_zero_count', 0) + 1
                if zero_count >= 5:  # 5 × 5min = 25 min of dead trend
                    return {
                        'action': 'exit',
                        'reason': f'moon_trend_dead ({zero_count} consecutive fails, pnl={current_pnl:.1%})',
                        'current_pnl': current_pnl,
                        'new_moon_trend_zero_count': zero_count,
                    }
                return {
                    'action': 'hold',
                    'reason': f'moon_trend_warning ({zero_count}/5)',
                    'current_pnl': current_pnl,
                    'moon_peak_pnl': moon_peak,
                    'new_moon_trend_zero_count': zero_count,
                    'moon_trail_factor': moon_trail_factor,
                }
            else:
                return {
                    'action': 'hold',
                    'reason': 'moon_trend_ok',
                    'current_pnl': current_pnl,
                    'moon_peak_pnl': moon_peak,
                    'new_moon_trend_zero_count': 0,
                    'moon_trail_factor': moon_trail_factor,
                }

        return {
            'action': 'hold',
            'reason': 'moon_ok',
            'current_pnl': current_pnl,
            'moon_peak_pnl': moon_peak,
            'moon_trail_factor': moon_trail_factor,
        }
