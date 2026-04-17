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

def score_trend(bars, symbol):
    """
    Matrix ① — Trend Direction
    Reuses existing check_multi_bar_trend logic.

    Returns: (score: int 0-100, reason: str, detail: str)
    """
    _lazy_import()
    if not bars or len(bars) < 3:
        return 50, 'insufficient_bars', 'Not enough bars for shape analysis (fail-open)'

    trend_ok, reason, detail = _trend_fn(bars, symbol)

    if trend_ok:
        if reason == 'passed_shape':
            return 100, reason, detail
        else:
            return 50, reason, detail  # insufficient_bars etc
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
                    return 0, f'dex_trend_weak txns={total_txns} ratio={vol_ratio:.1f} buys={buys} sells={sells}'
        except Exception as e:
            logging.getLogger('matrix_evaluator').warning(f"dex_trend_weak parsing failed: {e}")
            pass  # fall through to existing paths

    # --- Path 1: K-line bars available ---
    if bars and len(bars) >= 4:
        recent_vol = bars[0].get('volume', 0)
        prev_vols = [b.get('volume', 0) for b in bars[1:4]]
        avg_prev = sum(prev_vols) / len(prev_vols) if prev_vols else 0

        # All-zero = data absent, not volume decline — fall through to DexScreener
        if not (avg_prev <= 0 and recent_vol <= 0):
            if avg_prev <= 0:
                vol_ratio = 999.0
            else:
                vol_ratio = recent_vol / avg_prev

            tx = signal_tx24h or 0
            if vol_ratio >= 2.0 and (tx == 0 or tx >= 300):
                return 100, f'strong_volume ratio={vol_ratio:.1f} tx={tx}'
            elif vol_ratio >= 1.2 and (tx == 0 or tx >= 100):
                return 70, f'moderate_volume ratio={vol_ratio:.1f} tx={tx}'
            elif vol_ratio >= 0.8:
                return 40, f'flat_volume ratio={vol_ratio:.1f} tx={tx}'
            else:
                return 0, f'weak_volume ratio={vol_ratio:.1f} tx={tx}'
    
    # --- Path 2: DexScreener real-time volume ---
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
    Matrix ③ — Price Strength
    Evaluates growth from signal price and recovery from observed dip.

    Fix 4: For ATH tokens, use the LOWER of signal_price vs latest_ath_price as the
    comparison anchor. This prevents ATH peak prices from making P permanently low
    when the coin pulls back after an ATH signal.

    Returns: (score: int 0-100, reason: str)
    """
    if not current_price or current_price <= 0:
        return 0, 'no_price'
    if not signal_price or signal_price <= 0:
        return 50, 'no_signal_price'

    # Fix 4: use the lower of the two as the anchor price so ATH pullbacks don't kill P
    anchor_price = signal_price
    if latest_ath_price and latest_ath_price > 0:
        # If ATH is higher than original signal price, original signal_price is better anchor
        # If coin dropped back below signal_price after ATH, use current lowest
        anchor_price = signal_price  # always preserve original anchor

    growth_pct = ((current_price - anchor_price) / anchor_price) * 100

    recovery_pct = 0
    if lowest_price and lowest_price > 0:
        recovery_pct = ((current_price - lowest_price) / lowest_price) * 100

    # Healthy growth + recovery from support
    if 0 <= growth_pct <= 50 and recovery_pct >= 5:
        return 100, f'healthy growth={growth_pct:+.1f}% recovery={recovery_pct:.1f}%'

    # Fast growth but acceptable
    if 0 <= growth_pct <= 100 and recovery_pct >= 3:
        return 70, f'fast_growth growth={growth_pct:+.1f}% recovery={recovery_pct:.1f}%'

    # V-bounce from below signal price
    if growth_pct < 0 and recovery_pct >= 10:
        return 80, f'v_bounce growth={growth_pct:+.1f}% recovery={recovery_pct:.1f}%'

    # Already doubled — high risk
    if growth_pct > 100:
        return 30, f'overextended growth={growth_pct:+.1f}%'

    # Still at bottom
    if growth_pct < 0 and recovery_pct < 5:
        return 0, f'bottom growth={growth_pct:+.1f}% recovery={recovery_pct:.1f}%'

    # Default: marginal
    return 40, f'marginal growth={growth_pct:+.1f}% recovery={recovery_pct:.1f}%'


MIN_MOMENTUM_MOVE_PCT = 1.5  # 12s minimum move: 1.5%
# Data-driven: in 6h audit, all FIRE passes had <1% 6s move (noise), max observed
# meme coin 6s move was +3.69%. 5% would block ALL entries including Wifejak (+484%).
# 1.5% filters pure noise while allowing legitimate trend momentum through.
# Upgraded: 5×3s=12s window (from 3×3s=6s) to catch pulsed meme coin moves.


def score_realtime_momentum(token_ca, pool_address, interval_sec=3):
    """
    Matrix ④ — Realtime Momentum (5×3-second snapshots = 12s window)
    Only called when matrices ①②③⑤ are already passing.

    Requires: price must move UP by at least MIN_MOMENTUM_MOVE_PCT (1.5%) over 12 seconds.
    Uses 5 samples to better capture pulsed meme coin price action.

    Returns: (score: int 0-100, reason: str, snapshots: list)
    """
    _lazy_import()

    snapshots = []
    for i in range(5):
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
    if rises >= 3 and pct_move >= MIN_MOMENTUM_MOVE_PCT:
        return 100, f'ascending +{pct_move:.1f}% rises={rises}/4 [{snap_str}]', snapshots

    # Moderate: last > first by threshold, at least some rises
    if pct_move >= MIN_MOMENTUM_MOVE_PCT and rises >= 2:
        return 100, f'net_ascending +{pct_move:.1f}% rises={rises}/4 [{snap_str}]', snapshots

    # Weak but valid: overall up by threshold, even with dips mid-way
    if pct_move >= MIN_MOMENTUM_MOVE_PCT and s_last > s_first:
        return 60, f'choppy_up +{pct_move:.1f}% rises={rises}/4 [{snap_str}]', snapshots

    # Peak during window but ended lower (pump fading)
    if pct_max >= MIN_MOMENTUM_MOVE_PCT and pct_move < MIN_MOMENTUM_MOVE_PCT:
        return 0, f'fading peak={pct_max:+.1f}% end={pct_move:+.2f}% [{snap_str}]', snapshots

    # Below threshold
    if pct_move > 0:
        return 0, f'noise +{pct_move:.2f}% < {MIN_MOMENTUM_MOVE_PCT}% rises={rises}/4 [{snap_str}]', snapshots

    return 0, f'declining {pct_move:.2f}% rises={rises}/4 [{snap_str}]', snapshots



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

    # Thresholds for NOT_ATH entries
    NOT_ATH_THRESHOLDS = {
        'trend_min': 50,    # must be at least fail-open
        'volume_min': 40,   # at least not shrinking
        'price_min': 70,    # healthy range
        'signal_min': 40,   # 40 means survives even if >10m but <120m
        'momentum_min': 60, # at least not declining
        'min_passing': 4,   # at least 3 of 4 pre-matrices >= 60!
        'max_obs_minutes': 120,  # 2 hours max observation
    }

    # Thresholds for ATH entries (more lenient)
    # Data-validated 2026-04-15: P<=30 trades avg -16.8%. V=0 means zero liquidity.
    # Both are now hard-blocked. ATH still needs real buying momentum.
    ATH_THRESHOLDS = {
        'trend_min': 50,    # must pass
        'volume_min': 40,   # V=0 means no liquidity — hard block (was 0)
        'price_min': 40,    # P<=30 = chasing a top → avg -16.8% (was 30)
        'signal_min': 0,    # ATH = auto 100
        'momentum_min': 60, # must not decline — this IS the real filter for ATH
        'min_passing': 3,   # at least 3 of 5 >= 60
        'max_obs_minutes': 120,  # 2h — allow consolidation
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

        scores['trend'], reasons['trend'], _ = score_trend(bars, symbol)

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
            # Synthetic bars are the PRIMARY T score data source (see _get_synthetic_bars).
            cutoff = int(time.time()) - 3600
            self._price_history[ca] = [(t, p) for t, p in history if t >= cutoff]

        # --- Matrix ⑤ Signal Evolution ---
        scores['signal'], reasons['signal'] = score_signal_evolution(entry)

        # --- Check pre-momentum thresholds ---
        scores['momentum'] = None
        reasons['momentum'] = 'not_evaluated'

        ready = self._check_pre_momentum_pass(scores, thresholds)

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
            # --- Matrix ④ Realtime Momentum (costs ~6 seconds) ---
            log.info(
                f"[Matrix] ${symbol} pre-momentum PASS: "
                f"T={scores['trend']} V={scores['volume']} P={scores['price']} S={scores['signal']} "
                f"→ running 5×3s momentum check..."
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

        else:
            momentum_final_price = None

        return {
            'scores': scores,
            'reasons': reasons,
            'ready_for_momentum': ready,
            'action': action,
            'action_reason': action_reason,
            'current_price': current_price,
            'momentum_final_price': momentum_final_price,  # Fix 2: accurate trigger price
        }
    def _check_pre_momentum_pass(self, scores, thresholds):
        """Check if matrices ①④ meet thresholds for momentum trigger.
        Volume (②) and Signal (⑤) are pure bonus — never block, only add passing count.
        Only Trend (①) and Price (③) are structural hard-gates.
        """
        # Only real-time structural matrices that we can reliably measure
        hard_checks = [
            ('trend', scores.get('trend', 0), thresholds['trend_min']),
            ('price', scores.get('price', 0), thresholds['price_min']),
        ]

        passing_count = sum(1 for _, val, _ in hard_checks if val >= 60)
        hard_fails = any(val < mins for _, val, mins in hard_checks)

        if hard_fails:
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
        if age_minutes >= thresholds['max_obs_minutes']:
            return f'timeout ({age_minutes:.0f}min >= {thresholds["max_obs_minutes"]}min)'

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

        # === Hard Stop-Loss ===
        # Default -7.5% — momentum entries that immediately drop 7.5% are bad signals.
        # dynamic_sl can tighten this if trailing stop moves SL up.
        hard_sl = entry.get('dynamic_sl', -0.075)
        if current_pnl <= hard_sl:
            return {
                'action': 'exit',
                'reason': f'hard_sl ({current_pnl:.1%} <= {hard_sl:.1%})',
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
            if peak_pnl >= 0.20:
                base_factor = 0.7    # >= +20% preserve at least 70% (was 0.6, data: avg 45% retention too low)
            elif peak_pnl >= 0.10:
                base_factor = 0.55   # >= +10% preserve at least 55%
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
                vel_factor = 0.70    # neutral → moderate protection

            # Ratchet: use whichever is higher, never lower the factor
            current_factor = entry.get('_trail_factor', base_factor)
            trail_factor = max(base_factor, vel_factor, current_factor)
            trail_floor = peak_pnl * trail_factor

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
