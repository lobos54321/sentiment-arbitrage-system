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


def _lazy_import():
    """Lazy import to avoid circular dependency with paper_trade_monitor."""
    global _trend_fn, _bars_fn, _price_fn
    if _trend_fn is None:
        from paper_trade_monitor import (
            check_multi_bar_trend,
            get_notath_bars,
            fetch_realtime_price,
        )
        _trend_fn = check_multi_bar_trend
        _bars_fn = get_notath_bars
        _price_fn = fetch_realtime_price


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


def score_volume(bars, signal_tx24h=0):
    """
    Matrix ② — Volume Momentum
    Compares recent bar volume to previous 3-bar average.

    Returns: (score: int 0-100, reason: str)
    """
    if not bars or len(bars) < 4:
        return 50, 'insufficient_bars'

    recent_vol = bars[0].get('volume', 0)
    prev_vols = [b.get('volume', 0) for b in bars[1:4]]
    avg_prev = sum(prev_vols) / len(prev_vols) if prev_vols else 0

    # All-zero = data absent, not volume decline — fail-open
    if avg_prev <= 0 and recent_vol <= 0:
        return 50, 'no_volume_data (fail-open)'

    if avg_prev <= 0:
        vol_ratio = 999.0  # recent_vol > 0 with no prior data = new volume surge
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


def score_price_strength(current_price, signal_price, lowest_price):
    """
    Matrix ③ — Price Strength
    Evaluates growth from signal price and recovery from observed dip.

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


def score_realtime_momentum(token_ca, pool_address, interval_sec=3):
    """
    Matrix ④ — Realtime Momentum (3×3-second snapshots)
    Only called when matrices ①②③⑤ are already passing.

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

    s1, s2, s3 = snapshots[0], snapshots[1], snapshots[2]

    if s1 < s2 < s3:
        return 100, f'ascending s1={s1:.10f} s2={s2:.10f} s3={s3:.10f}', snapshots
    elif s3 >= s2 >= s1 and s3 > s1:
        return 60, f'flat_up s1={s1:.10f} s2={s2:.10f} s3={s3:.10f}', snapshots
    else:
        return 0, f'declining s1={s1:.10f} s2={s2:.10f} s3={s3:.10f}', snapshots


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
    ATH_THRESHOLDS = {
        'trend_min': 50,    # must pass
        'volume_min': 0,    # ATH self-carries volume
        'price_min': 70,    # must pass (anti-chase)
        'signal_min': 0,    # ATH = auto 100
        'momentum_min': 60, # must not decline
        'min_passing': 3,   # at least 3 of 5 >= 60
        'max_obs_minutes': 30,  # 30 min max for ATH
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

        # --- Matrix ① Trend ---
        bars = None
        if pool:
            bars = _bars_fn(pool, limit=5)

        # If no bars from kline_cache, build synthetic bars from our own price history
        if not bars or len(bars) < 3:
            bars = self._get_synthetic_bars(ca)

        scores['trend'], reasons['trend'], _ = score_trend(bars, symbol)

        # --- Matrix ② Volume ---
        scores['volume'], reasons['volume'] = score_volume(
            bars, signal_tx24h=entry.get('signal_tx24h', 0)
        )

        # --- Matrix ③ Price Strength ---
        current_price = None
        if pool:
            current_price, _, _ = _price_fn(ca, pool)

        scores['price'], reasons['price'] = score_price_strength(
            current_price,
            entry.get('signal_price'),
            entry.get('lowest_price'),
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
            # Keep only last 10 minutes of observations (avoid memory leak)
            cutoff = int(time.time()) - 600
            self._price_history[ca] = [(t, p) for t, p in history if t >= cutoff]

        # --- Matrix ⑤ Signal Evolution ---
        scores['signal'], reasons['signal'] = score_signal_evolution(entry)

        # --- Check pre-momentum thresholds ---
        scores['momentum'] = None
        reasons['momentum'] = 'not_evaluated'

        ready = self._check_pre_momentum_pass(scores, thresholds)

        # Hard blocks: no zero scores allowed for critical matrices
        hard_block = None
        if scores['trend'] == 0:
            ready = False
            hard_block = 'trend=0'
        if scores['price'] == 0:
            ready = False
            hard_block = (hard_block + '+' if hard_block else '') + 'price=0'
        if scores['volume'] < thresholds['volume_min']:
            # Volume is informational only — logged but not a blocker
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
                f"→ running 3×3s momentum check..."
            )

            # For re-entries: verify price > last exit price
            if entry.get('entry_count', 0) > 0 and entry.get('last_exit_pnl') is not None:
                if current_price and entry.get('entry_price') and current_price <= entry['entry_price']:
                    return {
                        'scores': scores, 'reasons': reasons,
                        'ready_for_momentum': False,
                        'action': 'wait',
                        'action_reason': 'reentry: price below last exit, waiting for recovery',
                    }

            scores['momentum'], reasons['momentum'], snaps = score_realtime_momentum(
                ca, pool
            )

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

        return {
            'scores': scores,
            'reasons': reasons,
            'ready_for_momentum': ready,
            'action': action,
            'action_reason': action_reason,
            'current_price': current_price,
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

    def _get_synthetic_bars(self, ca, bar_count=5):
        """Build synthetic 1-minute bars from accumulated price observations.
        
        Each evaluate() call records a (timestamp, price) pair. We bucket these
        into 1-minute windows to create OHLCV-like bars so that score_trend and
        score_volume can make real decisions instead of returning fail-open 50.
        
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
        signal_price = entry.get('signal_price')
        lowest_price = entry.get('lowest_price')
        if signal_price and lowest_price:
            if lowest_price < signal_price * 0.5:
                return f'price_collapse (lowest={lowest_price:.10f} < 50% of signal={signal_price:.10f})'

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

        # === Hard Stop-Loss (-7.5%) ===
        hard_sl = entry.get('dynamic_sl', -0.075)
        if current_pnl <= hard_sl:
            return {
                'action': 'exit',
                'reason': f'hard_sl ({current_pnl:.1%} <= {hard_sl:.1%})',
                'current_pnl': current_pnl,
                'trail_floor': None,
            }

        # === Profit Lock at +20% (check BEFORE trail to prevent competition) ===
        if peak_pnl >= 0.20 and not entry.get('has_locked_profit'):
            return {
                'action': 'lock_profit',
                'reason': f'profit_lock (peak={peak_pnl:.1%} >= 20%)',
                'current_pnl': current_pnl,
                'trail_floor': None,
            }

        # === Trailing Stop ===
        trail_floor = None
        if peak_pnl >= 0.05:  # +5% trail activation
            if peak_pnl < 0.20:
                trail_floor = peak_pnl * 0.5   # preserve 50% of peak
            else:
                trail_floor = peak_pnl * 0.6   # preserve 60% of peak

            if current_pnl < trail_floor:
                return {
                    'action': 'exit',
                    'reason': f'trail_stop (pnl={current_pnl:.1%} < floor={trail_floor:.1%}, peak={peak_pnl:.1%})',
                    'current_pnl': current_pnl,
                    'trail_floor': trail_floor,
                }

        # === Matrix-based soft exit (trend check) ===
        pool = entry.get('pool_address')
        should_check_matrix = (
            pool and
            time.time() - entry.get('last_matrix_check', 0) >= 60
        )

        if should_check_matrix:
            bars = _bars_fn(pool, limit=5)

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

        # === Moon Trail (factor 0.3) ===
        moon_floor = moon_peak * 0.3
        if moon_floor > 0 and current_pnl < moon_floor:
            return {
                'action': 'exit',
                'reason': f'moon_trail (pnl={current_pnl:.1%} < floor={moon_floor:.1%}, peak={moon_peak:.1%})',
                'current_pnl': current_pnl,
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
            bars = _bars_fn(pool, limit=5)
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
                }
            else:
                return {
                    'action': 'hold',
                    'reason': 'moon_trend_ok',
                    'current_pnl': current_pnl,
                    'moon_peak_pnl': moon_peak,
                    'new_moon_trend_zero_count': 0,
                }

        return {
            'action': 'hold',
            'reason': 'moon_ok',
            'current_pnl': current_pnl,
            'moon_peak_pnl': moon_peak,
        }
