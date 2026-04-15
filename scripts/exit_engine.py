#!/usr/bin/env python3
"""
Exit Engine — Guardian Thread + Exit Processing

Extracted from paper_trade_monitor.py for modularity.
All sell-side monitoring and execution logic lives here.
"""

import time
import logging
import threading

log = logging.getLogger('paper_trade_monitor')


# ─── EXIT Guardian Thread ─────────────────────────────────────────────────────
# Independent thread that monitors all positions every 3 seconds.
# Never blocked by SmartEntry or watchlist scanning.
# ─────────────────────────────────────────────────────────────────────────────

class ExitGuardianThread(threading.Thread):
    """Independent thread that monitors all positions every 3 seconds.
    Never blocked by SmartEntry or watchlist scanning.

    Checks:
    1. Hard stop-loss (emergency exit)
    2. Moon Bag trail floor (emergency exit)
    3. Breakeven stop for moon bags
    """

    def __init__(self, positions_ref, positions_lock, watchlist_store_ref,
                 exit_queue, fetch_price_fn):
        super().__init__(daemon=True, name='exit-guardian')
        self.positions = positions_ref      # shared dict reference
        self.lock = positions_lock          # threading.Lock
        self.store = watchlist_store_ref     # WatchlistStore instance
        self.exit_queue = exit_queue         # list to push exit signals (checked by main loop)
        self.exit_queue_lock = threading.Lock()
        self.fetch_price = fetch_price_fn   # fetch_realtime_price function
        self.interval = 3  # seconds
        self._running = True

    def run(self):
        log.info("[ExitGuardian] 🛡️ Started — monitoring positions every 3s")
        while self._running:
            try:
                self._check_all_positions()
            except Exception as e:
                log.error(f"[ExitGuardian] Error: {e}", exc_info=True)
            time.sleep(self.interval)

    def stop(self):
        self._running = False

    def get_pending_exits(self):
        """Retrieve and clear pending exit signals (called by main loop)."""
        with self.exit_queue_lock:
            exits = list(self.exit_queue)
            self.exit_queue.clear()
            return exits

    @staticmethod
    def _calc_velocity(ring, window_sec):
        """Price velocity in %/minute over window_sec from ring buffer."""
        now = time.time()
        pts = [(t, p) for t, p in ring if now - t <= window_sec]
        if len(pts) < 2:
            return 0.0
        dt_min = (pts[-1][0] - pts[0][0]) / 60.0
        if dt_min <= 0:
            return 0.0
        return ((pts[-1][1] - pts[0][1]) / pts[0][1] * 100) / dt_min

    @staticmethod
    def _calc_tick_volatility(ring):
        """Mean absolute tick-to-tick change as volume proxy."""
        prices = [p for _, p in ring]
        if len(prices) < 3:
            return 0.0
        changes = [abs(prices[i] - prices[i-1]) / prices[i-1]
                    for i in range(1, len(prices))]
        return sum(changes) / len(changes)

    def _check_all_positions(self):
        # Take snapshot under lock
        with self.lock:
            snapshot = list(self.positions.items())

        if not snapshot:
            return

        for trade_id, pos in snapshot:
            try:
                ca = pos.token_ca
                pool = pos.pool_address
                entry_price = pos.entry_price
                if not ca or not entry_price or entry_price <= 0:
                    continue

                # Fetch current price
                price, src, age_ms = self.fetch_price(ca, pool)
                if not price or price <= 0:
                    continue

                pnl = (price - entry_price) / entry_price

                # --- Get watchlist entry for dynamic_sl ---
                w_entry = self.store.get_by_ca(ca)
                hard_sl = -0.15
                if w_entry:
                    hard_sl = w_entry.get('dynamic_sl', -0.15)

                # === Hard Stop Loss (Double-Tap Confirmation) ===
                # P0 Fix: A single bad price read from Redis killed Coco (+73% → -20.8%).
                # Now we require TWO consecutive price checks to confirm SL breach.
                # If first check triggers SL, wait 1s and re-fetch. Only proceed if both agree.
                if pnl <= hard_sl:
                    first_price = price
                    first_pnl = pnl
                    first_src = src
                    log.info(
                        f"[ExitGuardian] ⚠️ {pos.symbol} SL CHECK #1: "
                        f"pnl={pnl*100:+.1f}% <= SL={hard_sl*100:.1f}% "
                        f"price=${price:.10f} src={src} — confirming in 1s..."
                    )

                    # Second check after 1 second delay
                    time.sleep(1.0)
                    price2, src2, age_ms2 = self.fetch_price(ca, pool)

                    if not price2 or price2 <= 0:
                        log.warning(
                            f"[ExitGuardian] {pos.symbol} SL CONFIRM FAILED: "
                            f"second price fetch returned None, holding position"
                        )
                        continue

                    pnl2 = (price2 - entry_price) / entry_price

                    # Check price divergence between the two reads
                    price_divergence = abs(price2 - first_price) / max(first_price, 1e-15)

                    if pnl2 > hard_sl:
                        # Second check says NOT in SL territory → price glitch, skip exit
                        log.warning(
                            f"[ExitGuardian] 🛡️ {pos.symbol} SL CANCELLED — price glitch detected! "
                            f"Check#1: pnl={first_pnl*100:+.1f}% price=${first_price:.10f} src={first_src} | "
                            f"Check#2: pnl={pnl2*100:+.1f}% price=${price2:.10f} src={src2} | "
                            f"divergence={price_divergence:.1%} — holding position"
                        )
                        continue

                    # Both checks confirm SL breach → proceed with exit
                    # Use the BETTER (higher) price of the two for the trigger
                    confirmed_price = max(first_price, price2)
                    confirmed_pnl = (confirmed_price - entry_price) / entry_price

                    log.info(
                        f"[ExitGuardian] 🚨 {pos.symbol} EMERGENCY SL CONFIRMED: "
                        f"Check#1: pnl={first_pnl*100:+.1f}% src={first_src} | "
                        f"Check#2: pnl={pnl2*100:+.1f}% src={src2} | "
                        f"divergence={price_divergence:.1%} — executing exit"
                    )
                    with self.exit_queue_lock:
                        self.exit_queue.append({
                            'trade_id': trade_id,
                            'symbol': pos.symbol,
                            'reason': f'guardian_hard_sl ({confirmed_pnl:.1%} <= {hard_sl:.1%})',
                            'trigger_price': confirmed_price,
                            'trigger_pnl': confirmed_pnl,
                        })
                    continue

                # === Update peak_pnl in real-time (critical for trail accuracy) ===
                if pnl > pos.peak_pnl:
                    pos.peak_pnl = pnl

                # === B+C: Record price into ring buffer for velocity calc ===
                pos.price_ring.append((time.time(), price))

                # === Compute real-time velocity + tick_volatility ===
                raw_vel_30s = self._calc_velocity(pos.price_ring, 30)
                vel_60s = self._calc_velocity(pos.price_ring, 60)
                tick_vol = self._calc_tick_volatility(pos.price_ring)

                # Multi-round velocity smoothing (3 rounds × 3s = 9s average)
                pos.vel_history.append(raw_vel_30s)
                smoothed_vel = sum(pos.vel_history) / len(pos.vel_history) if pos.vel_history else 0.0

                # Extreme crash bypass: vel < -15%/min → use raw, don't wait for avg
                # Data: GRASS crashed -75% in minutes = ~-15%/min sustained
                use_vel = raw_vel_30s if raw_vel_30s < -15.0 else smoothed_vel

                # Read Helius TPS from watchlist (written by main loop)
                tps_smooth = 0.0
                if w_entry:
                    tps_smooth = w_entry.get('_helius_tps', 0) or 0

                # Write to watchlist entry so main-loop evaluate_exit can read
                if w_entry:
                    w_entry['_guardian_velocity'] = use_vel
                    w_entry['_guardian_tick_vol'] = tick_vol

                # === Trail Floor Check (3s, velocity+volume driven, FULL RANGE) ===
                is_moon = w_entry and w_entry.get('status') == 'moon_bag'

                if not is_moon and pos.peak_pnl >= 0.05:
                    # Tiered base factor by peak level
                    if pos.peak_pnl >= 0.50:
                        base_factor = 0.65   # >= +50% — about to become moon bag
                    elif pos.peak_pnl >= 0.20:
                        base_factor = 0.6
                    elif pos.peak_pnl >= 0.10:
                        base_factor = 0.55
                    else:
                        base_factor = 0.5

                    # Velocity-driven factor
                    if raw_vel_30s < -15.0:
                        vel_factor = 0.85    # CRASH → emergency tight (skip smoothing)
                    elif use_vel < -5.0:
                        vel_factor = 0.75    # fast downtrend → very tight
                    elif use_vel > 15.0:
                        vel_factor = 0.70    # rocketing up → lock hard
                    elif use_vel > 5.0:
                        vel_factor = 0.60    # moderate pump
                    else:
                        vel_factor = base_factor

                    # Volume signals (B: tick_vol + C: Helius TPS)
                    if tick_vol < 0.001 and len(pos.price_ring) >= 5:
                        vel_factor = max(vel_factor, 0.70)   # B: no price movement
                    if tps_smooth >= 0 and tps_smooth < 0.5 and len(pos.price_ring) >= 5:
                        vel_factor = max(vel_factor, 0.70)   # C: real volume dried up

                    # Ratchet: never lower the factor
                    ratcheted = 0
                    if w_entry:
                        ratcheted = w_entry.get('_trail_factor', 0) or 0
                    trail_factor = max(base_factor, vel_factor, ratcheted)

                    # Write back ratchet for main-loop to read
                    if w_entry:
                        w_entry['_trail_factor'] = trail_factor

                    trail_floor = pos.peak_pnl * trail_factor

                    if pnl < trail_floor:
                        log.info(
                            f"[ExitGuardian] 📉 {pos.symbol} TRAIL STOP: "
                            f"pnl={pnl*100:+.1f}% < floor={trail_floor*100:.1f}% "
                            f"(peak={pos.peak_pnl*100:.1f}% factor={trail_factor:.2f}) "
                            f"vel={use_vel:.1f}%/min(raw={raw_vel_30s:.1f}) "
                            f"tick_vol={tick_vol:.4f} tps={tps_smooth:.1f} "
                            f"price=${price:.10f} src={src}"
                        )
                        with self.exit_queue_lock:
                            self.exit_queue.append({
                                'trade_id': trade_id,
                                'symbol': pos.symbol,
                                'reason': f'guardian_trail_stop (pnl={pnl:.1%} < floor={trail_floor:.1%}, peak={pos.peak_pnl:.1%}, vel={use_vel:.1f}, tps={tps_smooth:.1f})',
                                'trigger_price': price,
                                'trigger_pnl': pnl,
                            })
                        continue

                # === Profit Lock Detection (peak >= 20%, not yet locked) ===
                # Don't execute the lock from Guardian (too complex: 50% sell + moon bag state).
                # Instead, just log it so the main loop picks it up on next eval.
                if not is_moon and not has_locked and pos.peak_pnl >= 0.20:
                    log.info(
                        f"[ExitGuardian] 🌙 {pos.symbol} PROFIT LOCK DETECTED: "
                        f"peak={pos.peak_pnl*100:.1f}% >= 20% — main loop will execute lock"
                    )

                # === Moon Bag Breakeven Stop ===
                if is_moon and price <= entry_price:
                    log.info(
                        f"[ExitGuardian] 🔔 {pos.symbol} MOON BREAKEVEN: "
                        f"price=${price:.10f} <= entry=${entry_price:.10f}"
                    )
                    with self.exit_queue_lock:
                        self.exit_queue.append({
                            'trade_id': trade_id,
                            'symbol': pos.symbol,
                            'reason': f'guardian_moon_breakeven (price <= entry)',
                            'trigger_price': price,
                            'trigger_pnl': pnl,
                        })
                    continue

                # === Moon Bag Trail Floor ===
                if is_moon and w_entry:
                    moon_peak = max(w_entry.get('moon_peak_pnl', 0) or 0, pnl)
                    # Update peak in DB if needed
                    if pnl > (w_entry.get('moon_peak_pnl', 0) or 0):
                        self.store.update_position_state(w_entry['id'], moon_peak_pnl=pnl)

                    trail_factor = w_entry.get('moon_trail_factor', 0.2) or 0.2
                    moon_floor = moon_peak * trail_factor
                    if moon_peak > 0 and pnl < moon_floor:
                        log.info(
                            f"[ExitGuardian] 🔔 {pos.symbol} MOON TRAIL: "
                            f"pnl={pnl*100:+.1f}% < floor={moon_floor*100:.1f}% "
                            f"(peak={moon_peak*100:.1f}% factor={trail_factor})"
                        )
                        with self.exit_queue_lock:
                            self.exit_queue.append({
                                'trade_id': trade_id,
                                'symbol': pos.symbol,
                                'reason': f'guardian_moon_trail (pnl={pnl:.1%} < floor={moon_floor:.1%})',
                                'trigger_price': price,
                                'trigger_pnl': pnl,
                            })

            except Exception as e:
                sym = getattr(pos, 'symbol', '?') if pos else '?'
                log.warning(f"[ExitGuardian] Check failed for {sym}: {e}")


def process_guardian_exits(exit_guardian, positions, positions_lock, lifecycles,
                          strategy_id, build_lifecycle_state_fn, simulate_exit_fn):
    """Process pending Guardian exit signals and return list of positions to close.

    This encapsulates the Guardian exit processing that was previously inline in
    the main loop — preventing bugs like the to_close=[] ordering issue.

    Args:
        exit_guardian: ExitGuardianThread instance
        positions: shared positions dict
        positions_lock: threading.Lock
        lifecycles: lifecycle state dict
        strategy_id: current strategy ID
        build_lifecycle_state_fn: function to build lifecycle state
        simulate_exit_fn: function to simulate exit execution

    Returns:
        list of dicts ready for the to_close processing pipeline
    """
    to_close = []
    guardian_exits = exit_guardian.get_pending_exits()

    for gx in guardian_exits:
        gx_trade_id = gx.get('trade_id')
        if gx_trade_id not in positions:
            continue

        gx_pos = positions[gx_trade_id]
        gx_lifecycle_id = gx_pos.lifecycle_id
        gx_lifecycle = lifecycles.setdefault(
            gx_lifecycle_id,
            build_lifecycle_state_fn(
                gx_lifecycle_id, gx_pos.token_ca, gx_pos.symbol,
                gx_pos.signal_ts,
                getattr(gx_pos, 'premium_signal_id', None),
                getattr(gx_pos, 'signal_type', None)
            )
        )
        log.info(
            f"  [GUARDIAN_EXIT] 🚨 Processing {gx['symbol']}: {gx['reason']} "
            f"trigger_pnl={gx.get('trigger_pnl', 0)*100:+.1f}%"
        )
        # Simulate exit execution
        gx_sell_amount = int(float(gx_pos.token_amount_raw)) if gx_pos.token_amount_raw else 0
        gx_sim = simulate_exit_fn(
            gx_pos.token_ca, str(gx_sell_amount),
            getattr(gx_pos, 'token_decimals', 0) or 0,
            gx_pos.strategy_stage, strategy_id=strategy_id,
            lifecycle_id=gx_lifecycle_id
        )
        gx_trigger_pnl = gx.get('trigger_pnl', 0)
        to_close.append({
            'trade_id': gx_trade_id,
            'reason': gx['reason'],
            'pnl': gx_trigger_pnl,
            'trigger_pnl': gx_trigger_pnl,
            'exit_price': gx.get('trigger_price', gx_pos.entry_price),
            'exit_ts': int(time.time()),
            'mark_source': 'exit_guardian',
            'exit_eval': {
                'action': 'close',
                'execution': gx_sim if (gx_sim and gx_sim.get('success')) else {'success': True, 'synthetic': True},
            },
        })
        # IMPORTANT: Do NOT pop positions[gx_trade_id] here!
        # The main loop pipeline needs to get it to write DB logs.
        # It will be popped by close_position_as_... later.

    return to_close
