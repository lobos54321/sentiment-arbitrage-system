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

                # === Hard Stop Loss ===
                if pnl <= hard_sl:
                    log.info(
                        f"[ExitGuardian] 🚨 {pos.symbol} EMERGENCY SL: "
                        f"pnl={pnl*100:+.1f}% <= SL={hard_sl*100:.1f}% "
                        f"price=${price:.10f}"
                    )
                    with self.exit_queue_lock:
                        self.exit_queue.append({
                            'trade_id': trade_id,
                            'symbol': pos.symbol,
                            'reason': f'guardian_hard_sl ({pnl:.1%} <= {hard_sl:.1%})',
                            'trigger_price': price,
                            'trigger_pnl': pnl,
                        })
                    continue

                # === Moon Bag Breakeven Stop ===
                is_moon = w_entry and w_entry.get('status') == 'moon_bag'
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
            'execution': gx_sim if gx_sim.get('success') else None,
        })
        with positions_lock:
            positions.pop(gx_trade_id, None)

    return to_close
