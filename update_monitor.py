import sys
import re

with open('scripts/paper_trade_monitor.py', 'r') as f:
    code = f.read()

# Update Fast Lane Logic
fl_old = """                    _is_fast_lane = (_t_score and _t_score >= 100
                                       and _v_score and _v_score >= 100
                                       and _s_score and _s_score >= 100
                                       and _m_score and _m_score >= 100
                                       and _fl_bs_ratio >= 2.0
                                       and _fl_pc_m5 > 0  # 5min price must be UP
                                       and _entry_count == 0  # re-entries must go through SmartEntry
                                       and _fl_sig_type == 'ATH')  # NOT_ATH → SmartEntry"""

fl_new = """                    _is_fast_lane = (_t_score and _t_score >= 100
                                       and _v_score and _v_score >= 100
                                       and _s_score and _s_score >= 100
                                       and _m_score and _m_score >= 100
                                       and _fl_bs_ratio >= 2.0
                                       and _fl_pc_m5 > 5.0  # 5min price must be > 5.0%
                                       and _fl_sig_type == 'ATH')  # NOT_ATH → SmartEntry"""

code = code.replace(fl_old, fl_new)

code = code.replace(
    "if live_price is not None and live_price < trigger_price * 0.90:",
    "if live_price is not None and live_price < trigger_price * 0.85:"
)

code = code.replace(
    "if _fl_price is not None and trigger_price and _fl_price < trigger_price * 0.90:",
    "if _fl_price is not None and trigger_price and _fl_price < trigger_price * 0.85:"
)

# Replace the async smart entry pool logic with a direct call
async_call_old = """                            if _se_future is None:
                                # P=30 OVEREXTENDED GUARD: block P≤30 from SmartEntry path.
                                # P=30 means price already 2x'd from signal (growth>100%).
                                # Data: 12 ATH P=30 via SmartEntry = 8% win rate, -74.7% total.
                                # Fast-lane bypasses SmartEntry, so golden dogs with P=30 still pass.
                                _p_score = _scores.get('price', 50)
                                if _p_score <= 30 and _p_score > 0:
                                    _fl_last_log = pending.get('_p30_log_ts', 0)
                                    if time.time() - _fl_last_log >= 30:
                                        log.info(
                                            f"  [SmartEntry] {pending['symbol']} BLOCKED: "
                                            f"P={_p_score} overextended (growth>100%) — "
                                            f"only Fast-lane can buy P≤30"
                                        )
                                        pending['_p30_log_ts'] = time.time()
                                    pending_entries.pop(lifecycle_id, None)
                                    continue

                                # First time: submit SmartEntry to thread pool (non-blocking)
                                _se_future = smart_entry_pool.submit(
                                    evaluate_smart_entry,
                                    pending['token_ca'],
                                    symbol=pending['symbol'],
                                    pool_address=pending['pool'],
                                    entry_count=pending_w_entry.get('entry_count', 0) if pending_w_entry else 0,
                                )
                                pending['_smart_entry_future'] = _se_future
                                log.info(f"  [SmartEntry] {pending['symbol']} submitted to async thread pool")
                                continue  # Move to next pending coin, don't block

                            if not _se_future.done():
                                # Still running in background — skip, process other coins
                                continue

                            # Future completed — collect result
                            try:
                                should_enter, timing_reason, timing_detail, timing_trigger_price = _se_future.result()
                            except Exception as _se_err:
                                log.error(f"  [SmartEntry] {pending['symbol']} thread error: {_se_err}", exc_info=True)
                                pending_entries.pop(lifecycle_id, None)
                                continue
                            finally:
                                pending.pop('_smart_entry_future', None)  # Clean up future reference"""

async_call_new = """                            _p_score = _scores.get('price', 50)
                            if _p_score <= 30 and _p_score > 0:
                                _fl_last_log = pending.get('_p30_log_ts', 0)
                                if time.time() - _fl_last_log >= 30:
                                    log.info(
                                        f"  [SmartEntry] {pending['symbol']} BLOCKED: "
                                        f"P={_p_score} overextended — only Fast-lane can buy"
                                    )
                                    pending['_p30_log_ts'] = time.time()
                                pending_entries.pop(lifecycle_id, None)
                                continue

                            # Execute synchronous direct evaluation (No 15m wait loop)
                            try:
                                should_enter, timing_reason, timing_detail, timing_trigger_price = evaluate_smart_entry(
                                    pending['token_ca'],
                                    symbol=pending['symbol'],
                                    pool_address=pending['pool'],
                                    entry_count=pending_w_entry.get('entry_count', 0) if pending_w_entry else 0,
                                )
                            except Exception as _se_err:
                                log.error(f"  [SmartEntry] {pending['symbol']} evaluation error: {_se_err}", exc_info=True)
                                pending_entries.pop(lifecycle_id, None)
                                continue"""

if async_call_old in code:
    print("Found async call")
    code = code.replace(async_call_old, async_call_new)
else:
    print("Could not find async call block")

with open('scripts/paper_trade_monitor.py', 'w') as f:
    f.write(code)

print("Updated monitor.")
