# LOTTO Stale-Reclaim Design (winner-join driven)

Date: 2026-05-30
Status: Step 1 = implement now (shadow-only). Step 2 = deferred (needs shadow evidence + authorization).

## Evidence (why this, why now)

From `/api/paper/lotto-quote-gap-winner-join` (24h, server-side join of audit gaps to
actual gold/silver/bronze missed winners):

- 7 medal winners (5 silver, 2 bronze, 0 gold), all quote-executable.
- `top_unique_joined_winners` killer breakdown:

| peak | gap | tier | killer | component |
|---|---|---|---|---|
| +72.9% | 1.9% | silver | `lotto_mc_0` (missing market cap) | lotto_entry_gate |
| +62.9% | 1.2% | silver | `lotto_stale_2070s` | lotto_entry_gate |
| +61.9% | 0.01% | silver | `lotto_stale_1826s` | lotto_entry_gate |
| +59.8% | 0.8% | silver | `lotto_stale_2447s` | lotto_entry_gate |
| +55.9% | 3.6% | silver | `lotto_stale_2174s` | lotto_entry_gate |
| +42.5% | 5.0% | bronze | `momentum_fading` | smart_entry |
| +36.4% | 0.1% | silver | `lotto_stale_2215s` | lotto_entry_gate |

- 5/7 killed by `lotto_stale_*` (age just over the 30-min cutoff), 1/7 by `lotto_mc_0`
  (missing market cap), all quote-clean (gap <=5%).
- Confirmation gates (`no_kline_low_volume`, `not_ath_prebuy_kline_block`,
  `weak_buying_pressure`) had `medal_unique = 0` this window: they rejected only
  non-movers (peaks <=16%). The earlier "confirmation gate is the main killer"
  hypothesis is superseded: the dominant killer is the **30-min staleness expiry**.

Root constant: `scripts/lotto_engine.py` `LOTTO_ENTRY_STALE_SEC = 30*60`. It is the
first check in `evaluate_lotto_entry` (after data_health), before any momentum / quote /
liquidity evaluation. A 31-min-old token that is igniting hard is expired before its
good signals are even read.

## Critical loophole (already mitigated by existing infra)

Winner-join reports PEAK pnl, not entry-forward pnl. "Peaked +60%" does NOT prove that
entering a 40-min-old token is profitable — you might buy near the top and round-trip.

The existing `LOTTO_NOT_ATH_WATCH_SHADOW` mechanism already protects against this:
`_lotto_not_ath_watch_shadow_decision` requires two-horizon forward retention:
- `pnl_5m >= 0.05`, `pnl_15m >= 0.10`
- `pnl_15m >= pnl_5m * 0.5` else `reclaim_decayed` reject

So the safe path is to reuse this shadow (which measures forward retention), not to relax
the stale gate blindly.

## Design: reuse the existing NOT_ATH watch-shadow, do not build a new lane

Infrastructure that already exists:
- `LOTTO_NOT_ATH_WATCH_SHADOW` watches blocked LOTTO dogs, gates on quote_gap<8% /
  liq>5k / vol_m5>5k, confirms via two-horizon retention, and produces the
  `lotto_not_ath_reclaim_tiny_probe` mode.
- Gap 1: its `LOTTO_NOT_ATH_WATCH_PARENT_BLOCKERS` covers only `not_ath_v17` /
  `not_ath_prebuy_kline_*` — NOT `lotto_stale_*` or `lotto_mc_0`.
- Gap 2: it is `watch_only` (never fills).

### Step 1 — broaden shadow observation (IMPLEMENT NOW, shadow-only, no live fill)

Make the existing watch-shadow start OBSERVING the two dominant killers, so its existing
quality + forward-retention gates measure them. Pure measurement; creates no paper/live
entry.

Changes (`scripts/paper_trade_monitor.py`):
1. Add `'lotto_mc_0'` to `LOTTO_NOT_ATH_WATCH_PARENT_BLOCKERS` (exact match; the
   missing-market-cap case only — NOT `lotto_mc_<big>` legit too-big rejects).
2. Add a `LOTTO_NOT_ATH_WATCH_PARENT_BLOCKER_MATCH` SQL fragment that also matches
   `reject_reason LIKE 'lotto_stale_%'` (dynamic suffix), and use it in the two
   candidate queries (`record_lotto_not_ath_watch_shadow_candidates`,
   `_record_lotto_not_ath_watch_relaxed_observation_snapshots`). Both already filter
   `component IN ('upstream_gate','lotto_entry_gate')`, which `lotto_stale` satisfies.
3. Normalize parent_blocker: `lotto_stale_*` -> canonical `'lotto_stale'` in
   `_lotto_not_ath_watch_parent_blocker` so snapshots dedup/group by a stable label.

What Step 1 answers (and the winner-join cannot): of stale-but-clean dogs, how many pass
the two-horizon forward-retention confirmation. That is the forward-EV evidence required
before any fill.

### Step 2 — promote to ultra-tiny fill (DEFERRED, needs shadow evidence + authorization)

After Step 1 shadow shows positive forward retention for the stale cohort:
- Flip `lotto_not_ath_reclaim_tiny_probe` from watch_only to an ultra-tiny paper fill,
  gated through `entry-mode-registry`: `shadow_watch_only -> revival_canary -> live`,
  judged by winner-join net EV / precision / rug-rate (NOT recall on peaks).
- Implementation constraint: the live fill decision must use LIVE momentum
  (`dex_snapshot` m5 available in `evaluate_lotto_entry`), not the stored post-hoc
  horizon samples the shadow uses to validate the hypothesis.
- Add a gate-side `lotto_mc_0` fallback in `lotto_engine.py` (use DexScreener
  market_cap/fdv before expiring on missing signal MC). This is a LIVE gate change, so it
  belongs in Step 2, not Step 1.

## Boundaries / honesty

- Sample is small: n=7 medal winners, single 24h window, 0 gold. "Stale is the #1 killer"
  is a strong leading hypothesis, not yet conclusive. Step 1 exists to thicken it.
- Default stays conservative: the 30-min stale expiry is UNCHANGED. Only quote-clean +
  forward-retaining stale dogs get a second-chance observation (Step 1) and later an
  ultra-tiny shadow fill (Step 2).
- Fills are currently frozen (~1/24h), so there is no forward EV signal yet; Step 1 is how
  we start generating it for the stale cohort.

## Verification

- `python3 -m py_compile scripts/paper_trade_monitor.py scripts/lotto_engine.py`
- Run `test_paper_monitor_strategy_helpers.py` and any `test_lotto_*` / not-ath watch
  shadow tests.
- Production: confirm `lotto_not_ath_watch_shadow` decision events start appearing with
  `parent_blocker` in {`lotto_stale`, `lotto_mc_0`}; confirm NO new paper_trades fills are
  created by the change.
