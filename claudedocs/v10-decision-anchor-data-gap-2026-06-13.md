# V10 Decision-Anchor Data Gap

Status: offline research note, 2026-06-13.

This note follows `v10-clean-cohort-signal-anchor-audit-2026-06-13.md`.
The v10 label/return cohort and GMGN full-window signal-anchor evidence are now
complete. The next intended analysis is formal matched signal-vs-decision
ceiling plus executable funnel attribution.

That analysis is blocked by missing full-window paper decision data in the
current data room.

## Current Data Room Coverage

Data room:

`/Users/boliu/sas-data-room/chain-truth-recut-20260612T011545Z`

Raw/v10 signal window:

- rows: 1432
- dogs: 461
- duds: 971
- signal range: 2026-06-06 09:58:07 UTC to 2026-06-11 14:56:09 UTC

Available local paper DB candidate:

`/Users/boliu/sas_paper_current.db`

Paper event coverage:

- `a_class_decision_events`: 1922 rows, 2026-06-07 06:07:05 UTC to 2026-06-07 08:29:06 UTC
- `opportunity_events`: 810 rows, 2026-06-07 06:07:05 UTC to 2026-06-07 08:29:06 UTC
- `canonical_trade_ledger`: 2 rows, 2026-06-07 06:13:52 UTC to 2026-06-07 07:33:24 UTC

The available paper DB covers only a small slice of the v10 raw signal window.

## Partial Slice Result

Output:

`/Users/boliu/sas-data-room/chain-truth-recut-20260612T011545Z/v10-partial-decision-slice/decision-slice.json`

Matching rule:

`token_ca` and nearest decision event with `event_ts` in `[signal_ts - 60s, signal_ts + 900s]`.

Partial slice:

- v10 rows near the paper DB event window: 7
- matched: 7/7
- matched dogs: 1
- matched duds: 6
- matched source: all `a_class_decision_events`
- matched return domain: all `usd_gmgn`

Matched funnel:

- quote clean: 6/7
- quote executable: 5/7
- route available: 5/7
- would enter: 3/7
- did enter: 0/7
- block cause: `UNKNOWN` 3, `MARKET` 4
- hydrate outcome: `success` 5, `skipped_source_budget` 1, `skipped_hard_market_red` 1

Dog row in this partial slice:

- matched: 1/1
- quote clean: 1/1
- quote executable: 1/1
- would enter: 1/1
- hydrate outcome: `success`

## Interpretation

The partial slice proves that the field chain is usable:

- `provider_hydrate_outcome` is present in `a_class_decision_events`;
- `hydrate_outcome` is present in `opportunity_events`;
- quote/route/liquidity/spread/hard-blocker fields are available;
- decision events can be joined to v10 signal rows by token and bounded time.

It does **not** support a business conclusion because it covers only 7 v10
signal rows.

Do not use this partial slice to estimate win rate, capture rate, gate quality,
or executable rate.

## Required Full Data Pack Addition

The next full data room must include a paper decision subset covering the same
raw signal window:

2026-06-06 09:58:07 UTC to 2026-06-11 14:56:09 UTC

Required tables:

1. `a_class_decision_events`
2. `opportunity_events`
3. `canonical_trade_ledger`

Required fields from `a_class_decision_events`:

- `id`
- `event_ts`
- `token_ca`
- `symbol`
- `lifecycle_id`
- `signal_ts`
- `opportunity_ts`
- `action`
- `would_action`
- `grade`
- `score`
- `reason`
- `hard_blockers_json`
- `soft_notes_json`
- `block_cause`
- `recoverability`
- `classification_reason`
- `blocker_classifications_json`
- `quote_available`
- `quote_executable`
- `quote_clean`
- `route_available`
- `quote_source`
- `quote_age_sec`
- `data_confidence`
- `provider_reason`
- `provider_hydrate_outcome`
- `evidence_status`
- `quote_failure_reason`
- `route_failure_reason`
- `liquidity_usd`
- `spread_pct`
- `expected_rr`
- `defined_risk_pct`
- `expected_upside_pct`
- `matrix_json`

Required fields from `opportunity_events`:

- `id`
- `opportunity_key`
- `event_ts`
- `token_ca`
- `symbol`
- `lifecycle_id`
- `source_type`
- `source_component`
- `source_reason`
- `raw_signal_ts`
- `opportunity_ts`
- `quote_available`
- `quote_executable`
- `quote_clean`
- `route_available`
- `liquidity_usd`
- `spread_pct`
- `matrix_score`
- `expected_rr`
- `defined_risk_pct`
- `hard_blockers_json`
- `soft_notes_json`
- `would_enter_a_class`
- `did_enter`
- `linked_trade_id`
- `final_entry_decision_json`
- `quote_source`
- `quote_age_sec`
- `data_confidence`
- `provider_data_state`
- `provider_reason`
- `provider_attempts_json`
- `evidence_status`
- `quote_failure_reason`
- `block_cause`
- `recoverability`
- `classification_reason`
- `blocker_classifications_json`
- `hydrate_outcome`
- `hydrate_success`
- `path_sample_count`

Required fields from `canonical_trade_ledger`:

- `trade_id`
- `token_ca`
- `symbol`
- `lifecycle_id`
- `entry_ts`
- `entry_size_sol`
- `entry_price`
- `entry_quote_source`
- `entry_route_available`
- `entry_quote_executable`
- `entry_spread_pct`
- `entry_liquidity_usd`
- `entry_data_confidence`
- `exit_ts`
- `exit_price`
- `exit_reason`
- `realized_pnl_sol`
- `realized_pnl_pct`
- `peak_quote_pnl_pct`
- `time_to_peak_sec`
- `time_held_sec`
- `loss_cap_breach`
- `loss_cap_pct`

## Export Helper

Use the checked-in subset exporter to build the decision pack from a full
`paper_trades.db` snapshot. It exports only the required tables/columns and
widens the cohort window by a bounded margin for matching.

Example:

```bash
node scripts/export-paper-decision-subset.js \
  --paper-db /path/to/paper_trades.db \
  --out-db /path/to/paper_decision_subset.db \
  --cohort-dogs /Users/boliu/sas-data-room/chain-truth-recut-20260612T011545Z/cohort-rebuild-v10-final-native-return-guard/rebuilt-clean-dogs.json \
  --cohort-duds /Users/boliu/sas-data-room/chain-truth-recut-20260612T011545Z/cohort-rebuild-v10-final-native-return-guard/rebuilt-clean-duds.json \
  --margin-sec 900
```

Smoke validation against the local partial paper DB succeeded:

- output: `/Users/boliu/sas-data-room/chain-truth-recut-20260612T011545Z/paper-decision-subset-smoke/paper_decision_subset.db`
- `a_class_decision_events`: 1922 rows
- `opportunity_events`: 810 rows
- `canonical_trade_ledger`: 2 rows

This smoke DB is useful only to validate the exporter. It is not a valid
decision-anchor pack for v10 because the source paper DB covers only a small
slice of the v10 signal window.

Once a full-window subset exists, run the v10 decision funnel wrapper:

```bash
node scripts/run-v10-decision-funnel-audit.js \
  --paper-db /path/to/paper_decision_subset.db \
  --dogs /Users/boliu/sas-data-room/chain-truth-recut-20260612T011545Z/cohort-rebuild-v10-final-native-return-guard/rebuilt-clean-dogs.json \
  --duds /Users/boliu/sas-data-room/chain-truth-recut-20260612T011545Z/cohort-rebuild-v10-final-native-return-guard/rebuilt-clean-duds.json \
  --out-dir /path/to/v10-decision-funnel-audit
```

The wrapper consumes the v10 clean cohort directly, so it does not fall back to
the old `raw_signal_outcomes` eligible-row logic. Smoke validation against the
partial subset produced the expected warning shape: only 1/461 dogs and 6/971
duds matched a decision record. That validates the join path but proves again
that the partial subset cannot support a strategy conclusion.

For the normal path, use the one-command pack builder instead:

```bash
node scripts/build-v10-decision-anchor-pack.js \
  --paper-db /path/to/full/paper_trades.db \
  --out-dir /Users/boliu/sas-data-room/chain-truth-recut-20260612T011545Z/v10-decision-anchor-pack
```

This runs the subset exporter and v10 funnel wrapper together, then writes
`decision-anchor-pack-summary.json`. If match rates are very low, the summary is
marked `review_required` because that can mean either a partial paper DB or a
real pipeline coverage gap. Do not interpret low matching until source DB
coverage is confirmed.

The pack builder also records exported table ranges for
`a_class_decision_events`, `opportunity_events`, and `canonical_trade_ledger`.
If the decision tables start more than 1 hour after the cohort start, or end
more than 1 hour before the cohort end, the summary emits explicit range
warnings. Those warnings mean the pack is not yet a full v10 decision-anchor
pack.

If creating a fresh snapshot from a deployed shell, pass the v10 window
explicitly to `create-rawdog-audit-snapshot.sh` so it uses the fixed-window
exporter instead of the old rolling-`HOURS` fallback:

```bash
START_TS=1780739887 \
END_TS=1781189769 \
bash scripts/create-rawdog-audit-snapshot.sh
```

If the v10 cohort files are present in the deployed shell, this equivalent form
also works:

```bash
COHORT_DOGS=/path/to/rebuilt-clean-dogs.json \
COHORT_DUDS=/path/to/rebuilt-clean-duds.json \
bash scripts/create-rawdog-audit-snapshot.sh
```

Without either `START_TS`/`END_TS` or `COHORT_DOGS`/`COHORT_DUDS`, that snapshot
script falls back to a rolling recent-hours subset. The fallback is useful for
smoke/health checks, but it is not a valid v10 decision-anchor pack.

## Next Valid Analysis After Full Decision Pack

1. Matched signal-vs-decision ceiling on the same signal-level cohort.
2. Decision-time dog-vs-dud feature audit.
3. Executable funnel:
   raw dog → has decision record → quote clean → final executable → would enter → entered → held.
4. Blocker attribution by `INFRA` / `MARKET` / `POLICY`, using hydrate outcome and hard blockers.
5. Separate results by return domain:
   `usd_gmgn`, `spliced_curve_to_gmgn`, and `sol_curve`.

## Guardrail

Until the full decision subset exists, no strategy conclusion should be drawn
from decision-anchor analysis.

Live gate, matrix/RR thresholds, exit policy, and live size remain frozen.
