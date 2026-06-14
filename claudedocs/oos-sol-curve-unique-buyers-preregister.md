# OOS Preregistration - Sol Curve Unique Buyers

Status: preregistered analysis plan / no production strategy change
Created: 2026-06-14

This document locks the out-of-sample test plan before any new OOS data pack is generated or inspected. It exists to prevent the last remaining edge candidate from being fitted to the discovery window.

## 0. Decision Context

The current discovery window has produced the first artifact-controlled ex-ante signal candidate:

- Domain: `return_domain = sol_curve`
- Coverage condition: `has_trades = true`
- Primary feature: `unique_buyers` in the no-future window `[signal_ts - 900s, signal_ts]`
- Discovery-window result: weak-to-moderate separation, roughly AUC `0.58-0.65` after domain and coverage controls.

This is not live evidence. It is a candidate that must survive a locked OOS test.

Do not change live gate, matrix/RR, liquidity rules, final entry contract, exit, position size, or live execution based on the discovery-window result.

## 1. Discovery Data Is Frozen History

Discovery/reference data:

```text
/Users/boliu/sas-data-room/chain-truth-recut-20260612T011545Z
```

Discovery export pack:

```text
/Users/boliu/sas-data-room/chain-truth-recut-20260612T011545Z/v10-curve-feature-v1/stratified-samples/export-pack-v2
```

Dune export used for discovery analysis:

```text
/Users/boliu/sas-data-room/chain-truth-recut-20260612T011545Z/v10-curve-feature-v1/stratified-samples/dune-export-20260614T044443Z
```

Discovery pack facts:

- `299` signal windows
- `149` dog / `150` dud
- balanced by `return_domain`
- no-future feature window: `[signal_ts - 900s, signal_ts]`
- Dune export rows: `205,726`
- Dune export SHA256: `f16fb8db542a12b84355a15794e5b9711ce19a5b5be54c66a0b74c1cd4d1e8b1`

The discovery window may be used only to explain why this OOS test exists. It must not be used to change thresholds, swap the primary feature, add new success metrics, or choose an OOS time range.

## 2. OOS Time Split

The OOS window must be a new frozen data pack generated after this document is committed and pushed.

Rules:

1. The OOS window must not overlap the discovery window.
2. The OOS window should not be adjacent to the discovery window; leave a gap when data availability allows.
3. Prefer a materially different time period, ideally weeks apart, to reduce market-regime leakage.
4. The OOS pack must record:
   - `generated_at`
   - production commit
   - raw data window start/end
   - all file hashes
   - schema versions
   - cleaning and cohort-build script commits

If only a near-term OOS window is available, mark it as `near_window_oos` and treat success as provisional until a second independent pack reproduces it.

## 3. Symmetric Cohort Cleaning

Both dog and dud rows must pass the same cleaning and guardrails:

- native-return guard
- unit-domain guard
- `label_unit_suspect` checks
- chain-truth / GMGN full-window repairs when required
- same row-level schema for dog and dud

No asymmetric cleaning is allowed. If dog rows use v10 native-return guard, dud rows must use the same guard.

Rows that cannot be adjudicated must remain in quarantine or be excluded under a predeclared rule. They must not be silently promoted into clean dog or clean dud sets.

## 4. Cohort And Filter

Primary OOS cohort:

```text
return_domain = sol_curve
has_trades = true
```

Unit of analysis:

- Primary: signal row / token-anchor opportunity.
- Sensitivity: unique token-level de-duplication.

Dud definition:

- same bucket and same OOS pack as dog rows
- must be cleaned with the same return and unit guards
- should be matched to the same `return_domain = sol_curve` and `has_trades = true` filter

Do not compare sol-curve dogs to all duds. Do not pool `sol_curve`, `spliced_curve_to_gmgn`, and `usd_gmgn` into one headline AUC.

## 5. Feature Window

All features must be computed strictly from:

```text
[signal_ts - 900s, signal_ts]
```

No post-signal trades, no future bars, no `anchor + 5m`, no `anchor + 15m`, no `120m total volume`.

Rows with incomplete source coverage must be reported separately. Missing or incomplete windows cannot be treated as zero activity unless the export source proves full coverage for that window.

## 6. Primary And Secondary Features

Primary endpoint:

```text
unique_buyers
```

Secondary participation-breadth family:

- `unique_sellers`
- `sell_count`
- `trade_density_per_min`
- `buy_count`
- `total_sol_volume`
- `repeat_wallet_count`
- `recurring_wallet_ratio`

The primary endpoint cannot be changed after viewing OOS data. Secondary features are explanatory only unless a second preregistration is written for a later OOS pack.

## 7. Required Controls

The OOS report must include:

- dog/dud usable-rate symmetry
- dog/dud `has_trades` rate
- AUC for the full primary cohort
- AUC on trades-present rows only
- progress/stage split
- UTC date or regime split
- signal source split if source metadata exists
- unique-token sensitivity
- bootstrap confidence interval for primary AUC
- top-k precision for `k = 20` and `k = 30`, or the largest feasible k if the OOS sol-curve cohort is small

If usable-rate or trade-hit asymmetry is large, explain coverage before reading AUC.

## 8. Success, Failure, And Gray Zone

Success requires all of:

1. Primary `unique_buyers` AUC `> 0.60`.
2. 95% bootstrap CI lower bound `> 0.55`.
3. Top-20 or top-30 precision at least `+10pp` above the same-pack sol-curve base dog rate.
4. The signal survives the trades-present subset and at least one progress/stage split.
5. The result is not driven by one day, one signal source, or one token duplicate cluster.

Failure:

- AUC in `[0.50, 0.56]`, or
- no stable top-k lift, or
- the positive result disappears after trades-present / stage / token sensitivity checks.

Gray zone:

- AUC in `(0.56, 0.60]`, or
- AUC `> 0.60` but CI lower bound `<= 0.55`, or
- top-k lift exists but is unstable across splits.

Gray zone means: do not enter shadow gate and do not abandon the signal. Freeze interpretation and collect one more independent OOS pack.

## 9. Decision Rules

If OOS succeeds:

- design a `sol_curve`-only pure shadow ranking gate;
- the shadow gate may rank by `unique_buyers` and report top-k capture;
- it must not trade, change live gate, change size, change exit, or alter final contract.

If OOS fails:

- do not tune matrix/RR/liquidity/gate around this result;
- reassess sourcing or lower/partition capture goals.

If OOS is gray:

- collect another OOS pack under this same preregistration or write a new preregistration before changing anything.

## 10. Explicitly Forbidden

Forbidden before the OOS result is read:

- changing the primary feature away from `unique_buyers`
- changing success thresholds
- adding a pooled AUC headline as the main endpoint
- using post-signal volume, future bars, or 120m outcomes as features
- changing live gate/matrix/RR/liquidity/exit/size
- treating shadow success as live-readiness

Forbidden after OOS unless a separate live-readiness process is completed:

- live trading based on this feature
- bonding-curve executable changes
- final-entry-contract changes
- position-size changes

## 11. Pre-Registration Lock

This preregistration is valid only after it is committed and pushed to the research branch.

The commit hash is the lock. Any OOS pack generated before that commit is not valid for this preregistered test.

After commit, record:

```text
prereg_commit: <fill after commit>
branch: runtime-stability-marker-guard
```

## 12. Minimal OOS Execution Checklist

1. Generate a new frozen data pack from a non-overlapping time window.
2. Run symmetric v10-native-return-guard style cleaning for dog and dud.
3. Build the OOS curve-feature export pack from clean rows.
4. Run the Dune export using the locked `[signal_ts - 900s, signal_ts]` window.
5. Build the feature table.
6. Before reading AUC, inspect coverage and usable-rate symmetry.
7. Read only the preregistered primary endpoint and required controls.
8. Produce an OOS report that states: success, failure, or gray zone.

Live strategy remains frozen throughout.
