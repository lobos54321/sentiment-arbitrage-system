# V10 Decision-Anchor Pack Result

Status: research-only, generated 2026-06-13.

Data pack:

`/Users/boliu/sas-data-room/chain-truth-recut-20260612T011545Z/v10-decision-anchor-pack`

Source archive:

`/Users/boliu/Downloads/rawdog-audit-dbs.tgz`

Source archive SHA256:

`8629b4b1be8904cb2b8824fdeba7bac5eec09abd0cba1ae7e441fd4263d2700f`

## Coverage

The paper decision subset now contains:

- `a_class_decision_events`: 57,436 rows
- `opportunity_events`: 8,336 rows
- `canonical_trade_ledger`: 58 rows

The subset is still marked `review_required` because the decision tables start
well after the v10 raw cohort starts:

- v10 cohort start: 2026-06-06 09:58:07 UTC
- first decision event in subset: 2026-06-09 05:48:13 UTC

Therefore this is a valid decision-chain pack for the covered slice, but not a
complete 2026-06-06 to 2026-06-11 v10 pack.

## Funnel

All v10 clean cohort rows:

| Cohort | Rows | Has decision | Quote clean | Would enter | Entered |
| --- | ---: | ---: | ---: | ---: | ---: |
| Dogs | 461 | 195 (42.3%) | 190 | 43 | 0 |
| Duds | 971 | 453 (46.7%) | 443 | 120 | 6 |

Coverage-window rows only (`signal_ts >= first decision event - 60s`):

| Cohort | Rows | Has decision | Quote clean / has decision | Would enter / quote clean | Entered |
| --- | ---: | ---: | ---: | ---: | ---: |
| Dogs | 208 | 193 (92.8%) | 188 / 193 (97.4%) | 41 / 188 (21.8%) | 0 |
| Duds | 589 | 453 (76.9%) | 443 / 453 (97.8%) | 120 / 443 (27.1%) | 6 |

Interpretation:

- Once the paper decision chain exists, most covered dog rows do reach a
  decision record.
- Quote/route availability is not the main bottleneck in this covered slice.
- The main bucket is `quote_clean_no_would_enter`.
- The current `would_enter` decision does not discriminate dogs from duds. In
  this slice, duds pass `would_enter` slightly more often than dogs.

## Main Bucket Breakdown

For matched dogs in `quote_clean_no_would_enter`:

- Total: 147
- `hard_prefilter_failed`: 134
- `opportunity_matrix_red_cell`: 12
- missing reason: 1

Hard blockers inside that dog bucket:

- `liquidity_unknown`: 130
- `spread_too_high`: 28
- `spread_extreme`: 21
- `quote_age_unknown`: 2
- `liquidity_below_min`: 1

All 130 dog rows with `liquidity_unknown` in this bucket had:

- `provider_hydrate_outcome = success`
- `quote_clean = true`
- `quote_executable = true`
- `route_available = true`

So this is not simply "provider failed to hydrate quote". The quote route was
present, but the hard prefilter still lacked a liquidity/spread evidence model
that can certify this token stage.

For matched duds in `quote_clean_no_would_enter`:

- Total: 323
- `hard_prefilter_failed`: 283
- `opportunity_matrix_red_cell`: 39
- duplicate window: 1

Hard blockers inside that dud bucket:

- `liquidity_unknown`: 270
- `spread_too_high`: 51
- `spread_extreme`: 38
- `quote_age_unknown`: 13

For duds, 262/270 `liquidity_unknown` rows in the same bucket also had
`provider_hydrate_outcome = success`.

Interpretation:

- The phrase `quote_clean_no_would_enter` is still too broad.
- Most dogs in this bucket are not cleanly rejected by the matrix; they are
  stopped earlier by AMM-centric hard prefilter evidence, mostly
  `liquidity_unknown`.
- Pure matrix/RR rejection exists, but is a smaller sub-bucket:
  12 dogs and 40 duds with no hard blockers.

## Current Bottleneck

For the covered slice, the bottleneck is:

`raw dog -> decision record -> quote route clean -> hard prefilter evidence`

The specific unresolved issue is not "relax the gate". It is:

`liquidity_unknown / spread_* still mean AMM-style evidence, while many v10 dogs are pump.fun / GMGN / bonding-curve-stage tokens.`

More precisely: the system has a route/quote success signal, but it does not yet
have a stage-aware executable evidence contract that can turn GMGN/bonding-curve
state into `liquidity_known` and `spread_ok` equivalents.

This reinforces the existing direction:

1. Do not loosen gate/matrix/RR.
2. Define a bonding-curve/GMGN executable evidence contract.
3. Separate `quote_clean` from `final_executable`.
4. Re-run the decision funnel after provider/hydrate outcomes and bonding-stage
   liquidity evidence are exported in the same row.

## Guardrails

Do not use this pack to change live gate, exit, or size.

It is sufficient to decide the next research question:

> For rows with quote route clean but `liquidity_unknown`/`spread_*` hard
> blockers, can bonding-curve/GMGN evidence prove executable status at decision
> time, or are these true market blocks?
