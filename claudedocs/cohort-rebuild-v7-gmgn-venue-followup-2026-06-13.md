# Cohort Rebuild v7 — GMGN Venue Follow-up

Status: verified offline research artifact, 2026-06-13.

## Summary

The raw-dog label/return rebuild has reached full adjudication for the current frozen pack:

- input rows: 1724
- clean rows: 1724
- quarantine rows: 0
- clean dog unique: 463
- clean dud unique: 969
- active label-unit suspects: 0
- coverage incomplete: 0

This is a research/data-room result only. It does not justify changing live gate, matrix, RR, exit, or size.

## What Changed

The remaining v6 quarantine was not a hard data-source ceiling. A GMGN follow-up on the 23 remaining signal worklist returned:

- ok: 23/23
- bars available: 23/23
- nonzero volume: 23/23
- early 15m nonzero volume: 9/23

That showed the residual `venue_other` bucket was mostly a router limitation. The baseline unit router now allows `venue_other_needs_other_decoder` rows with a valid GMGN anchor price to route into `usd_gmgn` as `venue_other_gmgn_anchor`.

## Verified v7 Output

Data room:

`/Users/boliu/sas-data-room/chain-truth-recut-20260612T011545Z`

Main artifacts:

- `gmgn-full-window-v7-with-venue-followup/gmgn-full-window-touch.json`
- `baseline-unit-router-v5-gmgn-venue-followup/baseline-unit-routed.jsonl`
- `cohort-rebuild-v7-gmgn-venue-followup/rebuilt-rows.jsonl`
- `cohort-rebuild-v7-gmgn-venue-followup/rebuild-summary.json`
- `cohort-quarantine-followup-v6-after-venue/followup-summary.json`

Return domain distribution for clean dog rows:

- `spliced_curve_to_gmgn`: 314 rows / 216 unique signals
- `sol_curve`: 123 rows / 85 unique signals
- `usd_gmgn`: 183 rows / 162 unique signals

Clean rows by return domain:

- `spliced_curve_to_gmgn`: 1024
- `sol_curve`: 254
- `usd_gmgn`: 446

## Progression

| Rebuild | Clean rows | Quarantine rows | Clean dog unique | Clean dud unique | Clean active suspects |
|---|---:|---:|---:|---:|---:|
| v2 physical guard | 539 | 1185 | 219 | 224 | 0 |
| v3 GMGN peak follow-up | 1472 | 252 | 375 | 842 | 0 |
| v4 GMGN baseline follow-up | 1641 | 83 | 440 | 927 | 0 |
| v5 curve-baseline follow-up | 1677 | 47 | 449 | 953 | 0 |
| v6 bridge follow-up | 1695 | 29 | 456 | 953 | 0 |
| v7 venue follow-up | 1724 | 0 | 463 | 969 | 0 |

## Interpretation

The apprentice finding was directionally right: the prior 653 dog count was not trustworthy because `sol_curve` peak pollution remained. After physical guards, GMGN full-window peaks, baseline routing, bridge handling, and venue GMGN recovery, the current frozen cohort is fully adjudicated with zero active unit-suspect rows.

The v7 dog denominator is therefore the current best clean raw-dog discovery denominator for this frozen pack. It is still a raw-discovery/data-quality artifact, not a strategy edge result.

## Remaining Work

The next valid analysis is not a live strategy change. It is:

1. Generate or attach the decision-time pack so matched signal/decision ceiling and dog-vs-dud features can be recomputed on v7.
2. Re-run stage-stratified dog-vs-dud using v7 labels and strict no-leakage features.
3. Recompute matched signal-vs-decision capture ceiling from the same signal cohort.
4. Only after those are stable, discuss shadow gate or bonding-curve executable contract experiments.

Live gate, matrix, RR, exit, and live size remain frozen.
