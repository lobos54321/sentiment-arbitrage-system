# B1 Helius Canary Closure

Date: 2026-06-17.
Status: closure record. No production behavior change.

## Verdict

`PARTIAL`, therefore not accepted as a live curve-observability path.

The B1 run-card required:

- selected windows >= 20;
- complete_rate >= 0.90;
- usable_curve_window_rate >= 0.60;
- leakage rows = 0.

The Helius canary met completeness and leakage, but failed usability.

## Helius Run

Artifact:

`/Users/boliu/sas-data-room/b1-helius-canary-20260617T080012Z`

Verified summary:

- selected = 30
- complete = 28/30 = 0.933333
- usable curve windows = 0/30 = 0
- leakage rows = 0
- total cost units = 4727
- median latency = 556 ms

Trade-count distribution:

- 28 windows had 0 in-window curve trades;
- 1 window had 259 trades;
- 1 window had 432 trades.

The two trade-heavy Helius windows did not reach the window start, so they could
not count as usable.

## Dune Full-Window Comparison

Artifact:

`/Users/boliu/sas-data-room/b1-dune-comparison-canary-20260617T080153Z`

Same 30 windows.

Verified summary:

- selected = 30
- complete = 30/30 = 1
- usable curve windows = 2/30 = 0.066667
- leakage rows = 0

Trade-count distribution:

- 28 windows had 0 in-window curve trades;
- 1 window had 519 trades;
- 1 window had 785 trades.

The two Dune usable windows are the same two windows where Helius found trades.
The difference is that Dune had full-window coverage while Helius hit its page
budget.

## Interpretation

This is not primarily a provider failure.

The same selected windows are mostly empty under Dune, the already-verified
offline ex-ante path. Therefore increasing Helius pages or swapping providers
cannot change the core fact that 28/30 windows had no curve activity to observe.

The current premium-signals/gecko track reaches most tokens when the pump.fun
curve window is empty. That explains why curve-stage features such as
`unique_buyers` did not survive OOS: in this source, most decision-time windows
do not contain usable curve activity.

## Decision

Retire the bonding-curve live-observability engineering line for the current
source.

Do not:

- rerun B1 with looser thresholds;
- raise page budgets to chase the two hot windows;
- connect Helius into daily OOS;
- modify gate, matrix/RR, liquidity, exit, or live size.

Future work must be framed as sourcing or target review, not provider tuning.
