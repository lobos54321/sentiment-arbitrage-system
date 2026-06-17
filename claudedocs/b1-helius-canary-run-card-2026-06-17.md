# B1 Helius Curve Observability Canary Run Card

Status: sealed run-card, pre-key.
Date: 2026-06-17.
Scope: read-only observability. This is not an edge test.

## Purpose

Run the bounded Helius canary exactly once to decide whether a live, non-Dune
provider can observe the ex-ante pump.fun curve window:

`[signal_ts - 900, signal_ts]`

The result may only be used to decide whether the live curve-observability line
continues. It must not be used to change strategy, gates, exits, sizing, or to
claim edge.

## Inputs

- Provider: `helius`
- Cohort: latest producer clean dogs plus clean duds, used only for
  `(token_ca, signal_ts)` selection.
- Labels: ignored. Output must not contain labels or outcomes.
- Token filter: `token_ca` ending with `pump`.
- Time filter: `signal_ts` in `[now - 14d, now - 2h]`.
- Selection order: ascending `(token_ca, signal_ts)`.
- Target sample: 30 windows.
- Minimum sample for any PASS: 20 windows.
- Maximum sample: 50 windows.

## Hard Window Contract

- `pre_sec = 900`
- `post_sec = 0`
- `window_start_ts = signal_ts - 900`
- `window_end_ts = signal_ts`
- `block_time == signal_ts` is allowed.
- Any `block_time > signal_ts` is a leakage failure.

## Sealed Provider Verdict Thresholds

These thresholds are not runtime-tunable for B1:

- `complete_rate >= 0.90`
- `usable_curve_window_rate >= 0.60`
- `selected_n >= 20`
- leakage rows = 0

A usable curve window is:

- complete;
- has at least one in-window curve trade;
- has wallet data;
- has volume data.

`PASS` is allowed only if all sealed thresholds pass.

`PARTIAL` means the provider is not acceptable as the sole live curve provider.

`FAIL` means the live Helius curve-observability line is retired unless a new
run-card is explicitly written and locked.

## Required Command Shape

Do not pass any runtime override for the usable-window threshold. The script no
longer accepts one.

Example shape:

```bash
node scripts/run-curve-observability-canary.js \
  --provider helius \
  --dogs <latest-oos-dogs.json> \
  --duds <latest-oos-duds.json> \
  --out-dir <durable-canary-dir> \
  --helius-api-key-file <local-secret-file> \
  --helius-rpc-url-file <local-secret-file> \
  --limit 30
```

Secrets must remain in local files. Do not print them.

## Forbidden

- No AUC.
- No lift.
- No precision/recall.
- No dog/dud output.
- No return or tier output.
- No feature-value output.
- No live strategy change.
- No gate/matrix/RR/liquidity/exit/size change.
- No production write path.

## Decision Branches

If `PASS`:

- This only proves live curve observability is technically viable.
- It does not prove edge.
- At most one new preregistered feature line may be proposed, and only after a
  separate cost/leakage review.

If `PARTIAL` or `FAIL`:

- Stop the bonding-curve live-observability engineering line.
- Do not reroll parameters.
- Move to sourcing/target review.

## Current Evidence Context

The unique-buyers OOS line already nulled at the n=50 futility look point using
real ex-ante curve trades. Therefore B1 is closure on live provider provenance,
not a positive edge hunt.
