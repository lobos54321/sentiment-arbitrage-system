# Chain Truth Tier 1 / Alchemy Notes

Status: research baseline. The original `/tmp` run artifacts were not durable and must be regenerated from the scripts in this branch.

## Important Correction

The prior `15/96 has curve trades` number came from a shallow path-validation run. It must not be cited as market evidence.

Reason:

- shallow run used a small signature page budget;
- many rows did not reach the requested window start;
- `history_reached_start=false` means no-trade rows are coverage-incomplete, not true negatives.

Correct statement:

`96/96 path validation succeeded, but content coverage was incomplete.`

## Tier Worklists

Use:

- `scripts/build-chain-truth-worklist.js`
- `scripts/build-chain-truth-tier-worklists.js`

Required worklist definition:

`targeted_chain_truth_tokens UNION quarantine_tokens`

This avoids missing quarantine-only tokens.

## Tier 1 Windows

- Anchor window: `anchor +/- 90s`
- Polluted peak window: `claimed peak timestamp +/- 3m`

Rows with `history_reached_start=false` must be marked coverage-incomplete and cannot emit final no-trade verdicts.

## Next Required Upgrade

Transfer-heuristic prices can create physically impossible outliers. Final label adjudication and curve-entry ceiling require exact pump.fun `TradeEvent` decoding from raw transaction logs.

Until then:

- do not use heuristic max price as final proof;
- apply a physical feasibility filter to heuristic prices;
- keep gate / matrix / RR / exit / live size unchanged.

