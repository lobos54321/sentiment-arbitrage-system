# Free-Source Observation Policy

Status: research baseline.

## Decision

Helius is a backup source, not the default path for raw dog observation.

The raw observation pipeline should prefer:

1. GMGN/AMM sources when they provide usable volume and price.
2. Gecko/DexScreener only as price fallback, not as a trusted volume source.
3. Chain truth decoding only for rows where free sources cannot adjudicate the label or ex-ante features.

## Why

The expert panel found that the previous dog cohort often had Gecko price-only paths with zero usable volume. GMGN touch proved that GMGN can return historical volume for dog and dud tokens, but legal pre-anchor volume remains sparse around pre-graduation tokens. Chain truth is reserved for the dark/pre-grad rows and quarantined label rows.

## Required Guardrails

- Free-source rows that cannot prove usable volume must be marked `chain_truth_required`.
- Helius/Alchemy shallow runs are path validation only unless `history_reached_start=true`.
- `coverage_incomplete` rows cannot emit final negative verdicts.
- Research outputs must live in a durable data room, not `/tmp`.

## Primary Tools

- `scripts/run-free-source-coverage-audit.js`
- `scripts/build-chain-truth-worklist.js`
- `scripts/build-chain-truth-tier-worklists.js`

