# GMGN Read-Only Integration

GMGN is integrated as an optional read-only enrichment source for LOTTO entry
gate audit payloads. It does not submit swaps, orders, or strategy orders.

## Runtime Flags

- `GMGN_READONLY_ENABLED=true` enables token enrichment.
- `GMGN_API_KEY=<key>` is required for live read-only use.
- `GMGN_ALLOW_DEMO_KEY=true` allows the public GMGN demo key for local testing only.
- `GMGN_READONLY_CACHE_SEC=60` controls in-process token enrichment cache TTL.
- `GMGN_READONLY_TIMEOUT_SEC=6` controls per-command timeout.

## Current Wiring

`scripts/paper_trade_monitor.py` calls `fetch_gmgn_token_enrichment()` inside the
LOTTO gate and records:

- `gmgn_readonly`
- `gmgn_risk_flags`

The raw enrichment fields remain observational. Paper LOTTO decisions are made
through the explicit policy object below.

`scripts/gmgn_policy.py` adds a paper-only decision layer on top of this
enrichment. It can reject, downsize, or boost LOTTO paper entries, and records:

- `gmgn_policy`
- `gmgn_action`
- `gmgn_reason`

GMGN policy is scoped to paper LOTTO paths. It does not call GMGN swap/order
APIs and does not affect non-LOTTO entry edge budgets.

## Normalized Fields

The adapter normalizes GMGN token info into stable fields including:

- `smart_degen_count`
- `renowned_count`
- `sniper_count`
- `bundler_rate`
- `rat_trader_amount_rate`
- `entrapment_ratio`
- `bot_degen_rate`
- `fresh_wallet_rate`
- `top10_holder_rate`
- `creator_token_status`
- `creator_close`
- `dev_team_hold_rate`
- `creator_hold_rate`

## Next Step

Use the analysis script below to tune policy thresholds from paper outcomes.
Any change that raises size or increases entry frequency should get explicit
review first.

## Analysis And Candidate Tools

- `scripts/analyze_gmgn_lotto_edge.py` groups closed LOTTO paper outcomes by
  GMGN buckets such as `bundler_rate`, `smart_degen_count`, `renowned_count`,
  `rat_trader_amount_rate`, and `gmgn_policy.action`.
- `scripts/gmgn_candidate_scout.py` collects GMGN trending, signal, and trenches
  candidates into `data/gmgn_candidates.jsonl` for review. It does not register
  candidates into the watchlist.
