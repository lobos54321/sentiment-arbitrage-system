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

These fields are observational. They do not currently allow or reject entries.

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

After collecting samples, promote only high-confidence fields into gates or edge
budget. Any change that blocks entries, raises size, or increases entry
frequency should get explicit review first.
