# token-motion-trace-spec

When to use: designing/extending per-token lifecycle recording; any question of the form
"what did this token actually do between signal and outcome"; before adding another bespoke
per-question audit script (the trace makes those into queries).

Inputs today (inventory verified 2026-07-02): `raw_price_bars_1m` (279,509 bars / 4,477 tokens,
0–2h post-signal, multi-provider), `kline_cache.kline_1m` (~7d retention),
`lifecycle_tracks` (NOT_ATH-rejected cohort only), `raw_signal_outcomes` (11,250 signals, peak
marks 5m/15m/60m/120m + sustained, tiers), `raw_signal_observations` (first-bar lag, early-15m
completeness), `paper_decision_events` (production), `candidate_shadow_observations`
(single snapshot per signal×candidate).

## Design principle

**Event-sourced, append-only, one join spine.** Every subsystem writes events; every report is a
query. Never let a subsystem keep decisive state only in memory (that is how ATH stage and index
values were lost). Record-then-filter: perception writes at full fidelity; policy filters later
and its filtering is itself an event.

## Schema (v1 target)

`token_motion_events` (SQLite, production `paper_trades.db` or sibling):

```
mint TEXT, signal_id INTEGER, lifecycle_id TEXT,
ts_ms INTEGER,                      -- ONE clock: unix ms everywhere
domain TEXT,                        -- perceive | context | decide | stage | outcome | policy
event_type TEXT,                    -- from a REGISTERED enum, no free-form f-strings
payload_json TEXT,
PRIMARY KEY (mint, signal_id, ts_ms, domain, event_type)
```

Event families:

- `perceive`: signal arrival (source, signal_type, raw indices snapshot, **ath_stage**, MC,
  holders, top10), re-signal events.
- `context`: kline bar availability transitions, volume snapshot known/unknown flips, quote
  status (clean/executable/age), provider hydrate outcomes.
- `decide`: EVERY gate evaluation — component, decision, reason_code (registered enum),
  **price_at_decision, quote_age_at_decision** — including rejects (the missing half today).
- `stage`: pending_entry, final_entry_contract (with hard_blockers), paper intent, commit.
- `outcome`: peak marks at +5/15/60/120m, sustained peak, tier at maturation, post-reject-window
  outcome (for counterfactuals).
- `policy`: mode state transitions, clean-window state changes, budget/circuit events.

Materialized view `token_motion_trace(mint)` = ordered event list + derived path features
(time_to_peak, MC path once supply is stored, index deltas).

## Migration order (all read-only/additive)

1. **Stop dropping what is already in memory**: persist `signal.indices` (7 families ×
   current/signal) and per-signal `ath_stage` at `premium_signals` INSERT
   (`premium-signal-engine.js:521-533,1271`); store token supply/decimals once per mint (enables
   MC path from native-price bars).
2. Emit reject `decide` events with ts + price (this alone unlocks the reject-counterfactual
   skill's four requirements).
3. Normalize timestamps to ms at write time (`normalizeTimestampSec` pattern exists in
   raw-path-observer).
4. Canonical bar merge view over `raw_price_bars_1m` + `kline_cache.kline_1m` +
   `candidate_shadow_kline_cache.db` (provider precedence, dedupe) — do NOT create a fourth store.
5. Candidate re-observation: append-only history instead of UNIQUE(signal_id,candidate_id)
   single snapshot.
6. Backfill motion events from existing stores where derivable (decision events, outcomes, bars).

## Output contract

One SQL query per question: full funnel = GROUP BY over `stage` events; reject counterfactual =
`decide`(reject) JOIN `outcome`; coverage = `context` transitions. If a question needs a new
script instead of a query, the trace is missing an event family — extend the spec.

## Acceptance

For any mint in the last 24h: a single query returns signal→gates→stages→peaks→tier with
consistent ms timestamps, and reproduces the published funnel counts.

## Findings ledger

- **2026-07-02** (verified): fields silently dropped today — `signal.indices` never persisted
  (0/11,250 outcomes; index_lifecycle report 100% null), ATH stage in-memory only
  (`ath_counts.json` cumulative; all ATH1-4 Strategy-Memory hypotheses unvalidatable time-legally),
  no MC path (native price without supply/decimals), no reject timestamps (only
  `first_pending_ts`), candidate observations single-snapshot.
- **2026-07-02**: three overlapping 1m-bar stores with different PKs and retention; timestamps
  heterogeneous (premium_signals ms; outcomes/bars/lifecycle s; decision events REAL s).
- **2026-07-02**: lifecycle_tracks covers ONLY NOT_ATH-rejected signals (1,736 tracks; cadence
  gaps 237s–22,246s; 351 tracks <5 samples) — it is not a general motion trace.
- **2026-07-02**: price-path coverage skew: 2,412/4,477 tokens <30 bars; `kline_covered`
  4,528/11,250=40.25%; early_15m complete 45.3%; avg first-bar lag 384.7s. Historical (April)
  snapshot: median 21 bars, 100% <30 — the "29-candle thin window" era. Fixing collection forward
  beats backfilling history.
- **2026-07-03**: P5 motion trace v1 deployed read-only in commits `f57924d` and follow-up
  `8f563fa`. `premium_signals` now persists `indices_json`, `ath_stage`, `token_supply`, and
  `token_decimals`; `tokens` stores supply/decimals; append-only `token_motion_events` records
  `perceive/signal_received` and `decide/gate_evaluation`; `/api/agent/latest-status` exposes
  compact coverage. Remote smoke at `8f563fa`: latest post-deploy signal wrote `indices_json` and
  `ath_stage=NOT_ATH`, `token_motion_events=2`, and guardrails remained false. Supply/decimals are
  still forward-pending because current upstream signal rows do not provide those values.
- **2026-07-03**: secondary `sync-remote-premium-logs.js` writer path now preserves P5 carrier
  fields and creates `token_motion_events` for synced DBs. Remote tests passed:
  `sync-remote-premium-logs.test.mjs` 3/3, `dashboard-heavy-api-utils.test.mjs` 56/56, and
  `premium-signal-engine-dedup.test.mjs` 5/5 under `--test-force-exit`.
- **2026-07-04** (P5 token identity hydrate, commit `c09f382`): Forward premium-signal rows now
  backfill missing `token_supply` / `token_decimals` from the Solana mint account without blocking
  the entry decision path. `chain-snapshot-sol` exposes mint parsed `token_supply` and
  `token_decimals`; `premium-signal-engine` schedules a fire-and-forget identity hydrate after
  `saveSignalRecord`, updates only `premium_signals` / `tokens`, and appends
  `context/token_identity_hydrated` motion events. Remote verification on Zeabur passed
  `node --check` for the touched JS files and `node --test --test-force-exit
  /app/tests/premium-signal-engine-dedup.test.mjs` (6/6). AutoLoop
  `api_20260704T075406Z_844f034b` completed on deployed commit `c09f382` with `exit_code=0`,
  `tests_passed=true`, and guardrails false; `/api/agent/latest-status` then reported
  `token_supply_present=7`, `token_decimals_present=7`, and 7
  `context/token_identity_hydrated` events. This is read-only data-carrier enrichment only; no
  strategy, gate, final_entry_contract, executor, canary, wallet, or risk path was changed.
