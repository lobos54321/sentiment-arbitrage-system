# Agent entry contract — sentiment-arbitrage-system

Solana meme-token capture system (Telegram premium-signal perception → decision/filter funnel →
paper/tiny-canary execution) with a read-only capture-discovery AutoLoop. Current program goal:
**capture ≥60% of raw gold/silver dogs** (gold = sustained peak ≥+100% within 2h of signal,
silver ≥+50%; `src/analytics/raw-signal-outcomes.js`).

## Guardrails (hard, from `docs/agents/gold-silver-capture-discovery-loop.md`)

- Do NOT change strategy, entry policy, hard gates, exit gates, live executor, canary size,
  wallet config, or risk settings.
- Only fix data, report, evaluator, API, or test issues needed to make discovery auditable.
- Same-window discovery output cannot promote live trading; `promotion_allowed` stays false
  until an out-of-sample gate passes; mode re-enable is a human decision.

## Read this first

1. `docs/problem-solving-operating-principles.md` — the operating doctrine (reality first,
   measurement before strategy, one main contradiction, one action one verification).
2. `skills/README.md` — machine-consumable audit procedures + validated lessons. If your task
   matches a skill trigger, follow the skill; append verified lessons to its Findings ledger.
3. `claudedocs/capture-60-deep-audit-2026-07-02.md` — latest full audit: verified funnel,
   blocker taxonomy (POLICY / EPISTEMIC / CAUSAL / INSTRUMENTATION), prioritized Codex task
   queue P0–P6.

## System state entry points (production)

- Status: `https://sentiment-arbitrage.zeabur.app/api/agent/capture-discovery/latest?token=$DASHBOARD_TOKEN`
- Artifacts: `/api/data/download/agent-capture-discovery?artifact=<key>&token=...`
  (keys incl. `verdict`, `summary`, `handoff`, `registry`, `raw_funnel`, `capture_stage_metrics`,
  `capture_60_gap_report`, `pending_to_final_entry_audit`, `volume_kline_coverage`,
  `capture_cross_oos_freeze_registry`, `capture_cross_post_freeze_oos_validation`, ...; the
  server's `supported_artifacts` list is authoritative)
- Raw dog rows: `/api/paper/raw-dog-discovery?token=...&include_rows=1&rows_limit=50000`
- Container: `/app/data/agent_runs/latest/`, `/app/data/agent_handoffs/`, `/app/data/hypothesis_registry.json`

## Traps that have burned agents before

- **The local checkout routinely lags origin/main** (observed 127 commits behind while production
  ran a third commit). Before citing code for deployed behavior, check the verdict's commit
  fingerprints and diff against GitHub main.
- Artifacts regenerate at different times and the raw g/s denominator grows by backfill — **pin
  `since_ts`/`generated_at` for every number you quote**; never mix windows.
- Funnel numbers: distinguish signal events from unique tokens; stages are not strictly nested.
- `/usr/bin/python3` is broken on the local Mac; use `~/.local/bin/python3.11`. Local `/app` is
  read-only; Strategy-Memory local runs need `--out` or `STRATEGY_MEMORY_DATA_DIR`.
- AutoLoop has no scheduler: artifacts can be hours stale. Trigger via
  `POST /api/agent/capture-discovery/run` or the stage runner; `runner_status` only records
  API-triggered runs.
