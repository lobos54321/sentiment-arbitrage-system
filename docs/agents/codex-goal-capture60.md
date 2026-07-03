# CODEX GOAL — Capture-60 Measurement & Governance Repair

## Mission

Without touching any strategy / entry policy / hard gates / exit gates / executor / canary size /
wallet / risk settings, repair the capture pipeline of sentiment-arbitrage-system until the
evidence chain is clean and the system can legally enter paper trading: execute the task queue
**P0 → P6** in `claudedocs/capture-60-deep-audit-2026-07-02.md` §8, strictly in order, one task
at a time, and a task only counts as DONE after its acceptance criteria are verified against a
fresh production AutoLoop run.

## Read first (in this order)

1. `AGENTS.md` — entry contract, guardrails, known traps
2. `skills/README.md` + the skill matching your current task
3. `claudedocs/capture-60-deep-audit-2026-07-02.md` — verified facts + §8 task queue

Do NOT re-derive the audit. The funnel numbers, root causes (`not bars` refetch guard, split
kline caches, ≤30s lag policy, mode deadlock, decision-bridge logging gap) are verified with
file:line citations in the report.

## Success criteria (summary; full acceptance in report §8)

- **P0** decision bridge: next 24h window `decision_capture ≥ 0.90` with unchanged strategy;
  `capture_60_biggest_gap_stage` moves off `decision_capture`.
- **P1** kline/volume collection: both `volume_profile_coverage ≥ 0.8` and `kline_coverage ≥ 0.8`
  leave `clean_window_failed_conditions`; artifacts carry an explicit method-change field
  (no silent metric redefinition).
- **P2** mode re-enable contract: real 4-consecutive-clean-windows counter persisted; idempotent
  re-enable script with audit row (operator + reason); `paper_entry_proposal_readiness`
  de-circularized. **The LIVE switch itself is pressed by the human, never by you.**
- **P3** reject counterfactual instrumentation: reject events carry `reject_ts`,
  `price_at_reject`, `quote_age_at_reject` when available from the reject row or same-signal
  context; remaining price/quote gaps are explicit and deterministically attributed, never inferred
  from unrelated token-time rows; dud-inclusive per-reason denominators; shadow quote-fresh
  re-anchor variant for `entry_execution_signal_stale` (watch-only).
- **P4** OOS statistics: batch-pinned eval clocks, event-set family dedupe, self-cross exclusion,
  unique-token N≥10, BH-FDR q=0.1, two-window repeat rule, negative-control panel
  (reuse `holdout_negative_controls`). Artifact publishes per-family q-values + null repeat rate.
- **P5** motion trace v1 + persist dropped fields (`signal.indices`, per-signal `ath_stage`,
  supply/decimals) per `skills/token-motion-trace-spec.md`.
- **P6** loop hygiene: scheduler/cadence, exec-run provenance in runner_status, handoff `tasks[]`
  with acceptance criteria + `task_outcomes[]` linking commits and post-deploy verification runs.

## Operating rules

1. **One task, one verification** (doctrine: `docs/problem-solving-operating-principles.md`).
   Never batch P-items into one diff.
2. **Branch from origin/main latest.** The local checkout has previously lagged origin/main by
   127 commits with dirty files. Verify deployed behavior against the verdict's commit
   fingerprints, not against stale local files.
3. Verification loop per task: implement → deploy → trigger AutoLoop
   (`POST /api/agent/capture-discovery/run` with token, or the stage runner) → wait for the next
   window → compare acceptance criteria on fresh artifacts → record the outcome.
4. **Sediment as you go**: append each verified before/after result (commit hash, metric before →
   after, artifact refs) to the matching `skills/*.md` Findings ledger. Ledger is append-only.
5. Window discipline: pin `since_ts`/`generated_at` for every number; the raw g/s denominator
   grows by backfill — never compare across unpinned pulls.
6. Reason strings: when touching reject/decision events, use registered enum codes, not new
   free-form f-strings.

## Hard guardrails (violating any of these = stop immediately)

- No changes to strategy, entry policy, hard gates, exit gates, live executor, canary size,
  wallet config, risk settings, A_CLASS scoring.
- `promotion_allowed` stays false; same-window discovery evidence never promotes.
- Threshold changes to live gates (e.g. relaxing 300s staleness) are STRATEGY changes — the
  allowed path is registering shadow/watch-only variants.
- Mode re-enable: you implement the contract and the script; a human runs it.

## Escalate to the human (stop and ask) when

- A task fails its acceptance criteria twice.
- A required edit sits inside guardrailed files with unclear boundaries.
- P2 is implemented and verified — hand the switch to the human with the audit-row procedure.
- Production behavior contradicts the audit's verified facts (then update the skill ledger with
  the contradiction before proceeding).

## Definition of overall done

P0–P6 all verified, and a full AutoLoop window shows: decision capture ≥0.90, both coverages
≥0.80, `current_capture_stage` legally able to reach `paper_proposal_ready_requires_human_approval`
without hand-editing SQLite, OOS artifact carrying q-values + null panel, and the handoff
containing machine-readable `tasks[]` / `task_outcomes[]`.
