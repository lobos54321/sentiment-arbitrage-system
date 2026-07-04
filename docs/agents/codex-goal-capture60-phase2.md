# CODEX GOAL Phase 2 — From "can shoot" to "can measure and win"

Status inherited from Phase 1 (`docs/agents/codex-goal-capture60.md`): **P0 / P1 / P2 are DONE and
independently verified** (deployment_commit 6730630: decision capture 77/77=100%, coverage
method-versioned and cleared, clean-window counter 4/4, operator script audited). The human LIVE
switch is the operator's action, not yours.

## Pinned business objectives (user-decided 2026-07-03 — these drive P7's objective function)

- **Return metric**: rolling 24h realized net ROI on allocated strategy risk capital, target 200%.
- **Hard risk constraint**: max drawdown ≤ 15% of allocated risk capital.
  Implied sizing bound: per-trade capital risk ≤ ~2% (position ≤ ~10% at the -20% per-trade stop),
  so a 7-loss streak ≈ -14% stays inside budget.
- **Second perception source route**: pump.fun / on-chain realtime stream (primary) → GMGN
  (market confirmation) → X narrative (later) → smart money (precision layer, later).

## Task queue (STRICT order, one task at a time, verify each against a fresh AutoLoop window)

### P2.1 — Circuit-recovery SLA parameterization (small, do first)

Upgrade the P2 recovery contract from placeholder params to the tiered policy:

1. **Breach classes**: classify each circuit-breaker trip as `DATA_INFRA` (quote corruption,
   no-route trap, provider outage — evidence from context/quote artifacts) vs `MARKET`
   (real loss within normal data quality).
2. **Per-class clean-window requirements**: DATA_INFRA → 6 consecutive hourly buckets;
   MARKET → 24 consecutive hourly buckets **plus** a motion-trace review artifact for the
   breaching trade (auto-generated, human-readable).
3. Cooldown stays 24h.
4. **Paper/LIVE split**: after class-appropriate clean windows pass, **paper trading auto-resumes
   without human action** (zero real-capital risk; never starve evidence collection again).
   LIVE canary re-enable remains exclusively via the human-run operator script.
5. **Fail-closed + escalation SLA**: LIVE never auto-enables. If
   `PAPER_ENTRY_PROPOSAL_READY_REQUIRES_HUMAN_APPROVAL` persists >48h, handoff escalates to
   high-priority with a daily reminder field.

Acceptance: readiness artifact exposes `breach_class`, per-class requirements and streak;
self-test simulates both breach classes end-to-end incl. paper auto-resume; guardrail files
untouched (mode/evaluator layer only).

### P3 — Reject counterfactual instrumentation (unchanged from Phase 1; prerequisite for P7)

`reject_ts`, `price_at_reject`, `quote_age_at_reject` on every reject event; dud-inclusive
per-reason denominators; shadow quote-fresh re-anchor variant for `entry_execution_signal_stale`
(watch-only). Acceptance: next window computes P(gold/silver | rejected by reason) vs base rate
and peak-vs-reject ordering for 100% of QT kills.

### P4 — OOS statistics upgrade (unchanged; do before frozen definitions start "repeating" by luck)

Batch-pinned eval clocks, event-set family dedupe, self-cross exclusion, unique-token N≥10,
BH-FDR q=0.1, two-window repeat rule, negative-control panel reusing `holdout_negative_controls`.
Acceptance: validation artifact publishes per-family q-values + null-panel repeat rate.

### P7 — Exit-policy shadow lab (NEW; depends on P3; shadow-only, never touches live exits)

Replay engine over `raw_price_bars_1m` (~280k 1m bars, 0–2h post-signal) joined with P3
decision-time prices:

- Policy families to race: (a) fixed-multiple take-profit ladder, (b) trailing-drawdown stop,
  (c) tiered partial exits + tail-rider remainder. Parameter grids per family.
- **Objective function (pinned above)**: maximize rolling-24h realized net ROI on allocated risk
  capital subject to maxDD ≤ 15%, per-trade capital risk ≤ 2%, per-trade stop -20% unchanged.
- Must be time-legal (entry/exit decisions use only information available at decision time),
  execution-delay adjusted (reuse the delay-adjusted replay machinery), and slippage-sensitized
  (report results at 0/1/3/5% slippage assumptions).
- Output: per-policy distribution of 24h realized ROI, maxDD, win rate, tail-capture share
  (fraction of gold peak realized); ranked verdict under the pinned objective; artifact +
  download key + skills ledger entry.

Acceptance: an adversarial recheck reproduces rankings from the artifact; a future-data leakage
probe (shift-test: policy evaluated with +1 bar lookahead must strictly dominate the honest run)
passes; no production file touched.

### P5 — Motion trace v1 (unchanged)

Persist `signal.indices` + per-signal `ath_stage` + supply/decimals; ms timestamps;
`token_motion_events` per `skills/token-motion-trace-spec.md`; `/api/agent/latest-status`
compact endpoint.

### P6 — Loop hygiene (unchanged)

Scheduler/cadence, exec-run provenance, handoff `tasks[]` + `task_outcomes[]`.

### P8 — Second perception source: pump.fun realtime stream (NEW; shadow-only)

- Ingest pump.fun / on-chain realtime launches as a second signal source, written into the shadow
  path with distinct `signal_source` tags. **Zero impact on the production decision path.**
- Run 30 days side-by-side vs the Telegram source using the existing source/source_component EV
  machinery: per-source raw dog counts, quote-clean g/s, capture, overlap analysis (dogs seen by
  pump.fun but not TG = incremental recall).
- GMGN confirmation layer is a LATER task; X narrative and smart-money layers are NOT in scope —
  do not implement them.

Acceptance: comparison artifact with per-source denominators + overlap matrix; production funnel
metrics unchanged during the trial.

## Operating rules & guardrails

Identical to Phase 1 (`docs/agents/codex-goal-capture60.md`): one task one verification; branch
from origin/main latest; pin windows; sediment verified before/after results into `skills/`
ledgers; no strategy/gates/executor/canary/risk changes; `promotion_allowed` stays false;
threshold changes to live gates only ever as shadow/watch variants; escalate after two failed
acceptance runs. Additional escalation: when P7 produces a ranked winner, STOP and hand the
result to the human — adopting an exit policy is an S2 governance decision, not a code change.
