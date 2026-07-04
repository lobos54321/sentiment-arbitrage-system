# capture-blocker-triage

When to use: any `BLOCKED_*` classification, any new `top_blocker`, before spending effort on a
blocker — first establish which of the four classes it belongs to, because the fix type is
completely different per class.

Inputs: `context_blocker_monitor`, `volume_kline_coverage`, `context_dimension_eligibility`,
`a_class_fastlane` (mode state), verdict `clean_window_failed_conditions`; grep of runtime gate
code for consumers of the blocked field.

## The four blocker classes

| Class | Definition | Test | Fix type |
|---|---|---|---|
| INSTRUMENTATION | the event happened but was never recorded | a shadow/mirror artifact shows the gap closes when logged | logging/bridge fix |
| POLICY | a mode/switch/governance state blocks progression | blocker fires with zero data-quality involvement (e.g. `mode_disabled`) | human decision + auditable procedure |
| EPISTEMIC | a coverage/quality metric below threshold blocks *trust* (dimension eligibility, clean windows, promotion), but no runtime gate consumes the field | grep runtime gates for the field → zero consumers; capture cohorts show no survival difference | data-collection/meter fix; does NOT raise capture |
| CAUSAL | a runtime gate consumed missing/bad data and killed dogs | reason strings name the gate; cohort split shows survival difference | data fix recovers those specific events |

## Procedure

1. Name the exact blocker string and where it is computed (`file:line`).
2. **Consumer test**: grep `entry_engine.py`, `paper_trade_monitor.py`, `gmgn_policy.py`,
   `final_entry_contract.py`, `scout_quality.py` for the underlying field. Zero hits in runtime
   gates ⇒ EPISTEMIC (evaluator-only).
3. **Cohort test**: split the raw g/s funnel by field-known vs unknown; no survival difference ⇒
   not CAUSAL.
4. **Recoverability test**: is there a matured/recheck artifact proving the data exists later?
   (read-only recompute, `canonical_backfill_performed=false` pattern).
5. State the downstream chain: what does clearing this blocker actually unlock (capture events?
   promotion evaluation? a human decision?). Quantify.

## Output contract

`blocker`, `class`, `computed_at` (file:line), `consumers` (list or "none-runtime"),
`cohort_split` (numbers), `recoverable_rate`, `unlocks` (explicit chain), `recommended_fix_type`.

## Acceptance

The class assignment survives the consumer + cohort tests recomputed by a second agent.

## Findings ledger

- **2026-07-02** (verified CONFIRMED): `volume_profile_coverage_below_80pct` is EPISTEMIC.
  `volume_profile` is a post-hoc annotation (`candidate_shadow_observer.py:921-954`) with zero
  runtime-gate consumers; volume-unknown is 100% downstream of kline bar counts (242 = 200
  `insufficient_kline_bars_lt_3` + 42 `kline_bars_unavailable`); matured recompute known-rate
  92.05% proves the data exists. Backfilling raises capture 0%; it only (jointly with kline≥80%)
  satisfies clean-window preconditions for human mode re-enable.
- **2026-07-02**: volume 32.6% root cause = stale-payload bug: one-shot fetch 60–120s post-signal,
  `not bars` refetch guard (`candidate_shadow_observer.py:1891-1896`), payload frozen once signal
  leaves the 10-newest window, and the observer reads its own thin
  `candidate_shadow_kline_cache.db` instead of the dense `kline_cache.db`.
- **2026-07-02**: kline 41.2% is a threshold artifact: first-bar lag ≤30s policy
  (`raw-signal-outcomes.js:42-53`) vs minute-aligned bars (lag ~U(0,60s)) → structural ~50% cap;
  29/30 "uncovered" rows have complete paths; research-grade coverage 98.04%.
- **2026-07-02**: `mode_disabled` is POLICY with a missing procedure: circuit broken 2026-06-11
  (ERBAI −20.9% > 20% cap); **no code path restores LIVE**; `clean_windows_required=4` counter
  unimplemented; `paper_entry_proposal_readiness` circular (needs intent>0 which the mode blocks).
  ~21 days in SHADOW. Fix type = P2 mode contract + human decision, not more data.
- **2026-07-02**: the genuinely CAUSAL kline channel this window: `no_kline_low_volume` (4
  pending→final kills incl. CIT +11,302%) + `not_ath_prebuy_kline_unknown_data_blocked` (2) ≈ 6
  recoverable events — which still hit `mode_disabled` at final.
- **2026-07-02** (P1 verified, run `codex_p1c_20260702T210515Z`, commit `858e242`):
  `volume_profile_coverage_below_80pct` and `kline_coverage_below_80pct` are cleared for formal
  discovery by canonical P1 coverage methods. `volume_context.known_rate=0.935484`
  (`strict_first_look_known_rate=0.711982`) and
  `raw_gold_silver_kline.kline_coverage_rate=0.943662`
  (`strict_kline_coverage_rate=0.492958`). Candidate mesh coverage is 84/84 with
  `full_candidate_coverage_rate=1.0`. Latest verdict moved to
  `A_CLASS_STUCK_REVIEW_REQUIRED`, `promotion_allowed=false`; current capture stage is
  `mode_disabled_stuck_requires_human_review`. Current 24h funnel:
  detector 71/71, decision 69/71, pass_allow 18/71, pending 11/71, final eligibility 2/71,
  paper 0/71. Largest actual gap is decision→pass_allow; next stage is P2
  A_CLASS/final_entry_contract human review, not strategy tuning or risk changes.
- **2026-07-02** (P2 deployed/verified, run `codex_p2_20260702T213240Z`, commit `41dcc08`):
  implemented the A_CLASS clean-window counter and human-operated re-enable contract. The audit now
  persists `clean_window_counter` into `a_class_mode_runtime_state.detail_json` without changing
  runtime mode or circuit state. First post-deploy clean window recorded `streak=1/4`,
  `sufficient=false`, `failed_condition_codes=[]`, so the verdict correctly moved to
  `A_CLASS_EXPECTED_SHADOW` with `current_capture_stage=mode_disabled_clean_window_pending`.
  `paper_trade_intent_zero` and `paper_trade_committed_zero` are no longer proposal blockers while
  A_CLASS is SHADOW; proposal readiness is blocked only by `clean_window_streak_below_required`.
  Current 24h funnel: denominator 74, detector 74/74, decision 70/74, pass_allow 18/74, pending
  11/74, mode-disabled-adjusted final eligibility 2/74, paper 0/74. `promotion_allowed=false`.
  Next action is to collect the remaining clean hourly windows; only after `streak>=4` can the
  read-only audit emit a human approval handoff. The operator script is dry-run by default and must
  not be executed by AutoLoop.
- **2026-07-03** (P2 acceptance verified, run `codex_p2_20260703T000051Z`, commit `146a7a3`):
  four consecutive clean hourly windows are now recorded in
  `a_class_mode_runtime_state.detail_json.clean_window_counter`: `streak=4/4`,
  `sufficient=true`, `failed_condition_codes=[]`. Latest verdict is
  `A_CLASS_STUCK_REVIEW_REQUIRED`; `current_capture_stage=mode_disabled_stuck_requires_human_review`;
  `paper_entry_proposal_readiness.status=PAPER_ENTRY_PROPOSAL_READY_REQUIRES_HUMAN_APPROVAL`;
  `promotion_allowed=false`; A_CLASS remains SHADOW/circuit-broken. Current 24h funnel:
  denominator 77, detector 77/77, decision 77/77, pass_allow 19/77, pending 12/77,
  mode-disabled-adjusted final eligibility 2/77, paper 0/77. P2 is complete at the engineering
  contract level: the next action is a human A_CLASS review/re-enable decision using the dry-run
  operator script. AutoLoop/Codex must not execute the re-enable switch.
- **2026-07-03** (P2.1 deployed/verified, run `20260703T051745Z`, commit `84d4847`):
  the A_CLASS recovery SLA is now breach-class parameterized in
  `/app/data/agent_runs/latest/a_class_fastlane_mode_audit_24h.json`.
  Post-deploy artifact fields verified:
  `schema_version=a_class_fastlane_mode_readiness_audit.v3`,
  `breach_class=MARKET`, `circuit_recovery_sla.effective_clean_windows_required=24`,
  `clean_window_counter.schema_version=a_class_clean_window_counter.v2`,
  `clean_window_counter.streak=1`, `clean_window_counter.required=24`,
  `clean_window_counter.required_source=circuit_recovery_sla`, and
  `circuit_recovery_sla.motion_trace_review.available=true` for breaching trade 61 ERBAI.
  `paper_auto_resume_readiness.allowed=false` because the MARKET SLA has only 1/24 clean hourly
  buckets; `live_reenable_contract.live_auto_reenable_allowed=false`; `promotion_allowed=false`.
  AutoLoop completed with `classification=A_CLASS_EXPECTED_SHADOW`, `tests_passed=true`, and
  `current_capture_stage=mode_disabled_clean_window_pending`. This supersedes the old 4-window
  placeholder for this MARKET breach; paper can auto-resume only after the class-appropriate SLA
  passes, while LIVE still requires the human operator script.
- **2026-07-04** (P2.1 PAPER_MARKET live-test repair verified, AutoLoop generated
  `2026-07-04T01:31:52Z`, commit `e0e9ef3`): trade `71` was a paper-only A_CLASS loss
  (`paper_only=true`, symbol `USDC`, token
  `8R5xg6XqmxoJ5htazTTktmQ9gxJ7CszYDthh4gr6pump`, `probe_quote_guard_stop`, realized
  `pnl_pct=-0.297536130936832`). The old mode row was idempotently rewritten through
  `scripts/a_class_runtime_safety.py` with `should_record_event=false`, adding
  `detail.breach_class=PAPER_MARKET`,
  `detail.paper_recovery_contract.paper_auto_recovery_counter_started=true`, and
  `detail.paper_recovery_contract.live_reenable_requires_human_operator=true`.
  The recap artifact `/app/data/agent_runs/latest/a_class_paper_breach_recap_trade_71.json`
  marks `spoofed_symbol_review.spoofed_symbol_risk=true`,
  `quote_collapse.entry_to_exit_price_return_pct=-0.297536`, and
  `quote_collapse.stop_overshoot_pp=0.097536`. The latest readiness artifact
  `/app/data/agent_runs/latest/a_class_fastlane_mode_audit_24h.json` verifies
  `breach_class=PAPER_MARKET`,
  `circuit_recovery_sla.effective_clean_windows_required=24`,
  `clean_window_counter.streak=1`, `clean_window_counter.required=24`,
  `circuit_recovery_sla.motion_trace_review.available=true`,
  `paper_auto_resume_readiness.status=NOT_ELIGIBLE`,
  `live_reenable_contract.live_auto_reenable_allowed=false`, and
  `promotion_allowed=false`. The full AutoLoop verdict completed with
  `autoloop_execution_status=FULL_RUN_COMPLETED`, `tests_passed=true`, and
  `classification=A_CLASS_EXPECTED_SHADOW`. This confirms paper-only losses are recorded and
  routed into the class-appropriate recovery SLA without changing strategy, gates,
  final_entry_contract, executor, canary, wallet, or risk; paper evidence collection remains
  blocked only until cooldown and the 24 clean hourly buckets pass, while LIVE re-enable stays
  human-operated.
- **2026-07-04** (P2.1 PAPER_MARKET parameter patch): PAPER_MARKET no longer inherits the
  real-money MARKET sentence. `scripts/a_class_runtime_safety.py` now defaults paper-only market
  breaches to 4h cooldown + 6 clean hourly windows and writes top-level
  `detail.paper_auto_recovery_counter_started=true`. `scripts/a_class_fastlane_mode_readiness_audit.py`
  applies this effective SLA even to old rows whose stored `cooldown_until_ts` was written under
  the prior 24h policy, and it owns the system writer that changes eligible paper-only recovery
  states to `status=PAPER_ELIGIBLE`, `action=PAPER_ONLY`, with audit row
  `operator=system_paper_auto_resume`. LIVE/MARKET recovery remains human-only:
  `LIVE_MARKET`/`MARKET` do not auto-resume paper and still require
  `scripts/a_class_mode_reenable_operator.py`. `scripts/final_entry_contract.py` only treats
  `PAPER_ELIGIBLE/PAPER_ONLY` as passable for candidates explicitly marked
  `paper_only_scout` or `execution_scope=paper_only`; live-scope candidates remain
  `mode_disabled`. Idempotent duplicate breach writes use `event_ts + class cooldown`, not
  `now + cooldown`, and preserve existing `clean_window_counter` / ready trackers, so replaying
  trade `71` neither extends the paper cooldown nor erases accumulated clean windows. Verified locally
  with `a_class_fastlane_mode_readiness_audit.py --self-test`,
  `a_class_paper_breach_recap.py --self-test`, `a_class_mode_reenable_operator.py --self-test`,
  and pytest on A_CLASS runtime/final-entry tests. `promotion_allowed=false` and no strategy,
  gate, executor, canary, wallet, or risk settings were changed.
- **2026-07-04** (P6 loop hygiene patch prepared): AutoLoop handoffs now include a machine-readable
  `codex_handoff.json` sidecar plus a `Machine Readable Handoff` JSON block in the Markdown with
  `tasks[]`, `task_outcomes[]`, immutable guardrails, commit/run identifiers, and verification
  artifact paths. `agent_capture_discovery_loop.py` and `agent_autoloop_stage_runner.py` sync both
  Markdown and JSON handoff aliases into `agent_runs/latest` and `agent_handoffs`. The dashboard
  runner status writer now records scheduler/cadence settings and `exec_run_provenance`
  (trigger, command, commit, paths, guardrails, exit status), and `/api/agent/latest-status`
  exposes the compact provenance summary. Local verification passed:
  `generate_codex_handoff.py --self-test`, `agent_autoloop_stage_runner.py --self-test`,
  `py_compile` for the touched Python scripts, and `node --check src/web/dashboard-server.js`.
  This is reporting/orchestration only; `promotion_allowed=false` and no strategy, gate, executor,
  canary, wallet, or risk settings were changed.
