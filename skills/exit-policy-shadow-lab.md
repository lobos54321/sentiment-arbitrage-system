# Exit Policy Shadow Lab

## Purpose

P7 evaluates exit-policy hypotheses in a read-only shadow lab. It exists to find whether a simulated exit family can improve realized capture and rolling 24h ROI without changing production trading behavior.

This stage is not a production strategy change. It must never edit live exits, entry policy, gates, A_CLASS mode, executor, wallet, canary sizing, or risk settings.

## Non-Negotiables

- `promotion_allowed` must remain `false`.
- `allowed_use` must remain `shadow_only`.
- No production exit policy may be changed by this stage.
- Any champion result must stop the queue and require human review.
- The lab must be time-legal, execution-delay adjusted, and slippage-grid tested.
- The lab must include an adversarial ranking recheck.
- The future-data leakage probe must show that a +1 bar oracle strictly dominates the honest replay.
- If a champion is found, the next stage is human review, not P5/P6/P8.

## Required Policy Families

- Fixed-multiple take-profit ladder.
- Trailing-drawdown stop.
- Tiered partial exits with tail-rider remainder.

## Required Constraints

- Max drawdown target: `<= 15%`.
- Per-trade capital risk target: `<= 2%`.
- Stop-loss remains `-20%`.
- Slippage grid must include `0%`, `1%`, `3%`, and `5%`.
- Entry delay grid must include `0`, `5`, `10`, `20`, `30`, and `60` seconds.

## Findings Ledger

### 2026-07-03 P7 Remote Validation

- code_commit: `a3b23d8738279a39bb90b6b5e54262db3035cc19`
- remote_run_id: `api_20260703T074855Z_88e2ae47`
- remote_run_finished_at: `2026-07-03T08:06:25.552Z`
- remote_exit_code: `0`
- remote_timed_out: `false`
- tests_passed: `true`
- artifact: `/app/data/agent_runs/latest/exit_policy_shadow_lab_24h.json`
- artifact_schema: `exit_policy_shadow_lab.v1`
- classification: `EXIT_POLICY_SHADOW_LAB_READY`
- allowed_use: `shadow_only`
- promotion_allowed: `false`
- human_handoff_required: `true`
- live_exit_policy_changed: `false`
- production_files_touched: `[]`
- policy_families_tested:
  - `fixed_multiple_take_profit_ladder`
  - `tiered_partial_exits_tail_rider`
  - `trailing_drawdown_stop`
- policy_variants_tested: `21`
- policy_slippage_delay_cells: `504`
- sample_count: `472`
- champion_verdict: `CHAMPION_PENDING_HUMAN_REVIEW`
- champion_policy_id: `trail_a50_dd15_stop20`
- champion_family: `trailing_drawdown_stop`
- champion_objective_roi_pct: `974745.670075`
- champion_max_drawdown_pct: `14.606651`
- champion_win_rate: `0.620915`
- adversarial_ranking_recheck_ok: `true`
- future_data_leakage_probe_passes: `true`
- final_reviewer_classification: `A_CLASS_EXPECTED_SHADOW`
- final_reviewer_top_blocker: `discovery_same_window_not_promotion_evidence`
- final_reviewer_next_action: `wait_clean_windows_or_fix_failed_context_coverage`

Result:

P7 produced a shadow-only champion. The strict queue must stop here for human review. No production exit policy, strategy, gate, A_CLASS mode, executor, canary, wallet, or risk setting may be changed from this result alone.
