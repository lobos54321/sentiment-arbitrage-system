# Phase 3 Real Paper Validation

When to use: Phase 3 begins, P7 OOS passes, a wide-net paper experiment is proposed, or a user asks
what comes after signal mining.

Inputs:

- `/app/data/agent_runs/latest/p7_exit_policy_oos_validation.json`
- `/app/data/agent_runs/latest/reviewer_verdict.json`
- `/app/data/agent_runs/latest/capture_stage_metrics.json`
- `/app/data/agent_runs/latest/phase3_goal_loop.json`
- `/app/data/agent_runs/latest/p7_paper_proposal_checkpoint.json`
- `/app/data/agent_runs/latest/phase3_wide_net_paper_contract.json`
- `/app/data/agent_runs/latest/phase3_24h_path_observer_summary.json`
- `/app/data/agent_runs/latest/phase3_path_horizon_audit_24h.json`
- `/app/data/agent_runs/latest/p9_metric_predictiveness_ledger.json`
- `/app/data/agent_runs/latest/influence_kol_shadow_source_plan.json`
- `/app/data/agent_runs/latest/influence_kol_shadow_features_24h.json`

Procedure:

1. Separate replay/OOS evidence from true paper trading evidence.
2. Treat P7 OOS pass as permission to draft a paper proposal, not as strategy promotion.
3. Keep the wide-net paper experiment disabled until the human approves it.
4. If Influence/KOL data is requested, route acquisition through `agent-reach` X/Twitter backends
   and write only shadow/cached features.
5. Continue P4/FDR and P8 shadow trials in parallel; do not block them on the P7 checkpoint.

Output contract:

- `phase3_goal_loop.json` must include task IDs `P3.1` through `P3.5`.
- `P3.1` must have `human_approval_required_before_enablement=true`.
- `P3.3` must have `human_checkpoint_required=true` when P7 passed OOS.
- `P3.5` must declare `acquisition_backend=agent_reach_x_twitter` and `production_impact=zero`.
- P3.1/P3.2/P3.4/P3.5 reports must remain read-only/shadow and `promotion_allowed=false`.
- Every output must keep `promotion_allowed=false`.

Acceptance:

- No strategy, gate, executor, canary, wallet, risk, or production exit file is changed.
- A generated P7 proposal explicitly states that real paper evidence is separate and insufficient
  for production promotion.
- The Influence/KOL plan contains no write actions and no runtime dependency on live X availability.

Findings ledger:

### 2026-07-08 Phase 3 Charter

- source_doc: `docs/agents/codex-goal-capture60-phase3.md`
- generator_script: `scripts/phase3_goal_loop.py`
- wide_net_paper_contract: `scripts/phase3_wide_net_paper_contract.py`
- path_observer_materializer: `scripts/phase3_24h_path_observer.py`
- path_horizon_audit: `scripts/phase3_path_horizon_audit.py`
- metric_predictiveness_ledger: `scripts/p9_metric_predictiveness_ledger.py`
- influence_kol_shadow_source_plan: `scripts/influence_kol_shadow_source_plan.py`
- influence_kol_shadow_collector: `scripts/influence_kol_shadow_collector.py`
- trigger: `P7_EXIT_POLICY_OOS_PASSED_PENDING_HUMAN_REVIEW`
- active_theme: `real_quote_tail_validation`
- P7_status: `paper_proposal_checkpoint_open`
- wide_net_paper_status: `proposal_only_human_approval_required`
- influence_kol_status: `shadow_only_agent_reach_acquisition_layer`
- promotion_allowed: `false`
