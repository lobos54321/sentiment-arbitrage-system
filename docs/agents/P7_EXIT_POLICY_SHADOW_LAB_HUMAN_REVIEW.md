# P7 Exit Policy Shadow Lab Human Review

## Status

P7 is complete and the Phase 2 queue is paused for human review.

The pause is intentional. `docs/agents/codex-goal-capture60-phase2.md` requires the agent to stop when P7 produces a ranked winner because adopting an exit policy is an S2 governance decision, not an automatic code change.

## Evidence

- code commit tested: `a3b23d8738279a39bb90b6b5e54262db3035cc19`
- ledger commit: `74b69f0a86b51073d49a029474878c10e2a48ee7`
- AutoLoop run id: `api_20260703T074855Z_88e2ae47`
- AutoLoop exit code: `0`
- AutoLoop timed out: `false`
- AutoLoop tests passed: `true`
- final reviewer verdict: `A_CLASS_EXPECTED_SHADOW`
- final blocker: `discovery_same_window_not_promotion_evidence`
- promotion allowed: `false`
- strategy change allowed: `false`
- paper enablement allowed: `false`
- P7 artifact: `/app/data/agent_runs/latest/exit_policy_shadow_lab_24h.json`
- P7 download key: `agent_runs/latest/exit_policy_shadow_lab_24h.json`

## Champion

- champion verdict: `CHAMPION_PENDING_HUMAN_REVIEW`
- policy id: `trail_a50_dd15_stop20`
- family: `trailing_drawdown_stop`
- activation: `50%`
- trailing drawdown: `15%`
- stop: `-20%`
- sample count: `472`
- policy variants tested: `21`
- slippage/delay cells tested: `504`
- entry delay grid seconds: `0, 5, 10, 20, 30, 60`
- slippage grid percent: `0, 1, 3, 5`
- objective ROI percent: `974745.670075`
- max drawdown percent: `14.606651`
- win rate: `0.620915`
- ranking recheck: `passed`
- future-data leakage probe: `passed`
- live exit policy changed: `false`
- production files touched: `[]`

## Interpretation

The lab found a strong shadow-only exit candidate under the pinned P7 objective: maximize rolling 24h realized net ROI on allocated risk capital, constrained by max drawdown, per-trade capital risk, and unchanged stop loss.

This is not production evidence by itself.

The result is still same-window shadow evidence. It is suitable for human review and for designing the next bounded validation step. It is not sufficient to modify the production exit policy, entry policy, A_CLASS mode, executor, gates, canary sizing, wallet, or risk settings.

## Required Human Decision

Choose exactly one:

1. Reject the P7 champion.
   - Outcome: keep Phase 2 paused until a new instruction is provided.
   - No production changes.

2. Approve a stricter shadow/OOS validation step for `trail_a50_dd15_stop20`.
   - Outcome: Codex may create a read-only validation task for this exact policy.
   - Allowed scope: shadow-only replay, OOS windows, robustness checks, report generation, and ledger updates.
   - Still not allowed: production exit change, paper/live enablement, gate changes, A_CLASS changes, canary/risk changes.

3. Approve drafting a human-reviewed paper proposal.
   - Outcome: Codex may draft a proposal document only.
   - The proposal must include exact risk boundaries, rollback rules, monitoring criteria, and no automatic enablement.
   - Actual enablement remains a separate human-run action.

## Minimum Follow-Up Validation If Approved

Before this champion can be considered for any paper proposal, require at least:

- independent OOS replay windows with no overlap
- stable ranking under 1%, 3%, and 5% slippage
- stable ranking under 5, 10, 20, 30, and 60 second entry delay
- comparison against the current exit baseline
- drop-top-winner and drop-top-3-winners sensitivity
- per-token duplicate and cluster-risk report
- time legality recheck
- execution-delay recheck
- final reviewer verdict still showing `promotion_allowed=false`

## Explicitly Forbidden Without Separate Human Approval

Codex must not:

- edit live exit code
- edit production strategy
- edit entry policy
- edit hard gates
- edit final entry contract
- enable A_CLASS
- enable paper/live executor
- change canary size
- change wallet settings
- change risk budget
- continue to P5/P6/P8 while this P7 champion is pending human review

## Current Queue State

- P2.1: complete
- P3: complete
- P4: complete
- P7: complete, champion found
- P5: not started
- P6: not started
- P8: not started

The correct next state is human review, not automatic implementation.
