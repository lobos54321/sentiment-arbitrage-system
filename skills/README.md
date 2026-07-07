# Skills — machine-consumable experience for agents (Claude / Codex / Hermes)

This directory is the **sedimentation layer** of the capture-discovery loop: every audit that
produces a validated lesson writes it here, so the next agent starts from the lesson instead of
re-deriving it. Skills complement, and never override, the constitution:
`docs/problem-solving-operating-principles.md` and the guardrails in
`docs/agents/gold-silver-capture-discovery-loop.md` (no strategy / gates / executor / canary /
risk changes; discovery output cannot promote live trading).

## Skill file contract

Each skill is one markdown file with these sections, in order:

```
# <skill-name>
When to use:      trigger conditions (classification values, blocker names, questions)
Inputs:           artifact keys / DB tables / code paths required
Procedure:        numbered, deterministic steps an agent can execute
Output contract:  exact fields the resulting report/evaluator must publish
Acceptance:       how to verify the skill was applied correctly
Findings ledger:  append-only, dated, evidence-linked lessons (never edit old entries)
```

Rules:

- **Findings ledger is append-only.** New evidence gets a new dated entry; contradictions are
  recorded, not silently resolved.
- Every ledger entry must cite an artifact field path or `file:line` — no unsourced lessons.
- A skill that grows a stable Procedure should graduate into an evaluator script; note the
  script path in the skill when that happens (the skill remains the spec + ledger).
- When an AutoLoop verdict classification or blocker matches a skill's trigger, the handoff
  generator should reference the skill by name instead of restating the method
  (see `claudedocs/capture-60-deep-audit-2026-07-02.md` §8 P6).

## Index

| Skill | Trigger |
|---|---|
| [funnel-attribution-audit.md](funnel-attribution-audit.md) | any capture-rate question; `FUNNEL_DROPOFF_*`; "which stage is the bottleneck" |
| [capture-blocker-triage.md](capture-blocker-triage.md) | any `BLOCKED_*` classification or new top_blocker |
| [reject-reason-counterfactual.md](reject-reason-counterfactual.md) | `review_quality_timing_rejects_shadow_only`; "is reason X protective or harmful" |
| [post-freeze-oos-validation.md](post-freeze-oos-validation.md) | frozen definitions awaiting OOS; any `*_post_freeze_oos_validation` design change |
| [token-motion-trace-spec.md](token-motion-trace-spec.md) | designing/extending per-token lifecycle recording; "why can't we see what the token did" |
| [source-shadow-trial.md](source-shadow-trial.md) | adding or validating a shadow-only external signal/source trial such as pump.fun |
| [phase3-real-paper-validation.md](phase3-real-paper-validation.md) | P7 OOS passed; Phase 3 real paper validation / wide-net paper proposal / Influence-KOL shadow source |

## Lifecycle: how experience gets in here

1. **Audit** produces findings (agent run, workflow, or manual).
2. **Verify** — findings must survive an adversarial recomputation before they become lessons
   (the 2026-07-02 audit used independent verifier agents; cheaper: recompute from a fresh
   artifact pull).
3. **Sediment** — append to the matching skill's Findings ledger (or create a new skill if a
   genuinely new procedure emerged).
4. **Compile** — when a procedure stabilizes, ask Codex to turn it into an evaluator script +
   artifact + download key, and link it from the skill.
