# GOAL — Ex-Ante Predictability Verdict for Telegram Signals

Status: LOCKED decision charter, 2026-06-17. This is a **decision goal, not a discovery goal** — a commitment device that locks the gates, stop-losses, and pre-committed verdicts BEFORE execution so the line cannot be silently extended. **Success = a defensible, evidence-backed decision within budget — NOT finding an edge.** Strategy layer frozen throughout; no edge may be claimed without a passed OOS look point.

## Core question (the only one this goal answers)
> At ≤ `signal_ts`, does the Telegram signal source contain ANY usable **ex-ante** structure that distinguishes future winners (dog) from losers (dud) — when correctly staged (full domain) and using the signal's OWN metadata? Either it yields ONE prereg-ready primary, or it produces an evidence-based downgrade/pivot.

What was NOT answered before (and why this is new): `unique_buyers` answered "do pre-signal curve buyer-counts work?" (null); the Helius/Dune canary answered "is the provider blind?" (no — windows were empty); the source audit answered "does the OOS table's `source` field vary?" (no — flattened to a constant). **None tested whether the signal's own metadata / timing / stage at t0 carries ex-ante structure.**

## Locked context (verified this session)
- Dogs are real movers (sustained peak median +107%, p75 +181%) but **peak ~19 min AFTER signal** (median 1150s, 63% ≥15 min post); duds fizzle in ~1 min. ~33% of dogs are already **post-graduation** (USD/AMM) at signal.
- `raw_signal_outcomes` flattens the source to the constant `premium_signals`. Upstream `premium_signals` (sas_sentiment_current.db, 36,941 rows, 2026-02-26→06-07) has coarse+partial internal metadata (`signal_type` NEW_TRENDING/ATH, `is_ath`, `narrative_score` ~29%, `ai_narrative_tier` ~25%, `raw_message` ~50%; `signal_source` coarse, no per-channel id) — but is **0/106** on the OOS cohort (stale snapshot).

## CARDINAL INVARIANT (the #1 thing to protect)
**Gate 1 (labeled exploration) may ONLY touch a data window that closes BEFORE Gate 2's OOS window opens.** The prereg lock + OOS data collection happen AFTER the Gate-1 screen + candidate selection. Reusing the same recent data across Gate 1 → Gate 2 = fished = the run is void.

## Gates (cheap → expensive; each is a stop-loss)

### Gate 0 — Metadata / Logging Feasibility  (read-only, ≤1 day)
**Question:** is the Telegram internal metadata actually RECORDED — where, joinable to `(token_ca, signal_ts)`, point-in-time, with coverage + variance?
**Inventory:** for each candidate field (`signal_type`, `is_ath`, `narrative_score`, `ai_narrative_tier`, `raw_message`→channel/caller/keywords/first-call-vs-repeat, `source_event_id`, `signal_links_json`, and stage-at-signal): which DB/log/snapshot, the join key, the point-in-time rule, **per-field coverage %**, **per-stage support count**, and staleness vs any test window.
**Four-outcome decision tree:**
- (a) recorded + joinable + point-in-time + meaningful coverage → **proceed to Gate 1**.
- (b) recorded but the OOS projection dropped it → fix projection (small), then Gate 1.
- (c) recorded only in a stale/partial store → **INSTRUMENT-FORWARD**: collect forward N days before any test is possible; surface the timeline cost; no usable cohort now.
- (d) not recorded at all → **logging bug, not a research problem → STOP research, fix logging.**
**Stop-loss:** if no field clears coverage + variance + point-in-time + per-stage min-support → do NOT enter Gate 1; output is (c) or (d).

### Gate 1 — Quarantined Exploratory Screen  (uses historical/seen labels, firewalled, ≤1 day)
- **Pre-declare a small, theory-driven candidate set** (e.g. {stage-at-signal [baseline], `is_ath`, `signal_type`, `narrative_score`, `ai_narrative_tier`, channel-from-`raw_message`, first-call-vs-repeat}); **report ALL of them** (the effect on each), not just the promoted winner — so the multiple-comparison surface is visible.
- **Full-domain, stage-stratified, NO pooled metric** (Simpson): keep sol_curve + spliced/graduation-bridge + usd_gmgn/AMM + mature/recovery + unknown(honestly labeled).
- **Stage-at-signal is the BASELINE-to-beat:** metadata features must add separation AFTER partialling-out stage (so we do not re-discover the known stage/coverage shadow).
- **Per-stage min-support floor** — no conclusion from a stratum below the floor (the gmgn=3 lesson).
- **Output:** at most **1 primary + 1 backup** candidate for Gate 2, OR "nothing separates above the stage baseline."
- Discipline: descriptive only; NO edge claim; NO live; NO reuse of this data in Gate 2 (cardinal invariant).
- **Stop-loss (the big off-ramp):** if nothing separates above the stage baseline even in quarantined exploration → ex-ante structure likely doesn't exist → go straight to Gate 3 = downgrade.

### Gate 2 — New Locked Prereg OOS  (only if Gate 1 yields a candidate; FRESH forward data)
- ONE pre-registered primary (from Gate 1), full-domain, **ex-ante only (≤signal_ts)**, correct stage, sealed thresholds, look points 50/100/130, futility rule — the existing OOS machine.
- **EXECUTABILITY HARD GATE:** success requires not just statistical separation but that, at a realistic post-signal entry, **sufficient residual upside remains**. **Prefer t0-available features** (signal metadata / stage-at-signal) which are executability-clean; a feature only knowable AFTER the move starts = fail (late / future-info).
- Only place an edge may be claimed, and only at a powered look point.
- **Stop-loss:** futility-null → archive; at most ONE pre-committed alternative, else stop. No infinite feature stacking.

### Gate 3 — Business Posture Verdict
- **PASS** (OOS-confirmed AND executable) → design a shadow-only ranking gate (no live trading; a separate live-readiness process is required); re-evaluate the target.
- **NULL across gates** → downgrade 60/60/200 to a shadow-only / silver-side posture; the deliverable becomes the (genuinely valuable) measurement infrastructure + the honest characterization: "this is a post-signal momentum source, not an ex-ante-predictable one."

## Budget envelope
Phase 1 (Gate 0 + Gate 1) ≤ **1–2 days**; **no heavy Dune wallet-history join; no new provider integration; no live/strategy change; read-only.** If Gate 0 returns outcome (d) "not recorded," stop immediately (logging fix, not research). Hitting the ceiling without a Gate-1 candidate → default verdict = downgrade.

## Pre-committed verdicts (the ONLY allowed outcomes — no "keep looking")
1. **PREREG_READY** — Gate 1 produced a candidate clearing the stage-baseline + coverage/support → proceed to Gate 2 (a new locked prereg).
2. **INSTRUMENT_FIRST** — the metadata is conceptually right but not recorded/joinable now → fix logging/projection or collect forward; re-enter Gate 0 later.
3. **NO_EX_ANTE_STRUCTURE → DOWNGRADE** — nothing separates above the stage baseline / coverage too thin → evidence-based downgrade to shadow-only / target review.

## Hard prohibitions (frozen throughout)
No gate / matrix·RR / liquidity / exit / live-size / main change. No edge claim without a passed OOS look point. No rescuing `unique_buyers`. No leaked daily AUC. No post-hoc secondary-feature picking. No treating current-snapshot metadata as signal_ts-era data. Descriptive separation is NOT a strategy conclusion. This goal reframes the research STAGE and reaches a DECISION; it licenses no live change.
