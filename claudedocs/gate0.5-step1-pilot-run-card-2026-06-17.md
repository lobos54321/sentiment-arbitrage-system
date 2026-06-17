# Gate-0.5 Step-1 Pilot — LOCKED Run-Card (2026-06-17)

Purpose: feasibility ONLY — can historical premium_signals be reliably backfill-labeled (dog/dud outcome + stage-at-signal) to support a clean historical Explore/Held-out OOS? **NOT an edge test.** Once it produces labels, the touched rows are BURNED. Lock everything below BEFORE running; do not pass overriding flags at runtime.

## Feasibility facts (verified)
- Dune key present (`~/.dune_api_key`); pilot is technically runnable.
- Reconciliation universe = **335 observer-tiered signals in 6/06–6/07** (175 + 160).
- Metadata-covered windows: signal_type/is_ath **Apr–Jun**; +narrative_score **May–Jun**; **Feb–Mar metadata-dead (excluded)**.
- Observer label/tier definition to REUSE: **`src/analytics/raw-signal-outcomes.js`** (sustained-peak → gold/silver/bronze/sub25). The backfill labeler MUST reuse this exact logic on backfill-pulled data, not a fresh reimplementation (else reconciliation conflates data-source vs re-impl drift).

## 1. Sample (locked)
- n ≈ **200**, hard cap 200.
- Priority: the **6/06–6/07 observer-overlap** (reconciliation core; take as many of the 335 as fit), then fill from **Apr–Jun metadata-covered** windows.
- **ALL pilot rows → `burned_keys.txt`** (keyed `(token_ca, signal_ts)`); they may NEVER enter Gate-1 explore or Gate-2 OOS.

## 2. Stratification (locked)
Stratify by **labelability covariates ONLY**: date/month · token age · preliminary domain/stage proxy · price-provider availability · token liquidity / known coverage.
**Forbidden strata:** signal_type, is_ath, narrative_score, raw_message content. (Pilot tests labelability, not the candidate feature.)

## 3. Label Reconciliation — HARD GATE (highest priority)
On the 6/06–6/07 overlap, compare **backfill labeler (reusing `raw-signal-outcomes.js`) vs live observer** on: dog/dud · tier · sustained_peak_pct · baseline_price · peak timestamp · peak domain · coverage/missing reason.
- **dog/dud agreement ≥ 90% → PASS**; 80–90% → **PARTIAL (must explain)**; **< 90→**<80% → **NOT_FEASIBLE / blocker**.
- Report the agreement **with its CI** (n≈335 → ±~5pp). If n is too small to separate ~85% vs ~90%, the **difference taxonomy governs**: classify every disagreement as coverage / baseline / unit / sustained-definition.
- Reconciliation fail ⇒ do NOT proceed to historical Gate 1/2 (the backfill labels would be a new, uncalibrated metric).

## 4. Stage-resolution — HARD GATE (separate from outcome)
Report separately: stage-resolved rate · progress-available rate · baseline-at-signal available · graduation/AMM-status available · unknown-stage reasons.
- **≥ 70% → FEASIBLE** for stage-controlled Gate 2; 50–70% → **PARTIAL**; **< 50% → NOT_FEASIBLE** for stage-controlled Gate 2 (⇒ fall back to instrument-forward, where stage is captured live).
- Rationale: stage is the MANDATORY partial-out control in Gate 2; no historical stage ⇒ no clean stage-controlled test.

## 5. Outcome labelability
Report: labelable rate · complete-2h-window rate · sustained-peak-available · baseline-available · provider failure reasons · **cost per 100 signals** · extrapolated cost for 5k / 10k / full candidate (~20k Apr–Jun).

## 6. Cost ceiling (locked)
**≤ 30 Dune credits AND ≤ 200 signals.** If exceeded → abort, emit PARTIAL (cost-infeasible). Narrow [signal, signal+2h] windows only; NO 365d wallet-history join; no new provider.

## 7. Forbidden (locked)
No signal_type/is_ath/narrative_score dog-rate; no AUC/precision/recall/lift; no feature ranking; do NOT use pilot results to pick a primary; no leaked AUC; no gate/exit/size/live/main change. Pilot is feasibility, not edge.

## 8. Verdict (only these three)
- **HISTORICAL_BACKFILL_FEASIBLE** — reconciliation ≥90%, stage ≥70%, cost ok, clean Explore/Held-out split available, failure reasons not systematically dog/dud-biased → next: write the locked historical split + Gate-1/Gate-2 spec.
- **HISTORICAL_BACKFILL_PARTIAL** — name the blocker (reconciliation / stage / cost / coverage) → propose a shrunk window/domain, or fall back to instrument-forward.
- **HISTORICAL_BACKFILL_NOT_FEASIBLE** — → instrument-forward (collect fresh) or target downgrade.

## Output format
1. Metadata-only inventory (done in Step 0) 2. Pilot sample design 3. Labelability results 4. Stage-resolution results 5. Cost estimate 6. Failure-reason taxonomy 7. Reconciliation results (+ difference taxonomy) 8. Verdict 9. Next step.
