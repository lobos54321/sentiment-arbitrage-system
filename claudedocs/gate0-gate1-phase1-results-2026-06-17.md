# Phase 1 Results — Gate 0 + Gate 1 (Ex-Ante Predictability Verdict)

> 2026-06-17, read-only, executed inline per claudedocs/goal-ex-ante-predictability-verdict-2026-06-17.md. Descriptive only; NO edge claimed; SEEN/discovery-window data (hypothesis generation, firewalled from any future Gate-2 OOS). **PHASE-1 VERDICT: `INSTRUMENT_FIRST`** (carrying one named candidate). Strategy frozen.

## Gate 0 — Metadata/Logging feasibility
- [VERIFIED] Telegram metadata IS recorded in `premium_signals` (sas_sentiment_current.db, 36,941 rows, 2026-02-26→06-07): `signal_type` (NEW_TRENDING/ATH), `is_ath`, `narrative_score`, `ai_narrative_tier`, `raw_message`, `signal_source` (coarse). It is **point-in-time** (event timestamp = the signal; ts-match to labels is exact, p50 delta = 0) and **100% populated where a signal matches** (signal_type/is_ath/narrative_score/raw_message all 100% in matched rows; ai_tier ~26-44%).
- [VERIFIED] BUT coverage of the labeled cohort is only **190/1432 = 13.3%** point-in-time (89 dog / 101 dud); `premium_signals` ends 6/07 (OOS cohort 6/14-16 = **0/106**), and tapers near its end (only 290 rows in 6/06-6/08).
- **Gate-0 outcome = (a)-for-Gate-1 / (c)-for-Gate-2:** the metadata gives a usable 190-signal historical slice for an exploratory screen, but is far too sparse + stale to support a powered, stage-controlled OOS test → Gate 2 requires **instrument-forward**.

## Gate 1 — Quarantined exploratory screen (190-slice, 89 dog / 101 dud)
Descriptive dog-rate by the pre-declared candidate set (reporting ALL, not just the winner):
- **OVERALL slice dog-rate = 0.468** — vs full-cohort 0.32 ⇒ **premium-coverage SELECTION BIAS**; every rate below is conditional on "has premium coverage."
- **`is_ath`=0: 0.607 (n=107) vs is_ath=1: 0.289 (n=83).** `signal_type` NEW_TRENDING 0.607 vs ATH 0.289 (same field). `narrative_score`>5: 0.612 (n=103) vs ns=0: 0.299 (n=87). → **all one axis** (NEW_TRENDING+narrative vs ATH/no-narrative); ~2x dog-rate difference, descriptively.
- `ai_narrative_tier`: null 0.537 vs CONFIRMED 0.343 (counterintuitive — "CONFIRMED" *lower*; likely noise/confound).
- **BASELINE (stage-at-signal) could NOT be partialled out:** the stage join resolved for only 13/190 (177 "unknown") — so the charter's hard requirement ("metadata must add separation AFTER partialling-out stage") is **UNMET on this slice**.

### Why this is a candidate, not an edge (the gates it fails)
1. **Not stage-controlled** (93% stage-unresolved) — and `ATH` = *already at all-time-high* ⇒ the move may already be in ⇒ the NEW-vs-ATH axis is **plausibly a timing/stage proxy**, i.e. the same stage/timing story (how late Telegram fired), not independent structure.
2. **Selection bias** — 13% coverage; the slice is enriched in dogs (0.47 vs 0.32).
3. **Seen/discovery-window data** — hypothesis generation only; ATH-vs-NEW being different is easy to find post-hoc.
4. **One redundant axis, n~190.** ai_tier is counterintuitive.

## Phase-1 verdict: `INSTRUMENT_FIRST`
Gate 1 surfaced **one t0-available (executability-clean) candidate — `signal_type` / the NEW_TRENDING-vs-ATH axis** (narrative_score is its correlate; ai_tier is not) — so this is **not** a clean `NO_EX_ANTE_STRUCTURE → DOWNGRADE`. But a clean, powered, **stage-controlled** Gate-2 OOS is **impossible on current data** (13% coverage, stale ≤6/07, stage unresolvable on the slice). Therefore: **instrument-forward, then one Gate-2 prereg.**

### Instrument-forward spec (the next concrete work; cheap — projection/logging, no new provider/Dune/live)
1. Project `premium_signals.{signal_type, is_ath, narrative_score}` **+ a clean stage-at-signal** (baseline_price_unit / progress / graduation-proximity) into the daily OOS feature pipeline, at **full coverage**, for FRESH signals (fixes the flattening + the 13% gap).
2. Accrue N days of fresh, full-coverage, stage-bearing signals — this IS the firewall-clean Gate-2 OOS substrate (collected after the prereg lock).
3. **Then** write the Gate-2 prereg: primary = `signal_type` (NEW-vs-ATH), with **stage as a MANDATORY partial-out control** (else it re-discovers the stage shadow as a fake metadata edge), ex-ante only, executability hard gate (t0-available so entry-at-signal is clean), look points 50/100/130, futility rule.

### Honest prior (do not inflate)
I still lean that this ends at **DOWNGRADE**: the one hint is most likely a timing/stage proxy (ATH = move already in) that will collapse toward the stage baseline once stage is controlled. But instrument-forward is cheap (a projection fix on data already recorded), and `signal_type` is the one untested, t0-available, executability-clean family — so it clears the bar for "worth instrumenting + one clean prereg" over "downgrade now." If, after instrument-forward, the stage-controlled signal_type test is null → that is the evidence-based DOWNGRADE, cleanly reached.

## Discipline upheld
Read-only; no gate/exit/size/live/main change; no edge claimed; descriptive ≠ strategy; SEEN data only (firewalled from Gate 2); current-snapshot metadata not treated as a different signal_ts's data; no unique_buyers rescue; no leaked AUC. Budget: stayed within Phase-1 (no Dune wallet join, no new provider, local read-only).
