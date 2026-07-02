# Capture-60 Deep Audit — 2026-07-02

- Method: 13-agent workflow (6 code readers, 3 data analysts, 4 adversarial verifiers), 1,042 tool calls over production artifacts (commit `a31bb93c`) + GitHub main + local repo.
- Window audited: raw_funnel 24h window `2026-07-01T03:37:41Z → 2026-07-02T03:37:41Z`, n=70 raw gold/silver signal events (49 unique tokens, 45 gold / 25 silver events).
- Every number below was independently recomputed by a verifier agent from fresh artifact pulls. Corrections applied where the first pass was wrong.
- Governance respected: this document proposes **data / report / evaluator / API / test work only**. No strategy, gate, executor, canary, or risk changes.

---

## 1. The funnel is three different problems wearing one number

Verified stage table (funnel definitions: `offline_raw_gold_silver_funnel_audit.py:985-1017` over `paper_decision_events`):

| Stage | Count | Cumulative | Conditional | Dominant loss cause |
|---|---|---|---|---|
| detector (raw g/s) | 70 | 100% | — | — |
| decision record | 41 | 58.57% | 58.57% | **27/29 = logging gap** (`shadow_entry_hypotheses_matched_no_decision_bridge`), 2 signal-id mismatches |
| pass_allow | 30 | 42.86% | 73.17% | not_ath_v17 ×5, kline-unknown ×2, policy ×2, top10 ×1, matrix-wait ×1 |
| pending | 23 | 32.86% | 76.67% | policy ×2 + 5 singletons |
| final_entry_contract | 3 | 4.29% | true P(final\|pending) = **2/23 = 8.7%** (48416 skipped pending) | stale_before_final **10/21 (47.6%)**, no_kline_low_volume 4, mode_shadow_preblocked 3, gmgn_pre_seen policy 3, spread 1 |
| paper | 0 | 0% | 0/3 | **100% `mode_disabled`** — zero non-mode blockers in window |

Three distinct layers:

1. **Instrumentation illusion (detector→decision, −29).** 93% of the biggest transition dropoff is the shadow decision bridge not writing decision events. Optimistic decision capture if bridged: **68/70 = 97.1%** (`shadow_decision_bridge.optimistic_decision_record_count_if_shadow_gap_logged`; bridges the 27 shadow-observed cases, not the 2 id-mismatches). This is not filtering.
2. **The real strategy cliff (pending→final, 23→3).** Largest category is `stale_before_final` (10/21). Confirmed kills include 48707 (gold, peak **+14,364%**, negative_trend) and CIT (+11,302%, no_kline_low_volume).
3. **Policy deadlock (final→paper, 3→0).** All 3 finals (48416 STOCHASTIC gold +213%, 48622 LR gold +147.5%, 48706 PKD silver +63%) blocked *solely* by `mode_disabled`.

**60% math:** decision=41 < 42=ceil(0.6·70) caps overall below 60% by exactly 1 signal even with perfect downstream. Bridging all 29 no-decision events lifts pending to 52/70=74.3%; bridging all 10 QT pending rejects lifts final to only 13/70=18.57%. **Both the bridge and pending→final conversion are jointly necessary; neither alone reaches 60%.**

**Dominant survival correlate is quote_clean, not kline:** `source_quote_clean=true` (38/70) → decision 94.7%, would_enter 73.7%; false (32/70) → decision 21.9%, would_enter 15.6%. Kline coverage does **not** predict survival (uncovered rows have *higher* would-enter rate: 50.0% vs 42.9%).

## 2. The mode deadlock (why paper = 0 and will stay 0 without a human)

- `A_CLASS_FASTLANE` was circuit-broken on **2026-06-11T17:23:58Z** (trade 61 ERBAI, 0.001 SOL canary, realized −20.9% > 20% cap; `a_class_runtime_safety.py:230-375`). Cooldown (86,400s) ended 2026-06-12.
- **No code path ever writes `status=LIVE` / `circuit_broken=0`.** The only writer is the breach upsert. Effective state has been `SHADOW / recovery_required / cooldown_elapsed_requires_clean_windows` for ~21 days.
- `clean_windows_required=4` is stored but **unimplemented** — `classify()` uses a single boolean pass of current conditions (`a_class_fastlane_mode_readiness_audit.py:1173-1211`).
- `paper_entry_proposal_readiness` is **circular**: requires `paper_trade_intent>0`, which `mode_disabled` makes impossible (blocks pop pendings before the intent event, `paper_trade_monitor.py:26016-26033`).
- Even when clean windows pass: stage becomes `mode_disabled_stuck_requires_human_review` → human clears DB row → `paper_proposal_ready_requires_human_approval`. **There is no automated path to paper by design.** The decision to re-enable is a governance action, not a code fix.

## 3. The top blocker is epistemic, and mostly a stale-payload bug (verifier verdict: CONFIRMED)

`volume_profile_coverage_below_80pct` does **not** reduce capture:

- No runtime gate consumes `volume_profile` — it is a post-hoc annotation (`candidate_shadow_observer.py:921-954`); zero references in `entry_engine.py`, `paper_trade_monitor.py`, `gmgn_policy.py`, `final_entry_contract.py`. It only gates (a) dimension eligibility in the evaluator and (b) the clean-window precondition for human mode re-enable.
- **Volume known 32.59% (117/359) root cause:** the shadow observer fetches klines once ~60–120s post-signal, the `not bars` guard (`candidate_shadow_observer.py:1891-1896`) blocks any refetch once ≥1 bar exists, and the payload freezes when the signal leaves the 10-newest window. It also reads its **own** thin `candidate_shadow_kline_cache.db`, not the dense `kline_cache.db` that `run-raw-path-observer.js` keeps. The data exists: matured recheck classifies **230/245 (93.9%)** of unknowns today; matured volume cross known-rate **92.05%** — read-only proof, `canonical_backfill_performed=false`.
- **Kline coverage 41.2% root cause:** a policy artifact. `raw-signal-outcomes.js:42-53` requires baseline first-bar lag ≤30s; minute-aligned bars make lag ~uniform(0,60s), structurally capping coverage near 50%. 29/30 uncovered rows have complete kline paths (`coverage_reason='covered'`). Research-grade coverage allowing low confidence: **98.04%**.
- Volume-unknown is **100% downstream of kline bar counts** (242 unknown = 200 `insufficient_kline_bars_lt_3` + 42 `kline_bars_unavailable`; the 42 are young pump.fun tokens not yet indexed by GT/DexScreener at observation time).
- Genuinely causal kline channel: `no_kline_low_volume` (4 pending→final kills) + `not_ath_prebuy_kline_unknown_data_blocked` (2 decision→pass_allow) ≈ **6 recoverable events** — which would still hit `mode_disabled` at final.

**Blocker taxonomy that should be first-class in every verdict:** `POLICY` (mode deadlock) / `EPISTEMIC` (coverage below threshold — blocks trust, not capture) / `CAUSAL` (gate consumed bad/missing data and killed a dog) / `INSTRUMENTATION` (event never recorded). Today's top_blocker is EPISTEMIC; the binding constraint is POLICY; the biggest headline number is INSTRUMENTATION.

## 4. Reject reasons: what the evidence actually supports

Formal protective-vs-harmful classification is **uncomputable today** for every reason: (1) no dud-kill denominators exist (all reason tables are gold/silver-scoped only — filter precision unknowable); (2) no reject-event timestamps are exported (only `first_pending_ts`), so peak-vs-block ordering is computable for just 5 exact + 2 bounded of 10 pending-stage QT kills; (3) no would-have-been PnL exists (the one replay was `REJECTED_FUTURE_DATA`, proxy-only); (4) the gold/silver label window (2h from signal, `raw-signal-outcomes.js:195,366`) contains every pending-stage reject — label circularity.

Evidence-weighted leans on the named reasons (n is tiny; treat as hypotheses to instrument, not conclusions):

| Reason | Mechanism (verified) | Window evidence | Lean |
|---|---|---|---|
| `entry_execution_signal_stale` | age > 300s from **original signal_ts**, no quote re-anchor (`entry_readiness_policy.py:20,397-547`) | category `stale_before_final` = 10/21 of pending→final losses (#1); measurable case 48458 (silver +87.9%, peak at +6,927s — long after +300s) | **most likely killing catchable dogs** (monotone reason; late-peaking dogs by construction outlive 300s). Fix path is shadow re-anchor evaluation, not threshold loosening |
| `lotto_timing_negative_m5` | m5 ≤ −10% from DexScreener snapshot, fires at watchlist + pending stages (`paper_trade_monitor.py:942,12632-12667`) | 1 g/s kill this window | insufficient n; plausible dip-entry kills, plausible protection — needs counterfactual ledger |
| `dead_cat_below_high` | >15% below local high (10% risky-newborn), in-memory price history (`entry_engine.py:74-77,944-961`) | **0 kills this window**, n=1 historical | not a current problem; deprioritize |
| `chasing_top` | pc_m5>100 & bs<2 (`entry_engine.py:1026`) | 3 orderings: 1 protective (48434 peaked before pending), 2 harmful-leaning (48399, 48509) | mixed |
| `momentum_fading` | m5 decayed >50% from first fire (`entry_engine.py:1044`) | 1 protective (48372), 1 harmful-leaning (48493) | mixed |

Ceiling check: even bridging **all** QT pending rejects yields final 13/70=18.6% (verdict-window upper bound incl. upstream QT: (3+21)/70=34.3%) — QT tuning alone cannot reach 60%. Instrument first (see task P4), judge later.

## 5. The 19 frozen 2D hits: freeze discipline works, statistics don't exist yet (verifier: CONFIRMED)

- Freeze/OOS timing is clean: items frozen 11:04:01Z; eval_start = last-batch fingerprint time + 120s = 12:30:35Z; train window ended 03:37:40Z; zero overlap; `promotion_allowed=false` everywhere. Good.
- But the discovery mesh judged **3,780 cells** in 24h (84 candidates × 45 dim-slices; 48h/72h meshes add 4,704 + 5,376 more looks) with pure thresholds (dogs≥10, recall≥0.5, precision>0, lift≥0.2) and **no multiplicity control anywhere**. Expected false DISCOVERY_HITs under a binomial null ≈ **10.9 of the 22 observed** (hypergeometric null: ≈3.1, but then the only 5 Bonferroni survivors are *self-crosses* — candidates crossed with the dimension they themselves gate on, e.g. `notath_mc_*` × `market_cap_bucket`). Only 2 structurally independent crosses exist; **neither survives correction**.
- The 19 hits collapse to **6 unique gold/silver event-sets** (10 unique event/matched-set pairs) over just 28 events / 22 tokens; ~6-9 are duplicates via proxy dimensions or twin candidates.
- In-sample, the hits are capture-recall effects only: 12/19 negative decision_lift, 16/19 negative pass_allow_lift, final_entry_rate_after_match=0.0 for all.
- **OOS clock fragility:** eval_start pins to the whole 192-definition set fingerprint and resets when ANY batch is added (already 3 batches: 19@11:04, 146@11:47, 27@12:28). Churn in the 173 non-capture-first items can reset the 19 hits' clock indefinitely.
- **Repeat bar is weak:** directional lift>0 with min 3 selected events → ~≤50% per-definition false-repeat; across 192 definitions expect dozens of chance "repeats". The sibling pass_allow_60 track already shows 4/38 NO_REPEAT on exactly-3-event judgments.

Required OOS discipline (shadow/evaluator-only) is codified in `skills/post-freeze-oos-validation.md`. Core: batch-pinned clocks, unique-token effective-N (≥10 tokens not events), dedupe to event-set families before testing, exclude self-crosses, BH-FDR q=0.1 across survivors, two consecutive disjoint OOS windows, negative-control panel (the `holdout_negative_controls` machinery already exists in `v27_basic_contract_readiness.py` — apply it to discovery).

## 6. Token motion data: what exists vs what is silently dropped

Exists today: `raw_price_bars_1m` (279,509 bars / 4,477 tokens / 0–2h post-signal), `kline_cache.kline_1m` (92,797 / 1,654 tokens, ~7d), `lifecycle_tracks` (1,736 tracks — **NOT_ATH-rejected cohort only**), `raw_signal_outcomes` (11,250 signals, peak marks 5m/15m/60m/120m + sustained, tiers), `raw_signal_observations` (first_bar_lag avg 384.7s, early_15m complete 45.3%), `paper_decision_events` (production only), `candidate_shadow_observations` (single snapshot per signal×candidate).

Dropped or missing (all recoverable cheaply — most are "stop discarding what's already in memory"):

1. **`signal.indices` (7 index families × current/signal) are never persisted** — 0/11,250 outcomes have them; index_lifecycle report is 100% null on indexes/deltas.
2. **ATH stage is never stamped per signal** (in-memory `athCount` → cumulative `ath_counts.json` only) — every ATH1-4 Strategy-Memory hypothesis is unvalidatable time-legally.
3. **No MC path** — bars are native price without supply/decimals stored.
4. **No reject-event timestamps or price-at-reject** — the single most valuable missing field for Q3-style counterfactuals.
5. Candidate observations have no re-observation history; timestamps are heterogeneous (ms vs s); three 1m-bar stores overlap without a canonical merge view.

Full spec: `skills/token-motion-trace-spec.md`.

## 7. The loop today and what closes it

- AutoLoop is a 5,385-LOC read-only evaluator (`agent_capture_discovery_loop.py`) with staged runner; **no scheduler** — runs happen only on manual POST or exec (runner_status only tracks API runs; latest artifacts came from a stage-runner exec).
- Codex task generation covers only 17 `FIXABLE_BLOCKER_HINTS` integrity blockers (`generate_codex_handoff.py:22-40`); currently `handoff_needed=false` while a 9.4k-line handoff still carries real work with **no diff-scoped task list or acceptance criteria**.
- No linkage handoff→commit→post-deploy verification (19 Codex-authored commits exist on origin/main, all 2026-07-02 — the loop works, manually).
- No sedimentation: no CLAUDE.md/AGENTS.md/skills; doctrine (`docs/problem-solving-operating-principles.md`) exists but nothing loads it; registry `recent_runs` keeps 20 shallow rows.
- Local checkout is **127 commits behind origin/main** with 27 dirty files; freeze/OOS builder code exists only on GitHub main.

Closure created in this audit: `AGENTS.md` (entry contract for any agent), `skills/` (5 seed skills + registry). Remaining closure is Codex work — see task queue.

---

## 8. Codex task queue (all data/report/evaluator/API/test scope; no strategy changes)

**P0 — decision bridge logging gap.** Write decision events (or a bridge-mirror table) for the 27 `shadow_entry_hypotheses_matched_no_decision_bridge` cases + 2 signal-id mismatches. Acceptance: next 24h window shows decision capture ≥ 0.9 with unchanged strategy; `capture_60_biggest_gap_stage` moves off `decision_capture`.

**P1 — kline/volume coverage repair (clears both clean-window conditions).**
   a. Observer refetch: replace `not bars` guard with `len(bars) < 5 and age < 3600s` refetch; read from canonical `kline_cache.db` (or merge view) instead of `candidate_shadow_kline_cache.db`; allow payload upsert past the 10-newest window until bars≥5 or 1h.
   b. Materialize matured volume recompute into the coverage metric (the recheck already proves 93.9% classifiable; make the coverage artifact read the matured value as the canonical known-rate, keeping the raw first-look rate as a secondary field).
   c. Kline coverage policy: either lag threshold 30s→90s or make coverage confidence-weighted; keep the strict variant as a secondary metric. Acceptance: `volume_profile_coverage ≥ 0.8` and `kline_coverage ≥ 0.8` in `clean_window_failed_conditions` within 24–48h, with an artifact field proving method change (no silent redefinition).

**P2 — mode re-enable contract (evaluator + procedure, human holds the switch).** Implement the missing pieces so the human decision is auditable: (a) a real 4-consecutive-clean-windows counter persisted in the runtime state table; (b) a documented, idempotent re-enable script (writes status=LIVE, circuit_broken=0, audit row with operator + reason); (c) fix `paper_entry_proposal_readiness` circularity (readiness must be computable while mode is SHADOW — use mode-adjusted proxies). Acceptance: `current_capture_stage` can legally transition `mode_disabled_clean_window_pending → paper_proposal_ready_requires_human_approval` without hand-editing SQLite.

**P3 — reject counterfactual instrumentation.** Emit reject events with `reject_ts`, `price_at_reject`, `quote_age_at_reject`, and stage; add same-window **all-signal** (dud-inclusive) denominators per reason to `quality_timing_research`; add a shadow re-anchor evaluator for `entry_execution_signal_stale` (re-evaluate freshness against latest executable quote instead of signal_ts; log would-pass). Acceptance: next audit can compute P(gold/silver | rejected by reason r) vs base rate and peak-vs-reject ordering for 100% of QT kills.

**P4 — OOS statistics upgrade.** Per-batch OOS clocks (pin eval_start to each item's `frozen_at` batch); dedupe frozen definitions into event-set families; exclude self-crosses (candidate gated on its crossed dimension); unique-token effective-N ≥10; BH-FDR q=0.1; two-window repeat rule; negative-control panel from `holdout_negative_controls`. Acceptance: post-freeze validation artifact reports per-family q-values and a null-panel repeat rate.

**P5 — motion trace v1 + persistence of dropped fields.** Persist `signal.indices` and per-signal `ath_stage` at `premium_signals` INSERT; store supply/decimals for MC path; normalize timestamps to ms; create `token_motion_events` per `skills/token-motion-trace-spec.md`; single `/api/agent/latest-status` compact projection endpoint. Acceptance: one SQL query reproduces the full funnel + motion trace for any mint.

**P6 — loop hygiene.** Scheduler (or documented cadence) for the stage runner; runner_status provenance for exec runs; handoff gains a `tasks[]` section (scoped diff-sized items with acceptance criteria, auto-derived from classification + this queue) and a `task_outcomes[]` section linking commits + post-deploy verification re-runs.

## 9. Priority verdict

With guardrails unchanged, the order that actually moves 60% capture:

1. **P0 bridge** (recovers the headline stage same-window; pure instrumentation),
2. **P1 coverage repair** (clears both clean-window blockers — the promotion gatekeeper — and recovers ~6 causal kline events),
3. **P2 mode contract + human decision** (nothing reaches paper until this; it is the only 0%→>0% lever),
4. **P3 instrumentation** (makes the stale-before-final cliff — the real strategy question — answerable instead of arguable),
5. **P4 OOS stats** (prevents the 192 frozen definitions from "validating" by chance),
6. **P5/P6** (compounding infrastructure).

The single most important reframe: **the system is not failing to catch dogs; it is (a) not writing down half of what it catches, (b) grading its own homework with a coverage metric its runtime never consumes, and (c) forbidden by a dead-man switch from taking the shots it lines up.** Fix the ledger, fix the meter, then make the human decision explicit.
