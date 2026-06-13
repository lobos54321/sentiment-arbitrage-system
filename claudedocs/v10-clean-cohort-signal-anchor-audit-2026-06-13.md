# V10 Clean Cohort Signal-Anchor Audit

Status: offline research artifact, 2026-06-13.

This note follows `cohort-rebuild-v10-native-return-guard-2026-06-13.md`.
It uses the v10 label/return-clean cohort as the current raw-discovery
baseline and asks the next question: what can be inferred from complete
GMGN full-window evidence at the signal anchor?

## Inputs

Data room:

`/Users/boliu/sas-data-room/chain-truth-recut-20260612T011545Z`

Primary cohort:

- `cohort-rebuild-v10-final-native-return-guard/rebuilt-clean-dogs.json`
- `cohort-rebuild-v10-final-native-return-guard/rebuilt-clean-duds.json`

GMGN full-window evidence:

- Existing: `gmgn-full-window-v11-final-native-return-guard/gmgn-full-window-touch.json`
- Missing sol-curve follow-up: `gmgn-full-window-v12-missing-v10-sol-curve/gmgn-full-window-touch.json`
- Complete merged pack: `gmgn-full-window-v13-complete-v10/gmgn-full-window-touch.json`
- Dog-only merged pack: `gmgn-full-window-v13-complete-v10/gmgn-full-window-dogs.json`
- Dud-only merged pack: `gmgn-full-window-v13-complete-v10/gmgn-full-window-duds.json`

Reports:

- `stage-stratified-v10-complete/stage-stratified-audit.json`
- `v10-signal-anchor-summary/summary.json`

## Coverage Fix

The earlier GMGN full-window pack was incomplete for v10:

- v10 dogs: 461 rows, 404 already touched, 57 missing
- v10 duds: 971 rows, 873 already touched, 98 missing
- all 155 missing rows were `return_domain=sol_curve`

The v12 follow-up probed those 155 rows with checkpoint/resume enabled:

- ok: 155/155
- bars available: 155/155
- nonzero volume available: 155/155
- early-15m nonzero volume: 115/155 = 74.19%

Merged v13 coverage:

- total rows: 1432
- ok: 1432/1432
- bars available: 1432/1432
- nonzero volume available: 1432/1432
- early-15m nonzero volume: 728/1432 = 50.84%

Interpretation: the v10 sol-curve gap was not a GMGN source ceiling. It was a
coverage gap in the local touch pack.

## Signal-Anchor Ceiling

This is not a live strategy result. It is the paper ceiling if the system could
act at the signal anchor and use the GMGN full-window path for residual upside.

All v10 clean dogs, signal anchor:

| Delay | Evaluable | Silver | Silver Rate | Gold | Gold Rate |
| --- | ---: | ---: | ---: | ---: | ---: |
| T+0 | 461 | 407 | 88.29% | 233 | 50.54% |
| T+5m | 461 | 333 | 72.23% | 187 | 40.56% |
| T+15m | 461 | 302 | 65.51% | 170 | 36.88% |

By return domain, T+0:

| Domain | N | Silver Rate | Gold Rate |
| --- | ---: | ---: | ---: |
| `usd_gmgn` | 304 | 96.38% | 52.96% |
| `spliced_curve_to_gmgn` | 100 | 70.00% | 44.00% |
| `sol_curve` | 57 | 77.19% | 49.12% |

Important: this is signal-anchor only. The current data room does not include
the paper decision subset required to compute a formal matched signal-vs-decision
ceiling. Prior matched-ceiling numbers from dirty/v1 packs are historical only.

## Stage-Stratified Dog-vs-Dud

Volume visibility stage, v10:

| Stage | Dogs | Duds |
| --- | ---: | ---: |
| already volume-visible at anchor | 160 | 387 |
| volume visible within 5m | 15 | 35 |
| volume visible 5m to 15m | 40 | 91 |
| dark after 15m | 246 | 458 |

Lag AUC, dog greater than dud: `0.5317`.

Early-15m volume AUC by stage:

| Stage | Dogs | Duds | AUC | Dog Median | Dud Median |
| --- | ---: | ---: | ---: | ---: | ---: |
| already visible | 160 | 387 | 0.5434 | 24,325.80 | 21,216.84 |
| visible within 5m | 15 | 35 | 0.5848 | 40,956.77 | 20,718.59 |
| visible 5m to 15m | 40 | 91 | 0.4398 | 4,094.26 | 4,075.35 |
| dark after 15m | 246 | 458 | 0.5000 | 0 | 0 |

Pre-anchor GMGN volume:

- only the already-visible stage has nonzero pre-anchor volume in this pack;
- already-visible pre-5m/pre-15m AUC is `0.5781`;
- pre-anchor GMGN volume cannot distinguish the dark curve cohort because it is
  structurally zero there.

## Interpretation

1. v10 is the current clean raw-discovery denominator.
2. GMGN full-window evidence is now complete for v10.
3. Signal-anchor silver ceiling is high; gold ceiling is still below 60%.
4. A 5-minute delay is expensive: silver drops from 88.29% to 72.23%, gold from
   50.54% to 40.56%.
5. The GMGN stage/volume features now show weak discrimination, not a strong
   ex-ante gate:
   - visibility-stage AUC: `0.5317`
   - already-visible pre-anchor volume AUC: `0.5781`
   - early-15m is future information and can only support delayed confirmation,
     not immediate entry.
6. The dark-after-15m cohort remains the key unresolved feature problem. GMGN
   OHLCV confirms the path after it becomes visible, but does not provide
   pre-anchor curve microstructure for that dark cohort.

## What Is Still Blocked

The formal matched signal-vs-decision ceiling is not available from this data
room because it lacks the paper decision subset (`a_class_decision_events` /
`opportunity_events`). Do not cite prior dirty-pack matched-ceiling results as
current.

Required next data pack addition:

- paper decision subset with `token_ca`, event timestamp, decision/would-enter
  fields, hard blockers, block cause, quote/liquidity/spread/hydrate outcome.

## Next Valid Analyses

1. Build a decision-anchor pack for v10 and recompute matched signal-vs-decision
   ceiling on the same signal-level cohort.
2. Re-run dog-vs-dud using decision-time anchors, not just signal anchors.
3. Add curve microstructure features for the dark cohort:
   curve progress, buy/sell counts, unique buyers, net SOL flow, progress slope.
4. Split executable analysis by `return_domain`:
   `usd_gmgn`, `spliced_curve_to_gmgn`, and `sol_curve`.

## Guardrail

This audit still does not justify changes to live gate, matrix/RR thresholds,
exit policy, or live size. It only establishes a cleaner denominator and a
signal-anchor ceiling for the v10 frozen pack.
