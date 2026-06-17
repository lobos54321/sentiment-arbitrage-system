# Gate-0.5 Paid Pilot — Operator Run-Sheet (2026-06-18)

Goal: decide **HISTORICAL_BACKFILL_FEASIBLE / PARTIAL / NOT_FEASIBLE** — can historical premium_signals be backfill-labeled (+stage) and reconciled vs observer, to justify a clean historical held-out test of Telegram metadata. Feasibility ONLY; no edge claim, no live/gate/exit/size, strategy frozen. Tooling audited clean through commit 499f29a1.

## Active guardrails (all structural, fail-closed)
- Wrong premium DB → die (needs core metadata cols + ≥30k rows; `server_sentiment_arb.db` is rejected).
- `--max-credits 30` → fail-closed (incl. when Dune exposes no credit field, before fetching results).
- Reconciliation bars must match observer `path_provider`/`path_source_kind` → die before the labeler on mismatch.
- `local_cache`/non-reproducible observer sources → **excluded + reported** (out of denominator), not death.
- Dune is stage-tags only (label-free; can't become label bars).

## Prerequisites
- **Real `sas_sentiment_current.db`** (≈36,941 rows; the guard enforces identity). NOT `server_sentiment_arb.db`.
- Observer DB: `/Users/boliu/sas-data-room/oos-frozen-pack-20260617T001655Z/raw_signal_outcomes.snapshot.db`.
- Dune key at `~/.dune_api_key` (present).
- `OUT=~/sas-data-room/gate05-pilot-<UTCstamp>` ; `mkdir -p "$OUT"`.

## Step 1 — prepare (free, read-only)
```
node scripts/run-gate05-backfill-pilot.js --mode prepare \
  --premium-db <REAL sas_sentiment_current.db> \
  --observer-db <frozen observer DB> \
  --out-dir "$OUT/prepare"
```
Emits `pilot-signals.json`, `burned_keys.txt`, `signal_windows.csv`, `provider-request.json`, `prepare-manifest.json`. Verify `prepare-manifest.json` → `inputs.premium_db.identity` shows row_count ≈36,941 + month_inventory. Prioritizes the 6/06–6/07 overlap (≤200). Expectation: ~38 geckoterminal-source (reconcilable) + ~12 local_cache (will auto-exclude at evaluate).

## Step 2 — reconciliation bars = OBSERVER source (gecko) [paid-ish]
For the overlap signals whose observer `path_provider='geckoterminal'`, fetch geckoterminal OHLCV for `[signal_ts, signal_ts+7200]`, 1-minute, into JSONL with EXACTLY this schema (tags must match the observer or the guard dies):
```
{token_ca, timestamp(unix sec, minute-aligned), open, high, low, close, volume,
 provider:"geckoterminal", source_kind:"indexed_ohlcv", price_unit:"native"}
```
→ `$OUT/gecko-bars.jsonl`. Use the dedicated adapter (dry-run first):
```
python3 scripts/fetch-gate05-gecko-reconciliation-bars.py \
  --pilot-signals "$OUT/prepare/pilot-signals.json" \
  --observer-db <frozen observer DB> \
  --out-jsonl "$OUT/gecko-bars.jsonl" \
  --manifest "$OUT/gecko-bars-manifest.json" \
  --windows-csv "$OUT/gecko-windows.csv" \
  --dry-run
```
If the dry-run selection is correct, rerun without `--dry-run --force` to fetch. The adapter uses GeckoTerminal `token=base` (matching the raw-path observer shared-pool contract) and explicitly does **not** use `currency=usd` or Dune curve bars. If Gecko cannot re-pull bars for a geckoterminal observer signal, evaluate will fail closed; that is a pilot finding, not something to paper over.

## Step 3 — Dune stage-tags (paid, capped)
Splice `signal_windows.csv` rows into `{{SIGNAL_WINDOWS_VALUES}}` of `scripts/gate05-backfill-pilot-stage-tags-dune.template.sql`, then:
```
python3 scripts/run-dune-sql-export.py --sql "$OUT/stage-tags.sql" \
  --max-credits 30 --performance small \
  --out-jsonl "$OUT/stage-tags.jsonl" \
  --manifest "$OUT/stage-tags-manifest.json"
```
Label-free curve-presence tags only. The cap is now a real gate (cancels + fails-closed). Record the reported credits.

## Step 4 — evaluate (free)
```
node scripts/run-gate05-backfill-pilot.js --mode evaluate \
  --observer-db <frozen observer DB> \
  --pilot-signals "$OUT/prepare/pilot-signals.json" \
  --bars-jsonl "$OUT/gecko-bars.jsonl" \
  --stage-tags-jsonl "$OUT/stage-tags.jsonl" \
  --cost-credits <actual credits from Step 3> \
  --out-dir "$OUT/eval"
```
→ `pilot-evaluation-summary.json`: `reconciliation_source_match` (comparable_n, excluded_source_not_reproducible_n), `reconciliation` (dog/dud agreement + Wilson CI on comparable_n + difference_taxonomy), `stage_resolution` (from Dune tags), `outcome_labelability`, `cost`, `verdict`.

## Step 5 — read the pre-committed verdict
- **FEASIBLE** (recon ≥90% on comparable_n, stage ≥70%, cost ok, failures not dog/dud-biased) → write the locked historical Explore/Held-out split + Gate-1/Gate-2 spec.
- **PARTIAL** → name the blocker (reconciliation / stage / cost / coverage); shrink window/domain or fall back to instrument-forward.
- **NOT_FEASIBLE** → instrument-forward (collect fresh) or target downgrade.
- **By hand:** if `comparable_n` is small, the Wilson CI governs (run-card: "n too small to separate ~85% vs ~90% → difference-taxonomy governs"). Treat a small-n FEASIBLE as provisional.

## Interpretation guardrails
A FEASIBLE verdict only unlocks the *option* to run a clean historical Gate-1/Gate-2 on Telegram metadata; it is NOT an edge. Reconciliation tests data-source reproducibility, not predictiveness. Do not read any of this as a strategy result.
