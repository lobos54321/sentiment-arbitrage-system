# Expert Update Notice — Raw Dog Chain Truth Path

Date: 2026-06-12  
Branch: `runtime-stability-marker-guard`  
Latest research commit: `84c44885`  
Scope: research-only. No live strategy change. No `main` deployment.

## 0. Current Decision Discipline

Do not change:

- live gate
- matrix / RR thresholds
- exit policy
- live size
- final contract
- canonical ledger definitions

Reason: the current blocking question is still upstream of strategy:

> Are the frozen raw dogs, especially pump.fun pre-graduation candidates, real traded opportunities with usable chain-truth features at decision time?

Until chain-truth evidence answers that, strategy changes would be blind.

## 1. What Has Been Verified So Far

### 1.1 The Main Bottleneck Moved Upstream

Earlier expert panel conclusion:

- The bottleneck is not currently proven to be execution or exit first.
- The main bottleneck is signal/evidence coverage for pump.fun pre-graduation dogs.
- Runtime issues amplify the problem but are not the strategy answer.

The key observed structure:

- Many sustained raw dogs are pump.fun candidates seen before or near AMM migration.
- AMM-centric evidence (`liquidity_known`, `spread_ok`, AMM pool volume) does not represent this phase well.
- GMGN can retrieve historical volume for dog/dud cohorts, but early decision-time evidence must be anchored carefully to avoid future leakage.

### 1.2 Label Contamination Was Found

A label-cleaning audit found that some recorded raw-dog peaks were incompatible with the stored native price bars.

Interpretation:

- Some historical raw peak labels mixed native/SOL price paths with USD-denominated values, or were left stale after path repair.
- Those rows must be quarantined before feature, capture, or goal math.

Current policy:

- Use a clean pack for analysis.
- Keep a quarantine layer for chain-truth adjudication.
- Do not silently drop quarantined rows; that creates survivorship bias.

### 1.3 GMGN Volume Alone Is Not the Final Answer

Earlier raw GMGN touch showed that GMGN can retrieve historical bars for the cohorts.

However:

- Post-anchor volume can be future information if used as an ex-ante decision feature.
- `early_5m` / `early_15m` after anchor may only be used as delayed confirmation, not as decision-time evidence.
- Clean decision-time feature work must use:
  - pre-anchor features, or
  - delayed gate semantics, explicitly labeled as such.

### 1.4 AMM-Only Ceiling Is Not the Same as Curve-Entry Ceiling

Replay using first visible GMGN volume price measures an AMM/post-visible policy ceiling, not true curve-entry at decision time.

Meaning:

- AMM-only gold capture looked structurally capped below the 60% target.
- Silver capture is closer but sensitive to signal-to-decision latency.
- True pump.fun curve-entry ceiling is still unmeasured until chain-truth transaction/event decoding supplies decision-time curve prices.

## 2. What Was Implemented Since The Last Expert Round

### 2.1 Research Tools Were Restored And Persisted

Recovered and pushed to research branch:

- raw-dog label cleaning audit
- clean rawdog pack builder
- GMGN touch filter for clean cohorts
- free-source coverage audit
- chain-truth worklist v2 builder
- Tier 1 anchor/peak worklist builder

Commit:

- `2569236a Restore clean raw dog audit tooling`

### 2.2 Exact pump.fun TradeEvent Decoder Added

The curve decoder no longer relies only on transfer matching.

Implemented:

- exact Anchor `TradeEvent` payload decoder from `meta.logMessages`
- support for `Program data: <base64>` logs
- fields decoded:
  - mint
  - sol amount
  - token amount
  - buy/sell side
  - user
  - timestamp
  - virtual SOL reserves
  - virtual token reserves
  - real SOL reserves
  - real token reserves
  - fee / creator fee fields when present
- exact TradeEvent price takes priority over transfer heuristic
- transfer heuristic remains only as fallback
- outputs now separate:
  - `exact_trade_event_n`
  - `transfer_heuristic_trade_n`
  - `infeasible_transfer_price_n`
  - `history_reached_start_n`
  - `history_incomplete_n`

Commit:

- `29cfd790 Add exact pumpfun TradeEvent decoding audit`

### 2.3 Provider-Agnostic Chain Decoder Added

The decoder now supports:

- `--rpc-url`
- `--rpc-mode raw`
- `--transactions-json`

This means Alchemy, Helius, or exported raw transactions can be used. Helius is no longer a single point of failure for offline audit.

### 2.4 Data Room Builder Added

New script:

- `scripts/build-rawdog-chain-truth-data-room.sh`

It builds, offline and read-only:

- raw-dog label cleaning audit
- clean dog/dud pack
- quarantine pack
- optional GMGN-touch filtered cohorts
- free-source coverage audit
- chain-truth worklist v2
- Tier 1 anchor worklist
- Tier 1 peak worklist
- manifest with file hashes

Commit:

- `fc6bbfd8 Add rawdog chain truth data room builder`

### 2.5 Runbook Added

New document:

- `claudedocs/chain-truth-data-room-runbook.md`

It defines:

- Zeabur snapshot export
- local frozen data-room build
- Alchemy smoke
- Tier 1 anchor pass
- Tier 1 peak-window pass
- reading rules

Commit:

- `84c44885 Document chain truth data room runbook`

## 3. What Was Tested

All relevant local tests passed:

- raw-dog label cleaning
- clean-pack builder
- GMGN touch filter
- free-source audit
- chain-truth worklist builder
- Tier worklist builder
- GMGN touch audit
- pump.fun curve decoder

Result:

- 22/22 tests pass
- `git diff --check` passes
- decoder dry-run smoke passes
- data-room builder smoke with toy SQLite DB passes
- data-room builder smoke with toy GMGN touch input passes

Important caveat:

The smoke tests verify code paths and file production. They do not substitute for a real production frozen pack.

## 4. What Is Still Missing

The local machine currently does not have the current production `raw_signal_outcomes.db` snapshot or a current frozen pack.

Therefore, the following has not been run yet:

- real clean pack on current production data
- real chain-truth worklist v2 on current production data
- Alchemy Tier 1 anchor pass
- Alchemy Tier 1 peak-window adjudication
- exact TradeEvent-based curve-entry ceiling
- exact chain-truth adjudication of quarantined label rows

## 5. Next Required Data Step

Export production snapshot from Zeabur:

```bash
cd /app
bash scripts/create-rawdog-audit-snapshot.sh
```

Download:

```text
/app/data/audit-snapshots/rawdog-audit-dbs.tgz
```

Clean up production disk after download:

```bash
rm -rf /app/data/audit-snapshots
```

Build local data room:

```bash
cd /Users/boliu/sas-research

SNAPSHOT_TGZ=~/Downloads/rawdog-audit-dbs.tgz \
OUT_DIR=~/sas-data-room/chain-truth-$(date -u +%Y%m%dT%H%M%SZ) \
bash scripts/build-rawdog-chain-truth-data-room.sh
```

If current GMGN touch results are available:

```bash
SNAPSHOT_TGZ=~/Downloads/rawdog-audit-dbs.tgz \
DOG_TOUCH=~/sas-data-room/gmgn-dog-touch-results.json \
DUD_TOUCH=~/sas-data-room/gmgn-dud-touch-results.json \
OUT_DIR=~/sas-data-room/chain-truth-$(date -u +%Y%m%dT%H%M%SZ) \
bash scripts/build-rawdog-chain-truth-data-room.sh
```

## 6. Next Required Chain-Truth Runs

Smoke:

```bash
node scripts/run-helius-pumpfun-curve-decode-audit.js \
  --rpc-url "$ALCHEMY_RPC_URL" \
  --rpc-mode raw \
  --tokens-file ~/sas-data-room/<pack>/worklists/tiers/tier1-anchor-worklist-v2.txt \
  --out ~/sas-data-room/<pack>/chain-truth-tier1-anchor-smoke.json \
  --checkpoint-out ~/sas-data-room/<pack>/chain-truth-tier1-anchor-smoke.jsonl \
  --limit 2 \
  --pre-sec 90 \
  --post-sec 90 \
  --page-size 100 \
  --max-pages 3 \
  --rpc-tx-delay-ms 100
```

Full Tier 1 anchor:

```bash
node scripts/run-helius-pumpfun-curve-decode-audit.js \
  --rpc-url "$ALCHEMY_RPC_URL" \
  --rpc-mode raw \
  --tokens-file ~/sas-data-room/<pack>/worklists/tiers/tier1-anchor-worklist-v2.txt \
  --out ~/sas-data-room/<pack>/chain-truth-tier1-anchor.json \
  --checkpoint-out ~/sas-data-room/<pack>/chain-truth-tier1-anchor.jsonl \
  --pre-sec 90 \
  --post-sec 90 \
  --page-size 100 \
  --max-pages 3 \
  --rpc-tx-delay-ms 100 \
  --resume
```

Tier 1 peak-window adjudication:

```bash
node scripts/run-helius-pumpfun-curve-decode-audit.js \
  --rpc-url "$ALCHEMY_RPC_URL" \
  --rpc-mode raw \
  --tokens-file ~/sas-data-room/<pack>/worklists/tiers/tier1-peak-worklist-v2.txt \
  --out ~/sas-data-room/<pack>/chain-truth-tier1-peak.json \
  --checkpoint-out ~/sas-data-room/<pack>/chain-truth-tier1-peak.jsonl \
  --pre-sec 180 \
  --post-sec 180 \
  --page-size 100 \
  --max-pages 5 \
  --rpc-tx-delay-ms 100 \
  --resume
```

## 7. What The Next Expert Review Should Answer

After Tier 1 outputs exist, ask experts these exact questions:

1. Label truth:

   > Of the quarantined raw-dog labels, which are confirmed by exact TradeEvent prices and which were unit-contaminated false labels?

2. Curve-entry ceiling:

   > Using exact decision-time curve price, what is the true curve-entry silver/gold capture ceiling?

3. Pre-grad feature viability:

   > In the not-yet-visible / pre-graduation segment, do exact TradeEvent-derived features separate dog from same-bucket dud?

   Candidate features:
   - net SOL flow before decision
   - buy count
   - sell count
   - unique buyers
   - buy/sell imbalance
   - virtual reserve price
   - reserve/progress state if derivable

4. Contract definition:

   > If curve-entry ceiling and pre-grad features are favorable, what should `bonding_curve_final_entry_contract.v1` require?

5. Goal feasibility:

   > After label cleaning and exact curve-entry ceiling, is 60% silver or gold capture mathematically plausible under this signal source?

## 8. What Experts Should Not Do Yet

Experts should not recommend changing live strategy until Tier 1 exact outputs are read.

Do not ask them to:

- lower matrix thresholds
- loosen RR
- widen exit
- change live size
- promote A_CLASS live

Those actions are downstream of chain-truth and would be premature.

## 9. Current One-Line Status

The tooling to rebuild the frozen pack and run exact pump.fun chain truth is now in place and tested. The missing input is a current production raw DB snapshot. Once that snapshot is downloaded, Tier 1 can adjudicate label truth, curve-entry ceiling, and pre-grad feature viability.

