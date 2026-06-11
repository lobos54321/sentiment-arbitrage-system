# Raw Dog Chain-Truth Data Room Runbook

Status: research-only / no production writes.

Purpose: rebuild the frozen raw-dog evidence pack, clean label pack, chain-truth worklists, and Tier 1 inputs from a durable production snapshot. Do not use `/tmp` as the final location.

## 1. Export DB Snapshot From Zeabur

Run this in the Zeabur shell. It uses SQLite Online Backup and does not restart services.

```bash
cd /app
bash scripts/create-rawdog-audit-snapshot.sh
```

Download:

```text
/app/data/audit-snapshots/rawdog-audit-dbs.tgz
```

After the download completes:

```bash
rm -rf /app/data/audit-snapshots
```

Do not leave the snapshot on the production disk.

## 2. Build Local Durable Data Room

On the local machine:

```bash
cd /Users/boliu/sas-research

SNAPSHOT_TGZ=~/Downloads/rawdog-audit-dbs.tgz \
OUT_DIR=~/sas-data-room/chain-truth-$(date -u +%Y%m%dT%H%M%SZ) \
bash scripts/build-rawdog-chain-truth-data-room.sh
```

If GMGN touch results are available, include them:

```bash
SNAPSHOT_TGZ=~/Downloads/rawdog-audit-dbs.tgz \
DOG_TOUCH=~/sas-data-room/gmgn-dog-touch-results.json \
DUD_TOUCH=~/sas-data-room/gmgn-dud-touch-results.json \
OUT_DIR=~/sas-data-room/chain-truth-$(date -u +%Y%m%dT%H%M%SZ) \
bash scripts/build-rawdog-chain-truth-data-room.sh
```

Outputs include:

- `audits/raw-dog-label-cleaning.json`
- `clean-pack/clean-dogs.json`
- `clean-pack/clean-duds.json`
- `clean-pack/quarantine-rows.json`
- `worklists/chain-truth-worklist-v2.txt`
- `worklists/tiers/tier1-anchor-worklist-v2.txt`
- `worklists/tiers/tier1-peak-worklist-v2.txt`
- `manifest.json` with file hashes

## 3. Run Tier 1 With Alchemy

Alchemy key should stay outside the repo. The local file may contain either a full RPC URL or a shell assignment.

```bash
source ~/.alchemy_rpc
```

If the file is just a raw URL:

```bash
export ALCHEMY_RPC_URL="$(cat ~/.alchemy_rpc)"
```

Smoke first:

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

Then run the full Tier 1 anchor pass:

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

Peak-window adjudication:

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

## 4. Reading Rules

- `history_reached_start=false` means the fetch window is incomplete. Do not interpret missing trades as no trades.
- Prefer `exact_trade_event_n` over transfer heuristic.
- Any transfer-derived price with `price_feasible=false` cannot be used for peak adjudication.
- Do not use live rolling endpoints as the audit baseline.
- Do not change gate, matrix, RR, exit, or live size from these outputs alone.

