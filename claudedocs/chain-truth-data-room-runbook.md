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

If the same Zeabur archive includes `paper_decision_subset.db`, build the v10
decision-anchor pack directly from the downloaded archive:

```bash
cd /Users/boliu/sas-research

node scripts/build-v10-decision-anchor-pack.js \
  --snapshot-tgz ~/Downloads/rawdog-audit-dbs.tgz \
  --out-dir ~/sas-data-room/<pack>/v10-decision-anchor-pack
```

The decision pack builder imports only the subset DB from the archive, runs the
v10 clean-cohort funnel, and emits `review_required` if the subset does not
cover the full v10 window.

## 3. Run Tier 1 With Alchemy

Alchemy key should stay outside the repo. The local file may contain either a full RPC URL or a shell assignment.

```bash
cat > ~/.alchemy_rpc <<'EOF'
ALCHEMY_RPC_URL=https://...
EOF
chmod 600 ~/.alchemy_rpc
```

Validate the generated worklists without touching RPC:

```bash
DATA_ROOM_DIR=~/sas-data-room/<pack> \
MODE=smoke \
DRY_RUN=1 \
bash scripts/run-chain-truth-tier1-from-data-room.sh
```

Run a real two-token smoke:

```bash
DATA_ROOM_DIR=~/sas-data-room/<pack> \
MODE=smoke \
ALCHEMY_RPC_FILE=~/.alchemy_rpc \
bash scripts/run-chain-truth-tier1-from-data-room.sh
```

Then run the full Tier 1 anchor pass:

```bash
DATA_ROOM_DIR=~/sas-data-room/<pack> \
MODE=anchor \
ALCHEMY_RPC_FILE=~/.alchemy_rpc \
bash scripts/run-chain-truth-tier1-from-data-room.sh
```

Peak-window adjudication:

```bash
DATA_ROOM_DIR=~/sas-data-room/<pack> \
MODE=peak \
ALCHEMY_RPC_FILE=~/.alchemy_rpc \
bash scripts/run-chain-truth-tier1-from-data-room.sh
```

To run both anchor and peak passes:

```bash
DATA_ROOM_DIR=~/sas-data-room/<pack> \
MODE=all \
ALCHEMY_RPC_FILE=~/.alchemy_rpc \
bash scripts/run-chain-truth-tier1-from-data-room.sh
```

## 4. Reading Rules

- `history_reached_start=false` means the fetch window is incomplete. Do not interpret missing trades as no trades.
- Prefer `exact_trade_event_n` over transfer heuristic.
- Any transfer-derived price with `price_feasible=false` cannot be used for peak adjudication.
- Do not use live rolling endpoints as the audit baseline.
- Do not change gate, matrix, RR, exit, or live size from these outputs alone.
