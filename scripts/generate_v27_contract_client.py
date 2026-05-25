#!/usr/bin/env python3
"""Generate the deterministic v2.7 contract catalog client artifact."""

import argparse
import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "spec" / "telegram_dog_regime_capture" / "v2.7.0" / "contract-catalog.json"
OUTPUT_PATH = ROOT / "spec" / "telegram_dog_regime_capture" / "v2.7.0" / "generated" / "v27-contract-client.json"
GENERATION_TOOL_VERSION = "v2.7.0.contract_client_generator.v1"
CHECKED_AT = "2026-05-26T00:00:00Z"


def canonical_json_bytes(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def sha256_json(value):
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def load_json(path):
    with Path(path).open("r", encoding="utf-8") as fh:
        return json.load(fh)


def build_client(catalog):
    contracts = catalog.get("contracts") if isinstance(catalog, dict) else {}
    rows = []
    for contract_id in sorted(contracts):
        record = contracts[contract_id]
        rows.append(
            {
                "contract_id": contract_id,
                "section_id": record.get("section_id"),
                "mode_target": record.get("mode_target"),
                "required_fields": list(record.get("required_fields") or []),
                "failure_action": record.get("failure_action"),
            }
        )
    client = {
        "client_name": "v27_contract_catalog_client",
        "generation_tool_version": GENERATION_TOOL_VERSION,
        "checked_at": CHECKED_AT,
        "source_schema_file": "spec/telegram_dog_regime_capture/v2.7.0/contract-catalog.json",
        "source_schema_hash": sha256_json(catalog),
        "contract_count": len(rows),
        "contracts": rows,
    }
    client["generated_artifact_hash"] = sha256_json(
        {key: value for key, value in client.items() if key != "generated_artifact_hash"}
    )
    return client


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default=str(CATALOG_PATH))
    parser.add_argument("--output", default=str(OUTPUT_PATH))
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    catalog = load_json(args.catalog)
    client = build_client(catalog)
    output_path = Path(args.output)
    if args.check:
        existing = load_json(output_path)
        if existing != client:
            raise SystemExit(f"generated client stale: {output_path}")
        print(json.dumps({"status": "ok", "generated_artifact_hash": client["generated_artifact_hash"]}, sort_keys=True))
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(client, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": "written", "output": str(output_path), "generated_artifact_hash": client["generated_artifact_hash"]}, sort_keys=True))


if __name__ == "__main__":
    main()
