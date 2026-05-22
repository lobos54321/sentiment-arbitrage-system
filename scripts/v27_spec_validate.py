#!/usr/bin/env python3
"""Validate the v2.7 Telegram Dog Regime Capture seed spec.

The current v2.7 spec is still a seed, but it must already be machine-checkable:
section ids must be stable, every MVP/high-risk contract must have a catalog
record, and the M0 frozen direct-entry modes must be disabled in the live
registry.
"""

import argparse
import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC_DIR = ROOT / "spec" / "telegram_dog_regime_capture" / "v2.7.0"
MANIFEST_PATH = SPEC_DIR / "spec.manifest.json"
CATALOG_PATH = SPEC_DIR / "contract-catalog.json"
GAP_REGISTER_PATH = SPEC_DIR / "gap-register.json"
ENTRY_MODE_REGISTRY_PATH = ROOT / "config" / "entry-mode-registry.json"


class SpecValidationError(AssertionError):
    pass


def load_json(path):
    with Path(path).open("r", encoding="utf-8") as fh:
        return json.load(fh)


def canonical_json_bytes(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def compute_spec_hash(manifest, catalog, gap_register=None):
    manifest_for_hash = dict(manifest)
    manifest_for_hash.pop("computed_spec_hash", None)
    payload = {
        "manifest": manifest_for_hash,
        "contract_catalog": catalog,
    }
    if gap_register is not None:
        payload["gap_register"] = gap_register
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def validate_manifest(manifest):
    if manifest.get("spec_id") != "telegram_dog_regime_capture":
        raise SpecValidationError("spec_id mismatch")
    if manifest.get("spec_version") != "2.7.0":
        raise SpecValidationError("spec_version mismatch")
    if manifest.get("contract_catalog_file") != "contract-catalog.json":
        raise SpecValidationError("contract catalog file must be pinned")
    if manifest.get("gap_register_file") != "gap-register.json":
        raise SpecValidationError("gap register file must be pinned")

    sections = manifest.get("sections") or []
    expected = [f"S{idx:02d}" for idx in range(24)]
    actual = [section.get("section_id") for section in sections]
    if actual != expected:
        raise SpecValidationError(f"section ids not contiguous: {actual}")

    for view in manifest.get("rendered_views") or []:
        if len(str(view.get("sha256", ""))) != 64:
            raise SpecValidationError(f"rendered view hash invalid: {view.get('file')}")
        if int(view.get("lines") or 0) <= 0:
            raise SpecValidationError(f"rendered view lines invalid: {view.get('file')}")


def required_contract_ids(manifest):
    return set(manifest.get("mvp_blocking_contracts") or []) | set(
        manifest.get("high_risk_carry_forward_contracts") or []
    )


def validate_contract_catalog(manifest, catalog):
    required_record_fields = set(catalog.get("contract_record_required_fields") or [])
    if required_record_fields != {
        "contract_id",
        "section_id",
        "mode_target",
        "required_fields",
        "failure_action",
    }:
        raise SpecValidationError("contract record schema is incomplete")

    section_ids = {section["section_id"] for section in manifest["sections"]}
    contracts = catalog.get("contracts") or {}
    missing = sorted(required_contract_ids(manifest) - set(contracts))
    if missing:
        raise SpecValidationError(f"missing contract catalog records: {missing}")

    for contract_id, record in contracts.items():
        if record.get("section_id") not in section_ids:
            raise SpecValidationError(f"{contract_id} has invalid section_id")
        if not record.get("mode_target"):
            raise SpecValidationError(f"{contract_id} missing mode_target")
        required_fields = record.get("required_fields")
        if not isinstance(required_fields, list) or len(required_fields) < 3:
            raise SpecValidationError(f"{contract_id} must define at least three required fields")
        if len(required_fields) != len(set(required_fields)):
            raise SpecValidationError(f"{contract_id} has duplicate required fields")
        if not record.get("failure_action"):
            raise SpecValidationError(f"{contract_id} missing failure_action")


def gap_contract_ids(gap_register):
    ids = []
    for batch in gap_register.get("batches") or []:
        ids.extend(batch.get("contract_ids") or [])
    return ids


def validate_gap_register(manifest, catalog, gap_register):
    if gap_register.get("gap_register_schema_version") != 1:
        raise SpecValidationError("gap register schema version mismatch")
    if gap_register.get("status") != "machine_checkable_adversarial_gap_register":
        raise SpecValidationError("gap register status invalid")
    if not gap_register.get("coverage_rule"):
        raise SpecValidationError("gap register coverage rule missing")

    batches = gap_register.get("batches") or []
    if not batches:
        raise SpecValidationError("gap register has no batches")

    seen_batches = set()
    ids = []
    for batch in batches:
        batch_id = batch.get("batch_id")
        if not batch_id or batch_id in seen_batches:
            raise SpecValidationError(f"gap register batch invalid: {batch_id}")
        seen_batches.add(batch_id)
        contract_ids = batch.get("contract_ids")
        if not isinstance(contract_ids, list) or not contract_ids:
            raise SpecValidationError(f"gap register batch has no contracts: {batch_id}")
        for contract_id in contract_ids:
            if not isinstance(contract_id, str) or not contract_id:
                raise SpecValidationError(f"gap register contract id invalid in {batch_id}")
        ids.extend(contract_ids)

    duplicates = sorted({contract_id for contract_id in ids if ids.count(contract_id) > 1})
    if duplicates:
        raise SpecValidationError(f"gap register duplicates: {duplicates}")

    catalog_contracts = set((catalog.get("contracts") or {}).keys())
    required = required_contract_ids(manifest)
    missing_required = sorted((required - catalog_contracts) - set(ids))
    if missing_required:
        raise SpecValidationError(f"required contracts missing from catalog and gap register: {missing_required}")


def validate_m0_freeze(manifest, registry):
    modes = registry.get("modes") or {}
    for mode in manifest.get("m0_freeze_modes") or []:
        entry = modes.get(mode)
        if not isinstance(entry, dict):
            raise SpecValidationError(f"M0 freeze mode not registered: {mode}")
        if entry.get("paper_enabled") is not False:
            raise SpecValidationError(f"M0 freeze mode still paper-enabled: {mode}")
        if entry.get("tier") == "live":
            raise SpecValidationError(f"M0 freeze mode still live tier: {mode}")


def validate_all(
    manifest_path=MANIFEST_PATH,
    catalog_path=CATALOG_PATH,
    registry_path=ENTRY_MODE_REGISTRY_PATH,
    gap_register_path=None,
):
    manifest = load_json(manifest_path)
    catalog = load_json(catalog_path)
    gap_register_path = gap_register_path or Path(manifest_path).parent / manifest.get("gap_register_file", "gap-register.json")
    gap_register = load_json(gap_register_path)
    registry = load_json(registry_path)

    validate_manifest(manifest)
    validate_contract_catalog(manifest, catalog)
    validate_gap_register(manifest, catalog, gap_register)
    validate_m0_freeze(manifest, registry)

    return {
        "spec_id": manifest["spec_id"],
        "spec_version": manifest["spec_version"],
        "spec_hash": compute_spec_hash(manifest, catalog, gap_register),
        "section_count": len(manifest["sections"]),
        "required_contract_count": len(required_contract_ids(manifest)),
        "catalog_contract_count": len(catalog.get("contracts") or {}),
        "gap_register_count": len(gap_contract_ids(gap_register)),
        "m0_freeze_modes": manifest.get("m0_freeze_modes") or [],
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", default=str(MANIFEST_PATH))
    parser.add_argument("--catalog", default=str(CATALOG_PATH))
    parser.add_argument("--gap-register")
    parser.add_argument("--registry", default=str(ENTRY_MODE_REGISTRY_PATH))
    args = parser.parse_args()

    result = validate_all(args.manifest, args.catalog, args.registry, gap_register_path=args.gap_register)
    print(json.dumps(result, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
