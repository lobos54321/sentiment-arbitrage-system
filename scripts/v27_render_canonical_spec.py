#!/usr/bin/env python3
"""Render deterministic v2.7 canonical spec markdown views.

The repo-local JSON artifacts are the canonical source for the seed spec.
This renderer makes the human-readable views reproducible and lets the
manifest pin their line counts and hashes.
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

PARTS = [
    (
        "Telegram_Dog_Regime_Capture_System_v2.7.0_Canonical_Spec_Part1.md",
        ["S00", "S01", "S02", "S03", "S04", "S05", "S06"],
    ),
    (
        "Telegram_Dog_Regime_Capture_System_v2.7.0_Canonical_Spec_Part2.md",
        ["S07", "S08", "S09", "S10", "S11", "S12", "S13"],
    ),
    (
        "Telegram_Dog_Regime_Capture_System_v2.7.0_Canonical_Spec_Part3.md",
        ["S14", "S15", "S16", "S17", "S18", "S19", "S20", "S21", "S22", "S23"],
    ),
]


def load_json(path):
    with Path(path).open("r", encoding="utf-8") as fh:
        return json.load(fh)


def sha256_text(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def line_count(text):
    return len(text.splitlines())


def contracts_by_section(catalog):
    grouped = {}
    for contract_id, record in sorted((catalog.get("contracts") or {}).items()):
        grouped.setdefault(record.get("section_id"), []).append((contract_id, record))
    return grouped


def gap_batches_by_section(gap_register, catalog):
    grouped = {}
    contracts = catalog.get("contracts") or {}
    for batch in gap_register.get("batches") or []:
        section_map = {}
        for contract_id in batch.get("contract_ids") or []:
            record = contracts.get(contract_id) or {}
            section_id = record.get("section_id") or "UNCATALOGED"
            section_map.setdefault(section_id, []).append(contract_id)
        for section_id, contract_ids in section_map.items():
            grouped.setdefault(section_id, []).append(
                {
                    "batch_id": batch.get("batch_id"),
                    "theme": batch.get("theme"),
                    "contract_ids": sorted(contract_ids),
                }
            )
    return grouped


def render_contract(contract_id, record):
    lines = [
        f"#### {contract_id}",
        "",
        f"- Section: `{record.get('section_id')}`",
        f"- Mode target: `{record.get('mode_target')}`",
        f"- Failure action: `{record.get('failure_action')}`",
        f"- Required fields: `{', '.join(record.get('required_fields') or [])}`",
        "",
    ]
    return lines


def render_part(manifest, catalog, gap_register, part_index, file_name, section_ids):
    sections = {section.get("section_id"): section for section in manifest.get("sections") or []}
    contracts = contracts_by_section(catalog)
    gaps = gap_batches_by_section(gap_register, catalog)
    all_gap_ids = {
        contract_id
        for batch in gap_register.get("batches") or []
        for contract_id in batch.get("contract_ids") or []
    }
    catalog_ids = set((catalog.get("contracts") or {}).keys())
    missing_gap_catalog_records = sorted(all_gap_ids - catalog_ids)

    lines = [
        f"# Telegram Dog Regime Capture System v{manifest.get('spec_version')} Canonical Spec - Part {part_index}",
        "",
        "Generated from repo-local canonical JSON artifacts.",
        "",
        "## Source Artifacts",
        "",
        f"- Manifest: `{MANIFEST_PATH.relative_to(ROOT)}`",
        f"- Contract catalog: `{CATALOG_PATH.relative_to(ROOT)}`",
        f"- Gap register: `{GAP_REGISTER_PATH.relative_to(ROOT)}`",
        f"- Catalog contracts: `{len(catalog_ids)}`",
        f"- Gap register contracts: `{len(all_gap_ids)}`",
        f"- Gap contracts missing catalog records: `{len(missing_gap_catalog_records)}`",
        "",
        "## Release Principle",
        "",
        manifest.get("implementation_principle", ""),
        "",
        "## Next Required Step",
        "",
        manifest.get("next_required_step", ""),
        "",
    ]

    if missing_gap_catalog_records:
        lines.extend(
            [
                "## Uncataloged Gap Contracts",
                "",
                "The following gap-register contracts still lack field-level catalog records.",
                "",
            ]
        )
        lines.extend(f"- `{contract_id}`" for contract_id in missing_gap_catalog_records)
        lines.append("")

    for section_id in section_ids:
        section = sections[section_id]
        section_contracts = contracts.get(section_id, [])
        section_gaps = gaps.get(section_id, [])
        lines.extend(
            [
                f"## {section_id} - {section.get('title')}",
                "",
                f"- Section mode target: `{section.get('mode_target')}`",
                f"- Catalog contract count: `{len(section_contracts)}`",
                f"- Gap batch count: `{len(section_gaps)}`",
                "",
                "### Catalog Contracts",
                "",
            ]
        )
        if section_contracts:
            for contract_id, record in section_contracts:
                lines.extend(render_contract(contract_id, record))
        else:
            lines.extend(["No catalog contracts currently target this section.", ""])

        lines.extend(["### Gap Register Coverage", ""])
        if section_gaps:
            for batch in section_gaps:
                lines.extend(
                    [
                        f"#### {batch.get('batch_id')}",
                        "",
                        f"- Theme: {batch.get('theme')}",
                        f"- Contracts: `{', '.join(batch.get('contract_ids') or [])}`",
                        "",
                    ]
                )
        else:
            lines.extend(["No gap-register contracts currently target this section.", ""])

    return "\n".join(lines).rstrip() + "\n"


def render_all(manifest, catalog, gap_register):
    rendered = []
    for idx, (file_name, section_ids) in enumerate(PARTS, start=1):
        text = render_part(manifest, catalog, gap_register, idx, file_name, section_ids)
        rendered.append(
            {
                "file": file_name,
                "sections": section_ids,
                "text": text,
                "lines": line_count(text),
                "sha256": sha256_text(text),
            }
        )
    return rendered


def write_outputs(manifest_path=MANIFEST_PATH, catalog_path=CATALOG_PATH, gap_register_path=GAP_REGISTER_PATH):
    manifest_path = Path(manifest_path)
    spec_dir = manifest_path.parent
    manifest = load_json(manifest_path)
    catalog = load_json(catalog_path)
    gap_register = load_json(gap_register_path)
    rendered = render_all(manifest, catalog, gap_register)

    for item in rendered:
        (spec_dir / item["file"]).write_text(item["text"], encoding="utf-8")

    manifest["rendered_views"] = [
        {
            "file": item["file"],
            "lines": item["lines"],
            "sha256": item["sha256"],
            "sections": item["sections"],
        }
        for item in rendered
    ]
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return manifest["rendered_views"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", default=str(MANIFEST_PATH))
    parser.add_argument("--catalog", default=str(CATALOG_PATH))
    parser.add_argument("--gap-register", default=str(GAP_REGISTER_PATH))
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()

    if args.write:
        print(json.dumps(write_outputs(args.manifest, args.catalog, args.gap_register), indent=2, ensure_ascii=False))
        return

    manifest = load_json(args.manifest)
    catalog = load_json(args.catalog)
    gap_register = load_json(args.gap_register)
    rendered = render_all(manifest, catalog, gap_register)
    print(
        json.dumps(
            [
                {
                    "file": item["file"],
                    "lines": item["lines"],
                    "sha256": item["sha256"],
                    "sections": item["sections"],
                }
                for item in rendered
            ],
            indent=2,
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
