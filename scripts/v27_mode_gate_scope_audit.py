#!/usr/bin/env python3
"""CLI for auditing v2.7 mode-gate scope against the final catalog."""

import argparse
import json
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from v27_mode_gate_scope import build_mode_gate_scope_audit  # noqa: E402
from v27_mode_readiness import CATALOG_PATH, MODE_ORDER, MODE_REQUIREMENTS  # noqa: E402
from v27_spec_validate import load_json  # noqa: E402


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default=str(CATALOG_PATH))
    parser.add_argument("--strict-final-normal-tiny", action="store_true")
    args = parser.parse_args()

    audit = build_mode_gate_scope_audit(load_json(args.catalog), MODE_REQUIREMENTS, MODE_ORDER)
    print(json.dumps(audit, ensure_ascii=False, sort_keys=True, indent=2))
    if args.strict_final_normal_tiny and not audit["health"]["final_normal_tiny_blocking_scope_complete"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
