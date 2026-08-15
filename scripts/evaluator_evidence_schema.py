#!/usr/bin/env python3
"""Shared declarative numeric-evidence schema for evaluator manifests."""

from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
import re
from typing import Any


EVIDENCE_SCHEMA_PATH = (
    Path(__file__).resolve().parents[1]
    / "config"
    / "evaluator-snapshot-evidence-schema.json"
)
EVIDENCE_SCHEMA_VERSION_FIELD = "numeric_evidence_schema_version"
EVIDENCE_SCHEMA_SHA256_FIELD = "numeric_evidence_schema_sha256"
EVIDENCE_SCHEMA_VALIDATED_FIELD = (
    "numeric_evidence_schema_validated_before_publish"
)
JSON_SAFE_INTEGER_MAX = 2**53 - 1


def canonical_json(value: object) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _finite_schema_bound(value: object, *, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RuntimeError(f"evaluator evidence schema {field} must be numeric")
    numeric = float(value)
    if not math.isfinite(numeric):
        raise RuntimeError(f"evaluator evidence schema {field} must be finite")
    return numeric


def _validate_selectors(rule: dict[str, Any], *, category: str) -> None:
    selector_count = 0
    if "suffixes" in rule:
        raise RuntimeError(
            f"evaluator evidence schema {category}.{rule.get('id')} suffix selectors forbidden"
        )
    for field in ("path_patterns", "fields", "parent_path_patterns"):
        values = rule.get(field)
        if values is None:
            continue
        if (
            not isinstance(values, list)
            or not values
            or any(not isinstance(item, str) or not item for item in values)
        ):
            raise RuntimeError(
                f"evaluator evidence schema {category}.{rule.get('id')}.{field} invalid"
            )
        if field != "parent_path_patterns":
            selector_count += len(values)
    fields = rule.get("fields")
    parent_patterns = rule.get("parent_path_patterns")
    if fields is not None and parent_patterns is None:
        raise RuntimeError(
            f"evaluator evidence schema {category}.{rule.get('id')} "
            "field selectors require parent path patterns"
        )
    if parent_patterns is not None and fields is None:
        raise RuntimeError(
            f"evaluator evidence schema {category}.{rule.get('id')} "
            "parent path patterns require field selectors"
        )
    if isinstance(parent_patterns, list) and "*" in parent_patterns:
        raise RuntimeError(
            f"evaluator evidence schema {category}.{rule.get('id')} "
            "unbounded parent path forbidden"
        )
    if selector_count == 0:
        raise RuntimeError(
            f"evaluator evidence schema {category}.{rule.get('id')} has no selector"
        )


def _load_schema() -> dict[str, Any]:
    try:
        payload = json.loads(
            EVIDENCE_SCHEMA_PATH.read_text(encoding="utf-8"),
            parse_constant=lambda value: (_ for _ in ()).throw(
                ValueError(f"non-finite JSON constant: {value}")
            ),
        )
    except Exception as exc:
        raise RuntimeError(
            f"evaluator evidence schema unreadable: {type(exc).__name__}:{exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise RuntimeError("evaluator evidence schema root must be an object")
    if payload.get("schema_version") != "evaluator_snapshot_numeric_evidence.v3":
        raise RuntimeError("evaluator evidence schema version invalid")
    if payload.get("unknown_numeric_policy") != "reject":
        raise RuntimeError("evaluator evidence schema must reject unknown numerics")
    if payload.get("selector_policy") != (
        "explicit_path_or_field_with_parent_path_only"
    ):
        raise RuntimeError("evaluator evidence schema selector policy invalid")
    if payload.get("path_syntax") != (
        "dot_segments_with_single_segment_wildcards_array_suffixes_and_root_dollar"
    ):
        raise RuntimeError("evaluator evidence schema path syntax invalid")

    seen_ids: set[str] = set()
    for category in ("container_rules", "scalar_rules"):
        rules = payload.get(category)
        if not isinstance(rules, list) or not rules:
            raise RuntimeError(f"evaluator evidence schema {category} invalid")
        for rule in rules:
            if not isinstance(rule, dict):
                raise RuntimeError(f"evaluator evidence schema {category} rule invalid")
            rule_id = rule.get("id")
            if not isinstance(rule_id, str) or not rule_id or rule_id in seen_ids:
                raise RuntimeError(f"evaluator evidence schema rule id invalid: {rule_id}")
            seen_ids.add(rule_id)
            _validate_selectors(rule, category=category)
            if category == "container_rules":
                if rule.get("container_type") not in {"object", "array"}:
                    raise RuntimeError(
                        f"evaluator evidence schema container type invalid: {rule_id}"
                    )
                element_kind = rule.get("element_kind")
                if element_kind is not None and element_kind not in {
                    "safe_integer",
                    "finite_number",
                }:
                    raise RuntimeError(
                        f"evaluator evidence schema element kind invalid: {rule_id}"
                    )
                if element_kind is not None and not isinstance(
                    rule.get("element_nullable"), bool
                ):
                    raise RuntimeError(
                        f"evaluator evidence schema element nullability invalid: {rule_id}"
                    )
                bound_prefix = "element_"
            else:
                if rule.get("kind") not in {
                    "safe_integer",
                    "finite_number",
                    "safe_integer_or_decimal_identifier",
                    "safe_integer_or_iso8601_timestamp",
                    "finite_number_or_iso8601_timestamp",
                    "finite_number_or_evidence_timestamp",
                }:
                    raise RuntimeError(
                        f"evaluator evidence schema scalar kind invalid: {rule_id}"
                    )
                if not isinstance(rule.get("nullable"), bool):
                    raise RuntimeError(
                        f"evaluator evidence schema nullability invalid: {rule_id}"
                    )
                null_policy = rule.get("null_policy")
                if null_policy not in {
                    None,
                    "watermark_time_set_may_be_null_only_when_empty_or_alternate_present",
                    "watermark_table_may_be_null_only_when_rows_copied_zero",
                }:
                    raise RuntimeError(
                        f"evaluator evidence schema null policy invalid: {rule_id}"
                    )
                if null_policy is not None and rule.get("nullable") is not True:
                    raise RuntimeError(
                        f"evaluator evidence schema null policy requires nullable rule: {rule_id}"
                    )
                bound_prefix = ""
            minimum = rule.get(f"{bound_prefix}minimum")
            maximum = rule.get(f"{bound_prefix}maximum")
            if minimum is not None:
                minimum = _finite_schema_bound(
                    minimum,
                    field=f"{rule_id}.{bound_prefix}minimum",
                )
            if maximum is not None:
                maximum = _finite_schema_bound(
                    maximum,
                    field=f"{rule_id}.{bound_prefix}maximum",
                )
            if minimum is not None and maximum is not None and minimum > maximum:
                raise RuntimeError(
                    f"evaluator evidence schema bounds inverted: {rule_id}"
                )
    return payload


EVIDENCE_SCHEMA = _load_schema()
EVIDENCE_SCHEMA_VERSION = str(EVIDENCE_SCHEMA["schema_version"])
EVIDENCE_SCHEMA_SHA256 = hashlib.sha256(
    canonical_json(EVIDENCE_SCHEMA).encode("utf-8")
).hexdigest()
EVIDENCE_SCHEMA_FIELD_NAMES = frozenset(
    field
    for category in ("container_rules", "scalar_rules")
    for rule in EVIDENCE_SCHEMA[category]
    for field in rule.get("fields", [])
)


def is_json_safe_integer(value: object) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return abs(value) <= JSON_SAFE_INTEGER_MAX
    return bool(
        isinstance(value, float)
        and math.isfinite(value)
        and value.is_integer()
        and abs(value) <= JSON_SAFE_INTEGER_MAX
    )


def is_json_finite_number(value: object) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        numeric = float(value)
        return math.isfinite(numeric) and abs(numeric) <= JSON_SAFE_INTEGER_MAX
    except (OverflowError, TypeError, ValueError):
        return False


ISO8601_TIMESTAMP_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"
    r"(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})$"
)
SQLITE_UTC_TIMESTAMP_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{1,6})?$"
)
DECIMAL_IDENTIFIER_RE = re.compile(r"^(?:0|[1-9]\d{0,15})$")


def is_iso8601_timestamp(value: object) -> bool:
    if not isinstance(value, str) or not ISO8601_TIMESTAMP_RE.fullmatch(value):
        return False
    try:
        normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
        parsed = datetime.fromisoformat(normalized)
        timestamp = parsed.timestamp()
    except (OverflowError, TypeError, ValueError):
        return False
    return bool(
        parsed.tzinfo is not None
        and math.isfinite(timestamp)
        and timestamp >= 0
    )


def is_sqlite_utc_timestamp(value: object) -> bool:
    if not isinstance(value, str) or not SQLITE_UTC_TIMESTAMP_RE.fullmatch(value):
        return False
    try:
        parsed = datetime.fromisoformat(value).replace(tzinfo=timezone.utc)
        timestamp = parsed.timestamp()
    except (OverflowError, TypeError, ValueError):
        return False
    return bool(math.isfinite(timestamp) and timestamp >= 0)


def is_evidence_timestamp(value: object) -> bool:
    return is_iso8601_timestamp(value) or is_sqlite_utc_timestamp(value)


def is_decimal_identifier(value: object) -> bool:
    if not isinstance(value, str) or not DECIMAL_IDENTIFIER_RE.fullmatch(value):
        return False
    try:
        return int(value) <= JSON_SAFE_INTEGER_MAX
    except (TypeError, ValueError):
        return False


def _path_matches(pattern: str, path: str) -> bool:
    if pattern == "$":
        return path == ""
    expected = pattern.split(".") if pattern else []
    actual = path.split(".") if path else []
    return len(expected) == len(actual) and all(
        wanted == "*" or wanted == observed
        for wanted, observed in zip(expected, actual)
    )


def _rule_matches(rule: dict[str, Any], *, path: str, field: str) -> bool:
    parent_path = path.rsplit(".", 1)[0] if "." in path else ""
    return bool(
        any(
            _path_matches(pattern, path)
            for pattern in rule.get("path_patterns", [])
        )
        or (
            field in rule.get("fields", [])
            and any(
                _path_matches(pattern, parent_path)
                for pattern in rule.get("parent_path_patterns", [])
            )
        )
    )


def numeric_evidence_rule(
    path: str,
    field: str | None = None,
) -> tuple[str, dict[str, Any]] | None:
    """Return the first declared container/scalar rule for a manifest path."""

    path_leaf = path.rsplit(".", 1)[-1]
    if field is not None and str(field) != path_leaf:
        return None
    leaf = str(path_leaf)
    for rule in EVIDENCE_SCHEMA["container_rules"]:
        if _rule_matches(rule, path=path, field=leaf):
            return "container", rule
    for rule in EVIDENCE_SCHEMA["scalar_rules"]:
        if _rule_matches(rule, path=path, field=leaf):
            return "scalar", rule
    return None


def _validate_number(
    value: object,
    *,
    kind: str,
    nullable: bool,
    minimum: object = None,
    maximum: object = None,
) -> str | None:
    if value is None:
        return None if nullable else "null_not_allowed"
    if kind.endswith("_or_iso8601_timestamp") and is_iso8601_timestamp(value):
        return None
    if kind.endswith("_or_evidence_timestamp") and is_evidence_timestamp(value):
        return None
    if kind == "safe_integer_or_decimal_identifier":
        if is_decimal_identifier(value):
            numeric: int | float = int(value)
        elif is_json_safe_integer(value):
            numeric = int(value)
        else:
            return "expected_json_safe_integer_or_decimal_identifier"
    elif kind.startswith("safe_integer"):
        if not is_json_safe_integer(value):
            return "expected_json_safe_integer"
        numeric = int(value)
    else:
        if not is_json_finite_number(value):
            return "expected_finite_json_number"
        numeric = float(value)
    if minimum is not None and numeric < float(minimum):
        return "below_minimum"
    if maximum is not None and numeric > float(maximum):
        return "above_maximum"
    return None


def _conditional_null_allowed(
    payload: object,
    path: str,
    rule: dict[str, Any],
) -> bool:
    if rule.get("null_policy") is None:
        return True
    segments = path.split(".")
    if (
        len(segments) != 5
        or segments[0] != "databases"
        or segments[2] not in {"source_upper_watermarks", "upper_watermarks"}
        or not isinstance(payload, dict)
    ):
        return False
    databases = payload.get("databases")
    if not isinstance(databases, dict):
        return False
    database = databases.get(segments[1])
    if not isinstance(database, dict):
        return False
    selected_tables = database.get("selected_tables")
    if not isinstance(selected_tables, dict):
        return False
    selected = selected_tables.get(segments[3])
    if not isinstance(selected, dict):
        return False
    rows_copied = selected.get("rows_copied")
    table_empty = bool(
        is_json_safe_integer(rows_copied) and int(rows_copied) == 0
    )
    if (
        rule.get("null_policy")
        == "watermark_table_may_be_null_only_when_rows_copied_zero"
    ):
        return table_empty
    time_columns = selected.get("time_columns")
    if not isinstance(time_columns, list) or not all(
        isinstance(item, str) and item for item in time_columns
    ):
        selected_time_column = selected.get("time_column") or selected.get(
            "indexed_time_anchor"
        )
        time_columns = [selected_time_column] if selected_time_column else []
    if segments[4] not in time_columns:
        return True
    if table_empty:
        return True
    watermark_copies = database.get(segments[2])
    if not isinstance(watermark_copies, dict):
        return False
    table_watermarks = watermark_copies.get(segments[3])
    if not isinstance(table_watermarks, dict):
        return False
    return any(
        candidate != segments[4]
        and (
            is_json_finite_number(table_watermarks.get(candidate))
            or is_evidence_timestamp(table_watermarks.get(candidate))
        )
        for candidate in time_columns
    )


def validate_numeric_evidence_value(
    payload: object,
    path: str,
    value: object,
) -> dict[str, Any]:
    """Validate one declared leaf without rescanning the full manifest."""

    matched = numeric_evidence_rule(path)
    element = False
    if matched is None:
        parent_path = path[:-2] if path.endswith("[]") else path.rsplit(".", 1)[0]
        parent = numeric_evidence_rule(parent_path)
        if parent and parent[0] == "container" and parent[1].get("element_kind"):
            matched = parent
            element = True
    if matched is None or matched[0] not in {"scalar", "container"}:
        return {
            "accepted": False,
            "path": path,
            "code": "undeclared_numeric_evidence",
            "rule_id": None,
        }
    rule = matched[1]
    prefix = "element_" if element else ""
    if value is None and not _conditional_null_allowed(payload, path, rule):
        code = "null_condition_failed"
    else:
        code = _validate_number(
            value,
            kind=str(rule[f"{prefix}kind"]),
            nullable=bool(rule[f"{prefix}nullable"]),
            minimum=rule.get(f"{prefix}minimum"),
            maximum=rule.get(f"{prefix}maximum"),
        )
    return {
        "accepted": code is None,
        "path": path,
        "code": code,
        "rule_id": str(rule["id"]),
    }


def numeric_evidence_schema_binding_valid(payload: object) -> bool:
    return bool(
        isinstance(payload, dict)
        and payload.get(EVIDENCE_SCHEMA_VERSION_FIELD) == EVIDENCE_SCHEMA_VERSION
        and payload.get(EVIDENCE_SCHEMA_SHA256_FIELD) == EVIDENCE_SCHEMA_SHA256
        and payload.get(EVIDENCE_SCHEMA_VALIDATED_FIELD) is True
    )


def bind_numeric_evidence_schema(payload: dict[str, Any]) -> None:
    payload[EVIDENCE_SCHEMA_VERSION_FIELD] = EVIDENCE_SCHEMA_VERSION
    payload[EVIDENCE_SCHEMA_SHA256_FIELD] = EVIDENCE_SCHEMA_SHA256
    payload[EVIDENCE_SCHEMA_VALIDATED_FIELD] = True


def validate_numeric_evidence_schema(
    payload: object,
    *,
    require_binding: bool = False,
    max_errors: int = 256,
) -> dict[str, Any]:
    """Validate every declared numeric slot and reject undeclared numerics."""

    errors: list[dict[str, str]] = []
    error_count = 0
    numeric_leaf_count = 0
    declared_numeric_leaf_count = 0
    rule_match_counts: dict[str, int] = {}

    def reject(path: str, code: str, rule_id: str | None = None) -> None:
        nonlocal error_count
        error_count += 1
        if len(errors) < max(1, int(max_errors)):
            error = {"path": path or "$", "code": code}
            if rule_id:
                error["rule_id"] = rule_id
            errors.append(error)

    def validate_declared(
        value: object,
        *,
        path: str,
        rule: dict[str, Any],
        element: bool = False,
    ) -> None:
        nonlocal declared_numeric_leaf_count
        prefix = "element_" if element else ""
        code = _validate_number(
            value,
            kind=str(rule[f"{prefix}kind"]),
            nullable=bool(rule[f"{prefix}nullable"]),
            minimum=rule.get(f"{prefix}minimum"),
            maximum=rule.get(f"{prefix}maximum"),
        )
        rule_id = str(rule["id"])
        rule_match_counts[rule_id] = rule_match_counts.get(rule_id, 0) + 1
        if value is None and not _conditional_null_allowed(payload, path, rule):
            reject(path, "null_condition_failed", rule_id)
            return
        if value is not None and (
            is_json_safe_integer(value) or is_json_finite_number(value)
        ):
            declared_numeric_leaf_count += 1
        if code:
            reject(path, code, rule_id)

    def visit(value: object, path: str) -> None:
        nonlocal numeric_leaf_count
        if isinstance(value, dict):
            for field, child in value.items():
                child_path = f"{path}.{field}" if path else str(field)
                matched = numeric_evidence_rule(child_path, str(field))
                if matched and matched[0] == "container":
                    rule = matched[1]
                    expected_type = rule["container_type"]
                    shape_valid = (
                        isinstance(child, dict)
                        if expected_type == "object"
                        else isinstance(child, list)
                    )
                    if not shape_valid:
                        reject(
                            child_path,
                            f"expected_{expected_type}",
                            str(rule["id"]),
                        )
                        continue
                    rule_id = str(rule["id"])
                    rule_match_counts[rule_id] = rule_match_counts.get(rule_id, 0) + 1
                    element_kind = rule.get("element_kind")
                    if element_kind is None:
                        visit(child, child_path)
                        continue
                    items = child.items() if isinstance(child, dict) else enumerate(child)
                    for element_key, element in items:
                        element_path = (
                            f"{child_path}.{element_key}"
                            if isinstance(child, dict)
                            else f"{child_path}[]"
                        )
                        if isinstance(element, (dict, list)):
                            reject(
                                element_path,
                                "numeric_container_element_not_scalar",
                                rule_id,
                            )
                        else:
                            if isinstance(element, (int, float)) and not isinstance(
                                element, bool
                            ):
                                numeric_leaf_count += 1
                            validate_declared(
                                element,
                                path=element_path,
                                rule=rule,
                                element=True,
                            )
                    continue
                if matched and matched[0] == "scalar":
                    if isinstance(child, (int, float)) and not isinstance(child, bool):
                        numeric_leaf_count += 1
                    validate_declared(
                        child,
                        path=child_path,
                        rule=matched[1],
                    )
                    continue
                if str(field) in EVIDENCE_SCHEMA_FIELD_NAMES:
                    reject(
                        child_path,
                        "declared_numeric_field_parent_path_mismatch",
                    )
                    continue
                if isinstance(child, (dict, list)):
                    visit(child, child_path)
                elif isinstance(child, (int, float)) and not isinstance(child, bool):
                    numeric_leaf_count += 1
                    reject(child_path, "undeclared_numeric_evidence")
            return
        if isinstance(value, list):
            for child in value:
                child_path = f"{path}[]"
                if isinstance(child, (dict, list)):
                    visit(child, child_path)
                elif isinstance(child, (int, float)) and not isinstance(child, bool):
                    numeric_leaf_count += 1
                    reject(child_path, "undeclared_numeric_evidence")
            return
        reject(path, "root_not_object")

    if require_binding and not numeric_evidence_schema_binding_valid(payload):
        reject("$", "schema_binding_invalid")
    visit(payload, "")
    return {
        "accepted": error_count == 0,
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "schema_sha256": EVIDENCE_SCHEMA_SHA256,
        "binding_valid": numeric_evidence_schema_binding_valid(payload),
        "numeric_leaf_count": numeric_leaf_count,
        "declared_numeric_leaf_count": declared_numeric_leaf_count,
        "error_count": error_count,
        "errors": errors,
        "rule_match_counts": dict(sorted(rule_match_counts.items())),
    }


def numeric_evidence_schema_valid(
    payload: object,
    *,
    require_binding: bool = False,
) -> bool:
    return bool(
        validate_numeric_evidence_schema(
            payload,
            require_binding=require_binding,
            max_errors=1,
        )["accepted"]
    )


def require_numeric_evidence_schema(
    payload: object,
    *,
    require_binding: bool = False,
) -> dict[str, Any]:
    report = validate_numeric_evidence_schema(
        payload,
        require_binding=require_binding,
    )
    if not report["accepted"]:
        raise ValueError(
            "evaluator_snapshot_numeric_evidence_schema_invalid:"
            + canonical_json(report["errors"][:8])
        )
    return report
