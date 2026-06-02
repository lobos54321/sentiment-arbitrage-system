#!/usr/bin/env python3
"""Append-only paper evidence log.

SQLite is the query store for paper trading, but it should not be the only
place where trading evidence lands. This module writes compact JSONL events to
disk before critical SQLite writes, so a lock/corruption event cannot erase the
fact that a decision or entry happened.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import os
from pathlib import Path
import time
from typing import Any


log = logging.getLogger("paper_trade.evidence")

SCHEMA_VERSION = "paper_evidence_log.v1"
DEFAULT_DIR = Path(os.environ.get("DATA_DIR", "data")) / "paper_evidence_log"


def _truthy_env(name: str, default: str = "false") -> bool:
    return os.environ.get(name, default).strip().lower() in {"1", "true", "yes", "on"}


def _disabled() -> bool:
    if os.environ.get("PAPER_EVIDENCE_LOG_ENABLED", "true").strip().lower() in {"0", "false", "no", "off"}:
        return True
    # Avoid polluting the repo's data/ directory during ordinary unit tests.
    # Tests that validate this module set PAPER_EVIDENCE_LOG_DIR explicitly.
    if os.environ.get("PYTEST_CURRENT_TEST") and not os.environ.get("PAPER_EVIDENCE_LOG_DIR"):
        return True
    return False


def _log_dir() -> Path:
    return Path(os.environ.get("PAPER_EVIDENCE_LOG_DIR") or DEFAULT_DIR)


def _json_default(value: Any) -> str:
    try:
        return str(value)
    except Exception:
        return "<unserializable>"


def _json_safe(value: Any) -> Any:
    return json.loads(json.dumps(value, ensure_ascii=False, sort_keys=True, default=_json_default))


def _event_id(record: dict[str, Any]) -> str:
    material = json.dumps(record, ensure_ascii=False, sort_keys=True, default=_json_default)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def append_paper_evidence_event(
    *,
    source: str,
    event_type: str,
    payload: dict[str, Any] | None = None,
    idempotency_key: str | None = None,
    event_ts: float | int | None = None,
    critical: bool = False,
) -> str | None:
    """Best-effort append of one evidence event.

    This function intentionally never raises. Trading must continue if the
    evidence file cannot be written, but callers should invoke it before the
    corresponding SQLite write whenever the event is important for replay.
    """

    if _disabled():
        return None

    ts = float(event_ts if event_ts is not None else time.time())
    log_dir = _log_dir()
    day = time.strftime("%Y%m%d", time.gmtime(ts))
    path = log_dir / f"paper-events-{day}.jsonl"
    lock_path = log_dir / ".append.lock"
    record = {
        "schema_version": SCHEMA_VERSION,
        "written_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "event_ts": ts,
        "source": str(source or "unknown"),
        "event_type": str(event_type or "unknown"),
        "idempotency_key": str(idempotency_key) if idempotency_key is not None else None,
        "payload": _json_safe(payload or {}),
    }
    record["event_id"] = _event_id(record)

    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        line = json.dumps(record, ensure_ascii=False, sort_keys=True, default=_json_default)
        with lock_path.open("a+", encoding="utf-8") as lock_fh:
            fcntl.flock(lock_fh, fcntl.LOCK_EX)
            try:
                with path.open("a", encoding="utf-8") as fh:
                    fh.write(line)
                    fh.write("\n")
                    if critical or _truthy_env("PAPER_EVIDENCE_LOG_FSYNC"):
                        fh.flush()
                        os.fsync(fh.fileno())
            finally:
                fcntl.flock(lock_fh, fcntl.LOCK_UN)
        return str(path)
    except Exception as exc:
        log.debug("[PAPER_EVIDENCE] append failed source=%s type=%s error=%s", source, event_type, exc)
        return None
