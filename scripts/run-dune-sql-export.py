#!/usr/bin/env python3
"""Run a Dune SQL query and export all result rows as JSONL.

This is intentionally small and dependency-free so it can run on a local
machine without adding project packages. It reads DUNE_API_KEY from either the
environment or a key file whose content is `DUNE_API_KEY=...`.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


API_BASE = "https://api.dune.com/api/v1"
TRANSIENT_RETRIES = 5
TRANSIENT_RETRY_BASE_SECONDS = 2
RATE_LIMIT_RETRIES = 6
RATE_LIMIT_DEFAULT_SLEEP_SECONDS = 60


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def read_api_key(key_file: str | None) -> str:
    if os.environ.get("DUNE_API_KEY"):
        return os.environ["DUNE_API_KEY"].strip()
    if not key_file:
        key_file = os.path.expanduser("~/.dune_api_key")
    raw = Path(os.path.expanduser(key_file)).read_text(encoding="utf-8").strip()
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("DUNE_API_KEY="):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
        return line
    raise RuntimeError(f"No DUNE_API_KEY found in {key_file}")


def is_transient_network_error(exc: BaseException) -> bool:
    if isinstance(exc, (TimeoutError, urllib.error.URLError)):
        return True
    return False


def request_json(method: str, url: str, api_key: str, body: dict | None = None, timeout: int = 120) -> dict:
    data = None
    headers = {
        "X-Dune-API-Key": api_key,
        "Accept": "application/json",
    }
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    # Local Python cert stores can be stale on some macOS installs. We prefer a
    # successful API feedback loop over failing on local trust store state.
    context = ssl._create_unverified_context()
    for attempt in range(TRANSIENT_RETRIES + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout, context=context) as resp:
                payload = resp.read().decode("utf-8")
            break
        except urllib.error.HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace")
            if exc.code == 429 and attempt < RATE_LIMIT_RETRIES:
                retry_after = exc.headers.get("Retry-After")
                try:
                    sleep_for = int(retry_after) if retry_after else RATE_LIMIT_DEFAULT_SLEEP_SECONDS
                except ValueError:
                    sleep_for = RATE_LIMIT_DEFAULT_SLEEP_SECONDS
                print(
                    f"[dune] HTTP 429 rate limit; retrying in {sleep_for}s "
                    f"({attempt + 1}/{RATE_LIMIT_RETRIES})",
                    file=sys.stderr,
                    flush=True,
                )
                time.sleep(sleep_for)
                continue
            raise RuntimeError(f"Dune HTTP {exc.code} for {url}: {error_body}") from exc
        except Exception as exc:
            if not is_transient_network_error(exc) or attempt >= TRANSIENT_RETRIES:
                raise
            sleep_for = TRANSIENT_RETRY_BASE_SECONDS * (2 ** attempt)
            print(
                f"[dune] transient API error ({type(exc).__name__}: {exc}); "
                f"retrying in {sleep_for}s ({attempt + 1}/{TRANSIENT_RETRIES})",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(sleep_for)
    return json.loads(payload) if payload else {}


def execute_sql(sql: str, api_key: str, performance: str) -> str:
    response = request_json(
        "POST",
        f"{API_BASE}/sql/execute",
        api_key,
        {"sql": sql, "performance": performance},
        timeout=120,
    )
    execution_id = response.get("execution_id") or response.get("executionId")
    if not execution_id:
        raise RuntimeError(f"Could not find execution_id in response: {response}")
    return str(execution_id)


def poll_execution(execution_id: str, api_key: str, poll_seconds: int, timeout_seconds: int) -> dict:
    deadline = time.time() + timeout_seconds
    last = None
    while True:
        status = request_json("GET", f"{API_BASE}/execution/{execution_id}/status", api_key, timeout=60)
        last = status
        state = str(status.get("state") or status.get("status") or "").upper()
        print(f"[dune] {now_iso()} execution={execution_id} state={state or '?'}", flush=True)
        if state in {"QUERY_STATE_COMPLETED", "COMPLETED", "SUCCESS", "SUCCEEDED"}:
            return status
        if state in {"QUERY_STATE_FAILED", "FAILED", "CANCELLED", "CANCELED"}:
            raise RuntimeError(f"Dune execution failed: {status}")
        if time.time() > deadline:
            raise TimeoutError(f"Timed out waiting for execution {execution_id}; last status={last}")
        time.sleep(poll_seconds)


def extract_rows(result: dict) -> list[dict]:
    if isinstance(result.get("rows"), list):
        return result["rows"]
    nested = result.get("result")
    if isinstance(nested, dict) and isinstance(nested.get("rows"), list):
        return nested["rows"]
    if isinstance(result.get("results"), list):
        return result["results"]
    raise RuntimeError(f"Could not find rows in result page keys={list(result.keys())}")


def fetch_all_results(execution_id: str, api_key: str, out_jsonl: Path, page_limit: int) -> tuple[int, list[str]]:
    out_jsonl.parent.mkdir(parents=True, exist_ok=True)
    total = 0
    columns: list[str] = []
    with out_jsonl.open("w", encoding="utf-8") as out:
        offset = 0
        while True:
            query = urllib.parse.urlencode({"limit": page_limit, "offset": offset})
            page = request_json("GET", f"{API_BASE}/execution/{execution_id}/results?{query}", api_key, timeout=120)
            rows = extract_rows(page)
            if not columns and rows:
                columns = list(rows[0].keys())
            for row in rows:
                out.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
            total += len(rows)
            print(f"[dune] fetched offset={offset} rows={len(rows)} total={total}", flush=True)
            if len(rows) < page_limit:
                break
            offset += page_limit
    return total, columns


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sql", required=True, help="SQL file to execute")
    parser.add_argument("--out-jsonl", required=True, help="Output JSONL path")
    parser.add_argument("--manifest", required=True, help="Output manifest JSON path")
    parser.add_argument("--key-file", default="~/.dune_api_key")
    parser.add_argument("--execution-id", default="", help="Reuse an existing Dune execution id instead of executing SQL")
    parser.add_argument("--page-limit", type=int, default=1000)
    parser.add_argument("--poll-seconds", type=int, default=10)
    parser.add_argument("--timeout-seconds", type=int, default=3600)
    parser.add_argument("--performance", default=os.environ.get("DUNE_PERFORMANCE", "small"), choices=["small", "medium", "large", "free"])
    args = parser.parse_args(argv)

    sql_path = Path(args.sql)
    out_jsonl = Path(args.out_jsonl)
    manifest_path = Path(args.manifest)
    sql = sql_path.read_text(encoding="utf-8")
    api_key = read_api_key(args.key_file)

    started_at = now_iso()
    execution_id = args.execution_id or execute_sql(sql, api_key, args.performance)
    print(f"[dune] execution_id={execution_id}", flush=True)
    status = poll_execution(execution_id, api_key, args.poll_seconds, args.timeout_seconds)
    row_count, columns = fetch_all_results(execution_id, api_key, out_jsonl, args.page_limit)
    finished_at = now_iso()

    manifest = {
        "schema_version": "dune_sql_export_manifest.v1",
        "generated_at": finished_at,
        "started_at": started_at,
        "sql_file": str(sql_path),
        "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "execution_id": execution_id,
        "final_status": status,
        "out_jsonl": str(out_jsonl),
        "out_jsonl_sha256": sha256_file(out_jsonl),
        "row_count": row_count,
        "columns": columns,
        "page_limit": args.page_limit,
        "performance": args.performance,
    }
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"execution_id": execution_id, "row_count": row_count, "manifest": str(manifest_path)}, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
