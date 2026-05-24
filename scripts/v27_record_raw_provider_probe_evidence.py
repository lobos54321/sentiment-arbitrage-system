#!/usr/bin/env python3
"""Record a real read-only raw provider quote probe into the v2.7 event log."""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from v27_event_log import V27EventLog, sha256_hex  # noqa: E402
from v27_mirror_raw_provider_evidence import (  # noqa: E402
    DEFAULT_ENDPOINT,
    DEFAULT_PROVIDER,
    DEFAULT_RAW_PROVIDER_EVIDENCE_VERSION,
    RAW_PROVIDER_EVIDENCE_EVENT_TYPE,
)


DEFAULT_EVENT_LOG_DIR = PROJECT_ROOT / "data" / "v27_event_log"
DEFAULT_INPUT_MINT = "So11111111111111111111111111111111111111112"
DEFAULT_OUTPUT_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
DEFAULT_OUTPUT_SYMBOL = "USDC"
DEFAULT_AMOUNT_RAW = "1000000"
SOURCE = "v27_raw_provider_probe_evidence"
SCHEMA_VERSION = "v2.7.0.raw_provider_probe_evidence.v1"
HASH_ALGORITHM = "sha256(canonical_json)"
TRUSTED_PROOF_LEVEL = "provider_request_id_with_raw_response_hash"
RESPONSE_MATERIAL_TYPE = "provider_probe.rawResponse"


def _utc_now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _default_run_id():
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def _text(value, default=None):
    if value is None:
        return default
    text = str(value).strip()
    return text or default


def _fetch_jupiter_order(
    *,
    endpoint_base,
    input_mint,
    output_mint,
    amount_raw,
    slippage_bps=None,
    timeout_sec=10,
    api_key=None,
):
    params = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": str(amount_raw),
    }
    if slippage_bps is not None:
        params["slippageBps"] = str(slippage_bps)
    url = f"{endpoint_base}?{urllib.parse.urlencode(params)}"
    headers = {
        "Accept": "application/json",
        "User-Agent": "curl/8.7.1",
    }
    if api_key:
        headers["x-api-key"] = api_key
    started = time.monotonic()
    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            raw_body = response.read().decode("utf-8")
            data = json.loads(raw_body) if raw_body else {}
            status = int(response.status)
    except urllib.error.HTTPError as exc:
        raw_body = exc.read().decode("utf-8", errors="replace")
        try:
            data = json.loads(raw_body) if raw_body else {}
        except json.JSONDecodeError:
            data = {"error": raw_body}
        status = int(exc.code)
    latency_ms = max(0.0, (time.monotonic() - started) * 1000.0)
    return {
        "status": status,
        "data": data,
        "latency_ms": latency_ms,
        "url": url,
    }


def build_raw_provider_probe_payload(
    fetch_result,
    *,
    run_id,
    evidence_version=DEFAULT_RAW_PROVIDER_EVIDENCE_VERSION,
    provider=DEFAULT_PROVIDER,
    endpoint=DEFAULT_ENDPOINT,
    input_mint=DEFAULT_INPUT_MINT,
    output_mint=DEFAULT_OUTPUT_MINT,
    output_symbol=DEFAULT_OUTPUT_SYMBOL,
    amount_raw=DEFAULT_AMOUNT_RAW,
    slippage_bps=None,
    observed_at=None,
):
    observed_at = observed_at or _utc_now_iso()
    raw_response = fetch_result.get("data") if isinstance(fetch_result, dict) else {}
    if not isinstance(raw_response, dict):
        raw_response = {"raw_response": raw_response}
    status = int(fetch_result.get("status") or 0) if isinstance(fetch_result, dict) else 0
    latency_ms = float(fetch_result.get("latency_ms") or 0.0) if isinstance(fetch_result, dict) else 0.0
    request_id = _text(raw_response.get("requestId"))
    raw_response_available = bool(raw_response)
    request_parameters = {
        "input_mint": input_mint,
        "output_mint": output_mint,
        "input_amount_raw": str(amount_raw),
        "slippage_bps": slippage_bps,
        "quote_ts": observed_at,
        "http_status": status,
    }
    request_metadata = {
        "raw_provider_evidence_version": evidence_version,
        "probe_run_id": run_id,
        "side": "probe",
        "provider": provider,
        "endpoint": endpoint,
        "request_id": request_id,
        "request_parameters": request_parameters,
    }
    request_hash = sha256_hex(request_metadata)
    response_hash = sha256_hex(raw_response)
    provider_evidence_trusted = bool(
        200 <= status < 300
        and provider
        and endpoint
        and request_id
        and request_parameters
        and raw_response_available
        and response_hash
        and latency_ms >= 0
    )
    return {
        "probe_schema_version": SCHEMA_VERSION,
        "paper_trade_id": f"provider_probe:{run_id}",
        "probe_run_id": run_id,
        "token_ca": output_mint,
        "symbol": output_symbol,
        "chain": "solana",
        "canonical_pool_group": "provider_probe",
        "lifecycle_epoch": 0,
        "raw_provider_evidence_version": evidence_version,
        "provider_evidence_version": evidence_version,
        "provider": provider,
        "endpoint": endpoint,
        "request_id": request_id,
        "provider_request_id": request_id,
        "side": "probe",
        "latency_ms": latency_ms,
        "request_parameters": request_parameters,
        "request_metadata": request_metadata,
        "request_metadata_available": bool(request_parameters),
        "request_metadata_hash": sha256_hex(request_metadata),
        "request_hash": request_hash,
        "response_hash": response_hash,
        "raw_response_hash": response_hash if raw_response_available else None,
        "raw_response_available": raw_response_available,
        "response_material_type": RESPONSE_MATERIAL_TYPE,
        "hash_algorithm": HASH_ALGORITHM,
        "evidence_source": SOURCE,
        "provider_evidence_proof_level": TRUSTED_PROOF_LEVEL if provider_evidence_trusted else "provider_probe_without_trusted_request_id",
        "provider_evidence_trusted": provider_evidence_trusted,
        "decision_available_at": observed_at,
        "observed_at": observed_at,
        "raw_response": raw_response,
        "http_status": status,
    }


def record_raw_provider_probe_evidence(
    event_log_dir,
    *,
    run_id=None,
    evidence_version=DEFAULT_RAW_PROVIDER_EVIDENCE_VERSION,
    provider=DEFAULT_PROVIDER,
    endpoint=DEFAULT_ENDPOINT,
    endpoint_base="https://api.jup.ag/ultra/v1/order",
    input_mint=DEFAULT_INPUT_MINT,
    output_mint=DEFAULT_OUTPUT_MINT,
    output_symbol=DEFAULT_OUTPUT_SYMBOL,
    amount_raw=DEFAULT_AMOUNT_RAW,
    slippage_bps=None,
    timeout_sec=10,
    api_key=None,
    dry_run=False,
    fetcher=None,
):
    run_id = run_id or _default_run_id()
    fetcher = fetcher or _fetch_jupiter_order
    fetch_result = fetcher(
        endpoint_base=endpoint_base,
        input_mint=input_mint,
        output_mint=output_mint,
        amount_raw=amount_raw,
        slippage_bps=slippage_bps,
        timeout_sec=timeout_sec,
        api_key=api_key,
    )
    payload = build_raw_provider_probe_payload(
        fetch_result,
        run_id=run_id,
        evidence_version=evidence_version,
        provider=provider,
        endpoint=endpoint,
        input_mint=input_mint,
        output_mint=output_mint,
        output_symbol=output_symbol,
        amount_raw=amount_raw,
        slippage_bps=slippage_bps,
    )
    summary = {
        "event_log_dir": str(event_log_dir),
        "run_id": run_id,
        "http_status": payload.get("http_status"),
        "provider_request_id": payload.get("provider_request_id"),
        "trusted_provider_evidence": 1 if payload.get("provider_evidence_trusted") is True else 0,
        "raw_response_available": bool(payload.get("raw_response_available")),
        "response_material_type": payload.get("response_material_type"),
        "dry_run": bool(dry_run),
        "appended": 0,
        "duplicate": 0,
        "failed": 0,
        "failures": [],
    }
    if dry_run:
        summary["payload"] = payload
        return summary
    try:
        result = V27EventLog(event_log_dir).append_event(
            event_type=RAW_PROVIDER_EVIDENCE_EVENT_TYPE,
            aggregate_id=":".join(
                [
                    "raw_provider_evidence",
                    "solana",
                    output_mint,
                    "provider_probe",
                    "0",
                    run_id,
                    "probe",
                ]
            ),
            payload=payload,
            source=SOURCE,
            idempotency_key=f"raw_provider_probe_evidence:{run_id}:{evidence_version}",
            observed_at=payload.get("observed_at"),
            available_at=payload.get("decision_available_at"),
        )
        status = result.get("status")
        if status == "appended":
            summary["appended"] = 1
        elif status == "duplicate":
            summary["duplicate"] = 1
        else:
            summary["failed"] = 1
            summary["failures"].append({"reason": f"unexpected status {status}"})
    except Exception as exc:
        summary["failed"] = 1
        summary["failures"].append({"reason": str(exc)})
    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-log-dir", default=str(DEFAULT_EVENT_LOG_DIR))
    parser.add_argument("--run-id")
    parser.add_argument("--evidence-version", default=os.environ.get("V27_RAW_PROVIDER_EVIDENCE_VERSION", DEFAULT_RAW_PROVIDER_EVIDENCE_VERSION))
    parser.add_argument("--provider", default=os.environ.get("V27_RAW_PROVIDER_DEFAULT_PROVIDER", DEFAULT_PROVIDER))
    parser.add_argument("--endpoint", default=os.environ.get("V27_RAW_PROVIDER_DEFAULT_ENDPOINT", DEFAULT_ENDPOINT))
    parser.add_argument("--endpoint-base", default=os.environ.get("V27_RAW_PROVIDER_ENDPOINT_BASE", "https://api.jup.ag/ultra/v1/order"))
    parser.add_argument("--input-mint", default=os.environ.get("V27_RAW_PROVIDER_PROBE_INPUT_MINT", DEFAULT_INPUT_MINT))
    parser.add_argument("--output-mint", default=os.environ.get("V27_RAW_PROVIDER_PROBE_OUTPUT_MINT", DEFAULT_OUTPUT_MINT))
    parser.add_argument("--output-symbol", default=os.environ.get("V27_RAW_PROVIDER_PROBE_OUTPUT_SYMBOL", DEFAULT_OUTPUT_SYMBOL))
    parser.add_argument("--amount-raw", default=os.environ.get("V27_RAW_PROVIDER_PROBE_AMOUNT_RAW", DEFAULT_AMOUNT_RAW))
    parser.add_argument("--slippage-bps", type=int)
    parser.add_argument("--timeout-sec", type=float, default=float(os.environ.get("V27_RAW_PROVIDER_PROBE_TIMEOUT_SEC", "10")))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()

    summary = record_raw_provider_probe_evidence(
        args.event_log_dir,
        run_id=args.run_id,
        evidence_version=args.evidence_version,
        provider=args.provider,
        endpoint=args.endpoint,
        endpoint_base=args.endpoint_base,
        input_mint=args.input_mint,
        output_mint=args.output_mint,
        output_symbol=args.output_symbol,
        amount_raw=args.amount_raw,
        slippage_bps=args.slippage_bps,
        timeout_sec=args.timeout_sec,
        api_key=os.environ.get("JUPITER_API_KEY"),
        dry_run=args.dry_run,
    )
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    if args.strict and (
        summary.get("failed")
        or summary.get("trusted_provider_evidence") != 1
        or (not args.dry_run and summary.get("appended") != 1 and summary.get("duplicate") != 1)
    ):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
