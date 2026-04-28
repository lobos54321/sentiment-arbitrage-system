#!/usr/bin/env python3
"""Healthcheck paper trader logs from a URL or local log file.

This script is intentionally log-driven: the current Zeabur runtime is the
source of truth for live paper status, while local SQLite snapshots may be old.
"""

import argparse
import re
import sys
import urllib.error
import urllib.request
from collections import Counter, deque
from dataclasses import dataclass


TIMESTAMP_RE = re.compile(r"(?:\[)?(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})")
HEARTBEAT_RE = re.compile(
    r"heartbeat\s+signals=(?P<signals>\d+).*?age_min=(?P<age>[\d.]+).*?"
    r"watching=(?P<watching>\d+).*?active_positions=(?P<active>\d+).*?pending=(?P<pending>\d+)",
    re.IGNORECASE,
)
LATEST_SIGNAL_RE = re.compile(r"premium_signals latest:\s+(?P<latest>.+?)\s+\((?P<age>.+? ago)", re.IGNORECASE)
REGISTER_RE = re.compile(r"\[WATCHLIST\]\s+Registering\s+(?P<symbol>.+?)\s+\((?P<type>.+?)\)", re.IGNORECASE)
PREMIUM_SIGNAL_RE = re.compile(r"PREMIUM SIGNAL:\s+\$(?P<symbol>[^\s(]+)", re.IGNORECASE)
FIRE_RE = re.compile(r"\bFIRE\s+(?P<symbol>.+?)!", re.IGNORECASE)
ENTERED_RE = re.compile(r"Entered\s+(?P<symbol>.+?)/stage(?P<stage>\w+)\s+@", re.IGNORECASE)
CLOSED_RE = re.compile(
    r"CLOSED\s+(?P<symbol>.+?)/stage(?P<stage>\w+).*?pnl=(?P<pnl>[+-]?\d+(?:\.\d+)?)%",
    re.IGNORECASE,
)
EXIT_MATRIX_RE = re.compile(
    r"EXIT_MATRIX\s+(?P<symbol>.+?)/stage(?P<stage>\w+).*?action=(?P<action>\w+).*?"
    r"pnl=(?P<pnl>[+-]?\d+(?:\.\d+)?)%.*?reason=(?P<reason>[^\s]+)",
    re.IGNORECASE,
)
WAIT_RE = re.compile(r"\[WATCHLIST\]\s+(?P<symbol>.+?)\s+WAIT\s+reason=(?P<reason>.+)$", re.IGNORECASE)
UNKNOWN_CONTINUE_RE = re.compile(r"UNKNOWN_DATA.*?continue=1", re.IGNORECASE)
COOLDOWN_SECONDS_RE = re.compile(r"\s+\(\d+s remaining\)")


@dataclass
class Event:
    ts: str
    kind: str
    detail: str


def extract_ts(line):
    match = TIMESTAMP_RE.search(line)
    return match.group(1).replace("T", " ") if match else "-"


def read_url(url, timeout):
    request = urllib.request.Request(url, headers={"User-Agent": "paper-healthcheck/1.0"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        return response.read().decode(charset, errors="replace")


def read_inputs(args):
    chunks = []
    for path in args.file or []:
        with open(path, "r", encoding="utf-8", errors="replace") as handle:
            chunks.append(handle.read())
    for url in args.url or []:
        try:
            chunks.append(read_url(url, args.timeout))
        except (urllib.error.URLError, TimeoutError) as exc:
            print(f"ERROR: failed to fetch URL: {exc}", file=sys.stderr)
            return None
    if not chunks and not sys.stdin.isatty():
        chunks.append(sys.stdin.read())
    return "\n".join(chunks)


def pct(values, numerator):
    return (numerator / len(values) * 100.0) if values else 0.0


def analyze(text):
    counters = Counter()
    risk_lines = []
    events = deque(maxlen=30)
    waits = Counter()
    closed_pnls = []
    latest_signal = None
    last_heartbeat = None
    last_start = None
    last_signal_event = None
    last_entry = None
    last_close = None

    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        ts = extract_ts(line)
        lower = line.lower()

        if "paper-trader" in line and "starting" in line:
            last_start = Event(ts, "start", line)
            events.append(last_start)
        elif "Paper Trade Monitor Started" in line:
            events.append(Event(ts, "started", line))

        match = LATEST_SIGNAL_RE.search(line)
        if match:
            latest_signal = Event(ts, "latest_signal", match.group(0))
            events.append(latest_signal)

        match = HEARTBEAT_RE.search(line)
        if match:
            last_heartbeat = Event(ts, "heartbeat", match.group(0))
        elif "heartbeat" in lower:
            last_heartbeat = Event(ts, "heartbeat", line)

        match = REGISTER_RE.search(line)
        if match:
            counters["registered"] += 1
            detail = f"{match.group('symbol')} type={match.group('type')}"
            last_signal_event = Event(ts, "registered", detail)
            events.append(last_signal_event)

        match = PREMIUM_SIGNAL_RE.search(line)
        if match:
            counters["premium_signals"] += 1
            last_signal_event = Event(ts, "premium_signal", match.group("symbol"))
            events.append(last_signal_event)

        match = FIRE_RE.search(line)
        if match:
            counters["fires"] += 1
            events.append(Event(ts, "fire", match.group("symbol")))

        match = ENTERED_RE.search(line)
        if match:
            counters["entries"] += 1
            last_entry = Event(ts, "entry", f"{match.group('symbol')}/stage{match.group('stage')}")
            events.append(last_entry)

        match = CLOSED_RE.search(line)
        if match:
            pnl = float(match.group("pnl"))
            closed_pnls.append(pnl)
            counters["closed"] += 1
            if pnl > 0:
                counters["wins"] += 1
            else:
                counters["losses"] += 1
            last_close = Event(ts, "closed", f"{match.group('symbol')}/stage{match.group('stage')} pnl={pnl:+.1f}%")
            events.append(last_close)

        match = EXIT_MATRIX_RE.search(line)
        if match and match.group("action").lower() == "exit":
            counters[f"exit_reason:{match.group('reason')}"] += 1

        match = WAIT_RE.search(line)
        if match:
            reason = COOLDOWN_SECONDS_RE.sub("", match.group("reason")).strip()
            waits[reason] += 1

        if "error" in lower:
            counters["errors"] += 1
            risk_lines.append(Event(ts, "error", line))
        if "warning" in lower:
            counters["warnings"] += 1
        if "http 401" in lower or "invalid api key" in lower:
            counters["http_401"] += 1
            risk_lines.append(Event(ts, "http_401", line))
        if "http 429" in lower or "rate_limited" in lower or "rate limit" in lower:
            counters["rate_limited"] += 1
            risk_lines.append(Event(ts, "rate_limited", line))
        if "no_route" in lower or "no route" in lower:
            counters["no_route"] += 1
            risk_lines.append(Event(ts, "no_route", line))
        if "quote failed" in lower or "quote_failed" in lower:
            counters["quote_failed"] += 1
            risk_lines.append(Event(ts, "quote_failed", line))
        if UNKNOWN_CONTINUE_RE.search(line):
            counters["unknown_data_continue"] += 1
            risk_lines.append(Event(ts, "unknown_data_continue", line))

    return {
        "counters": counters,
        "events": list(events),
        "risk_lines": risk_lines[-20:],
        "waits": waits,
        "closed_pnls": closed_pnls,
        "latest_signal": latest_signal,
        "last_heartbeat": last_heartbeat,
        "last_start": last_start,
        "last_signal_event": last_signal_event,
        "last_entry": last_entry,
        "last_close": last_close,
    }


def print_event(label, event):
    if event:
        print(f"{label}: {event.ts} {event.detail}")
    else:
        print(f"{label}: none observed")


def print_report(result):
    counters = result["counters"]
    pnls = result["closed_pnls"]
    risk_score = 0
    if not result["last_heartbeat"]:
        risk_score += 1
    if counters["unknown_data_continue"]:
        risk_score += 3
    if counters["http_401"] or counters["rate_limited"]:
        risk_score += 2
    if pnls and min(pnls) <= -20:
        risk_score += 3
    if counters["errors"]:
        risk_score += 2

    status = "OK"
    if risk_score >= 6:
        status = "CRITICAL"
    elif risk_score >= 3:
        status = "WARN"

    print("=== Paper Trader Healthcheck ===")
    print(f"Status: {status}")
    print_event("Last start", result["last_start"])
    print_event("Latest signal snapshot", result["latest_signal"])
    print_event("Last heartbeat", result["last_heartbeat"])
    print_event("Last signal event", result["last_signal_event"])
    print_event("Last entry", result["last_entry"])
    print_event("Last close", result["last_close"])

    print("\nActivity:")
    print(f"  premium_signals={counters['premium_signals']} registered={counters['registered']} fires={counters['fires']} entries={counters['entries']} closed={counters['closed']}")
    if pnls:
        wins = sum(1 for pnl in pnls if pnl > 0)
        print(f"  closed_pnl: n={len(pnls)} win_rate={pct(pnls, wins):.1f}% avg={sum(pnls)/len(pnls):+.1f}% min={min(pnls):+.1f}% max={max(pnls):+.1f}%")

    print("\nRisk counters:")
    for key in ["unknown_data_continue", "http_401", "rate_limited", "no_route", "quote_failed", "errors", "warnings"]:
        print(f"  {key}={counters[key]}")

    exit_reasons = {k.split(":", 1)[1]: v for k, v in counters.items() if k.startswith("exit_reason:")}
    if exit_reasons:
        print("\nExit reasons:")
        for reason, count in sorted(exit_reasons.items(), key=lambda item: (-item[1], item[0])):
            print(f"  {reason}: {count}")

    if result["waits"]:
        print("\nTop wait reasons:")
        for reason, count in result["waits"].most_common(8):
            print(f"  {count:4d} {reason}")

    if result["risk_lines"]:
        print("\nRecent risk lines:")
        for event in result["risk_lines"]:
            print(f"  [{event.kind}] {event.ts} {event.detail[:260]}")

    if result["events"]:
        print("\nRecent events:")
        for event in result["events"][-12:]:
            print(f"  [{event.kind}] {event.ts} {event.detail[:180]}")


def main():
    parser = argparse.ArgumentParser(description="Analyze paper trader logs for runtime and trading risks.")
    parser.add_argument("--url", action="append", help="Log URL to fetch. Can be provided multiple times.")
    parser.add_argument("--file", action="append", help="Local log file to analyze. Can be provided multiple times.")
    parser.add_argument("--timeout", type=float, default=30.0, help="URL fetch timeout in seconds.")
    args = parser.parse_args()

    text = read_inputs(args)
    if text is None:
        return 2
    if not text.strip():
        print("ERROR: no log content provided", file=sys.stderr)
        return 2
    print_report(analyze(text))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
