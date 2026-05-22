#!/usr/bin/env python3
"""Append-only v2.7 event log seed implementation.

This is the first M2 building block. It does not replace the current paper DB;
it provides a small sidecar event log that can mirror existing decisions while
preserving the v2.7 sequencing and idempotency invariants.
"""

import argparse
import fcntl
import hashlib
import json
import os
import time
import uuid
from pathlib import Path


STATE_FILE = "sequencer-state.json"
EVENT_FILE = "events.jsonl"
LOCK_FILE = "sequencer.lock"


class V27EventLogError(RuntimeError):
    pass


def canonical_json_bytes(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def sha256_hex(value):
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


class V27EventLog:
    def __init__(self, base_dir):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.state_path = self.base_dir / STATE_FILE
        self.event_path = self.base_dir / EVENT_FILE
        self.lock_path = self.base_dir / LOCK_FILE

    def _load_state(self):
        if not self.state_path.exists():
            return self._empty_state()
        with self.state_path.open("r", encoding="utf-8") as fh:
            state = json.load(fh)
        state.setdefault("last_global_seq", 0)
        state.setdefault("aggregate_last_seq", {})
        state.setdefault("idempotency_index", {})
        return state

    def _empty_state(self):
        return {
            "last_global_seq": 0,
            "aggregate_last_seq": {},
            "idempotency_index": {},
        }

    def _event_file_metadata(self):
        if not self.event_path.exists():
            return {
                "event_file_size_bytes": 0,
                "event_file_mtime_ns": None,
            }
        stat = self.event_path.stat()
        return {
            "event_file_size_bytes": stat.st_size,
            "event_file_mtime_ns": stat.st_mtime_ns,
        }

    def _state_matches_event_file(self, state):
        metadata = self._event_file_metadata()
        return (
            state.get("event_file_size_bytes") == metadata["event_file_size_bytes"]
            and state.get("event_file_mtime_ns") == metadata["event_file_mtime_ns"]
        )

    def _write_state(self, state):
        state = dict(state)
        state.update(self._event_file_metadata())
        tmp = self.state_path.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(state, fh, sort_keys=True, separators=(",", ":"))
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self.state_path)

    def _read_event_by_id(self, event_id):
        if not self.event_path.exists():
            return None
        with self.event_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                if not line.strip():
                    continue
                event = json.loads(line)
                if event.get("event_id") == event_id:
                    return event
        return None

    def _build_state_from_events(self):
        state = self._empty_state()
        event_count = 0

        for event in self.iter_events() or []:
            event_count += 1
            if event.get("global_seq") != event_count:
                raise V27EventLogError(
                    f"global_seq gap or reorder detected: expected {event_count}, got {event.get('global_seq')}"
                )

            aggregate_id = event.get("aggregate_id")
            if not aggregate_id:
                raise V27EventLogError(f"missing aggregate_id: {event.get('event_id')}")
            aggregate_seq = event.get("aggregate_seq")
            expected_aggregate_seq = int(state["aggregate_last_seq"].get(aggregate_id, 0)) + 1
            if aggregate_seq != expected_aggregate_seq:
                raise V27EventLogError(f"aggregate_seq gap for {aggregate_id}")
            state["aggregate_last_seq"][aggregate_id] = aggregate_seq

            idempotency_key = event.get("idempotency_key")
            if not idempotency_key:
                raise V27EventLogError(f"missing idempotency key: {event.get('event_id')}")
            if idempotency_key in state["idempotency_index"]:
                raise V27EventLogError(f"duplicate idempotency key: {idempotency_key}")
            state["idempotency_index"][idempotency_key] = event.get("event_id")

            expected_hash = sha256_hex({key: value for key, value in event.items() if key != "event_hash"})
            if event.get("event_hash") != expected_hash:
                raise V27EventLogError(f"event_hash mismatch: {event.get('event_id')}")

        state["last_global_seq"] = event_count
        return {
            "state": state,
            "event_count": event_count,
            "last_global_seq": event_count,
            "aggregate_count": len(state["aggregate_last_seq"]),
            "idempotency_count": len(state["idempotency_index"]),
        }

    def _load_reconciled_state(self):
        stored_state = self._load_state()
        if self._state_matches_event_file(stored_state):
            return stored_state
        event_state = self._build_state_from_events()["state"]
        self._write_state(event_state)
        return self._load_state()

    def append_event(
        self,
        *,
        event_type,
        aggregate_id,
        payload,
        source="v27_mirror",
        idempotency_key=None,
        observed_at=None,
        available_at=None,
        causal_parent_event_id=None,
    ):
        if not event_type:
            raise V27EventLogError("event_type is required")
        if not aggregate_id:
            raise V27EventLogError("aggregate_id is required")
        if not isinstance(payload, dict):
            raise V27EventLogError("payload must be a dict")

        idempotency_key = idempotency_key or sha256_hex(
            {
                "event_type": event_type,
                "aggregate_id": aggregate_id,
                "payload": payload,
            }
        )

        with self.lock_path.open("a+", encoding="utf-8") as lock_fh:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)
            state = self._load_reconciled_state()

            existing_event_id = state["idempotency_index"].get(idempotency_key)
            if existing_event_id:
                existing = self._read_event_by_id(existing_event_id)
                if existing is None:
                    raise V27EventLogError(f"idempotency index points to missing event: {existing_event_id}")
                return {
                    "status": "duplicate",
                    "event": existing,
                }

            global_seq = int(state["last_global_seq"]) + 1
            aggregate_last = int(state["aggregate_last_seq"].get(aggregate_id, 0))
            aggregate_seq = aggregate_last + 1
            event_id = f"v27evt_{global_seq:012d}_{uuid.uuid4().hex[:12]}"
            now = _now_iso()

            event = {
                "event_id": event_id,
                "event_type": event_type,
                "source": source,
                "global_seq": global_seq,
                "aggregate_id": aggregate_id,
                "aggregate_seq": aggregate_seq,
                "observed_at": observed_at or now,
                "ingested_at": now,
                "available_at": available_at or observed_at or now,
                "causal_parent_event_id": causal_parent_event_id,
                "idempotency_key": idempotency_key,
                "event_schema_version": "v2.7.0.seed",
                "payload": payload,
            }
            event["event_hash"] = sha256_hex({key: value for key, value in event.items() if key != "event_hash"})

            with self.event_path.open("a", encoding="utf-8") as event_fh:
                event_fh.write(json.dumps(event, sort_keys=True, separators=(",", ":")))
                event_fh.write("\n")
                event_fh.flush()
                os.fsync(event_fh.fileno())

            state["last_global_seq"] = global_seq
            state["aggregate_last_seq"][aggregate_id] = aggregate_seq
            state["idempotency_index"][idempotency_key] = event_id
            self._write_state(state)

            return {
                "status": "appended",
                "event": event,
            }

    def iter_events(self):
        if not self.event_path.exists():
            return
        with self.event_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    yield json.loads(line)

    def verify(self):
        summary = self._build_state_from_events()
        return {
            "event_count": summary["event_count"],
            "last_global_seq": summary["last_global_seq"],
            "aggregate_count": summary["aggregate_count"],
            "idempotency_count": summary["idempotency_count"],
        }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", required=True)
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--event-type")
    parser.add_argument("--aggregate-id")
    parser.add_argument("--payload-json", default="{}")
    parser.add_argument("--idempotency-key")
    args = parser.parse_args()

    log = V27EventLog(args.dir)
    if args.verify:
        print(json.dumps(log.verify(), sort_keys=True, indent=2))
        return

    result = log.append_event(
        event_type=args.event_type,
        aggregate_id=args.aggregate_id,
        payload=json.loads(args.payload_json),
        idempotency_key=args.idempotency_key,
    )
    print(json.dumps(result, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
