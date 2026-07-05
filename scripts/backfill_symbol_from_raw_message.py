#!/usr/bin/env python3
"""Backfill UNKNOWN symbols from preserved raw_message text (N1, INSTRUMENTATION).

Root cause being repaired: the NEW_TRENDING symbol regex anchored on a leading
`📍` emoji while the channel now sends `🪙 SYMBOL：$xxx`, so ~31% of
premium_signals and ~70% of raw_signal_outcomes carry symbol='UNKNOWN' even
though the symbol is present verbatim in raw_message. The live parser is fixed
in src/inputs/premium-channel-listener.js; this script repairs history.

Scope: metadata-only. Touches ONLY the `symbol` (+ new `symbol_source` audit
column) of rows currently UNKNOWN/empty. Never touches gates, strategy,
decisions, outcomes, prices, or tiers. Default is --dry-run; writes require
--execute. Idempotent: a second --execute run updates 0 rows.

Usage:
  python3 scripts/backfill_symbol_from_raw_message.py --dry-run   # default
  python3 scripts/backfill_symbol_from_raw_message.py --execute
  python3 scripts/backfill_symbol_from_raw_message.py --self-test
"""
from __future__ import annotations
import argparse, json, os, re, sqlite3, sys, tempfile

SYMBOL_FIELD_RE = re.compile(r"SYMBOL\s*[：:]\s*\$?([^\s\n]+)", re.I)
BOLD_HEADER_RE = re.compile(r"\*\*([^*\n]{1,32})\*\*\s*New\s+Trending", re.I)
ATH_RE = re.compile(r"(?:New\s+)?ATH\s+\$([^\s`*]+)", re.I)
TRAILING_JUNK_RE = re.compile(r"[*,)\]|]+$")
UNKNOWN_SQL = "(symbol IS NULL OR symbol='' OR upper(symbol)='UNKNOWN')"


def extract_symbol(raw: str):
    if not raw:
        return None
    for pattern in (SYMBOL_FIELD_RE, BOLD_HEADER_RE, ATH_RE):
        m = pattern.search(raw)
        if m:
            sym = TRAILING_JUNK_RE.sub("", m.group(1).strip())
            if 0 < len(sym) <= 32:
                return sym
    return None


def ensure_column(db, table, column, decl="TEXT"):
    cols = [r[1] for r in db.execute(f"PRAGMA table_info({table})")]
    if column not in cols:
        db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")


def backfill_premium(db, execute):
    rows = db.execute(
        f"SELECT id, raw_message FROM premium_signals "
        f"WHERE {UNKNOWN_SQL} AND raw_message IS NOT NULL AND raw_message != ''"
    ).fetchall()
    updates, failures = [], 0
    for pid, raw in rows:
        sym = extract_symbol(raw)
        if sym:
            updates.append((sym, pid))
        else:
            failures += 1
    if execute and updates:
        ensure_column(db, "premium_signals", "symbol_source")
        db.executemany(
            "UPDATE premium_signals SET symbol=?, symbol_source='backfill_v2' WHERE id=?",
            updates,
        )
        db.commit()
    return {"candidates": len(rows), "extracted": len(updates), "unextractable": failures}


def backfill_outcomes(outcomes_db, premium_db_path, execute):
    """Propagate known symbols from premium_signals into raw_signal_outcomes
    (join on raw_signal_outcomes.signal_id == premium_signals.id)."""
    # Plain-path attach (URI form breaks on non-URI connections); only SELECTs run
    # against the attached premium DB, so read-only semantics are preserved by use.
    outcomes_db.execute("ATTACH DATABASE ? AS prem", (premium_db_path,))
    rows = outcomes_db.execute(
        f"SELECT o.id, p.symbol FROM raw_signal_outcomes o "
        f"JOIN prem.premium_signals p ON CAST(o.signal_id AS INTEGER) = p.id "
        f"WHERE (o.symbol IS NULL OR o.symbol='' OR upper(o.symbol)='UNKNOWN') "
        f"AND p.symbol IS NOT NULL AND p.symbol != '' AND upper(p.symbol) != 'UNKNOWN'"
    ).fetchall()
    if execute and rows:
        ensure_column(outcomes_db, "raw_signal_outcomes", "symbol_source")
        outcomes_db.executemany(
            "UPDATE raw_signal_outcomes SET symbol=?, symbol_source='backfill_v2_from_premium' WHERE id=?",
            [(sym, oid) for oid, sym in rows],
        )
        outcomes_db.commit()
    outcomes_db.execute("DETACH DATABASE prem")
    return {"propagated": len(rows)}


def unknown_stats(db, table, with_raw_message=False):
    total = db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    unk = db.execute(f"SELECT count(*) FROM {table} WHERE {UNKNOWN_SQL}").fetchone()[0]
    out = {"total": total, "unknown": unk, "unknown_rate": round(unk / total, 4) if total else 0}
    if with_raw_message:
        denom = db.execute(
            f"SELECT count(*) FROM {table} WHERE raw_message IS NOT NULL AND raw_message != ''"
        ).fetchone()[0]
        unk_rm = db.execute(
            f"SELECT count(*) FROM {table} WHERE {UNKNOWN_SQL} AND raw_message IS NOT NULL AND raw_message != ''"
        ).fetchone()[0]
        out["with_raw_message"] = denom
        out["unknown_rate_given_raw_message"] = round(unk_rm / denom, 4) if denom else 0
    return out


def run(premium_path, outcomes_path, execute):
    report = {"schema_version": "symbol_backfill.v2", "mode": "execute" if execute else "dry_run",
              "allowed_scope": "symbol_metadata_only", "promotion_allowed": False}
    prem = sqlite3.connect(premium_path if execute else f"file:{premium_path}?mode=ro", uri=not execute)
    prem.execute("PRAGMA busy_timeout=15000")  # live writers share these DBs
    report["premium_before"] = unknown_stats(prem, "premium_signals", with_raw_message=True)
    report["premium_backfill"] = backfill_premium(prem, execute)
    if execute:
        report["premium_after"] = unknown_stats(prem, "premium_signals", with_raw_message=True)
    prem.close()
    out = sqlite3.connect(outcomes_path if execute else f"file:{outcomes_path}?mode=ro", uri=not execute)
    out.execute("PRAGMA busy_timeout=15000")
    report["outcomes_before"] = unknown_stats(out, "raw_signal_outcomes")
    report["outcomes_backfill"] = backfill_outcomes(out, premium_path, execute)
    if execute:
        report["outcomes_after"] = unknown_stats(out, "raw_signal_outcomes")
    out.close()
    return report


def self_test():
    tmp = tempfile.mkdtemp()
    ppath, opath = os.path.join(tmp, "prem.db"), os.path.join(tmp, "out.db")
    p = sqlite3.connect(ppath)
    p.execute("CREATE TABLE premium_signals (id INTEGER PRIMARY KEY, symbol TEXT, raw_message TEXT)")
    p.executemany("INSERT INTO premium_signals (id, symbol, raw_message) VALUES (?,?,?)", [
        (1, "UNKNOWN", "🔥 **emilio** New Trending \n\n🪙 SYMBOL：$emilio\n🏦 MC: 19K"),
        (2, "UNKNOWN", "🔥 **Chaton** New Trending \n\n🏦 MC: 54K"),          # header fallback
        (3, "UNKNOWN", "📈New ATH $KIRBY is up **51%** 📈"),                    # ATH form
        (4, "KEEP", "🪙 SYMBOL：$SHOULDNOTCHANGE"),                             # already known
        (5, "UNKNOWN", "no symbol anywhere here"),                              # unextractable
        (6, "UNKNOWN", None),                                                   # no raw_message
    ])
    p.commit(); p.close()
    o = sqlite3.connect(opath)
    o.execute("CREATE TABLE raw_signal_outcomes (id INTEGER PRIMARY KEY, signal_id TEXT, symbol TEXT)")
    o.executemany("INSERT INTO raw_signal_outcomes (id, signal_id, symbol) VALUES (?,?,?)", [
        (10, "1", "UNKNOWN"), (11, "2", "UNKNOWN"), (12, "4", "UNKNOWN"),
        (13, "5", "UNKNOWN"),                       # premium also unextractable -> stays UNKNOWN
        (14, "1", "ALREADYSET"),                    # known -> untouched
    ])
    o.commit(); o.close()

    dry = run(ppath, opath, execute=False)
    assert dry["premium_backfill"]["extracted"] == 3, dry
    r1 = run(ppath, opath, execute=True)
    p = sqlite3.connect(ppath)
    got = dict(p.execute("SELECT id, symbol FROM premium_signals").fetchall())
    assert got[1] == "emilio" and got[2] == "Chaton" and got[3] == "KIRBY", got
    assert got[4] == "KEEP" and got[5] == "UNKNOWN", got
    src = dict(p.execute("SELECT id, symbol_source FROM premium_signals").fetchall())
    assert src[1] == "backfill_v2" and src[4] is None, src
    p.close()
    o = sqlite3.connect(opath)
    ogot = dict(o.execute("SELECT id, symbol FROM raw_signal_outcomes").fetchall())
    assert ogot[10] == "emilio" and ogot[11] == "Chaton" and ogot[12] == "KEEP", ogot
    assert ogot[13] == "UNKNOWN" and ogot[14] == "ALREADYSET", ogot
    o.close()
    r2 = run(ppath, opath, execute=True)   # idempotency
    assert r2["premium_backfill"]["extracted"] == 0, r2
    assert r2["outcomes_backfill"]["propagated"] == 0, r2
    print("SELF-TEST PASSED: extraction (field/header/ATH), known rows untouched, "
          "propagation joined, audit column set, idempotent second run")
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--premium-db", default="/app/data/sentiment_arb.db")
    ap.add_argument("--outcomes-db", default="/app/data/raw_signal_outcomes.db")
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--dry-run", action="store_true", help="default behavior; explicit flag allowed")
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--out")
    a = ap.parse_args()
    if a.self_test:
        return 0 if self_test() else 1
    report = run(a.premium_db, a.outcomes_db, execute=a.execute)
    text = json.dumps(report, indent=2)
    if a.out:
        with open(a.out, "w") as fh:
            fh.write(text)
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
