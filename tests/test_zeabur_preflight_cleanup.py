import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import zeabur_preflight_cleanup as preflight


def test_checkpoint_quarantines_zero_byte_paper_db(tmp_path, monkeypatch):
    paper = tmp_path / "paper_trades.db"
    paper.write_bytes(b"")
    recovery_dir = tmp_path / "recovery"
    monkeypatch.setattr(preflight, "RECOVERY_DIR", recovery_dir)

    preflight.checkpoint_db(paper)

    assert not paper.exists()
    quarantines = list(recovery_dir.glob("paper_trades_corrupt_*"))
    assert len(quarantines) == 1
    manifest = json.loads((quarantines[0] / "manifest.json").read_text())
    assert "zero-byte paper DB" in manifest["reason"]
    moved_names = {item["to"].split("/")[-1] for item in manifest["moved"]}
    assert "paper_trades.db" in moved_names
    assert "paper_trades.db.integrity_error" in moved_names
