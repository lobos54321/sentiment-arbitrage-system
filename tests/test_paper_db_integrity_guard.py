import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from paper_db_integrity_guard import (
    PaperDBIntegrityMarked,
    paper_db_integrity_marker_path,
    require_unmarked_paper_db,
)


def test_require_unmarked_paper_db_blocks_marked_paper_db(tmp_path):
    paper = tmp_path / "paper_trades.db"
    paper.write_bytes(b"SQLite format 3\x00")
    paper_db_integrity_marker_path(paper).write_text(
        "context=pending_entry\nerror=database disk image is malformed\n",
        encoding="utf-8",
    )

    with pytest.raises(PaperDBIntegrityMarked, match="pending_entry"):
        require_unmarked_paper_db(paper, component="unit_test")


def test_require_unmarked_paper_db_ignores_non_paper_db(tmp_path):
    db = tmp_path / "sentiment_arb.db"
    db.write_bytes(b"")
    paper_db_integrity_marker_path(db).write_text("marker", encoding="utf-8")

    require_unmarked_paper_db(db, component="unit_test")
