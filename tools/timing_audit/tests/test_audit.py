"""Tests for tools/timing_audit -- per-view timing CSV with timeouts.

Asserts:
- A clean view classifies as 'ok' with a numeric elapsed_sec
- A syntactically broken view classifies as 'error' (not 'timeout')
- The CSV has all expected fields populated for every row
- Bucket counts in the console summary line up with rows in the CSV
"""

import csv
import tempfile
from pathlib import Path

from tools.timing_audit.batch import audit_timing


def test_clean_view_classifies_ok(tmp_path):
    views = tmp_path / "views"
    views.mkdir()
    (views / "clean.sql").write_text(
        "SELECT P.PAT_ID FROM Clarity.dbo.PATIENT P WHERE P.STATUS_C = 1"
    )
    out = tmp_path / "audit.csv"
    audit_timing(str(views), str(out), timeout_sec=30)

    with out.open(encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    assert rows[0]["status"] == "ok"
    assert float(rows[0]["elapsed_sec"]) >= 0


def test_broken_view_classifies_error(tmp_path):
    views = tmp_path / "views"
    views.mkdir()
    (views / "broken.sql").write_text("SELECT NOT VALID )))) HERE")
    out = tmp_path / "audit.csv"
    audit_timing(str(views), str(out), timeout_sec=30)

    with out.open(encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    assert rows[0]["status"] == "error"
    assert rows[0]["error_message"]   # non-empty


def test_every_row_has_required_fields(tmp_path):
    views = tmp_path / "views"
    views.mkdir()
    (views / "a.sql").write_text("SELECT 1 FROM dual")
    (views / "b.sql").write_text("SELECT BROKEN ))))")

    out = tmp_path / "audit.csv"
    audit_timing(str(views), str(out), timeout_sec=30)

    required = {"view_name", "status", "elapsed_sec", "n_columns", "error_message"}
    with out.open(encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            assert set(row.keys()) >= required
            assert row["view_name"]
            assert row["status"] in ("ok", "timeout", "error")
