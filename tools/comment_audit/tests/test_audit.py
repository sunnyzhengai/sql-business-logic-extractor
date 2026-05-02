"""Tests for tools/comment_audit -- corpus comment counts + samples CSV."""

import csv
import tempfile
from pathlib import Path

from tools.comment_audit.batch import audit_comments


def test_audit_writes_per_view_and_samples_csvs(tmp_path):
    views = tmp_path / "views"
    views.mkdir()
    (views / "v1.sql").write_text(
        "/* This view returns active members.\n"
        "   Used by the monthly reconciliation job. */\n"
        "SELECT P.STATUS_C  /* Denied */\n"
        "FROM Clarity.dbo.PATIENT P  -- legacy table"
    )
    (views / "v2.sql").write_text(
        "-- TODO fix the join\n"
        "SELECT 1 FROM dual"
    )

    out = tmp_path / "out.csv"
    audit_comments(str(views), str(out))
    samples = tmp_path / "out_samples.csv"

    # Per-view CSV: 2 rows, one per view, with non-zero comment counts.
    with out.open(encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 2
    by_name = {r["view_name"]: r for r in rows}
    assert int(by_name["v1"]["n_comments"]) == 3
    assert int(by_name["v1"]["n_doc"]) == 1
    assert int(by_name["v1"]["n_label"]) == 1
    assert int(by_name["v2"]["n_todo"]) == 1

    # Samples CSV: one row per comment, 4 total (3 from v1, 1 from v2).
    with samples.open(encoding="utf-8-sig") as f:
        sample_rows = list(csv.DictReader(f))
    assert len(sample_rows) == 4
    intents = {r["intent"] for r in sample_rows}
    assert {"label", "todo"}.issubset(intents)


def test_audit_redacts_literals_and_numbers(tmp_path):
    views = tmp_path / "views"
    views.mkdir()
    (views / "v.sql").write_text(
        "SELECT 1  -- patient_id 12345 in batch 67890"
    )
    out = tmp_path / "out.csv"
    audit_comments(str(views), str(out))

    samples = tmp_path / "out_samples.csv"
    with samples.open(encoding="utf-8-sig") as f:
        text = f.read()
    # The numbers 12345 and 67890 must NOT appear in the samples CSV.
    assert "12345" not in text
    assert "67890" not in text


def test_audit_handles_view_with_no_comments(tmp_path):
    views = tmp_path / "views"
    views.mkdir()
    (views / "clean.sql").write_text("SELECT 1 FROM dual")
    out = tmp_path / "out.csv"
    audit_comments(str(views), str(out))

    with out.open(encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    assert int(rows[0]["n_comments"]) == 0
