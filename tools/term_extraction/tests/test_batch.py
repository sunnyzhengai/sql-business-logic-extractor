"""Tests for tools/term_extraction/batch.py -- corpus walker."""

import csv
import json
from pathlib import Path

from tools.term_extraction.batch import extract_corpus_terms


def test_extract_corpus_writes_json_and_csv(tmp_path):
    views = tmp_path / "views"
    views.mkdir()
    # Two views with overlapping name tokens
    (views / "v_a.sql").write_text(
        "SELECT P.IS_PREGNANT_YN AS Pregnant FROM Clarity.dbo.PATIENT P "
        "WHERE P.STATUS_C = 1"
    )
    (views / "v_b.sql").write_text(
        "SELECT P.IS_PREGNANT_YN AS PregnancyFlag FROM Clarity.dbo.PATIENT P "
        "WHERE P.STATUS_C = 1"
    )

    out = tmp_path / "terms.json"
    extract_corpus_terms(str(views), str(out))

    # JSON
    records = json.loads(out.read_text())
    assert isinstance(records, list)
    assert len(records) >= 2
    pregnant_records = [r for r in records if "pregnant" in r["name_tokens"]]
    assert len(pregnant_records) == 2

    # CSV (sibling file)
    csv_path = out.with_suffix(".csv")
    assert csv_path.is_file()
    with csv_path.open(encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) >= 2
    # name_tokens column is a comma-joined string in the CSV
    pregnant_rows = [r for r in rows if "pregnant" in r["name_tokens"]]
    assert len(pregnant_rows) == 2


def test_extract_corpus_handles_failing_view_gracefully(tmp_path):
    """One bad view should not stop the corpus walk."""
    views = tmp_path / "views"
    views.mkdir()
    (views / "good.sql").write_text(
        "SELECT P.IS_PREGNANT_YN AS Pregnant FROM Clarity.dbo.PATIENT P"
    )
    (views / "bad.sql").write_text("SELECT NOT VALID )))) HERE")

    out = tmp_path / "terms.json"
    extract_corpus_terms(str(views), str(out))

    records = json.loads(out.read_text())
    # Good view's term should still be present even though bad.sql failed.
    assert any(r["view_name"] == "good" for r in records)
