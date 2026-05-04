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


def test_cte_scope_filters_do_not_pollute_main_term(tmp_path):
    """Phase D scope correctness: a CTE-scope filter should NOT appear
    in a Term emitted from the main scope's column. The Term's filter
    list is its OWN scope's filters only."""
    views = tmp_path / "views"
    views.mkdir()
    (views / "v_cte.sql").write_text(
        "WITH ActivePatients AS ("
        "  SELECT P.IS_PREGNANT_YN AS Pregnant"
        "  FROM Clarity.dbo.PATIENT P"
        "  WHERE P.STATUS_C = 1"
        ") "
        "SELECT AP.Pregnant FROM ActivePatients AP "
        "WHERE AP.Pregnant = 'Y'"
    )
    out = tmp_path / "terms.json"
    extract_corpus_terms(str(views), str(out))
    records = json.loads(out.read_text())
    main_records = [r for r in records if r["view_name"] == "v_cte"]
    assert main_records, f"no main-scope term emitted; got {records}"
    main = main_records[0]
    # The main scope owns AP.Pregnant = 'Y' but NOT P.STATUS_C = 1
    main_filters = " | ".join(main["filters"])
    assert "Pregnant = 'Y'" in main_filters
    assert "STATUS_C = 1" not in main_filters


def test_all_scopes_flag_emits_cte_internal_terms(tmp_path):
    """With all_scopes=True, intermediate scope terms appear with a
    scope-suffixed view_name."""
    views = tmp_path / "views"
    views.mkdir()
    (views / "v_cte.sql").write_text(
        "WITH ActivePatients AS ("
        "  SELECT P.IS_PREGNANT_YN AS Pregnant"
        "  FROM Clarity.dbo.PATIENT P"
        "  WHERE P.STATUS_C = 1"
        ") "
        "SELECT AP.Pregnant FROM ActivePatients AP"
    )
    out = tmp_path / "terms.json"
    extract_corpus_terms(str(views), str(out), all_scopes=True)
    records = json.loads(out.read_text())
    cte_records = [r for r in records if r["view_name"].startswith("v_cte#cte:")]
    assert cte_records, f"no CTE-scope term emitted; got {records}"
    # The CTE-scope term carries its own scope's filter, NOT main's
    cte_filters = " | ".join(cte_records[0]["filters"])
    assert "STATUS_C = 1" in cte_filters


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
