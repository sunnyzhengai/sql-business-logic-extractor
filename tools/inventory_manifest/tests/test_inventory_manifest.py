"""Tests for tools/inventory_manifest."""

import csv
import json
from pathlib import Path

from tools.extract_corpus.batch import extract_corpus
from tools.inventory_manifest.batch import build_inventory_manifest


def _seed_views(views_dir: Path) -> None:
    """A small corpus that exercises:
      - inventory references (Tool 1)
      - JOIN right_table that's a ZC table
      - duplicate references across views (dedup test)"""
    views_dir.mkdir(parents=True, exist_ok=True)
    (views_dir / "v_a.sql").write_text(
        "SELECT P.PAT_ID, Z.NAME AS Race\n"
        "FROM Clarity.dbo.PATIENT P\n"
        "INNER JOIN Clarity.dbo.ZC_PATIENT_RACE Z ON Z.PATIENT_RACE_C = P.RACE_C\n"
        "WHERE P.STATUS_C = 1\n"
    )
    (views_dir / "v_b.sql").write_text(
        "SELECT E.PAT_ENC_CSN_ID, Z.NAME AS ApptType\n"
        "FROM Clarity.dbo.PAT_ENC E\n"
        "INNER JOIN Clarity.dbo.ZC_APPT_STATUS Z ON Z.APPT_STATUS_C = E.APPT_STATUS_C\n"
    )


def _run_manifest(tmp_path: Path):
    views = tmp_path / "views"
    _seed_views(views)
    corpus = tmp_path / "corpus.jsonl"
    extract_corpus(str(views), str(corpus))
    out = tmp_path / "inv_out"
    build_inventory_manifest(str(corpus), str(out))
    return out


def test_used_tables_includes_facts_and_zcs(tmp_path):
    out = _run_manifest(tmp_path)
    tables = (out / "used_tables.txt").read_text().splitlines()
    assert "PATIENT" in tables
    assert "PAT_ENC" in tables
    assert "ZC_PATIENT_RACE" in tables
    assert "ZC_APPT_STATUS" in tables


def test_used_zc_tables_filters_to_zc_only(tmp_path):
    out = _run_manifest(tmp_path)
    zc_tables = (out / "used_zc_tables.txt").read_text().splitlines()
    assert sorted(zc_tables) == ["ZC_APPT_STATUS", "ZC_PATIENT_RACE"]
    # Non-ZC tables should NOT be here
    assert "PATIENT" not in zc_tables
    assert "PAT_ENC" not in zc_tables


def test_used_columns_csv_dedupes(tmp_path):
    out = _run_manifest(tmp_path)
    with (out / "used_columns.csv").open(encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    pairs = {(r["table_name"], r["column_name"]) for r in rows}
    # PATIENT.PAT_ID referenced (SELECT) -- in inventory
    assert ("PATIENT", "PAT_ID") in pairs
    # PAT_ENC.PAT_ENC_CSN_ID
    assert ("PAT_ENC", "PAT_ENC_CSN_ID") in pairs


def test_zc_values_clause_is_paste_ready(tmp_path):
    out = _run_manifest(tmp_path)
    text = (out / "zc_tables_values_clause.sql").read_text()
    # Each line is a VALUES tuple
    assert "('ZC_APPT_STATUS')" in text
    assert "('ZC_PATIENT_RACE')" in text
    # Last entry has NO trailing comma (T-SQL syntax requirement)
    lines = [l.strip() for l in text.splitlines() if l.strip().startswith("('")]
    assert lines, "no values lines emitted"
    assert not lines[-1].endswith(","), f"last line has trailing comma: {lines[-1]}"


def test_cte_aliases_filtered_out(tmp_path):
    """CTE aliases (e.g., `WITH ActivePatients AS (...) SELECT * FROM
    ActivePatients`) appear in the inventory as `table='ActivePatients'`
    -- they are NOT base tables and must be excluded from the manifest."""
    views = tmp_path / "views"
    views.mkdir()
    (views / "v_cte.sql").write_text(
        "WITH ActivePatients AS (\n"
        "  SELECT P.PAT_ID FROM Clarity.dbo.PATIENT P WHERE P.STATUS_C = 1\n"
        ")\n"
        "SELECT AP.PAT_ID FROM ActivePatients AP\n"
    )
    corpus = tmp_path / "corpus.jsonl"
    extract_corpus(str(views), str(corpus))
    out = tmp_path / "inv_out"
    build_inventory_manifest(str(corpus), str(out))
    tables = (out / "used_tables.txt").read_text().splitlines()
    # PATIENT is a real base table
    assert "PATIENT" in tables
    # ActivePatients is a CTE -- must NOT appear
    assert "ActivePatients" not in tables
    assert "ACTIVEPATIENTS" not in [t.upper() for t in tables]


def test_empty_corpus_writes_empty_manifests(tmp_path):
    """A corpus with no views produces empty manifest files (graceful)."""
    corpus = tmp_path / "corpus.jsonl"
    corpus.write_text('{"schema_version": 3, "n_views": 0}\n')
    out = tmp_path / "inv_out"
    build_inventory_manifest(str(corpus), str(out))
    # Files exist but have no entries (just trailing newline)
    assert (out / "used_tables.txt").read_text().strip() == ""
    assert (out / "used_zc_tables.txt").read_text().strip() == ""
