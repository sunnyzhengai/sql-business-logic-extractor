"""Tests for tools/view_shape_compare.

The user's stated 4 scenarios:
  - View A and B with same tables + same joins  -> table_identical (in clusters.csv)
  - View C is a subset of A's tables             -> fact_subset (in cross_pairs.csv)
  - View D overlaps A's tables                   -> fact_overlap (in cross_pairs.csv)
  - Plus: dim tables (PATIENT, CLARITY_*, ZC_*) excluded from fact comparison

Plus design extensions:
  - dim_extension when only dim joins differ
  - same_facts_different_joins when fact tables match but joins differ
  - same_driver as a weak signal between otherwise-unrelated pairs
"""

import csv as _csv
import json
from pathlib import Path

import pytest

from tools.extract_corpus.batch import extract_corpus
from tools.view_shape_compare.batch import compare_view_shapes
from tools.view_shape_compare.clusters import (
    FLAG_DIM_EXTENSION,
    FLAG_FACT_OVERLAP,
    FLAG_FACT_SUBSET,
    FLAG_FACT_SUPERSET,
    FLAG_SAME_DRIVER,
    FLAG_SAME_FACTS_DIFFERENT_JOINS,
    FLAG_TABLE_IDENTICAL,
    build_clusters,
)
from tools.view_shape_compare.dim_filter import DimFilter
from tools.view_shape_compare.features import (
    _normalize_join_type,
    _normalize_on,
    view_shape_from_dict,
)


# ---------- dim_filter ----------------------------------------------------

def test_dim_filter_exact_and_prefix():
    df = DimFilter.from_lines(["PATIENT", "ZC_*", "# comment", "", "CLARITY_*"])
    assert df.is_dim("PATIENT")
    assert df.is_dim("patient")
    assert df.is_dim("Clarity.dbo.PATIENT")
    assert df.is_dim("ZC_RACE")
    assert df.is_dim("CLARITY_DEP")
    assert not df.is_dim("ENCOUNTER")
    assert not df.is_dim("")


# ---------- normalization helpers ----------------------------------------

def test_normalize_join_type_collapses_synonyms():
    assert _normalize_join_type("JOIN") == "INNER"
    assert _normalize_join_type("INNER JOIN") == "INNER"
    assert _normalize_join_type("LEFT OUTER JOIN") == "LEFT"
    assert _normalize_join_type("LEFT JOIN") == "LEFT"
    assert _normalize_join_type("CROSS JOIN") == "CROSS"


def test_normalize_on_handles_alias_order():
    a = _normalize_on("E.PAT_ID = P.PAT_ID")
    b = _normalize_on("P.PAT_ID = E.PAT_ID")
    assert a == b
    c = _normalize_on("E.PAT_ID = P.PAT_ID AND E.ENC_ID = X.ENC_ID")
    d = _normalize_on("E.ENC_ID = X.ENC_ID AND E.PAT_ID = P.PAT_ID")
    assert c == d


# ---------- end-to-end: user's 4 scenarios -------------------------------

def _seed_user_scenarios(views_dir: Path) -> None:
    views_dir.mkdir(parents=True, exist_ok=True)
    # View A: ENCOUNTER + ORDER_PROC + DIAGNOSIS
    (views_dir / "v_a.sql").write_text(
        "SELECT E.ENC_ID, O.ORDER_ID, D.DX_ID\n"
        "FROM Clarity.dbo.ENCOUNTER E\n"
        "INNER JOIN Clarity.dbo.ORDER_PROC O ON O.ENC_ID = E.ENC_ID\n"
        "INNER JOIN Clarity.dbo.DIAGNOSIS D ON D.ENC_ID = E.ENC_ID\n"
    )
    # View B: same tables, same joins as A -> table_identical with A
    (views_dir / "v_b.sql").write_text(
        "SELECT E.ENC_ID, O.ORDER_ID, D.DX_ID\n"
        "FROM Clarity.dbo.ENCOUNTER E\n"
        "INNER JOIN Clarity.dbo.ORDER_PROC O ON O.ENC_ID = E.ENC_ID\n"
        "INNER JOIN Clarity.dbo.DIAGNOSIS D ON D.ENC_ID = E.ENC_ID\n"
    )
    # View C: subset of A's facts -> fact_subset relative to A
    (views_dir / "v_c.sql").write_text(
        "SELECT E.ENC_ID, O.ORDER_ID\n"
        "FROM Clarity.dbo.ENCOUNTER E\n"
        "INNER JOIN Clarity.dbo.ORDER_PROC O ON O.ENC_ID = E.ENC_ID\n"
    )
    # View D: overlap with A (shares ENCOUNTER, has new MEDICATION + NOTES)
    (views_dir / "v_d.sql").write_text(
        "SELECT E.ENC_ID, M.MED_ID, N.NOTE_ID\n"
        "FROM Clarity.dbo.ENCOUNTER E\n"
        "INNER JOIN Clarity.dbo.MEDICATION M ON M.ENC_ID = E.ENC_ID\n"
        "INNER JOIN Clarity.dbo.NOTES N ON N.ENC_ID = E.ENC_ID\n"
    )
    # View E: A + PATIENT (dim) -> dim_extension relative to A
    (views_dir / "v_e.sql").write_text(
        "SELECT E.ENC_ID, O.ORDER_ID, D.DX_ID, P.PAT_NAME\n"
        "FROM Clarity.dbo.ENCOUNTER E\n"
        "INNER JOIN Clarity.dbo.ORDER_PROC O ON O.ENC_ID = E.ENC_ID\n"
        "INNER JOIN Clarity.dbo.DIAGNOSIS D ON D.ENC_ID = E.ENC_ID\n"
        "LEFT JOIN Clarity.dbo.PATIENT P ON P.PAT_ID = E.PAT_ID\n"
    )
    # View F: same fact tables as A but different fact join graph
    # (D joins ORDER_PROC instead of ENCOUNTER) -> same_facts_different_joins
    (views_dir / "v_f.sql").write_text(
        "SELECT E.ENC_ID, O.ORDER_ID, D.DX_ID\n"
        "FROM Clarity.dbo.ENCOUNTER E\n"
        "INNER JOIN Clarity.dbo.ORDER_PROC O ON O.ENC_ID = E.ENC_ID\n"
        "INNER JOIN Clarity.dbo.DIAGNOSIS D ON D.ORDER_ID = O.ORDER_ID\n"
    )


def _run_compare(tmp_path: Path):
    views = tmp_path / "views"
    _seed_user_scenarios(views)
    corpus = tmp_path / "corpus.jsonl"
    extract_corpus(str(views), str(corpus))
    out = tmp_path / "shape_out"
    compare_view_shapes(str(corpus), str(out))
    return out


def _read(path: Path):
    with path.open(encoding="utf-8-sig") as f:
        return list(_csv.DictReader(f))


def test_table_identical_clusters_a_and_b(tmp_path):
    out = _run_compare(tmp_path)
    rows = _read(out / "clusters.csv")
    cluster = next(
        r for r in rows
        if "v_a" in r["members"].split("; ") and "v_b" in r["members"].split("; ")
    )
    flags = set(cluster["flags"].split(", "))
    assert FLAG_TABLE_IDENTICAL in flags
    # And nothing else should be in the cluster (v_e adds dim, v_f differs).
    assert set(cluster["members"].split("; ")) == {"v_a", "v_b"}


def test_fact_subset_pair_for_v_c_relative_to_a(tmp_path):
    out = _run_compare(tmp_path)
    pairs = _read(out / "cross_pairs.csv")
    related = [r for r in pairs if {"v_a", "v_c"} == {r["view_a"], r["view_b"]}]
    assert related, f"no v_a/v_c pair in {pairs}"
    flags = {r["flag"] for r in related}
    assert FLAG_FACT_SUBSET in flags or FLAG_FACT_SUPERSET in flags


def test_fact_overlap_pair_for_v_d_relative_to_a(tmp_path):
    out = _run_compare(tmp_path)
    pairs = _read(out / "cross_pairs.csv")
    related = [r for r in pairs if {"v_a", "v_d"} == {r["view_a"], r["view_b"]}]
    assert related, f"no v_a/v_d pair in {pairs}"
    assert any(r["flag"] == FLAG_FACT_OVERLAP for r in related)


def test_dim_extension_pair_for_v_e_relative_to_a(tmp_path):
    out = _run_compare(tmp_path)
    pairs = _read(out / "cross_pairs.csv")
    related = [r for r in pairs if {"v_a", "v_e"} == {r["view_a"], r["view_b"]}]
    assert related, f"no v_a/v_e pair in {pairs}"
    dim_rows = [r for r in related if r["flag"] == FLAG_DIM_EXTENSION]
    assert dim_rows
    # Detail should mention PATIENT (the dim v_e adds)
    assert any("PATIENT" in r["detail"] for r in dim_rows)


def test_same_facts_different_joins_for_v_f_relative_to_a(tmp_path):
    out = _run_compare(tmp_path)
    pairs = _read(out / "cross_pairs.csv")
    related = [r for r in pairs if {"v_a", "v_f"} == {r["view_a"], r["view_b"]}]
    assert related, f"no v_a/v_f pair in {pairs}"
    assert any(r["flag"] == FLAG_SAME_FACTS_DIFFERENT_JOINS for r in related)


def test_features_csv_per_view(tmp_path):
    out = _run_compare(tmp_path)
    feats = _read(out / "features.csv")
    by_name = {r["view_name"]: r for r in feats}
    # PATIENT is dim -> not counted in v_e's facts
    assert "PATIENT" not in by_name["v_e"]["fact_tables"]
    # PATIENT IS counted in v_e's all_tables (n_dims = 1)
    assert int(by_name["v_e"]["n_dims"]) >= 1
    # ENCOUNTER drives all the views in this fixture
    for v in ("v_a", "v_b", "v_c", "v_d", "v_e", "v_f"):
        assert by_name[v]["driver_table"] == "ENCOUNTER"


# ---------- shape extraction unit -----------------------------------------

def test_view_shape_from_dict_skips_views_without_main():
    fake_view = {"view_name": "v_failed", "scopes": []}
    df = DimFilter.empty()
    assert view_shape_from_dict(fake_view, df) is None
