"""Tests for tools/view_shape_compare.

Covers:
  - User's 4 stated scenarios (table_identical, fact_subset, fact_overlap)
  - dim_extension (same facts + joins, different dim tables)
  - same_facts_different_joins (same facts, different join graph)
  - CTE-internal tables/joins are aggregated into the view's shape
    (the bug surfaced when the user asked for CTE visibility in JSON)
  - JSON output: pairs.json + features.json with side-by-side diff +
    per-scope decomposition
"""

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
    FLAG_SAME_FACTS_DIFFERENT_JOINS,
    FLAG_TABLE_IDENTICAL,
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
    assert _normalize_on("E.PAT_ID = P.PAT_ID") == _normalize_on("P.PAT_ID = E.PAT_ID")
    a = _normalize_on("E.PAT_ID = P.PAT_ID AND E.ENC_ID = X.ENC_ID")
    b = _normalize_on("E.ENC_ID = X.ENC_ID AND E.PAT_ID = P.PAT_ID")
    assert a == b


# ---------- end-to-end fixtures -------------------------------------------

def _seed_user_scenarios(views_dir: Path) -> None:
    views_dir.mkdir(parents=True, exist_ok=True)
    (views_dir / "v_a.sql").write_text(
        "SELECT E.ENC_ID, O.ORDER_ID, D.DX_ID\n"
        "FROM Clarity.dbo.ENCOUNTER E\n"
        "INNER JOIN Clarity.dbo.ORDER_PROC O ON O.ENC_ID = E.ENC_ID\n"
        "INNER JOIN Clarity.dbo.DIAGNOSIS D ON D.ENC_ID = E.ENC_ID\n"
    )
    (views_dir / "v_b.sql").write_text(
        "SELECT E.ENC_ID, O.ORDER_ID, D.DX_ID\n"
        "FROM Clarity.dbo.ENCOUNTER E\n"
        "INNER JOIN Clarity.dbo.ORDER_PROC O ON O.ENC_ID = E.ENC_ID\n"
        "INNER JOIN Clarity.dbo.DIAGNOSIS D ON D.ENC_ID = E.ENC_ID\n"
    )
    (views_dir / "v_c.sql").write_text(
        "SELECT E.ENC_ID, O.ORDER_ID\n"
        "FROM Clarity.dbo.ENCOUNTER E\n"
        "INNER JOIN Clarity.dbo.ORDER_PROC O ON O.ENC_ID = E.ENC_ID\n"
    )
    (views_dir / "v_d.sql").write_text(
        "SELECT E.ENC_ID, M.MED_ID, N.NOTE_ID\n"
        "FROM Clarity.dbo.ENCOUNTER E\n"
        "INNER JOIN Clarity.dbo.MEDICATION M ON M.ENC_ID = E.ENC_ID\n"
        "INNER JOIN Clarity.dbo.NOTES N ON N.ENC_ID = E.ENC_ID\n"
    )
    (views_dir / "v_e.sql").write_text(
        "SELECT E.ENC_ID, O.ORDER_ID, D.DX_ID, P.PAT_NAME\n"
        "FROM Clarity.dbo.ENCOUNTER E\n"
        "INNER JOIN Clarity.dbo.ORDER_PROC O ON O.ENC_ID = E.ENC_ID\n"
        "INNER JOIN Clarity.dbo.DIAGNOSIS D ON D.ENC_ID = E.ENC_ID\n"
        "LEFT JOIN Clarity.dbo.PATIENT P ON P.PAT_ID = E.PAT_ID\n"
    )
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


def _load_pairs(out_dir: Path):
    return json.loads((out_dir / "pairs.json").read_text())


def _pair_for(pairs_doc, name_a: str, name_b: str):
    """Find the pair JSON for an unordered pair of view names."""
    target = {name_a, name_b}
    for p in pairs_doc["pairs"]:
        if {p["view_a"], p["view_b"]} == target:
            return p
    raise AssertionError(
        f"pair {target} not in pairs.json; got "
        f"{[(p['view_a'], p['view_b']) for p in pairs_doc['pairs']]}"
    )


# ---------- pair-level findings (the user's stated scenarios) ----------

def test_table_identical_pair_a_b(tmp_path):
    out = _run_compare(tmp_path)
    pairs = _load_pairs(out)
    p = _pair_for(pairs, "v_a", "v_b")
    assert FLAG_TABLE_IDENTICAL in p["flags"]
    # Side-by-side: nothing different on any axis
    for axis in ("fact_tables", "dim_tables", "fact_joins"):
        assert p[axis]["only_a"] == []
        assert p[axis]["only_b"] == []


def test_fact_subset_pair_v_c(tmp_path):
    out = _run_compare(tmp_path)
    pairs = _load_pairs(out)
    p = _pair_for(pairs, "v_a", "v_c")
    assert any(f in p["flags"] for f in (FLAG_FACT_SUBSET, FLAG_FACT_SUPERSET))
    # ENCOUNTER and ORDER_PROC are shared; DIAGNOSIS is in only one side.
    shared = set(p["fact_tables"]["shared"])
    only_one = set(p["fact_tables"]["only_a"]) | set(p["fact_tables"]["only_b"])
    assert "ENCOUNTER" in shared and "ORDER_PROC" in shared
    assert "DIAGNOSIS" in only_one


def test_fact_overlap_pair_v_d(tmp_path):
    out = _run_compare(tmp_path)
    pairs = _load_pairs(out)
    p = _pair_for(pairs, "v_a", "v_d")
    assert FLAG_FACT_OVERLAP in p["flags"]
    assert "ENCOUNTER" in p["fact_tables"]["shared"]
    # Each side has unique facts
    assert p["fact_tables"]["only_a"] and p["fact_tables"]["only_b"]


def test_dim_extension_pair_v_e(tmp_path):
    out = _run_compare(tmp_path)
    pairs = _load_pairs(out)
    p = _pair_for(pairs, "v_a", "v_e")
    assert FLAG_DIM_EXTENSION in p["flags"]
    # PATIENT is the dim v_e adds.
    only_one = set(p["dim_tables"]["only_a"]) | set(p["dim_tables"]["only_b"])
    assert "PATIENT" in only_one


def test_same_facts_different_joins_pair_v_f(tmp_path):
    out = _run_compare(tmp_path)
    pairs = _load_pairs(out)
    p = _pair_for(pairs, "v_a", "v_f")
    assert FLAG_SAME_FACTS_DIFFERENT_JOINS in p["flags"]
    # Same fact tables, but fact_joins differ on the DIAGNOSIS join
    assert p["fact_tables"]["only_a"] == [] and p["fact_tables"]["only_b"] == []
    assert p["fact_joins"]["only_a"] or p["fact_joins"]["only_b"]


# ---------- multi-scope aggregation (CTE bug fix) -----------------------

def test_cte_tables_show_up_in_view_shape(tmp_path):
    """A view whose fact tables live INSIDE a CTE must report those
    fact tables at the view level. Before the fix, only main-scope
    tables were counted, so this view had fact_tables = {}."""
    views = tmp_path / "views"
    views.mkdir()
    (views / "v_cte.sql").write_text(
        "WITH ActiveEncs AS ("
        "  SELECT E.ENC_ID, E.PAT_ID FROM Clarity.dbo.ENCOUNTER E "
        "  WHERE E.STATUS_C = 1"
        ") "
        "SELECT AE.ENC_ID FROM ActiveEncs AE"
    )
    corpus = tmp_path / "corpus.jsonl"
    extract_corpus(str(views), str(corpus))
    out = tmp_path / "shape_out"
    compare_view_shapes(str(corpus), str(out))

    feats = json.loads((out / "features.json").read_text())
    view = next(v for v in feats["views"] if v["view_name"] == "v_cte")
    # ENCOUNTER lives inside the CTE; aggregation pulls it up to the view.
    assert "ENCOUNTER" in view["fact_tables"]
    # And it's tagged as coming from the CTE scope in the per-scope detail.
    cte_scope = next(s for s in view["scopes"] if s["id"] == "cte:ActiveEncs")
    assert "ENCOUNTER" in cte_scope["fact_tables"]


def test_pair_includes_per_scope_decomposition(tmp_path):
    """Each pair entry exposes both views' scope trees so readers can
    see WHERE each table came from -- main vs a specific CTE."""
    out = _run_compare(tmp_path)
    pairs = _load_pairs(out)
    p = _pair_for(pairs, "v_a", "v_b")
    assert "scopes_a" in p and "scopes_b" in p
    assert p["scopes_a"][0]["id"] == "main"
    assert "ENCOUNTER" in p["scopes_a"][0]["fact_tables"]


def test_features_json_per_view(tmp_path):
    out = _run_compare(tmp_path)
    feats = json.loads((out / "features.json").read_text())
    by_name = {v["view_name"]: v for v in feats["views"]}
    # PATIENT is dim -> not in v_e's facts
    assert "PATIENT" not in by_name["v_e"]["fact_tables"]
    assert "PATIENT" in by_name["v_e"]["dim_tables"]
    # Driver
    for name in ("v_a", "v_b", "v_c", "v_d", "v_e", "v_f"):
        assert by_name[name]["driver_table"] == "ENCOUNTER"


# ---------- shape extraction unit -----------------------------------------

def test_view_shape_returns_none_for_view_with_no_scopes():
    fake_view = {"view_name": "v_failed", "scopes": []}
    df = DimFilter.empty()
    assert view_shape_from_dict(fake_view, df) is None
