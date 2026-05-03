"""Tests for resolve_all_scoped() — Phase D scope-correct resolution.

The new method must:
- Emit one scope per structural unit (CTE, derived table, subquery,
  set-op branch, main SELECT, lateral).
- NOT propagate filters across scope boundaries. Each scope owns only
  the predicates declared inside it.
- Capture cross-scope dataflow via scope-qualified base_columns and
  reads_from_scopes / reads_from_tables edges.
- Coexist with resolve_all() — that legacy method is unchanged.
"""

import pytest

from sql_logic_extractor.extract import SQLBusinessLogicExtractor, to_dict
from sql_logic_extractor.resolve import (
    LineageResolver,
    ResolvedScope,
    ResolvedScopeTree,
    resolve_query,
)


# ---------- helper -------------------------------------------------------

def _scoped(sql: str) -> ResolvedScopeTree:
    extractor = SQLBusinessLogicExtractor(dialect="tsql")
    logic = to_dict(extractor.extract(sql))
    return LineageResolver(logic).resolve_all_scoped()


def _scope(tree: ResolvedScopeTree, scope_id: str) -> ResolvedScope:
    for s in tree.scopes:
        if s.id == scope_id:
            return s
    raise AssertionError(f"scope {scope_id!r} not in tree; got {[s.id for s in tree.scopes]}")


# ---------- simple cases -------------------------------------------------

def test_simple_select_emits_main_scope():
    tree = _scoped("SELECT P.PAT_ID FROM Clarity.dbo.PATIENT P WHERE P.STATUS_C = 1")
    assert tree.view_outputs == ["main"]
    main = _scope(tree, "main")
    assert main.kind == "main"
    assert any("STATUS_C = 1" in f.expression for f in main.filters)
    # Extractor records the unqualified table name (existing behavior).
    assert "PATIENT" in main.reads_from_tables


def test_simple_select_base_columns_are_scope_qualified():
    tree = _scoped("SELECT P.PAT_ID FROM Clarity.dbo.PATIENT P WHERE P.STATUS_C = 1")
    main = _scope(tree, "main")
    assert any(c.name == "PAT_ID" for c in main.columns)
    pat_id = next(c for c in main.columns if c.name == "PAT_ID")
    # base_columns is "table:Clarity.dbo.PATIENT.PAT_ID" (or whatever the
    # extractor recorded as the base table name)
    assert any(b.startswith("table:") and b.endswith(".PAT_ID")
               for b in pat_id.base_columns), pat_id.base_columns


# ---------- CTE filter scoping (the core fix) ----------------------------

def test_cte_filters_do_not_propagate_to_main():
    """The user's stated requirement: CTE-scope filters stay in the CTE.
    Main scope sees only its own WHERE."""
    sql = """
    WITH ActivePatients AS (
        SELECT P.PAT_ID
        FROM Clarity.dbo.PATIENT P
        WHERE P.STATUS_C = 1
    )
    SELECT AP.PAT_ID
    FROM ActivePatients AP
    WHERE AP.PAT_ID > 100
    """
    tree = _scoped(sql)
    cte = _scope(tree, "cte:ActivePatients")
    main = _scope(tree, "main")

    # CTE owns STATUS_C = 1, NOT the main filter
    cte_exprs = " | ".join(f.expression for f in cte.filters)
    assert "STATUS_C = 1" in cte_exprs
    assert "PAT_ID > 100" not in cte_exprs

    # Main owns PAT_ID > 100, NOT the CTE filter
    main_exprs = " | ".join(f.expression for f in main.filters)
    assert "PAT_ID > 100" in main_exprs
    assert "STATUS_C = 1" not in main_exprs

    # Main's column points at the CTE via scope-qualified base_columns
    main_col = main.columns[0]
    assert any(b.startswith("cte:ActivePatients.") for b in main_col.base_columns), \
        main_col.base_columns
    # And the dataflow edge is recorded
    assert "cte:ActivePatients" in main.reads_from_scopes


def test_nested_ctes_keep_filters_in_their_own_scope():
    sql = """
    WITH C1 AS (
        SELECT P.PAT_ID FROM Clarity.dbo.PATIENT P WHERE P.STATUS_C = 1
    ),
    C2 AS (
        SELECT C1.PAT_ID FROM C1 WHERE C1.PAT_ID > 100
    )
    SELECT C2.PAT_ID FROM C2 WHERE C2.PAT_ID < 1000
    """
    tree = _scoped(sql)
    c1 = " | ".join(f.expression for f in _scope(tree, "cte:C1").filters)
    c2 = " | ".join(f.expression for f in _scope(tree, "cte:C2").filters)
    mn = " | ".join(f.expression for f in _scope(tree, "main").filters)

    assert "STATUS_C = 1" in c1 and "PAT_ID > 100" not in c1 and "< 1000" not in c1
    assert "PAT_ID > 100" in c2 and "STATUS_C = 1" not in c2 and "< 1000" not in c2
    assert "< 1000" in mn and "STATUS_C = 1" not in mn and "PAT_ID > 100" not in mn


# ---------- WHERE-EXISTS gets its own scope ------------------------------

def test_where_exists_creates_subquery_scope():
    sql = """
    SELECT P.PAT_ID
    FROM Clarity.dbo.PATIENT P
    WHERE P.STATUS_C = 1
      AND EXISTS (
            SELECT 1 FROM Clarity.dbo.ENCOUNTER E
            WHERE E.PAT_ID = P.PAT_ID AND E.ENC_DATE > '2024-01-01'
      )
    """
    tree = _scoped(sql)
    ids = [s.id for s in tree.scopes]
    # Main scope present
    assert "main" in ids
    # Some EXISTS-context subquery scope present
    exists_scopes = [s for s in tree.scopes if s.kind == "exists" or s.id.startswith("exists:")]
    assert exists_scopes, f"no exists subquery scope in {ids}"
    # The exists scope owns its OWN filter, not the parent's
    e_filters = " | ".join(f.expression for s in exists_scopes for f in s.filters)
    assert "ENC_DATE > '2024-01-01'" in e_filters
    assert "STATUS_C = 1" not in e_filters


# ---------- Derived table in FROM ----------------------------------------

def test_derived_table_in_from_creates_derived_scope():
    sql = """
    SELECT t.PAT_ID
    FROM (
        SELECT P.PAT_ID
        FROM Clarity.dbo.PATIENT P
        WHERE P.STATUS_C = 1
    ) t
    WHERE t.PAT_ID > 100
    """
    tree = _scoped(sql)
    ids = [s.id for s in tree.scopes]
    assert "main" in ids
    assert "derived:t" in ids, ids

    derived = _scope(tree, "derived:t")
    main = _scope(tree, "main")
    # Filter scoping
    assert any("STATUS_C = 1" in f.expression for f in derived.filters)
    assert not any("PAT_ID > 100" in f.expression for f in derived.filters)
    assert any("PAT_ID > 100" in f.expression for f in main.filters)
    assert not any("STATUS_C = 1" in f.expression for f in main.filters)
    # Dataflow edge
    assert "derived:t" in main.reads_from_scopes


# ---------- UNION at top level -------------------------------------------

def test_top_level_union_emits_branch_scopes():
    sql = """
    SELECT P.PAT_ID FROM Clarity.dbo.PATIENT P WHERE P.STATUS_C = 1
    UNION ALL
    SELECT P.PAT_ID FROM Clarity.dbo.PATIENT P WHERE P.STATUS_C = 2
    """
    tree = _scoped(sql)
    ids = [s.id for s in tree.scopes]
    # Two branch scopes; the kind is union/union_all-derived
    branch_scopes = [s for s in tree.scopes
                      if s.id.startswith("union") or s.id.startswith("union_all")]
    assert len(branch_scopes) >= 2, ids
    assert tree.view_outputs and tree.view_outputs[0] in ids
    # Each branch owns its own filter
    f0 = " | ".join(f.expression for f in branch_scopes[0].filters)
    f1 = " | ".join(f.expression for f in branch_scopes[1].filters)
    assert ("STATUS_C = 1" in f0 and "STATUS_C = 2" not in f0) or \
           ("STATUS_C = 2" in f0 and "STATUS_C = 1" not in f0)
    assert ("STATUS_C = 1" in f1 and "STATUS_C = 2" not in f1) or \
           ("STATUS_C = 2" in f1 and "STATUS_C = 1" not in f1)


# ---------- Backward compat: resolve_all() unchanged ---------------------

def test_resolve_all_still_works_for_simple_view():
    """Legacy flat-form path remains intact."""
    resolved = resolve_query(
        "SELECT P.PAT_ID FROM Clarity.dbo.PATIENT P WHERE P.STATUS_C = 1",
        dialect="tsql",
    )
    assert resolved.columns
    assert resolved.columns[0].name == "PAT_ID"
