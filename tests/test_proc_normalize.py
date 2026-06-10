"""Test cases for proc -> view normalization (select_into_to_cte).

Run: python3 -m pytest tests/test_proc_normalize.py -v
"""

import pytest
import sqlglot
from sqlglot import parse_one

from sql_logic_extractor.proc_normalize import (
    ProcNotViewShaped,
    select_into_to_cte,
    _insert_statement_separators,
)


@pytest.mark.parametrize("sql, n_statements", [
    # new statements that need a separator inserted
    ("SELECT a INTO #x FROM b\nSELECT c FROM #x", 2),
    ("SELECT a FROM t\nDROP TABLE #y\nSELECT b FROM u", 3),
    ("MERGE t USING s ON s.k=t.k WHEN MATCHED THEN UPDATE SET t.a=1\nSELECT * FROM t", 2),
    # continuations that must NOT be split
    ("INSERT INTO #t (a) SELECT a FROM s", 1),
    ("WITH c AS (SELECT 1 AS x) SELECT * FROM c", 1),
    ("WITH c AS (SELECT k FROM o) MERGE t USING c ON c.k=t.k WHEN MATCHED THEN UPDATE SET t.a=1", 1),
    ("SELECT a FROM t\nUNION ALL\nSELECT b FROM u", 1),
    ("SELECT a FROM t\nEXCEPT\nSELECT b FROM u", 1),
    ("SELECT * FROM (SELECT a FROM t) z", 1),
    ("MERGE t USING s ON s.k=t.k WHEN MATCHED THEN UPDATE SET t.a=s.a WHEN NOT MATCHED THEN INSERT (a) VALUES (s.a)", 1),
])
def test_statement_separator_counts(sql, n_statements):
    """Tokenizer-based splitter: new statements get a `;`, continuations don't."""
    fixed = _insert_statement_separators(sql, "tsql")
    parsed = [s for s in sqlglot.parse(fixed, dialect="tsql") if s is not None]
    assert len(parsed) == n_statements


def _norm(sql: str) -> str:
    """Round-trip the output through sqlglot so comparisons are
    whitespace/format-insensitive -- we care about structure, not layout."""
    return parse_one(sql, dialect="tsql").sql(dialect="tsql")


# ============================================================
# Happy path -- CTE-shaped procs become view-shaped SELECTs
# ============================================================

def test_single_temp_table():
    sql = """
    CREATE PROCEDURE [rpt].[Foo] AS
    BEGIN
        IF OBJECT_ID('tempdb..#stage') IS NOT NULL DROP TABLE #stage;
        SELECT a, b INTO #stage FROM base WHERE x > 0;
        SELECT a, SUM(b) AS total FROM #stage GROUP BY a;
    END
    """
    out = select_into_to_cte(sql)
    assert out.startswith("CREATE VIEW [rpt].[Foo] AS")
    expected = (
        "WITH stage AS (SELECT a, b FROM base WHERE x > 0) "
        "SELECT a, SUM(b) AS total FROM stage GROUP BY a"
    )
    # The CREATE VIEW prefix isn't parseable as a SELECT, so compare the body.
    body = out.split(" AS\n", 1)[1]
    assert _norm(body) == _norm(expected)


def test_chained_temp_tables_dependency_order():
    """#b is built from #a -> CTEs must be emitted a-before-b."""
    sql = """
    CREATE PROCEDURE rpt.Chain AS
    BEGIN
        SELECT col1, id INTO #a FROM base WHERE y = 1;
        SELECT x, z INTO #b FROM #a a JOIN other o ON o.id = a.id;
        SELECT * FROM #b;
    END
    """
    out = select_into_to_cte(sql)
    body = out.split(" AS\n", 1)[1]
    expected = (
        "WITH a AS (SELECT col1, id FROM base WHERE y = 1), "
        "b AS (SELECT x, z FROM a AS a JOIN other AS o ON o.id = a.id) "
        "SELECT * FROM b"
    )
    assert _norm(body) == _norm(expected)


def test_set_nocount_is_skipped():
    sql = """
    CREATE PROCEDURE rpt.WithSet AS
    BEGIN
        SET NOCOUNT ON;
        SELECT a INTO #t FROM base;
        SELECT a FROM #t;
    END
    """
    out = select_into_to_cte(sql)
    body = out.split(" AS\n", 1)[1]
    assert _norm(body) == _norm("WITH t AS (SELECT a FROM base) SELECT a FROM t")


def test_emit_create_view_false_returns_bare_select():
    sql = """
    CREATE PROCEDURE rpt.Bare AS
    BEGIN
        SELECT a INTO #t FROM base;
        SELECT a FROM #t;
    END
    """
    out = select_into_to_cte(sql, emit_create_view=False)
    assert not out.upper().startswith("CREATE VIEW")
    assert _norm(out) == _norm("WITH t AS (SELECT a FROM base) SELECT a FROM t")


def test_no_wrapper_bare_body():
    """A bare body (no CREATE PROCEDURE) still normalizes; no view prefix."""
    sql = "SELECT a INTO #t FROM base; SELECT a FROM #t;"
    out = select_into_to_cte(sql)
    assert not out.upper().startswith("CREATE VIEW")  # no proc name recovered
    assert _norm(out) == _norm("WITH t AS (SELECT a FROM base) SELECT a FROM t")


# ============================================================
# Constraint violations -> ProcNotViewShaped (with stable reason)
# ============================================================

def test_insert_into_temp_rejected():
    """A second write to a temp = accumulation, not CTE-equivalent."""
    sql = """
    CREATE PROCEDURE rpt.Acc AS
    BEGIN
        SELECT a INTO #t FROM base;
        INSERT INTO #t SELECT a FROM archive;
        SELECT a FROM #t;
    END
    """
    with pytest.raises(ProcNotViewShaped) as ei:
        select_into_to_cte(sql)
    assert ei.value.reason == "unsupported_statement"


def test_update_temp_rejected():
    sql = """
    CREATE PROCEDURE rpt.Upd AS
    BEGIN
        SELECT a, b INTO #t FROM base;
        UPDATE #t SET b = 0;
        SELECT a, b FROM #t;
    END
    """
    with pytest.raises(ProcNotViewShaped) as ei:
        select_into_to_cte(sql)
    assert ei.value.reason == "unsupported_statement"


def test_select_into_persistent_table_rejected():
    sql = """
    CREATE PROCEDURE etl.Load AS
    BEGIN
        SELECT a, b INTO dbo.RealTable FROM base;
        SELECT a FROM dbo.RealTable;
    END
    """
    with pytest.raises(ProcNotViewShaped) as ei:
        select_into_to_cte(sql)
    assert ei.value.reason == "select_into_persistent"


def test_temp_redefined_rejected():
    sql = """
    CREATE PROCEDURE rpt.Redef AS
    BEGIN
        SELECT a INTO #t FROM base;
        SELECT a FROM #t;
        SELECT b INTO #t FROM other;
        SELECT b FROM #t;
    END
    """
    with pytest.raises(ProcNotViewShaped) as ei:
        select_into_to_cte(sql)
    # Two terminal SELECTs OR a redefinition -- either way it's not a view.
    assert ei.value.reason in ("temp_redefined", "multiple_terminal_selects")


def test_multiple_terminal_selects_rejected():
    sql = """
    CREATE PROCEDURE rpt.Multi AS
    BEGIN
        SELECT a INTO #t FROM base;
        SELECT a FROM #t;
        SELECT a * 2 FROM #t;
    END
    """
    with pytest.raises(ProcNotViewShaped) as ei:
        select_into_to_cte(sql)
    assert ei.value.reason == "multiple_terminal_selects"


def test_no_terminal_select_rejected():
    sql = """
    CREATE PROCEDURE rpt.NoOut AS
    BEGIN
        SELECT a INTO #t FROM base;
    END
    """
    with pytest.raises(ProcNotViewShaped) as ei:
        select_into_to_cte(sql)
    assert ei.value.reason == "no_terminal_select"


def test_undefined_temp_reference_rejected():
    """Reading a temp staged outside this proc -> not self-contained."""
    sql = """
    CREATE PROCEDURE rpt.External AS
    BEGIN
        SELECT a FROM #staged_elsewhere;
    END
    """
    with pytest.raises(ProcNotViewShaped) as ei:
        select_into_to_cte(sql)
    assert ei.value.reason == "undefined_temp_reference"


# ============================================================
# Missing statement separators -- tokenizer-based splitting
# (T-SQL omits `;`; sqlglot needs it). Regression guards.
# ============================================================

def test_semicolonless_stage_and_read():
    """No semicolons between statements (common T-SQL) still normalizes."""
    sql = """
    CREATE PROCEDURE rpt.NoSemis AS
    BEGIN
        SELECT a, b INTO #stage FROM base LEFT OUTER JOIN x ON x.k = base.k
        IF OBJECT_ID('tempdb..#stage2') IS NOT NULL DROP TABLE #stage2;
        SELECT a INTO #stage2 FROM #stage
        SELECT a FROM #stage2
    END
    """
    out = select_into_to_cte(sql)
    body = out.split(" AS\n", 1)[1]
    assert "stage" in _norm(body) and "stage2" in _norm(body)


def test_union_all_not_split():
    """A UNION ALL in the terminal SELECT must NOT be split into two statements
    (regression: the splitter once cut after the ALL token)."""
    sql = """
    CREATE PROCEDURE rpt.U AS
    BEGIN
        SELECT a INTO #x FROM base
        SELECT a FROM #x
        UNION ALL
        SELECT a FROM other
    END
    """
    out = select_into_to_cte(sql)          # must not raise multiple_terminal_selects
    assert "UNION ALL" in _norm(out)
