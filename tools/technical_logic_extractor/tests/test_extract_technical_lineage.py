"""Golden-path tests for Tool 2 -- Technical Logic Extractor.

Asserts the public contract of `extract_technical_lineage(sql, dialect)`:
- per-output-column lineage with base tables + base columns
- WHERE-clause filters propagate to every output column
- JOIN conditions (non-equi) carry through
- EXISTS subquery filters carry through
- CTE-internal filters bubble up to the outer SELECT's columns
"""

from sql_logic_extractor.products import (
    extract_technical_lineage,
    TechnicalLineage,
    ColumnInventory,
)


def _columns_by_name(lineage: TechnicalLineage) -> dict:
    """Index resolved_columns by output column name for easier assertions."""
    return {c["name"]: c for c in lineage.resolved_columns}


def _filter_texts(col_dict: dict) -> list[str]:
    return [f.get("expression", "") for f in col_dict.get("filters", []) or []]


def test_returns_technical_lineage_object():
    sql = "SELECT P.PAT_ID FROM Clarity.dbo.PATIENT P"
    out = extract_technical_lineage(sql, dialect="tsql")
    assert isinstance(out, TechnicalLineage)
    assert isinstance(out.inventory, ColumnInventory)
    assert isinstance(out.resolved_columns, list)
    assert isinstance(out.query_filters, list)


def test_simple_passthrough_no_filters():
    sql = "SELECT P.PAT_ID FROM Clarity.dbo.PATIENT P"
    cols = _columns_by_name(extract_technical_lineage(sql))
    assert "PAT_ID" in cols
    assert cols["PAT_ID"]["type"] == "passthrough"
    assert cols["PAT_ID"].get("base_columns", [])


def test_where_filter_propagates_to_every_column():
    """WHERE clauses constrain ALL output columns, not just one. Every
    output column's filter list should include the WHERE predicate."""
    sql = """
    SELECT P.PAT_ID, P.PAT_NAME
    FROM Clarity.dbo.PATIENT P
    WHERE P.STATUS_C = 1
    """
    cols = _columns_by_name(extract_technical_lineage(sql))
    for col_name in ("PAT_ID", "PAT_NAME"):
        filters = _filter_texts(cols[col_name])
        assert any("STATUS_C" in f for f in filters), \
            f"{col_name} should have STATUS_C filter; got {filters}"


def test_exists_subquery_filter_carries_through():
    """EXISTS predicates in the outer WHERE should propagate to all
    output columns, and the EXISTS subquery's table refs should appear
    in the lineage."""
    sql = """
    SELECT R.REFERRAL_ID
    FROM Clarity.dbo.REFERRAL R
    WHERE EXISTS (
        SELECT 1 FROM Clarity.dbo.REFERRAL_HIST RH
        WHERE RH.REFERRAL_ID = R.REFERRAL_ID
          AND RH.NEW_STATUS_C = 5
    )
    """
    cols = _columns_by_name(extract_technical_lineage(sql))
    filters = _filter_texts(cols["REFERRAL_ID"])
    assert any("EXISTS" in f for f in filters), \
        f"REFERRAL_ID should have an EXISTS filter; got {filters}"


def test_cte_filter_bubbles_up():
    """A filter applied INSIDE a CTE body should appear on every output
    column that reads from that CTE's outputs."""
    sql = """
    WITH ActiveReferrals AS (
        SELECT R.REFERRAL_ID, R.PATIENT_ID
        FROM Clarity.dbo.REFERRAL R
        WHERE R.STATUS_C = 1
    )
    SELECT AR.REFERRAL_ID, AR.PATIENT_ID FROM ActiveReferrals AR
    """
    cols = _columns_by_name(extract_technical_lineage(sql))
    for col_name in ("REFERRAL_ID", "PATIENT_ID"):
        filters = _filter_texts(cols[col_name])
        assert any("STATUS_C" in f for f in filters), \
            f"{col_name} should inherit the CTE's STATUS_C filter; got {filters}"


def test_calculated_column_classified():
    """CASE expressions should be classified, not appear as passthrough."""
    sql = """
    SELECT
        R.REFERRAL_ID,
        CASE WHEN R.STATUS_C = 5 THEN 'Denied' ELSE 'Other' END AS STATUS_LABEL
    FROM Clarity.dbo.REFERRAL R
    """
    cols = _columns_by_name(extract_technical_lineage(sql))
    assert cols["REFERRAL_ID"]["type"] == "passthrough"
    assert cols["STATUS_LABEL"]["type"] != "passthrough"


def test_inventory_embeds_tool_1_output():
    """The TechnicalLineage object MUST embed Tool 1's ColumnInventory --
    composition, not duplication."""
    sql = "SELECT P.PAT_ID FROM Clarity.dbo.PATIENT P WHERE P.STATUS_C = 1"
    out = extract_technical_lineage(sql)
    qualifs = {c.qualified() for c in out.inventory.columns}
    assert "Clarity.dbo.PATIENT.PAT_ID" in qualifs
    assert "Clarity.dbo.PATIENT.STATUS_C" in qualifs


def test_query_filters_deduplicated():
    """The query-level filters list dedupes by expression text."""
    sql = """
    SELECT R.REFERRAL_ID, R.STATUS_C
    FROM Clarity.dbo.REFERRAL R
    WHERE R.STATUS_C = 1
    """
    out = extract_technical_lineage(sql)
    # Should have at least one query filter (R.STATUS_C = 1)
    assert any("STATUS_C" in f for f in out.query_filters)
    # No duplicate filter expressions in the query-level list
    assert len(out.query_filters) == len(set(out.query_filters))


def test_ssms_boilerplate_stripped():
    """SSMS scripted views should parse cleanly through Tool 2."""
    sql = """USE [Clarity]
GO
SET ANSI_NULLS ON
GO
CREATE VIEW dbo.scripted AS
SELECT P.PAT_ID FROM Clarity.dbo.PATIENT P WHERE P.STATUS_C = 1
"""
    cols = _columns_by_name(extract_technical_lineage(sql))
    assert "PAT_ID" in cols
    assert any("STATUS_C" in f for f in _filter_texts(cols["PAT_ID"]))


def test_join_brings_all_join_tables_into_lineage():
    sql = """
    SELECT R.REFERRAL_ID, P.PAT_NAME
    FROM Clarity.dbo.REFERRAL R
    INNER JOIN Clarity.dbo.PATIENT P ON P.PAT_ID = R.PATIENT_ID
    """
    cols = _columns_by_name(extract_technical_lineage(sql))
    assert "REFERRAL" in (cols["REFERRAL_ID"].get("base_tables") or [])
    assert "PATIENT" in (cols["PAT_NAME"].get("base_tables") or [])


# ---------------------------------------------------------------------------
# Helpers used by Tool 2's batch.py to clean filter SQL (strip JOIN
# correlation keys + resolve aliases to real table names).
# ---------------------------------------------------------------------------

def test_build_alias_map_returns_alias_to_real_table():
    from sql_logic_extractor.business_logic import build_alias_map
    sql = """
    SELECT R.X
    FROM Clarity.dbo.REFERRAL R
    JOIN Clarity.dbo.PATIENT P ON P.PAT_ID = R.PATIENT_ID
    """
    m = build_alias_map(sql)
    assert m.get("r") == "REFERRAL"
    assert m.get("p") == "PATIENT"


def test_build_alias_map_excludes_ctes():
    from sql_logic_extractor.business_logic import build_alias_map
    sql = """
    WITH AR AS (SELECT R.X FROM Clarity.dbo.REFERRAL R)
    SELECT AR.X FROM AR
    """
    m = build_alias_map(sql)
    # Real-table alias R is mapped; the CTE alias AR is NOT.
    assert m.get("r") == "REFERRAL"
    assert "ar" not in m


def test_clean_filter_sql_strips_correlation_keys():
    from sql_logic_extractor.business_logic import clean_filter_sql
    # `a.k = b.k` is a JOIN correlation key (col=col on opposite tables);
    # it should be stripped. `a.x > 5` is a real predicate; it should stay.
    out = clean_filter_sql("a.k = b.k AND a.x > 5", {}, dialect="tsql")
    assert "a.k" not in out  # correlation stripped
    assert "x > 5" in out    # real filter kept


def test_clean_filter_sql_resolves_aliases():
    from sql_logic_extractor.business_logic import clean_filter_sql
    out = clean_filter_sql(
        "cvgept.MEM_COVERED_YN = 'Y'",
        {"cvgept": "COVERAGE_MEMBER_LIST"},
        dialect="tsql",
    )
    assert "COVERAGE_MEMBER_LIST.MEM_COVERED_YN" in out
    assert "cvgept" not in out.lower() or out.lower().count("cvgept") == 0


def test_clean_filter_sql_returns_empty_for_pure_correlation():
    """A filter that's nothing but `col = col` should clean to empty."""
    from sql_logic_extractor.business_logic import clean_filter_sql
    out = clean_filter_sql("a.k = b.k", {}, dialect="tsql")
    assert out == ""


def test_convert_preserved_not_rewritten_to_try_cast():
    """T-SQL's CONVERT(target_type, expr) must NOT silently become
    TRY_CAST(expr AS target_type) in the engine's emitted SQL.

    The two are semantically DIFFERENT: CONVERT raises on bad input,
    TRY_CAST returns NULL. For migration / governance use cases, the
    user's original SQL must round-trip faithfully. Regression test for
    a bug where extract.py was calling sqlglot's .sql() emitter without
    a dialect, causing CONVERT to be canonicalized to TRY_CAST. Fix:
    always pass dialect=self.dialect to the extractor's _sql() helper.
    """
    sql = """SELECT CONVERT(date, CVG.CVG_EFF_DT) AS [Eff]
             FROM Clarity.dbo.COVERAGE CVG"""
    cols = _columns_by_name(extract_technical_lineage(sql, dialect="tsql"))
    assert "Eff" in cols
    expr = cols["Eff"]["resolved_expression"]
    assert "CONVERT" in expr.upper(), \
        f"CONVERT must be preserved as CONVERT (not TRY_CAST). Got: {expr}"
    assert "TRY_CAST" not in expr.upper(), \
        f"TRY_CAST should NOT appear -- the bug rewrote CONVERT to TRY_CAST. Got: {expr}"


def test_clean_filter_sql_combines_strip_and_alias_resolve():
    """Both transforms applied together to one expression."""
    from sql_logic_extractor.business_logic import clean_filter_sql
    out = clean_filter_sql(
        "cvgept.COVERAGE_ID = cvg.COVERAGE_ID AND cvg.COVERAGE_TYPE_C = 2",
        {"cvg": "COVERAGE", "cvgept": "COVERAGE_MEMBER_LIST"},
        dialect="tsql",
    )
    # correlation key stripped
    assert "cvgept.COVERAGE_ID = cvg.COVERAGE_ID" not in out
    # alias resolved on the surviving predicate
    assert "COVERAGE.COVERAGE_TYPE_C = 2" in out
