"""Golden-path tests for Tool 1 -- Column Lineage Extractor.

Asserts the public contract of `extract_columns(sql, dialect)` against
the SQL shapes that real production views actually use.

Each test asserts on the SET of (database, schema, table, column) tuples
returned, ignoring order, so the test isn't brittle to traversal order
changes inside the engine."""

from sql_logic_extractor.products import extract_columns, ColumnIdentifier


def _tuples(inventory) -> set[tuple]:
    """Convert inventory to a set of (db, schema, table, column) for comparison."""
    return {(c.database or "", c.schema or "", c.table, c.column)
            for c in inventory.columns}


def test_three_part_name_captured():
    """Database.schema.table qualification should populate all three slots."""
    sql = """
    SELECT P.PAT_ID, P.PAT_NAME
    FROM Clarity.dbo.PATIENT P
    """
    got = _tuples(extract_columns(sql))
    assert ("Clarity", "dbo", "PATIENT", "PAT_ID") in got
    assert ("Clarity", "dbo", "PATIENT", "PAT_NAME") in got


def test_two_part_name_captured():
    """schema.table (no database) should leave database empty."""
    sql = "SELECT H.PAT_ID, H.ADMIT_DATE FROM dbo.HSP_ACCOUNT H"
    got = _tuples(extract_columns(sql))
    assert ("", "dbo", "HSP_ACCOUNT", "PAT_ID") in got
    assert ("", "dbo", "HSP_ACCOUNT", "ADMIT_DATE") in got


def test_join_with_aliases_resolves():
    """Aliases in JOIN clauses should resolve to the underlying tables."""
    sql = """
    SELECT R.REFERRAL_ID, P.PAT_NAME, H.ADMIT_DATE
    FROM Clarity.dbo.REFERRAL R
    INNER JOIN Clarity.dbo.PATIENT P ON P.PAT_ID = R.PATIENT_ID
    LEFT  JOIN dbo.HSP_ACCOUNT H ON H.PAT_ID = R.PATIENT_ID
    """
    got = _tuples(extract_columns(sql))
    # Every column should resolve to its real table, never to the alias name
    aliases = {"r", "p", "h"}
    assert all(t.lower() not in aliases for _, _, t, _ in got), \
        f"Found alias-named tables: {got}"
    assert ("Clarity", "dbo", "REFERRAL", "REFERRAL_ID") in got
    assert ("Clarity", "dbo", "PATIENT", "PAT_NAME") in got
    assert ("", "dbo", "HSP_ACCOUNT", "ADMIT_DATE") in got


def test_self_join_dedups_to_one_table():
    """Two aliases for the same underlying table should not produce
    duplicate rows when the same column is selected through both."""
    sql = """
    SELECT C1.X, C2.X
    FROM Clarity.dbo.C C1
    JOIN Clarity.dbo.C C2 ON C1.K = C2.K
    """
    got = _tuples(extract_columns(sql))
    rows_for_x = [r for r in got if r[3] == "X"]
    assert len(rows_for_x) == 1, f"Expected 1 row for X, got: {rows_for_x}"
    assert ("Clarity", "dbo", "C", "X") in got


def test_two_different_tables_same_column_kept_separate():
    """Same column name on different tables should produce TWO distinct rows."""
    sql = """
    SELECT A.STREET, B.STREET
    FROM Clarity.dbo.PRIMARY_ADDRESS A
    JOIN Clarity.dbo.SECONDARY_ADDRESS B ON A.PAT_ID = B.PAT_ID
    """
    got = _tuples(extract_columns(sql))
    assert ("Clarity", "dbo", "PRIMARY_ADDRESS", "STREET") in got
    assert ("Clarity", "dbo", "SECONDARY_ADDRESS", "STREET") in got


def test_cte_alias_does_not_appear():
    """References to CTE aliases must NOT appear in the inventory.
    The columns the CTE body reads from real tables should appear."""
    sql = """
    WITH ActiveReferrals AS (
        SELECT R.REFERRAL_ID, R.STATUS_C
        FROM Clarity.dbo.REFERRAL R
        WHERE R.STATUS_C = 1
    )
    SELECT AR.REFERRAL_ID FROM ActiveReferrals AR
    """
    got = _tuples(extract_columns(sql))
    # The CTE alias must not appear as a referenced table
    cte_referenced = [r for r in got
                      if r[2].lower() in {"ar", "activereferrals"}]
    assert not cte_referenced, f"CTE refs leaked into inventory: {cte_referenced}"
    # The base table columns from inside the CTE must appear
    assert ("Clarity", "dbo", "REFERRAL", "REFERRAL_ID") in got
    assert ("Clarity", "dbo", "REFERRAL", "STATUS_C") in got


def test_create_view_self_name_excluded():
    """The view's own name (left of CREATE VIEW) must not appear as a
    referenced table in its own inventory."""
    sql = """
    CREATE VIEW dbo.my_view AS
    SELECT P.PAT_ID FROM Clarity.dbo.PATIENT P
    """
    got = _tuples(extract_columns(sql))
    self_refs = [r for r in got if r[2].lower() == "my_view"]
    assert not self_refs, f"View self-ref leaked: {self_refs}"
    assert ("Clarity", "dbo", "PATIENT", "PAT_ID") in got


def test_ssms_boilerplate_stripped():
    """SSMS scripted views start with USE / GO / SET-options that
    sqlglot.parse_one chokes on as multi-statement input. The resolver's
    preprocess_ssms strips them; the wrapper must respect that."""
    sql = """USE [Clarity]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW dbo.scripted_view AS
SELECT P.PAT_ID, P.BIRTH_DATE FROM Clarity.dbo.PATIENT P
"""
    got = _tuples(extract_columns(sql))
    assert ("Clarity", "dbo", "PATIENT", "PAT_ID") in got
    assert ("Clarity", "dbo", "PATIENT", "BIRTH_DATE") in got


def test_exists_subquery_columns_captured():
    """Columns inside an EXISTS subquery should appear in the inventory."""
    sql = """
    SELECT R.REFERRAL_ID FROM Clarity.dbo.REFERRAL R
    WHERE EXISTS (
        SELECT 1 FROM Clarity.dbo.REFERRAL_HIST RH
        WHERE RH.REFERRAL_ID = R.REFERRAL_ID
          AND RH.NEW_STATUS_C = 5
    )
    """
    got = _tuples(extract_columns(sql))
    assert ("Clarity", "dbo", "REFERRAL", "REFERRAL_ID") in got
    assert ("Clarity", "dbo", "REFERRAL_HIST", "REFERRAL_ID") in got
    assert ("Clarity", "dbo", "REFERRAL_HIST", "NEW_STATUS_C") in got


def test_convert_does_not_swallow_inner_column():
    """T-SQL CONVERT(date, X) must surface the inner column X."""
    sql = """
    SELECT CONVERT(date, CVG.CVG_EFF_DT) AS Eff_Date
    FROM Clarity.dbo.COVERAGE CVG
    """
    got = _tuples(extract_columns(sql))
    assert ("Clarity", "dbo", "COVERAGE", "CVG_EFF_DT") in got


def test_old_alias_equals_syntax():
    """Older T-SQL `[alias] = expr` SELECT-list syntax should work."""
    sql = """
    SELECT [Effective Date] = CONVERT(date, CVG.CVG_EFF_DT),
           [Patient ID]    = CVG.PAT_ID
    FROM Clarity.dbo.COVERAGE CVG
    """
    got = _tuples(extract_columns(sql))
    assert ("Clarity", "dbo", "COVERAGE", "CVG_EFF_DT") in got
    assert ("Clarity", "dbo", "COVERAGE", "PAT_ID") in got


def test_returns_column_inventory_object():
    """Public contract: returns a ColumnInventory with sql, dialect, columns."""
    sql = "SELECT PAT_ID FROM Clarity.dbo.PATIENT"
    inv = extract_columns(sql, dialect="tsql")
    assert inv.sql == sql
    assert inv.dialect == "tsql"
    assert all(isinstance(c, ColumnIdentifier) for c in inv.columns)


def test_qualified_string_format():
    """ColumnIdentifier.qualified() produces dotted form, omitting empties."""
    c = ColumnIdentifier(database="Clarity", schema="dbo", table="PATIENT", column="PAT_ID")
    assert c.qualified() == "Clarity.dbo.PATIENT.PAT_ID"
    c2 = ColumnIdentifier(database=None, schema="dbo", table="X", column="Y")
    assert c2.qualified() == "dbo.X.Y"
    c3 = ColumnIdentifier(database=None, schema=None, table="X", column="Y")
    assert c3.qualified() == "X.Y"
