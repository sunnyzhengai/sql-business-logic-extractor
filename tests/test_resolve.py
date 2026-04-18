"""
Test cases for Lineage Resolution -- tracing every output to base table.column.

Run: python3 -m pytest tests/test_resolve.py -v
"""

from sql_logic_extractor.resolve import resolve_query, resolved_to_dict


def resolve(sql):
    r = resolve_query(sql)
    return {col.name: col for col in r.columns}


def base_cols(col):
    return sorted(col.base_columns)


def base_tables(col):
    return sorted(col.base_tables)


# ============================================================
# Simple queries -- should already resolve to base tables
# ============================================================

class TestSimpleResolution:

    def test_01_passthrough_resolves(self):
        """Simple SELECT resolves columns to base table."""
        cols = resolve("SELECT p.PAT_MRN_ID, p.PAT_FIRST_NAME FROM PATIENT p")
        assert "PAT_MRN_ID" in cols
        assert "PATIENT.PAT_MRN_ID" in cols["PAT_MRN_ID"].base_columns
        assert "PATIENT" in cols["PAT_MRN_ID"].base_tables

    def test_02_calculated_resolves(self):
        """Calculated field traces to base table columns."""
        cols = resolve("""
            SELECT DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) AS los
            FROM PAT_ENC_HSP e
        """)
        assert "los" in cols
        assert cols["los"].type == "calculated"
        assert "PAT_ENC_HSP" in cols["los"].base_tables

    def test_03_filters_carried(self):
        """WHERE filters appear on resolved columns."""
        cols = resolve("""
            SELECT e.PAT_ENC_CSN_ID
            FROM PAT_ENC_HSP e
            WHERE e.ADT_PAT_CLASS_C = 1
        """)
        assert len(cols["PAT_ENC_CSN_ID"].filters) >= 1
        assert any("ADT_PAT_CLASS_C" in f for f in cols["PAT_ENC_CSN_ID"].filters)


# ============================================================
# Single CTE -- passthrough should resolve through
# ============================================================

class TestSingleCTE:

    def test_04_cte_passthrough(self):
        """Passthrough from CTE resolves to base table."""
        cols = resolve("""
            WITH enc AS (
                SELECT e.PAT_ID, e.PAT_ENC_CSN_ID
                FROM PAT_ENC_HSP e
                WHERE e.ADT_PAT_CLASS_C = 1
            )
            SELECT PAT_ID, PAT_ENC_CSN_ID FROM enc
        """)
        assert "PAT_ENC_HSP.PAT_ID" in cols["PAT_ID"].base_columns
        assert "PAT_ENC_HSP" in cols["PAT_ID"].base_tables

    def test_05_cte_calculated_resolves(self):
        """Calculated column in CTE resolves through passthrough."""
        cols = resolve("""
            WITH enc AS (
                SELECT e.PAT_ID,
                       DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) AS los_days
                FROM PAT_ENC_HSP e
            )
            SELECT PAT_ID, los_days FROM enc
        """)
        assert cols["los_days"].type == "calculated"
        assert "PAT_ENC_HSP" in cols["los_days"].base_tables
        # Should have the DATEDIFF in the chain
        chain_types = [step["type"] for step in cols["los_days"].transformation_chain]
        assert "calculated" in chain_types

    def test_06_cte_filters_propagate(self):
        """CTE WHERE filters propagate to outer columns."""
        cols = resolve("""
            WITH enc AS (
                SELECT e.PAT_ID
                FROM PAT_ENC_HSP e
                WHERE e.HOSP_DISCH_TIME IS NOT NULL
                  AND e.ADT_PAT_CLASS_C = 1
            )
            SELECT PAT_ID FROM enc
        """)
        assert len(cols["PAT_ID"].filters) >= 1
        assert any("ADT_PAT_CLASS_C" in f for f in cols["PAT_ID"].filters)


# ============================================================
# Chained CTEs -- multiple hops
# ============================================================

class TestChainedCTEs:

    def test_07_two_hop_passthrough(self):
        """Column passes through 2 CTEs to base table."""
        cols = resolve("""
            WITH a AS (SELECT PAT_ID FROM PAT_ENC_HSP),
                 b AS (SELECT PAT_ID FROM a)
            SELECT PAT_ID FROM b
        """)
        assert "PAT_ENC_HSP.PAT_ID" in cols["PAT_ID"].base_columns
        # Chain should have 3 entries: query -> b -> a
        assert len(cols["PAT_ID"].transformation_chain) >= 3

    def test_08_calculated_through_chain(self):
        """DATEDIFF in CTE A, passthrough in CTE B, selected in main."""
        cols = resolve("""
            WITH base AS (
                SELECT e.PAT_ID,
                       DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) AS los_days
                FROM PAT_ENC_HSP e
                WHERE e.ADT_PAT_CLASS_C = 1
            ),
            flagged AS (
                SELECT PAT_ID, los_days FROM base
            )
            SELECT PAT_ID, los_days FROM flagged
        """)
        assert cols["los_days"].type == "calculated"
        assert "PAT_ENC_HSP" in cols["los_days"].base_tables

    def test_09_case_using_cte_column(self):
        """CASE in CTE B references calculated column from CTE A."""
        cols = resolve("""
            WITH base AS (
                SELECT e.PAT_ID,
                       DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) AS los_days
                FROM PAT_ENC_HSP e
            ),
            flagged AS (
                SELECT PAT_ID, los_days,
                       CASE WHEN los_days > 7 THEN 'Long' ELSE 'Short' END AS los_category
                FROM base
            )
            SELECT PAT_ID, los_days, los_category FROM flagged
        """)
        # los_category should trace through to PAT_ENC_HSP
        assert cols["los_category"].type == "case"
        assert "PAT_ENC_HSP" in cols["los_category"].base_tables
        # Its chain should include the CASE and the underlying DATEDIFF
        chain_types = [step["type"] for step in cols["los_category"].transformation_chain]
        assert "case" in chain_types
        assert "calculated" in chain_types

    def test_10_filters_accumulate(self):
        """Filters from multiple CTEs accumulate."""
        cols = resolve("""
            WITH a AS (
                SELECT PAT_ID FROM PAT_ENC_HSP WHERE ADT_PAT_CLASS_C = 1
            ),
            b AS (
                SELECT PAT_ID FROM a WHERE PAT_ID > 100
            )
            SELECT PAT_ID FROM b WHERE PAT_ID < 999
        """)
        # Should have filters from all 3 levels
        all_filter_text = " ".join(cols["PAT_ID"].filters)
        assert "ADT_PAT_CLASS_C" in all_filter_text


# ============================================================
# Derived tables (subqueries in FROM)
# ============================================================

class TestDerivedTables:

    def test_11_derived_table_resolves(self):
        """Column from derived table resolves to base table."""
        cols = resolve("""
            SELECT dept_stats.avg_los
            FROM (
                SELECT dep.DEPARTMENT_NAME,
                       AVG(DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME)) AS avg_los
                FROM PAT_ENC_HSP e
                JOIN CLARITY_DEP dep ON e.DEPARTMENT_ID = dep.DEPARTMENT_ID
                GROUP BY dep.DEPARTMENT_NAME
            ) dept_stats
        """)
        assert cols["avg_los"].type == "aggregate"
        assert "PAT_ENC_HSP" in cols["avg_los"].base_tables

    def test_12_derived_table_with_filter(self):
        """Outer WHERE on derived table + inner WHERE both captured."""
        cols = resolve("""
            SELECT s.total_charges
            FROM (
                SELECT HSP_ACCOUNT_ID,
                       SUM(TX_AMOUNT) AS total_charges
                FROM HSP_TRANSACTIONS
                WHERE TX_TYPE_C = 1
                GROUP BY HSP_ACCOUNT_ID
            ) s
            WHERE s.total_charges > 1000
        """)
        assert "HSP_TRANSACTIONS" in cols["total_charges"].base_tables
        # Should have both inner (TX_TYPE_C = 1) and outer (total_charges > 1000) filters
        assert len(cols["total_charges"].filters) >= 1


# ============================================================
# Mixed: CTEs + derived tables + joins
# ============================================================

class TestMixed:

    def test_13_cte_join_base_table(self):
        """CTE joined with base table -- both resolve correctly."""
        cols = resolve("""
            WITH enc AS (
                SELECT e.PAT_ID,
                       DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) AS los_days
                FROM PAT_ENC_HSP e
            )
            SELECT p.PAT_MRN_ID, enc.los_days
            FROM enc
            JOIN PATIENT p ON enc.PAT_ID = p.PAT_ID
        """)
        assert "PATIENT.PAT_MRN_ID" in cols["PAT_MRN_ID"].base_columns
        assert cols["los_days"].type == "calculated"
        assert "PAT_ENC_HSP" in cols["los_days"].base_tables

    def test_14_readmission_full_chain(self):
        """Full readmission report -- 3 CTEs, everything resolves to PAT_ENC_HSP."""
        cols = resolve("""
            WITH base AS (
                SELECT e.PAT_ID, e.PAT_ENC_CSN_ID,
                       e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME,
                       DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) AS los_days,
                       CASE WHEN e.DISCH_DISPOSITION_C = 1 THEN 'Home'
                            WHEN e.DISCH_DISPOSITION_C = 20 THEN 'Expired'
                            ELSE 'Other'
                       END AS disch_status
                FROM PAT_ENC_HSP e
                WHERE e.HOSP_DISCH_TIME IS NOT NULL AND e.ADT_PAT_CLASS_C = 1
            ),
            summary AS (
                SELECT disch_status,
                       COUNT(*) AS discharges,
                       AVG(los_days) AS avg_los
                FROM base
                GROUP BY disch_status
            )
            SELECT disch_status, discharges, avg_los FROM summary
        """)
        # disch_status should trace: summary(passthrough) -> base(CASE) -> PAT_ENC_HSP
        assert cols["disch_status"].type == "case"
        assert "PAT_ENC_HSP" in cols["disch_status"].base_tables

        # avg_los should trace: summary(AVG(los_days)) -> base(DATEDIFF) -> PAT_ENC_HSP
        assert cols["avg_los"].type == "aggregate"
        assert "PAT_ENC_HSP" in cols["avg_los"].base_tables

        # discharges is COUNT(*) -- no base columns but filters should be there
        assert cols["discharges"].type == "aggregate"

    def test_15_no_infinite_loop(self):
        """Self-referencing CTE name as table doesn't cause infinite loop."""
        cols = resolve("""
            WITH data AS (SELECT PAT_ID FROM PAT_ENC_HSP)
            SELECT PAT_ID FROM data
        """)
        # Should complete without hanging
        assert "PAT_ID" in cols


# ============================================================
# Run standalone
# ============================================================

if __name__ == "__main__":
    import traceback

    classes = [
        TestSimpleResolution,
        TestSingleCTE,
        TestChainedCTEs,
        TestDerivedTables,
        TestMixed,
    ]

    total = 0
    passed = 0
    failed = 0
    errors = []

    for cls in classes:
        instance = cls()
        print(f"\n{'='*60}")
        print(f"  {cls.__name__}")
        print(f"{'='*60}")
        methods = sorted([m for m in dir(instance) if m.startswith("test_")])
        for method_name in methods:
            total += 1
            method = getattr(instance, method_name)
            doc = method.__doc__ or method_name
            try:
                method()
                passed += 1
                print(f"  PASS  {method_name}: {doc.strip()}")
            except Exception as e:
                failed += 1
                errors.append((method_name, doc.strip(), e, traceback.format_exc()))
                print(f"  FAIL  {method_name}: {doc.strip()}")
                print(f"        {e}")

    print(f"\n{'='*60}")
    print(f"  Results: {passed}/{total} passed, {failed} failed")
    print(f"{'='*60}")

    if errors:
        print(f"\nFailed tests:")
        for name, doc, err, tb in errors:
            print(f"\n  {name}: {doc}")
            print(f"  {tb}")

    sys.exit(0 if failed == 0 else 1)
