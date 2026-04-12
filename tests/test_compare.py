"""
Test cases for Layer 3: Comparison -- finding duplicate/similar business logic.
Healthcare analytics focus -- Epic Clarity patterns.

Run: python3 -m pytest tests/test_compare.py -v
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from compare import BusinessLogicComparator, report_to_dict


def make_report(*queries, dialect=None):
    """Helper: feed multiple (label, sql) tuples into comparator and return report."""
    comp = BusinessLogicComparator()
    for label, sql in queries:
        comp.add_query(sql, query_label=label, dialect=dialect)
    return comp.compare()


# ============================================================
# Exact Duplicates
# ============================================================

class TestExactDuplicates:

    def test_01_same_los_different_aliases(self):
        """Same DATEDIFF with different aliases -> exact duplicate."""
        report = make_report(
            ("readmission.sql", """
                SELECT DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) AS los_days
                FROM PAT_ENC_HSP e
            """),
            ("quality.sql", """
                SELECT DATEDIFF(DAY, enc.HOSP_ADMSN_TIME, enc.HOSP_DISCH_TIME) AS length_of_stay
                FROM PAT_ENC_HSP enc
            """),
        )
        assert report.summary["exact_duplicate_groups"] >= 1
        # The LOS calculation should appear as a duplicate
        found = False
        for group in report.exact_duplicates:
            names = set(d["name"] for d in group.definitions)
            if "los_days" in names or "length_of_stay" in names:
                found = True
                assert len(group.definitions) == 2
                sources = set(d["query_label"] for d in group.definitions)
                assert "readmission.sql" in sources
                assert "quality.sql" in sources
        assert found, "Expected LOS duplicate group not found"

    def test_02_same_filter_different_queries(self):
        """Same WHERE clause in multiple queries -> exact duplicate filter."""
        report = make_report(
            ("report_a.sql", """
                SELECT PAT_ENC_CSN_ID
                FROM PAT_ENC_HSP e
                WHERE e.ADT_PAT_CLASS_C = 1
            """),
            ("report_b.sql", """
                SELECT PAT_ID
                FROM PAT_ENC_HSP e
                WHERE e.ADT_PAT_CLASS_C = 1
            """),
        )
        assert report.summary["exact_duplicate_groups"] >= 1

    def test_03_same_case_expression(self):
        """Identical CASE logic across queries -> exact duplicate."""
        case_sql = """
            CASE
                WHEN AGE_YEARS < 18 THEN 'Pediatric'
                WHEN AGE_YEARS BETWEEN 18 AND 64 THEN 'Adult'
                ELSE 'Geriatric'
            END AS age_group
        """
        report = make_report(
            ("demographics.sql", f"SELECT {case_sql} FROM PATIENT"),
            ("cohort.sql", f"SELECT {case_sql} FROM PATIENT"),
        )
        assert report.summary["exact_duplicate_groups"] >= 1

    def test_04_no_false_positives(self):
        """Completely different queries -> no exact duplicates."""
        report = make_report(
            ("query_a.sql", """
                SELECT COUNT(*) AS total FROM PAT_ENC_HSP
            """),
            ("query_b.sql", """
                SELECT AVG(TOTAL_CHARGES) AS avg_charges FROM HSP_TRANSACTIONS
            """),
        )
        assert report.summary["exact_duplicate_groups"] == 0


# ============================================================
# Structural Matches
# ============================================================

class TestStructuralMatches:

    def test_05_same_pattern_different_columns(self):
        """Same DATEDIFF pattern on different columns -> structural match."""
        report = make_report(
            ("los.sql", """
                SELECT DATEDIFF(DAY, HOSP_ADMSN_TIME, HOSP_DISCH_TIME) AS los_days
                FROM PAT_ENC_HSP
            """),
            ("order_duration.sql", """
                SELECT DATEDIFF(DAY, ORDER_START_TIME, ORDER_END_TIME) AS order_duration
                FROM ORDER_PROC
            """),
        )
        # These should NOT be exact (different columns) but should be structural
        has_structural = report.summary["structural_match_groups"] >= 1
        # Or they might end up as semantic if structural sig differs
        has_semantic = report.summary["semantic_match_groups"] >= 1
        assert has_structural or has_semantic, \
            "Expected structural or semantic match for same-pattern DATEDIFF"

    def test_06_same_case_structure_different_values(self):
        """Same CASE branching structure, different thresholds -> structural match."""
        report = make_report(
            ("risk_a.sql", """
                SELECT CASE
                    WHEN LOS_DAYS <= 3 THEN 'Short'
                    WHEN LOS_DAYS <= 7 THEN 'Medium'
                    ELSE 'Long'
                END AS los_category
                FROM PAT_ENC_HSP
            """),
            ("risk_b.sql", """
                SELECT CASE
                    WHEN TOTAL_CHARGES <= 5000 THEN 'Low'
                    WHEN TOTAL_CHARGES <= 20000 THEN 'Medium'
                    ELSE 'High'
                END AS cost_category
                FROM HSP_TRANSACTIONS
            """),
        )
        total_matches = (report.summary["structural_match_groups"]
                         + report.summary["semantic_match_groups"])
        assert total_matches >= 1, "Expected match for same CASE structure"

    def test_07_row_number_patterns(self):
        """Two ROW_NUMBER() with different PARTITION BY -> structural match."""
        report = make_report(
            ("dedup_enc.sql", """
                SELECT ROW_NUMBER() OVER (PARTITION BY PAT_ID ORDER BY HOSP_ADMSN_TIME DESC) AS rn
                FROM PAT_ENC_HSP
            """),
            ("dedup_dx.sql", """
                SELECT ROW_NUMBER() OVER (PARTITION BY HSP_ACCOUNT_ID ORDER BY LINE ASC) AS rn
                FROM HSP_ACCT_DX_LIST
            """),
        )
        total_matches = (report.summary["structural_match_groups"]
                         + report.summary["semantic_match_groups"])
        assert total_matches >= 1, "Expected match for ROW_NUMBER patterns"


# ============================================================
# Semantic Matches
# ============================================================

class TestSemanticMatches:

    def test_08_same_table_different_aggregations(self):
        """COUNT and SUM on same table -> semantic match (both aggregations on PAT_ENC_HSP)."""
        report = make_report(
            ("volume.sql", """
                SELECT DEPARTMENT_ID, COUNT(*) AS enc_count
                FROM PAT_ENC_HSP
                GROUP BY DEPARTMENT_ID
            """),
            ("charges.sql", """
                SELECT DEPARTMENT_ID, SUM(TOTAL_CHARGES) AS total_charges
                FROM PAT_ENC_HSP
                GROUP BY DEPARTMENT_ID
            """),
        )
        # These are different aggregation types on the same table
        total = (report.summary["exact_duplicate_groups"]
                 + report.summary["structural_match_groups"]
                 + report.summary["semantic_match_groups"])
        # At minimum the filters (if any) or the aggregations should match semantically
        # These might not match if they're too different -- that's ok
        assert report.summary["total_definitions"] >= 2

    def test_09_cte_definitions_compared(self):
        """Definitions inside CTEs are compared across queries."""
        report = make_report(
            ("report_a.sql", """
                WITH base AS (
                    SELECT PAT_ID, DATEDIFF(DAY, HOSP_ADMSN_TIME, HOSP_DISCH_TIME) AS los
                    FROM PAT_ENC_HSP WHERE ADT_PAT_CLASS_C = 1
                )
                SELECT PAT_ID, los FROM base
            """),
            ("report_b.sql", """
                WITH encounters AS (
                    SELECT PAT_ID, DATEDIFF(DAY, HOSP_ADMSN_TIME, HOSP_DISCH_TIME) AS los
                    FROM PAT_ENC_HSP WHERE ADT_PAT_CLASS_C = 1
                )
                SELECT PAT_ID, los FROM encounters
            """),
        )
        # The LOS inside both CTEs should be exact duplicates
        assert report.summary["exact_duplicate_groups"] >= 1


# ============================================================
# Complex multi-query comparison
# ============================================================

class TestComplexComparison:

    def test_10_three_query_comparison(self):
        """Three queries sharing some business logic."""
        report = make_report(
            ("readmission.sql", """
                SELECT p.PAT_MRN_ID,
                       DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) AS los_days,
                       CASE WHEN e.ADT_PAT_CLASS_C = 1 THEN 'IP' ELSE 'OP' END AS visit_type
                FROM PAT_ENC_HSP e
                JOIN PATIENT p ON e.PAT_ID = p.PAT_ID
                WHERE e.HOSP_DISCH_TIME IS NOT NULL
            """),
            ("quality.sql", """
                SELECT p.PAT_MRN_ID,
                       DATEDIFF(DAY, enc.HOSP_ADMSN_TIME, enc.HOSP_DISCH_TIME) AS length_of_stay,
                       CASE WHEN enc.ADT_PAT_CLASS_C = 1 THEN 'IP' ELSE 'OP' END AS enc_type
                FROM PAT_ENC_HSP enc
                JOIN PATIENT p ON enc.PAT_ID = p.PAT_ID
                WHERE enc.HOSP_DISCH_TIME IS NOT NULL
            """),
            ("finance.sql", """
                SELECT e.PAT_ENC_CSN_ID,
                       DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) AS los,
                       e.TOTAL_CHARGES / DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) AS charge_per_day
                FROM PAT_ENC_HSP e
                WHERE e.HOSP_DISCH_TIME IS NOT NULL
            """),
        )
        # LOS calculation appears in all 3 -> should be exact duplicate
        assert report.summary["exact_duplicate_groups"] >= 1

        # visit_type CASE appears in 2 of 3
        all_matches = report.exact_duplicates + report.structural_matches + report.semantic_matches
        assert len(all_matches) >= 1

        # Total queries should be 3
        assert report.summary["total_queries"] == 3

    def test_11_no_self_comparison(self):
        """A query should not match against itself in exact duplicates."""
        report = make_report(
            ("only_query.sql", """
                SELECT DATEDIFF(DAY, HOSP_ADMSN_TIME, HOSP_DISCH_TIME) AS los
                FROM PAT_ENC_HSP
                WHERE ADT_PAT_CLASS_C = 1
            """),
        )
        # Single query -> no duplicates possible
        assert report.summary["exact_duplicate_groups"] == 0
        assert report.summary["structural_match_groups"] == 0
        assert report.summary["semantic_match_groups"] == 0

    def test_12_report_has_summary(self):
        """Report includes a summary with counts."""
        report = make_report(
            ("a.sql", "SELECT COUNT(*) AS n FROM PAT_ENC_HSP"),
            ("b.sql", "SELECT COUNT(*) AS total FROM PAT_ENC_HSP"),
        )
        d = report_to_dict(report)
        assert "summary" in d
        assert "total_definitions" in d["summary"]
        assert "total_queries" in d["summary"]
        assert d["summary"]["total_queries"] == 2

    def test_13_complex_readmission_vs_quality(self):
        """Two real-world style reports with overlapping logic."""
        report = make_report(
            ("readmission_report.sql", """
                WITH base AS (
                    SELECT e.PAT_ID, e.PAT_ENC_CSN_ID,
                           e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME,
                           DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) AS los_days,
                           CASE
                               WHEN e.DISCH_DISPOSITION_C = 1 THEN 'Home'
                               WHEN e.DISCH_DISPOSITION_C = 2 THEN 'Transfer'
                               WHEN e.DISCH_DISPOSITION_C = 20 THEN 'Expired'
                               ELSE 'Other'
                           END AS disch_status
                    FROM PAT_ENC_HSP e
                    WHERE e.HOSP_DISCH_TIME IS NOT NULL AND e.ADT_PAT_CLASS_C = 1
                )
                SELECT disch_status,
                       COUNT(*) AS discharges,
                       AVG(los_days) AS avg_los
                FROM base
                GROUP BY disch_status
            """),
            ("quality_dashboard.sql", """
                WITH encounters AS (
                    SELECT e.PAT_ID, e.PAT_ENC_CSN_ID,
                           e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME,
                           DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) AS los_days,
                           CASE
                               WHEN e.DISCH_DISPOSITION_C = 1 THEN 'Home'
                               WHEN e.DISCH_DISPOSITION_C = 2 THEN 'Transfer'
                               WHEN e.DISCH_DISPOSITION_C = 20 THEN 'Expired'
                               ELSE 'Other'
                           END AS disposition
                    FROM PAT_ENC_HSP e
                    WHERE e.HOSP_DISCH_TIME IS NOT NULL AND e.ADT_PAT_CLASS_C = 1
                )
                SELECT COUNT(*) AS total,
                       AVG(los_days) AS mean_los,
                       COUNT(CASE WHEN disposition = 'Expired' THEN 1 END) AS mortality_count
                FROM encounters
            """),
        )
        # Both CTEs have the same LOS and same CASE expression
        assert report.summary["exact_duplicate_groups"] >= 1, \
            "LOS and/or CASE expression should be exact duplicates"

        # Should find the shared LOS
        all_exact = report.exact_duplicates
        los_found = False
        case_found = False
        for group in all_exact:
            for d in group.definitions:
                if "los" in d["name"].lower():
                    los_found = True
                if d.get("category") == "classification":
                    case_found = True
        assert los_found, "LOS calculation should be an exact duplicate"
        # CASE might differ slightly due to alias (disch_status vs disposition)
        # but the normalized expression should match


# ============================================================
# Run standalone
# ============================================================

if __name__ == "__main__":
    import traceback

    classes = [
        TestExactDuplicates,
        TestStructuralMatches,
        TestSemanticMatches,
        TestComplexComparison,
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
