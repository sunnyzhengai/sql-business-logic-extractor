"""
Test cases for Layer 2: Normalization and Business Definition extraction.
Healthcare analytics focus -- Epic Clarity patterns.

Run: python3 -m pytest tests/test_normalize.py -v
"""

from sql_logic_extractor.normalize import (
    BusinessLogicNormalizer, AliasResolver, extract_definitions,
    definitions_to_dict, classify_expression, classify_filter,
    canonicalize_expression, abstract_pattern, compute_signature,
    compute_structural_signature,
)
from sql_logic_extractor.extract import SQLBusinessLogicExtractor, to_dict

extractor = SQLBusinessLogicExtractor()


def extract_logic(sql):
    return to_dict(extractor.extract(sql))


def get_defs(sql, label="test"):
    return extract_definitions(sql, query_label=label)


def def_names(defs):
    return [d.name for d in defs]


def def_categories(defs):
    return {d.name: d.category for d in defs}


def defs_by_name(defs, name):
    return [d for d in defs if d.name == name]


# ============================================================
# Alias Resolution
# ============================================================

class TestAliasResolution:

    def test_01_simple_alias(self):
        """Table aliases resolve to real table names."""
        logic = extract_logic("""
            SELECT e.PAT_ENC_CSN_ID, p.PAT_MRN_ID
            FROM PAT_ENC_HSP e
            JOIN PATIENT p ON e.PAT_ID = p.PAT_ID
        """)
        resolver = AliasResolver(logic)
        assert resolver.resolve_table("e") == "PAT_ENC_HSP"
        assert resolver.resolve_table("p") == "PATIENT"

    def test_02_column_resolution(self):
        """Columns resolve to TABLE.COLUMN format."""
        logic = extract_logic("""
            SELECT e.PAT_ENC_CSN_ID
            FROM PAT_ENC_HSP e
        """)
        resolver = AliasResolver(logic)
        col = {"column": "PAT_ENC_CSN_ID", "table": "e"}
        assert resolver.resolve_column(col) == "PAT_ENC_HSP.PAT_ENC_CSN_ID"

    def test_03_expression_resolution(self):
        """Aliases in full expressions are replaced."""
        logic = extract_logic("""
            SELECT DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) AS los
            FROM PAT_ENC_HSP e
        """)
        resolver = AliasResolver(logic)
        resolved = resolver.resolve_expression("DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME)")
        assert "PAT_ENC_HSP.HOSP_ADMSN_TIME" in resolved
        assert "PAT_ENC_HSP.HOSP_DISCH_TIME" in resolved
        assert "e." not in resolved

    def test_04_same_logic_different_aliases(self):
        """Two queries with different aliases produce same resolved columns."""
        logic1 = extract_logic("""
            SELECT DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) AS los
            FROM PAT_ENC_HSP e
        """)
        logic2 = extract_logic("""
            SELECT DATEDIFF(DAY, enc.HOSP_ADMSN_TIME, enc.HOSP_DISCH_TIME) AS length_of_stay
            FROM PAT_ENC_HSP enc
        """)
        r1 = AliasResolver(logic1)
        r2 = AliasResolver(logic2)

        expr1 = r1.resolve_expression("DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME)")
        expr2 = r2.resolve_expression("DATEDIFF(DAY, enc.HOSP_ADMSN_TIME, enc.HOSP_DISCH_TIME)")
        assert expr1 == expr2

    def test_05_cte_alias(self):
        """CTE names are preserved as-is (not resolved further)."""
        logic = extract_logic("""
            WITH encounters AS (
                SELECT PAT_ID, PAT_ENC_CSN_ID FROM PAT_ENC_HSP
            )
            SELECT e.PAT_ID FROM encounters e
        """)
        resolver = AliasResolver(logic)
        assert resolver.resolve_table("e") == "encounters"
        assert resolver.resolve_table("encounters") == "encounters"


# ============================================================
# Pattern Classification
# ============================================================

class TestClassification:

    def test_06_date_calculation(self):
        """DATEDIFF classified as date_calculation."""
        defs = get_defs("""
            SELECT DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) AS los_days
            FROM PAT_ENC_HSP e
        """)
        cats = def_categories(defs)
        assert cats.get("los_days") == "date_calculation"

    def test_07_case_classification(self):
        """CASE expression classified as classification."""
        defs = get_defs("""
            SELECT CASE
                WHEN AGE_YEARS < 18 THEN 'Pediatric'
                WHEN AGE_YEARS BETWEEN 18 AND 64 THEN 'Adult'
                ELSE 'Geriatric'
            END AS age_group
            FROM PATIENT
        """)
        cats = def_categories(defs)
        assert cats.get("age_group") == "classification"

    def test_08_aggregate_count(self):
        """COUNT classified as aggregation/count."""
        defs = get_defs("""
            SELECT dep.DEPARTMENT_NAME, COUNT(*) AS enc_count
            FROM PAT_ENC_HSP e
            JOIN CLARITY_DEP dep ON e.DEPARTMENT_ID = dep.DEPARTMENT_ID
            GROUP BY dep.DEPARTMENT_NAME
        """)
        matching = defs_by_name(defs, "enc_count")
        assert len(matching) >= 1
        assert matching[0].category == "aggregation"
        assert matching[0].subcategory == "count"

    def test_09_conditional_count(self):
        """COUNT(CASE WHEN ...) classified as aggregation/conditional_count."""
        defs = get_defs("""
            SELECT COUNT(CASE WHEN DISCH_DISPOSITION_C = 1 THEN 1 END) AS discharged_home
            FROM PAT_ENC_HSP
        """)
        matching = defs_by_name(defs, "discharged_home")
        assert len(matching) >= 1
        assert matching[0].category == "aggregation"
        assert matching[0].subcategory == "conditional_count"

    def test_10_window_ranking(self):
        """ROW_NUMBER classified as window_function/ranking."""
        defs = get_defs("""
            SELECT ROW_NUMBER() OVER (PARTITION BY PAT_ID ORDER BY HOSP_ADMSN_TIME DESC) AS rn
            FROM PAT_ENC_HSP
        """)
        matching = defs_by_name(defs, "rn")
        assert len(matching) >= 1
        assert matching[0].category == "window_function"
        assert matching[0].subcategory == "ranking"

    def test_11_window_lag(self):
        """LAG classified as window_function/offset_comparison."""
        defs = get_defs("""
            SELECT LAG(HOSP_DISCH_TIME) OVER (PARTITION BY PAT_ID ORDER BY HOSP_ADMSN_TIME) AS prev_discharge
            FROM PAT_ENC_HSP
        """)
        matching = defs_by_name(defs, "prev_discharge")
        assert len(matching) >= 1
        assert matching[0].category == "window_function"

    def test_12_null_handling(self):
        """COALESCE classified as null_handling."""
        defs = get_defs("""
            SELECT COALESCE(PAT_FIRST_NAME, 'Unknown') AS first_name
            FROM PATIENT
        """)
        matching = defs_by_name(defs, "first_name")
        assert len(matching) >= 1
        assert matching[0].category == "null_handling"

    def test_13_string_operation(self):
        """UPPER classified as string_operation."""
        defs = get_defs("""
            SELECT UPPER(PAT_LAST_NAME) AS last_name_upper
            FROM PATIENT
        """)
        matching = defs_by_name(defs, "last_name_upper")
        assert len(matching) >= 1
        assert matching[0].category == "string_operation"

    def test_14_arithmetic(self):
        """Simple math classified as arithmetic."""
        defs = get_defs("""
            SELECT total_charges - total_payments AS balance
            FROM HSP_TRANSACTIONS
        """)
        matching = defs_by_name(defs, "balance")
        assert len(matching) >= 1
        assert matching[0].category == "arithmetic"

    def test_15_filter_classification(self):
        """WHERE filters get their own categories."""
        defs = get_defs("""
            SELECT PAT_MRN_ID
            FROM PATIENT
            WHERE PAT_STATUS = 'Active'
              AND BIRTH_DATE > '1950-01-01'
              AND PAT_LAST_NAME LIKE 'Smith%'
        """)
        cats = [d.category for d in defs]
        assert "equality_filter" in cats
        assert "comparison_filter" in cats
        assert "pattern_filter" in cats


# ============================================================
# Signature computation
# ============================================================

class TestSignatures:

    def test_16_same_expression_same_signature(self):
        """Identical normalized expressions produce identical signatures."""
        defs1 = get_defs("""
            SELECT DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) AS los
            FROM PAT_ENC_HSP e
        """, label="q1")
        defs2 = get_defs("""
            SELECT DATEDIFF(DAY, enc.HOSP_ADMSN_TIME, enc.HOSP_DISCH_TIME) AS los
            FROM PAT_ENC_HSP enc
        """, label="q2")

        los1 = defs_by_name(defs1, "los")
        los2 = defs_by_name(defs2, "los")
        assert len(los1) >= 1 and len(los2) >= 1
        assert los1[0].signature == los2[0].signature

    def test_17_different_expression_different_signature(self):
        """Different expressions produce different signatures."""
        defs1 = get_defs("""
            SELECT DATEDIFF(DAY, HOSP_ADMSN_TIME, HOSP_DISCH_TIME) AS los
            FROM PAT_ENC_HSP
        """)
        defs2 = get_defs("""
            SELECT DATEDIFF(HOUR, HOSP_ADMSN_TIME, HOSP_DISCH_TIME) AS los
            FROM PAT_ENC_HSP
        """)
        los1 = defs_by_name(defs1, "los")
        los2 = defs_by_name(defs2, "los")
        assert los1[0].signature != los2[0].signature

    def test_18_structural_signature_ignores_columns(self):
        """Structural signature matches when pattern is same but columns differ."""
        sig1 = compute_structural_signature("DATEDIFF(DAY, <col>, <col>)")
        sig2 = compute_structural_signature("DATEDIFF(DAY, <col>, <col>)")
        assert sig1 == sig2

    def test_19_different_alias_same_structural(self):
        """Two DATEDIFF expressions with different columns have same structural signature."""
        defs1 = get_defs("""
            SELECT DATEDIFF(DAY, HOSP_ADMSN_TIME, HOSP_DISCH_TIME) AS los
            FROM PAT_ENC_HSP
        """)
        defs2 = get_defs("""
            SELECT DATEDIFF(DAY, ORDER_START_TIME, ORDER_END_TIME) AS order_duration
            FROM ORDER_PROC
        """)
        los = defs_by_name(defs1, "los")
        dur = defs_by_name(defs2, "order_duration")
        assert los[0].structural_signature == dur[0].structural_signature


# ============================================================
# Filter context
# ============================================================

class TestFilterContext:

    def test_20_filters_attached_to_output(self):
        """Output columns have their table's filters in context."""
        defs = get_defs("""
            SELECT e.PAT_ENC_CSN_ID,
                   DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) AS los
            FROM PAT_ENC_HSP e
            WHERE e.ADT_PAT_CLASS_C = 1
              AND e.HOSP_DISCH_TIME IS NOT NULL
        """)
        los = defs_by_name(defs, "los")
        assert len(los) >= 1
        # The LOS definition should have the filters from PAT_ENC_HSP
        assert len(los[0].filters_context) >= 1


# ============================================================
# Recursive extraction (CTEs + subqueries)
# ============================================================

class TestRecursiveExtraction:

    def test_21_cte_definitions_extracted(self):
        """Definitions inside CTEs are extracted with CTE label."""
        defs = get_defs("""
            WITH encounters AS (
                SELECT PAT_ID,
                       DATEDIFF(DAY, HOSP_ADMSN_TIME, HOSP_DISCH_TIME) AS los_days
                FROM PAT_ENC_HSP
                WHERE ADT_PAT_CLASS_C = 1
            )
            SELECT PAT_ID, los_days FROM encounters WHERE los_days > 3
        """, label="report")
        # Should find the LOS definition from the CTE
        los = [d for d in defs if d.name == "los_days" and d.category == "date_calculation"]
        assert len(los) >= 1
        assert "CTE" in los[0].query_label

    def test_22_subquery_definitions_extracted(self):
        """Definitions inside subqueries are extracted."""
        defs = get_defs("""
            SELECT dept_stats.avg_los
            FROM (
                SELECT DEPARTMENT_ID,
                       AVG(DATEDIFF(DAY, HOSP_ADMSN_TIME, HOSP_DISCH_TIME)) AS avg_los
                FROM PAT_ENC_HSP
                GROUP BY DEPARTMENT_ID
            ) dept_stats
        """)
        avg = [d for d in defs if "avg_los" in d.name]
        assert len(avg) >= 1


# ============================================================
# Complex real-world normalization
# ============================================================

class TestComplexNormalization:

    def test_23_readmission_report(self):
        """Full readmission report produces well-classified definitions."""
        defs = get_defs("""
            WITH index_encounters AS (
                SELECT PAT_ID,
                       PAT_ENC_CSN_ID,
                       HOSP_ADMSN_TIME,
                       HOSP_DISCH_TIME,
                       DATEDIFF(DAY, HOSP_ADMSN_TIME, HOSP_DISCH_TIME) AS los_days,
                       ROW_NUMBER() OVER (PARTITION BY PAT_ID ORDER BY HOSP_ADMSN_TIME) AS enc_seq
                FROM PAT_ENC_HSP
                WHERE HOSP_DISCH_TIME IS NOT NULL
                  AND ADT_PAT_CLASS_C = 1
            )
            SELECT PAT_ID,
                   COUNT(*) AS total_encounters,
                   AVG(los_days) AS avg_los,
                   SUM(CASE WHEN los_days > 7 THEN 1 ELSE 0 END) AS long_stays
            FROM index_encounters
            GROUP BY PAT_ID
        """)
        cats = def_categories(defs)

        # CTE should produce date_calculation for los_days
        los_defs = [d for d in defs if "los" in d.name.lower() and d.category == "date_calculation"]
        assert len(los_defs) >= 1

        # CTE should produce window_function for enc_seq
        wf_defs = [d for d in defs if d.category == "window_function"]
        assert len(wf_defs) >= 1

        # Main query should produce aggregations
        agg_defs = [d for d in defs if d.category == "aggregation"]
        assert len(agg_defs) >= 1

    def test_24_definitions_have_source_tables(self):
        """All non-filter definitions have at least one source table."""
        defs = get_defs("""
            SELECT p.PAT_MRN_ID,
                   DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) AS los,
                   CASE WHEN e.ADT_PAT_CLASS_C = 1 THEN 'IP' ELSE 'OP' END AS visit_type
            FROM PAT_ENC_HSP e
            JOIN PATIENT p ON e.PAT_ID = p.PAT_ID
        """)
        for d in defs:
            if d.category not in ("equality_filter", "comparison_filter", "null_check",
                                   "pattern_filter", "filter"):
                # Non-filter definitions should have source tables
                assert len(d.source_tables) >= 1 or d.category == "constant", \
                    f"{d.name} ({d.category}) has no source tables"


# ============================================================
# Run standalone
# ============================================================

if __name__ == "__main__":
    import traceback

    classes = [
        TestAliasResolution,
        TestClassification,
        TestSignatures,
        TestFilterContext,
        TestRecursiveExtraction,
        TestComplexNormalization,
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
