"""
Test cases for SQL Business Logic Extractor
Healthcare analytics focus -- Epic Clarity patterns, simple to complex.

Run: python3 -m pytest tests/test_queries.py -v
"""

import json
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extract import SQLBusinessLogicExtractor, to_dict

extractor = SQLBusinessLogicExtractor()


def extract(sql):
    return to_dict(extractor.extract(sql))


def names(outputs):
    return [o["name"] for o in outputs]


def types(outputs):
    return {o["name"]: o["type"] for o in outputs}


def filter_exprs(filters, scope=None):
    return [f["expression"] for f in filters if scope is None or f["scope"] == scope]


# ============================================================
# LEVEL 1: Simple queries
# ============================================================

class TestLevel1_Simple:

    def test_01_basic_select(self):
        """Plain column select -- no transformations."""
        sql = """
        SELECT PAT_MRN_ID, PAT_FIRST_NAME, PAT_LAST_NAME
        FROM PATIENT
        """
        r = extract(sql)
        assert len(r["outputs"]) == 3
        assert all(o["type"] == "passthrough" for o in r["outputs"])
        assert r["sources"][0]["name"] == "PATIENT"
        # No lineage for pure passthrough with no filters
        # (or minimal lineage -- depends on design choice)

    def test_02_select_with_alias(self):
        """Column aliases -- still passthrough."""
        sql = """
        SELECT PAT_MRN_ID AS mrn,
               PAT_FIRST_NAME AS first_name,
               PAT_LAST_NAME AS last_name
        FROM PATIENT
        """
        r = extract(sql)
        assert names(r["outputs"]) == ["mrn", "first_name", "last_name"]
        assert all(o["type"] == "passthrough" for o in r["outputs"])

    def test_03_where_filter(self):
        """Simple WHERE -- single filter condition."""
        sql = """
        SELECT PAT_MRN_ID, PAT_FIRST_NAME
        FROM PATIENT
        WHERE PAT_STATUS = 'Active'
        """
        r = extract(sql)
        where_filters = filter_exprs(r["filters"], "where")
        assert len(where_filters) == 1
        assert "PAT_STATUS" in where_filters[0]

    def test_04_multiple_where(self):
        """Multiple AND conditions -- should split into separate filters."""
        sql = """
        SELECT PAT_MRN_ID
        FROM PATIENT
        WHERE PAT_STATUS = 'Active'
          AND BIRTH_DATE > '1950-01-01'
          AND STATE_ABBR = 'TX'
        """
        r = extract(sql)
        where_filters = filter_exprs(r["filters"], "where")
        assert len(where_filters) == 3

    def test_05_calculated_column(self):
        """Basic arithmetic -- age calculation from birth date."""
        sql = """
        SELECT PAT_MRN_ID,
               DATEDIFF(YEAR, BIRTH_DATE, GETDATE()) AS age
        FROM PATIENT
        """
        r = extract(sql)
        t = types(r["outputs"])
        assert t["PAT_MRN_ID"] == "passthrough"
        assert t["age"] == "calculated"

    def test_06_literal_column(self):
        """Literal/constant value in SELECT."""
        sql = """
        SELECT PAT_MRN_ID,
               'Inpatient' AS encounter_type,
               1 AS is_active
        FROM PATIENT
        """
        r = extract(sql)
        t = types(r["outputs"])
        assert t["encounter_type"] == "literal"
        assert t["is_active"] == "literal"


# ============================================================
# LEVEL 2: JOINs and relationships
# ============================================================

class TestLevel2_Joins:

    def test_07_inner_join(self):
        """Standard inner join -- encounter to patient."""
        sql = """
        SELECT p.PAT_MRN_ID,
               e.PAT_ENC_CSN_ID,
               e.HOSP_ADMSN_TIME,
               e.HOSP_DISCH_TIME
        FROM PAT_ENC_HSP e
        JOIN PATIENT p ON e.PAT_ID = p.PAT_ID
        WHERE e.HOSP_DISCH_TIME IS NOT NULL
        """
        r = extract(sql)
        assert len(r["joins"]) == 1
        assert r["joins"][0]["join_type"] == "JOIN"
        assert "e.PAT_ID = p.PAT_ID" in r["joins"][0]["on_expression"]

    def test_08_left_join(self):
        """Left join -- encounters with optional diagnosis."""
        sql = """
        SELECT e.PAT_ENC_CSN_ID,
               dx.DX_NAME
        FROM PAT_ENC_HSP e
        LEFT JOIN HSP_ACCT_DX_LIST dl ON e.HSP_ACCOUNT_ID = dl.HSP_ACCOUNT_ID
        LEFT JOIN CLARITY_EDG dx ON dl.DX_ID = dx.DX_ID
        WHERE dl.LINE = 1
        """
        r = extract(sql)
        assert len(r["joins"]) == 2
        assert "LEFT" in r["joins"][0]["join_type"]

    def test_09_multi_join_calculated(self):
        """Multiple joins with calculated LOS (length of stay)."""
        sql = """
        SELECT p.PAT_MRN_ID,
               e.PAT_ENC_CSN_ID,
               dep.DEPARTMENT_NAME,
               DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) AS los_days,
               DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) * dep.COST_PER_DAY AS total_cost
        FROM PAT_ENC_HSP e
        JOIN PATIENT p ON e.PAT_ID = p.PAT_ID
        JOIN CLARITY_DEP dep ON e.DEPARTMENT_ID = dep.DEPARTMENT_ID
        WHERE e.HOSP_DISCH_TIME IS NOT NULL
          AND e.ADT_PAT_CLASS_C = 1
        """
        r = extract(sql)
        t = types(r["outputs"])
        assert t["los_days"] == "calculated"
        assert t["total_cost"] == "calculated"
        assert len(r["joins"]) == 2
        # Lineage: total_cost depends on columns from both e and dep
        cost_lineage = [l for l in r["lineage"] if l["output"] == "total_cost"][0]
        tables_in_lineage = set()
        for dep in cost_lineage["depends_on"]:
            parts = dep.split(".")
            if len(parts) > 1:
                tables_in_lineage.add(parts[0])
        assert "e" in tables_in_lineage
        assert "dep" in tables_in_lineage


# ============================================================
# LEVEL 3: CASE expressions (business rules)
# ============================================================

class TestLevel3_Case:

    def test_10_simple_case(self):
        """CASE for patient classification."""
        sql = """
        SELECT PAT_MRN_ID,
               CASE
                   WHEN AGE_YEARS < 18 THEN 'Pediatric'
                   WHEN AGE_YEARS BETWEEN 18 AND 64 THEN 'Adult'
                   ELSE 'Geriatric'
               END AS age_group
        FROM PATIENT
        """
        r = extract(sql)
        assert types(r["outputs"])["age_group"] == "case"
        assert len(r["case_expressions"]) == 1
        assert len(r["case_expressions"][0]["branches"]) == 2
        assert r["case_expressions"][0]["else_result"] is not None

    def test_11_nested_case(self):
        """CASE with multiple business rules -- readmission risk."""
        sql = """
        SELECT e.PAT_ENC_CSN_ID,
               CASE
                   WHEN dx.DX_NAME LIKE '%heart failure%' AND e.LOS_DAYS > 7 THEN 'High'
                   WHEN dx.DX_NAME LIKE '%pneumonia%' THEN 'Medium'
                   WHEN e.ED_DEPARTURE_TIME IS NOT NULL THEN 'Medium'
                   ELSE 'Low'
               END AS readmit_risk,
               CASE e.ADT_PAT_CLASS_C
                   WHEN 1 THEN 'Inpatient'
                   WHEN 2 THEN 'Outpatient'
                   WHEN 3 THEN 'Emergency'
                   ELSE 'Other'
               END AS visit_type
        FROM PAT_ENC_HSP e
        LEFT JOIN HSP_ACCT_DX_LIST dl ON e.HSP_ACCOUNT_ID = dl.HSP_ACCOUNT_ID AND dl.LINE = 1
        LEFT JOIN CLARITY_EDG dx ON dl.DX_ID = dx.DX_ID
        """
        r = extract(sql)
        assert len(r["case_expressions"]) == 2
        risk = [c for c in r["case_expressions"] if c["output_name"] == "readmit_risk"][0]
        assert len(risk["branches"]) == 3


# ============================================================
# LEVEL 4: Aggregations
# ============================================================

class TestLevel4_Aggregation:

    def test_12_simple_aggregate(self):
        """COUNT and AVG with GROUP BY."""
        sql = """
        SELECT dep.DEPARTMENT_NAME,
               COUNT(DISTINCT e.PAT_ENC_CSN_ID) AS encounter_count,
               AVG(DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME)) AS avg_los
        FROM PAT_ENC_HSP e
        JOIN CLARITY_DEP dep ON e.DEPARTMENT_ID = dep.DEPARTMENT_ID
        WHERE e.HOSP_DISCH_TIME IS NOT NULL
        GROUP BY dep.DEPARTMENT_NAME
        """
        r = extract(sql)
        t = types(r["outputs"])
        assert t["encounter_count"] == "aggregate"
        assert t["avg_los"] == "aggregate"
        assert len(r["aggregations"]) >= 2
        # GROUP BY should be captured
        assert any("DEPARTMENT_NAME" in g for agg in r["aggregations"] for g in agg.get("group_by", []))

    def test_13_having_filter(self):
        """HAVING clause -- post-aggregation filter."""
        sql = """
        SELECT p.PRIMARY_DX_CODE,
               COUNT(*) AS patient_count,
               AVG(e.TOTAL_CHARGES) AS avg_charges
        FROM PAT_ENC_HSP e
        JOIN PATIENT p ON e.PAT_ID = p.PAT_ID
        GROUP BY p.PRIMARY_DX_CODE
        HAVING COUNT(*) >= 10
           AND AVG(e.TOTAL_CHARGES) > 5000
        """
        r = extract(sql)
        having = filter_exprs(r["filters"], "having")
        assert len(having) == 2

    def test_14_aggregate_with_case(self):
        """Conditional aggregation -- pivot-style counting."""
        sql = """
        SELECT dep.DEPARTMENT_NAME,
               COUNT(CASE WHEN e.DISCH_DISPOSITION_C = 1 THEN 1 END) AS discharged_home,
               COUNT(CASE WHEN e.DISCH_DISPOSITION_C = 2 THEN 1 END) AS transferred,
               COUNT(CASE WHEN e.DISCH_DISPOSITION_C = 20 THEN 1 END) AS expired,
               COUNT(*) AS total
        FROM PAT_ENC_HSP e
        JOIN CLARITY_DEP dep ON e.DEPARTMENT_ID = dep.DEPARTMENT_ID
        GROUP BY dep.DEPARTMENT_NAME
        """
        r = extract(sql)
        assert types(r["outputs"])["discharged_home"] == "aggregate"
        assert types(r["outputs"])["total"] == "aggregate"


# ============================================================
# LEVEL 5: Window functions
# ============================================================

class TestLevel5_Window:

    def test_15_row_number(self):
        """ROW_NUMBER for deduplication -- common Epic pattern."""
        sql = """
        SELECT *
        FROM (
            SELECT e.PAT_ENC_CSN_ID,
                   e.PAT_ID,
                   e.HOSP_ADMSN_TIME,
                   ROW_NUMBER() OVER (
                       PARTITION BY e.PAT_ID
                       ORDER BY e.HOSP_ADMSN_TIME DESC
                   ) AS rn
            FROM PAT_ENC_HSP e
            WHERE e.HOSP_DISCH_TIME IS NOT NULL
        ) ranked
        WHERE rn = 1
        """
        r = extract(sql)
        # The inner query is a derived table (subquery in FROM)
        # It should appear as a subquery source, and the outer query sees SELECT *
        assert any(s["type"] == "subquery" for s in r.get("sources", []))

    def test_16_lag_readmission(self):
        """LAG for readmission detection -- days since last discharge."""
        sql = """
        SELECT PAT_ID,
               PAT_ENC_CSN_ID,
               HOSP_ADMSN_TIME,
               HOSP_DISCH_TIME,
               LAG(HOSP_DISCH_TIME) OVER (
                   PARTITION BY PAT_ID
                   ORDER BY HOSP_ADMSN_TIME
               ) AS prev_discharge,
               DATEDIFF(DAY,
                   LAG(HOSP_DISCH_TIME) OVER (PARTITION BY PAT_ID ORDER BY HOSP_ADMSN_TIME),
                   HOSP_ADMSN_TIME
               ) AS days_since_last_discharge
        FROM PAT_ENC_HSP
        WHERE HOSP_DISCH_TIME IS NOT NULL
        """
        r = extract(sql)
        t = types(r["outputs"])
        assert t["prev_discharge"] == "window"
        # Contains LAG inside DATEDIFF, so classified as window (window takes precedence)
        assert t["days_since_last_discharge"] in ("calculated", "window")
        assert len(r["window_functions"]) >= 1

    def test_17_running_total(self):
        """SUM OVER for running totals -- cumulative charges."""
        sql = """
        SELECT PAT_ENC_CSN_ID,
               SERVICE_DATE,
               TX_AMOUNT,
               SUM(TX_AMOUNT) OVER (
                   PARTITION BY PAT_ENC_CSN_ID
                   ORDER BY SERVICE_DATE
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
               ) AS cumulative_charges
        FROM HSP_TRANSACTIONS
        """
        r = extract(sql)
        assert types(r["outputs"])["cumulative_charges"] == "window"
        assert len(r["window_functions"]) >= 1


# ============================================================
# LEVEL 6: CTEs
# ============================================================

class TestLevel6_CTE:

    def test_18_simple_cte(self):
        """Single CTE -- encounter base."""
        sql = """
        WITH encounters AS (
            SELECT e.PAT_ENC_CSN_ID,
                   e.PAT_ID,
                   e.HOSP_ADMSN_TIME,
                   e.HOSP_DISCH_TIME,
                   DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) AS los_days
            FROM PAT_ENC_HSP e
            WHERE e.HOSP_DISCH_TIME IS NOT NULL
              AND e.ADT_PAT_CLASS_C = 1
        )
        SELECT enc.PAT_ID,
               enc.los_days,
               p.PAT_MRN_ID
        FROM encounters enc
        JOIN PATIENT p ON enc.PAT_ID = p.PAT_ID
        WHERE enc.los_days > 3
        """
        r = extract(sql)
        assert len(r["ctes"]) == 1
        assert r["ctes"][0]["name"] == "encounters"
        # CTE should have its own extracted logic
        cte_logic = r["ctes"][0].get("logic", {})
        assert cte_logic  # should not be empty

    def test_19_chained_ctes(self):
        """Multiple CTEs referencing each other -- readmission analysis."""
        sql = """
        WITH discharges AS (
            SELECT PAT_ID,
                   PAT_ENC_CSN_ID,
                   HOSP_DISCH_TIME,
                   DISCH_DISPOSITION_C
            FROM PAT_ENC_HSP
            WHERE HOSP_DISCH_TIME IS NOT NULL
              AND ADT_PAT_CLASS_C = 1
        ),
        readmissions AS (
            SELECT d1.PAT_ID,
                   d1.PAT_ENC_CSN_ID AS index_csn,
                   d2.PAT_ENC_CSN_ID AS readmit_csn,
                   DATEDIFF(DAY, d1.HOSP_DISCH_TIME, d2.HOSP_DISCH_TIME) AS days_to_readmit
            FROM discharges d1
            JOIN discharges d2
                ON d1.PAT_ID = d2.PAT_ID
                AND d2.HOSP_DISCH_TIME > d1.HOSP_DISCH_TIME
                AND DATEDIFF(DAY, d1.HOSP_DISCH_TIME, d2.HOSP_DISCH_TIME) <= 30
        )
        SELECT r.PAT_ID,
               p.PAT_MRN_ID,
               r.index_csn,
               r.readmit_csn,
               r.days_to_readmit
        FROM readmissions r
        JOIN PATIENT p ON r.PAT_ID = p.PAT_ID
        ORDER BY r.days_to_readmit
        """
        r = extract(sql)
        assert len(r["ctes"]) == 2
        cte_names = [c["name"] for c in r["ctes"]]
        assert "discharges" in cte_names
        assert "readmissions" in cte_names


# ============================================================
# LEVEL 7: Subqueries
# ============================================================

class TestLevel7_Subquery:

    def test_20_subquery_in_where(self):
        """IN subquery -- patients with a specific diagnosis."""
        sql = """
        SELECT p.PAT_MRN_ID, p.PAT_FIRST_NAME, p.PAT_LAST_NAME
        FROM PATIENT p
        WHERE p.PAT_ID IN (
            SELECT DISTINCT e.PAT_ID
            FROM PAT_ENC_HSP e
            JOIN HSP_ACCT_DX_LIST dl ON e.HSP_ACCOUNT_ID = dl.HSP_ACCOUNT_ID
            JOIN CLARITY_EDG dx ON dl.DX_ID = dx.DX_ID
            WHERE dx.ICD10_CODE LIKE 'I50%'
        )
        """
        r = extract(sql)
        assert len(r["subqueries"]) >= 1
        sub = r["subqueries"][0]
        assert sub["context"] in ("where", "in")
        assert sub.get("logic") is not None

    def test_21_exists_subquery(self):
        """EXISTS -- encounters with at least one medication order."""
        sql = """
        SELECT e.PAT_ENC_CSN_ID, e.HOSP_ADMSN_TIME
        FROM PAT_ENC_HSP e
        WHERE EXISTS (
            SELECT 1
            FROM ORDER_MED om
            WHERE om.PAT_ENC_CSN_ID = e.PAT_ENC_CSN_ID
              AND om.ORDER_STATUS_C = 2
        )
        """
        r = extract(sql)
        assert any(s["context"] == "exists" for s in r["subqueries"])

    def test_22_scalar_subquery(self):
        """Scalar subquery in SELECT -- latest vitals."""
        sql = """
        SELECT e.PAT_ENC_CSN_ID,
               (SELECT MAX(fm.RECORDED_TIME)
                FROM IP_FLWSHT_MEAS fm
                WHERE fm.PAT_ENC_CSN_ID = e.PAT_ENC_CSN_ID
                  AND fm.FLO_MEAS_ID = '5'
               ) AS last_bp_time
        FROM PAT_ENC_HSP e
        """
        r = extract(sql)
        assert types(r["outputs"])["last_bp_time"] == "subquery"
        assert len(r["subqueries"]) >= 1

    def test_23_derived_table(self):
        """Subquery in FROM -- inline view."""
        sql = """
        SELECT dept_stats.DEPARTMENT_NAME,
               dept_stats.avg_los,
               dept_stats.encounter_count
        FROM (
            SELECT dep.DEPARTMENT_NAME,
                   AVG(DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME)) AS avg_los,
                   COUNT(*) AS encounter_count
            FROM PAT_ENC_HSP e
            JOIN CLARITY_DEP dep ON e.DEPARTMENT_ID = dep.DEPARTMENT_ID
            WHERE e.HOSP_DISCH_TIME IS NOT NULL
            GROUP BY dep.DEPARTMENT_NAME
        ) dept_stats
        WHERE dept_stats.avg_los > 5
        """
        r = extract(sql)
        # Should find the derived table as a source
        assert any(s["type"] == "subquery" for s in r.get("sources", []))


# ============================================================
# LEVEL 8: Set operations
# ============================================================

class TestLevel8_SetOps:

    def test_24_union_all(self):
        """UNION ALL -- combining inpatient and ED encounters."""
        sql = """
        SELECT PAT_ENC_CSN_ID, PAT_ID, HOSP_ADMSN_TIME, 'Inpatient' AS source
        FROM PAT_ENC_HSP
        WHERE ADT_PAT_CLASS_C = 1

        UNION ALL

        SELECT PAT_ENC_CSN_ID, PAT_ID, HOSP_ADMSN_TIME, 'ED' AS source
        FROM PAT_ENC_HSP
        WHERE ADT_PAT_CLASS_C = 3
        """
        r = extract(sql)
        assert len(r.get("set_operations", [])) >= 1

    def test_25_union_in_cte(self):
        """UNION inside a CTE -- all encounter types."""
        sql = """
        WITH all_encounters AS (
            SELECT PAT_ID, PAT_ENC_CSN_ID, 'IP' AS enc_type FROM PAT_ENC_HSP WHERE ADT_PAT_CLASS_C = 1
            UNION ALL
            SELECT PAT_ID, PAT_ENC_CSN_ID, 'OP' AS enc_type FROM PAT_ENC WHERE ENC_TYPE_C = 101
            UNION ALL
            SELECT PAT_ID, PAT_ENC_CSN_ID, 'ED' AS enc_type FROM PAT_ENC_HSP WHERE ADT_PAT_CLASS_C = 3
        )
        SELECT p.PAT_MRN_ID, ae.enc_type, COUNT(*) AS cnt
        FROM all_encounters ae
        JOIN PATIENT p ON ae.PAT_ID = p.PAT_ID
        GROUP BY p.PAT_MRN_ID, ae.enc_type
        """
        r = extract(sql)
        assert len(r["ctes"]) == 1
        cte_logic = r["ctes"][0].get("logic", {})
        assert cte_logic.get("set_operations")


# ============================================================
# LEVEL 9: Complex real-world queries
# ============================================================

class TestLevel9_Complex:

    def test_26_readmission_report(self):
        """Full 30-day readmission report -- CTEs, window, CASE, aggregation."""
        sql = """
        WITH index_encounters AS (
            SELECT e.PAT_ID,
                   e.PAT_ENC_CSN_ID,
                   e.HOSP_ADMSN_TIME,
                   e.HOSP_DISCH_TIME,
                   e.DEPARTMENT_ID,
                   DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) AS los_days,
                   ROW_NUMBER() OVER (PARTITION BY e.PAT_ID ORDER BY e.HOSP_ADMSN_TIME) AS encounter_seq
            FROM PAT_ENC_HSP e
            WHERE e.HOSP_DISCH_TIME IS NOT NULL
              AND e.ADT_PAT_CLASS_C = 1
              AND e.HOSP_ADMSN_TIME >= '2025-01-01'
        ),
        with_readmit AS (
            SELECT ie.*,
                   LEAD(ie.HOSP_ADMSN_TIME) OVER (
                       PARTITION BY ie.PAT_ID ORDER BY ie.HOSP_ADMSN_TIME
                   ) AS next_admit_time,
                   DATEDIFF(DAY, ie.HOSP_DISCH_TIME,
                       LEAD(ie.HOSP_ADMSN_TIME) OVER (
                           PARTITION BY ie.PAT_ID ORDER BY ie.HOSP_ADMSN_TIME
                       )
                   ) AS days_to_readmit
            FROM index_encounters ie
        ),
        flagged AS (
            SELECT wr.*,
                   CASE
                       WHEN wr.days_to_readmit <= 30 THEN 1
                       ELSE 0
                   END AS is_30day_readmit,
                   CASE
                       WHEN wr.days_to_readmit <= 7 THEN 'Early'
                       WHEN wr.days_to_readmit BETWEEN 8 AND 30 THEN 'Late'
                       ELSE 'None'
                   END AS readmit_category
            FROM with_readmit wr
        )
        SELECT dep.DEPARTMENT_NAME,
               COUNT(*) AS total_discharges,
               SUM(f.is_30day_readmit) AS readmissions,
               CAST(SUM(f.is_30day_readmit) AS FLOAT) / COUNT(*) * 100 AS readmit_rate_pct,
               AVG(f.los_days) AS avg_los,
               COUNT(CASE WHEN f.readmit_category = 'Early' THEN 1 END) AS early_readmits,
               COUNT(CASE WHEN f.readmit_category = 'Late' THEN 1 END) AS late_readmits
        FROM flagged f
        JOIN CLARITY_DEP dep ON f.DEPARTMENT_ID = dep.DEPARTMENT_ID
        GROUP BY dep.DEPARTMENT_NAME
        HAVING COUNT(*) >= 50
        ORDER BY readmit_rate_pct DESC
        """
        r = extract(sql)
        # CTEs
        assert len(r["ctes"]) == 3
        cte_names = [c["name"] for c in r["ctes"]]
        assert "index_encounters" in cte_names
        assert "with_readmit" in cte_names
        assert "flagged" in cte_names

        # Outputs
        t = types(r["outputs"])
        assert t["total_discharges"] == "aggregate"
        # Contains SUM/COUNT so classified as aggregate (aggregate takes precedence over calculated)
        assert t["readmit_rate_pct"] in ("calculated", "aggregate")

        # Filters
        assert any(f["scope"] == "having" for f in r["filters"])

        # Order
        assert len(r.get("order_by", [])) >= 1

    def test_27_cost_attribution(self):
        """Cost attribution -- joins, CASE, aggregation, multiple calculated fields."""
        sql = """
        SELECT p.PAT_MRN_ID,
               e.PAT_ENC_CSN_ID,
               dep.DEPARTMENT_NAME,
               ser.PROV_NAME AS attending_name,
               dx.DX_NAME AS primary_dx,
               DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) AS los_days,
               t.total_charges,
               t.total_payments,
               t.total_charges - t.total_payments AS balance,
               CASE
                   WHEN t.total_charges = 0 THEN 0
                   ELSE ROUND(t.total_payments / t.total_charges * 100, 1)
               END AS collection_rate_pct,
               CASE
                   WHEN DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) <= 2 THEN 'Short'
                   WHEN DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) <= 5 THEN 'Medium'
                   WHEN DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) <= 10 THEN 'Long'
                   ELSE 'Extended'
               END AS los_category
        FROM PAT_ENC_HSP e
        JOIN PATIENT p ON e.PAT_ID = p.PAT_ID
        JOIN CLARITY_DEP dep ON e.DEPARTMENT_ID = dep.DEPARTMENT_ID
        JOIN CLARITY_SER ser ON e.ATTENDING_PROV_ID = ser.PROV_ID
        LEFT JOIN HSP_ACCT_DX_LIST dl ON e.HSP_ACCOUNT_ID = dl.HSP_ACCOUNT_ID AND dl.LINE = 1
        LEFT JOIN CLARITY_EDG dx ON dl.DX_ID = dx.DX_ID
        LEFT JOIN (
            SELECT HSP_ACCOUNT_ID,
                   SUM(CASE WHEN TX_TYPE_C = 1 THEN TX_AMOUNT ELSE 0 END) AS total_charges,
                   SUM(CASE WHEN TX_TYPE_C = 2 THEN TX_AMOUNT ELSE 0 END) AS total_payments
            FROM HSP_TRANSACTIONS
            GROUP BY HSP_ACCOUNT_ID
        ) t ON e.HSP_ACCOUNT_ID = t.HSP_ACCOUNT_ID
        WHERE e.HOSP_DISCH_TIME IS NOT NULL
          AND e.ADT_PAT_CLASS_C = 1
          AND e.HOSP_ADMSN_TIME >= '2025-01-01'
        """
        r = extract(sql)
        t = types(r["outputs"])
        assert t["los_days"] == "calculated"
        assert t["balance"] == "calculated"
        assert t["collection_rate_pct"] == "case"
        assert t["los_category"] == "case"
        assert len(r["joins"]) >= 5
        # Derived table in JOIN -- may appear as subquery source or as a join with subquery content
        has_subquery_source = any(s["type"] == "subquery" for s in r.get("sources", []))
        has_subquery_join = any("SELECT" in j.get("right_table", "") for j in r.get("joins", []))
        assert has_subquery_source or has_subquery_join

    def test_28_medication_timing(self):
        """Window + CTE + CASE -- medication administration timing analysis."""
        sql = """
        WITH med_events AS (
            SELECT om.PAT_ENC_CSN_ID,
                   om.ORDER_MED_ID,
                   om.DESCRIPTION AS med_name,
                   ma.TAKEN_TIME,
                   ma.SCHEDULED_TIME,
                   DATEDIFF(MINUTE, ma.SCHEDULED_TIME, ma.TAKEN_TIME) AS delay_minutes,
                   ROW_NUMBER() OVER (
                       PARTITION BY om.PAT_ENC_CSN_ID, om.ORDER_MED_ID
                       ORDER BY ma.TAKEN_TIME
                   ) AS admin_seq,
                   LAG(ma.TAKEN_TIME) OVER (
                       PARTITION BY om.PAT_ENC_CSN_ID, om.ORDER_MED_ID
                       ORDER BY ma.TAKEN_TIME
                   ) AS prev_admin_time
            FROM ORDER_MED om
            JOIN MAR_ADMIN_INFO ma ON om.ORDER_MED_ID = ma.ORDER_MED_ID
            WHERE ma.MAR_ACTION_C = 1
              AND ma.TAKEN_TIME IS NOT NULL
        )
        SELECT med_name,
               COUNT(*) AS total_admins,
               AVG(delay_minutes) AS avg_delay_min,
               COUNT(CASE WHEN delay_minutes > 60 THEN 1 END) AS late_admins,
               CAST(COUNT(CASE WHEN delay_minutes > 60 THEN 1 END) AS FLOAT)
                   / COUNT(*) * 100 AS late_pct,
               CASE
                   WHEN AVG(delay_minutes) <= 15 THEN 'On Time'
                   WHEN AVG(delay_minutes) <= 60 THEN 'Slightly Delayed'
                   ELSE 'Significantly Delayed'
               END AS timing_category
        FROM med_events
        GROUP BY med_name
        HAVING COUNT(*) >= 20
        ORDER BY avg_delay_min DESC
        """
        r = extract(sql)
        assert len(r["ctes"]) == 1
        cte_logic = r["ctes"][0].get("logic", {})
        # CTE should have window functions
        assert cte_logic.get("window_functions")
        # Main query
        t = types(r["outputs"])
        assert t["total_admins"] == "aggregate"
        # Contains AVG inside CASE branches, so classified as aggregate (aggregate precedence)
        assert t["timing_category"] in ("case", "aggregate")


# ============================================================
# LEVEL 10: Edge cases
# ============================================================

class TestLevel10_EdgeCases:

    def test_29_select_star(self):
        """SELECT * -- should detect star."""
        sql = "SELECT * FROM PATIENT WHERE PAT_STATUS = 'Active'"
        r = extract(sql)
        assert any(o["type"] == "star" for o in r["outputs"])

    def test_30_distinct(self):
        """DISTINCT flag."""
        sql = "SELECT DISTINCT PAT_ID FROM PAT_ENC_HSP"
        r = extract(sql)
        assert r.get("distinct") is True

    def test_31_or_conditions(self):
        """OR in WHERE -- should NOT split (only AND splits)."""
        sql = """
        SELECT PAT_MRN_ID
        FROM PATIENT
        WHERE PAT_STATUS = 'Active' OR PAT_STATUS = 'Inactive'
        """
        r = extract(sql)
        where_filters = filter_exprs(r["filters"], "where")
        # OR stays as one expression, not split
        assert len(where_filters) == 1

    def test_32_coalesce_and_functions(self):
        """COALESCE, ISNULL, string functions -- all calculated."""
        sql = """
        SELECT PAT_MRN_ID,
               COALESCE(PAT_FIRST_NAME, 'Unknown') AS first_name,
               UPPER(PAT_LAST_NAME) AS last_name_upper,
               CONCAT(PAT_FIRST_NAME, ' ', PAT_LAST_NAME) AS full_name
        FROM PATIENT
        """
        r = extract(sql)
        t = types(r["outputs"])
        assert t["first_name"] == "calculated"
        assert t["last_name_upper"] == "calculated"
        assert t["full_name"] == "calculated"

    def test_33_between_and_in(self):
        """BETWEEN and IN in WHERE."""
        sql = """
        SELECT PAT_ENC_CSN_ID
        FROM PAT_ENC_HSP
        WHERE HOSP_ADMSN_TIME BETWEEN '2025-01-01' AND '2025-12-31'
          AND ADT_PAT_CLASS_C IN (1, 2, 3)
          AND DEPARTMENT_ID NOT IN (100, 200)
        """
        r = extract(sql)
        where_filters = filter_exprs(r["filters"], "where")
        assert len(where_filters) == 3

    def test_34_multiple_statements(self):
        """Multiple SQL statements -- should handle gracefully."""
        sql = """
        SELECT 1 AS a FROM DUAL;
        SELECT 2 AS b FROM DUAL;
        """
        r = extract(sql)
        # Should at least parse the first statement
        assert len(r["outputs"]) >= 1


# ============================================================
# Run standalone
# ============================================================

if __name__ == "__main__":
    import traceback

    classes = [
        TestLevel1_Simple,
        TestLevel2_Joins,
        TestLevel3_Case,
        TestLevel4_Aggregation,
        TestLevel5_Window,
        TestLevel6_CTE,
        TestLevel7_Subquery,
        TestLevel8_SetOps,
        TestLevel9_Complex,
        TestLevel10_EdgeCases,
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
                errors.append((method_name, doc.strip(), e))
                print(f"  FAIL  {method_name}: {doc.strip()}")
                print(f"        {e}")

    print(f"\n{'='*60}")
    print(f"  Results: {passed}/{total} passed, {failed} failed")
    print(f"{'='*60}")

    if errors:
        print(f"\nFailed tests:")
        for name, doc, err in errors:
            print(f"\n  {name}: {doc}")
            print(f"  Error: {err}")

    sys.exit(0 if failed == 0 else 1)
