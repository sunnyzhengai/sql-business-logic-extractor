#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Dump extraction output for every test query across all test files.

Generates per-query output files with:
  - Formatted SQL (pretty-printed)
  - L1: Raw extraction
  - L2: Normalized business definitions
  - L4: Plain English translation
  - L5: Resolved lineage to base table.column
"""

import json
import os

import sqlglot
from sql_logic_extractor.extract import SQLBusinessLogicExtractor, to_dict
from sql_logic_extractor.normalize import extract_definitions, definitions_to_dict
from sql_logic_extractor.translate import translate_query
from sql_logic_extractor.resolve import resolve_query, resolved_to_dict

OUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUT_DIR, exist_ok=True)

extractor = SQLBusinessLogicExtractor()


def fmt_sql(sql):
    """Pretty-print SQL using sqlglot."""
    try:
        return sqlglot.transpile(sql.strip(), pretty=True)[0]
    except Exception:
        return sql.strip()


# ---------------------------------------------------------------------------
# ALL test queries from all test files
# ---------------------------------------------------------------------------

QUERIES = {
    # =======================================================================
    # Layer 1 tests (test_queries.py)
    # =======================================================================

    # Level 1: Simple
    "L1_01_basic_select": """
        SELECT PAT_MRN_ID, PAT_FIRST_NAME, PAT_LAST_NAME FROM PATIENT
    """,
    "L1_02_select_with_alias": """
        SELECT PAT_MRN_ID AS mrn, PAT_FIRST_NAME AS first_name, PAT_LAST_NAME AS last_name FROM PATIENT
    """,
    "L1_03_where_filter": """
        SELECT PAT_MRN_ID, PAT_FIRST_NAME FROM PATIENT WHERE PAT_STATUS = 'Active'
    """,
    "L1_04_multiple_where": """
        SELECT PAT_MRN_ID FROM PATIENT
        WHERE PAT_STATUS = 'Active' AND BIRTH_DATE > '1950-01-01' AND STATE_ABBR = 'TX'
    """,
    "L1_05_calculated_column": """
        SELECT PAT_MRN_ID, DATEDIFF(YEAR, BIRTH_DATE, GETDATE()) AS age FROM PATIENT
    """,
    "L1_06_literal_column": """
        SELECT PAT_MRN_ID, 'Inpatient' AS encounter_type, 1 AS is_active FROM PATIENT
    """,

    # Level 2: JOINs
    "L1_07_inner_join": """
        SELECT p.PAT_MRN_ID, e.PAT_ENC_CSN_ID, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME
        FROM PAT_ENC_HSP e JOIN PATIENT p ON e.PAT_ID = p.PAT_ID
        WHERE e.HOSP_DISCH_TIME IS NOT NULL
    """,
    "L1_08_left_join": """
        SELECT e.PAT_ENC_CSN_ID, dx.DX_NAME
        FROM PAT_ENC_HSP e
        LEFT JOIN HSP_ACCT_DX_LIST dl ON e.HSP_ACCOUNT_ID = dl.HSP_ACCOUNT_ID
        LEFT JOIN CLARITY_EDG dx ON dl.DX_ID = dx.DX_ID
        WHERE dl.LINE = 1
    """,
    "L1_09_multi_join_calculated": """
        SELECT p.PAT_MRN_ID, e.PAT_ENC_CSN_ID, dep.DEPARTMENT_NAME,
               DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) AS los_days,
               DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) * dep.COST_PER_DAY AS total_cost
        FROM PAT_ENC_HSP e
        JOIN PATIENT p ON e.PAT_ID = p.PAT_ID
        JOIN CLARITY_DEP dep ON e.DEPARTMENT_ID = dep.DEPARTMENT_ID
        WHERE e.HOSP_DISCH_TIME IS NOT NULL AND e.ADT_PAT_CLASS_C = 1
    """,

    # Level 3: CASE
    "L1_10_simple_case": """
        SELECT PAT_MRN_ID,
               CASE WHEN AGE_YEARS < 18 THEN 'Pediatric'
                    WHEN AGE_YEARS BETWEEN 18 AND 64 THEN 'Adult'
                    ELSE 'Geriatric'
               END AS age_group
        FROM PATIENT
    """,
    "L1_11_nested_case": """
        SELECT e.PAT_ENC_CSN_ID,
               CASE WHEN dx.DX_NAME LIKE '%heart failure%' AND e.LOS_DAYS > 7 THEN 'High'
                    WHEN dx.DX_NAME LIKE '%pneumonia%' THEN 'Medium'
                    WHEN e.ED_DEPARTURE_TIME IS NOT NULL THEN 'Medium'
                    ELSE 'Low'
               END AS readmit_risk,
               CASE e.ADT_PAT_CLASS_C
                    WHEN 1 THEN 'Inpatient' WHEN 2 THEN 'Outpatient'
                    WHEN 3 THEN 'Emergency' ELSE 'Other'
               END AS visit_type
        FROM PAT_ENC_HSP e
        LEFT JOIN HSP_ACCT_DX_LIST dl ON e.HSP_ACCOUNT_ID = dl.HSP_ACCOUNT_ID AND dl.LINE = 1
        LEFT JOIN CLARITY_EDG dx ON dl.DX_ID = dx.DX_ID
    """,

    # Level 4: Aggregation
    "L1_12_simple_aggregate": """
        SELECT dep.DEPARTMENT_NAME,
               COUNT(DISTINCT e.PAT_ENC_CSN_ID) AS encounter_count,
               AVG(DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME)) AS avg_los
        FROM PAT_ENC_HSP e
        JOIN CLARITY_DEP dep ON e.DEPARTMENT_ID = dep.DEPARTMENT_ID
        WHERE e.HOSP_DISCH_TIME IS NOT NULL
        GROUP BY dep.DEPARTMENT_NAME
    """,
    "L1_13_having_filter": """
        SELECT p.PRIMARY_DX_CODE, COUNT(*) AS patient_count, AVG(e.TOTAL_CHARGES) AS avg_charges
        FROM PAT_ENC_HSP e JOIN PATIENT p ON e.PAT_ID = p.PAT_ID
        GROUP BY p.PRIMARY_DX_CODE
        HAVING COUNT(*) >= 10 AND AVG(e.TOTAL_CHARGES) > 5000
    """,
    "L1_14_conditional_aggregation": """
        SELECT dep.DEPARTMENT_NAME,
               COUNT(CASE WHEN e.DISCH_DISPOSITION_C = 1 THEN 1 END) AS discharged_home,
               COUNT(CASE WHEN e.DISCH_DISPOSITION_C = 2 THEN 1 END) AS transferred,
               COUNT(CASE WHEN e.DISCH_DISPOSITION_C = 20 THEN 1 END) AS expired,
               COUNT(*) AS total
        FROM PAT_ENC_HSP e
        JOIN CLARITY_DEP dep ON e.DEPARTMENT_ID = dep.DEPARTMENT_ID
        GROUP BY dep.DEPARTMENT_NAME
    """,

    # Level 5: Window functions
    "L1_15_row_number": """
        SELECT * FROM (
            SELECT e.PAT_ENC_CSN_ID, e.PAT_ID, e.HOSP_ADMSN_TIME,
                   ROW_NUMBER() OVER (PARTITION BY e.PAT_ID ORDER BY e.HOSP_ADMSN_TIME DESC) AS rn
            FROM PAT_ENC_HSP e WHERE e.HOSP_DISCH_TIME IS NOT NULL
        ) ranked WHERE rn = 1
    """,
    "L1_16_lag_readmission": """
        SELECT PAT_ID, PAT_ENC_CSN_ID, HOSP_ADMSN_TIME, HOSP_DISCH_TIME,
               LAG(HOSP_DISCH_TIME) OVER (PARTITION BY PAT_ID ORDER BY HOSP_ADMSN_TIME) AS prev_discharge,
               DATEDIFF(DAY, LAG(HOSP_DISCH_TIME) OVER (PARTITION BY PAT_ID ORDER BY HOSP_ADMSN_TIME), HOSP_ADMSN_TIME) AS days_since_last_discharge
        FROM PAT_ENC_HSP WHERE HOSP_DISCH_TIME IS NOT NULL
    """,
    "L1_17_running_total": """
        SELECT PAT_ENC_CSN_ID, SERVICE_DATE, TX_AMOUNT,
               SUM(TX_AMOUNT) OVER (PARTITION BY PAT_ENC_CSN_ID ORDER BY SERVICE_DATE
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_charges
        FROM HSP_TRANSACTIONS
    """,

    # Level 6: CTEs
    "L1_18_simple_cte": """
        WITH encounters AS (
            SELECT e.PAT_ENC_CSN_ID, e.PAT_ID, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME,
                   DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) AS los_days
            FROM PAT_ENC_HSP e
            WHERE e.HOSP_DISCH_TIME IS NOT NULL AND e.ADT_PAT_CLASS_C = 1
        )
        SELECT enc.PAT_ID, enc.los_days, p.PAT_MRN_ID
        FROM encounters enc JOIN PATIENT p ON enc.PAT_ID = p.PAT_ID
        WHERE enc.los_days > 3
    """,
    "L1_19_chained_ctes": """
        WITH discharges AS (
            SELECT PAT_ID, PAT_ENC_CSN_ID, HOSP_DISCH_TIME, DISCH_DISPOSITION_C
            FROM PAT_ENC_HSP WHERE HOSP_DISCH_TIME IS NOT NULL AND ADT_PAT_CLASS_C = 1
        ),
        readmissions AS (
            SELECT d1.PAT_ID, d1.PAT_ENC_CSN_ID AS index_csn, d2.PAT_ENC_CSN_ID AS readmit_csn,
                   DATEDIFF(DAY, d1.HOSP_DISCH_TIME, d2.HOSP_DISCH_TIME) AS days_to_readmit
            FROM discharges d1 JOIN discharges d2
                ON d1.PAT_ID = d2.PAT_ID AND d2.HOSP_DISCH_TIME > d1.HOSP_DISCH_TIME
                AND DATEDIFF(DAY, d1.HOSP_DISCH_TIME, d2.HOSP_DISCH_TIME) <= 30
        )
        SELECT r.PAT_ID, p.PAT_MRN_ID, r.index_csn, r.readmit_csn, r.days_to_readmit
        FROM readmissions r JOIN PATIENT p ON r.PAT_ID = p.PAT_ID
        ORDER BY r.days_to_readmit
    """,

    # Level 7: Subqueries
    "L1_20_subquery_in_where": """
        SELECT p.PAT_MRN_ID, p.PAT_FIRST_NAME, p.PAT_LAST_NAME
        FROM PATIENT p
        WHERE p.PAT_ID IN (
            SELECT DISTINCT e.PAT_ID FROM PAT_ENC_HSP e
            JOIN HSP_ACCT_DX_LIST dl ON e.HSP_ACCOUNT_ID = dl.HSP_ACCOUNT_ID
            JOIN CLARITY_EDG dx ON dl.DX_ID = dx.DX_ID
            WHERE dx.ICD10_CODE LIKE 'I50%'
        )
    """,
    "L1_21_exists_subquery": """
        SELECT e.PAT_ENC_CSN_ID, e.HOSP_ADMSN_TIME
        FROM PAT_ENC_HSP e
        WHERE EXISTS (
            SELECT 1 FROM ORDER_MED om
            WHERE om.PAT_ENC_CSN_ID = e.PAT_ENC_CSN_ID AND om.ORDER_STATUS_C = 2
        )
    """,
    "L1_22_scalar_subquery": """
        SELECT e.PAT_ENC_CSN_ID,
               (SELECT MAX(fm.RECORDED_TIME) FROM IP_FLWSHT_MEAS fm
                WHERE fm.PAT_ENC_CSN_ID = e.PAT_ENC_CSN_ID AND fm.FLO_MEAS_ID = '5') AS last_bp_time
        FROM PAT_ENC_HSP e
    """,
    "L1_23_derived_table": """
        SELECT dept_stats.DEPARTMENT_NAME, dept_stats.avg_los, dept_stats.encounter_count
        FROM (
            SELECT dep.DEPARTMENT_NAME,
                   AVG(DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME)) AS avg_los,
                   COUNT(*) AS encounter_count
            FROM PAT_ENC_HSP e
            JOIN CLARITY_DEP dep ON e.DEPARTMENT_ID = dep.DEPARTMENT_ID
            WHERE e.HOSP_DISCH_TIME IS NOT NULL
            GROUP BY dep.DEPARTMENT_NAME
        ) dept_stats WHERE dept_stats.avg_los > 5
    """,

    # Level 8: Set operations
    "L1_24_union_all": """
        SELECT PAT_ENC_CSN_ID, PAT_ID, HOSP_ADMSN_TIME, 'Inpatient' AS source
        FROM PAT_ENC_HSP WHERE ADT_PAT_CLASS_C = 1
        UNION ALL
        SELECT PAT_ENC_CSN_ID, PAT_ID, HOSP_ADMSN_TIME, 'ED' AS source
        FROM PAT_ENC_HSP WHERE ADT_PAT_CLASS_C = 3
    """,
    "L1_25_union_in_cte": """
        WITH all_encounters AS (
            SELECT PAT_ID, PAT_ENC_CSN_ID, 'IP' AS enc_type FROM PAT_ENC_HSP WHERE ADT_PAT_CLASS_C = 1
            UNION ALL
            SELECT PAT_ID, PAT_ENC_CSN_ID, 'OP' AS enc_type FROM PAT_ENC WHERE ENC_TYPE_C = 101
            UNION ALL
            SELECT PAT_ID, PAT_ENC_CSN_ID, 'ED' AS enc_type FROM PAT_ENC_HSP WHERE ADT_PAT_CLASS_C = 3
        )
        SELECT p.PAT_MRN_ID, ae.enc_type, COUNT(*) AS cnt
        FROM all_encounters ae JOIN PATIENT p ON ae.PAT_ID = p.PAT_ID
        GROUP BY p.PAT_MRN_ID, ae.enc_type
    """,

    # Level 9: Complex
    "L1_26_readmission_report": """
        WITH index_encounters AS (
            SELECT e.PAT_ID, e.PAT_ENC_CSN_ID, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME, e.DEPARTMENT_ID,
                   DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) AS los_days,
                   ROW_NUMBER() OVER (PARTITION BY e.PAT_ID ORDER BY e.HOSP_ADMSN_TIME) AS encounter_seq
            FROM PAT_ENC_HSP e
            WHERE e.HOSP_DISCH_TIME IS NOT NULL AND e.ADT_PAT_CLASS_C = 1 AND e.HOSP_ADMSN_TIME >= '2025-01-01'
        ),
        with_readmit AS (
            SELECT ie.*,
                   LEAD(ie.HOSP_ADMSN_TIME) OVER (PARTITION BY ie.PAT_ID ORDER BY ie.HOSP_ADMSN_TIME) AS next_admit_time,
                   DATEDIFF(DAY, ie.HOSP_DISCH_TIME,
                       LEAD(ie.HOSP_ADMSN_TIME) OVER (PARTITION BY ie.PAT_ID ORDER BY ie.HOSP_ADMSN_TIME)
                   ) AS days_to_readmit
            FROM index_encounters ie
        ),
        flagged AS (
            SELECT wr.*,
                   CASE WHEN wr.days_to_readmit <= 30 THEN 1 ELSE 0 END AS is_30day_readmit,
                   CASE WHEN wr.days_to_readmit <= 7 THEN 'Early'
                        WHEN wr.days_to_readmit BETWEEN 8 AND 30 THEN 'Late' ELSE 'None'
                   END AS readmit_category
            FROM with_readmit wr
        )
        SELECT dep.DEPARTMENT_NAME,
               COUNT(*) AS total_discharges, SUM(f.is_30day_readmit) AS readmissions,
               CAST(SUM(f.is_30day_readmit) AS FLOAT) / COUNT(*) * 100 AS readmit_rate_pct,
               AVG(f.los_days) AS avg_los,
               COUNT(CASE WHEN f.readmit_category = 'Early' THEN 1 END) AS early_readmits,
               COUNT(CASE WHEN f.readmit_category = 'Late' THEN 1 END) AS late_readmits
        FROM flagged f JOIN CLARITY_DEP dep ON f.DEPARTMENT_ID = dep.DEPARTMENT_ID
        GROUP BY dep.DEPARTMENT_NAME HAVING COUNT(*) >= 50
        ORDER BY readmit_rate_pct DESC
    """,
    "L1_27_cost_attribution": """
        SELECT p.PAT_MRN_ID, e.PAT_ENC_CSN_ID, dep.DEPARTMENT_NAME,
               ser.PROV_NAME AS attending_name, dx.DX_NAME AS primary_dx,
               DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) AS los_days,
               t.total_charges, t.total_payments,
               t.total_charges - t.total_payments AS balance,
               CASE WHEN t.total_charges = 0 THEN 0
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
            FROM HSP_TRANSACTIONS GROUP BY HSP_ACCOUNT_ID
        ) t ON e.HSP_ACCOUNT_ID = t.HSP_ACCOUNT_ID
        WHERE e.HOSP_DISCH_TIME IS NOT NULL AND e.ADT_PAT_CLASS_C = 1 AND e.HOSP_ADMSN_TIME >= '2025-01-01'
    """,
    "L1_28_medication_timing": """
        WITH med_events AS (
            SELECT om.PAT_ENC_CSN_ID, om.ORDER_MED_ID, om.DESCRIPTION AS med_name,
                   ma.TAKEN_TIME, ma.SCHEDULED_TIME,
                   DATEDIFF(MINUTE, ma.SCHEDULED_TIME, ma.TAKEN_TIME) AS delay_minutes,
                   ROW_NUMBER() OVER (PARTITION BY om.PAT_ENC_CSN_ID, om.ORDER_MED_ID ORDER BY ma.TAKEN_TIME) AS admin_seq,
                   LAG(ma.TAKEN_TIME) OVER (PARTITION BY om.PAT_ENC_CSN_ID, om.ORDER_MED_ID ORDER BY ma.TAKEN_TIME) AS prev_admin_time
            FROM ORDER_MED om
            JOIN MAR_ADMIN_INFO ma ON om.ORDER_MED_ID = ma.ORDER_MED_ID
            WHERE ma.MAR_ACTION_C = 1 AND ma.TAKEN_TIME IS NOT NULL
        )
        SELECT med_name, COUNT(*) AS total_admins,
               AVG(delay_minutes) AS avg_delay_min,
               COUNT(CASE WHEN delay_minutes > 60 THEN 1 END) AS late_admins,
               CAST(COUNT(CASE WHEN delay_minutes > 60 THEN 1 END) AS FLOAT) / COUNT(*) * 100 AS late_pct,
               CASE WHEN AVG(delay_minutes) <= 15 THEN 'On Time'
                    WHEN AVG(delay_minutes) <= 60 THEN 'Slightly Delayed'
                    ELSE 'Significantly Delayed'
               END AS timing_category
        FROM med_events GROUP BY med_name
        HAVING COUNT(*) >= 20 ORDER BY avg_delay_min DESC
    """,

    # Level 10: Edge cases
    "L1_29_select_star": "SELECT * FROM PATIENT WHERE PAT_STATUS = 'Active'",
    "L1_30_distinct": "SELECT DISTINCT PAT_ID FROM PAT_ENC_HSP",
    "L1_31_or_conditions": """
        SELECT PAT_MRN_ID FROM PATIENT WHERE PAT_STATUS = 'Active' OR PAT_STATUS = 'Inactive'
    """,
    "L1_32_coalesce_functions": """
        SELECT PAT_MRN_ID,
               COALESCE(PAT_FIRST_NAME, 'Unknown') AS first_name,
               UPPER(PAT_LAST_NAME) AS last_name_upper,
               CONCAT(PAT_FIRST_NAME, ' ', PAT_LAST_NAME) AS full_name
        FROM PATIENT
    """,
    "L1_33_between_and_in": """
        SELECT PAT_ENC_CSN_ID FROM PAT_ENC_HSP
        WHERE HOSP_ADMSN_TIME BETWEEN '2025-01-01' AND '2025-12-31'
          AND ADT_PAT_CLASS_C IN (1, 2, 3) AND DEPARTMENT_ID NOT IN (100, 200)
    """,

    # =======================================================================
    # Layer 2 tests (test_normalize.py) -- additional queries
    # =======================================================================
    "L2_06_date_calculation": """
        SELECT DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) AS los_days FROM PAT_ENC_HSP e
    """,
    "L2_09_conditional_count": """
        SELECT COUNT(CASE WHEN DISCH_DISPOSITION_C = 1 THEN 1 END) AS discharged_home FROM PAT_ENC_HSP
    """,
    "L2_10_window_ranking": """
        SELECT ROW_NUMBER() OVER (PARTITION BY PAT_ID ORDER BY HOSP_ADMSN_TIME DESC) AS rn FROM PAT_ENC_HSP
    """,
    "L2_11_window_lag": """
        SELECT LAG(HOSP_DISCH_TIME) OVER (PARTITION BY PAT_ID ORDER BY HOSP_ADMSN_TIME) AS prev_discharge FROM PAT_ENC_HSP
    """,
    "L2_14_arithmetic": """
        SELECT total_charges - total_payments AS balance FROM HSP_TRANSACTIONS
    """,
    "L2_15_filter_classification": """
        SELECT PAT_MRN_ID FROM PATIENT
        WHERE PAT_STATUS = 'Active' AND BIRTH_DATE > '1950-01-01' AND PAT_LAST_NAME LIKE 'Smith%'
    """,

    # =======================================================================
    # Layer 3 tests (test_compare.py) -- comparison pairs
    # =======================================================================
    "L3_los_query_a": """
        SELECT DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) AS los_days FROM PAT_ENC_HSP e
    """,
    "L3_los_query_b": """
        SELECT DATEDIFF(DAY, enc.HOSP_ADMSN_TIME, enc.HOSP_DISCH_TIME) AS length_of_stay FROM PAT_ENC_HSP enc
    """,
    "L3_case_query_a": """
        SELECT CASE WHEN LOS_DAYS <= 3 THEN 'Short' WHEN LOS_DAYS <= 7 THEN 'Medium' ELSE 'Long' END AS los_category FROM PAT_ENC_HSP
    """,
    "L3_case_query_b": """
        SELECT CASE WHEN TOTAL_CHARGES <= 5000 THEN 'Low' WHEN TOTAL_CHARGES <= 20000 THEN 'Medium' ELSE 'High' END AS cost_category FROM HSP_TRANSACTIONS
    """,

    # =======================================================================
    # Layer 5 tests (test_resolve.py) -- lineage resolution
    # =======================================================================
    "L5_04_cte_passthrough": """
        WITH enc AS (
            SELECT e.PAT_ID, e.PAT_ENC_CSN_ID FROM PAT_ENC_HSP e WHERE e.ADT_PAT_CLASS_C = 1
        )
        SELECT PAT_ID, PAT_ENC_CSN_ID FROM enc
    """,
    "L5_07_two_hop_passthrough": """
        WITH a AS (SELECT PAT_ID FROM PAT_ENC_HSP),
             b AS (SELECT PAT_ID FROM a)
        SELECT PAT_ID FROM b
    """,
    "L5_09_case_using_cte_column": """
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
    """,
    "L5_12_derived_table_with_filter": """
        SELECT s.total_charges
        FROM (
            SELECT HSP_ACCOUNT_ID, SUM(TX_AMOUNT) AS total_charges
            FROM HSP_TRANSACTIONS WHERE TX_TYPE_C = 1
            GROUP BY HSP_ACCOUNT_ID
        ) s
        WHERE s.total_charges > 1000
    """,
    "L5_14_readmission_full_chain": """
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
            SELECT disch_status, COUNT(*) AS discharges, AVG(los_days) AS avg_los
            FROM base GROUP BY disch_status
        )
        SELECT disch_status, discharges, avg_los FROM summary
    """,
}


# ---------------------------------------------------------------------------
# Generate all outputs
# ---------------------------------------------------------------------------

print(f"Dumping {len(QUERIES)} queries to {OUT_DIR}/\n")

for name, sql in QUERIES.items():
    sql = sql.strip()
    formatted = fmt_sql(sql)

    # Layer 1: Extract
    logic = to_dict(extractor.extract(sql))
    with open(os.path.join(OUT_DIR, f"{name}_L1_extract.json"), "w") as f:
        json.dump(logic, f, indent=2, default=str)

    # Layer 2: Normalize
    defs = extract_definitions(sql, query_label=name)
    with open(os.path.join(OUT_DIR, f"{name}_L2_definitions.json"), "w") as f:
        json.dump({"definitions": definitions_to_dict(defs)}, f, indent=2, default=str)

    # Layer 4: Translate
    translated = translate_query(sql, query_label=name)
    with open(os.path.join(OUT_DIR, f"{name}_L4_english.json"), "w") as f:
        json.dump({"definitions": translated}, f, indent=2, default=str)

    # Layer 5: Resolve lineage
    resolved = resolve_query(sql)
    with open(os.path.join(OUT_DIR, f"{name}_L5_lineage.json"), "w") as f:
        json.dump(resolved_to_dict(resolved), f, indent=2, default=str)

    # Combined human-readable text output
    lines = []
    lines.append(f"{'='*70}")
    lines.append(f"  {name}")
    lines.append(f"{'='*70}")
    lines.append("")
    lines.append("FORMATTED SQL:")
    lines.append("-" * 40)
    lines.append(formatted)
    lines.append("")

    lines.append("BUSINESS DEFINITIONS (Plain English):")
    lines.append("-" * 40)
    for t in translated:
        lines.append(f"  {t['name']}")
        lines.append(f"    {t['business_description']}")
        if "source" in t:
            lines.append(f"    Source: {t['source']}")
        if "conditions" in t:
            for c in t["conditions"]:
                lines.append(f"    Condition: {c}")
        lines.append(f"    Technical: {t['technical_expression']}")
        lines.append("")

    lines.append("LINEAGE (Resolved to base tables):")
    lines.append("-" * 40)
    for col in resolved.columns:
        lines.append(f"  {col.name} ({col.type})")
        if col.resolved_expression:
            lines.append(f"    Resolved: {col.resolved_expression}")
        if col.base_columns:
            lines.append(f"    Base columns: {', '.join(col.base_columns)}")
        if col.base_tables:
            lines.append(f"    Base tables: {', '.join(col.base_tables)}")
        if col.filters:
            for flt in col.filters:
                lines.append(f"    Filter: {flt}")
        if col.transformation_chain and len(col.transformation_chain) > 1:
            lines.append(f"    Chain:")
            for i, step in enumerate(col.transformation_chain):
                indent = "      " + "  " * i
                scope = step.get("scope", "")
                sname = step.get("name", "")
                stype = step.get("type", "")
                sexpr = step.get("expression", "")
                if stype == "passthrough":
                    lines.append(f"{indent}-> {scope}.{sname} (passthrough)")
                else:
                    lines.append(f"{indent}-> {scope}.{sname} = {sexpr} ({stype})")
        lines.append("")

    with open(os.path.join(OUT_DIR, f"{name}_combined.txt"), "w") as f:
        f.write("\n".join(lines))

    n_out = len(logic.get('outputs', []))
    n_def = len(defs)
    n_res = len(resolved.columns)
    print(f"  {name}: {n_out} outputs, {n_def} defs, {n_res} resolved")

print(f"\nDone. {len(QUERIES)} queries x 5 files = {len(QUERIES)*5} files in {OUT_DIR}/")
