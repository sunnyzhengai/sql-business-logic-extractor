#!/usr/bin/env python3
"""Dump extraction + normalization output for every test query to JSON files."""

import json
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extract import SQLBusinessLogicExtractor, to_dict
from normalize import extract_definitions, definitions_to_dict

OUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUT_DIR, exist_ok=True)

extractor = SQLBusinessLogicExtractor()

QUERIES = {
    # Level 1
    "01_basic_select": """
        SELECT PAT_MRN_ID, PAT_FIRST_NAME, PAT_LAST_NAME FROM PATIENT
    """,
    "02_select_with_alias": """
        SELECT PAT_MRN_ID AS mrn, PAT_FIRST_NAME AS first_name, PAT_LAST_NAME AS last_name FROM PATIENT
    """,
    "03_where_filter": """
        SELECT PAT_MRN_ID, PAT_FIRST_NAME FROM PATIENT WHERE PAT_STATUS = 'Active'
    """,
    "04_multiple_where": """
        SELECT PAT_MRN_ID FROM PATIENT
        WHERE PAT_STATUS = 'Active' AND BIRTH_DATE > '1950-01-01' AND STATE_ABBR = 'TX'
    """,
    "05_calculated_column": """
        SELECT PAT_MRN_ID, DATEDIFF(YEAR, BIRTH_DATE, GETDATE()) AS age FROM PATIENT
    """,
    "06_literal_column": """
        SELECT PAT_MRN_ID, 'Inpatient' AS encounter_type, 1 AS is_active FROM PATIENT
    """,

    # Level 2
    "07_inner_join": """
        SELECT p.PAT_MRN_ID, e.PAT_ENC_CSN_ID, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME
        FROM PAT_ENC_HSP e JOIN PATIENT p ON e.PAT_ID = p.PAT_ID
        WHERE e.HOSP_DISCH_TIME IS NOT NULL
    """,
    "08_left_join": """
        SELECT e.PAT_ENC_CSN_ID, dx.DX_NAME
        FROM PAT_ENC_HSP e
        LEFT JOIN HSP_ACCT_DX_LIST dl ON e.HSP_ACCOUNT_ID = dl.HSP_ACCOUNT_ID
        LEFT JOIN CLARITY_EDG dx ON dl.DX_ID = dx.DX_ID
        WHERE dl.LINE = 1
    """,
    "09_multi_join_calculated": """
        SELECT p.PAT_MRN_ID, e.PAT_ENC_CSN_ID, dep.DEPARTMENT_NAME,
               DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) AS los_days,
               DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) * dep.COST_PER_DAY AS total_cost
        FROM PAT_ENC_HSP e
        JOIN PATIENT p ON e.PAT_ID = p.PAT_ID
        JOIN CLARITY_DEP dep ON e.DEPARTMENT_ID = dep.DEPARTMENT_ID
        WHERE e.HOSP_DISCH_TIME IS NOT NULL AND e.ADT_PAT_CLASS_C = 1
    """,

    # Level 3
    "10_simple_case": """
        SELECT PAT_MRN_ID,
               CASE WHEN AGE_YEARS < 18 THEN 'Pediatric'
                    WHEN AGE_YEARS BETWEEN 18 AND 64 THEN 'Adult'
                    ELSE 'Geriatric'
               END AS age_group
        FROM PATIENT
    """,
    "11_nested_case": """
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

    # Level 4
    "12_simple_aggregate": """
        SELECT dep.DEPARTMENT_NAME,
               COUNT(DISTINCT e.PAT_ENC_CSN_ID) AS encounter_count,
               AVG(DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME)) AS avg_los
        FROM PAT_ENC_HSP e
        JOIN CLARITY_DEP dep ON e.DEPARTMENT_ID = dep.DEPARTMENT_ID
        WHERE e.HOSP_DISCH_TIME IS NOT NULL
        GROUP BY dep.DEPARTMENT_NAME
    """,
    "13_having_filter": """
        SELECT p.PRIMARY_DX_CODE, COUNT(*) AS patient_count, AVG(e.TOTAL_CHARGES) AS avg_charges
        FROM PAT_ENC_HSP e JOIN PATIENT p ON e.PAT_ID = p.PAT_ID
        GROUP BY p.PRIMARY_DX_CODE
        HAVING COUNT(*) >= 10 AND AVG(e.TOTAL_CHARGES) > 5000
    """,
    "14_conditional_aggregation": """
        SELECT dep.DEPARTMENT_NAME,
               COUNT(CASE WHEN e.DISCH_DISPOSITION_C = 1 THEN 1 END) AS discharged_home,
               COUNT(CASE WHEN e.DISCH_DISPOSITION_C = 2 THEN 1 END) AS transferred,
               COUNT(CASE WHEN e.DISCH_DISPOSITION_C = 20 THEN 1 END) AS expired,
               COUNT(*) AS total
        FROM PAT_ENC_HSP e
        JOIN CLARITY_DEP dep ON e.DEPARTMENT_ID = dep.DEPARTMENT_ID
        GROUP BY dep.DEPARTMENT_NAME
    """,

    # Level 5
    "15_row_number": """
        SELECT * FROM (
            SELECT e.PAT_ENC_CSN_ID, e.PAT_ID, e.HOSP_ADMSN_TIME,
                   ROW_NUMBER() OVER (PARTITION BY e.PAT_ID ORDER BY e.HOSP_ADMSN_TIME DESC) AS rn
            FROM PAT_ENC_HSP e WHERE e.HOSP_DISCH_TIME IS NOT NULL
        ) ranked WHERE rn = 1
    """,
    "16_lag_readmission": """
        SELECT PAT_ID, PAT_ENC_CSN_ID, HOSP_ADMSN_TIME, HOSP_DISCH_TIME,
               LAG(HOSP_DISCH_TIME) OVER (PARTITION BY PAT_ID ORDER BY HOSP_ADMSN_TIME) AS prev_discharge,
               DATEDIFF(DAY, LAG(HOSP_DISCH_TIME) OVER (PARTITION BY PAT_ID ORDER BY HOSP_ADMSN_TIME), HOSP_ADMSN_TIME) AS days_since_last_discharge
        FROM PAT_ENC_HSP WHERE HOSP_DISCH_TIME IS NOT NULL
    """,
    "17_running_total": """
        SELECT PAT_ENC_CSN_ID, SERVICE_DATE, TX_AMOUNT,
               SUM(TX_AMOUNT) OVER (PARTITION BY PAT_ENC_CSN_ID ORDER BY SERVICE_DATE
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_charges
        FROM HSP_TRANSACTIONS
    """,

    # Level 6
    "18_simple_cte": """
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
    "19_chained_ctes": """
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

    # Level 7
    "20_subquery_in_where": """
        SELECT p.PAT_MRN_ID, p.PAT_FIRST_NAME, p.PAT_LAST_NAME
        FROM PATIENT p
        WHERE p.PAT_ID IN (
            SELECT DISTINCT e.PAT_ID FROM PAT_ENC_HSP e
            JOIN HSP_ACCT_DX_LIST dl ON e.HSP_ACCOUNT_ID = dl.HSP_ACCOUNT_ID
            JOIN CLARITY_EDG dx ON dl.DX_ID = dx.DX_ID
            WHERE dx.ICD10_CODE LIKE 'I50%'
        )
    """,
    "21_exists_subquery": """
        SELECT e.PAT_ENC_CSN_ID, e.HOSP_ADMSN_TIME
        FROM PAT_ENC_HSP e
        WHERE EXISTS (
            SELECT 1 FROM ORDER_MED om
            WHERE om.PAT_ENC_CSN_ID = e.PAT_ENC_CSN_ID AND om.ORDER_STATUS_C = 2
        )
    """,
    "22_scalar_subquery": """
        SELECT e.PAT_ENC_CSN_ID,
               (SELECT MAX(fm.RECORDED_TIME) FROM IP_FLWSHT_MEAS fm
                WHERE fm.PAT_ENC_CSN_ID = e.PAT_ENC_CSN_ID AND fm.FLO_MEAS_ID = '5') AS last_bp_time
        FROM PAT_ENC_HSP e
    """,
    "23_derived_table": """
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

    # Level 8
    "24_union_all": """
        SELECT PAT_ENC_CSN_ID, PAT_ID, HOSP_ADMSN_TIME, 'Inpatient' AS source
        FROM PAT_ENC_HSP WHERE ADT_PAT_CLASS_C = 1
        UNION ALL
        SELECT PAT_ENC_CSN_ID, PAT_ID, HOSP_ADMSN_TIME, 'ED' AS source
        FROM PAT_ENC_HSP WHERE ADT_PAT_CLASS_C = 3
    """,

    # Level 9
    "26_readmission_report": """
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
}

print(f"Dumping {len(QUERIES)} queries to {OUT_DIR}/\n")

for name, sql in QUERIES.items():
    # Layer 1
    logic = to_dict(extractor.extract(sql.strip()))
    with open(os.path.join(OUT_DIR, f"{name}_L1_extract.json"), "w") as f:
        json.dump(logic, f, indent=2, default=str)

    # Layer 2
    defs = extract_definitions(sql.strip(), query_label=name)
    with open(os.path.join(OUT_DIR, f"{name}_L2_definitions.json"), "w") as f:
        json.dump({"definitions": definitions_to_dict(defs)}, f, indent=2, default=str)

    print(f"  {name}: {len(logic.get('outputs', []))} outputs, {len(defs)} definitions")

print(f"\nDone. Files in {OUT_DIR}/")
