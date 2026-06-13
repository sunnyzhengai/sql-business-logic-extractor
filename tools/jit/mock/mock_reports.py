"""Generate the Report Glossary (8 mock report entries) and their SQL views.

Run directly:
    python3 -m tools.jit.mock.mock_reports [output_dir]

Outputs:
    {output_dir}/report_glossary/         — one YAML per report
    {output_dir}/report_sql/              — one SQL file per report (the view DDL)
"""

from __future__ import annotations

from pathlib import Path

import yaml


REPORTS = [
    {
        "report_name": "VW_DIABETIC_COHORT",
        "description": "Identifies diabetic patients — patients with active diabetes diagnoses — from the problem list for population health reporting and care management outreach.",
        "primary_purpose": "Diabetes cohort identification",
        "key_metrics": ["patient_count", "diagnosis_prevalence"],
        "developer": "J. Smith",
        "business_requester": "Population Health Team",
        "created_date": "2023-06-15",
        "last_modified": "2024-03-15",
        "parameters": [
            {"name": "icd10_pattern", "default": "E11%", "type": "string",
             "description": "ICD-10 code pattern for diabetes type"},
            {"name": "active_only", "default": True, "type": "boolean",
             "description": "Filter to active problems only (RESOLVED_DATE IS NULL)"},
        ],
        "tables_used": ["PATIENT", "PROBLEM_LIST", "CLARITY_EDG"],
        "domains": ["diagnosis", "demographics"],
        "column_count": 8,
        "sql_complexity": "medium",
        "inline_comments": [
            "-- Active diabetes only, excludes gestational",
            "-- Uses problem list, not encounter dx, for chronic condition tracking",
        ],
        "sql": """\
CREATE VIEW VW_DIABETIC_COHORT AS
WITH diabetic_patients AS (
    -- Active diabetes only, excludes gestational
    SELECT DISTINCT pl.PAT_ID, edg.DX_ID, edg.DX_NAME, edg.CURRENT_ICD10_LIST
    FROM PROBLEM_LIST pl
    JOIN CLARITY_EDG edg ON edg.DX_ID = pl.DX_ID
    WHERE edg.CURRENT_ICD10_LIST LIKE 'E11%'
      AND pl.RESOLVED_DATE IS NULL
),
-- Uses problem list, not encounter dx, for chronic condition tracking
active_only AS (
    SELECT dp.PAT_ID, dp.DX_NAME, dp.CURRENT_ICD10_LIST
    FROM diabetic_patients dp
)
SELECT
    p.PAT_ID,
    p.PAT_NAME,
    p.BIRTH_DATE,
    p.SEX,
    a.DX_NAME AS DIABETES_DIAGNOSIS,
    a.CURRENT_ICD10_LIST AS ICD10_CODE,
    p.ZIP,
    1 AS IS_DIABETIC
FROM PATIENT p
JOIN active_only a ON a.PAT_ID = p.PAT_ID;""",
    },
    {
        "report_name": "VW_ED_UTILIZATION",
        "description": "Analyzes emergency department visit frequency per patient, identifying high utilizers (>3 visits in 12 months) for care coordination intervention.",
        "primary_purpose": "ED utilization and high-utilizer identification",
        "key_metrics": ["ed_visit_count", "high_utilizer_count", "avg_visits_per_patient"],
        "developer": "M. Chen",
        "business_requester": "Care Coordination",
        "created_date": "2023-09-01",
        "last_modified": "2024-11-20",
        "parameters": [
            {"name": "utilization_threshold", "default": 3, "type": "integer",
             "description": "Number of visits above which a patient is a high utilizer"},
            {"name": "lookback_months", "default": 12, "type": "integer",
             "description": "Number of months to look back for visit counts"},
        ],
        "tables_used": ["PAT_ENC", "CLARITY_DEP", "PATIENT"],
        "domains": ["encounters", "demographics"],
        "column_count": 6,
        "sql_complexity": "medium",
        "inline_comments": [
            "-- ED departments identified by name pattern",
            "-- High utilizer = >3 visits in rolling 12 months",
        ],
        "sql": """\
CREATE VIEW VW_ED_UTILIZATION AS
WITH ed_encounters AS (
    -- ED departments identified by name pattern
    SELECT enc.PAT_ENC_CSN_ID, enc.PAT_ID, enc.CONTACT_DATE,
           dep.DEPARTMENT_NAME
    FROM PAT_ENC enc
    JOIN CLARITY_DEP dep ON dep.DEPARTMENT_ID = enc.DEPARTMENT_ID
    WHERE dep.DEPARTMENT_NAME LIKE '%Emergency%'
      AND enc.APPT_STATUS_C = 2
),
-- High utilizer = >3 visits in rolling 12 months
high_utilizers AS (
    SELECT PAT_ID,
           COUNT(*) AS visit_count
    FROM ed_encounters
    WHERE CONTACT_DATE >= date('now', '-12 months')
    GROUP BY PAT_ID
    HAVING COUNT(*) > 3
)
SELECT
    p.PAT_ID,
    p.PAT_NAME,
    COALESCE(hu.visit_count, 0) AS ED_VISIT_COUNT,
    CASE WHEN hu.PAT_ID IS NOT NULL THEN 1 ELSE 0 END AS IS_HIGH_UTILIZER,
    p.BIRTH_DATE,
    p.ZIP
FROM PATIENT p
LEFT JOIN high_utilizers hu ON hu.PAT_ID = p.PAT_ID
WHERE hu.PAT_ID IS NOT NULL;""",
    },
    {
        "report_name": "VW_PCP_COMPLIANCE",
        "description": "Tracks PCP appointment compliance, identifying patients with active PCP assignments who have missed (no-show) visits in the past 6 months.",
        "primary_purpose": "PCP appointment no-show tracking",
        "key_metrics": ["noshow_count", "noshow_rate", "patients_with_pcp"],
        "developer": "A. Patel",
        "business_requester": "Quality Improvement",
        "created_date": "2024-01-10",
        "last_modified": "2024-08-05",
        "parameters": [
            {"name": "lookback_months", "default": 6, "type": "integer",
             "description": "Months to look back for no-show events"},
        ],
        "tables_used": ["PAT_ENC", "PAT_PCP", "CLARITY_DEP", "PATIENT"],
        "domains": ["encounters", "demographics"],
        "column_count": 7,
        "sql_complexity": "medium",
        "inline_comments": [
            "-- Active PCP = TERM_DATE IS NULL",
            "-- No-show = APPT_STATUS_C = 4",
            "-- PCP departments = Family Medicine specialty",
        ],
        "sql": """\
CREATE VIEW VW_PCP_COMPLIANCE AS
WITH pcp_assignments AS (
    -- Active PCP = TERM_DATE IS NULL
    SELECT PAT_ID, PCP_PROV_ID, EFF_DATE
    FROM PAT_PCP
    WHERE TERM_DATE IS NULL
),
missed_visits AS (
    -- No-show = APPT_STATUS_C = 4, PCP departments = Family Medicine specialty
    SELECT DISTINCT enc.PAT_ID, enc.PAT_ENC_CSN_ID, enc.CONTACT_DATE
    FROM PAT_ENC enc
    JOIN CLARITY_DEP dep ON dep.DEPARTMENT_ID = enc.DEPARTMENT_ID
    WHERE dep.SPECIALTY = 'Family Medicine'
      AND enc.APPT_STATUS_C = 4
      AND enc.CONTACT_DATE >= date('now', '-6 months')
)
SELECT
    p.PAT_ID,
    p.PAT_NAME,
    pcp.PCP_PROV_ID,
    pcp.EFF_DATE AS PCP_SINCE,
    mv.CONTACT_DATE AS NOSHOW_DATE,
    mv.PAT_ENC_CSN_ID AS NOSHOW_CSN,
    1 AS HAS_NOSHOW
FROM PATIENT p
JOIN pcp_assignments pcp ON pcp.PAT_ID = p.PAT_ID
JOIN missed_visits mv ON mv.PAT_ID = p.PAT_ID;""",
    },
    {
        "report_name": "VW_READMISSION_30DAY",
        "description": "Identifies 30-day hospital readmissions by comparing discharge dates to subsequent admission dates for the same patient.",
        "primary_purpose": "30-day readmission tracking",
        "key_metrics": ["readmission_count", "readmission_rate"],
        "developer": "J. Smith",
        "business_requester": "Quality Department",
        "created_date": "2023-04-20",
        "last_modified": "2024-06-10",
        "parameters": [
            {"name": "readmission_window_days", "default": 30, "type": "integer",
             "description": "Days after discharge within which a new admission counts as readmission"},
        ],
        "tables_used": ["PAT_ENC_HSP", "HSP_ACCOUNT", "PATIENT"],
        "domains": ["encounters", "billing"],
        "column_count": 8,
        "sql_complexity": "complex",
        "inline_comments": [
            "-- Only discharged patients (HOSP_DISCH_TIME IS NOT NULL)",
            "-- Readmission = new admission within 30 days of discharge",
        ],
        "sql": """\
CREATE VIEW VW_READMISSION_30DAY AS
WITH admissions AS (
    -- Only discharged patients (HOSP_DISCH_TIME IS NOT NULL)
    SELECT h.PAT_ENC_CSN_ID, h.PAT_ID,
           h.HOSP_ADMSN_TIME, h.HOSP_DISCH_TIME,
           a.TOT_CHGS
    FROM PAT_ENC_HSP h
    JOIN HSP_ACCOUNT a ON a.PRIM_ENC_CSN_ID = h.PAT_ENC_CSN_ID
    WHERE h.HOSP_DISCH_TIME IS NOT NULL
)
-- Readmission = new admission within 30 days of discharge
SELECT
    a1.PAT_ID,
    p.PAT_NAME,
    a1.PAT_ENC_CSN_ID AS INDEX_CSN,
    a1.HOSP_ADMSN_TIME AS INDEX_ADMIT,
    a1.HOSP_DISCH_TIME AS INDEX_DISCHARGE,
    a2.PAT_ENC_CSN_ID AS READMIT_CSN,
    a2.HOSP_ADMSN_TIME AS READMIT_DATE,
    CAST(julianday(a2.HOSP_ADMSN_TIME) - julianday(a1.HOSP_DISCH_TIME) AS INTEGER) AS DAYS_TO_READMIT
FROM admissions a1
JOIN admissions a2 ON a2.PAT_ID = a1.PAT_ID
    AND a2.HOSP_ADMSN_TIME > a1.HOSP_DISCH_TIME
    AND julianday(a2.HOSP_ADMSN_TIME) - julianday(a1.HOSP_DISCH_TIME) <= 30
JOIN PATIENT p ON p.PAT_ID = a1.PAT_ID;""",
    },
    {
        "report_name": "VW_LOS_REPORT",
        "description": "Calculates length of stay in days for hospital encounters, using hospital admission and discharge times from PAT_ENC_HSP.",
        "primary_purpose": "Length of stay analysis",
        "key_metrics": ["avg_los", "median_los", "total_patient_days"],
        "developer": "M. Chen",
        "business_requester": "Operations",
        "created_date": "2023-03-01",
        "last_modified": "2024-09-15",
        "parameters": [],
        "tables_used": ["PAT_ENC_HSP", "HSP_ACCOUNT", "PATIENT"],
        "domains": ["encounters", "billing"],
        "column_count": 7,
        "sql_complexity": "simple",
        "inline_comments": [
            "-- LOS = discharge - admission in days",
            "-- Excludes encounters without discharge (still admitted)",
        ],
        "sql": """\
CREATE VIEW VW_LOS_REPORT AS
WITH stays AS (
    -- LOS = discharge - admission in days
    -- Excludes encounters without discharge (still admitted)
    SELECT h.PAT_ENC_CSN_ID, h.PAT_ID,
           h.HOSP_ADMSN_TIME, h.HOSP_DISCH_TIME,
           CAST(julianday(h.HOSP_DISCH_TIME) - julianday(h.HOSP_ADMSN_TIME) AS INTEGER) AS LOS_DAYS,
           a.TOT_CHGS
    FROM PAT_ENC_HSP h
    JOIN HSP_ACCOUNT a ON a.PRIM_ENC_CSN_ID = h.PAT_ENC_CSN_ID
    WHERE h.HOSP_DISCH_TIME IS NOT NULL
)
SELECT
    s.PAT_ID,
    p.PAT_NAME,
    s.PAT_ENC_CSN_ID,
    s.HOSP_ADMSN_TIME,
    s.HOSP_DISCH_TIME,
    s.LOS_DAYS,
    s.TOT_CHGS
FROM stays s
JOIN PATIENT p ON p.PAT_ID = s.PAT_ID;""",
    },
    {
        "report_name": "VW_MEDICATION_DIABETIC",
        "description": "Lists medication orders for diabetic patients, linking diabetes cohort identification with prescribed medications for formulary analysis.",
        "primary_purpose": "Diabetes medication prescribing patterns",
        "key_metrics": ["rx_count", "unique_medications", "patients_on_insulin"],
        "developer": "A. Patel",
        "business_requester": "Pharmacy",
        "created_date": "2024-02-01",
        "last_modified": "2024-12-01",
        "parameters": [
            {"name": "icd10_pattern", "default": "E11%", "type": "string",
             "description": "ICD-10 pattern for diabetes cohort"},
        ],
        "tables_used": ["PROBLEM_LIST", "CLARITY_EDG", "ORDER_MED", "CLARITY_MEDICATION", "PATIENT"],
        "domains": ["diagnosis", "medications"],
        "column_count": 7,
        "sql_complexity": "medium",
        "inline_comments": [
            "-- Same diabetes cohort definition as VW_DIABETIC_COHORT",
            "-- Links to medication orders for prescribing analysis",
        ],
        "sql": """\
CREATE VIEW VW_MEDICATION_DIABETIC AS
WITH diabetic_pats AS (
    -- Same diabetes cohort definition as VW_DIABETIC_COHORT
    SELECT DISTINCT pl.PAT_ID
    FROM PROBLEM_LIST pl
    JOIN CLARITY_EDG edg ON edg.DX_ID = pl.DX_ID
    WHERE edg.CURRENT_ICD10_LIST LIKE 'E11%'
      AND pl.RESOLVED_DATE IS NULL
)
-- Links to medication orders for prescribing analysis
SELECT
    p.PAT_ID,
    p.PAT_NAME,
    om.ORDER_MED_ID,
    m.NAME AS MEDICATION_NAME,
    m.GENERIC_NAME,
    m.PHARM_CLASS_C,
    om.ORDER_STATUS_C
FROM diabetic_pats dp
JOIN PATIENT p ON p.PAT_ID = dp.PAT_ID
JOIN ORDER_MED om ON om.PAT_ID = dp.PAT_ID
JOIN CLARITY_MEDICATION m ON m.MEDICATION_ID = om.MEDICATION_ID;""",
    },
    {
        "report_name": "VW_BILLING_SUMMARY",
        "description": "Summarizes professional billing charges by department, aggregating transaction amounts for financial analysis and departmental budgeting.",
        "primary_purpose": "Departmental billing summary",
        "key_metrics": ["total_charges", "avg_charge", "transaction_count"],
        "developer": "R. Kim",
        "business_requester": "Finance",
        "created_date": "2023-07-15",
        "last_modified": "2024-10-01",
        "parameters": [
            {"name": "tx_type", "default": 1, "type": "integer",
             "description": "Transaction type (1=charges)"},
        ],
        "tables_used": ["ARPB_TRANSACTIONS", "CLARITY_DEP"],
        "domains": ["billing"],
        "column_count": 5,
        "sql_complexity": "simple",
        "inline_comments": [
            "-- TX_TYPE_C = 1 filters to charge transactions only",
        ],
        "sql": """\
CREATE VIEW VW_BILLING_SUMMARY AS
WITH charges AS (
    -- TX_TYPE_C = 1 filters to charge transactions only
    SELECT t.TX_ID, t.PATIENT_ID, t.SERVICE_DATE, t.AMOUNT,
           t.DEPARTMENT_ID
    FROM ARPB_TRANSACTIONS t
    WHERE t.TX_TYPE_C = 1
)
SELECT
    dep.DEPARTMENT_NAME,
    dep.SPECIALTY,
    COUNT(*) AS TRANSACTION_COUNT,
    SUM(c.AMOUNT) AS TOTAL_CHARGES,
    AVG(c.AMOUNT) AS AVG_CHARGE
FROM charges c
JOIN CLARITY_DEP dep ON dep.DEPARTMENT_ID = c.DEPARTMENT_ID
GROUP BY dep.DEPARTMENT_NAME, dep.SPECIALTY;""",
    },
    {
        "report_name": "VW_REFERRAL_TRACKING",
        "description": "Tracks active referrals (non-denied, non-canceled) with referring and receiving provider details for referral management workflows.",
        "primary_purpose": "Referral status tracking",
        "key_metrics": ["open_referrals", "referral_completion_rate"],
        "developer": "J. Smith",
        "business_requester": "Referral Management",
        "created_date": "2024-05-01",
        "last_modified": "2025-01-15",
        "parameters": [
            {"name": "exclude_statuses", "default": [4, 5], "type": "list",
             "description": "Referral status codes to exclude (4=Canceled, 5=Denied)"},
        ],
        "tables_used": ["REFERRAL", "CLARITY_SER", "PATIENT"],
        "domains": ["referrals"],
        "column_count": 7,
        "sql_complexity": "simple",
        "inline_comments": [
            "-- Excludes canceled (4) and denied (5) referrals",
        ],
        "sql": """\
CREATE VIEW VW_REFERRAL_TRACKING AS
WITH active_referrals AS (
    -- Excludes canceled (4) and denied (5) referrals
    SELECT r.REFERRAL_ID, r.PAT_ID, r.ENTRY_DATE,
           r.RFL_STATUS_C, r.PCP_PROV_ID, r.REFERRING_PROV_ID
    FROM REFERRAL r
    WHERE r.RFL_STATUS_C NOT IN (4, 5)
)
SELECT
    ar.REFERRAL_ID,
    p.PAT_ID,
    p.PAT_NAME,
    ar.ENTRY_DATE,
    ar.RFL_STATUS_C,
    pcp.PROV_NAME AS PCP_NAME,
    ref.PROV_NAME AS REFERRING_PROVIDER
FROM active_referrals ar
JOIN PATIENT p ON p.PAT_ID = ar.PAT_ID
LEFT JOIN CLARITY_SER pcp ON pcp.PROV_ID = ar.PCP_PROV_ID
LEFT JOIN CLARITY_SER ref ON ref.PROV_ID = ar.REFERRING_PROV_ID;""",
    },
]


def generate_report_glossary(output_dir: str | Path = "tools/jit/data"):
    """Write report glossary YAML files and SQL view files."""
    output_dir = Path(output_dir)
    glossary_dir = output_dir / "report_glossary"
    sql_dir = output_dir / "report_sql"
    glossary_dir.mkdir(parents=True, exist_ok=True)
    sql_dir.mkdir(parents=True, exist_ok=True)

    for report in REPORTS:
        # Write glossary YAML (without SQL)
        glossary_entry = {k: v for k, v in report.items() if k != "sql"}
        glossary_entry["source_sql_path"] = f"report_sql/{report['report_name']}.sql"

        yaml_path = glossary_dir / f"{report['report_name']}.yaml"
        with open(yaml_path, "w") as f:
            yaml.dump(glossary_entry, f, default_flow_style=False,
                      sort_keys=False, allow_unicode=True)

        # Write SQL file
        sql_path = sql_dir / f"{report['report_name']}.sql"
        with open(sql_path, "w") as f:
            f.write(report["sql"])
            f.write("\n")

    print(f"Report Glossary: {len(REPORTS)} entries written to {glossary_dir}")
    print(f"Report SQL: {len(REPORTS)} files written to {sql_dir}")
    return glossary_dir


def load_report_glossary(glossary_dir: str | Path = "tools/jit/data/report_glossary") -> list[dict]:
    """Load all report glossary entries from YAML files."""
    glossary_dir = Path(glossary_dir)
    reports = []
    for yaml_path in sorted(glossary_dir.glob("*.yaml")):
        with open(yaml_path) as f:
            reports.append(yaml.safe_load(f))
    return reports


if __name__ == "__main__":
    import sys
    output = sys.argv[1] if len(sys.argv) > 1 else "tools/jit/data"
    generate_report_glossary(output)
