"""Generate the Business Definition Glossary and pre-populate learned_terms.

Run directly:
    python3 -m tools.jit.mock.mock_definitions [output_dir]

Outputs:
    {output_dir}/definition_glossary/    — one YAML per definition
    {output_dir}/learned_terms.yaml      — pre-populated from definitions
"""

from __future__ import annotations

from pathlib import Path

import yaml


DEFINITIONS = [
    {
        "definition_name": "diabetic_patients_problem_list",
        "label": "Diabetic patients (active problem list)",
        "description": "Patients with an active diabetes diagnosis on their problem list, identified by ICD-10 E11.% codes. Uses PROBLEM_LIST for chronic condition tracking.",
        "domain": "diagnosis",
        "backbone": {
            "anchor_table": "PATIENT",
            "tables": ["PATIENT", "PROBLEM_LIST", "CLARITY_EDG"],
            "joins": [
                {"from": "PATIENT", "to": "PROBLEM_LIST",
                 "on": "PROBLEM_LIST.PAT_ID = PATIENT.PAT_ID",
                 "type": "INNER", "grain_impact": True},
                {"from": "PROBLEM_LIST", "to": "CLARITY_EDG",
                 "on": "CLARITY_EDG.DX_ID = PROBLEM_LIST.DX_ID",
                 "type": "INNER", "grain_impact": False},
            ],
            "characteristic_filters": [
                {"expression": "CLARITY_EDG.CURRENT_ICD10_LIST LIKE 'E11%'",
                 "english": "ICD-10 diabetes codes (Type 2)",
                 "is_definitional": True},
                {"expression": "PROBLEM_LIST.RESOLVED_DATE IS NULL",
                 "english": "Active (unresolved) problems only",
                 "is_definitional": True},
            ],
            "output_grain": "patient",
        },
        "parameters": [
            {"name": "icd10_pattern", "default": "E11%", "type": "string",
             "description": "ICD-10 pattern to match"},
            {"name": "active_only", "default": True, "type": "boolean",
             "description": "Filter to active problems (RESOLVED_DATE IS NULL)"},
        ],
        "source_reports": ["VW_DIABETIC_COHORT", "VW_MEDICATION_DIABETIC"],
        "source_scopes": [
            "VW_DIABETIC_COHORT::cte:diabetic_patients",
            "VW_MEDICATION_DIABETIC::cte:diabetic_pats",
        ],
        "equivalent_definitions": [],
        "sql_template": """\
SELECT DISTINCT p.PAT_ID
FROM PATIENT p
JOIN PROBLEM_LIST pl ON pl.PAT_ID = p.PAT_ID
JOIN CLARITY_EDG edg ON edg.DX_ID = pl.DX_ID
WHERE edg.CURRENT_ICD10_LIST LIKE '{icd10_pattern}'
  AND pl.RESOLVED_DATE IS NULL""",
    },
    {
        "definition_name": "active_problems_only",
        "label": "Active problems filter",
        "description": "Filters the problem list to only active (unresolved) conditions. A building block used inside cohort definitions.",
        "domain": "diagnosis",
        "backbone": {
            "anchor_table": "PROBLEM_LIST",
            "tables": ["PROBLEM_LIST"],
            "joins": [],
            "characteristic_filters": [
                {"expression": "PROBLEM_LIST.RESOLVED_DATE IS NULL",
                 "english": "Active (unresolved) problems only",
                 "is_definitional": True},
            ],
            "output_grain": "problem",
        },
        "parameters": [],
        "source_reports": ["VW_DIABETIC_COHORT"],
        "source_scopes": ["VW_DIABETIC_COHORT::cte:active_only"],
        "equivalent_definitions": [],
        "sql_template": """\
SELECT * FROM PROBLEM_LIST
WHERE RESOLVED_DATE IS NULL""",
    },
    {
        "definition_name": "ed_encounters",
        "label": "ED encounters",
        "description": "All emergency department encounters, identified by department name containing 'Emergency'. Completed visits only.",
        "domain": "encounters",
        "backbone": {
            "anchor_table": "PAT_ENC",
            "tables": ["PAT_ENC", "CLARITY_DEP"],
            "joins": [
                {"from": "PAT_ENC", "to": "CLARITY_DEP",
                 "on": "CLARITY_DEP.DEPARTMENT_ID = PAT_ENC.DEPARTMENT_ID",
                 "type": "INNER", "grain_impact": False},
            ],
            "characteristic_filters": [
                {"expression": "CLARITY_DEP.DEPARTMENT_NAME LIKE '%Emergency%'",
                 "english": "Emergency department visits",
                 "is_definitional": True},
                {"expression": "PAT_ENC.APPT_STATUS_C = 2",
                 "english": "Completed visits only",
                 "is_definitional": True},
            ],
            "output_grain": "encounter",
        },
        "parameters": [
            {"name": "date_from", "default": None, "type": "date",
             "description": "Start date for encounters"},
            {"name": "date_to", "default": None, "type": "date",
             "description": "End date for encounters"},
        ],
        "source_reports": ["VW_ED_UTILIZATION"],
        "source_scopes": ["VW_ED_UTILIZATION::cte:ed_encounters"],
        "equivalent_definitions": [],
        "sql_template": """\
SELECT enc.PAT_ENC_CSN_ID, enc.PAT_ID, enc.CONTACT_DATE
FROM PAT_ENC enc
JOIN CLARITY_DEP dep ON dep.DEPARTMENT_ID = enc.DEPARTMENT_ID
WHERE dep.DEPARTMENT_NAME LIKE '%Emergency%'
  AND enc.APPT_STATUS_C = 2""",
    },
    {
        "definition_name": "ed_high_utilizers",
        "label": "ED high utilizers (>3 visits in 12 months)",
        "description": "Patients with more than 3 emergency department visits in a rolling 12-month window. Used to identify patients for care coordination intervention.",
        "domain": "encounters",
        "backbone": {
            "anchor_table": "PAT_ENC",
            "tables": ["PAT_ENC", "CLARITY_DEP"],
            "joins": [
                {"from": "PAT_ENC", "to": "CLARITY_DEP",
                 "on": "CLARITY_DEP.DEPARTMENT_ID = PAT_ENC.DEPARTMENT_ID",
                 "type": "INNER", "grain_impact": False},
            ],
            "characteristic_filters": [
                {"expression": "CLARITY_DEP.DEPARTMENT_NAME LIKE '%Emergency%'",
                 "english": "Emergency department visits",
                 "is_definitional": True},
                {"expression": "COUNT(*) > 3",
                 "english": "More than 3 visits",
                 "is_definitional": True},
            ],
            "output_grain": "patient",
            "group_by": "PAT_ID",
            "having": "COUNT(*) > 3",
        },
        "parameters": [
            {"name": "threshold", "default": 3, "type": "integer",
             "description": "Visit count threshold for high utilizer"},
            {"name": "lookback_months", "default": 12, "type": "integer",
             "description": "Months to look back"},
        ],
        "source_reports": ["VW_ED_UTILIZATION"],
        "source_scopes": ["VW_ED_UTILIZATION::cte:high_utilizers"],
        "equivalent_definitions": [],
        "sql_template": """\
SELECT enc.PAT_ID, COUNT(*) AS visit_count
FROM PAT_ENC enc
JOIN CLARITY_DEP dep ON dep.DEPARTMENT_ID = enc.DEPARTMENT_ID
WHERE dep.DEPARTMENT_NAME LIKE '%Emergency%'
  AND enc.CONTACT_DATE >= date('now', '-{lookback_months} months')
GROUP BY enc.PAT_ID
HAVING COUNT(*) > {threshold}""",
    },
    {
        "definition_name": "pcp_assignments_active",
        "label": "Active PCP assignments",
        "description": "Patients with an active (non-terminated) primary care provider assignment.",
        "domain": "encounters",
        "backbone": {
            "anchor_table": "PAT_PCP",
            "tables": ["PAT_PCP"],
            "joins": [],
            "characteristic_filters": [
                {"expression": "PAT_PCP.TERM_DATE IS NULL",
                 "english": "Active PCP assignment (not terminated)",
                 "is_definitional": True},
            ],
            "output_grain": "patient_pcp",
        },
        "parameters": [],
        "source_reports": ["VW_PCP_COMPLIANCE"],
        "source_scopes": ["VW_PCP_COMPLIANCE::cte:pcp_assignments"],
        "equivalent_definitions": [],
        "sql_template": """\
SELECT PAT_ID, PCP_PROV_ID, EFF_DATE
FROM PAT_PCP
WHERE TERM_DATE IS NULL""",
    },
    {
        "definition_name": "missed_pcp_visits",
        "label": "Missed PCP visits (no-show)",
        "description": "Patients who had a no-show appointment at a Family Medicine department in the specified lookback period.",
        "domain": "encounters",
        "backbone": {
            "anchor_table": "PAT_ENC",
            "tables": ["PAT_ENC", "CLARITY_DEP"],
            "joins": [
                {"from": "PAT_ENC", "to": "CLARITY_DEP",
                 "on": "CLARITY_DEP.DEPARTMENT_ID = PAT_ENC.DEPARTMENT_ID",
                 "type": "INNER", "grain_impact": False},
            ],
            "characteristic_filters": [
                {"expression": "CLARITY_DEP.SPECIALTY = 'Family Medicine'",
                 "english": "PCP / Family Medicine departments",
                 "is_definitional": True},
                {"expression": "PAT_ENC.APPT_STATUS_C = 4",
                 "english": "No-show appointment status",
                 "is_definitional": True},
            ],
            "output_grain": "patient",
        },
        "parameters": [
            {"name": "lookback_months", "default": 6, "type": "integer",
             "description": "Months to look back for no-show events"},
        ],
        "source_reports": ["VW_PCP_COMPLIANCE"],
        "source_scopes": ["VW_PCP_COMPLIANCE::cte:missed_visits"],
        "equivalent_definitions": [],
        "sql_template": """\
SELECT DISTINCT enc.PAT_ID
FROM PAT_ENC enc
JOIN CLARITY_DEP dep ON dep.DEPARTMENT_ID = enc.DEPARTMENT_ID
WHERE dep.SPECIALTY = 'Family Medicine'
  AND enc.APPT_STATUS_C = 4
  AND enc.CONTACT_DATE >= date('now', '-{lookback_months} months')""",
    },
    {
        "definition_name": "hospital_admissions",
        "label": "Hospital admissions (discharged)",
        "description": "Completed hospital admissions with both admission and discharge times. Shared foundation for LOS and readmission analyses.",
        "domain": "encounters",
        "backbone": {
            "anchor_table": "PAT_ENC_HSP",
            "tables": ["PAT_ENC_HSP", "HSP_ACCOUNT"],
            "joins": [
                {"from": "PAT_ENC_HSP", "to": "HSP_ACCOUNT",
                 "on": "HSP_ACCOUNT.PRIM_ENC_CSN_ID = PAT_ENC_HSP.PAT_ENC_CSN_ID",
                 "type": "INNER", "grain_impact": False},
            ],
            "characteristic_filters": [
                {"expression": "PAT_ENC_HSP.HOSP_DISCH_TIME IS NOT NULL",
                 "english": "Discharged patients only (excludes currently admitted)",
                 "is_definitional": True},
            ],
            "output_grain": "encounter",
        },
        "parameters": [],
        "source_reports": ["VW_READMISSION_30DAY", "VW_LOS_REPORT"],
        "source_scopes": [
            "VW_READMISSION_30DAY::cte:admissions",
            "VW_LOS_REPORT::cte:stays",
        ],
        "equivalent_definitions": [],
        "sql_template": """\
SELECT h.PAT_ENC_CSN_ID, h.PAT_ID,
       h.HOSP_ADMSN_TIME, h.HOSP_DISCH_TIME,
       a.TOT_CHGS
FROM PAT_ENC_HSP h
JOIN HSP_ACCOUNT a ON a.PRIM_ENC_CSN_ID = h.PAT_ENC_CSN_ID
WHERE h.HOSP_DISCH_TIME IS NOT NULL""",
    },
    {
        "definition_name": "readmission_30day",
        "label": "30-day readmission",
        "description": "A hospital admission that occurs within 30 days of a prior discharge for the same patient. Self-join pattern on PAT_ENC_HSP.",
        "domain": "encounters",
        "backbone": {
            "anchor_table": "PAT_ENC_HSP",
            "tables": ["PAT_ENC_HSP", "HSP_ACCOUNT"],
            "joins": [
                {"from": "PAT_ENC_HSP", "to": "HSP_ACCOUNT",
                 "on": "HSP_ACCOUNT.PRIM_ENC_CSN_ID = PAT_ENC_HSP.PAT_ENC_CSN_ID",
                 "type": "INNER", "grain_impact": False},
                {"from": "PAT_ENC_HSP a1", "to": "PAT_ENC_HSP a2",
                 "on": "a2.PAT_ID = a1.PAT_ID AND a2.HOSP_ADMSN_TIME > a1.HOSP_DISCH_TIME AND julianday(a2.HOSP_ADMSN_TIME) - julianday(a1.HOSP_DISCH_TIME) <= 30",
                 "type": "INNER", "grain_impact": True},
            ],
            "characteristic_filters": [
                {"expression": "julianday(a2.HOSP_ADMSN_TIME) - julianday(a1.HOSP_DISCH_TIME) <= 30",
                 "english": "Readmission within 30 days of discharge",
                 "is_definitional": True},
            ],
            "output_grain": "readmission_pair",
        },
        "parameters": [
            {"name": "readmission_window_days", "default": 30, "type": "integer",
             "description": "Days after discharge for readmission window"},
        ],
        "source_reports": ["VW_READMISSION_30DAY"],
        "source_scopes": ["VW_READMISSION_30DAY::main"],
        "equivalent_definitions": [],
        "sql_template": """\
SELECT a1.PAT_ID,
       a1.PAT_ENC_CSN_ID AS index_csn,
       a1.HOSP_DISCH_TIME AS index_discharge,
       a2.PAT_ENC_CSN_ID AS readmit_csn,
       a2.HOSP_ADMSN_TIME AS readmit_date
FROM PAT_ENC_HSP a1
JOIN HSP_ACCOUNT ha1 ON ha1.PRIM_ENC_CSN_ID = a1.PAT_ENC_CSN_ID
JOIN PAT_ENC_HSP a2 ON a2.PAT_ID = a1.PAT_ID
    AND a2.HOSP_ADMSN_TIME > a1.HOSP_DISCH_TIME
    AND julianday(a2.HOSP_ADMSN_TIME) - julianday(a1.HOSP_DISCH_TIME) <= {readmission_window_days}
WHERE a1.HOSP_DISCH_TIME IS NOT NULL""",
    },
    {
        "definition_name": "length_of_stay",
        "label": "Length of stay (days)",
        "description": "Days between hospital admission and discharge. Calculated as DATEDIFF of HOSP_ADMSN_TIME and HOSP_DISCH_TIME.",
        "domain": "encounters",
        "backbone": {
            "anchor_table": "PAT_ENC_HSP",
            "tables": ["PAT_ENC_HSP", "HSP_ACCOUNT"],
            "joins": [
                {"from": "PAT_ENC_HSP", "to": "HSP_ACCOUNT",
                 "on": "HSP_ACCOUNT.PRIM_ENC_CSN_ID = PAT_ENC_HSP.PAT_ENC_CSN_ID",
                 "type": "INNER", "grain_impact": False},
            ],
            "characteristic_filters": [
                {"expression": "PAT_ENC_HSP.HOSP_DISCH_TIME IS NOT NULL",
                 "english": "Discharged patients only",
                 "is_definitional": True},
            ],
            "output_grain": "encounter",
            "computed_columns": [
                {"name": "LOS_DAYS",
                 "expression": "CAST(julianday(HOSP_DISCH_TIME) - julianday(HOSP_ADMSN_TIME) AS INTEGER)",
                 "english": "Length of stay in days"},
            ],
        },
        "parameters": [],
        "source_reports": ["VW_LOS_REPORT"],
        "source_scopes": ["VW_LOS_REPORT::cte:stays"],
        "equivalent_definitions": [],
        "sql_template": """\
SELECT h.PAT_ENC_CSN_ID, h.PAT_ID,
       h.HOSP_ADMSN_TIME, h.HOSP_DISCH_TIME,
       CAST(julianday(h.HOSP_DISCH_TIME) - julianday(h.HOSP_ADMSN_TIME) AS INTEGER) AS LOS_DAYS
FROM PAT_ENC_HSP h
JOIN HSP_ACCOUNT a ON a.PRIM_ENC_CSN_ID = h.PAT_ENC_CSN_ID
WHERE h.HOSP_DISCH_TIME IS NOT NULL""",
    },
    {
        "definition_name": "diabetic_medications",
        "label": "Medication orders for diabetic patients",
        "description": "All medication orders for patients in the diabetic cohort. Combines the diabetes population definition with ORDER_MED.",
        "domain": "medications",
        "backbone": {
            "anchor_table": "ORDER_MED",
            "tables": ["ORDER_MED", "CLARITY_MEDICATION"],
            "joins": [
                {"from": "ORDER_MED", "to": "CLARITY_MEDICATION",
                 "on": "CLARITY_MEDICATION.MEDICATION_ID = ORDER_MED.MEDICATION_ID",
                 "type": "INNER", "grain_impact": False},
            ],
            "characteristic_filters": [],
            "output_grain": "medication_order",
            "depends_on": ["diabetic_patients_problem_list"],
        },
        "parameters": [],
        "source_reports": ["VW_MEDICATION_DIABETIC"],
        "source_scopes": ["VW_MEDICATION_DIABETIC::main"],
        "equivalent_definitions": [],
        "sql_template": """\
SELECT om.ORDER_MED_ID, om.PAT_ID,
       m.NAME AS MEDICATION_NAME, m.GENERIC_NAME
FROM ORDER_MED om
JOIN CLARITY_MEDICATION m ON m.MEDICATION_ID = om.MEDICATION_ID""",
    },
    {
        "definition_name": "billing_charges",
        "label": "Billing charge transactions",
        "description": "Professional billing charge transactions (TX_TYPE_C = 1). The foundation for revenue and utilization financial analysis.",
        "domain": "billing",
        "backbone": {
            "anchor_table": "ARPB_TRANSACTIONS",
            "tables": ["ARPB_TRANSACTIONS"],
            "joins": [],
            "characteristic_filters": [
                {"expression": "ARPB_TRANSACTIONS.TX_TYPE_C = 1",
                 "english": "Charge transactions only (excludes payments/adjustments)",
                 "is_definitional": True},
            ],
            "output_grain": "transaction",
        },
        "parameters": [
            {"name": "date_from", "default": None, "type": "date",
             "description": "Service date range start"},
            {"name": "date_to", "default": None, "type": "date",
             "description": "Service date range end"},
        ],
        "source_reports": ["VW_BILLING_SUMMARY"],
        "source_scopes": ["VW_BILLING_SUMMARY::cte:charges"],
        "equivalent_definitions": [],
        "sql_template": """\
SELECT TX_ID, PATIENT_ID, SERVICE_DATE, AMOUNT, DEPARTMENT_ID
FROM ARPB_TRANSACTIONS
WHERE TX_TYPE_C = 1""",
    },
    {
        "definition_name": "active_referrals",
        "label": "Active referrals (non-canceled, non-denied)",
        "description": "Referrals that are not canceled or denied. Includes authorized, open, pending, and closed statuses.",
        "domain": "referrals",
        "backbone": {
            "anchor_table": "REFERRAL",
            "tables": ["REFERRAL"],
            "joins": [],
            "characteristic_filters": [
                {"expression": "REFERRAL.RFL_STATUS_C NOT IN (4, 5)",
                 "english": "Excludes canceled (4) and denied (5) referrals",
                 "is_definitional": True},
            ],
            "output_grain": "referral",
        },
        "parameters": [],
        "source_reports": ["VW_REFERRAL_TRACKING"],
        "source_scopes": ["VW_REFERRAL_TRACKING::cte:active_referrals"],
        "equivalent_definitions": [],
        "sql_template": """\
SELECT REFERRAL_ID, PAT_ID, ENTRY_DATE, RFL_STATUS_C,
       PCP_PROV_ID, REFERRING_PROV_ID
FROM REFERRAL
WHERE RFL_STATUS_C NOT IN (4, 5)""",
    },
]


def _build_learned_terms(definitions: list[dict]) -> dict:
    """Extract learned terms from definitions for pre-population.

    Scans definition labels, descriptions, and filter English translations
    for domain-specific terms and maps them to their category + tables.
    """
    terms = {}

    # Direct mappings from known definitions
    term_mappings = [
        {
            "term": "diabetes",
            "aliases": ["diabetic", "diabetic patients", "diabetes mellitus", "type 2 diabetes"],
            "category": "diagnosis",
            "tables": ["PROBLEM_LIST", "CLARITY_EDG"],
            "filters": {"CLARITY_EDG": "CURRENT_ICD10_LIST LIKE 'E11%'"},
            "route": "PATIENT -> PROBLEM_LIST -> CLARITY_EDG",
            "source_definition": "diabetic_patients_problem_list",
            "icd10": "E11.%",
        },
        {
            "term": "emergency",
            "aliases": ["ER", "ED", "emergency department", "emergency room", "emergency visits"],
            "category": "encounter",
            "tables": ["PAT_ENC", "CLARITY_DEP"],
            "filters": {"CLARITY_DEP": "DEPARTMENT_NAME LIKE '%Emergency%'"},
            "route": "PATIENT -> PAT_ENC -> CLARITY_DEP",
            "source_definition": "ed_encounters",
        },
        {
            "term": "high utilizer",
            "aliases": ["frequent flyer", "frequent visitor", "high utilization", "frequent ER"],
            "category": "encounter",
            "tables": ["PAT_ENC", "CLARITY_DEP"],
            "filters": {"CLARITY_DEP": "DEPARTMENT_NAME LIKE '%Emergency%'"},
            "route": "PATIENT -> PAT_ENC -> CLARITY_DEP",
            "source_definition": "ed_high_utilizers",
        },
        {
            "term": "no-show",
            "aliases": ["missed visit", "missed appointment", "no show", "noshow", "missed PCP"],
            "category": "encounter",
            "tables": ["PAT_ENC"],
            "filters": {"PAT_ENC": "APPT_STATUS_C = 4"},
            "route": "PATIENT -> PAT_ENC",
            "source_definition": "missed_pcp_visits",
        },
        {
            "term": "readmission",
            "aliases": ["readmit", "readmitted", "30-day readmission", "hospital readmission"],
            "category": "encounter",
            "tables": ["PAT_ENC_HSP"],
            "filters": {},
            "route": "PATIENT -> PAT_ENC -> PAT_ENC_HSP",
            "source_definition": "readmission_30day",
        },
        {
            "term": "length of stay",
            "aliases": ["LOS", "hospital days", "stay duration", "inpatient days"],
            "category": "encounter",
            "tables": ["PAT_ENC_HSP"],
            "filters": {},
            "route": "PATIENT -> PAT_ENC -> PAT_ENC_HSP",
            "source_definition": "length_of_stay",
        },
        {
            "term": "referral",
            "aliases": ["referred", "referral order", "specialty referral"],
            "category": "referral",
            "tables": ["REFERRAL"],
            "filters": {},
            "route": "PATIENT -> REFERRAL",
            "source_definition": "active_referrals",
        },
        {
            "term": "hypertension",
            "aliases": ["high blood pressure", "HTN", "hypertensive"],
            "category": "diagnosis",
            "tables": ["PROBLEM_LIST", "CLARITY_EDG"],
            "filters": {"CLARITY_EDG": "CURRENT_ICD10_LIST LIKE 'I1%'"},
            "route": "PATIENT -> PROBLEM_LIST -> CLARITY_EDG",
            "icd10": "I10-I15",
        },
        {
            "term": "PCP",
            "aliases": ["primary care", "primary care provider", "family medicine", "family doctor"],
            "category": "encounter",
            "tables": ["PAT_PCP", "CLARITY_DEP"],
            "filters": {"CLARITY_DEP": "SPECIALTY = 'Family Medicine'"},
            "route": "PATIENT -> PAT_PCP",
            "source_definition": "pcp_assignments_active",
        },
    ]

    for mapping in term_mappings:
        key = mapping["term"].lower().replace(" ", "_").replace("-", "_")
        entry = {
            "term": mapping["term"],
            "aliases": mapping["aliases"],
            "category": mapping["category"],
            "tables": mapping["tables"],
            "route": mapping["route"],
            "confirmed_by": "corpus_build",
            "confirmed_date": "2026-06-11",
        }
        if "filters" in mapping and mapping["filters"]:
            entry["filters"] = mapping["filters"]
        if "source_definition" in mapping:
            entry["source_definition"] = mapping["source_definition"]
        if "icd10" in mapping:
            entry["icd10"] = mapping["icd10"]
        terms[key] = entry

    return terms


def generate_definition_glossary(output_dir: str | Path = "tools/jit/data"):
    """Write definition glossary YAML files and learned_terms.yaml."""
    output_dir = Path(output_dir)
    glossary_dir = output_dir / "definition_glossary"
    glossary_dir.mkdir(parents=True, exist_ok=True)

    for defn in DEFINITIONS:
        # Add usage tracking fields
        entry = dict(defn)
        entry["validated_by"] = []
        entry["used_in_queries"] = []
        entry["validation_count"] = 0
        entry["usage_count"] = 0

        yaml_path = glossary_dir / f"{defn['definition_name']}.yaml"
        with open(yaml_path, "w") as f:
            yaml.dump(entry, f, default_flow_style=False,
                      sort_keys=False, allow_unicode=True)

    # Generate learned_terms
    learned_terms = _build_learned_terms(DEFINITIONS)
    terms_path = output_dir / "learned_terms.yaml"
    with open(terms_path, "w") as f:
        yaml.dump(learned_terms, f, default_flow_style=False,
                  sort_keys=False, allow_unicode=True)

    # Initialize empty route_preferences
    prefs_path = output_dir / "route_preferences.yaml"
    with open(prefs_path, "w") as f:
        yaml.dump({}, f)

    # Initialize empty user_library
    import json
    lib_path = output_dir / "user_library.json"
    with open(lib_path, "w") as f:
        json.dump({"users": {}, "saved_queries": {}}, f, indent=2)

    print(f"Definition Glossary: {len(DEFINITIONS)} entries → {glossary_dir}")
    print(f"Learned Terms: {len(learned_terms)} terms → {terms_path}")
    print(f"Route Preferences: initialized → {prefs_path}")
    print(f"User Library: initialized → {lib_path}")
    return glossary_dir


def load_definition_glossary(glossary_dir: str | Path = "tools/jit/data/definition_glossary") -> list[dict]:
    """Load all definition glossary entries from YAML files."""
    glossary_dir = Path(glossary_dir)
    definitions = []
    for yaml_path in sorted(glossary_dir.glob("*.yaml")):
        with open(yaml_path) as f:
            definitions.append(yaml.safe_load(f))
    return definitions


def load_learned_terms(path: str | Path = "tools/jit/data/learned_terms.yaml") -> dict:
    """Load learned terms glossary."""
    with open(path) as f:
        return yaml.safe_load(f) or {}


if __name__ == "__main__":
    import sys
    output = sys.argv[1] if len(sys.argv) > 1 else "tools/jit/data"
    generate_definition_glossary(output)
