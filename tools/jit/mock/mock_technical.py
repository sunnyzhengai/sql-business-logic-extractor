"""Generate the Technical Glossary — schema organized by domain.

Run directly:
    python3 -m tools.jit.mock.mock_technical [output_dir]

Outputs:
    {output_dir}/technical_glossary.yaml
    {output_dir}/route_catalog.yaml
"""

from __future__ import annotations

from pathlib import Path

import yaml


TECHNICAL_GLOSSARY = {
    "domains": {
        "encounters": {
            "description": "Patient visits, appointments, and clinical encounters",
            "anchor_tables": [
                {
                    "name": "PAT_ENC",
                    "description": "All patient encounters — outpatient, inpatient, ED, telehealth",
                    "primary_key": "PAT_ENC_CSN_ID",
                    "grain": "one row per encounter",
                    "key_columns": [
                        {"name": "PAT_ID", "description": "Patient identifier (FK to PATIENT)"},
                        {"name": "CONTACT_DATE", "description": "Date of encounter"},
                        {"name": "ENC_TYPE_C", "description": "Encounter type code"},
                        {"name": "DEPARTMENT_ID", "description": "Department (FK to CLARITY_DEP)"},
                        {"name": "APPT_STATUS_C", "description": "Appointment status: 1=Scheduled, 2=Completed, 3=Canceled, 4=No Show"},
                        {"name": "VISIT_PROV_ID", "description": "Visit provider (FK to CLARITY_SER)"},
                    ],
                    "satellite_tables": [
                        {
                            "name": "PAT_ENC_DX",
                            "join": "PAT_ENC_DX.PAT_ENC_CSN_ID = PAT_ENC.PAT_ENC_CSN_ID",
                            "relationship": "many diagnoses per encounter",
                            "description": "Encounter-level diagnoses",
                        },
                        {
                            "name": "F_SCHED_APPT",
                            "join": "F_SCHED_APPT.PAT_ENC_CSN_ID = PAT_ENC.PAT_ENC_CSN_ID",
                            "relationship": "one-to-one scheduling extension",
                            "description": "Scheduling fact table with check-in/checkout times",
                        },
                    ],
                },
                {
                    "name": "PAT_ENC_HSP",
                    "description": "Hospital/inpatient encounters — extends PAT_ENC with admission and discharge details",
                    "primary_key": "PAT_ENC_CSN_ID",
                    "grain": "one row per hospital encounter",
                    "key_columns": [
                        {"name": "PAT_ID", "description": "Patient identifier"},
                        {"name": "HOSP_ADMSN_TIME", "description": "Hospital admission datetime"},
                        {"name": "HOSP_DISCH_TIME", "description": "Hospital discharge datetime"},
                        {"name": "ADT_PAT_CLASS_C", "description": "Patient class (inpatient, observation, ED)"},
                    ],
                    "satellite_tables": [
                        {
                            "name": "HSP_ADMIT_DIAG",
                            "join": "HSP_ADMIT_DIAG.PAT_ENC_CSN_ID = PAT_ENC_HSP.PAT_ENC_CSN_ID",
                            "relationship": "many admission diagnoses per encounter",
                            "description": "Diagnoses recorded at time of hospital admission",
                        },
                    ],
                },
            ],
        },
        "billing": {
            "description": "Financial transactions, charges, payments, and hospital accounts",
            "anchor_tables": [
                {
                    "name": "ARPB_TRANSACTIONS",
                    "description": "Professional billing transactions — charges, payments, adjustments",
                    "primary_key": "TX_ID",
                    "grain": "one row per transaction",
                    "key_columns": [
                        {"name": "PATIENT_ID", "description": "Patient (FK to PATIENT.PAT_ID)"},
                        {"name": "SERVICE_DATE", "description": "Date of service"},
                        {"name": "POST_DATE", "description": "Date posted to billing system"},
                        {"name": "TX_TYPE_C", "description": "Transaction type (1=Charge)"},
                        {"name": "AMOUNT", "description": "Transaction amount"},
                    ],
                    "satellite_tables": [],
                },
                {
                    "name": "HSP_ACCOUNT",
                    "description": "Hospital account/billing summary — one per hospital stay",
                    "primary_key": "HSP_ACCOUNT_ID",
                    "grain": "one row per hospital account",
                    "key_columns": [
                        {"name": "PAT_ID", "description": "Patient identifier"},
                        {"name": "ADM_DATE_TIME", "description": "Admission datetime"},
                        {"name": "DISCH_DATE_TIME", "description": "Discharge datetime"},
                        {"name": "TOT_CHGS", "description": "Total charges"},
                        {"name": "TOT_PMTS", "description": "Total payments"},
                    ],
                    "satellite_tables": [
                        {
                            "name": "HSP_ACCT_DX_LIST",
                            "join": "HSP_ACCT_DX_LIST.HSP_ACCOUNT_ID = HSP_ACCOUNT.HSP_ACCOUNT_ID",
                            "relationship": "many discharge diagnoses per account",
                            "description": "Final discharge diagnosis list for billing",
                        },
                    ],
                },
            ],
        },
        "medications": {
            "description": "Medication orders and prescriptions",
            "anchor_tables": [
                {
                    "name": "ORDER_MED",
                    "description": "Medication orders — prescriptions, administrations",
                    "primary_key": "ORDER_MED_ID",
                    "grain": "one row per medication order",
                    "key_columns": [
                        {"name": "PAT_ID", "description": "Patient identifier"},
                        {"name": "PAT_ENC_CSN_ID", "description": "Encounter (FK to PAT_ENC)"},
                        {"name": "MEDICATION_ID", "description": "Medication (FK to CLARITY_MEDICATION)"},
                        {"name": "ORDER_STATUS_C", "description": "Order status"},
                    ],
                    "satellite_tables": [],
                },
            ],
        },
        "procedures": {
            "description": "Procedure and lab orders, surgeries",
            "anchor_tables": [
                {
                    "name": "ORDER_PROC",
                    "description": "Procedure/lab orders",
                    "primary_key": "ORDER_PROC_ID",
                    "grain": "one row per order",
                    "key_columns": [
                        {"name": "PAT_ID", "description": "Patient identifier"},
                        {"name": "PAT_ENC_CSN_ID", "description": "Encounter (FK to PAT_ENC)"},
                        {"name": "PROC_ID", "description": "Procedure (FK to CLARITY_EAP)"},
                        {"name": "ORDERING_DATE", "description": "Date ordered"},
                        {"name": "ORDER_STATUS_C", "description": "Order status"},
                    ],
                    "satellite_tables": [],
                },
            ],
        },
        "referrals": {
            "description": "Referral orders and tracking",
            "anchor_tables": [
                {
                    "name": "REFERRAL",
                    "description": "Referral orders — internal, incoming, outgoing",
                    "primary_key": "REFERRAL_ID",
                    "grain": "one row per referral",
                    "key_columns": [
                        {"name": "PAT_ID", "description": "Patient identifier"},
                        {"name": "ENTRY_DATE", "description": "Referral entry date"},
                        {"name": "RFL_STATUS_C", "description": "Status: 1=Authorized, 2=Open, 3=Pending, 4=Canceled, 5=Denied, 6=Closed"},
                        {"name": "PCP_PROV_ID", "description": "PCP provider (FK to CLARITY_SER)"},
                        {"name": "REFERRING_PROV_ID", "description": "Referring provider (FK to CLARITY_SER)"},
                    ],
                    "satellite_tables": [],
                },
            ],
        },
        "diagnosis": {
            "description": "Diagnosis tracking — problem lists, encounter diagnoses, admission diagnoses",
            "anchor_tables": [
                {
                    "name": "PROBLEM_LIST",
                    "description": "Patient problem list — chronic and active conditions",
                    "primary_key": "PROBLEM_LIST_ID",
                    "grain": "one row per problem per patient",
                    "key_columns": [
                        {"name": "PAT_ID", "description": "Patient identifier"},
                        {"name": "DX_ID", "description": "Diagnosis (FK to CLARITY_EDG)"},
                        {"name": "NOTED_DATE", "description": "Date problem was noted"},
                        {"name": "RESOLVED_DATE", "description": "Date resolved (NULL = active)"},
                    ],
                    "satellite_tables": [],
                },
            ],
        },
    },
    "dimensions": [
        {
            "name": "PATIENT",
            "description": "Patient demographics — the universal join point",
            "primary_key": "PAT_ID",
            "joins_to_all_domains": True,
            "key_columns": [
                {"name": "PAT_NAME", "description": "Patient full name"},
                {"name": "BIRTH_DATE", "description": "Date of birth"},
                {"name": "SEX", "description": "Sex"},
                {"name": "ZIP", "description": "ZIP code"},
            ],
        },
        {
            "name": "CLARITY_SER",
            "description": "Providers — physicians, nurses, clinicians",
            "primary_key": "PROV_ID",
            "joins_to_all_domains": False,
            "key_columns": [
                {"name": "PROV_NAME", "description": "Provider name"},
            ],
        },
        {
            "name": "CLARITY_DEP",
            "description": "Departments — clinics, EDs, wards",
            "primary_key": "DEPARTMENT_ID",
            "joins_to_all_domains": False,
            "key_columns": [
                {"name": "DEPARTMENT_NAME", "description": "Department name"},
                {"name": "SPECIALTY", "description": "Clinical specialty"},
            ],
        },
        {
            "name": "CLARITY_EDG",
            "description": "Diagnoses — ICD-10 coded conditions",
            "primary_key": "DX_ID",
            "joins_to_all_domains": False,
            "key_columns": [
                {"name": "DX_NAME", "description": "Diagnosis name"},
                {"name": "CURRENT_ICD10_LIST", "description": "ICD-10 code(s)"},
            ],
        },
        {
            "name": "CLARITY_MEDICATION",
            "description": "Medications — drugs, formulations",
            "primary_key": "MEDICATION_ID",
            "joins_to_all_domains": False,
            "key_columns": [
                {"name": "NAME", "description": "Medication name"},
                {"name": "GENERIC_NAME", "description": "Generic drug name"},
            ],
        },
        {
            "name": "CLARITY_EAP",
            "description": "Procedures — CPT/HCPCS coded procedures and labs",
            "primary_key": "PROC_ID",
            "joins_to_all_domains": False,
            "key_columns": [
                {"name": "PROC_NAME", "description": "Procedure name"},
                {"name": "PROC_CODE", "description": "CPT/HCPCS code"},
            ],
        },
    ],
}


ROUTE_CATALOG = {
    "diagnosis": {
        "description": "Routes to find diagnosis/condition information",
        "routes": [
            {
                "name": "Active problem list",
                "path": ["PATIENT", "PROBLEM_LIST", "CLARITY_EDG"],
                "description": "Chronic/ongoing conditions — best for 'patients with X disease'",
                "filter_column": "CLARITY_EDG.CURRENT_ICD10_LIST",
                "active_filter": "PROBLEM_LIST.RESOLVED_DATE IS NULL",
            },
            {
                "name": "Encounter diagnoses",
                "path": ["PATIENT", "PAT_ENC", "PAT_ENC_DX", "CLARITY_EDG"],
                "description": "Diagnoses recorded at each visit — best for 'diagnosed with X during a visit'",
                "filter_column": "CLARITY_EDG.CURRENT_ICD10_LIST",
            },
            {
                "name": "Discharge/billing diagnoses",
                "path": ["PATIENT", "HSP_ACCOUNT", "HSP_ACCT_DX_LIST", "CLARITY_EDG"],
                "description": "Final discharge diagnoses for billing — best for 'billed for X' or 'discharged with X'",
                "filter_column": "CLARITY_EDG.CURRENT_ICD10_LIST",
            },
        ],
    },
    "medication": {
        "description": "Routes to find medication/prescription information",
        "routes": [
            {
                "name": "Medication orders",
                "path": ["PATIENT", "PAT_ENC", "ORDER_MED", "CLARITY_MEDICATION"],
                "description": "Prescribed medications — 'patients taking X' or 'patients on X'",
                "filter_column": "CLARITY_MEDICATION.NAME",
            },
        ],
    },
    "procedure": {
        "description": "Routes to find procedure/lab information",
        "routes": [
            {
                "name": "Procedure orders",
                "path": ["PATIENT", "PAT_ENC", "ORDER_PROC", "CLARITY_EAP"],
                "description": "Ordered procedures and labs — 'patients who had X'",
                "filter_column": "CLARITY_EAP.PROC_NAME",
            },
        ],
    },
    "encounter": {
        "description": "Routes to find visit/appointment information",
        "routes": [
            {
                "name": "All encounters",
                "path": ["PATIENT", "PAT_ENC"],
                "description": "All patient visits — filter by department, status, type",
                "filter_column": "PAT_ENC.APPT_STATUS_C",
            },
            {
                "name": "Hospital encounters",
                "path": ["PATIENT", "PAT_ENC", "PAT_ENC_HSP"],
                "description": "Inpatient stays — admission/discharge times, LOS",
                "filter_column": "PAT_ENC_HSP.HOSP_ADMSN_TIME",
            },
            {
                "name": "ED encounters",
                "path": ["PATIENT", "PAT_ENC", "CLARITY_DEP"],
                "description": "Emergency department visits — filter by ED department names",
                "filter_column": "CLARITY_DEP.DEPARTMENT_NAME",
            },
        ],
    },
    "referral": {
        "description": "Routes to find referral information",
        "routes": [
            {
                "name": "Referral orders",
                "path": ["PATIENT", "REFERRAL", "CLARITY_SER"],
                "description": "Referral tracking with provider details",
                "filter_column": "REFERRAL.RFL_STATUS_C",
            },
        ],
    },
    "billing": {
        "description": "Routes to find financial/billing information",
        "routes": [
            {
                "name": "Professional billing",
                "path": ["PATIENT", "ARPB_TRANSACTIONS"],
                "description": "Professional billing charges and payments",
                "filter_column": "ARPB_TRANSACTIONS.TX_TYPE_C",
            },
            {
                "name": "Hospital billing",
                "path": ["PATIENT", "HSP_ACCOUNT"],
                "description": "Hospital account charges, payments, adjustments",
                "filter_column": "HSP_ACCOUNT.TOT_CHGS",
            },
        ],
    },
}


def generate_technical_glossary(output_dir: str | Path = "tools/jit/data"):
    """Write technical glossary and route catalog YAML files."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    tech_path = output_dir / "technical_glossary.yaml"
    with open(tech_path, "w") as f:
        yaml.dump(TECHNICAL_GLOSSARY, f, default_flow_style=False,
                  sort_keys=False, allow_unicode=True)

    route_path = output_dir / "route_catalog.yaml"
    with open(route_path, "w") as f:
        yaml.dump(ROUTE_CATALOG, f, default_flow_style=False,
                  sort_keys=False, allow_unicode=True)

    n_domains = len(TECHNICAL_GLOSSARY["domains"])
    n_anchors = sum(len(d["anchor_tables"])
                    for d in TECHNICAL_GLOSSARY["domains"].values())
    n_dims = len(TECHNICAL_GLOSSARY["dimensions"])
    n_routes = sum(len(cat["routes"]) for cat in ROUTE_CATALOG.values())

    print(f"Technical Glossary: {n_domains} domains, {n_anchors} anchors, {n_dims} dimensions → {tech_path}")
    print(f"Route Catalog: {n_routes} routes across {len(ROUTE_CATALOG)} categories → {route_path}")
    return tech_path, route_path


def load_technical_glossary(path: str | Path = "tools/jit/data/technical_glossary.yaml") -> dict:
    """Load the technical glossary."""
    with open(path) as f:
        return yaml.safe_load(f)


def load_route_catalog(path: str | Path = "tools/jit/data/route_catalog.yaml") -> dict:
    """Load the route catalog."""
    with open(path) as f:
        return yaml.safe_load(f)


if __name__ == "__main__":
    import sys
    output = sys.argv[1] if len(sys.argv) > 1 else "tools/jit/data"
    generate_technical_glossary(output)
