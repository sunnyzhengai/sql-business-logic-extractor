#!/usr/bin/env python3
"""
SQL Business Logic Extractor — Metadata Layer

Column/table descriptions for Epic Clarity (mock data for development).
In production, replace COLUMN_DESCRIPTIONS and TABLE_DESCRIPTIONS with
a CSV/DB export from your Clarity metadata tables.

Load your own: metadata.load_csv("path/to/export.csv")
Expected CSV columns: table_name, column_name, description
"""

import csv
import os

# ---------------------------------------------------------------------------
# Mock Epic Clarity metadata
# ---------------------------------------------------------------------------

TABLE_DESCRIPTIONS = {
    "PATIENT": "Master patient demographics table",
    "PAT_ENC_HSP": "Hospital patient encounters (inpatient, ED, observation)",
    "PAT_ENC": "All patient encounters (outpatient, inpatient, ED)",
    "CLARITY_DEP": "Department reference table",
    "CLARITY_SER": "Provider/service reference table",
    "CLARITY_EDG": "Diagnosis reference table (ICD-10, descriptions)",
    "CLARITY_LOC": "Location/facility reference table",
    "CLARITY_ADT": "Admit/Discharge/Transfer events",
    "HSP_ACCT_DX_LIST": "Diagnosis list for hospital accounts (primary, secondary, etc.)",
    "HSP_TRANSACTIONS": "Hospital account charges, payments, and adjustments",
    "ORDER_PROC": "Procedure and imaging orders",
    "ORDER_MED": "Medication orders",
    "MAR_ADMIN_INFO": "Medication administration records",
    "IP_FLWSHT_MEAS": "Inpatient flowsheet measurements (vitals, assessments)",
    "IDENTITY_ID": "Patient MRN and identity cross-reference",
    "V_CUBE_D_ENCOUNTER": "Encounter reporting cube (prebuilt analytics view)",
}

COLUMN_DESCRIPTIONS = {
    # PATIENT
    ("PATIENT", "PAT_ID"): "Internal patient identifier",
    ("PATIENT", "PAT_MRN_ID"): "Medical Record Number (MRN)",
    ("PATIENT", "PAT_FIRST_NAME"): "Patient first name",
    ("PATIENT", "PAT_LAST_NAME"): "Patient last name",
    ("PATIENT", "BIRTH_DATE"): "Patient date of birth",
    ("PATIENT", "PAT_STATUS"): "Patient account status (Active, Inactive, Deceased)",
    ("PATIENT", "STATE_ABBR"): "Patient state of residence (2-letter abbreviation)",
    ("PATIENT", "AGE_YEARS"): "Patient age in years (calculated)",
    ("PATIENT", "SEX_C"): "Patient sex category (1=Female, 2=Male, 3=Other)",
    ("PATIENT", "PRIMARY_DX_CODE"): "Patient primary diagnosis code",

    # PAT_ENC_HSP
    ("PAT_ENC_HSP", "PAT_ENC_CSN_ID"): "Contact Serial Number — unique encounter identifier",
    ("PAT_ENC_HSP", "PAT_ID"): "Internal patient identifier (FK to PATIENT)",
    ("PAT_ENC_HSP", "HOSP_ADMSN_TIME"): "Hospital admission date and time",
    ("PAT_ENC_HSP", "HOSP_DISCH_TIME"): "Hospital discharge date and time",
    ("PAT_ENC_HSP", "ADT_PAT_CLASS_C"): "Patient class (1=Inpatient, 2=Outpatient, 3=Emergency, 4=Observation)",
    ("PAT_ENC_HSP", "DEPARTMENT_ID"): "Department where patient is located (FK to CLARITY_DEP)",
    ("PAT_ENC_HSP", "ATTENDING_PROV_ID"): "Attending provider ID (FK to CLARITY_SER)",
    ("PAT_ENC_HSP", "DISCH_DISPOSITION_C"): "Discharge disposition (1=Home, 2=Transfer, 20=Expired)",
    ("PAT_ENC_HSP", "HSP_ACCOUNT_ID"): "Hospital account ID (FK to HSP_TRANSACTIONS)",
    ("PAT_ENC_HSP", "TOTAL_CHARGES"): "Total charges for the encounter",
    ("PAT_ENC_HSP", "ED_DEPARTURE_TIME"): "Time patient departed the Emergency Department",
    ("PAT_ENC_HSP", "LOS_DAYS"): "Length of stay in days (calculated)",

    # PAT_ENC
    ("PAT_ENC", "PAT_ID"): "Internal patient identifier",
    ("PAT_ENC", "PAT_ENC_CSN_ID"): "Contact Serial Number — unique encounter identifier",
    ("PAT_ENC", "ENC_TYPE_C"): "Encounter type category",

    # CLARITY_DEP
    ("CLARITY_DEP", "DEPARTMENT_ID"): "Department identifier",
    ("CLARITY_DEP", "DEPARTMENT_NAME"): "Department name",
    ("CLARITY_DEP", "COST_PER_DAY"): "Estimated cost per patient day for the department",

    # CLARITY_SER
    ("CLARITY_SER", "PROV_ID"): "Provider identifier",
    ("CLARITY_SER", "PROV_NAME"): "Provider full name",

    # CLARITY_EDG
    ("CLARITY_EDG", "DX_ID"): "Diagnosis identifier",
    ("CLARITY_EDG", "DX_NAME"): "Diagnosis name/description",
    ("CLARITY_EDG", "ICD10_CODE"): "ICD-10 diagnosis code",

    # HSP_ACCT_DX_LIST
    ("HSP_ACCT_DX_LIST", "HSP_ACCOUNT_ID"): "Hospital account identifier",
    ("HSP_ACCT_DX_LIST", "DX_ID"): "Diagnosis identifier (FK to CLARITY_EDG)",
    ("HSP_ACCT_DX_LIST", "LINE"): "Diagnosis priority line (1=Primary, 2=Secondary, etc.)",

    # HSP_TRANSACTIONS
    ("HSP_TRANSACTIONS", "HSP_ACCOUNT_ID"): "Hospital account identifier",
    ("HSP_TRANSACTIONS", "TX_AMOUNT"): "Transaction amount (charge, payment, or adjustment)",
    ("HSP_TRANSACTIONS", "TX_TYPE_C"): "Transaction type (1=Charge, 2=Payment, 3=Adjustment)",
    ("HSP_TRANSACTIONS", "SERVICE_DATE"): "Date the service was provided",

    # ORDER_MED
    ("ORDER_MED", "ORDER_MED_ID"): "Medication order identifier",
    ("ORDER_MED", "PAT_ENC_CSN_ID"): "Encounter identifier (FK to PAT_ENC_HSP)",
    ("ORDER_MED", "DESCRIPTION"): "Medication name/description",
    ("ORDER_MED", "ORDER_STATUS_C"): "Order status (1=Pending, 2=Active/Complete, 3=Discontinued)",
    ("ORDER_MED", "ORDER_START_TIME"): "Order start date and time",
    ("ORDER_MED", "ORDER_END_TIME"): "Order end date and time",

    # MAR_ADMIN_INFO
    ("MAR_ADMIN_INFO", "ORDER_MED_ID"): "Medication order identifier (FK to ORDER_MED)",
    ("MAR_ADMIN_INFO", "TAKEN_TIME"): "Time the medication was actually administered",
    ("MAR_ADMIN_INFO", "SCHEDULED_TIME"): "Time the medication was scheduled to be given",
    ("MAR_ADMIN_INFO", "MAR_ACTION_C"): "Administration action (1=Given, 2=Held, 3=Refused)",

    # IP_FLWSHT_MEAS
    ("IP_FLWSHT_MEAS", "PAT_ENC_CSN_ID"): "Encounter identifier",
    ("IP_FLWSHT_MEAS", "RECORDED_TIME"): "Time the measurement was recorded",
    ("IP_FLWSHT_MEAS", "FLO_MEAS_ID"): "Flowsheet measurement type (5=Blood Pressure, 8=Heart Rate, etc.)",

    # ORDER_PROC
    ("ORDER_PROC", "ORDER_START_TIME"): "Procedure order start time",
    ("ORDER_PROC", "ORDER_END_TIME"): "Procedure order end time",
}

# Categorical value descriptions (for WHERE filters and CASE branches)
VALUE_DESCRIPTIONS = {
    ("PAT_ENC_HSP", "ADT_PAT_CLASS_C"): {
        "1": "Inpatient",
        "2": "Outpatient",
        "3": "Emergency",
        "4": "Observation",
    },
    ("PAT_ENC_HSP", "DISCH_DISPOSITION_C"): {
        "1": "Home",
        "2": "Transfer to another facility",
        "20": "Expired (deceased)",
    },
    ("ORDER_MED", "ORDER_STATUS_C"): {
        "1": "Pending",
        "2": "Active/Complete",
        "3": "Discontinued",
    },
    ("MAR_ADMIN_INFO", "MAR_ACTION_C"): {
        "1": "Given",
        "2": "Held",
        "3": "Refused",
    },
    ("HSP_TRANSACTIONS", "TX_TYPE_C"): {
        "1": "Charge",
        "2": "Payment",
        "3": "Adjustment",
    },
    ("PATIENT", "PAT_STATUS"): {
        "'Active'": "Active",
        "'Inactive'": "Inactive",
        "'Deceased'": "Deceased",
    },
}


# ---------------------------------------------------------------------------
# Lookup functions
# ---------------------------------------------------------------------------

def get_table_description(table_name: str) -> str:
    return TABLE_DESCRIPTIONS.get(table_name.upper(), "")


def get_column_description(table_name: str, column_name: str) -> str:
    return COLUMN_DESCRIPTIONS.get((table_name.upper(), column_name.upper()), "")


def get_value_description(table_name: str, column_name: str, value: str) -> str:
    vals = VALUE_DESCRIPTIONS.get((table_name.upper(), column_name.upper()), {})
    return vals.get(str(value), "")


def describe_column(table_name: str, column_name: str) -> str:
    """Return 'Description (TABLE.COLUMN)' or just 'TABLE.COLUMN' if no description."""
    desc = get_column_description(table_name, column_name)
    qualified = f"{table_name}.{column_name}"
    if desc:
        return f"{desc} ({qualified})"
    return qualified


def describe_qualified(qualified_col: str) -> str:
    """Describe a 'TABLE.COLUMN' string."""
    parts = qualified_col.split(".")
    if len(parts) == 2:
        return describe_column(parts[0], parts[1])
    return qualified_col


# ---------------------------------------------------------------------------
# Load from CSV (for production use)
# ---------------------------------------------------------------------------

def load_csv(path: str):
    """Load column descriptions from a CSV file.
    Expected columns: table_name, column_name, description
    Optionally: value, value_description (for categorical columns)
    """
    global COLUMN_DESCRIPTIONS, TABLE_DESCRIPTIONS, VALUE_DESCRIPTIONS

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            table = row.get("table_name", "").upper().strip()
            column = row.get("column_name", "").upper().strip()
            desc = row.get("description", "").strip()
            value = row.get("value", "").strip()
            value_desc = row.get("value_description", "").strip()

            if table and column and desc:
                COLUMN_DESCRIPTIONS[(table, column)] = desc
            if table and not column and desc:
                TABLE_DESCRIPTIONS[table] = desc
            if table and column and value and value_desc:
                key = (table, column)
                if key not in VALUE_DESCRIPTIONS:
                    VALUE_DESCRIPTIONS[key] = {}
                VALUE_DESCRIPTIONS[key][value] = value_desc
