"""Generate a mock SQLite database with correlated healthcare data.

Run directly to create the database:
    python -m tools.jit.mock.mock_db [output_path]

Data correlation design (for the demo question):
    500 PATIENT
     └─ 90 have diabetes (PROBLEM_LIST + CLARITY_EDG, E11.%)
         └─ 45 of those have 4+ ED encounters in past year
             └─ 20 of those have no-show PCP in past 6 months

    Also present (for contrast):
     - 60 non-diabetic patients with 4+ ED visits
     - 30 diabetic patients with no-show PCP but <4 ED visits
"""

from __future__ import annotations

import random
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path

# Reproducible data
random.seed(42)

# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------

NUM_PATIENTS = 500
NUM_DIABETIC = 90
NUM_DIABETIC_ED_HIGH = 45      # diabetic + 4+ ED visits
NUM_DIABETIC_ED_HIGH_NOSHOW = 20  # diabetic + 4+ ED + PCP no-show

NUM_NONDIABETIC_ED_HIGH = 60   # non-diabetic with 4+ ED (for contrast)
NUM_DIABETIC_PCP_NOSHOW_LOW_ED = 30  # diabetic + PCP no-show but <4 ED

TODAY = date(2026, 6, 11)
ONE_YEAR_AGO = TODAY - timedelta(days=365)
SIX_MONTHS_AGO = TODAY - timedelta(days=182)

SPECIALTIES = [
    "Family Medicine", "Internal Medicine", "Cardiology", "Dermatology",
    "Endocrinology", "Gastroenterology", "Neurology", "Oncology",
    "Orthopedics", "Pediatrics", "Psychiatry", "Pulmonology",
    "Rheumatology", "Surgery", "Urology",
]

FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael",
    "Linda", "David", "Elizabeth", "William", "Barbara", "Richard", "Susan",
    "Joseph", "Jessica", "Thomas", "Sarah", "Christopher", "Karen",
    "Daniel", "Lisa", "Matthew", "Nancy", "Anthony", "Betty", "Mark",
    "Margaret", "Donald", "Sandra", "Steven", "Ashley", "Paul", "Dorothy",
    "Andrew", "Kimberly", "Joshua", "Emily", "Kenneth", "Donna",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
    "Ramirez", "Lewis", "Robinson",
]


# --------------------------------------------------------------------------
# Schema DDL
# --------------------------------------------------------------------------

DDL = """
-- Lookup tables
CREATE TABLE ZC_APPT_STATUS (
    APPT_STATUS_C INTEGER PRIMARY KEY,
    NAME TEXT
);

CREATE TABLE ZC_DISP_ENC_TYPE (
    DISP_ENC_TYPE_C INTEGER PRIMARY KEY,
    NAME TEXT
);

CREATE TABLE ZC_SPECIALTY (
    SPECIALTY_C INTEGER PRIMARY KEY,
    NAME TEXT
);

-- Dimension tables
CREATE TABLE PATIENT (
    PAT_ID INTEGER PRIMARY KEY,
    PAT_NAME TEXT,
    BIRTH_DATE TEXT,
    SEX TEXT,
    ZIP TEXT
);

CREATE TABLE CLARITY_SER (
    PROV_ID INTEGER PRIMARY KEY,
    PROV_NAME TEXT
);

CREATE TABLE CLARITY_DEP (
    DEPARTMENT_ID INTEGER PRIMARY KEY,
    DEPARTMENT_NAME TEXT,
    SPECIALTY TEXT
);

CREATE TABLE CLARITY_EDG (
    DX_ID INTEGER PRIMARY KEY,
    DX_NAME TEXT,
    CURRENT_ICD10_LIST TEXT
);

CREATE TABLE CLARITY_MEDICATION (
    MEDICATION_ID INTEGER PRIMARY KEY,
    NAME TEXT,
    GENERIC_NAME TEXT,
    PHARM_CLASS_C INTEGER
);

CREATE TABLE CLARITY_EAP (
    PROC_ID INTEGER PRIMARY KEY,
    PROC_NAME TEXT,
    PROC_CODE TEXT
);

-- Fact tables
CREATE TABLE PAT_ENC (
    PAT_ENC_CSN_ID INTEGER PRIMARY KEY,
    PAT_ID INTEGER REFERENCES PATIENT(PAT_ID),
    CONTACT_DATE TEXT,
    APPT_TIME TEXT,
    ENC_TYPE_C INTEGER,
    DEPARTMENT_ID INTEGER REFERENCES CLARITY_DEP(DEPARTMENT_ID),
    VISIT_PROV_ID INTEGER REFERENCES CLARITY_SER(PROV_ID),
    APPT_STATUS_C INTEGER REFERENCES ZC_APPT_STATUS(APPT_STATUS_C),
    REFERRAL_ID INTEGER
);

CREATE TABLE PAT_ENC_HSP (
    PAT_ENC_CSN_ID INTEGER PRIMARY KEY REFERENCES PAT_ENC(PAT_ENC_CSN_ID),
    PAT_ID INTEGER REFERENCES PATIENT(PAT_ID),
    HOSP_ADMSN_TIME TEXT,
    HOSP_DISCH_TIME TEXT,
    ADT_PAT_CLASS_C INTEGER
);

CREATE TABLE HSP_ACCOUNT (
    HSP_ACCOUNT_ID INTEGER PRIMARY KEY,
    PAT_ID INTEGER REFERENCES PATIENT(PAT_ID),
    PRIM_ENC_CSN_ID INTEGER REFERENCES PAT_ENC(PAT_ENC_CSN_ID),
    ADM_DATE_TIME TEXT,
    DISCH_DATE_TIME TEXT,
    TOT_CHGS REAL,
    TOT_PMTS REAL,
    TOT_ADJ REAL
);

CREATE TABLE PAT_ENC_DX (
    PAT_ENC_CSN_ID INTEGER REFERENCES PAT_ENC(PAT_ENC_CSN_ID),
    LINE INTEGER,
    DX_ID INTEGER REFERENCES CLARITY_EDG(DX_ID),
    PRIMARY_YN TEXT,
    PRIMARY KEY (PAT_ENC_CSN_ID, LINE)
);

CREATE TABLE HSP_ADMIT_DIAG (
    PAT_ENC_CSN_ID INTEGER REFERENCES PAT_ENC_HSP(PAT_ENC_CSN_ID),
    LINE INTEGER,
    DX_ID INTEGER REFERENCES CLARITY_EDG(DX_ID),
    PRIMARY KEY (PAT_ENC_CSN_ID, LINE)
);

CREATE TABLE HSP_ACCT_DX_LIST (
    HSP_ACCOUNT_ID INTEGER REFERENCES HSP_ACCOUNT(HSP_ACCOUNT_ID),
    LINE INTEGER,
    DX_ID INTEGER REFERENCES CLARITY_EDG(DX_ID),
    PRIMARY KEY (HSP_ACCOUNT_ID, LINE)
);

CREATE TABLE PROBLEM_LIST (
    PROBLEM_LIST_ID INTEGER PRIMARY KEY,
    PAT_ID INTEGER REFERENCES PATIENT(PAT_ID),
    DX_ID INTEGER REFERENCES CLARITY_EDG(DX_ID),
    NOTED_DATE TEXT,
    RESOLVED_DATE TEXT,
    DATE_OF_ENTRY TEXT
);

CREATE TABLE PAT_PCP (
    PAT_ID INTEGER REFERENCES PATIENT(PAT_ID),
    PCP_PROV_ID INTEGER REFERENCES CLARITY_SER(PROV_ID),
    EFF_DATE TEXT,
    TERM_DATE TEXT,
    PRIMARY KEY (PAT_ID, PCP_PROV_ID, EFF_DATE)
);

CREATE TABLE ORDER_MED (
    ORDER_MED_ID INTEGER PRIMARY KEY,
    PAT_ID INTEGER REFERENCES PATIENT(PAT_ID),
    PAT_ENC_CSN_ID INTEGER REFERENCES PAT_ENC(PAT_ENC_CSN_ID),
    MEDICATION_ID INTEGER REFERENCES CLARITY_MEDICATION(MEDICATION_ID),
    ORDER_STATUS_C INTEGER
);

CREATE TABLE ORDER_PROC (
    ORDER_PROC_ID INTEGER PRIMARY KEY,
    PAT_ID INTEGER REFERENCES PATIENT(PAT_ID),
    PAT_ENC_CSN_ID INTEGER REFERENCES PAT_ENC(PAT_ENC_CSN_ID),
    PROC_ID INTEGER REFERENCES CLARITY_EAP(PROC_ID),
    ORDERING_DATE TEXT,
    ORDER_STATUS_C INTEGER
);

CREATE TABLE REFERRAL (
    REFERRAL_ID INTEGER PRIMARY KEY,
    PAT_ID INTEGER REFERENCES PATIENT(PAT_ID),
    ENTRY_DATE TEXT,
    RFL_STATUS_C INTEGER,
    RFL_TYPE_C INTEGER,
    PCP_PROV_ID INTEGER REFERENCES CLARITY_SER(PROV_ID),
    REFERRING_PROV_ID INTEGER REFERENCES CLARITY_SER(PROV_ID)
);

CREATE TABLE ARPB_TRANSACTIONS (
    TX_ID INTEGER PRIMARY KEY,
    PATIENT_ID INTEGER REFERENCES PATIENT(PAT_ID),
    SERVICE_DATE TEXT,
    POST_DATE TEXT,
    TX_TYPE_C INTEGER,
    AMOUNT REAL,
    SERV_PROVIDER_ID INTEGER REFERENCES CLARITY_SER(PROV_ID),
    DEPARTMENT_ID INTEGER REFERENCES CLARITY_DEP(DEPARTMENT_ID)
);
"""


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

def _random_date(start: date, end: date) -> str:
    delta = (end - start).days
    d = start + timedelta(days=random.randint(0, max(delta, 0)))
    return d.isoformat()


def _random_datetime(start: date, end: date) -> str:
    delta = (end - start).days
    d = start + timedelta(days=random.randint(0, max(delta, 0)),
                          hours=random.randint(6, 20),
                          minutes=random.choice([0, 15, 30, 45]))
    return datetime.combine(d, datetime.min.time().replace(
        hour=random.randint(6, 20),
        minute=random.choice([0, 15, 30, 45])
    )).isoformat()


def _random_name() -> str:
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


# --------------------------------------------------------------------------
# Data generation
# --------------------------------------------------------------------------

def _populate_lookups(conn: sqlite3.Connection):
    cur = conn.cursor()

    # ZC_APPT_STATUS
    statuses = [
        (1, "SCHEDULED"), (2, "COMPLETED"), (3, "CANCELED"),
        (4, "NO SHOW"), (5, "LEFT WITHOUT SEEN"), (6, "ARRIVED"),
    ]
    cur.executemany("INSERT INTO ZC_APPT_STATUS VALUES (?, ?)", statuses)

    # ZC_DISP_ENC_TYPE
    enc_types = [
        (1, "Registration"), (2, "Walk-In"), (3, "Hospital Encounter"),
        (5, "Canceled"),
    ]
    cur.executemany("INSERT INTO ZC_DISP_ENC_TYPE VALUES (?, ?)", enc_types)

    # ZC_SPECIALTY
    for i, spec in enumerate(SPECIALTIES, 1):
        cur.execute("INSERT INTO ZC_SPECIALTY VALUES (?, ?)", (i, spec))

    conn.commit()


def _populate_dimensions(conn: sqlite3.Connection):
    cur = conn.cursor()

    # CLARITY_SER — 50 providers
    for prov_id in range(1, 51):
        cur.execute("INSERT INTO CLARITY_SER VALUES (?, ?)",
                     (prov_id, _random_name()))

    # CLARITY_DEP — 20 departments
    departments = []
    # 3 ED departments
    for i in range(1, 4):
        departments.append((i, f"Emergency Department {i}", "Emergency Medicine"))
    # 5 PCP/Family Medicine departments
    for i in range(4, 9):
        departments.append((i, f"Primary Care Clinic {i-3}", "Family Medicine"))
    # 12 specialty departments
    for i in range(9, 21):
        spec = SPECIALTIES[(i - 9) % len(SPECIALTIES)]
        departments.append((i, f"{spec} Clinic", spec))
    cur.executemany("INSERT INTO CLARITY_DEP VALUES (?, ?, ?)", departments)

    # CLARITY_EDG — 200 diagnoses
    diagnoses = []
    dx_id = 1

    # 15 diabetes-related (E11.x)
    diabetes_names = [
        ("E11.9", "Type 2 diabetes mellitus without complications"),
        ("E11.65", "Type 2 diabetes mellitus with hyperglycemia"),
        ("E11.8", "Type 2 diabetes mellitus with unspecified complications"),
        ("E11.21", "Type 2 diabetes mellitus with diabetic nephropathy"),
        ("E11.22", "Type 2 diabetes mellitus with diabetic CKD"),
        ("E11.311", "Type 2 diabetes with unspecified diabetic retinopathy with macular edema"),
        ("E11.319", "Type 2 diabetes with unspecified diabetic retinopathy without macular edema"),
        ("E11.40", "Type 2 diabetes mellitus with diabetic neuropathy, unspecified"),
        ("E11.41", "Type 2 diabetes mellitus with diabetic mononeuropathy"),
        ("E11.42", "Type 2 diabetes mellitus with diabetic polyneuropathy"),
        ("E11.51", "Type 2 diabetes mellitus with diabetic peripheral angiopathy without gangrene"),
        ("E11.52", "Type 2 diabetes mellitus with diabetic peripheral angiopathy with gangrene"),
        ("E11.610", "Type 2 diabetes mellitus with diabetic neuropathic arthropathy"),
        ("E11.620", "Type 2 diabetes mellitus with diabetic dermatitis"),
        ("E11.69", "Type 2 diabetes mellitus with other specified complication"),
    ]
    for icd10, name in diabetes_names:
        diagnoses.append((dx_id, name, icd10))
        dx_id += 1

    # 20 hypertension
    htn_codes = [
        ("I10", "Essential (primary) hypertension"),
        ("I11.0", "Hypertensive heart disease with heart failure"),
        ("I11.9", "Hypertensive heart disease without heart failure"),
        ("I12.0", "Hypertensive CKD with stage 5 or ESRD"),
        ("I12.9", "Hypertensive CKD with stage 1-4 or unspecified"),
        ("I13.0", "Hypertensive heart and CKD with heart failure"),
        ("I13.10", "Hypertensive heart and CKD without heart failure"),
        ("I15.0", "Renovascular hypertension"),
        ("I15.1", "Hypertension secondary to other renal disorders"),
        ("I15.2", "Hypertension secondary to endocrine disorders"),
    ]
    for icd10, name in htn_codes:
        diagnoses.append((dx_id, name, icd10))
        dx_id += 1
    # Pad to 20 with repeated patterns
    for i in range(10):
        diagnoses.append((dx_id, f"Hypertension variant {i+1}", f"I1{i%6}.{i}"))
        dx_id += 1

    # 10 asthma
    for i in range(10):
        diagnoses.append((dx_id, f"Asthma type {i+1}", f"J45.{i}"))
        dx_id += 1

    # Fill remaining to 200 with mixed conditions
    other_conditions = [
        ("J06.9", "Acute upper respiratory infection"),
        ("M54.5", "Low back pain"),
        ("K21.0", "GERD with esophagitis"),
        ("F32.1", "Major depressive disorder, moderate"),
        ("F41.1", "Generalized anxiety disorder"),
        ("G43.909", "Migraine, unspecified"),
        ("N39.0", "Urinary tract infection"),
        ("J20.9", "Acute bronchitis, unspecified"),
        ("R10.9", "Unspecified abdominal pain"),
        ("M79.3", "Panniculitis"),
    ]
    while dx_id <= 200:
        icd10, name = other_conditions[(dx_id - 1) % len(other_conditions)]
        # Make codes unique by appending index
        diagnoses.append((dx_id, f"{name} ({dx_id})", f"{icd10}.{dx_id}"))
        dx_id += 1

    cur.executemany("INSERT INTO CLARITY_EDG VALUES (?, ?, ?)", diagnoses)

    # CLARITY_MEDICATION — 30 medications
    meds = [
        (1, "Metformin 500mg", "Metformin", 1),
        (2, "Metformin 1000mg", "Metformin", 1),
        (3, "Glipizide 5mg", "Glipizide", 1),
        (4, "Insulin Glargine", "Insulin Glargine", 2),
        (5, "Lisinopril 10mg", "Lisinopril", 3),
        (6, "Amlodipine 5mg", "Amlodipine", 3),
        (7, "Atorvastatin 20mg", "Atorvastatin", 4),
        (8, "Omeprazole 20mg", "Omeprazole", 5),
        (9, "Albuterol Inhaler", "Albuterol", 6),
        (10, "Amoxicillin 500mg", "Amoxicillin", 7),
    ]
    for i in range(11, 31):
        meds.append((i, f"Medication {i}", f"Generic {i}", (i % 7) + 1))
    cur.executemany("INSERT INTO CLARITY_MEDICATION VALUES (?, ?, ?, ?)", meds)

    # CLARITY_EAP — 20 procedures
    procs = [
        (1, "CBC", "85025"), (2, "BMP", "80048"), (3, "HbA1c", "83036"),
        (4, "Lipid Panel", "80061"), (5, "Urinalysis", "81001"),
        (6, "Chest X-Ray", "71046"), (7, "CT Head", "70450"),
        (8, "MRI Brain", "70553"), (9, "EKG", "93000"),
        (10, "Echocardiogram", "93306"),
    ]
    for i in range(11, 21):
        procs.append((i, f"Procedure {i}", f"9{i:04d}"))
    cur.executemany("INSERT INTO CLARITY_EAP VALUES (?, ?, ?)", procs)

    conn.commit()


def _populate_patients(conn: sqlite3.Connection):
    cur = conn.cursor()
    for pat_id in range(1, NUM_PATIENTS + 1):
        # Age distribution: 100 pediatric (0-17), 300 adult (18-64), 100 elderly (65+)
        if pat_id <= 100:
            birth_year = random.randint(2009, 2026)
        elif pat_id <= 400:
            birth_year = random.randint(1962, 2008)
        else:
            birth_year = random.randint(1935, 1961)
        birth_date = date(birth_year, random.randint(1, 12), random.randint(1, 28))
        sex = random.choice(["Male", "Female", "Female", "Male", "Other"])
        cur.execute("INSERT INTO PATIENT VALUES (?, ?, ?, ?, ?)",
                     (pat_id, _random_name(), birth_date.isoformat(),
                      sex, f"{random.randint(10000, 99999)}"))
    conn.commit()


def _populate_encounters(conn: sqlite3.Connection):
    """Generate PAT_ENC records with correlated ED visits and PCP no-shows."""
    cur = conn.cursor()
    csn_id = 1

    # Build patient sets for correlated data
    all_pats = list(range(1, NUM_PATIENTS + 1))
    random.shuffle(all_pats)

    diabetic_pats = set(all_pats[:NUM_DIABETIC])                    # 90
    diabetic_ed_high = set(all_pats[:NUM_DIABETIC_ED_HIGH])         # 45 (subset)
    diabetic_ed_high_noshow = set(all_pats[:NUM_DIABETIC_ED_HIGH_NOSHOW])  # 20 (subset)

    # Non-diabetic with high ED (for contrast)
    nondiabetic_ed_high = set(all_pats[NUM_DIABETIC:NUM_DIABETIC + NUM_NONDIABETIC_ED_HIGH])

    # Diabetic with PCP no-show but LOW ED (for contrast)
    diabetic_noshow_low_ed = set(all_pats[NUM_DIABETIC_ED_HIGH:NUM_DIABETIC_ED_HIGH + NUM_DIABETIC_PCP_NOSHOW_LOW_ED])

    ed_dept_ids = [1, 2, 3]          # departments 1-3 are ED
    pcp_dept_ids = [4, 5, 6, 7, 8]   # departments 4-8 are PCP
    other_dept_ids = list(range(9, 21))

    for pat_id in range(1, NUM_PATIENTS + 1):
        # Base encounters for everyone: 2-4 routine visits
        n_routine = random.randint(2, 4)
        for _ in range(n_routine):
            dept_id = random.choice(other_dept_ids + pcp_dept_ids)
            contact = _random_date(ONE_YEAR_AGO - timedelta(days=365), TODAY)
            status = random.choices([2, 1, 3], weights=[80, 10, 10])[0]
            cur.execute(
                "INSERT INTO PAT_ENC VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (csn_id, pat_id, contact, contact, 2, dept_id,
                 random.randint(1, 50), status, None))
            csn_id += 1

        # ED encounters — high utilizers get 4-8 in past year
        if pat_id in diabetic_ed_high or pat_id in nondiabetic_ed_high:
            n_ed = random.randint(4, 8)
        else:
            # Others get 0-2 ED visits
            n_ed = random.randint(0, 2)

        for _ in range(n_ed):
            dept_id = random.choice(ed_dept_ids)
            contact = _random_date(ONE_YEAR_AGO, TODAY)
            cur.execute(
                "INSERT INTO PAT_ENC VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (csn_id, pat_id, contact, contact, 3, dept_id,
                 random.randint(1, 50), 2, None))  # status=COMPLETED
            csn_id += 1

        # PCP no-show encounters in last 6 months
        if pat_id in diabetic_ed_high_noshow or pat_id in diabetic_noshow_low_ed:
            # 1-3 no-show PCP visits
            n_noshow = random.randint(1, 3)
            for _ in range(n_noshow):
                dept_id = random.choice(pcp_dept_ids)
                contact = _random_date(SIX_MONTHS_AGO, TODAY)
                cur.execute(
                    "INSERT INTO PAT_ENC VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (csn_id, pat_id, contact, contact, 2, dept_id,
                     random.randint(1, 50), 4, None))  # status=NO SHOW
                csn_id += 1
        else:
            # Small chance of random no-show for others (5%)
            if random.random() < 0.05:
                dept_id = random.choice(pcp_dept_ids)
                contact = _random_date(SIX_MONTHS_AGO, TODAY)
                cur.execute(
                    "INSERT INTO PAT_ENC VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (csn_id, pat_id, contact, contact, 2, dept_id,
                     random.randint(1, 50), 4, None))
                csn_id += 1

    conn.commit()
    return diabetic_pats, diabetic_ed_high, diabetic_ed_high_noshow


def _populate_diagnoses(conn: sqlite3.Connection, diabetic_pats: set[int]):
    """Populate PAT_ENC_DX and PROBLEM_LIST with correlated diabetes data."""
    cur = conn.cursor()

    diabetes_dx_ids = list(range(1, 16))  # DX_IDs 1-15 are diabetes
    other_dx_ids = list(range(16, 201))

    # PROBLEM_LIST — active chronic conditions
    problem_id = 1
    for pat_id in range(1, NUM_PATIENTS + 1):
        if pat_id in diabetic_pats:
            # Active diabetes on problem list
            dx_id = random.choice(diabetes_dx_ids)
            cur.execute(
                "INSERT INTO PROBLEM_LIST VALUES (?, ?, ?, ?, ?, ?)",
                (problem_id, pat_id, dx_id,
                 _random_date(date(2020, 1, 1), date(2024, 1, 1)),
                 None,  # RESOLVED_DATE = NULL → active
                 _random_date(date(2020, 1, 1), date(2024, 1, 1))))
            problem_id += 1

        # Everyone gets 0-3 other problems
        for _ in range(random.randint(0, 3)):
            dx_id = random.choice(other_dx_ids)
            resolved = None if random.random() < 0.6 else _random_date(
                date(2023, 1, 1), TODAY)
            cur.execute(
                "INSERT INTO PROBLEM_LIST VALUES (?, ?, ?, ?, ?, ?)",
                (problem_id, pat_id, dx_id,
                 _random_date(date(2019, 1, 1), date(2024, 1, 1)),
                 resolved,
                 _random_date(date(2019, 1, 1), date(2024, 1, 1))))
            problem_id += 1

    # PAT_ENC_DX — encounter-level diagnoses
    # Get all encounters
    cur_read = conn.cursor()
    cur_read.execute("SELECT PAT_ENC_CSN_ID, PAT_ID FROM PAT_ENC")
    encounters = cur_read.fetchall()

    for csn_id, pat_id in encounters:
        n_dx = random.randint(1, 3)
        for line in range(1, n_dx + 1):
            if pat_id in diabetic_pats and line == 1 and random.random() < 0.4:
                dx_id = random.choice(diabetes_dx_ids)
            else:
                dx_id = random.choice(other_dx_ids)
            primary = "Y" if line == 1 else "N"
            cur.execute(
                "INSERT OR IGNORE INTO PAT_ENC_DX VALUES (?, ?, ?, ?)",
                (csn_id, line, dx_id, primary))

    conn.commit()


def _populate_hospital_encounters(conn: sqlite3.Connection):
    """Create PAT_ENC_HSP and HSP_ACCOUNT for hospital encounters."""
    cur = conn.cursor()

    # Get encounters with ENC_TYPE_C = 3 (hospital) from ED departments
    cur_read = conn.cursor()
    cur_read.execute("""
        SELECT PAT_ENC_CSN_ID, PAT_ID, CONTACT_DATE
        FROM PAT_ENC WHERE ENC_TYPE_C = 3
    """)
    hospital_encs = cur_read.fetchall()

    hsp_account_id = 1
    for csn_id, pat_id, contact_date in hospital_encs:
        # ~50% of ED encounters result in admission
        if random.random() < 0.5:
            adm_dt = contact_date + "T08:00:00"
            los_hours = random.randint(4, 240)
            disch_dt = (datetime.fromisoformat(contact_date) +
                        timedelta(hours=los_hours)).isoformat()

            cur.execute(
                "INSERT INTO PAT_ENC_HSP VALUES (?, ?, ?, ?, ?)",
                (csn_id, pat_id, adm_dt, disch_dt, 1))

            cur.execute(
                "INSERT INTO HSP_ACCOUNT VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (hsp_account_id, pat_id, csn_id, adm_dt, disch_dt,
                 round(random.uniform(1000, 50000), 2),
                 round(random.uniform(500, 40000), 2),
                 round(random.uniform(0, 5000), 2)))
            hsp_account_id += 1

    conn.commit()


def _populate_admission_diagnoses(conn: sqlite3.Connection, diabetic_pats: set[int]):
    """Populate HSP_ADMIT_DIAG and HSP_ACCT_DX_LIST for hospital stays."""
    cur = conn.cursor()
    diabetes_dx_ids = list(range(1, 16))
    other_dx_ids = list(range(16, 201))

    # HSP_ADMIT_DIAG — diagnoses at admission (from PAT_ENC_HSP)
    cur_read = conn.cursor()
    cur_read.execute("SELECT PAT_ENC_CSN_ID, PAT_ID FROM PAT_ENC_HSP")
    hsp_encs = cur_read.fetchall()

    for csn_id, pat_id in hsp_encs:
        n_dx = random.randint(1, 3)
        for line in range(1, n_dx + 1):
            if pat_id in diabetic_pats and line == 1 and random.random() < 0.5:
                dx_id = random.choice(diabetes_dx_ids)
            else:
                dx_id = random.choice(other_dx_ids)
            cur.execute(
                "INSERT OR IGNORE INTO HSP_ADMIT_DIAG VALUES (?, ?, ?)",
                (csn_id, line, dx_id))

    # HSP_ACCT_DX_LIST — discharge/billing diagnoses (from HSP_ACCOUNT)
    cur_read.execute("SELECT HSP_ACCOUNT_ID, PAT_ID FROM HSP_ACCOUNT")
    accounts = cur_read.fetchall()

    for acct_id, pat_id in accounts:
        n_dx = random.randint(1, 4)
        for line in range(1, n_dx + 1):
            if pat_id in diabetic_pats and line == 1 and random.random() < 0.6:
                dx_id = random.choice(diabetes_dx_ids)
            else:
                dx_id = random.choice(other_dx_ids)
            cur.execute(
                "INSERT OR IGNORE INTO HSP_ACCT_DX_LIST VALUES (?, ?, ?)",
                (acct_id, line, dx_id))

    conn.commit()


def _populate_pcp_assignments(conn: sqlite3.Connection):
    """Assign PCPs to ~80% of patients."""
    cur = conn.cursor()
    pcp_provs = list(range(1, 20))  # first 19 providers are PCPs

    for pat_id in range(1, NUM_PATIENTS + 1):
        if random.random() < 0.8:
            prov_id = random.choice(pcp_provs)
            eff_date = _random_date(date(2018, 1, 1), date(2024, 1, 1))
            cur.execute(
                "INSERT OR IGNORE INTO PAT_PCP VALUES (?, ?, ?, ?)",
                (pat_id, prov_id, eff_date, None))  # TERM_DATE NULL = active

    conn.commit()


def _populate_orders(conn: sqlite3.Connection, diabetic_pats: set[int]):
    """Medication and procedure orders."""
    cur = conn.cursor()

    diabetes_med_ids = [1, 2, 3, 4]  # metformin, glipizide, insulin
    other_med_ids = list(range(5, 31))

    order_med_id = 1
    order_proc_id = 1

    cur_read = conn.cursor()
    cur_read.execute("SELECT PAT_ENC_CSN_ID, PAT_ID FROM PAT_ENC")
    encounters = cur_read.fetchall()

    for csn_id, pat_id in encounters:
        # ~30% of encounters have a med order
        if random.random() < 0.3:
            if pat_id in diabetic_pats and random.random() < 0.5:
                med_id = random.choice(diabetes_med_ids)
            else:
                med_id = random.choice(other_med_ids)
            cur.execute(
                "INSERT INTO ORDER_MED VALUES (?, ?, ?, ?, ?)",
                (order_med_id, pat_id, csn_id, med_id, 2))  # status=2 completed
            order_med_id += 1

        # ~20% of encounters have a proc order
        if random.random() < 0.2:
            proc_id = random.choice(list(range(1, 21)))
            cur.execute(
                "INSERT INTO ORDER_PROC VALUES (?, ?, ?, ?, ?, ?)",
                (order_proc_id, pat_id, csn_id, proc_id,
                 _random_date(ONE_YEAR_AGO, TODAY), 2))
            order_proc_id += 1

    conn.commit()


def _populate_referrals(conn: sqlite3.Connection):
    """Referral records for ~15% of patients."""
    cur = conn.cursor()
    ref_id = 1

    for pat_id in range(1, NUM_PATIENTS + 1):
        if random.random() < 0.15:
            cur.execute(
                "INSERT INTO REFERRAL VALUES (?, ?, ?, ?, ?, ?, ?)",
                (ref_id, pat_id,
                 _random_date(ONE_YEAR_AGO, TODAY),
                 random.choice([1, 2, 3, 6]),  # authorized/open/pending/closed
                 1,
                 random.randint(1, 19),   # PCP provider
                 random.randint(20, 50)))  # specialist provider
            ref_id += 1

    conn.commit()


def _populate_billing(conn: sqlite3.Connection):
    """ARPB_TRANSACTIONS for billing analysis."""
    cur = conn.cursor()
    tx_id = 1

    cur_read = conn.cursor()
    cur_read.execute("""
        SELECT PAT_ENC_CSN_ID, PAT_ID, CONTACT_DATE, DEPARTMENT_ID
        FROM PAT_ENC WHERE APPT_STATUS_C = 2
    """)
    completed = cur_read.fetchall()

    for csn_id, pat_id, contact_date, dept_id in completed:
        # ~40% of completed encounters generate a billing transaction
        if random.random() < 0.4:
            post_date = (datetime.fromisoformat(contact_date) +
                         timedelta(days=random.randint(1, 30))).date().isoformat()
            cur.execute(
                "INSERT INTO ARPB_TRANSACTIONS VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (tx_id, pat_id, contact_date, post_date, 1,
                 round(random.uniform(50, 2000), 2),
                 random.randint(1, 50), dept_id))
            tx_id += 1

    conn.commit()


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def create_mock_db(db_path: str | Path = "mock.db") -> sqlite3.Connection:
    """Create and populate the mock SQLite database.

    Returns the connection (caller should close when done).
    """
    db_path = Path(db_path)
    if db_path.exists():
        db_path.unlink()

    conn = sqlite3.connect(str(db_path))
    conn.executescript(DDL)

    _populate_lookups(conn)
    _populate_dimensions(conn)
    _populate_patients(conn)

    diabetic_pats, diabetic_ed_high, diabetic_ed_high_noshow = \
        _populate_encounters(conn)

    _populate_diagnoses(conn, diabetic_pats)
    _populate_hospital_encounters(conn)
    _populate_admission_diagnoses(conn, diabetic_pats)
    _populate_pcp_assignments(conn)
    _populate_orders(conn, diabetic_pats)
    _populate_referrals(conn)
    _populate_billing(conn)

    print(f"Mock database created: {db_path}")
    print(f"  Patients: {NUM_PATIENTS}")
    print(f"  Diabetic: {len(diabetic_pats)}")

    # Verify key counts
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM PATIENT")
    print(f"  PATIENT rows: {cur.fetchone()[0]}")

    cur.execute("SELECT COUNT(*) FROM PAT_ENC")
    print(f"  PAT_ENC rows: {cur.fetchone()[0]}")

    cur.execute("SELECT COUNT(*) FROM PROBLEM_LIST")
    print(f"  PROBLEM_LIST rows: {cur.fetchone()[0]}")

    # Verify the demo question chain
    cur.execute("""
        SELECT COUNT(DISTINCT p.PAT_ID)
        FROM PATIENT p
        JOIN PROBLEM_LIST pl ON pl.PAT_ID = p.PAT_ID
        JOIN CLARITY_EDG edg ON edg.DX_ID = pl.DX_ID
        WHERE edg.CURRENT_ICD10_LIST LIKE 'E11%'
          AND pl.RESOLVED_DATE IS NULL
    """)
    n_diab = cur.fetchone()[0]
    print(f"  Diabetic patients (query): {n_diab}")

    cur.execute(f"""
        WITH diabetic_pats AS (
            SELECT DISTINCT p.PAT_ID
            FROM PATIENT p
            JOIN PROBLEM_LIST pl ON pl.PAT_ID = p.PAT_ID
            JOIN CLARITY_EDG edg ON edg.DX_ID = pl.DX_ID
            WHERE edg.CURRENT_ICD10_LIST LIKE 'E11%'
              AND pl.RESOLVED_DATE IS NULL
        ),
        er_visits AS (
            SELECT enc.PAT_ID, COUNT(*) as visit_count
            FROM PAT_ENC enc
            JOIN CLARITY_DEP dep ON dep.DEPARTMENT_ID = enc.DEPARTMENT_ID
            WHERE dep.DEPARTMENT_NAME LIKE '%Emergency%'
              AND enc.CONTACT_DATE >= '{ONE_YEAR_AGO.isoformat()}'
              AND enc.PAT_ID IN (SELECT PAT_ID FROM diabetic_pats)
            GROUP BY enc.PAT_ID
            HAVING COUNT(*) > 3
        )
        SELECT COUNT(*) FROM er_visits
    """)
    n_ed = cur.fetchone()[0]
    print(f"  Diabetic + 4+ ED visits: {n_ed}")

    cur.execute(f"""
        WITH diabetic_pats AS (
            SELECT DISTINCT p.PAT_ID
            FROM PATIENT p
            JOIN PROBLEM_LIST pl ON pl.PAT_ID = p.PAT_ID
            JOIN CLARITY_EDG edg ON edg.DX_ID = pl.DX_ID
            WHERE edg.CURRENT_ICD10_LIST LIKE 'E11%'
              AND pl.RESOLVED_DATE IS NULL
        ),
        er_high AS (
            SELECT enc.PAT_ID
            FROM PAT_ENC enc
            JOIN CLARITY_DEP dep ON dep.DEPARTMENT_ID = enc.DEPARTMENT_ID
            WHERE dep.DEPARTMENT_NAME LIKE '%Emergency%'
              AND enc.CONTACT_DATE >= '{ONE_YEAR_AGO.isoformat()}'
              AND enc.PAT_ID IN (SELECT PAT_ID FROM diabetic_pats)
            GROUP BY enc.PAT_ID
            HAVING COUNT(*) > 3
        ),
        missed_pcp AS (
            SELECT DISTINCT enc.PAT_ID
            FROM PAT_ENC enc
            JOIN CLARITY_DEP dep ON dep.DEPARTMENT_ID = enc.DEPARTMENT_ID
            WHERE dep.SPECIALTY = 'Family Medicine'
              AND enc.APPT_STATUS_C = 4
              AND enc.CONTACT_DATE >= '{SIX_MONTHS_AGO.isoformat()}'
              AND enc.PAT_ID IN (SELECT PAT_ID FROM er_high)
        )
        SELECT COUNT(*) FROM missed_pcp
    """)
    n_noshow = cur.fetchone()[0]
    print(f"  Diabetic + 4+ ED + PCP no-show: {n_noshow}")

    return conn


if __name__ == "__main__":
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else "mock.db"
    conn = create_mock_db(path)
    conn.close()
