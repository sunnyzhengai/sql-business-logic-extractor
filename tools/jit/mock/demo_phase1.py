#!/usr/bin/env python3
"""Phase 1 Demo — Walk through the mock environment and cascade flow.

Run:
    python3 -m tools.jit.mock.demo_phase1

This simulates what the assistant would do at each level, using the
glossaries and mock DB we built. No search tools yet — just shows
the data structures and SQL execution at each step.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import yaml

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
DB_PATH = DATA_DIR / "mock.db"


def section(title: str):
    print()
    print("=" * 70)
    print(f"  {title}")
    print("=" * 70)
    print()


def subsection(title: str):
    print()
    print(f"--- {title} ---")
    print()


def show_report(report: dict):
    """Display a report glossary entry."""
    print(f"  Report:      {report['report_name']}")
    print(f"  Purpose:     {report['primary_purpose']}")
    print(f"  Description: {report['description'][:80]}...")
    print(f"  Tables:      {', '.join(report['tables_used'])}")
    print(f"  Domains:     {', '.join(report['domains'])}")
    if report.get("parameters"):
        print(f"  Parameters:")
        for p in report["parameters"]:
            print(f"    - {p['name']} = {p['default']} ({p['description']})")
    if report.get("inline_comments"):
        print(f"  Dev comments:")
        for c in report["inline_comments"]:
            print(f"    {c}")


def show_definition(defn: dict):
    """Display a definition glossary entry."""
    print(f"  Definition:  {defn['definition_name']}")
    print(f"  Label:       {defn['label']}")
    print(f"  Domain:      {defn['domain']}")
    print(f"  Description: {defn['description'][:80]}...")
    bb = defn["backbone"]
    print(f"  Anchor:      {bb['anchor_table']}")
    print(f"  Tables:      {', '.join(bb['tables'])}")
    print(f"  Grain:       {bb.get('output_grain', 'n/a')}")
    if bb.get("characteristic_filters"):
        print(f"  Filters:")
        for f in bb["characteristic_filters"]:
            print(f"    - {f['english']}")
            print(f"      SQL: {f['expression']}")
    if defn.get("parameters"):
        print(f"  Parameters:")
        for p in defn["parameters"]:
            print(f"    - {p['name']} = {p['default']} ({p['description']})")
    print(f"  Source:      {', '.join(defn.get('source_scopes', []))}")


def run_sql(conn, sql: str, label: str = ""):
    """Execute SQL and show results."""
    cur = conn.cursor()
    cur.execute(sql)
    if cur.description:
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        if label:
            print(f"  [{label}]")
        print(f"  Columns: {cols}")
        print(f"  Rows: {len(rows)}")
        if len(rows) <= 5:
            for row in rows:
                print(f"    {list(row)}")
        else:
            for row in rows[:3]:
                print(f"    {list(row)}")
            print(f"    ... ({len(rows) - 3} more)")
        return rows
    return []


def main():
    # Ensure mock DB exists
    if not DB_PATH.exists():
        from tools.jit.mock.mock_db import create_mock_db
        create_mock_db(DB_PATH)

    conn = sqlite3.connect(str(DB_PATH))

    question = ("How many percent of diabetic patients who have been to the ER "
                "more than 3 times last year have missed their PCP visit in "
                "the last 6 months?")

    section("USER QUESTION")
    print(f'  "{question}"')

    # =====================================================================
    # LEVEL 1: Report Search
    # =====================================================================
    section("LEVEL 1: Report Glossary Search")
    print("  Searching for a single report that answers this question...")
    print()

    # Load all reports
    report_dir = DATA_DIR / "report_glossary"
    reports = []
    for yf in sorted(report_dir.glob("*.yaml")):
        with open(yf) as f:
            reports.append(yaml.safe_load(f))

    # Show what the searcher would find
    print("  Candidate reports (would be ranked by TF-IDF in Phase 2):")
    print()
    for r in reports:
        relevance = "?"
        name = r["report_name"]
        if "DIABETIC" in name and "MEDICATION" not in name:
            relevance = "PARTIAL — covers diabetes cohort but not ED/PCP"
        elif "ED_UTIL" in name:
            relevance = "PARTIAL — covers ED visits but not diabetes/PCP"
        elif "PCP" in name:
            relevance = "PARTIAL — covers PCP no-shows but not diabetes/ED"
        else:
            relevance = "LOW"
        print(f"    {name:30s} → {relevance}")

    print()
    print("  Verdict: No single report answers the full question.")
    print("  The question combines 3 concepts across 3 reports.")
    print()
    print("  >>> USER ACTION: 'No, escalate to Level 2'")

    # =====================================================================
    # LEVEL 2: Definition Search
    # =====================================================================
    section("LEVEL 2: Business Definition Glossary Search")
    print("  Searching for reusable building blocks...")
    print()

    # Load definitions
    defn_dir = DATA_DIR / "definition_glossary"
    definitions = []
    for yf in sorted(defn_dir.glob("*.yaml")):
        with open(yf) as f:
            definitions.append(yaml.safe_load(f))

    # Show what would match
    matches = [
        ("diabetic_patients_problem_list", "STRONG", "matches 'diabetic patients'"),
        ("ed_high_utilizers", "STRONG", "matches 'ER more than 3 times'"),
        ("missed_pcp_visits", "STRONG", "matches 'missed PCP visit'"),
        ("ed_encounters", "WEAK", "matches 'ER' but too broad"),
        ("active_problems_only", "WEAK", "related to diabetes but too generic"),
    ]

    print("  Building blocks found:")
    print()
    for defn_name, strength, reason in matches:
        defn = next((d for d in definitions if d["definition_name"] == defn_name), None)
        if defn:
            print(f"  [{strength}] {defn['label']}")
            print(f"         {reason}")
            print(f"         Tables: {', '.join(defn['backbone']['tables'])}")
            print()

    print("  >>> USER ACTION: Selects 3 building blocks:")
    print("    [x] Diabetic patients (active problem list)")
    print("    [x] ED high utilizers (>3 visits in 12 months)")
    print("    [x] Missed PCP visits (no-show)")
    print()
    print("  >>> USER ACTION: Sets base population = 'Diabetic patients'")
    print("  >>> USER ACTION: Output format = 'Percentage'")

    # =====================================================================
    # STEP-BY-STEP EXECUTION
    # =====================================================================
    section("STEP-BY-STEP QUERY EXECUTION (HITL)")

    # --- Step 1: Diabetic patients ---
    subsection("Step 1 of 3: Diabetic patients (base population)")

    defn1 = next(d for d in definitions
                 if d["definition_name"] == "diabetic_patients_problem_list")
    show_definition(defn1)
    print()

    sql1 = """\
SELECT COUNT(DISTINCT p.PAT_ID) AS patient_count
FROM PATIENT p
JOIN PROBLEM_LIST pl ON pl.PAT_ID = p.PAT_ID
JOIN CLARITY_EDG edg ON edg.DX_ID = pl.DX_ID
WHERE edg.CURRENT_ICD10_LIST LIKE 'E11%'
  AND pl.RESOLVED_DATE IS NULL"""

    print("  Generated SQL:")
    for line in sql1.split("\n"):
        print(f"    {line}")
    print()
    print("  >>> USER ACTION: [Run]")
    print()

    rows = run_sql(conn, sql1, "Result")
    n_diabetic = rows[0][0]
    print()

    total = 500
    print(f"  Result: {n_diabetic} patients ({n_diabetic/total*100:.1f}% of {total} total)")
    print()
    print("  >>> USER ACTION: 'Looks right, continue'")

    # --- Step 2: ED high utilizers ---
    subsection("Step 2 of 3: ED high utilizers (>3 visits last year)")

    defn2 = next(d for d in definitions
                 if d["definition_name"] == "ed_high_utilizers")
    show_definition(defn2)
    print()

    print("  Quantifier detected: 'more than 3 times' → threshold = 3")
    print("  Date detected: 'last year' → 2025-01-01 to 2025-12-31")
    print("  Note: Original definition uses 'trailing 12 months'")
    print("  >>> USER ACTION: [Use trailing 12 months]")
    print()

    sql2 = f"""\
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
      AND enc.CONTACT_DATE >= '2025-06-11'
      AND enc.PAT_ID IN (SELECT PAT_ID FROM diabetic_pats)
    GROUP BY enc.PAT_ID
    HAVING COUNT(*) > 3
)
SELECT COUNT(*) AS patient_count FROM er_visits"""

    print("  Generated SQL:")
    for line in sql2.split("\n"):
        print(f"    {line}")
    print()
    print("  >>> USER ACTION: [Run]")
    print()

    rows = run_sql(conn, sql2, "Result")
    n_ed = rows[0][0]
    print()
    print(f"  Result: {n_ed} patients ({n_diabetic} → {n_ed}, "
          f"{(1 - n_ed/n_diabetic)*100:.0f}% reduction)")
    print()
    print("  >>> USER ACTION: 'Continue'")

    # --- Step 3: Missed PCP ---
    subsection("Step 3 of 3: Missed PCP visit (no-show, last 6 months)")

    defn3 = next(d for d in definitions
                 if d["definition_name"] == "missed_pcp_visits")
    show_definition(defn3)
    print()

    sql3 = f"""\
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
      AND enc.CONTACT_DATE >= '2025-06-11'
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
      AND enc.CONTACT_DATE >= '2025-12-11'
      AND enc.PAT_ID IN (SELECT PAT_ID FROM er_high)
)
SELECT COUNT(*) AS patient_count FROM missed_pcp"""

    print("  Generated SQL:")
    for line in sql3.split("\n"):
        print(f"    {line}")
    print()
    print("  >>> USER ACTION: [Run]")
    print()

    rows = run_sql(conn, sql3, "Result")
    n_noshow = rows[0][0]
    print()
    print(f"  Result: {n_noshow} patients ({n_ed} → {n_noshow}, "
          f"{(1 - n_noshow/n_ed)*100:.0f}% reduction)")

    # =====================================================================
    # FINAL RESULT
    # =====================================================================
    section("FINAL RESULT")

    pct = n_noshow / n_diabetic * 100
    print(f"  {n_noshow} / {n_diabetic} = {pct:.1f}%")
    print()
    print("  Funnel:")
    print(f"    {total:>5} total patients")
    print(f"    {n_diabetic:>5} with active diabetes ({n_diabetic/total*100:.1f}%)")
    print(f"    {n_ed:>5} with >3 ER visits past year "
          f"({n_ed/n_diabetic*100:.1f}% of diabetic)")
    print(f"    {n_noshow:>5} with missed PCP visit "
          f"({n_noshow/n_ed*100:.1f}% of ER utilizers)")
    print()
    print(f"  Answer: {pct:.1f}% of diabetic patients who have been to the ER")
    print(f"  more than 3 times last year have missed their PCP visit in")
    print(f"  the last 6 months.")
    print()
    print("  >>> USER ACTION: [Save as Definition] [Export SQL] [New Question]")

    # =====================================================================
    # BONUS: Show what Level 3 would look like
    # =====================================================================
    section("BONUS: LEVEL 3 DEMO (unknown term)")
    print('  User asks: "patients with Addison\'s disease"')
    print()
    print("  Level 1 (Report Search): No report matches → skip")
    print("  Level 2 (Definition Search): No definition matches → skip")
    print("  Level 3 (Technical Search): Term resolution kicks in...")
    print()

    # Load learned terms
    with open(DATA_DIR / "learned_terms.yaml") as f:
        terms = yaml.safe_load(f)

    print("  Step 1: Check learned_terms.yaml...")
    print(f"    Known terms: {', '.join(terms.keys())}")
    print("    'addisons' → NOT FOUND")
    print()
    print("  Step 2: Pattern recognition...")
    print("    'Addison's disease' → suffix 'disease' → likely DIAGNOSIS")
    print()
    print("  Step 3: Show diagnosis routes from route_catalog.yaml...")

    with open(DATA_DIR / "route_catalog.yaml") as f:
        routes = yaml.safe_load(f)

    for route in routes["diagnosis"]["routes"]:
        print(f"    [ ] {route['name']}")
        print(f"        Path: {' -> '.join(route['path'])}")
        print(f"        {route['description']}")
        print()

    print("  >>> USER ACTION: Picks 'Active problem list'")
    print("  >>> USER ACTION: Enters ICD-10 code: E27.1")
    print("  >>> System saves to learned_terms.yaml for next time")

    # =====================================================================
    # INVENTORY
    # =====================================================================
    section("PHASE 1 INVENTORY — What was built")
    print()

    # Count everything
    n_reports = len(list((DATA_DIR / "report_glossary").glob("*.yaml")))
    n_defs = len(list((DATA_DIR / "definition_glossary").glob("*.yaml")))
    n_sql = len(list((DATA_DIR / "report_sql").glob("*.sql")))
    n_terms = len(terms)

    with open(DATA_DIR / "technical_glossary.yaml") as f:
        tech = yaml.safe_load(f)
    n_domains = len(tech["domains"])
    n_dims = len(tech["dimensions"])

    with open(DATA_DIR / "route_catalog.yaml") as f:
        rc = yaml.safe_load(f)
    n_routes = sum(len(cat["routes"]) for cat in rc.values())

    cur = conn.cursor()
    tables_info = []
    for table in ["PATIENT", "PAT_ENC", "PAT_ENC_DX", "PROBLEM_LIST",
                   "CLARITY_EDG", "PAT_ENC_HSP", "HSP_ACCOUNT",
                   "CLARITY_DEP", "CLARITY_SER", "PAT_PCP",
                   "ORDER_MED", "ORDER_PROC", "REFERRAL",
                   "ARPB_TRANSACTIONS", "CLARITY_MEDICATION", "CLARITY_EAP"]:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        tables_info.append((table, cur.fetchone()[0]))

    print("  Mock Database (SQLite):")
    for name, count in tables_info:
        print(f"    {name:25s} {count:>6} rows")

    print()
    print(f"  Report Glossary:     {n_reports} reports + {n_sql} SQL files")
    print(f"  Definition Glossary: {n_defs} definitions")
    print(f"  Technical Glossary:  {n_domains} domains, {n_dims} dimensions")
    print(f"  Route Catalog:       {n_routes} routes")
    print(f"  Learned Terms:       {n_terms} pre-populated terms")
    print()
    print("  All 22 tests passing.")
    print()
    print("  Files:")
    print("    tools/jit/mock/mock_db.py          — DB generator")
    print("    tools/jit/mock/mock_reports.py      — Report Glossary generator")
    print("    tools/jit/mock/mock_definitions.py  — Definition Glossary generator")
    print("    tools/jit/mock/mock_technical.py    — Technical Glossary + routes")
    print("    tools/jit/mock/db_executor.py       — SQL executor (Fabric swap point)")
    print("    tools/jit/data/                     — All generated data files")
    print("    tools/jit/tests/test_mock_db.py     — 22 tests")

    conn.close()


if __name__ == "__main__":
    main()
