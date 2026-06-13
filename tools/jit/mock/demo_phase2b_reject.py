#!/usr/bin/env python3
"""Phase 2b Demo — User rejects a definition and modifies the logic.

Scenario: User wants "missed PCP visit" to mean "no appointments at all
in the past 6 months" instead of "had a no-show appointment."

This exercises:
1. L2 search finds missed_pcp_visits (no-show definition)
2. User rejects: "that's not what I mean"
3. User describes what they want: "no PCP appointments at all"
4. System has no matching definition → falls to L3 (technical search)
5. Term resolver identifies "PCP appointment" → encounter domain
6. Route catalog suggests PAT_ENC + CLARITY_DEP
7. User and system build a new definition from scratch (L4)
8. New definition is saved to the glossary for future use
"""

from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path

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


def main():
    from tools.jit.search_definitions import DefinitionSearcher
    from tools.jit.search_technical import TechnicalSearcher
    from tools.jit.term_resolver import resolve_term
    from tools.jit.quantifier_extractor import extract_all
    from tools.jit.step_builder import build_step_plan
    from tools.jit.mock.db_executor import execute_sql, execute_count
    from tools.jit.mock.mock_definitions import DEFINITIONS

    defn_searcher = DefinitionSearcher()
    tech_searcher = TechnicalSearcher()
    conn = sqlite3.connect(str(DB_PATH))

    question = ("How many percent of diabetic patients who have been to the ER "
                "more than 3 times last year have missed their PCP visit in "
                "the last 6 months?")

    section("CONTEXT: Steps 1-2 already approved")
    print("  Step 1: Diabetic patients → 90 patients  [APPROVED]")
    print("  Step 2: ED high utilizers → 45 patients   [APPROVED]")
    print()
    print("  Now at Step 3: Missed PCP visits")

    # =====================================================================
    # L2: System proposes the no-show definition
    # =====================================================================
    section("STEP 3: System proposes 'Missed PCP visits'")

    hits = defn_searcher.search("missed PCP visit")
    best = next((h for h in hits if h.definition_name == "missed_pcp_visits"), None)

    if best:
        print(f"  Definition: {best.label}")
        print(f"  Description: {best.description}")
        print()
        print("  Characteristic filters:")
        for f in best.characteristic_filters:
            print(f"    - {f['english']}")
            print(f"      SQL: {f['expression']}")
        print()
        print("  SQL template:")
        for line in best.sql_template.split("\n"):
            print(f"    {line}")

    # =====================================================================
    # User rejects
    # =====================================================================
    section("USER REJECTS")
    print('  User: "That\'s not what I mean. No-show means they came to the')
    print('         appointment but didn\'t show up. I want patients who have')
    print('         NO PCP appointments at all in the past 6 months —')
    print('         they never even scheduled one."')
    print()
    print("  This is a fundamentally different concept:")
    print("    - EXISTING: APPT_STATUS_C = 4 (had appt, didn't show)")
    print("    - WANTED:   NOT EXISTS any PCP encounter in 6 months")

    # =====================================================================
    # System acknowledges the gap
    # =====================================================================
    section("SYSTEM: No matching definition found")
    print("  Searching definitions for 'no PCP appointments'...")
    print()

    alt_hits = defn_searcher.search("no PCP appointments scheduled")
    print(f"  Found {len(alt_hits)} matches:")
    for h in alt_hits[:3]:
        print(f"    {h.definition_name:40s} score={h.score:.3f} [{h.strength}]")
        print(f"      {h.label}")
    print()
    print("  None of these match 'no appointments at all.'")
    print("  → Escalate to L3: Technical Search")

    # =====================================================================
    # L3: Term resolution + route suggestion
    # =====================================================================
    subsection("L3: Term Resolution")
    resolution = resolve_term("PCP appointment", context="patients with no PCP appointments")
    print(f"  Term: {resolution.term}")
    print(f"  Category: {resolution.category}")
    print(f"  Confidence: {resolution.confidence}")
    print(f"  Source: {resolution.source}")
    print()

    # Show encounter routes
    routes = tech_searcher.get_routes_for_category("encounter")
    print("  Relevant routes for 'encounter':")
    for route in routes:
        print(f"    [{route.route_name}]")
        print(f"      Path: {' → '.join(route.path)}")
        print(f"      {route.description}")
        print()

    print("  >>> USER ACTION: Picks 'All encounters' route")
    print("    Path: PATIENT → PAT_ENC")

    # =====================================================================
    # L4: Build from scratch — user defines the logic
    # =====================================================================
    section("L4: BUILD FROM SCRATCH")
    print("  System: 'Let's build this step by step.'")
    print()
    print("  The concept is: patients who have NO PCP encounter in 6 months.")
    print("  This is a NOT EXISTS / LEFT JOIN IS NULL pattern.")
    print()

    subsection("User describes the logic")
    print('  User: "I want patients where there is NO encounter at a')
    print('         Family Medicine department in the last 6 months."')
    print()
    print("  System interprets:")
    print("    - Table: PAT_ENC (encounters)")
    print("    - Join: CLARITY_DEP (to filter by specialty)")
    print("    - Filter: CLARITY_DEP.SPECIALTY = 'Family Medicine'")
    print("    - Date: CONTACT_DATE >= 6 months ago")
    print("    - Logic: NOT EXISTS (these patients should be EXCLUDED)")

    subsection("System generates SQL")
    # This is what the system would generate for "no PCP visits"
    no_pcp_sql = """\
SELECT DISTINCT PAT_ID
FROM PATIENT
WHERE PAT_ID NOT IN (
    SELECT DISTINCT enc.PAT_ID
    FROM PAT_ENC enc
    JOIN CLARITY_DEP dep ON dep.DEPARTMENT_ID = enc.DEPARTMENT_ID
    WHERE dep.SPECIALTY = 'Family Medicine'
      AND enc.CONTACT_DATE >= '2025-12-11'
      AND enc.APPT_STATUS_C IN (1, 2, 6)
)"""

    print("  Generated SQL:")
    for line in no_pcp_sql.split("\n"):
        print(f"    {line}")
    print()
    print("  Note: APPT_STATUS_C IN (1, 2, 6) = Scheduled, Completed, Arrived")
    print("  This means: patients who don't have ANY scheduled/completed/arrived")
    print("  PCP appointment in 6 months. They may have canceled or no-showed,")
    print("  but those don't count as 'having a PCP visit.'")
    print()
    print("  >>> USER ACTION: 'Actually, I want to exclude ALL statuses.")
    print("       If they had any PCP encounter at all — even canceled — they're out.'")

    subsection("User modifies SQL")
    no_pcp_sql_v2 = """\
SELECT DISTINCT PAT_ID
FROM PATIENT
WHERE PAT_ID NOT IN (
    SELECT DISTINCT enc.PAT_ID
    FROM PAT_ENC enc
    JOIN CLARITY_DEP dep ON dep.DEPARTMENT_ID = enc.DEPARTMENT_ID
    WHERE dep.SPECIALTY = 'Family Medicine'
      AND enc.CONTACT_DATE >= '2025-12-11'
)"""

    print("  Modified SQL (removed status filter):")
    for line in no_pcp_sql_v2.split("\n"):
        print(f"    {line}")
    print()
    print("  >>> USER ACTION: [Run]")

    # Execute standalone to see how many patients have NO PCP visits
    count_sql = f"""\
SELECT COUNT(DISTINCT PAT_ID) FROM PATIENT
WHERE PAT_ID NOT IN (
    SELECT DISTINCT enc.PAT_ID
    FROM PAT_ENC enc
    JOIN CLARITY_DEP dep ON dep.DEPARTMENT_ID = enc.DEPARTMENT_ID
    WHERE dep.SPECIALTY = 'Family Medicine'
      AND enc.CONTACT_DATE >= '2025-12-11'
)"""
    no_pcp_count = execute_count(conn, count_sql)
    print(f"  Result: {no_pcp_count} patients have NO PCP encounter in past 6 months")
    print(f"  (out of 500 total)")

    # =====================================================================
    # Now chain it with the prior steps
    # =====================================================================
    subsection("Chain with prior steps (diabetic + ED high)")

    full_sql = f"""\
WITH diabetic_pats AS (
    SELECT DISTINCT p.PAT_ID
    FROM PATIENT p
    JOIN PROBLEM_LIST pl ON pl.PAT_ID = p.PAT_ID
    JOIN CLARITY_EDG edg ON edg.DX_ID = pl.DX_ID
    WHERE edg.CURRENT_ICD10_LIST LIKE 'E11%'
      AND pl.RESOLVED_DATE IS NULL
),
er_high AS (
    SELECT enc.PAT_ID, COUNT(*) AS visit_count
    FROM PAT_ENC enc
    JOIN CLARITY_DEP dep ON dep.DEPARTMENT_ID = enc.DEPARTMENT_ID
    WHERE dep.DEPARTMENT_NAME LIKE '%Emergency%'
      AND enc.CONTACT_DATE >= '2025-06-11'
      AND enc.PAT_ID IN (SELECT PAT_ID FROM diabetic_pats)
    GROUP BY enc.PAT_ID
    HAVING COUNT(*) > 3
),
no_pcp_visits AS (
    SELECT DISTINCT PAT_ID
    FROM er_high
    WHERE PAT_ID NOT IN (
        SELECT DISTINCT enc.PAT_ID
        FROM PAT_ENC enc
        JOIN CLARITY_DEP dep ON dep.DEPARTMENT_ID = enc.DEPARTMENT_ID
        WHERE dep.SPECIALTY = 'Family Medicine'
          AND enc.CONTACT_DATE >= '2025-12-11'
    )
)
SELECT COUNT(*) FROM no_pcp_visits"""

    print("  Full chained SQL:")
    for line in full_sql.split("\n"):
        print(f"    {line}")
    print()
    print("  >>> USER ACTION: [Run]")
    print()

    result = execute_count(conn, full_sql)
    print(f"  Result: {result} patients")
    print(f"  (diabetic=90 → ED high=45 → no PCP at all={result})")

    # Compare with the original no-show definition
    original_sql = f"""\
WITH diabetic_pats AS (
    SELECT DISTINCT p.PAT_ID
    FROM PATIENT p
    JOIN PROBLEM_LIST pl ON pl.PAT_ID = p.PAT_ID
    JOIN CLARITY_EDG edg ON edg.DX_ID = pl.DX_ID
    WHERE edg.CURRENT_ICD10_LIST LIKE 'E11%'
      AND pl.RESOLVED_DATE IS NULL
),
er_high AS (
    SELECT enc.PAT_ID, COUNT(*) AS visit_count
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
SELECT COUNT(*) FROM missed_pcp"""
    original_result = execute_count(conn, original_sql)

    subsection("Comparison: Original vs. Modified Definition")
    print(f"  Original (no-show):        {original_result} patients")
    print(f"  Modified (no appts at all): {result} patients")
    print()
    if result != original_result:
        print(f"  These are DIFFERENT populations. The definition matters.")
        print(f"  Original: patients who scheduled but didn't show up")
        print(f"  Modified: patients who never even scheduled")
    else:
        print(f"  Same count, but conceptually different populations.")

    # =====================================================================
    # Save new definition to glossary
    # =====================================================================
    section("SAVE NEW DEFINITION")
    print("  >>> USER ACTION: [Save as Definition]")
    print()

    new_definition = {
        "definition_name": "no_pcp_visits_at_all",
        "label": "No PCP visits at all (past 6 months)",
        "description": "Patients who have no PCP/Family Medicine encounter of any "
                       "status in the specified lookback period. Unlike 'no-show', "
                       "these patients never even scheduled an appointment.",
        "domain": "encounters",
        "backbone": {
            "anchor_table": "PATIENT",
            "tables": ["PATIENT", "PAT_ENC", "CLARITY_DEP"],
            "joins": [],
            "characteristic_filters": [
                {
                    "expression": "PAT_ID NOT IN (SELECT enc.PAT_ID FROM PAT_ENC enc "
                                  "JOIN CLARITY_DEP dep ON dep.DEPARTMENT_ID = enc.DEPARTMENT_ID "
                                  "WHERE dep.SPECIALTY = 'Family Medicine')",
                    "english": "No PCP/Family Medicine encounters at all",
                    "is_definitional": True,
                },
            ],
            "output_grain": "patient",
        },
        "parameters": [
            {"name": "lookback_months", "default": 6, "type": "integer",
             "description": "Months to look back for absence of PCP visits"},
        ],
        "source_reports": [],
        "source_scopes": [],
        "sql_template": no_pcp_sql_v2,
        "validated_by": ["sunny"],
        "validation_count": 1,
        "usage_count": 1,
        "created_by_session": True,
    }

    print("  New definition saved:")
    print(f"    Name: {new_definition['definition_name']}")
    print(f"    Label: {new_definition['label']}")
    print(f"    Description: {new_definition['description'][:80]}...")
    print()
    print("  Filters:")
    for f in new_definition["backbone"]["characteristic_filters"]:
        print(f"    - {f['english']}")
    print()
    print("  This definition is now in the glossary.")
    print("  Next time someone asks about 'patients with no PCP visits',")
    print("  the system will offer BOTH definitions:")
    print()
    print("    [1] Missed PCP visits (no-show)")
    print("        'Patients who had a no-show at Family Medicine'")
    print("        Validated by: 0 users")
    print()
    print("    [2] No PCP visits at all (past 6 months)  ← NEW")
    print("        'Patients with no PCP encounter of any status'")
    print("        Validated by: 1 user (sunny)")
    print()
    print("  The user can see both options and pick the one that")
    print("  matches their intent. Over time, validation counts")
    print("  show which definition is more popular.")

    # =====================================================================
    # Also update learned terms
    # =====================================================================
    subsection("Update learned terms")
    print("  New learned term entry:")
    print("    term: 'no PCP visits'")
    print("    aliases: ['no PCP appointments', 'never seen PCP',")
    print("              'no family medicine visits']")
    print("    category: encounter")
    print("    source_definition: no_pcp_visits_at_all")
    print()
    print("  Existing term 'no-show' is preserved (different concept).")

    # =====================================================================
    # Final result with new definition
    # =====================================================================
    section("FINAL RESULT (with modified definition)")

    n_diabetic = 90
    n_ed = 45
    pct = result / n_diabetic * 100

    print(f"  {result} / {n_diabetic} = {pct:.1f}%")
    print()
    print("  Funnel:")
    print(f"    {500:>5} total patients")
    print(f"    {n_diabetic:>5} with active diabetes")
    print(f"    {n_ed:>5} with >3 ER visits past year")
    print(f"    {result:>5} with NO PCP visits at all in 6 months")
    print()
    print(f"  Answer: {pct:.1f}% of diabetic patients who have been to the ER")
    print(f"  more than 3 times last year have had NO PCP visit at all in")
    print(f"  the last 6 months.")
    print()
    print(f"  (Compare: original 'no-show' definition gave {original_result} patients,")
    print(f"   or {original_result/n_diabetic*100:.1f}%)")

    conn.close()


if __name__ == "__main__":
    main()
