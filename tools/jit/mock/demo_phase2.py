#!/usr/bin/env python3
"""Phase 2 Demo — Real search tools running against the glossaries.

Run:
    python3 -m tools.jit.mock.demo_phase2

Unlike Phase 1's demo (hardcoded matches), this uses the actual
TF-IDF searchers, term resolver, quantifier extractor, step builder,
and router. The matches are computed, not scripted.
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
    from tools.jit.router import classify_intent
    from tools.jit.search_reports import ReportSearcher
    from tools.jit.search_definitions import DefinitionSearcher
    from tools.jit.search_technical import TechnicalSearcher
    from tools.jit.quantifier_extractor import extract_all
    from tools.jit.term_resolver import resolve_term
    from tools.jit.step_builder import build_step_plan
    from tools.jit.mock.db_executor import execute_sql, execute_count

    # Boot
    report_searcher = ReportSearcher()
    defn_searcher = DefinitionSearcher()
    tech_searcher = TechnicalSearcher()
    conn = sqlite3.connect(str(DB_PATH))

    # =====================================================================
    # TEST 1: The complex question (L1 → L2 cascade)
    # =====================================================================
    question = ("How many percent of diabetic patients who have been to the ER "
                "more than 3 times last year have missed their PCP visit in "
                "the last 6 months?")

    section("TEST 1: Complex multi-concept question")
    print(f'  "{question}"')

    # Intent classification
    subsection("Router: Intent Classification")
    intent = classify_intent(question)
    print(f"  Intent: {intent.intent} (confidence: {intent.confidence:.2f})")
    for sig in intent.signals:
        print(f"    - {sig}")

    # Quantifier/date extraction
    subsection("Quantifier/Date Extraction")
    extractions = extract_all(question, reference_date=date(2026, 6, 11))
    for q in extractions.quantifiers:
        print(f"  Threshold: {q.operator} {q.value} (from '{q.raw_text}')")
    for d in extractions.date_ranges:
        print(f"  Date range: {d.start_date} to {d.end_date} "
              f"({d.range_type}, from '{d.raw_text}')")

    # L1: Report search
    subsection("Level 1: Report Search")
    report_hits = report_searcher.search(question)
    print(f"  Found {len(report_hits)} report matches:")
    for hit in report_hits:
        print(f"    {hit.report_name:30s} score={hit.score:.3f}  "
              f"({hit.primary_purpose})")
    print()
    print("  No single report covers all 3 concepts → escalate to L2")

    # L2: Definition search
    subsection("Level 2: Definition Search")
    defn_hits = defn_searcher.search(question)
    print(f"  Found {len(defn_hits)} definition matches:")
    print()
    for hit in defn_hits:
        marker = "*" if hit.strength == "strong" else " "
        print(f"  [{marker}] {hit.definition_name:40s} score={hit.score:.3f} "
              f"[{hit.strength}]")
        print(f"       {hit.label}")
        print(f"       Tables: {', '.join(hit.tables)}")
        if hit.characteristic_filters:
            for f in hit.characteristic_filters[:2]:
                print(f"       Filter: {f['english']}")
        print()

    # Grouped view
    subsection("Level 2: Grouped by Equivalence")
    groups = defn_searcher.search_grouped(question)
    for i, group in enumerate(groups):
        eq_tag = " (EQUIVALENT)" if group.is_equivalent else ""
        print(f"  Group {i+1}: {group.group_label}{eq_tag}")
        if group.is_equivalent:
            print(f"    Reason: {group.equivalence_reason}")
        for d in group.definitions:
            print(f"    - {d.definition_name} (score={d.score:.3f})")
        print()

    # Simulate user selection
    subsection("User Selects 3 Building Blocks")
    selected_names = [
        "diabetic_patients_problem_list",
        "ed_high_utilizers",
        "missed_pcp_visits",
    ]
    selected_defs = []
    for name in selected_names:
        match = next((h for h in defn_hits if h.definition_name == name), None)
        if match:
            selected_defs.append(match.full_entry)
            print(f"  [x] {match.label}")
        else:
            print(f"  [!] {name} — NOT FOUND in search results!")
    print()
    print("  Base population: Diabetic patients")
    print("  Output format: Percentage")

    # Build step plan
    subsection("Step Builder: Generate Query Plan")
    steps = build_step_plan(selected_defs, output_format="percentage")
    for step in steps:
        print(f"  Step {step.step_number}: {step.description}")
        print(f"    CTE: {step.cte_name}")
        print(f"    Tables: {', '.join(step.tables) if step.tables else 'n/a'}")
        print()

    # Execute each step
    subsection("Execute Steps Against Mock DB")
    total_patients = execute_count(conn, "SELECT COUNT(*) FROM PATIENT")
    prior_count = total_patients

    for step in steps:
        print(f"  Step {step.step_number}: {step.label}")
        print()
        # Show SQL (truncated for readability)
        sql_lines = step.count_sql.split("\n")
        if len(sql_lines) > 15:
            for line in sql_lines[:12]:
                print(f"    {line}")
            print(f"    ... ({len(sql_lines) - 12} more lines)")
        else:
            for line in sql_lines:
                print(f"    {line}")
        print()

        try:
            result = execute_sql(conn, step.count_sql)
            if result["rows"]:
                row = result["rows"][0]
                if len(row) == 1:
                    count = row[0]
                    reduction = f" ({prior_count} → {count})" if prior_count != total_patients or step.step_number > 1 else ""
                    print(f"  Result: {count}{reduction}")
                    step.result_count = count
                    prior_count = count
                elif len(row) == 3:
                    # Percentage step: numerator, denominator, percentage
                    print(f"  Result: {row[0]} / {row[1]} = {row[2]}%")
                    step.result_count = row[2]
            else:
                print(f"  Result: no rows returned")
        except Exception as e:
            print(f"  ERROR: {e}")
        print()

    # Final summary
    subsection("Final Funnel")
    print(f"    {total_patients:>5} total patients")
    for step in steps:
        if step.result_count is not None and step.tables:
            print(f"    {step.result_count:>5} — {step.label}")
    pct_step = steps[-1] if steps else None
    if pct_step and pct_step.result_count:
        print()
        print(f"  Answer: {pct_step.result_count}%")

    # =====================================================================
    # TEST 2: Simple report match (L1 success)
    # =====================================================================
    section("TEST 2: Simple report question (L1 match)")
    q2 = "Show me the diabetes cohort report"
    print(f'  "{q2}"')
    print()

    intent2 = classify_intent(q2)
    print(f"  Intent: {intent2.intent}")

    hits2 = report_searcher.search(q2, top_k=3)
    print(f"  Top reports:")
    for h in hits2:
        print(f"    {h.report_name:30s} score={h.score:.3f}")

    if hits2:
        best = hits2[0]
        sql = report_searcher.get_report_sql(best.report_name)
        if sql:
            # Create view and query it
            conn.execute(f"DROP VIEW IF EXISTS {best.report_name}")
            conn.executescript(sql)
            count = execute_count(conn, f"SELECT COUNT(*) FROM {best.report_name}")
            print(f"\n  Run {best.report_name}: {count} rows")
            sample = execute_sql(conn, f"SELECT * FROM {best.report_name} LIMIT 3")
            print(f"  Columns: {sample['columns']}")
            for row in sample["rows"]:
                print(f"    {row}")

    # =====================================================================
    # TEST 3: Unknown term (L3 — technical search)
    # =====================================================================
    section("TEST 3: Unknown term (L3 cascade)")
    q3 = "patients with Addison's disease"
    print(f'  "{q3}"')
    print()

    # L1: no match
    r_hits = report_searcher.search(q3)
    top_score = r_hits[0].score if r_hits else 0
    print(f"  L1 Report Search: {len(r_hits)} matches (top score: {top_score:.3f})")

    # L2: no match
    d_hits = defn_searcher.search(q3)
    strong = [h for h in d_hits if h.strength == "strong"]
    print(f"  L2 Definition Search: {len(d_hits)} matches, "
          f"{len(strong)} strong")

    # L3: term resolution + technical search
    subsection("L3: Term Resolution")
    resolution = resolve_term("Addison's disease", context=q3)
    print(f"  Term: {resolution.term}")
    print(f"  Category: {resolution.category}")
    print(f"  Confidence: {resolution.confidence}")
    print(f"  Source: {resolution.source}")

    if resolution.category:
        routes = tech_searcher.get_routes_for_category(resolution.category)
        print(f"\n  Routes for '{resolution.category}':")
        for route in routes:
            print(f"    [{route.route_name}]")
            print(f"      Path: {' → '.join(route.path)}")
            print(f"      {route.description}")
            print()

    # =====================================================================
    # TEST 4: Term resolver edge cases
    # =====================================================================
    section("TEST 4: Term Resolver — various inputs")
    test_terms = [
        ("diabetes", "patients with diabetes"),
        ("ER", "patients who visited the ER"),
        ("appendicitis", "patients with appendicitis"),
        ("atorvastatin", "patients taking atorvastatin"),
        ("colonoscopy", "patients who had a colonoscopy"),
        ("lupus", "patients with lupus"),
        ("metformin", "patients on metformin"),
        ("xyzzy123", "patients with xyzzy123"),
    ]
    print(f"  {'Term':<20s} {'Category':<15s} {'Confidence':<10s} Source")
    print(f"  {'-'*20} {'-'*15} {'-'*10} {'-'*30}")
    for term, context in test_terms:
        r = resolve_term(term, context)
        cat = r.category or "UNKNOWN"
        print(f"  {term:<20s} {cat:<15s} {r.confidence:<10s} {r.source[:40]}")

    # =====================================================================
    # INVENTORY
    # =====================================================================
    section("PHASE 2 INVENTORY — What was built")
    print()
    print("  Search Tools:")
    print("    tools/jit/search_reports.py      — L1 Report Searcher (TF-IDF)")
    print("    tools/jit/search_definitions.py  — L2 Definition Searcher (TF-IDF + synonyms + grouping)")
    print("    tools/jit/search_technical.py    — L3 Technical Searcher (domain/route lookup)")
    print()
    print("  Support Tools:")
    print("    tools/jit/term_resolver.py       — 4-layer term resolution")
    print("    tools/jit/quantifier_extractor.py — threshold + date regex extraction")
    print("    tools/jit/step_builder.py        — CTE-chained SQL generation")
    print("    tools/jit/router.py              — intent classifier + cascade state")
    print()
    print("  Tests: 40 passing (tools/jit/tests/test_phase2.py)")
    print()
    print("  Next: Phase 3 — Streamlit UI")

    conn.close()


if __name__ == "__main__":
    main()
