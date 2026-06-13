"""Phase 2 tests — search tools, term resolver, quantifier extraction, step builder, router."""

from __future__ import annotations

import sqlite3
import tempfile
from datetime import date
from pathlib import Path

import pytest

DATA_DIR = Path(__file__).resolve().parents[1] / "data"


# ---------------------------------------------------------------------------
# Term Resolver
# ---------------------------------------------------------------------------

class TestTermResolver:

    def test_known_term_diabetes(self):
        from tools.jit.term_resolver import resolve_term
        r = resolve_term("diabetic patients")
        assert r.category == "diagnosis"
        assert r.confidence == "known"

    def test_known_term_er_alias(self):
        from tools.jit.term_resolver import resolve_term
        r = resolve_term("ER")
        assert r.category == "encounter"
        assert "alias" in r.source

    def test_pattern_suffix_disease(self):
        from tools.jit.term_resolver import resolve_term
        r = resolve_term("Addison's disease")
        assert r.category == "diagnosis"
        assert r.confidence == "pattern"

    def test_pattern_suffix_itis(self):
        from tools.jit.term_resolver import resolve_term
        r = resolve_term("appendicitis")
        assert r.category == "diagnosis"

    def test_pattern_suffix_statin(self):
        from tools.jit.term_resolver import resolve_term
        r = resolve_term("atorvastatin")
        assert r.category == "medication"

    def test_pattern_suffix_ectomy(self):
        from tools.jit.term_resolver import resolve_term
        r = resolve_term("cholecystectomy")
        assert r.category == "procedure"

    def test_verb_frame_patients_with(self):
        from tools.jit.term_resolver import resolve_term
        r = resolve_term("lupus", context="patients with lupus")
        assert r.category == "diagnosis"

    def test_verb_frame_taking(self):
        from tools.jit.term_resolver import resolve_term
        r = resolve_term("metoprolol", context="patients taking metoprolol")
        # Could match suffix (-olol → medication) or verb frame
        assert r.category == "medication"

    def test_unknown_term(self):
        from tools.jit.term_resolver import resolve_term
        r = resolve_term("xyzzy123")
        assert r.category is None
        assert r.confidence == "unknown"

    def test_synonym_expansion(self):
        from tools.jit.term_resolver import expand_synonyms
        expanded = expand_synonyms("ER visits for pt")
        assert "emergency" in expanded
        assert "patient" in expanded


# ---------------------------------------------------------------------------
# Quantifier Extractor
# ---------------------------------------------------------------------------

class TestQuantifierExtractor:

    def test_more_than(self):
        from tools.jit.quantifier_extractor import extract_quantifiers
        results = extract_quantifiers("more than 3 times")
        assert len(results) == 1
        assert results[0].operator == ">"
        assert results[0].value == 3

    def test_at_least(self):
        from tools.jit.quantifier_extractor import extract_quantifiers
        results = extract_quantifiers("at least 5 visits")
        assert len(results) == 1
        assert results[0].operator == ">="
        assert results[0].value == 5

    def test_fewer_than(self):
        from tools.jit.quantifier_extractor import extract_quantifiers
        results = extract_quantifiers("fewer than 10 encounters")
        assert len(results) == 1
        assert results[0].operator == "<"
        assert results[0].value == 10

    def test_no_quantifier(self):
        from tools.jit.quantifier_extractor import extract_quantifiers
        results = extract_quantifiers("diabetic patients")
        assert len(results) == 0

    def test_last_year(self):
        from tools.jit.quantifier_extractor import extract_date_ranges
        ref = date(2026, 6, 11)
        results = extract_date_ranges("visits last year", reference_date=ref)
        assert len(results) == 1
        assert results[0].start_date == "2025-01-01"
        assert results[0].end_date == "2025-12-31"
        assert results[0].range_type == "calendar_year"

    def test_last_6_months(self):
        from tools.jit.quantifier_extractor import extract_date_ranges
        ref = date(2026, 6, 11)
        results = extract_date_ranges("in the last 6 months", reference_date=ref)
        assert len(results) == 1
        assert results[0].range_type == "trailing"
        assert "2026" in results[0].end_date

    def test_specific_year(self):
        from tools.jit.quantifier_extractor import extract_date_ranges
        results = extract_date_ranges("visits in 2024")
        assert len(results) == 1
        assert results[0].start_date == "2024-01-01"
        assert results[0].end_date == "2024-12-31"

    def test_combined_extraction(self):
        from tools.jit.quantifier_extractor import extract_all
        ref = date(2026, 6, 11)
        result = extract_all(
            "more than 3 ER visits last year",
            reference_date=ref,
        )
        assert len(result.quantifiers) == 1
        assert result.quantifiers[0].value == 3
        assert len(result.date_ranges) == 1
        assert result.has_extractions


# ---------------------------------------------------------------------------
# Report Searcher
# ---------------------------------------------------------------------------

class TestReportSearcher:

    def test_diabetes_question_finds_cohort(self):
        from tools.jit.search_reports import ReportSearcher
        searcher = ReportSearcher()
        hits = searcher.search("diabetic patients cohort")
        assert len(hits) > 0
        names = [h.report_name for h in hits]
        assert "VW_DIABETIC_COHORT" in names

    def test_ed_question_finds_utilization(self):
        from tools.jit.search_reports import ReportSearcher
        searcher = ReportSearcher()
        hits = searcher.search("emergency department visit frequency")
        assert len(hits) > 0
        names = [h.report_name for h in hits]
        assert "VW_ED_UTILIZATION" in names

    def test_billing_question(self):
        from tools.jit.search_reports import ReportSearcher
        searcher = ReportSearcher()
        hits = searcher.search("billing charges by department")
        assert len(hits) > 0
        assert hits[0].report_name == "VW_BILLING_SUMMARY"

    def test_complex_question_partial_matches(self):
        from tools.jit.search_reports import ReportSearcher
        searcher = ReportSearcher()
        hits = searcher.search(
            "How many percent of diabetic patients who have been to the ER "
            "more than 3 times last year have missed their PCP visit"
        )
        # Should find multiple partial matches, not one perfect match
        assert len(hits) >= 2


# ---------------------------------------------------------------------------
# Definition Searcher
# ---------------------------------------------------------------------------

class TestDefinitionSearcher:

    def test_diabetes_finds_definition(self):
        from tools.jit.search_definitions import DefinitionSearcher
        searcher = DefinitionSearcher()
        hits = searcher.search("diabetic patients")
        assert len(hits) > 0
        names = [h.definition_name for h in hits]
        assert "diabetic_patients_problem_list" in names

    def test_ed_visits_finds_definitions(self):
        from tools.jit.search_definitions import DefinitionSearcher
        searcher = DefinitionSearcher()
        hits = searcher.search("emergency department visits more than 3")
        names = [h.definition_name for h in hits]
        assert "ed_high_utilizers" in names or "ed_encounters" in names

    def test_missed_pcp_finds_definition(self):
        from tools.jit.search_definitions import DefinitionSearcher
        searcher = DefinitionSearcher()
        hits = searcher.search("missed PCP appointment no show")
        names = [h.definition_name for h in hits]
        assert "missed_pcp_visits" in names

    def test_complex_question_finds_multiple(self):
        from tools.jit.search_definitions import DefinitionSearcher
        searcher = DefinitionSearcher()
        hits = searcher.search(
            "diabetic patients ER visits missed PCP"
        )
        names = [h.definition_name for h in hits]
        # Should find at least 2 of the 3 key definitions
        key_defs = {"diabetic_patients_problem_list", "ed_high_utilizers",
                     "ed_encounters", "missed_pcp_visits"}
        found = key_defs & set(names)
        assert len(found) >= 2, f"Only found: {found}"

    def test_equivalence_grouping(self):
        from tools.jit.search_definitions import DefinitionSearcher
        searcher = DefinitionSearcher()
        groups = searcher.search_grouped("diabetes")
        # diabetic_patients_problem_list and diabetic_medications share
        # similar tables — might be grouped or not depending on filters
        assert len(groups) > 0

    def test_strong_weak_classification(self):
        from tools.jit.search_definitions import DefinitionSearcher
        searcher = DefinitionSearcher()
        hits = searcher.search("diabetic patients")
        strong = [h for h in hits if h.strength == "strong"]
        assert len(strong) >= 1


# ---------------------------------------------------------------------------
# Technical Searcher
# ---------------------------------------------------------------------------

class TestTechnicalSearcher:

    def test_diagnosis_domain(self):
        from tools.jit.search_technical import TechnicalSearcher
        searcher = TechnicalSearcher()
        matches = searcher.search_domains("diagnosis condition")
        assert len(matches) > 0
        names = [m.domain_name for m in matches]
        assert "diagnosis" in names

    def test_routes_for_diagnosis(self):
        from tools.jit.search_technical import TechnicalSearcher
        searcher = TechnicalSearcher()
        routes = searcher.get_routes_for_category("diagnosis")
        assert len(routes) == 4  # 4 diagnosis routes

    def test_unknown_term_addisons(self):
        from tools.jit.search_technical import TechnicalSearcher
        searcher = TechnicalSearcher()
        result = searcher.suggest_for_unknown_term(
            "Addison's disease",
            context="patients with Addison's disease",
        )
        assert result["resolution"].category == "diagnosis"
        assert len(result["routes"]) == 4  # 4 diagnosis routes


# ---------------------------------------------------------------------------
# Step Builder
# ---------------------------------------------------------------------------

class TestStepBuilder:

    def _load_definitions(self, *names):
        from tools.jit.mock.mock_definitions import DEFINITIONS
        return [d for d in DEFINITIONS if d["definition_name"] in names]

    def test_single_step(self):
        from tools.jit.step_builder import build_step_plan
        defs = self._load_definitions("diabetic_patients_problem_list")
        steps = build_step_plan(defs)
        assert len(steps) == 1
        assert "E11%" in steps[0].count_sql
        assert steps[0].step_number == 1

    def test_multi_step_chaining(self):
        from tools.jit.step_builder import build_step_plan
        defs = self._load_definitions(
            "diabetic_patients_problem_list",
            "ed_high_utilizers",
            "missed_pcp_visits",
        )
        steps = build_step_plan(defs)
        assert len(steps) == 3
        # Step 2 should reference step 1's CTE
        assert "diabetic_patients_problem_list" in steps[1].count_sql
        # Step 3 should reference step 2's CTE
        assert "ed_high_utilizers" in steps[2].count_sql

    def test_percentage_output(self):
        from tools.jit.step_builder import build_step_plan
        defs = self._load_definitions(
            "diabetic_patients_problem_list",
            "ed_high_utilizers",
        )
        steps = build_step_plan(defs, output_format="percentage")
        # Should have 3 steps: base, filter, percentage
        assert len(steps) == 3
        assert "percentage" in steps[-1].label.lower()

    def test_sql_executes(self):
        """Verify generated SQL actually runs against mock DB."""
        from tools.jit.step_builder import build_step_plan
        from tools.jit.mock.db_executor import execute_count

        db_path = DATA_DIR / "mock.db"
        if not db_path.exists():
            pytest.skip("mock.db not generated")

        conn = sqlite3.connect(str(db_path))
        defs = self._load_definitions(
            "diabetic_patients_problem_list",
            "ed_high_utilizers",
        )
        steps = build_step_plan(defs)

        for step in steps:
            count = execute_count(conn, step.count_sql)
            assert count > 0, f"Step {step.step_number} returned 0"

        conn.close()


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

class TestRouter:

    def test_data_intent(self):
        from tools.jit.router import classify_intent
        r = classify_intent("how many diabetic patients have ER visits")
        assert r.intent == "data"

    def test_percentage_intent(self):
        from tools.jit.router import classify_intent
        r = classify_intent("what percentage of patients are diabetic")
        assert r.intent == "data"

    def test_structural_intent(self):
        from tools.jit.router import classify_intent
        r = classify_intent("what does VW_DIABETIC_COHORT do")
        assert r.intent == "structural"

    def test_report_intent(self):
        from tools.jit.router import classify_intent
        r = classify_intent("show me the diabetes report")
        assert r.intent == "report"

    def test_concept_intent(self):
        from tools.jit.router import classify_intent
        r = classify_intent("diabetic patients with complications")
        assert r.intent == "concept"
