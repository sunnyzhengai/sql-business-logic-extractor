"""Tests for tools.jit.pattern_classifier.

Run from the repo root:
    python -m pytest tools/jit/tests/test_pattern_classifier.py -v
"""

from __future__ import annotations

import unittest


# ---------------------------------------------------------------------------
# Fixtures -- views representing different clinical capture patterns
# ---------------------------------------------------------------------------

VIEW_ENCOUNTER_DX = {
    "view_name": "VW_CLINIC_DX",
    "scopes": [{
        "id": "main", "kind": "main",
        "reads_from_tables": ["PATIENT", "PAT_ENC", "PAT_ENC_DX", "CLARITY_EDG"],
        "joins": [
            {"right_table": "PAT_ENC", "join_type": "INNER JOIN", "on_expression": ""},
            {"right_table": "PAT_ENC_DX", "join_type": "INNER JOIN", "on_expression": ""},
            {"right_table": "CLARITY_EDG", "join_type": "INNER JOIN", "on_expression": ""},
        ],
        "reads_from_scopes": [], "columns": [], "filters": [],
    }],
}

VIEW_MEDICATION = {
    "view_name": "VW_MED_ORDERS",
    "scopes": [{
        "id": "main", "kind": "main",
        "reads_from_tables": ["PATIENT", "ORDER_MED", "CLARITY_MEDICATION"],
        "joins": [
            {"right_table": "ORDER_MED", "join_type": "INNER JOIN", "on_expression": ""},
            {"right_table": "CLARITY_MEDICATION", "join_type": "LEFT JOIN", "on_expression": ""},
        ],
        "reads_from_scopes": [], "columns": [], "filters": [],
    }],
}

VIEW_LAB_RESULT = {
    "view_name": "VW_LAB_VALUES",
    "scopes": [{
        "id": "main", "kind": "main",
        "reads_from_tables": ["PATIENT", "ORDER_PROC", "ORDER_RESULTS"],
        "joins": [
            {"right_table": "ORDER_PROC", "join_type": "INNER JOIN", "on_expression": ""},
            {"right_table": "ORDER_RESULTS", "join_type": "INNER JOIN", "on_expression": ""},
        ],
        "reads_from_scopes": [], "columns": [], "filters": [],
    }],
}

VIEW_COMBINED_DX_MED = {
    "view_name": "VW_DIABETES_COHORT",
    "scopes": [{
        "id": "main", "kind": "main",
        "reads_from_tables": ["PATIENT", "PAT_ENC", "PAT_ENC_DX", "CLARITY_EDG",
                              "ORDER_MED", "CLARITY_MEDICATION"],
        "joins": [
            {"right_table": "PAT_ENC", "join_type": "INNER JOIN", "on_expression": ""},
            {"right_table": "PAT_ENC_DX", "join_type": "INNER JOIN", "on_expression": ""},
            {"right_table": "CLARITY_EDG", "join_type": "INNER JOIN", "on_expression": ""},
            {"right_table": "ORDER_MED", "join_type": "LEFT JOIN", "on_expression": ""},
            {"right_table": "CLARITY_MEDICATION", "join_type": "LEFT JOIN", "on_expression": ""},
        ],
        "reads_from_scopes": [], "columns": [], "filters": [],
    }],
}

VIEW_DEMOGRAPHICS_ONLY = {
    "view_name": "VW_PATIENT_LIST",
    "scopes": [{
        "id": "main", "kind": "main",
        "reads_from_tables": ["PATIENT"],
        "joins": [],
        "reads_from_scopes": [], "columns": [], "filters": [],
    }],
}

VIEW_REFERRAL = {
    "view_name": "VW_REFERRAL_STATUS",
    "scopes": [{
        "id": "main", "kind": "main",
        "reads_from_tables": ["REFERRAL", "ZC_RFL_STATUS"],
        "joins": [
            {"right_table": "ZC_RFL_STATUS", "join_type": "LEFT JOIN", "on_expression": ""},
        ],
        "reads_from_scopes": [], "columns": [], "filters": [],
    }],
}

VIEW_HOSPITALIZATION = {
    "view_name": "VW_INPATIENT_STAYS",
    "scopes": [{
        "id": "main", "kind": "main",
        "reads_from_tables": ["PATIENT", "PAT_ENC_HSP", "HSP_ADMIT_DIAG", "CLARITY_EDG"],
        "joins": [
            {"right_table": "PAT_ENC_HSP", "join_type": "INNER JOIN", "on_expression": ""},
            {"right_table": "HSP_ADMIT_DIAG", "join_type": "INNER JOIN", "on_expression": ""},
            {"right_table": "CLARITY_EDG", "join_type": "INNER JOIN", "on_expression": ""},
        ],
        "reads_from_scopes": [], "columns": [], "filters": [],
    }],
}

VIEW_ED = {
    "view_name": "VW_ED_ARRIVALS",
    "scopes": [{
        "id": "main", "kind": "main",
        "reads_from_tables": ["PATIENT", "PAT_ENC", "ED_IEV_PAT_INFO", "ED_IEV_EVENT_INFO"],
        "joins": [
            {"right_table": "PAT_ENC", "join_type": "INNER JOIN", "on_expression": ""},
            {"right_table": "ED_IEV_PAT_INFO", "join_type": "INNER JOIN", "on_expression": ""},
            {"right_table": "ED_IEV_EVENT_INFO", "join_type": "INNER JOIN", "on_expression": ""},
        ],
        "reads_from_scopes": [], "columns": [], "filters": [],
    }],
}

VIEW_EMPTY = {
    "view_name": "VW_EMPTY",
    "scopes": [{
        "id": "main", "kind": "main",
        "reads_from_tables": [],
        "joins": [],
        "reads_from_scopes": [], "columns": [], "filters": [],
    }],
}

VIEW_ENCOUNTER_ONLY = {
    "view_name": "VW_ALL_VISITS",
    "scopes": [{
        "id": "main", "kind": "main",
        "reads_from_tables": ["PATIENT", "PAT_ENC", "CLARITY_DEP"],
        "joins": [
            {"right_table": "PAT_ENC", "join_type": "INNER JOIN", "on_expression": ""},
            {"right_table": "CLARITY_DEP", "join_type": "LEFT JOIN", "on_expression": ""},
        ],
        "reads_from_scopes": [], "columns": [], "filters": [],
    }],
}

VIEW_PROBLEM_LIST = {
    "view_name": "VW_ACTIVE_PROBLEMS",
    "scopes": [{
        "id": "main", "kind": "main",
        "reads_from_tables": ["PATIENT", "PROBLEM_LIST", "CLARITY_EDG"],
        "joins": [
            {"right_table": "PROBLEM_LIST", "join_type": "INNER JOIN", "on_expression": ""},
            {"right_table": "CLARITY_EDG", "join_type": "INNER JOIN", "on_expression": ""},
        ],
        "reads_from_scopes": [], "columns": [], "filters": [],
    }],
}


class TestClassifyView(unittest.TestCase):

    def test_encounter_diagnosis(self):
        from tools.jit.pattern_classifier import classify_view
        patterns = classify_view(VIEW_ENCOUNTER_DX)
        names = {p.name for p in patterns}
        self.assertIn("encounter_diagnosis", names)
        # Should NOT have generic "encounter" since encounter_diagnosis implies it
        self.assertNotIn("encounter", names)

    def test_medication(self):
        from tools.jit.pattern_classifier import classify_view
        patterns = classify_view(VIEW_MEDICATION)
        names = {p.name for p in patterns}
        self.assertIn("medication", names)

    def test_lab_result(self):
        from tools.jit.pattern_classifier import classify_view
        patterns = classify_view(VIEW_LAB_RESULT)
        names = {p.name for p in patterns}
        self.assertIn("lab_result", names)
        self.assertIn("procedure", names)

    def test_combined_dx_and_med(self):
        """A view touching both PAT_ENC_DX and ORDER_MED gets both labels."""
        from tools.jit.pattern_classifier import classify_view
        patterns = classify_view(VIEW_COMBINED_DX_MED)
        names = {p.name for p in patterns}
        self.assertIn("encounter_diagnosis", names)
        self.assertIn("medication", names)
        self.assertEqual(len(names), 2)

    def test_demographics_only(self):
        """A view touching only PATIENT gets the demographics label."""
        from tools.jit.pattern_classifier import classify_view
        patterns = classify_view(VIEW_DEMOGRAPHICS_ONLY)
        self.assertEqual(len(patterns), 1)
        self.assertEqual(patterns[0].name, "demographics")
        self.assertEqual(patterns[0].anchor_table, "PATIENT")

    def test_referral(self):
        from tools.jit.pattern_classifier import classify_view
        patterns = classify_view(VIEW_REFERRAL)
        names = {p.name for p in patterns}
        self.assertIn("referral", names)

    def test_hospitalization_with_admission_dx(self):
        """PAT_ENC_HSP + HSP_ADMIT_DIAG = hospitalization + admission_diagnosis."""
        from tools.jit.pattern_classifier import classify_view
        patterns = classify_view(VIEW_HOSPITALIZATION)
        names = {p.name for p in patterns}
        self.assertIn("hospitalization", names)
        self.assertIn("admission_diagnosis", names)
        # encounter should be suppressed (hospitalization implies it)
        self.assertNotIn("encounter", names)

    def test_ed_events(self):
        from tools.jit.pattern_classifier import classify_view
        patterns = classify_view(VIEW_ED)
        names = {p.name for p in patterns}
        self.assertIn("ed_event", names)
        self.assertNotIn("encounter", names)

    def test_empty_view(self):
        from tools.jit.pattern_classifier import classify_view
        patterns = classify_view(VIEW_EMPTY)
        self.assertEqual(patterns, [])

    def test_encounter_only_not_suppressed(self):
        """PAT_ENC without any specific domain table = plain encounter."""
        from tools.jit.pattern_classifier import classify_view
        patterns = classify_view(VIEW_ENCOUNTER_ONLY)
        names = {p.name for p in patterns}
        self.assertIn("encounter", names)

    def test_problem_list_suppresses_diagnosis_master(self):
        """PROBLEM_LIST + CLARITY_EDG: problem_list matched, diagnosis_master suppressed."""
        from tools.jit.pattern_classifier import classify_view
        patterns = classify_view(VIEW_PROBLEM_LIST)
        names = {p.name for p in patterns}
        self.assertIn("problem_list", names)
        self.assertNotIn("diagnosis_master", names)

    def test_anchor_table_populated(self):
        from tools.jit.pattern_classifier import classify_view
        patterns = classify_view(VIEW_ENCOUNTER_DX)
        enc_dx = [p for p in patterns if p.name == "encounter_diagnosis"][0]
        self.assertEqual(enc_dx.anchor_table, "PAT_ENC_DX")


class TestClassifyCorpus(unittest.TestCase):

    def test_groups_by_pattern(self):
        from tools.jit.pattern_classifier import classify_corpus
        views = [VIEW_ENCOUNTER_DX, VIEW_MEDICATION, VIEW_LAB_RESULT,
                 VIEW_DEMOGRAPHICS_ONLY]
        groups = classify_corpus(views)
        self.assertIn("encounter_diagnosis", groups)
        self.assertIn("medication", groups)
        self.assertIn("lab_result", groups)
        self.assertIn("demographics", groups)

    def test_combined_view_appears_in_both_groups(self):
        from tools.jit.pattern_classifier import classify_corpus
        groups = classify_corpus([VIEW_COMBINED_DX_MED])
        self.assertIn("encounter_diagnosis", groups)
        self.assertIn("medication", groups)
        self.assertEqual(groups["encounter_diagnosis"][0]["view_name"],
                         "VW_DIABETES_COHORT")
        self.assertEqual(groups["medication"][0]["view_name"],
                         "VW_DIABETES_COHORT")

    def test_unclassified_for_empty(self):
        from tools.jit.pattern_classifier import classify_corpus
        groups = classify_corpus([VIEW_EMPTY])
        self.assertIn("_unclassified", groups)


class TestSummarize(unittest.TestCase):

    def test_markdown_output(self):
        from tools.jit.pattern_classifier import summarize_corpus_patterns
        views = [VIEW_ENCOUNTER_DX, VIEW_MEDICATION, VIEW_REFERRAL,
                 VIEW_DEMOGRAPHICS_ONLY]
        md = summarize_corpus_patterns(views)
        self.assertIn("Capture Pattern Summary", md)
        self.assertIn("Encounter Diagnosis", md)
        self.assertIn("Medication", md)
        self.assertIn("Referral", md)
        self.assertIn("Demographics", md)


if __name__ == "__main__":
    unittest.main()
