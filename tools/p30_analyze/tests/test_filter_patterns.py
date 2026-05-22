"""Tests for tools.p30_analyze.filter_patterns.

Run from the repo root:
    python -m unittest tools.p30_analyze.tests.test_filter_patterns
"""

from __future__ import annotations

import unittest


def _view_with_filters(view_name: str, filters: list[dict]) -> dict:
    """Helper: build a minimal view dict whose main scope carries the given filters."""
    return {
        "view_name": view_name,
        "scopes": [{
            "id": "main",
            "kind": "main",
            "reads_from_tables": [], "joins": [], "reads_from_scopes": [],
            "columns": [],
            "filters": filters,
        }],
    }


def _f(english: str = "", expression: str = "", kind: str = "where") -> dict:
    return {"english": english, "expression": expression, "kind": kind}


class TestAnalyzeFilterPatterns(unittest.TestCase):

    def test_common_filter_across_views(self):
        from tools.p30_analyze.filter_patterns import analyze_filter_patterns

        views = [
            _view_with_filters("V1", [_f("Active patients only", "STATUS_C = 1")]),
            _view_with_filters("V2", [_f("Active patients only", "STATUS_C = 1")]),
            _view_with_filters("V3", [_f("Active patients only", "STATUS_C = 1")]),
        ]
        result = analyze_filter_patterns(views, {0: {"V1", "V2", "V3"}})
        self.assertEqual(len(result[0]), 1)
        r = result[0][0]
        self.assertEqual(r["english"], "Active patients only")
        self.assertEqual(r["sql"], "STATUS_C = 1")
        self.assertEqual(r["n_views"], 3)
        self.assertEqual(r["views"], ["V1", "V2", "V3"])

    def test_whitespace_normalization_groups_near_identical(self):
        """Slight whitespace differences in SQL shouldn't split a group."""
        from tools.p30_analyze.filter_patterns import analyze_filter_patterns

        views = [
            _view_with_filters("V1", [_f("", "STATUS_C  =  1")]),  # double-spaces
            _view_with_filters("V2", [_f("", "STATUS_C = 1")]),
        ]
        result = analyze_filter_patterns(views, {0: {"V1", "V2"}})
        self.assertEqual(len(result[0]), 1)
        self.assertEqual(result[0][0]["n_views"], 2)

    def test_distinct_filters_are_separate_records(self):
        from tools.p30_analyze.filter_patterns import analyze_filter_patterns

        views = [
            _view_with_filters("V1", [_f("Active members")]),
            _view_with_filters("V2", [_f("Inactive members")]),
        ]
        result = analyze_filter_patterns(views, {0: {"V1", "V2"}})
        self.assertEqual(len(result[0]), 2)
        names = {r["english"] for r in result[0]}
        self.assertEqual(names, {"Active members", "Inactive members"})

    def test_min_views_threshold_filters_out_low_frequency(self):
        from tools.p30_analyze.filter_patterns import analyze_filter_patterns

        views = [
            _view_with_filters("V1", [_f("Common filter")]),
            _view_with_filters("V2", [_f("Common filter")]),
            _view_with_filters("V3", [_f("One-off filter")]),
        ]
        # min_views=2 should drop "One-off filter".
        result = analyze_filter_patterns(
            views, {0: {"V1", "V2", "V3"}}, min_views=2,
        )
        self.assertEqual(len(result[0]), 1)
        self.assertEqual(result[0][0]["english"], "Common filter")

    def test_filter_with_no_english_falls_back_to_expression(self):
        from tools.p30_analyze.filter_patterns import analyze_filter_patterns

        views = [
            _view_with_filters("V1", [_f("", "DEPT_ID = 100")]),
            _view_with_filters("V2", [_f("", "DEPT_ID = 100")]),
        ]
        result = analyze_filter_patterns(views, {0: {"V1", "V2"}})
        self.assertEqual(len(result[0]), 1)
        r = result[0][0]
        self.assertEqual(r["n_views"], 2)
        # English is empty; SQL is the representative.
        self.assertEqual(r["sql"], "DEPT_ID = 100")


class TestCountFilterPatterns(unittest.TestCase):

    def test_default_min_views_2(self):
        from tools.p30_analyze.filter_patterns import count_filter_patterns

        filters = {
            0: [{"n_views": 5}, {"n_views": 1}, {"n_views": 3}],
            1: [{"n_views": 2}],
        }
        # Default min_views=2: count records with n_views >= 2.
        # Community 0: 5 and 3 qualify; 1 doesn't -> 2.
        # Community 1: 2 qualifies -> 1.
        # Total: 3.
        self.assertEqual(count_filter_patterns(filters), 3)


if __name__ == "__main__":
    unittest.main()
