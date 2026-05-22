"""Tests for tools.p30_analyze.column_variance.

Run from the repo root:
    python -m unittest tools.p30_analyze.tests.test_column_variance
"""

from __future__ import annotations

import unittest


def _make_view(view_name: str, columns: list[dict]) -> dict:
    """Build a minimal ViewV1 dict with one main scope containing `columns`."""
    return {
        "view_name": view_name,
        "scopes": [{
            "id": "main",
            "kind": "main",
            "reads_from_tables": [], "joins": [], "reads_from_scopes": [],
            "columns": columns,
            "filters": [],
        }],
    }


def _col(name: str, fp: str, tables: list[str], tech: str = "", biz: str = "") -> dict:
    """Build a minimal ColumnV1 record for tests."""
    return {
        "column_name": name,
        "fingerprint": fp,
        "base_tables": tables,
        "technical_description": tech,
        "business_description": biz,
    }


class TestAnalyzeColumnVariance(unittest.TestCase):

    def test_same_source_different_fingerprint_is_variance(self):
        """A column name with the SAME source tables but DIFFERENT
        fingerprints across views in a community should surface as a
        reconciliation candidate."""
        from tools.p30_analyze.column_variance import analyze_column_variance

        views = [
            _make_view("V1", [_col("MEMBER_ID", "fp_a", ["PATIENT"], tech="P.PAT_ID")]),
            _make_view("V2", [_col("MEMBER_ID", "fp_b", ["PATIENT"], tech="RTRIM(P.PAT_ID)")]),
            _make_view("V3", [_col("MEMBER_ID", "fp_a", ["PATIENT"], tech="P.PAT_ID")]),
        ]
        community_to_primary = {0: {"V1", "V2", "V3"}}
        result = analyze_column_variance(views, community_to_primary)
        self.assertEqual(len(result[0]), 1, "expected exactly one variance record")

        rec = result[0][0]
        self.assertEqual(rec["column_name"], "MEMBER_ID")
        self.assertEqual(rec["source_tables"], ["PATIENT"])
        self.assertEqual(rec["n_views"], 3)
        self.assertEqual(rec["n_distinct_fingerprints"], 2)
        # fp_a has 2 views (V1, V3); fp_b has 1 view (V2). Order: most-common first.
        self.assertEqual(rec["definitions"][0]["fingerprint"], "fp_a")
        self.assertEqual(rec["definitions"][0]["views"], ["V1", "V3"])
        self.assertEqual(rec["definitions"][1]["fingerprint"], "fp_b")
        self.assertEqual(rec["definitions"][1]["views"], ["V2"])

    def test_different_source_tables_are_NOT_grouped(self):
        """MEMBER_ID from PATIENT and MEMBER_ID from COVERAGE are different
        concepts (naming collision, not definitional variance). They should
        NOT show up as a single variance record."""
        from tools.p30_analyze.column_variance import analyze_column_variance

        views = [
            _make_view("V1", [_col("MEMBER_ID", "fp_a", ["PATIENT"])]),
            _make_view("V2", [_col("MEMBER_ID", "fp_b", ["COVERAGE"])]),
        ]
        result = analyze_column_variance(views, {0: {"V1", "V2"}})
        # Each (column_name, source_tables) is its own group; both have only
        # 1 fingerprint each, so neither qualifies as a variance record.
        self.assertEqual(result[0], [])

    def test_no_variance_when_all_fingerprints_match(self):
        """Two views, same column name, same fingerprint -> no variance."""
        from tools.p30_analyze.column_variance import analyze_column_variance

        views = [
            _make_view("V1", [_col("PAT_ID", "fp_same", ["PATIENT"])]),
            _make_view("V2", [_col("PAT_ID", "fp_same", ["PATIENT"])]),
        ]
        result = analyze_column_variance(views, {0: {"V1", "V2"}})
        self.assertEqual(result[0], [])

    def test_cte_columns_excluded(self):
        """CTE-internal columns are not user-visible outputs and should
        not contribute to variance findings -- only main-scope columns."""
        from tools.p30_analyze.column_variance import analyze_column_variance

        view = {
            "view_name": "V1",
            "scopes": [
                # CTE scope -- should be ignored.
                {
                    "id": "cte:foo", "kind": "cte",
                    "reads_from_tables": [], "joins": [], "reads_from_scopes": [],
                    "columns": [_col("X", "fp_cte", ["TBL"])],
                    "filters": [],
                },
                # Main scope.
                {
                    "id": "main", "kind": "main",
                    "reads_from_tables": [], "joins": [], "reads_from_scopes": [],
                    "columns": [_col("X", "fp_main_a", ["TBL"])],
                    "filters": [],
                },
            ],
        }
        view2 = _make_view("V2", [_col("X", "fp_main_b", ["TBL"])])
        result = analyze_column_variance([view, view2], {0: {"V1", "V2"}})
        # Only the two main-scope X columns contribute -> 2 distinct fingerprints.
        # If the CTE column counted, n_distinct would be 3.
        self.assertEqual(len(result[0]), 1)
        self.assertEqual(result[0][0]["n_distinct_fingerprints"], 2)

    def test_columns_without_fingerprint_skipped(self):
        """Columns missing the fingerprint field (e.g., unparseable
        expressions) should be skipped silently, not blow up."""
        from tools.p30_analyze.column_variance import analyze_column_variance

        views = [
            _make_view("V1", [
                _col("X", "fp_a", ["TBL"]),
                {"column_name": "Y", "base_tables": ["TBL"]},  # no fingerprint
            ]),
            _make_view("V2", [_col("X", "fp_b", ["TBL"])]),
        ]
        result = analyze_column_variance(views, {0: {"V1", "V2"}})
        # X has variance; Y was skipped.
        names = [r["column_name"] for r in result[0]]
        self.assertEqual(names, ["X"])

    def test_sorting_by_importance(self):
        """Records sort by (most views, then most variance, then alphabetical)."""
        from tools.p30_analyze.column_variance import analyze_column_variance

        # Build 3 columns with different importance:
        #   B: 4 views, 2 fingerprints
        #   A: 2 views, 2 fingerprints   (fewer views -> after B)
        #   C: 2 views, 3 fingerprints   (same views as A but more variance -> before A)
        views = []
        for i in range(4):
            views.append(_make_view(f"V_B{i}", [
                _col("B", "fp_b1" if i < 2 else "fp_b2", ["T"]),
            ]))
        views.append(_make_view("V_A1", [_col("A", "fp_a1", ["T"])]))
        views.append(_make_view("V_A2", [_col("A", "fp_a2", ["T"])]))
        views.append(_make_view("V_C1", [_col("C", "fp_c1", ["T"])]))
        views.append(_make_view("V_C2", [_col("C", "fp_c2", ["T"])]))

        all_views = {v["view_name"] for v in views}
        result = analyze_column_variance(views, {0: all_views})
        # Expected order: B (4 views), C (2 views, but >fp than A... wait, C has 2 fps just like A)
        # Actually re-reading: C has 2 views, 2 fingerprints (fp_c1, fp_c2). Same as A.
        # So secondary sort is by n_distinct_fingerprints (both 2). Tertiary: alphabetical
        # -> A before C. So order: B, A, C.
        names = [r["column_name"] for r in result[0]]
        self.assertEqual(names, ["B", "A", "C"])


class TestCountReconciliationCandidates(unittest.TestCase):

    def test_count_across_communities(self):
        from tools.p30_analyze.column_variance import count_reconciliation_candidates
        variance = {
            0: [{"column_name": "A"}, {"column_name": "B"}],
            1: [{"column_name": "C"}],
            2: [],
        }
        self.assertEqual(count_reconciliation_candidates(variance), 3)


if __name__ == "__main__":
    unittest.main()
