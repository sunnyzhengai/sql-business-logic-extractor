"""Tests for tools.p30_analyze.join_paths.

Run from the repo root:
    python -m unittest tools.p30_analyze.tests.test_join_paths
"""

from __future__ import annotations

import unittest


def _view_with_joins(view_name: str, from_table: str,
                       joins: list[tuple[str, str]]) -> dict:
    """Helper: build a minimal view dict whose main scope has the given
    from-table + joins (list of (right_table, join_type) tuples)."""
    return {
        "view_name": view_name,
        "scopes": [{
            "id": "main",
            "kind": "main",
            "reads_from_tables": [from_table] + [j[0] for j in joins],
            "joins": [
                {"right_table": rt, "join_type": jt, "on_expression": ""}
                for rt, jt in joins
            ],
            "reads_from_scopes": [],
            "columns": [],
            "filters": [],
        }],
    }


class TestAnalyzeJoinPaths(unittest.TestCase):

    def test_common_edge_across_views(self):
        """Three views all do PATIENT -> COVERAGE. The edge should report
        n_views=3 with all three view names."""
        from tools.p20_index.graph_builder import build_graph
        from tools.p30_analyze.join_paths import analyze_join_paths

        views = [
            _view_with_joins("V1", "PATIENT", [("COVERAGE", "INNER JOIN")]),
            _view_with_joins("V2", "PATIENT", [("COVERAGE", "INNER JOIN")]),
            _view_with_joins("V3", "PATIENT", [("COVERAGE", "LEFT JOIN")]),  # different type
        ]
        g = build_graph(views)
        result = analyze_join_paths(g, {0: {"V1", "V2", "V3"}})

        # One common edge: PATIENT -> COVERAGE.
        edges = result[0]
        self.assertEqual(len(edges), 1)
        e = edges[0]
        self.assertEqual(e["from_table"], "PATIENT")
        self.assertEqual(e["to_table"], "COVERAGE")
        self.assertEqual(e["n_views"], 3)
        self.assertEqual(e["views"], ["V1", "V2", "V3"])
        # 2 distinct join types observed (INNER twice, LEFT once);
        # representative is INNER (the most common).
        self.assertEqual(e["join_type"], "INNER JOIN")
        self.assertEqual(e["n_distinct_join_types"], 2)

    def test_sorts_by_view_count_desc(self):
        """Edge used by more views should sort first."""
        from tools.p20_index.graph_builder import build_graph
        from tools.p30_analyze.join_paths import analyze_join_paths

        views = [
            # Edge A->B: 3 views.
            _view_with_joins("V1", "A", [("B", "INNER JOIN")]),
            _view_with_joins("V2", "A", [("B", "INNER JOIN")]),
            _view_with_joins("V3", "A", [("B", "INNER JOIN")]),
            # Edge A->C: 1 view.
            _view_with_joins("V4", "A", [("C", "INNER JOIN")]),
        ]
        g = build_graph(views)
        result = analyze_join_paths(g, {0: {"V1", "V2", "V3", "V4"}})
        ordered = [(r["from_table"], r["to_table"]) for r in result[0]]
        # A->B (3 views) should come before A->C (1 view).
        self.assertEqual(ordered[0], ("A", "B"))
        self.assertEqual(ordered[1], ("A", "C"))

    def test_views_outside_community_are_skipped(self):
        """A view that's NOT in the community shouldn't contribute to the
        community's join-edge counts."""
        from tools.p20_index.graph_builder import build_graph
        from tools.p30_analyze.join_paths import analyze_join_paths

        views = [
            _view_with_joins("V1", "A", [("B", "INNER JOIN")]),  # community 0
            _view_with_joins("V2", "A", [("B", "INNER JOIN")]),  # community 1 (not 0)
        ]
        g = build_graph(views)
        result = analyze_join_paths(g, {0: {"V1"}, 1: {"V2"}})
        # Community 0's A->B edge has only 1 view (V1, not V2).
        self.assertEqual(result[0][0]["n_views"], 1)
        self.assertEqual(result[0][0]["views"], ["V1"])
        # Community 1 sees the same edge with its own 1 view.
        self.assertEqual(result[1][0]["n_views"], 1)
        self.assertEqual(result[1][0]["views"], ["V2"])

    def test_empty_community_returns_empty_list(self):
        from tools.p20_index.graph_builder import build_graph
        from tools.p30_analyze.join_paths import analyze_join_paths

        g = build_graph([])
        result = analyze_join_paths(g, {0: set()})
        self.assertEqual(result[0], [])


if __name__ == "__main__":
    unittest.main()
