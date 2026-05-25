"""Tests for tools.p30_analyze.view_expansion.

Covers the recursive view-of-view -> base-table resolution and the
Louvain-ready projection built from the expanded view sets.
"""

from __future__ import annotations

import unittest


class TestExpandViewToBaseTables(unittest.TestCase):

    def test_simple_view_unchanged(self):
        """A view that reads only base tables stays as-is."""
        from tools.p30_analyze.view_expansion import expand_view_to_base_tables
        view_to_tables = {
            "VW_A": {"table::PATIENT", "table::PAT_ENC"},
        }
        result = expand_view_to_base_tables(view_to_tables, {"VW_A"})
        self.assertEqual(result["VW_A"], {"table::PATIENT", "table::PAT_ENC"})

    def test_one_layer_view_of_view_expanded(self):
        """VW_A reads VW_B which reads base tables -> VW_A's expanded
        table set is VW_B's base tables."""
        from tools.p30_analyze.view_expansion import expand_view_to_base_tables
        view_to_tables = {
            "VW_A": {"table::VW_B"},                      # foundation-view reference
            "VW_B": {"table::PATIENT", "table::PAT_ENC"}, # base tables
        }
        result = expand_view_to_base_tables(view_to_tables, {"VW_A", "VW_B"})
        self.assertEqual(result["VW_A"], {"table::PATIENT", "table::PAT_ENC"})
        self.assertEqual(result["VW_B"], {"table::PATIENT", "table::PAT_ENC"})

    def test_two_layers_of_indirection(self):
        """VW_A -> VW_B -> VW_C -> base tables."""
        from tools.p30_analyze.view_expansion import expand_view_to_base_tables
        view_to_tables = {
            "VW_A": {"table::VW_B"},
            "VW_B": {"table::VW_C"},
            "VW_C": {"table::PATIENT", "table::ENCOUNTER"},
        }
        result = expand_view_to_base_tables(
            view_to_tables, {"VW_A", "VW_B", "VW_C"},
        )
        self.assertEqual(result["VW_A"], {"table::PATIENT", "table::ENCOUNTER"})

    def test_view_with_mix_of_base_tables_and_view_references(self):
        """VW_A reads base table X AND foundation view VW_B."""
        from tools.p30_analyze.view_expansion import expand_view_to_base_tables
        view_to_tables = {
            "VW_A": {"table::X", "table::VW_B"},
            "VW_B": {"table::Y", "table::Z"},
        }
        result = expand_view_to_base_tables(view_to_tables, {"VW_A", "VW_B"})
        self.assertEqual(result["VW_A"], {"table::X", "table::Y", "table::Z"})

    def test_cycle_break(self):
        """A -> B -> A should not infinite-loop. The cycle is broken;
        each view returns ONLY its non-view-reference base tables."""
        from tools.p30_analyze.view_expansion import expand_view_to_base_tables
        view_to_tables = {
            "VW_A": {"table::VW_B", "table::X"},
            "VW_B": {"table::VW_A", "table::Y"},
        }
        result = expand_view_to_base_tables(view_to_tables, {"VW_A", "VW_B"})
        # Each view contributes its own base table; the cycle reference
        # short-circuits and contributes nothing on the second visit.
        # End result: each contains both X and Y (one direct, one
        # through the other view before the cycle breaks).
        self.assertIn("table::X", result["VW_A"])
        self.assertIn("table::Y", result["VW_A"])
        self.assertIn("table::X", result["VW_B"])
        self.assertIn("table::Y", result["VW_B"])

    def test_foundation_view_not_in_corpus_treated_as_base(self):
        """If VW_A references VW_B but VW_B isn't in view_names (e.g.
        it lives in a different database or wasn't exported), VW_B is
        treated as a base table -- we have no further information."""
        from tools.p30_analyze.view_expansion import expand_view_to_base_tables
        view_to_tables = {
            "VW_A": {"table::VW_B", "table::X"},
            # VW_B intentionally absent from view_to_tables
        }
        result = expand_view_to_base_tables(view_to_tables, {"VW_A"})
        self.assertEqual(result["VW_A"], {"table::VW_B", "table::X"})


class TestBuildExpandedTableProjection(unittest.TestCase):

    def _mini_graph(self):
        """Build a tiny graph: 2 views, both reading from a foundation
        view that reads from 2 base tables."""
        import networkx as nx
        g = nx.MultiDiGraph()
        # Table-typed nodes.
        for t in ("table::FOUNDATION_VIEW", "table::PATIENT", "table::PAT_ENC"):
            g.add_node(t, ntype="table")
        # View-typed nodes (don't add to projection; only consumed by build_expanded_table_projection).
        for v in ("view::VW_A", "view::VW_B"):
            g.add_node(v, ntype="view")
        return g

    def test_expanded_projection_groups_views_through_foundation(self):
        """Two views that both go through the same foundation view end
        up sharing the foundation's base tables in the projection,
        producing weighted edges between those base tables."""
        from tools.p30_analyze.view_expansion import build_expanded_table_projection

        g = self._mini_graph()
        views = [{"view_name": "VW_A"}, {"view_name": "VW_B"}, {"view_name": "FOUNDATION_VIEW"}]
        # Simulate view_to_tables output: each report view reads the
        # foundation view; the foundation view reads two base tables.
        view_to_tables_map = {
            "VW_A": {"table::FOUNDATION_VIEW"},
            "VW_B": {"table::FOUNDATION_VIEW"},
            "FOUNDATION_VIEW": {"table::PATIENT", "table::PAT_ENC"},
        }
        table_g, expanded = build_expanded_table_projection(
            g, views, view_to_tables_map,
        )
        # VW_A and VW_B now both have PATIENT + PAT_ENC after expansion.
        self.assertEqual(expanded["VW_A"], {"table::PATIENT", "table::PAT_ENC"})
        self.assertEqual(expanded["VW_B"], {"table::PATIENT", "table::PAT_ENC"})
        # The projection has an edge between the base tables.
        self.assertTrue(table_g.has_edge("table::PATIENT", "table::PAT_ENC"))
        # FOUNDATION_VIEW is NOT in the projection -- it got expanded
        # away; only base tables remain as projection nodes.
        self.assertNotIn("table::FOUNDATION_VIEW", table_g.nodes)


if __name__ == "__main__":
    unittest.main()
