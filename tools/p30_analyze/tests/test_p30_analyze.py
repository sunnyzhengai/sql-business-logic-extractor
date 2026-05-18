"""Tests for the tools.p30_analyze modules.

Run from the repo root:
    python -m unittest tools.p30_analyze.tests.test_p30_analyze

The five modules under test are small enough that a single test file
with one class per module reads more naturally than five tiny files.
Each test class targets one module; the shared SAMPLE fixtures sit at
the top so all classes use the same realistic graph.
"""

from __future__ import annotations

import unittest


# ---------------------------------------------------------------------------
# Fixture views -- two synthetic views with overlapping tables.
# ---------------------------------------------------------------------------
# These are the same fixtures used by tests in p20_index/tests and
# operate/tests. Duplicated here so each test file is self-contained.

SAMPLE_VIEW_CLINIC = {
    "view_name": "VW_CLINIC_DX",
    "scopes": [
        {
            "id": "main",
            "kind": "main",
            "reads_from_tables": ["EPIC.PATIENT", "PAT_ENC", "PAT_ENC_DX", "ZC_DX_TYPE"],
            "joins": [
                {"right_table": "PAT_ENC", "join_type": "INNER JOIN", "on_expression": ""},
                {"right_table": "PAT_ENC_DX", "join_type": "INNER JOIN", "on_expression": ""},
                {"right_table": "ZC_DX_TYPE", "join_type": "LEFT JOIN", "on_expression": ""},
            ],
            "reads_from_scopes": [],
            "columns": [
                {"column_name": "PAT_ID", "column_type": "key",
                 "base_columns": ["table:PATIENT.PAT_ID"], "base_tables": ["PATIENT"]},
            ],
            "filters": [],
        },
    ],
}

SAMPLE_VIEW_INPATIENT = {
    "view_name": "VW_INPATIENT_DX",
    "scopes": [
        {
            "id": "main",
            "kind": "main",
            "reads_from_tables": ["EPIC.PATIENT", "PAT_ENC_HSP", "HSP_DX", "ZC_DX_TYPE"],
            "joins": [
                {"right_table": "PAT_ENC_HSP", "join_type": "INNER JOIN", "on_expression": ""},
                {"right_table": "HSP_DX", "join_type": "INNER JOIN", "on_expression": ""},
                {"right_table": "ZC_DX_TYPE", "join_type": "LEFT JOIN", "on_expression": ""},
            ],
            "reads_from_scopes": [],
            "columns": [
                {"column_name": "PAT_ID", "column_type": "key",
                 "base_columns": ["table:PATIENT.PAT_ID"], "base_tables": ["PATIENT"]},
            ],
            "filters": [],
        },
    ],
}


# ---------------------------------------------------------------------------
# projection.py -- table-only subgraph for community detection
# ---------------------------------------------------------------------------


class TestExtractTableProjection(unittest.TestCase):
    """The undirected weighted projection used for community detection."""

    def test_projection_contains_only_table_nodes(self):
        from tools.p20_index.graph_builder import build_graph
        from tools.p30_analyze.projection import extract_table_projection
        g = build_graph([SAMPLE_VIEW_CLINIC, SAMPLE_VIEW_INPATIENT])
        table_g = extract_table_projection(g)
        for node, attrs in table_g.nodes(data=True):
            self.assertEqual(
                attrs.get("ntype"), "table",
                f"Projection should contain only tables, found {attrs}"
            )

    def test_projection_aggregates_weights(self):
        """PATIENT and ZC_DX_TYPE co-occur in BOTH views -> edge weight 2."""
        from tools.p20_index.graph_builder import build_graph
        from tools.p30_analyze.projection import extract_table_projection
        g = build_graph([SAMPLE_VIEW_CLINIC, SAMPLE_VIEW_INPATIENT])
        table_g = extract_table_projection(g)
        weight = table_g["table::PATIENT"]["table::ZC_DX_TYPE"].get("weight")
        self.assertEqual(
            weight, 2,
            "Edge weight should equal the number of views in which "
            "two tables co-occur (here: 2 views)"
        )


# ---------------------------------------------------------------------------
# bridges.py -- high-degree dimension detection
# ---------------------------------------------------------------------------


class TestDetectBridgeTables(unittest.TestCase):
    """Tables with very high degree should be classified as bridges."""

    def test_high_degree_table_is_flagged_as_bridge(self):
        """A constructed graph where PATIENT connects to all other tables
        should classify PATIENT as a bridge."""
        import networkx as nx
        from tools.p30_analyze.bridges import detect_bridge_tables

        g = nx.Graph()
        g.add_node("table::PATIENT", ntype="table", label="PATIENT")
        for i in range(10):
            g.add_node(f"table::T{i}", ntype="table", label=f"T{i}")
            g.add_edge("table::PATIENT", f"table::T{i}", weight=1)
        # T0..T9 don't connect to each other; PATIENT is the only hub.
        bridges = detect_bridge_tables(g, percentile=90.0)
        self.assertIn("table::PATIENT", bridges)

    def test_empty_graph_returns_empty_set(self):
        import networkx as nx
        from tools.p30_analyze.bridges import detect_bridge_tables
        self.assertEqual(detect_bridge_tables(nx.Graph()), set())


class TestProjectWithoutBridges(unittest.TestCase):
    """project_without_bridges should leave the original untouched."""

    def test_removes_bridges_returns_new_graph(self):
        import networkx as nx
        from tools.p30_analyze.bridges import project_without_bridges

        g = nx.Graph()
        for node in ["A", "B", "C"]:
            g.add_node(node)
        g.add_edge("A", "B")
        g.add_edge("B", "C")

        result = project_without_bridges(g, {"B"})
        self.assertEqual(set(result.nodes), {"A", "C"})
        # Original unchanged.
        self.assertEqual(set(g.nodes), {"A", "B", "C"})


# ---------------------------------------------------------------------------
# communities.py -- Louvain wrapper
# ---------------------------------------------------------------------------


class TestDetectTableCommunities(unittest.TestCase):
    """Louvain results on the projection graph."""

    def test_communities_partition_all_tables(self):
        """Every table should belong to exactly one community."""
        from tools.p20_index.graph_builder import build_graph
        from tools.p30_analyze.projection import extract_table_projection
        from tools.p30_analyze.communities import detect_table_communities

        g = build_graph([SAMPLE_VIEW_CLINIC, SAMPLE_VIEW_INPATIENT])
        table_g = extract_table_projection(g)
        communities = detect_table_communities(table_g)
        # The union of all communities equals the set of table nodes.
        union = set()
        for community in communities:
            union |= community
        self.assertEqual(union, set(table_g.nodes))

    def test_communities_sorted_largest_first(self):
        from tools.p20_index.graph_builder import build_graph
        from tools.p30_analyze.projection import extract_table_projection
        from tools.p30_analyze.communities import detect_table_communities

        g = build_graph([SAMPLE_VIEW_CLINIC, SAMPLE_VIEW_INPATIENT])
        table_g = extract_table_projection(g)
        communities = detect_table_communities(table_g)
        sizes = [len(c) for c in communities]
        self.assertEqual(sizes, sorted(sizes, reverse=True))


# ---------------------------------------------------------------------------
# primary_community.py -- per-view assignment + cross-domain spans
# ---------------------------------------------------------------------------


class TestAssignViewsToCommunities(unittest.TestCase):
    """Each view should be assigned to exactly one primary community."""

    def test_view_with_tables_in_two_communities_has_one_primary(self):
        from tools.p20_index.graph_builder import build_graph
        from tools.p30_analyze.primary_community import assign_views_to_communities

        g = build_graph([SAMPLE_VIEW_CLINIC])
        # Manually construct two communities. The clinic view touches both.
        communities = [
            {"table::PATIENT", "table::PAT_ENC"},
            {"table::PAT_ENC_DX", "table::ZC_DX_TYPE"},
        ]
        primary, spans = assign_views_to_communities(g, communities)

        # Exactly one community should claim VW_CLINIC_DX as primary.
        primary_count = sum(
            1 for views in primary.values() if "VW_CLINIC_DX" in views
        )
        self.assertEqual(primary_count, 1)
        # The spans list records that the view touches both.
        self.assertEqual(spans["VW_CLINIC_DX"], [0, 1])

    def test_single_community_view_has_singleton_span(self):
        from tools.p20_index.graph_builder import build_graph
        from tools.p30_analyze.primary_community import assign_views_to_communities

        g = build_graph([SAMPLE_VIEW_CLINIC])
        # One mega-community containing every table the view touches.
        communities = [
            {"table::PATIENT", "table::PAT_ENC",
             "table::PAT_ENC_DX", "table::ZC_DX_TYPE"},
        ]
        _, spans = assign_views_to_communities(g, communities)
        # Single-domain view: spans = [0] (touches only community 0).
        self.assertEqual(spans["VW_CLINIC_DX"], [0])


# ---------------------------------------------------------------------------
# community_analysis.py -- per-community summary
# ---------------------------------------------------------------------------


class TestAnalyzeCommunity(unittest.TestCase):
    """Per-community summary: top tables, leaf tables, core tables."""

    def test_returns_expected_keys(self):
        from tools.p20_index.graph_builder import build_graph
        from tools.p30_analyze.community_analysis import analyze_community

        g = build_graph([SAMPLE_VIEW_CLINIC])
        result = analyze_community(
            g,
            community_tables={"table::PATIENT", "table::PAT_ENC", "table::ZC_DX_TYPE"},
            primary_views={"VW_CLINIC_DX"},
        )
        expected_keys = {
            "n_tables", "n_primary_views", "top_tables", "leaf_tables",
            "core_tables", "primary_views", "zc_table_count", "table_node_ids",
        }
        self.assertEqual(set(result.keys()), expected_keys)
        self.assertEqual(result["n_tables"], 3)
        self.assertEqual(result["n_primary_views"], 1)
        self.assertEqual(result["primary_views"], ["VW_CLINIC_DX"])

    def test_zc_table_count_correct(self):
        from tools.p20_index.graph_builder import build_graph
        from tools.p30_analyze.community_analysis import analyze_community

        g = build_graph([SAMPLE_VIEW_CLINIC])
        result = analyze_community(
            g,
            community_tables={"table::ZC_DX_TYPE", "table::PAT_ENC"},
            primary_views=set(),
        )
        self.assertEqual(result["zc_table_count"], 1)


if __name__ == "__main__":
    unittest.main()
