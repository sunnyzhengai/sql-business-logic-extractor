"""Unit tests for tools.operate.validate_graph_pivot.

Run from the repo root:
    python -m unittest tools.operate.tests.test_validate_graph_pivot
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path


# ----------------------------------------------------------------------------
# Fixture corpora -- two synthetic views with overlapping tables.
# ----------------------------------------------------------------------------
#
# View A: clinic-side diagnosis path
#   main: FROM PATIENT JOIN PAT_ENC JOIN PAT_ENC_DX JOIN ZC_DX_TYPE
# View B: inpatient-side diagnosis path
#   main: FROM PATIENT JOIN PAT_ENC_HSP JOIN HSP_DX JOIN ZC_DX_TYPE
#
# Expected: PATIENT and ZC_DX_TYPE are shared "bridge" tables; the clinic and
# inpatient paths form two table neighborhoods. Even with only two views we
# should get a clean graph with the right schema.

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
                {
                    "column_name": "PAT_ID",
                    "column_type": "key",
                    "base_columns": ["table:PATIENT.PAT_ID"],
                    "base_tables": ["PATIENT"],
                },
                {
                    "column_name": "DX_NAME",
                    "column_type": "label",
                    "base_columns": ["table:ZC_DX_TYPE.NAME"],
                    "base_tables": ["ZC_DX_TYPE"],
                },
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
                {
                    "column_name": "PAT_ID",
                    "column_type": "key",
                    "base_columns": ["table:PATIENT.PAT_ID"],
                    "base_tables": ["PATIENT"],
                },
                {
                    "column_name": "DX_NAME",
                    "column_type": "label",
                    "base_columns": ["table:ZC_DX_TYPE.NAME"],
                    "base_tables": ["ZC_DX_TYPE"],
                },
            ],
            "filters": [],
        },
    ],
}


# NOTE: corpus loading is tested in tools.shared.tests.test_corpus_io
# (moved there in Phase 2a). The `load_corpus` function lives in
# tools.shared.corpus_io and is imported here for the end-to-end test
# below.


class TestGraphConstruction(unittest.TestCase):
    """Schema invariants on the built graph."""

    def test_build_graph_produces_all_node_types(self):
        from tools.operate.validate_graph_pivot import build_graph
        g = build_graph([SAMPLE_VIEW_CLINIC, SAMPLE_VIEW_INPATIENT])
        node_types = {attrs.get("ntype") for _, attrs in g.nodes(data=True)}
        self.assertIn("view", node_types)
        self.assertIn("scope", node_types)
        self.assertIn("table", node_types)
        self.assertIn("column", node_types)

    def test_table_nodes_are_global_across_views(self):
        """PATIENT should be a single node referenced by both views."""
        from tools.operate.validate_graph_pivot import build_graph
        g = build_graph([SAMPLE_VIEW_CLINIC, SAMPLE_VIEW_INPATIENT])
        patient_nodes = [n for n, a in g.nodes(data=True)
                          if a.get("ntype") == "table" and a.get("label") == "PATIENT"]
        self.assertEqual(len(patient_nodes), 1,
                          "PATIENT should be a single GLOBAL node, not duplicated per view")

    def test_zc_tables_are_flagged(self):
        from tools.operate.validate_graph_pivot import build_graph
        g = build_graph([SAMPLE_VIEW_CLINIC])
        zc_nodes = [a for _, a in g.nodes(data=True)
                     if a.get("ntype") == "table" and a.get("is_zc")]
        self.assertEqual(len(zc_nodes), 1)
        self.assertEqual(zc_nodes[0]["label"], "ZC_DX_TYPE")

    def test_join_edges_carry_view_and_scope_provenance(self):
        """Each JOIN edge must record which view and scope produced it."""
        from tools.operate.validate_graph_pivot import build_graph
        g = build_graph([SAMPLE_VIEW_CLINIC])
        join_edges = [(u, v, a) for u, v, a in g.edges(data=True)
                        if a.get("relation") == "JOIN"]
        self.assertGreater(len(join_edges), 0)
        for u, v, attrs in join_edges:
            self.assertEqual(attrs.get("view"), "VW_CLINIC_DX")
            self.assertEqual(attrs.get("scope"), "main")
            self.assertIn("join_type", attrs)

    def test_co_occurrence_edges_link_all_table_pairs_in_scope(self):
        """Within one scope, every pair of tables should have a CO_OCCURS edge."""
        from tools.operate.validate_graph_pivot import build_graph
        g = build_graph([SAMPLE_VIEW_CLINIC])
        co_edges = [(u, v) for u, v, a in g.edges(data=True)
                     if a.get("relation") == "CO_OCCURS_IN_SCOPE"]
        # The clinic view has 4 tables: PATIENT, PAT_ENC, PAT_ENC_DX, ZC_DX_TYPE.
        # That's C(4,2) = 6 unordered pairs, expressed as 6 directed edges.
        self.assertEqual(len(co_edges), 6)


class TestTableProjection(unittest.TestCase):
    """The undirected weighted projection used for community detection."""

    def test_projection_contains_only_table_nodes(self):
        from tools.operate.validate_graph_pivot import (
            build_graph, extract_table_projection,
        )
        g = build_graph([SAMPLE_VIEW_CLINIC, SAMPLE_VIEW_INPATIENT])
        table_g = extract_table_projection(g)
        for node, attrs in table_g.nodes(data=True):
            self.assertEqual(attrs.get("ntype"), "table",
                              f"Projection should contain only tables, found {attrs}")

    def test_projection_aggregates_weights(self):
        """If PATIENT and ZC_DX_TYPE co-occur in BOTH views, the edge weight is 2."""
        from tools.operate.validate_graph_pivot import (
            build_graph, extract_table_projection,
        )
        g = build_graph([SAMPLE_VIEW_CLINIC, SAMPLE_VIEW_INPATIENT])
        table_g = extract_table_projection(g)
        weight = table_g["table::PATIENT"]["table::ZC_DX_TYPE"].get("weight")
        self.assertEqual(weight, 2,
                          "Edge weight should equal the number of views in which "
                          "two tables co-occur (here: 2 views)")


class TestCommunityDetection(unittest.TestCase):
    """Louvain results on the projection graph."""

    def test_communities_partition_all_tables(self):
        """Every table should belong to exactly one community."""
        from tools.operate.validate_graph_pivot import (
            build_graph, extract_table_projection, detect_table_communities,
        )
        g = build_graph([SAMPLE_VIEW_CLINIC, SAMPLE_VIEW_INPATIENT])
        table_g = extract_table_projection(g)
        communities = detect_table_communities(table_g)
        # The union of all communities should equal the set of table nodes.
        union = set()
        for community in communities:
            union |= community
        all_tables = set(table_g.nodes)
        self.assertEqual(union, all_tables)


# NOTE: infrastructure-view filtering is tested in
# tools.shared.tests.test_view_filter (moved there in Phase 2a).
# The end-to-end test below exercises the filter via run_validation.


class TestBridgeDetection(unittest.TestCase):
    """Tables with very high degree should be classified as bridges."""

    def test_high_degree_table_is_flagged_as_bridge(self):
        """A constructed graph where PATIENT connects to all other tables
        should classify PATIENT as a bridge."""
        import networkx as nx
        from tools.operate.validate_graph_pivot import detect_bridge_tables

        g = nx.Graph()
        g.add_node("table::PATIENT", ntype="table", label="PATIENT")
        for i in range(10):
            g.add_node(f"table::T{i}", ntype="table", label=f"T{i}")
            g.add_edge("table::PATIENT", f"table::T{i}", weight=1)
        # T0..T9 don't connect to each other; PATIENT is the only hub.
        bridges = detect_bridge_tables(g, percentile=90.0)
        self.assertIn("table::PATIENT", bridges)


class TestPrimaryCommunityAssignment(unittest.TestCase):
    """Each view should be assigned to exactly one primary community."""

    def test_view_with_tables_in_two_communities_has_one_primary(self):
        from tools.operate.validate_graph_pivot import (
            build_graph, assign_views_to_communities,
        )
        g = build_graph([SAMPLE_VIEW_CLINIC])
        # Manually construct two communities: one containing PATIENT+PAT_ENC,
        # one containing PAT_ENC_DX+ZC_DX_TYPE. The view touches both, but
        # should be assigned a single primary.
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
        # And the spans list should record that it touches both.
        self.assertEqual(spans["VW_CLINIC_DX"], [0, 1])


class TestEndToEndOrchestration(unittest.TestCase):
    """Verify the full run_validation pipeline produces all artifacts."""

    def test_run_validation_writes_all_artifacts(self):
        from tools.operate.validate_graph_pivot import run_validation
        with tempfile.TemporaryDirectory() as d:
            corpus_path = Path(d) / "corpus.jsonl"
            output_dir = Path(d) / "out"
            with corpus_path.open("w", encoding="utf-8") as f:
                f.write(json.dumps({"schema_version": 3, "n_views": 2}) + "\n")
                f.write(json.dumps(SAMPLE_VIEW_CLINIC) + "\n")
                f.write(json.dumps(SAMPLE_VIEW_INPATIENT) + "\n")
            result = run_validation(corpus_path, output_dir)
            # All file-existence assertions must happen INSIDE the with block;
            # otherwise the TemporaryDirectory context cleans up the files
            # before we can check them.
            self.assertTrue(Path(result["graph_html"]).is_file())
            self.assertTrue(Path(result["communities_index_html"]).is_file())
            self.assertTrue(Path(result["communities_md"]).is_file())
            self.assertTrue(Path(result["validation_report"]).is_file())
            # The communities/ dir should contain at least one per-community HTML.
            community_htmls = list((output_dir / "communities").glob("community_*.html"))
            self.assertGreater(len(community_htmls), 0)
            # Sanity-check the report content.
            report = Path(result["validation_report"]).read_text(encoding="utf-8")
            self.assertIn("Verdict", report)
            self.assertIn("Bridge tables", report)
            self.assertIn("Cross-domain views", report)

    def test_run_validation_excludes_infrastructure_views(self):
        """End-to-end: a corpus with one business view and one Collibra view
        should report 1 business view + 1 excluded."""
        from tools.operate.validate_graph_pivot import run_validation
        with tempfile.TemporaryDirectory() as d:
            corpus_path = Path(d) / "corpus.jsonl"
            output_dir = Path(d) / "out"
            collibra_view = {
                "view_name": "VW_COLLIBRA_TABLE_INVENTORY",
                "scopes": [{
                    "id": "main", "kind": "main",
                    "reads_from_tables": ["sys.tables"],
                    "joins": [], "reads_from_scopes": [],
                    "columns": [], "filters": [],
                }],
            }
            with corpus_path.open("w", encoding="utf-8") as f:
                f.write(json.dumps({"schema_version": 3, "n_views": 2}) + "\n")
                f.write(json.dumps(SAMPLE_VIEW_CLINIC) + "\n")
                f.write(json.dumps(collibra_view) + "\n")
            result = run_validation(corpus_path, output_dir)
            self.assertEqual(result["n_views_total"], 2)
            self.assertEqual(result["n_views_business"], 1)
            self.assertEqual(result["n_views_excluded"], 1)


if __name__ == "__main__":
    unittest.main()
