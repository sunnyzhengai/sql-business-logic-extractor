"""Tests for tools.p20_index.graph_builder.

Run from the repo root:
    python -m unittest tools.p20_index.tests.test_graph_builder

Asserts the contract of build_graph and build_corpus_graph:

  - All four node types appear (view, scope, table, column)
  - Tables are GLOBAL across views (one PATIENT node, even if many views
    reference it)
  - ZC_* tables are flagged via the is_zc node attribute
  - JOIN edges carry view + scope provenance + join_type
  - CO_OCCURS_IN_SCOPE edges link every pair of tables in a scope
    (this is the input to community detection in p30_analyze)
  - build_corpus_graph honors a view_filter when loading from a path
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path


# ---------------------------------------------------------------------------
# Fixture views -- two synthetic views with overlapping tables.
# ---------------------------------------------------------------------------
# These mirror the fixtures used in the operate-layer validation tests.
# The clinic + inpatient pair gives us:
#  - A shared table (PATIENT) that should be a single global node.
#  - A shared ZC table (ZC_DX_TYPE) to exercise the is_zc flag.
#  - Different "fact" paths (PAT_ENC vs PAT_ENC_HSP) -- exactly the kind
#    of structure community detection should pull apart in production.


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


class TestBuildGraph(unittest.TestCase):
    """Schema invariants on the typed graph."""

    def test_produces_all_node_types(self):
        from tools.p20_index.graph_builder import build_graph
        g = build_graph([SAMPLE_VIEW_CLINIC, SAMPLE_VIEW_INPATIENT])
        node_types = {attrs.get("ntype") for _, attrs in g.nodes(data=True)}
        self.assertIn("view", node_types)
        self.assertIn("scope", node_types)
        self.assertIn("table", node_types)
        self.assertIn("column", node_types)

    def test_table_nodes_are_global_across_views(self):
        """PATIENT should be a single node referenced by both views."""
        from tools.p20_index.graph_builder import build_graph
        g = build_graph([SAMPLE_VIEW_CLINIC, SAMPLE_VIEW_INPATIENT])
        patient_nodes = [
            n for n, a in g.nodes(data=True)
            if a.get("ntype") == "table" and a.get("label") == "PATIENT"
        ]
        self.assertEqual(len(patient_nodes), 1,
                          "PATIENT should be a single GLOBAL node, not duplicated per view")

    def test_zc_tables_are_flagged(self):
        from tools.p20_index.graph_builder import build_graph
        g = build_graph([SAMPLE_VIEW_CLINIC])
        zc_nodes = [
            a for _, a in g.nodes(data=True)
            if a.get("ntype") == "table" and a.get("is_zc")
        ]
        self.assertEqual(len(zc_nodes), 1)
        self.assertEqual(zc_nodes[0]["label"], "ZC_DX_TYPE")

    def test_join_edges_carry_view_and_scope_provenance(self):
        """Each JOIN edge must record which view + scope produced it."""
        from tools.p20_index.graph_builder import build_graph
        g = build_graph([SAMPLE_VIEW_CLINIC])
        join_edges = [
            (u, v, a) for u, v, a in g.edges(data=True)
            if a.get("relation") == "JOIN"
        ]
        self.assertGreater(len(join_edges), 0)
        for u, v, attrs in join_edges:
            self.assertEqual(attrs.get("view"), "VW_CLINIC_DX")
            self.assertEqual(attrs.get("scope"), "main")
            self.assertIn("join_type", attrs)

    def test_co_occurrence_edges_link_all_table_pairs_in_scope(self):
        """Within one scope, every pair of tables gets a CO_OCCURS edge.

        This is the input to community detection: tables that frequently
        co-occur in scopes end up in the same community.
        """
        from tools.p20_index.graph_builder import build_graph
        g = build_graph([SAMPLE_VIEW_CLINIC])
        co_edges = [
            (u, v) for u, v, a in g.edges(data=True)
            if a.get("relation") == "CO_OCCURS_IN_SCOPE"
        ]
        # The clinic view has 4 tables: PATIENT, PAT_ENC, PAT_ENC_DX, ZC_DX_TYPE.
        # That is C(4, 2) = 6 unordered pairs, expressed as 6 directed edges
        # (we don't add the reverse direction; co-occurrence is symmetric in
        # meaning even though the edge is recorded once per pair).
        self.assertEqual(len(co_edges), 6)


class TestBuildCorpusGraph(unittest.TestCase):
    """The path-based convenience wrapper around build_graph."""

    def test_view_filter_limits_to_named_views(self):
        from tools.p20_index.graph_builder import build_corpus_graph
        with tempfile.TemporaryDirectory() as d:
            corpus = Path(d) / "corpus.jsonl"
            with corpus.open("w", encoding="utf-8") as f:
                f.write(json.dumps({"schema_version": 3, "n_views": 2}) + "\n")
                f.write(json.dumps(SAMPLE_VIEW_CLINIC) + "\n")
                f.write(json.dumps(SAMPLE_VIEW_INPATIENT) + "\n")
            g = build_corpus_graph(corpus, view_filter=["VW_INPATIENT_DX"])
            view_nodes = [
                a.get("label") for _, a in g.nodes(data=True)
                if a.get("ntype") == "view"
            ]
            self.assertEqual(view_nodes, ["VW_INPATIENT_DX"])

    def test_no_filter_includes_all_views(self):
        from tools.p20_index.graph_builder import build_corpus_graph
        with tempfile.TemporaryDirectory() as d:
            corpus = Path(d) / "corpus.jsonl"
            with corpus.open("w", encoding="utf-8") as f:
                f.write(json.dumps({"schema_version": 3, "n_views": 2}) + "\n")
                f.write(json.dumps(SAMPLE_VIEW_CLINIC) + "\n")
                f.write(json.dumps(SAMPLE_VIEW_INPATIENT) + "\n")
            g = build_corpus_graph(corpus)
            view_nodes = sorted(
                a.get("label") for _, a in g.nodes(data=True)
                if a.get("ntype") == "view"
            )
            self.assertEqual(view_nodes, ["VW_CLINIC_DX", "VW_INPATIENT_DX"])


if __name__ == "__main__":
    unittest.main()
