"""Smoke tests for tools.p20_index.graph_builder.

Run from the repo root:
    python -m unittest tools.p20_index.tests.test_graph_builder

Asserts the basic shape of the graphs produced by build_view_graph,
build_cluster_graph, and build_corpus_graph: typed nodes, expected
edge relations, global table nodes across views, ZC flag detection,
and view filtering on corpus load.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path


# Fixture views used by every test below. Shared with the rendering
# tests in tools/p50_present/tests/test_render.py (intentionally
# duplicated -- small fixtures, simpler than a shared module).
SAMPLE_VIEW_A = {
    "view_name": "VW_PATIENT_COVERAGE",
    "scopes": [
        {
            "id": "main",
            "kind": "main",
            "reads_from_tables": ["EPIC.PATIENT", "EPIC.COVERAGE"],
            "joins": [
                {"right_table": "EPIC.COVERAGE", "join_type": "INNER JOIN"},
                {"right_table": "ZC_CLM_AP_STAT", "join_type": "LEFT JOIN"},
            ],
            "reads_from_scopes": ["cohort"],
            "columns": [
                {
                    "column_name": "PAT_ID",
                    "column_type": "key",
                    "business_description": "Patient identifier",
                    "technical_description": "PATIENT.PAT_ID",
                    "base_columns": ["table:PATIENT.PAT_ID"],
                },
                {
                    "column_name": "STATUS",
                    "column_type": "label",
                    "business_description": "Coverage status name",
                    "technical_description": "ZC.NAME",
                    "base_columns": ["cte:cohort.STATUS_C"],
                },
            ],
            "filters": [
                {
                    "kind": "where",
                    "expression": "PATIENT.ACTIVE_C = 1",
                    "english": "Active patients only",
                },
            ],
        },
        {
            "id": "cohort",
            "kind": "cte",
            "reads_from_tables": ["EPIC.COVERAGE"],
            "joins": [],
            "reads_from_scopes": [],
            "columns": [
                {
                    "column_name": "STATUS_C",
                    "column_type": "code",
                    "base_columns": ["table:COVERAGE.STATUS_C"],
                },
            ],
            "filters": [],
        },
    ],
}


SAMPLE_VIEW_B = {
    "view_name": "VW_PATIENT_DEMO",
    "scopes": [
        {
            "id": "main",
            "kind": "main",
            "reads_from_tables": ["EPIC.PATIENT"],
            "joins": [],
            "reads_from_scopes": [],
            "columns": [
                {
                    "column_name": "PAT_ID",
                    "column_type": "key",
                    "base_columns": ["table:PATIENT.PAT_ID"],
                },
            ],
            "filters": [],
        },
    ],
}


class TestBuild(unittest.TestCase):
    def test_build_view_graph_has_typed_nodes(self):
        from tools.p20_index.graph_builder import build_view_graph
        g = build_view_graph(SAMPLE_VIEW_A)

        types = {a.get("ntype") for _, a in g.nodes(data=True)}
        self.assertIn("view", types)
        self.assertIn("scope", types)
        self.assertIn("column", types)
        self.assertIn("table", types)
        self.assertIn("filter", types)

    def test_view_graph_has_expected_edges(self):
        from tools.p20_index.graph_builder import build_view_graph
        g = build_view_graph(SAMPLE_VIEW_A)
        relations = {a.get("relation") for _, _, a in g.edges(data=True)}
        # All these relations should appear at least once.
        for r in [
            "HAS_SCOPE", "READS_FROM_TABLE", "JOINS",
            "CONTAINS_COLUMN", "DERIVED_FROM", "REFERENCES_TABLE",
            "READS_FROM_SCOPE", "HAS_FILTER",
        ]:
            self.assertIn(r, relations, f"missing relation {r}")

    def test_table_nodes_are_global(self):
        """PATIENT should be a single node even when both views reference it."""
        from tools.p20_index.graph_builder import build_cluster_graph
        g = build_cluster_graph([SAMPLE_VIEW_A, SAMPLE_VIEW_B])
        patient_nodes = [n for n, a in g.nodes(data=True)
                          if a.get("ntype") == "table" and a.get("label") == "PATIENT"]
        self.assertEqual(len(patient_nodes), 1,
                          "PATIENT table should be a single global node")

    def test_zc_table_flagged(self):
        from tools.p20_index.graph_builder import build_view_graph
        g = build_view_graph(SAMPLE_VIEW_A)
        zc = [n for n, a in g.nodes(data=True)
                if a.get("ntype") == "table" and a.get("label") == "ZC_CLM_AP_STAT"]
        self.assertEqual(len(zc), 1)
        self.assertTrue(g.nodes[zc[0]].get("is_zc"))

    def test_build_corpus_graph_with_filter(self):
        from tools.p20_index.graph_builder import build_corpus_graph
        with tempfile.TemporaryDirectory() as d:
            corpus = Path(d) / "corpus.jsonl"
            with corpus.open("w", encoding="utf-8") as f:
                f.write(json.dumps({"_header": "test"}) + "\n")
                f.write(json.dumps(SAMPLE_VIEW_A) + "\n")
                f.write(json.dumps(SAMPLE_VIEW_B) + "\n")
            g = build_corpus_graph(corpus, view_filter=["VW_PATIENT_DEMO"])
            view_nodes = [n for n, a in g.nodes(data=True) if a.get("ntype") == "view"]
            self.assertEqual(view_nodes, ["VW_PATIENT_DEMO"])


if __name__ == "__main__":
    unittest.main()
