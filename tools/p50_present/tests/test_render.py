"""Smoke tests for tools.p50_present.render.

Run from the repo root:
    python -m unittest tools.p50_present.tests.test_render

Verifies that render_pyvis writes a valid self-contained HTML file
and that export_graphml writes a valid GraphML file. The render
functions take a graph as input; we build one via
tools.p20_index.graph_builder for these tests.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path


# Fixture view used by both render tests. Duplicates the one in
# tools/p20_index/tests/test_graph_builder.py (intentionally; small
# fixtures, simpler than a shared module).
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


class TestRender(unittest.TestCase):
    def test_render_pyvis_writes_html(self):
        # Build a graph using p20_index, then render with p50_present.
        # This is the natural cross-phase test: p50_present consumes graphs
        # produced by p20_index.
        from tools.p20_index.graph_builder import build_graph
        from tools.p50_present.render import render_pyvis
        g = build_graph([SAMPLE_VIEW_A])
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "view.html"
            render_pyvis(g, out)
            self.assertTrue(out.is_file())
            txt = out.read_text(encoding="utf-8")
            self.assertIn("VW_PATIENT_COVERAGE", txt)

    def test_export_graphml_writes_xml(self):
        from tools.p20_index.graph_builder import build_graph
        from tools.p50_present.render import export_graphml
        g = build_graph([SAMPLE_VIEW_A])
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "view.graphml"
            export_graphml(g, out)
            self.assertTrue(out.is_file())
            txt = out.read_text(encoding="utf-8")
            self.assertIn("<graphml", txt)


if __name__ == "__main__":
    unittest.main()
