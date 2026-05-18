"""Tests for tools.p50_present.view_html.

Run from the repo root:
    python -m unittest tools.p50_present.tests.test_view_html
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path


# Same fixture used in p30_analyze view-membership tests.
SAMPLE_VIEW_CLINIC = {
    "view_name": "VW_CLINIC_DX",
    "scopes": [{
        "id": "main",
        "kind": "main",
        "reads_from_tables": ["EPIC.PATIENT", "PAT_ENC", "PAT_ENC_DX", "ZC_DX_TYPE"],
        "joins": [
            {"right_table": "PAT_ENC", "join_type": "INNER JOIN", "on_expression": ""},
            {"right_table": "PAT_ENC_DX", "join_type": "INNER JOIN", "on_expression": ""},
            {"right_table": "ZC_DX_TYPE", "join_type": "LEFT JOIN", "on_expression": ""},
        ],
        "reads_from_scopes": [],
        "columns": [],
        "filters": [],
    }],
}


class TestRenderViewHtml(unittest.TestCase):

    def test_renders_html_with_view_name(self):
        from tools.p20_index.graph_builder import build_graph
        from tools.p50_present.view_html import render_view_html

        g = build_graph([SAMPLE_VIEW_CLINIC])
        communities = [{"table::PAT_ENC", "table::PAT_ENC_DX", "table::ZC_DX_TYPE"}]
        bridges = {"table::PATIENT"}

        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "view.html"
            result = render_view_html(
                g=g,
                view_name="VW_CLINIC_DX",
                communities=communities,
                bridge_tables=bridges,
                output_path=out,
                driver_label="PATIENT",
            )
            self.assertTrue(Path(result).is_file())
            content = Path(result).read_text(encoding="utf-8")
            # The view's tables should appear as labels in the rendered HTML.
            self.assertIn("PATIENT", content)
            self.assertIn("PAT_ENC", content)
            self.assertIn("ZC_DX_TYPE", content)

    def test_view_with_no_tables_returns_stub_html(self):
        from tools.p20_index.graph_builder import build_graph
        from tools.p50_present.view_html import render_view_html

        # An "empty" view -- main scope with no tables, no joins.
        empty_view = {
            "view_name": "VW_EMPTY",
            "scopes": [{
                "id": "main", "kind": "main",
                "reads_from_tables": [], "joins": [],
                "reads_from_scopes": [], "columns": [], "filters": [],
            }],
        }
        g = build_graph([empty_view])
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "empty.html"
            result = render_view_html(
                g=g, view_name="VW_EMPTY",
                communities=[], bridge_tables=set(),
                output_path=out,
            )
            self.assertTrue(Path(result).is_file())
            content = Path(result).read_text(encoding="utf-8")
            self.assertIn("VW_EMPTY", content)
            # Stub should mention "no detectable" or similar.
            self.assertIn("no detectable", content.lower())


class TestViewHtmlFilename(unittest.TestCase):

    def test_safe_filename_format(self):
        from tools.p50_present.view_html import view_html_filename

        name = view_html_filename("VW_CLINIC_DX")
        # Lowercased, safe characters, prefixed with view_
        self.assertTrue(name.startswith("view_"))
        self.assertTrue(name.endswith(".html"))
        # No spaces, dots, etc. in the middle (between view_ and .html).
        middle = name[len("view_"):-len(".html")]
        for ch in middle:
            self.assertTrue(ch.isalnum() or ch == "_",
                              f"unexpected character {ch!r} in {name}")


if __name__ == "__main__":
    unittest.main()
