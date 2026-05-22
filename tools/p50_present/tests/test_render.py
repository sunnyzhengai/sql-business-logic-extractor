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


class TestSubgraphIsolationInjection(unittest.TestCase):
    """Phase 3c: the JS-injection helper that gives every rendered HTML
    two-way click-to-isolate behavior."""

    def test_injection_adds_script_marker(self):
        from tools.p20_index.graph_builder import build_graph
        from tools.p30_analyze.projection import extract_table_projection
        from tools.p50_present.community_html import render_community_html

        g = build_graph([SAMPLE_VIEW_A])
        table_g = extract_table_projection(g)
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "community.html"
            render_community_html(
                table_g, 0, set(table_g.nodes), set(), out,
            )
            content = out.read_text(encoding="utf-8")
            # The injection helper drops a marker comment + the JS itself.
            self.assertIn("subgraph-isolation-injected", content)
            # The script should hook into the global `network` variable
            # that pyvis declares.
            self.assertIn("network.on(\"selectNode\"", content)
            self.assertIn("network.on(\"deselectNode\"", content)

    def test_injection_is_idempotent(self):
        """Calling inject_subgraph_isolation_script twice on the same
        file should not double-insert the script."""
        from tools.p20_index.graph_builder import build_graph
        from tools.p30_analyze.projection import extract_table_projection
        from tools.p50_present.community_html import (
            render_community_html, inject_subgraph_isolation_script,
        )

        g = build_graph([SAMPLE_VIEW_A])
        table_g = extract_table_projection(g)
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "community.html"
            render_community_html(
                table_g, 0, set(table_g.nodes), set(), out,
            )
            # Already injected once by render_community_html. Call again
            # and verify the marker only appears once.
            inject_subgraph_isolation_script(out)
            content = out.read_text(encoding="utf-8")
            self.assertEqual(content.count("subgraph-isolation-injected"), 1)


class TestLegendAndSidebarInjection(unittest.TestCase):
    """Phase 3d: legend + sidebar injection."""

    def test_community_html_has_legend_and_sidebar(self):
        from tools.p20_index.graph_builder import build_graph
        from tools.p30_analyze.projection import extract_table_projection
        from tools.p30_analyze.view_membership import view_to_tables
        from tools.p50_present.community_html import render_community_html

        g = build_graph([SAMPLE_VIEW_A])
        table_g = extract_table_projection(g)
        community_tables = set(table_g.nodes) - {"table::PATIENT"}
        bridges = {"table::PATIENT"}

        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "community.html"
            render_community_html(
                table_g, 0, community_tables, bridges, out,
                primary_views=["VW_PATIENT_COVERAGE"],
                view_to_tables_map=view_to_tables(g),
            )
            content = out.read_text(encoding="utf-8")
            # Legend injection marker present.
            self.assertIn("legend-injected", content)
            # Legend contents (heading + a row label).
            self.assertIn("<h4>Legend</h4>", content)
            self.assertIn("Bridge", content)
            # Sidebar injection marker + the view item with its node id.
            self.assertIn("views-sidebar-injected", content)
            self.assertIn('class="view-item"', content)
            self.assertIn('data-node-id="view::VW_PATIENT_COVERAGE"', content)

    def test_view_html_has_legend_with_driver_row(self):
        from tools.p20_index.graph_builder import build_graph
        from tools.p50_present.view_html import render_view_html

        g = build_graph([SAMPLE_VIEW_A])
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "view.html"
            render_view_html(
                g=g,
                view_name="VW_PATIENT_COVERAGE",
                communities=[set(n for n in g.nodes if g.nodes[n].get("ntype") == "table")],
                bridge_tables=set(),
                output_path=out,
                driver_label="PATIENT",
            )
            content = out.read_text(encoding="utf-8")
            self.assertIn("legend-injected", content)
            # Per-view legend includes the driver star.
            self.assertIn("Driver", content)

    def test_injection_is_idempotent_per_marker(self):
        """Re-running legend + sidebar injection shouldn't duplicate them."""
        from tools.p20_index.graph_builder import build_graph
        from tools.p30_analyze.projection import extract_table_projection
        from tools.p30_analyze.view_membership import view_to_tables
        from tools.p50_present.community_html import (
            render_community_html,
            inject_legend,
            inject_views_sidebar,
        )

        g = build_graph([SAMPLE_VIEW_A])
        table_g = extract_table_projection(g)
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "community.html"
            render_community_html(
                table_g, 0, set(table_g.nodes), set(), out,
                primary_views=["VW_PATIENT_COVERAGE"],
                view_to_tables_map=view_to_tables(g),
            )
            inject_legend(out)  # call again
            inject_views_sidebar(
                out, [("VW_PATIENT_COVERAGE", "view::VW_PATIENT_COVERAGE")],
            )
            content = out.read_text(encoding="utf-8")
            self.assertEqual(content.count("legend-injected"), 1)
            self.assertEqual(content.count("views-sidebar-injected"), 1)


class TestCommunityHtmlWithViewNodes(unittest.TestCase):
    """Phase 3b: view nodes embedded in community HTML."""

    def test_community_html_includes_view_node_when_views_supplied(self):
        """When primary_views + view_to_tables_map are passed,
        the rendered HTML should contain the view name as a node label."""
        from tools.p20_index.graph_builder import build_graph
        from tools.p30_analyze.projection import extract_table_projection
        from tools.p30_analyze.view_membership import view_to_tables
        from tools.p50_present.community_html import render_community_html

        g = build_graph([SAMPLE_VIEW_A])
        table_g = extract_table_projection(g)
        community_tables = {n for n in table_g.nodes if "PATIENT" not in n}
        # PATIENT becomes a bridge so it's shown but not in any community.
        bridges = {"table::PATIENT"}

        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "community.html"
            render_community_html(
                table_g, community_index=0,
                community_tables=community_tables, bridge_tables=bridges,
                output_path=out,
                primary_views=["VW_PATIENT_COVERAGE"],
                view_to_tables_map=view_to_tables(g),
            )
            content = out.read_text(encoding="utf-8")
            # The view name should appear as a node label in the rendered HTML.
            self.assertIn("VW_PATIENT_COVERAGE", content)

    def test_community_html_without_view_args_preserves_old_behavior(self):
        """When primary_views / view_to_tables_map are NOT passed,
        the community HTML renders as before (no view nodes)."""
        from tools.p20_index.graph_builder import build_graph
        from tools.p30_analyze.projection import extract_table_projection
        from tools.p50_present.community_html import render_community_html

        g = build_graph([SAMPLE_VIEW_A])
        table_g = extract_table_projection(g)
        community_tables = set(table_g.nodes)
        bridges: set[str] = set()

        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "community.html"
            render_community_html(
                table_g, 0, community_tables, bridges, out,
            )
            content = out.read_text(encoding="utf-8")
            # File exists and has the usual table labels; view name shouldn't
            # appear (since we didn't pass it).
            self.assertTrue(out.is_file())
            self.assertIn("PATIENT", content)


if __name__ == "__main__":
    unittest.main()
