"""Tests for tools.p50_present.community_overview -- the per-community
"big picture" artifact (frequency-colored substrate + view stripes)."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tools.p50_present.community_overview import (
    build_community_substrate,
    frequency_layout,
    render_substrate_svg,
    render_view_stripe_svg,
    write_community_overview,
)


# ---------------------------------------------------------------------------
# Mock corpus: three views with a shared core + per-view variations.
# - PAT_ENC + PATIENT used by ALL three (core).
# - CLARITY_DEP used by 2 of 3 (backbone).
# - ZC_PATIENT_STATUS used by 1 (outlier).
# ---------------------------------------------------------------------------

def _views_three_with_core_and_outlier() -> list[dict]:
    return [
        {
            "view_name": "VW_A",
            "scopes": [{
                "id": "main", "kind": "main",
                "reads_from_tables": ["PAT_ENC", "PATIENT", "CLARITY_DEP"],
                "reads_from_scopes": [], "columns": [],
                "joins": [
                    {"right_table": "PATIENT", "right_alias": "P",
                     "join_type": "INNER JOIN", "on_expression": "",
                     "columns": []},
                    {"right_table": "CLARITY_DEP", "right_alias": "D",
                     "join_type": "INNER JOIN", "on_expression": "",
                     "columns": []},
                ],
            }],
        },
        {
            "view_name": "VW_B",
            "scopes": [{
                "id": "main", "kind": "main",
                "reads_from_tables": ["PAT_ENC", "PATIENT", "CLARITY_DEP"],
                "reads_from_scopes": [], "columns": [],
                "joins": [
                    {"right_table": "PATIENT", "right_alias": "P",
                     "join_type": "INNER JOIN", "on_expression": "",
                     "columns": []},
                    {"right_table": "CLARITY_DEP", "right_alias": "D",
                     "join_type": "INNER JOIN", "on_expression": "",
                     "columns": []},
                ],
            }],
        },
        {
            "view_name": "VW_C",
            "scopes": [{
                "id": "main", "kind": "main",
                "reads_from_tables": ["PAT_ENC", "PATIENT", "ZC_PATIENT_STATUS"],
                "reads_from_scopes": [], "columns": [],
                "joins": [
                    {"right_table": "PATIENT", "right_alias": "P",
                     "join_type": "INNER JOIN", "on_expression": "",
                     "columns": []},
                    {"right_table": "ZC_PATIENT_STATUS", "right_alias": "Z",
                     "join_type": "LEFT JOIN", "on_expression": "",
                     "columns": []},
                ],
            }],
        },
    ]


# ---------------------------------------------------------------------------
# Substrate building + frequency tracking
# ---------------------------------------------------------------------------

class TestBuildSubstrate(unittest.TestCase):

    def test_nodes_union_across_views(self):
        nodes, _, _, _, _ = build_community_substrate(
            _views_three_with_core_and_outlier()
        )
        self.assertEqual(
            nodes,
            {"PAT_ENC", "PATIENT", "CLARITY_DEP", "ZC_PATIENT_STATUS"},
        )

    def test_node_frequency_counts_views_not_occurrences(self):
        _, _, node_freq, _, _ = build_community_substrate(
            _views_three_with_core_and_outlier()
        )
        # PAT_ENC and PATIENT are in every view -- freq = 3.
        self.assertEqual(node_freq["PAT_ENC"], 3)
        self.assertEqual(node_freq["PATIENT"], 3)
        # CLARITY_DEP in 2 views.
        self.assertEqual(node_freq["CLARITY_DEP"], 2)
        # ZC_PATIENT_STATUS in only one view.
        self.assertEqual(node_freq["ZC_PATIENT_STATUS"], 1)

    def test_edge_frequency_dedupes_within_a_view(self):
        """Even if a view references PATIENT in multiple scopes, the
        per-view edge set is computed once and the frequency bump is
        once. So edge_freq counts VIEWS containing the edge, not
        instances of the edge across scopes."""
        _, _, _, edge_freq, _ = build_community_substrate(
            _views_three_with_core_and_outlier()
        )
        # PAT_ENC <-> PATIENT is in all three views.
        edge = tuple(sorted(["PAT_ENC", "PATIENT"]))
        self.assertEqual(edge_freq[edge], 3)
        # PAT_ENC <-> CLARITY_DEP is only in 2 views.
        edge2 = tuple(sorted(["PAT_ENC", "CLARITY_DEP"]))
        self.assertEqual(edge_freq[edge2], 2)

    def test_per_view_set_returned(self):
        _, _, _, _, per_view = build_community_substrate(
            _views_three_with_core_and_outlier()
        )
        self.assertEqual(set(per_view.keys()), {"VW_A", "VW_B", "VW_C"})
        # VW_C is the outlier with ZC_PATIENT_STATUS.
        v_nodes, _ = per_view["VW_C"]
        self.assertIn("ZC_PATIENT_STATUS", v_nodes)
        self.assertNotIn("CLARITY_DEP", v_nodes)


# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------

class TestLayout(unittest.TestCase):

    def test_layout_deterministic_with_fixed_seed(self):
        nodes, edges, _, _, _ = build_community_substrate(
            _views_three_with_core_and_outlier()
        )
        a = frequency_layout(nodes, edges, seed=42)
        b = frequency_layout(nodes, edges, seed=42)
        self.assertEqual(a, b)

    def test_layout_covers_all_nodes(self):
        nodes, edges, _, _, _ = build_community_substrate(
            _views_three_with_core_and_outlier()
        )
        coords = frequency_layout(nodes, edges)
        self.assertEqual(set(coords.keys()), nodes)


# ---------------------------------------------------------------------------
# SVG rendering
# ---------------------------------------------------------------------------

class TestSubstrateSVG(unittest.TestCase):

    def test_substrate_includes_every_table_name(self):
        views = _views_three_with_core_and_outlier()
        nodes, edges, nf, ef, _ = build_community_substrate(views)
        coords = frequency_layout(nodes, edges)
        svg = render_substrate_svg(nodes, edges, coords, nf, ef, len(views))
        for tbl in ("PAT_ENC", "PATIENT", "CLARITY_DEP", "ZC_PATIENT_STATUS"):
            self.assertIn(tbl, svg)

    def test_core_has_higher_opacity_than_outlier(self):
        """The whole point of the artifact: core tables look more
        prominent than outliers. Extract the fill-opacity from a
        node we know is core (PAT_ENC, freq=3) vs an outlier
        (ZC_PATIENT_STATUS, freq=1) and check the relationship."""
        import re
        views = _views_three_with_core_and_outlier()
        nodes, edges, nf, ef, _ = build_community_substrate(views)
        coords = frequency_layout(nodes, edges)
        svg = render_substrate_svg(nodes, edges, coords, nf, ef, len(views))

        # Each node's <text> ends in `>{table_name}</text>` -- find
        # the fill-opacity on the preceding <circle> via a search.
        def opacity_for(tbl: str) -> float:
            text_pos = svg.index(f">{tbl}</text>")
            window = svg[:text_pos]
            m = re.findall(r'fill-opacity="([\d.]+)"', window)
            return float(m[-1]) if m else 0.0

        core = opacity_for("PAT_ENC")
        outlier = opacity_for("ZC_PATIENT_STATUS")
        self.assertGreater(core, outlier)


class TestStripeSVG(unittest.TestCase):

    def test_stripe_lit_for_view_tables_only(self):
        """For VW_C, the lit color #2c7fb8 appears on its 3 tables
        (PAT_ENC, PATIENT, ZC_PATIENT_STATUS) and the faded grey
        appears on CLARITY_DEP (not in VW_C)."""
        views = _views_three_with_core_and_outlier()
        nodes, edges, nf, ef, per_view = build_community_substrate(views)
        coords = frequency_layout(nodes, edges)
        v_nodes, v_edges = per_view["VW_C"]
        svg = render_view_stripe_svg(
            nodes, edges, coords, v_nodes, v_edges, title="VW_C",
        )
        self.assertIn("#2c7fb8", svg)   # lit base color
        self.assertIn("#dcdcdc", svg)   # faded grey

    def test_stripe_with_href_wraps_in_anchor(self):
        views = _views_three_with_core_and_outlier()
        nodes, edges, nf, ef, per_view = build_community_substrate(views)
        coords = frequency_layout(nodes, edges)
        v_nodes, v_edges = per_view["VW_A"]
        svg = render_view_stripe_svg(
            nodes, edges, coords, v_nodes, v_edges,
            title="VW_A",
            href="../community_shapes/community_00_pat_enc_shapes.html#view-VW_A",
        )
        self.assertIn('<a href="', svg)
        self.assertIn(
            'community_shapes/community_00_pat_enc_shapes.html#view-VW_A',
            svg,
        )


# ---------------------------------------------------------------------------
# HTML wrapper
# ---------------------------------------------------------------------------

class TestWriteCommunityOverview(unittest.TestCase):

    def test_writes_html_with_substrate_and_stripes(self):
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "community_05_PAT_ENC_overview.html"
            written = write_community_overview(
                _views_three_with_core_and_outlier(), out,
                community_label="Community 5 -- PAT_ENC",
                shape_file_relpath_by_view={
                    "VW_A": "../community_shapes/c05.html#view-VW_A",
                    "VW_B": "../community_shapes/c05.html#view-VW_B",
                    "VW_C": "../community_shapes/c05.html#view-VW_C",
                },
            )
            self.assertTrue(written.exists())
            content = written.read_text(encoding="utf-8")
            self.assertIn("Community 5 -- PAT_ENC", content)
            # The community substrate panel + a stripe for each view.
            # 4 <svg> total: 1 substrate + 3 stripes.
            self.assertEqual(content.count("<svg "), 4)
            # Footer meta lists core size / outlier size.
            self.assertIn("core", content)
            self.assertIn("outlier", content)
            # Stripe links land on the per-view shape HTML anchors.
            self.assertIn("#view-VW_A", content)


if __name__ == "__main__":
    unittest.main()
