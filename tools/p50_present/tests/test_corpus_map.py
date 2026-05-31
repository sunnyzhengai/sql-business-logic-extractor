"""Tests for tools.p50_present.corpus_map (Stage 1 landscape).

Exercises the four pieces of the corpus map:
  - substrate union (tables + edges across the corpus)
  - force-directed layout determinism (fixed seed -> identical coords)
  - table -> community lookup
  - HTML rendering with the community link footer
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tools.p50_present.corpus_map import (
    build_corpus_substrate,
    force_directed_layout,
    render_corpus_overview_svg,
    write_corpus_map,
    _table_to_community,
)


# ---------------------------------------------------------------------------
# Mock corpus: three views spanning two communities
# ---------------------------------------------------------------------------

def _corpus_views() -> list[dict]:
    """Three simple views over a mix of tables: PAT_ENC/PATIENT/
    CLARITY_DEP/ZC_PATIENT_STATUS for community 0, MED_DISP/MED_ADMIN/
    PHARMACY for community 1."""
    return [
        {
            "view_name": "VW_CLINICAL_A",
            "scopes": [{
                "id": "main", "kind": "main",
                "reads_from_tables": ["PAT_ENC", "PATIENT"],
                "reads_from_scopes": [], "columns": [],
                "joins": [{"right_table": "PATIENT", "right_alias": "P",
                            "join_type": "INNER JOIN", "on_expression": "",
                            "columns": []}],
            }],
        },
        {
            "view_name": "VW_CLINICAL_B",
            "scopes": [{
                "id": "main", "kind": "main",
                "reads_from_tables": ["PAT_ENC", "CLARITY_DEP", "ZC_PATIENT_STATUS"],
                "reads_from_scopes": [], "columns": [],
                "joins": [
                    {"right_table": "CLARITY_DEP", "right_alias": "D",
                     "join_type": "INNER JOIN", "on_expression": "", "columns": []},
                    {"right_table": "ZC_PATIENT_STATUS", "right_alias": "Z",
                     "join_type": "LEFT JOIN", "on_expression": "", "columns": []},
                ],
            }],
        },
        {
            "view_name": "VW_PHARMACY",
            "scopes": [{
                "id": "main", "kind": "main",
                "reads_from_tables": ["MED_DISP", "MED_ADMIN", "PHARMACY"],
                "reads_from_scopes": [], "columns": [],
                "joins": [
                    {"right_table": "MED_ADMIN", "right_alias": "MA",
                     "join_type": "INNER JOIN", "on_expression": "", "columns": []},
                    {"right_table": "PHARMACY", "right_alias": "RX",
                     "join_type": "INNER JOIN", "on_expression": "", "columns": []},
                ],
            }],
        },
    ]


def _corpus_communities() -> list[set[str]]:
    """Two communities, table nodes prefixed `table::` to match the
    graph-node-id convention. PAT_ENC / PATIENT / CLARITY_DEP /
    ZC_PATIENT_STATUS in community 0; MED_DISP / MED_ADMIN / PHARMACY
    in community 1."""
    return [
        {
            "table::PAT_ENC", "table::PATIENT",
            "table::CLARITY_DEP", "table::ZC_PATIENT_STATUS",
        },
        {
            "table::MED_DISP", "table::MED_ADMIN", "table::PHARMACY",
        },
    ]


# ---------------------------------------------------------------------------
# build_corpus_substrate
# ---------------------------------------------------------------------------

class TestSubstrate(unittest.TestCase):

    def test_unions_tables_across_views(self):
        nodes, _ = build_corpus_substrate(_corpus_views())
        # All tables from all views appear in the corpus-level nodes set.
        self.assertEqual(
            nodes,
            {"PAT_ENC", "PATIENT", "CLARITY_DEP", "ZC_PATIENT_STATUS",
             "MED_DISP", "MED_ADMIN", "PHARMACY"},
        )

    def test_dedups_tables_across_views(self):
        """PAT_ENC appears in views A and B; the substrate has it
        ONCE."""
        nodes, _ = build_corpus_substrate(_corpus_views())
        self.assertEqual(
            sum(1 for n in nodes if n == "PAT_ENC"), 1,
        )

    def test_edges_connect_first_table_to_each_join(self):
        _, edges = build_corpus_substrate(_corpus_views())
        # View A: PAT_ENC -> PATIENT
        self.assertIn(("PATIENT", "PAT_ENC"), edges)
        # View B: PAT_ENC -> CLARITY_DEP and PAT_ENC -> ZC_PATIENT_STATUS
        self.assertIn(("CLARITY_DEP", "PAT_ENC"), edges)
        self.assertIn(("PAT_ENC", "ZC_PATIENT_STATUS"), edges)
        # View C: MED_DISP -> MED_ADMIN and MED_DISP -> PHARMACY
        self.assertIn(("MED_ADMIN", "MED_DISP"), edges)
        self.assertIn(("MED_DISP", "PHARMACY"), edges)


# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------

class TestLayout(unittest.TestCase):

    def test_layout_is_deterministic(self):
        """Same input -> identical coords. Critical for stable
        diffs across re-runs."""
        nodes, edges = build_corpus_substrate(_corpus_views())
        a = force_directed_layout(nodes, edges, seed=42)
        b = force_directed_layout(nodes, edges, seed=42)
        self.assertEqual(a, b)

    def test_layout_returns_a_coord_for_every_node(self):
        nodes, edges = build_corpus_substrate(_corpus_views())
        coords = force_directed_layout(nodes, edges)
        self.assertEqual(set(coords.keys()), nodes)

    def test_layout_coords_fit_inside_canvas(self):
        nodes, edges = build_corpus_substrate(_corpus_views())
        coords = force_directed_layout(nodes, edges, width=800, height=600)
        for n, (x, y) in coords.items():
            self.assertGreaterEqual(x, 0, f"{n} x out of range")
            self.assertLessEqual(x, 800, f"{n} x out of range")
            self.assertGreaterEqual(y, 0, f"{n} y out of range")
            self.assertLessEqual(y, 600, f"{n} y out of range")

    def test_empty_corpus_returns_empty_layout(self):
        coords = force_directed_layout(set(), set())
        self.assertEqual(coords, {})


# ---------------------------------------------------------------------------
# Community lookup
# ---------------------------------------------------------------------------

class TestCommunityLookup(unittest.TestCase):

    def test_table_to_community_matches_with_or_without_prefix(self):
        communities = _corpus_communities()
        self.assertEqual(_table_to_community(communities, "PAT_ENC"), 0)
        self.assertEqual(_table_to_community(communities, "MED_ADMIN"), 1)
        self.assertIsNone(_table_to_community(communities, "UNKNOWN"))

    def test_table_to_community_handles_empty_input(self):
        self.assertIsNone(_table_to_community([], "PAT_ENC"))
        self.assertIsNone(_table_to_community(_corpus_communities(), ""))


# ---------------------------------------------------------------------------
# HTML wrapper
# ---------------------------------------------------------------------------

class TestWriteCorpusMap(unittest.TestCase):

    def test_writes_html_with_svg_and_community_list(self):
        views = _corpus_views()
        communities = _corpus_communities()
        community_files = {
            0: ("community_00_pat_enc_shapes.html", "PAT_ENC"),
            1: ("community_01_med_disp_shapes.html", "MED_DISP"),
        }
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "corpus_map.html"
            written = write_corpus_map(
                views, communities, out,
                community_files=community_files,
            )
            self.assertTrue(written.exists())
            content = written.read_text(encoding="utf-8")
            # The SVG element is present.
            self.assertIn("<svg ", content)
            # Each table appears in the SVG (as a <title> tooltip).
            for table in ("PAT_ENC", "PATIENT", "MED_DISP", "PHARMACY"):
                self.assertIn(table, content)
            # Community links rendered with the correct href.
            self.assertIn("community_00_pat_enc_shapes.html", content)
            self.assertIn("community_01_med_disp_shapes.html", content)

    def test_communities_without_files_render_as_no_link(self):
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "corpus_map.html"
            written = write_corpus_map(
                _corpus_views(), _corpus_communities(), out,
                community_files=None,  # no link map -> all unlinked
            )
            content = written.read_text(encoding="utf-8")
            self.assertIn("no shapes file", content)


if __name__ == "__main__":
    unittest.main()
