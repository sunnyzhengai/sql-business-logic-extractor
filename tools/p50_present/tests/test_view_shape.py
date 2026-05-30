"""Tests for tools.p50_present.view_shape (v4 -- query-unfolding model).

The v3 deduped-substrate model is tagged `view_shape_v3` and the
tests for that model are preserved at that tag. This file tests the
v4 model where:

  - Each SQL occurrence of a table is its own ShapeNode (self-joins
    produce two nodes; same table in CTE + main produces two nodes).
  - CTEs and subqueries appear as separate ShapeScopes (clusters).
  - Edges respect SQL join order; cross-scope edges connect a
    consumer node to the referenced scope's driver_node.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tools.p50_present.view_shape import (
    ShapeEdge,
    ShapeNode,
    ShapeScope,
    ViewShape,
    build_view_shape,
    layout_shape,
    render_view_shape_panel,
    write_community_shapes,
    _resolve_cross_scope_edges,
)


# ---------------------------------------------------------------------------
# Mock corpus dicts (ViewV1 shape, only the fields the shape model uses)
# ---------------------------------------------------------------------------

def _make_view_a() -> dict:
    """Flat two-table join."""
    return {
        "view_name": "VW_A",
        "view_outputs": ["main"],
        "scopes": [
            {
                "id": "main", "kind": "main",
                "reads_from_tables": ["PAT_ENC", "PATIENT"],
                "reads_from_scopes": [],
                "joins": [{
                    "right_table": "PATIENT", "right_alias": "P",
                    "join_type": "INNER JOIN",
                    "on_expression": "PE.PAT_ID = P.PAT_ID",
                    "columns": [],
                }],
                "columns": [],
            },
        ],
    }


def _make_view_self_join() -> dict:
    """Self-join: PAT_ENC A JOIN PAT_ENC B -- v4 must produce TWO
    PAT_ENC nodes (one per alias), unlike v3 which deduped."""
    return {
        "view_name": "VW_SELF",
        "view_outputs": ["main"],
        "scopes": [
            {
                "id": "main", "kind": "main",
                "reads_from_tables": ["PAT_ENC"],
                "reads_from_scopes": [],
                "joins": [{
                    "right_table": "PAT_ENC", "right_alias": "B",
                    "join_type": "INNER JOIN",
                    "on_expression": "A.PAT_ID = B.PARENT_PAT_ID",
                    "columns": [],
                }],
                "columns": [],
            },
        ],
    }


def _make_view_cte() -> dict:
    """CTE wrapping a join, consumed from main."""
    return {
        "view_name": "VW_CTE",
        "view_outputs": ["main"],
        "scopes": [
            {
                "id": "main", "kind": "main",
                "reads_from_tables": ["PATIENT"],
                "reads_from_scopes": ["cte:EncDept"],
                "joins": [{
                    "right_table": "PATIENT", "right_alias": "P",
                    "join_type": "INNER JOIN",
                    "on_expression": "ED.PAT_ID = P.PAT_ID",
                    "columns": [],
                }],
                "columns": [],
            },
            {
                "id": "cte:EncDept", "kind": "cte",
                "reads_from_tables": ["PAT_ENC", "CLARITY_DEP"],
                "reads_from_scopes": [],
                "joins": [{
                    "right_table": "CLARITY_DEP", "right_alias": "D",
                    "join_type": "INNER JOIN",
                    "on_expression": "PE.DEPARTMENT_ID = D.DEPARTMENT_ID",
                    "columns": [],
                }],
                "columns": [],
            },
        ],
    }


def _make_view_join_subquery() -> dict:
    """JOIN-clause subquery -- the bug 1 case. After the extract.py +
    resolve.py fixes: the join's right_table is the subquery's alias
    ('sub'), reads_from_scopes lists 'join:sub' (alias-based id), and
    the subquery scope itself has id 'join:sub' with kind 'join'."""
    return {
        "view_name": "VW_JSUB",
        "view_outputs": ["main"],
        "scopes": [
            {
                "id": "main", "kind": "main",
                "reads_from_tables": ["PATIENT"],
                "reads_from_scopes": ["join:sub"],
                "joins": [{
                    "right_table": "sub", "right_alias": "sub",
                    "join_type": "INNER JOIN",
                    "on_expression": "p.PAT_ID = sub.PAT_ID",
                    "columns": [],
                }],
                "columns": [],
            },
            {
                "id": "join:sub", "kind": "join",
                "reads_from_tables": ["PAT_ENC", "CLARITY_DEP"],
                "reads_from_scopes": [],
                "joins": [{
                    "right_table": "CLARITY_DEP", "right_alias": "d",
                    "join_type": "INNER JOIN",
                    "on_expression": "e.DEPARTMENT_ID = d.DEPARTMENT_ID",
                    "columns": [],
                }],
                "columns": [],
            },
        ],
    }


# ---------------------------------------------------------------------------
# build_view_shape: occurrence-as-node semantics
# ---------------------------------------------------------------------------

class TestBuildViewShape(unittest.TestCase):

    def test_two_table_join_produces_two_nodes_one_edge(self):
        shape = build_view_shape(_make_view_a())
        self.assertEqual(len(shape.scopes), 1)
        main = shape.scopes[0]
        self.assertEqual(main.id, "main")
        # Two occurrence nodes: PAT_ENC (from), PATIENT (join).
        self.assertEqual([n.table for n in main.nodes], ["PAT_ENC", "PATIENT"])
        self.assertEqual([n.role for n in main.nodes], ["from", "join"])
        # One join edge.
        self.assertEqual(len(main.edges), 1)
        e = main.edges[0]
        self.assertEqual(e.source_id, main.nodes[0].id)
        self.assertEqual(e.target_id, main.nodes[1].id)
        self.assertEqual(e.join_type, "INNER JOIN")

    def test_self_join_produces_two_distinct_nodes_same_table(self):
        """Self-joins are the canonical v4-vs-v3 difference."""
        shape = build_view_shape(_make_view_self_join())
        main = shape.scopes[0]
        # Two nodes, both PAT_ENC, but distinct IDs and one carries
        # the SQL alias ('B').
        self.assertEqual(len(main.nodes), 2)
        self.assertEqual({n.table for n in main.nodes}, {"PAT_ENC"})
        self.assertNotEqual(main.nodes[0].id, main.nodes[1].id)
        self.assertEqual(main.nodes[1].alias, "B")
        # And one edge connecting them -- in v3 this got dropped as a
        # degenerate self-loop; in v4 it's a real edge between two
        # distinct occurrence nodes.
        self.assertEqual(len(main.edges), 1)

    def test_cte_appears_as_its_own_scope_with_cross_edge(self):
        shape = build_view_shape(_make_view_cte())
        # main + cte:EncDept = 2 scopes.
        self.assertEqual(len(shape.scopes), 2)
        scope_ids = {s.id for s in shape.scopes}
        self.assertEqual(scope_ids, {"main", "cte:EncDept"})
        # main has PATIENT as FROM-driver and PATIENT-as-JOIN... wait,
        # actually main's reads_from_tables=[PATIENT] (FROM) and the
        # JOIN's right_table is also PATIENT? Let me re-check mock:
        # the mock has FROM combined C JOIN PATIENT P, so the
        # corpus's main reads_from_tables=[PATIENT] (the JOIN target,
        # since `combined` goes to reads_from_scopes). So we get one
        # main node (PATIENT, role='from') and the JOIN to PATIENT
        # creates a second PATIENT node. That's mock-design quirky
        # but the v4 model is correct: each SQL appearance is a node.
        main = shape.scope_by_id("main")
        cte = shape.scope_by_id("cte:EncDept")
        self.assertIsNotNone(main)
        self.assertIsNotNone(cte)
        # CTE has two nodes: PAT_ENC (from), CLARITY_DEP (join).
        self.assertEqual([n.table for n in cte.nodes],
                          ["PAT_ENC", "CLARITY_DEP"])
        # Cross-scope edge: main has a reads_from_scopes ref to the
        # CTE, so we expect at least one cross-scope edge whose
        # target resolves to the CTE's driver.
        resolved = _resolve_cross_scope_edges(shape)
        self.assertTrue(
            any(e.kind == "cross_scope"
                and e.target_id == cte.driver_node_id
                for e in resolved),
            f"no cross-scope edge to CTE driver in {resolved}"
        )

    def test_join_subquery_renders_as_separate_scope(self):
        shape = build_view_shape(_make_view_join_subquery())
        scope_ids = {s.id for s in shape.scopes}
        self.assertEqual(scope_ids, {"main", "join:sub"})
        jsub = shape.scope_by_id("join:sub")
        self.assertEqual([n.table for n in jsub.nodes],
                          ["PAT_ENC", "CLARITY_DEP"])
        # Cross-scope edge exists from main to the join-subquery's
        # driver.
        resolved = _resolve_cross_scope_edges(shape)
        self.assertTrue(
            any(e.kind == "cross_scope"
                and e.target_id == jsub.driver_node_id
                for e in resolved)
        )


# ---------------------------------------------------------------------------
# Layout: deterministic across runs, scope clusters don't overlap
# ---------------------------------------------------------------------------

class TestLayout(unittest.TestCase):

    def test_layout_is_deterministic(self):
        shape = build_view_shape(_make_view_cte())
        coords_a, boxes_a, w_a, h_a = layout_shape(shape)
        coords_b, boxes_b, w_b, h_b = layout_shape(shape)
        self.assertEqual(coords_a, coords_b)
        self.assertEqual(boxes_a, boxes_b)
        self.assertEqual((w_a, h_a), (w_b, h_b))

    def test_scope_clusters_dont_vertically_overlap(self):
        shape = build_view_shape(_make_view_cte())
        _, boxes, _, _ = layout_shape(shape)
        # Sort scope boxes top-to-bottom; each one's bottom must be
        # <= the next one's top (or equal at exactly the seam).
        sorted_boxes = sorted(boxes.values(), key=lambda b: b[1])
        for i in range(len(sorted_boxes) - 1):
            x1, y1, w1, h1 = sorted_boxes[i]
            x2, y2, w2, h2 = sorted_boxes[i + 1]
            self.assertLessEqual(y1 + h1, y2,
                                  "scope clusters overlap vertically")


# ---------------------------------------------------------------------------
# SVG renderer
# ---------------------------------------------------------------------------

class TestSVGRendering(unittest.TestCase):

    def test_panel_includes_each_occurrence_node_label(self):
        shape = build_view_shape(_make_view_self_join())
        svg = render_view_shape_panel(shape)
        # Both PAT_ENC occurrences should appear (one as plain, one
        # with the (B) alias suffix).
        self.assertIn("PAT_ENC", svg)
        self.assertIn("(B)", svg)

    def test_panel_renders_cluster_label_for_cte(self):
        shape = build_view_shape(_make_view_cte())
        svg = render_view_shape_panel(shape)
        # Cluster label appears in plain text.
        self.assertIn("CTE: EncDept", svg)
        self.assertIn("main", svg)

    def test_join_type_label_omitted_for_inner_join(self):
        """INNER / plain JOIN doesn't get a label drawn (saves
        visual noise); other types do."""
        shape = build_view_shape(_make_view_a())
        svg = render_view_shape_panel(shape)
        # 'INNER JOIN' text should NOT appear as an edge label.
        # (It still appears inside the <title> tooltip metadata,
        # but not as a standalone SVG text label.)
        # Strip <title> tags and check.
        import re
        no_titles = re.sub(r"<title>[^<]*</title>", "", svg)
        self.assertNotIn(">INNER JOIN<", no_titles)


# ---------------------------------------------------------------------------
# HTML wrapper
# ---------------------------------------------------------------------------

class TestWriteHTML(unittest.TestCase):

    def test_write_community_shapes_emits_expected_structure(self):
        views = [
            _make_view_a(),
            _make_view_self_join(),
            _make_view_cte(),
            _make_view_join_subquery(),
        ]
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "community_demo_shapes.html"
            written = write_community_shapes(
                views, out, community_label="Demo community",
            )
            self.assertTrue(written.exists())
            content = written.read_text(encoding="utf-8")
            # All four views appear by name.
            for name in ("VW_A", "VW_SELF", "VW_CTE", "VW_JSUB"):
                self.assertIn(name, content)
                self.assertIn(f'data-view="{name}"', content)
            # Compare picker present, overlay button NOT present (v4
            # dropped overlay mode).
            self.assertIn('id="cmp-a"', content)
            self.assertIn('id="cmp-b"', content)
            self.assertIn('id="cmp-pair"', content)
            self.assertIn('id="cmp-all"', content)
            self.assertNotIn('id="cmp-overlay"', content)
            # First two views (alphabetic = VW_A, VW_CTE) get the
            # default selection.
            self.assertIn('value="VW_A" selected', content)
            self.assertIn('value="VW_CTE" selected', content)
            # 4 SVG panels (no separate overlay skeleton in v4).
            self.assertEqual(content.count("<svg "), 4)


if __name__ == "__main__":
    unittest.main()
