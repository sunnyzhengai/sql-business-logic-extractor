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


def _make_view_union_in_cte() -> dict:
    """CTE body is a UNION of two single-table SELECTs. After resolve.py
    emits branch sub-scopes (`cte:foo/union:0`, `cte:foo/union:1`),
    the CTE itself becomes a wrapper scope -- v4 skips its (merged-by-
    fc904a6) flat data and renders the branches independently.
    """
    return {
        "view_name": "VW_UCTE",
        "view_outputs": ["main"],
        "scopes": [
            {
                "id": "main", "kind": "main",
                "reads_from_tables": ["PATIENT"],
                "reads_from_scopes": ["cte:combined"],
                "joins": [{
                    "right_table": "PATIENT", "right_alias": "P",
                    "join_type": "INNER JOIN",
                    "on_expression": "C.PAT_ID = P.PAT_ID",
                    "columns": [],
                }],
                "columns": [],
            },
            {
                # The wrapper. fc904a6 left it with both branches'
                # tables/joins merged in -- v4 must skip rendering this
                # in favor of the branch sub-scopes below.
                "id": "cte:combined", "kind": "cte",
                "reads_from_tables": ["PAT_ENC", "PAT_ENC_HSP"],
                "reads_from_scopes": [],
                "joins": [],
                "columns": [],
            },
            {
                "id": "cte:combined/union:0", "kind": "union",
                "reads_from_tables": ["PAT_ENC"],
                "reads_from_scopes": [], "joins": [], "columns": [],
            },
            {
                "id": "cte:combined/union:1", "kind": "union",
                "reads_from_tables": ["PAT_ENC_HSP"],
                "reads_from_scopes": [], "joins": [], "columns": [],
            },
        ],
    }


def _make_view_cross_apply() -> dict:
    """CROSS APPLY (LATERAL) -- the inner subquery is recorded as a
    lateral:<alias> scope, parallel to JOIN-clause subqueries."""
    return {
        "view_name": "VW_XAPPLY",
        "view_outputs": ["main"],
        "scopes": [
            {
                "id": "main", "kind": "main",
                "reads_from_tables": ["PATIENT"],
                "reads_from_scopes": ["lateral:sub"],
                "joins": [{
                    "right_table": "sub", "right_alias": "sub",
                    "join_type": "JOIN",
                    "on_expression": "",
                    "columns": [],
                }],
                "columns": [],
            },
            {
                "id": "lateral:sub", "kind": "lateral",
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

    def test_union_in_cte_renders_branches_skipping_wrapper(self):
        """Regression: UNION inside a CTE. The CTE acts as a wrapper
        (its flat reads_from_tables / joins are duplicated branch
        data merged by fc904a6); the branches are the real visible
        scopes. v4 must skip the wrapper and fan the consumer's
        cross-scope edge out to each branch's driver, so BOTH
        branches' tables connect back to main."""
        shape = build_view_shape(_make_view_union_in_cte())
        scope_ids = {s.id for s in shape.scopes}
        # Wrapper 'cte:combined' is omitted; both branches survive.
        self.assertNotIn("cte:combined", scope_ids)
        self.assertIn("cte:combined/union:0", scope_ids)
        self.assertIn("cte:combined/union:1", scope_ids)
        # Each branch has exactly its own table.
        b0 = shape.scope_by_id("cte:combined/union:0")
        b1 = shape.scope_by_id("cte:combined/union:1")
        self.assertEqual([n.table for n in b0.nodes], ["PAT_ENC"])
        self.assertEqual([n.table for n in b1.nodes], ["PAT_ENC_HSP"])
        # Cross-scope edge from main fans out to BOTH branch drivers
        # (the wrapper-fanout logic). Without this, the second
        # branch's table would float as an orphan -- the exact bug
        # Yang reported.
        resolved = _resolve_cross_scope_edges(shape)
        targets = {e.target_id for e in resolved if e.kind == "cross_scope"}
        self.assertIn(b0.driver_node_id, targets)
        self.assertIn(b1.driver_node_id, targets)

    def test_wrapper_scope_label_has_path_breadcrumb(self):
        """Branch sub-scope labels include the parent breadcrumb so
        the modeler can read the nesting at a glance."""
        shape = build_view_shape(_make_view_union_in_cte())
        b0 = shape.scope_by_id("cte:combined/union:0")
        # Path-style label: 'CTE: combined · UNION branch 0'.
        self.assertIn("CTE: combined", b0.label)
        self.assertIn("UNION branch 0", b0.label)

    def test_cte_join_produces_placeholder_in_main(self):
        """Q1: when main JOINs to a CTE, main should contain a
        placeholder ShapeNode (kind='scope_ref') representing the
        CTE consumption. The placeholder targets the inner scope;
        a cross-scope edge connects them."""
        shape = build_view_shape(_make_view_cte())
        main = shape.scope_by_id("main")
        # main should have two nodes: PATIENT (real FROM-driver-ish
        # from the corpus shape) and a placeholder for the CTE.
        kinds_in_main = [n.kind for n in main.nodes]
        self.assertIn("scope_ref", kinds_in_main,
                       "main should have at least one scope_ref placeholder")
        placeholder = next(n for n in main.nodes if n.kind == "scope_ref")
        self.assertEqual(placeholder.target_scope_id, "cte:EncDept")
        # Cross-scope edge originates from the placeholder, not from
        # the prior table-node directly.
        cse = [e for e in shape.cross_scope_edges
                if e.kind == "cross_scope"]
        self.assertTrue(
            any(e.source_id == placeholder.id for e in cse),
            "cross-scope edge should originate from the placeholder"
        )

    def test_view_of_view_emits_placeholder_with_target_view_name(self):
        """Q2: when a view's FROM references another view in the
        corpus, that reference is recognized as a foreign view and
        rendered as a scope_ref placeholder with target_view_name
        set so the renderer can hyperlink it."""
        view = {
            "view_name": "VW_USES_FOUNDATION",
            "view_outputs": ["main"],
            "scopes": [{
                "id": "main", "kind": "main",
                "reads_from_tables": ["V_FOO"],
                "reads_from_scopes": [],
                "joins": [],
                "columns": [],
            }],
        }
        corpus_views = {"V_FOO", "VW_USES_FOUNDATION", "OTHER_VIEW"}
        shape = build_view_shape(view, corpus_view_names=corpus_views)
        main = shape.scope_by_id("main")
        self.assertEqual(len(main.nodes), 1)
        n = main.nodes[0]
        self.assertEqual(n.kind, "scope_ref")
        self.assertEqual(n.target_view_name, "V_FOO")

    def test_view_of_view_skipped_when_target_not_in_corpus(self):
        """If `FROM SomeTable` and SomeTable is NOT in the corpus
        view set, treat it as a plain base table (no placeholder)."""
        view = {
            "view_name": "VW_LOCAL_ONLY",
            "view_outputs": ["main"],
            "scopes": [{
                "id": "main", "kind": "main",
                "reads_from_tables": ["PAT_ENC"],
                "reads_from_scopes": [],
                "joins": [],
                "columns": [],
            }],
        }
        corpus_views = {"VW_LOCAL_ONLY"}  # no PAT_ENC
        shape = build_view_shape(view, corpus_view_names=corpus_views)
        main = shape.scope_by_id("main")
        self.assertEqual(main.nodes[0].kind, "table")

    def test_cross_apply_lateral_renders_as_separate_scope(self):
        """CROSS APPLY -- the lateral:sub scope is rendered alongside
        main; cross-scope edge connects main's driver to the
        lateral's driver."""
        shape = build_view_shape(_make_view_cross_apply())
        scope_ids = {s.id for s in shape.scopes}
        self.assertEqual(scope_ids, {"main", "lateral:sub"})
        lat = shape.scope_by_id("lateral:sub")
        self.assertEqual([n.table for n in lat.nodes],
                          ["PAT_ENC", "CLARITY_DEP"])
        # Cluster label uses the T-SQL-familiar CROSS APPLY naming.
        self.assertEqual(lat.label, "CROSS APPLY: sub")

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

    def test_view_of_view_placeholder_gets_hyperlink(self):
        """When a scope_ref placeholder's target_view_name is in the
        view_links map, the renderer wraps the placeholder's <g> in
        an <a xlink:href>."""
        view = {
            "view_name": "VW_HOST",
            "view_outputs": ["main"],
            "scopes": [{
                "id": "main", "kind": "main",
                "reads_from_tables": ["V_FOO"],
                "reads_from_scopes": [],
                "joins": [],
                "columns": [],
            }],
        }
        corpus_views = {"V_FOO", "VW_HOST"}
        shape = build_view_shape(view, corpus_view_names=corpus_views)
        view_links = {"V_FOO": "../community_03_v_foo_shapes.html#view-V_FOO"}
        svg = render_view_shape_panel(shape, view_links=view_links)
        # The placeholder's target view is in the link map -> the
        # node's <g> should be wrapped in <a xlink:href>.
        self.assertIn(
            'xlink:href="../community_03_v_foo_shapes.html#view-V_FOO"',
            svg,
        )

    def test_placeholder_renders_as_rounded_rectangle(self):
        """Scope_ref placeholders get a rounded <rect> instead of a
        <circle>; the modeler distinguishes 'real table' from
        'consumed scope' visually."""
        shape = build_view_shape(_make_view_cte())
        svg = render_view_shape_panel(shape)
        # At least one <rect rx="6" ry="6"> appears (the placeholder).
        self.assertIn('<rect ', svg)
        self.assertIn('rx="6"', svg)

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
