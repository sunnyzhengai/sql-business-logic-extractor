"""Tests for tools.p50_present.view_shape.

Covers the four mock views from the design conversation:

  A - flat: PAT_ENC + PATIENT
  B - CTE wraps a filter: same shape as A (CTE is invisible)
  C - CTE pre-joins CLARITY_DEP: adds one edge over A/B
  D - flat: extends C with ZC_PATIENT_STATUS

Key correctness invariants:
  - A and B produce identical extended trees (CTE wrapping irrelevant)
  - C extends A/B with one additional edge (PAT_ENC -- CLARITY_DEP)
  - D extends C with one additional edge (PATIENT -- ZC_PATIENT_STATUS)
  - Substrate union covers all 4 distinct tables and 3 distinct edges
  - Hierarchical layout is deterministic across runs
  - SVG output marks the right nodes / edges as lit vs faded
"""

from __future__ import annotations

import unittest

from tools.p50_present.view_shape import (
    community_substrate,
    hierarchical_layout,
    render_view_shape_panel,
    view_extended_tree,
    write_community_shapes,
    _pick_root,
)


# ---------------------------------------------------------------------------
# Mock corpus dicts -- ViewV1 shape minus the fields shape doesn't use.
# ---------------------------------------------------------------------------

def _make_view_a() -> dict:
    """Flat: SELECT ... FROM PAT_ENC PE JOIN PATIENT P ON ..."""
    return {
        "view_name": "VW_A",
        "view_outputs": ["main"],
        "scopes": [
            {
                "id": "main", "kind": "main",
                "reads_from_tables": ["PAT_ENC", "PATIENT"],
                "reads_from_scopes": [],
                "joins": [
                    {"right_table": "PATIENT", "join_type": "INNER JOIN",
                     "on_expression": "PE.PAT_ID = P.PAT_ID"},
                ],
                "columns": [],
            },
        ],
    }


def _make_view_b() -> dict:
    """CTE wraps a filter; shape is equivalent to A.

    WITH ActiveEnc AS (SELECT ... FROM PAT_ENC WHERE STATUS_C = 2)
    SELECT ... FROM ActiveEnc AE JOIN PATIENT P ON AE.PAT_ID = P.PAT_ID
    """
    return {
        "view_name": "VW_B",
        "view_outputs": ["main"],
        "scopes": [
            {
                "id": "main", "kind": "main",
                "reads_from_tables": ["PATIENT"],
                "reads_from_scopes": ["cte:ActiveEnc"],
                "joins": [
                    # Join from main to the CTE (right_table is the bare
                    # CTE name, no `cte:` prefix -- corpus convention).
                    {"right_table": "PATIENT", "join_type": "INNER JOIN",
                     "on_expression": "AE.PAT_ID = P.PAT_ID"},
                ],
                "columns": [],
            },
            {
                "id": "cte:ActiveEnc", "kind": "cte",
                "reads_from_tables": ["PAT_ENC"],
                "reads_from_scopes": [],
                "joins": [],
                "columns": [],
            },
        ],
    }


def _make_view_c() -> dict:
    """CTE pre-joins PAT_ENC + CLARITY_DEP; main joins PATIENT.

    WITH EncDept AS (SELECT ... FROM PAT_ENC PE JOIN CLARITY_DEP D ON ...)
    SELECT ... FROM EncDept ED JOIN PATIENT P ON ED.PAT_ID = P.PAT_ID
    """
    return {
        "view_name": "VW_C",
        "view_outputs": ["main"],
        "scopes": [
            {
                "id": "main", "kind": "main",
                "reads_from_tables": ["PATIENT"],
                "reads_from_scopes": ["cte:EncDept"],
                "joins": [
                    {"right_table": "PATIENT", "join_type": "INNER JOIN",
                     "on_expression": "ED.PAT_ID = P.PAT_ID"},
                ],
                "columns": [],
            },
            {
                "id": "cte:EncDept", "kind": "cte",
                "reads_from_tables": ["PAT_ENC", "CLARITY_DEP"],
                "reads_from_scopes": [],
                "joins": [
                    {"right_table": "CLARITY_DEP", "join_type": "INNER JOIN",
                     "on_expression": "PE.DEPARTMENT_ID = D.DEPARTMENT_ID"},
                ],
                "columns": [],
            },
        ],
    }


def _make_view_d() -> dict:
    """Flat extension of C: adds ZC_PATIENT_STATUS as a lookup.

    SELECT ... FROM PAT_ENC PE
    JOIN CLARITY_DEP D ON ...
    JOIN PATIENT P ON ...
    LEFT JOIN ZC_PATIENT_STATUS ZC ON P.STATUS_C = ZC.PAT_STATUS_C
    """
    return {
        "view_name": "VW_D",
        "view_outputs": ["main"],
        "scopes": [
            {
                "id": "main", "kind": "main",
                "reads_from_tables": ["PAT_ENC", "CLARITY_DEP", "PATIENT",
                                       "ZC_PATIENT_STATUS"],
                "reads_from_scopes": [],
                "joins": [
                    {"right_table": "CLARITY_DEP", "join_type": "INNER JOIN",
                     "on_expression": "PE.DEPARTMENT_ID = D.DEPARTMENT_ID",
                     "columns": [
                         {"column": "DEPARTMENT_ID", "table": "PAT_ENC",
                          "table_alias": "PE"},
                         {"column": "DEPARTMENT_ID", "table": "CLARITY_DEP",
                          "table_alias": "D"},
                     ]},
                    {"right_table": "PATIENT", "join_type": "INNER JOIN",
                     "on_expression": "PE.PAT_ID = P.PAT_ID",
                     "columns": [
                         {"column": "PAT_ID", "table": "PAT_ENC",
                          "table_alias": "PE"},
                         {"column": "PAT_ID", "table": "PATIENT",
                          "table_alias": "P"},
                     ]},
                    # Third join chains off PATIENT, not the scope's
                    # FROM-clause driver -- this is the case that
                    # exposes the "always use scope driver" bug.
                    {"right_table": "ZC_PATIENT_STATUS",
                     "join_type": "LEFT JOIN",
                     "on_expression": "P.STATUS_C = ZC.PAT_STATUS_C",
                     "columns": [
                         {"column": "STATUS_C", "table": "PATIENT",
                          "table_alias": "P"},
                         {"column": "PAT_STATUS_C",
                          "table": "ZC_PATIENT_STATUS",
                          "table_alias": "ZC"},
                     ]},
                ],
                "columns": [],
            },
        ],
    }


# ---------------------------------------------------------------------------
# Per-view extended-tree tests
# ---------------------------------------------------------------------------

class TestViewExtendedTree(unittest.TestCase):

    def test_view_a_simple_two_table_join(self):
        nodes, edges = view_extended_tree(_make_view_a())
        self.assertEqual(nodes, {"PAT_ENC", "PATIENT"})
        # Edges canonicalize via Python sort: '_' (ASCII 95) comes after
        # letters, so 'PATIENT' < 'PAT_ENC' and the tuple is in that order.
        self.assertEqual(edges, {("PATIENT", "PAT_ENC")})

    def test_view_b_cte_wrapping_is_invisible(self):
        """B has a CTE that just filters PAT_ENC. The shape should
        match A exactly -- this is the WHOLE POINT of the extended-
        tree concept (semantically equivalent SQL -> identical shape).
        """
        nodes_a, edges_a = view_extended_tree(_make_view_a())
        nodes_b, edges_b = view_extended_tree(_make_view_b())
        self.assertEqual(nodes_a, nodes_b)
        self.assertEqual(edges_a, edges_b)

    def test_view_c_cte_pre_join_extends_into_main_tree(self):
        """C's CTE adds CLARITY_DEP and its join inside the CTE. The
        extended tree should reflect both edges, and the CTE-result's
        consumption by main should connect to PATIENT via PAT_ENC."""
        nodes, edges = view_extended_tree(_make_view_c())
        self.assertEqual(
            nodes, {"PAT_ENC", "PATIENT", "CLARITY_DEP"},
        )
        self.assertEqual(
            edges,
            {
                ("PATIENT", "PAT_ENC"),        # main -> CTE_driver(PAT_ENC)
                ("CLARITY_DEP", "PAT_ENC"),    # CTE's internal join
            },
        )

    def test_view_d_extends_c_with_one_lookup(self):
        nodes, edges = view_extended_tree(_make_view_d())
        self.assertEqual(
            nodes,
            {"PAT_ENC", "PATIENT", "CLARITY_DEP", "ZC_PATIENT_STATUS"},
        )
        self.assertEqual(
            edges,
            {
                ("CLARITY_DEP", "PAT_ENC"),
                ("PATIENT", "PAT_ENC"),
                ("PATIENT", "ZC_PATIENT_STATUS"),
            },
        )

    def test_c_subset_of_d(self):
        """The variance/coverage story: D contains all of C's edges
        plus one new lookup edge."""
        _, edges_c = view_extended_tree(_make_view_c())
        _, edges_d = view_extended_tree(_make_view_d())
        self.assertTrue(edges_c.issubset(edges_d))
        self.assertEqual(edges_d - edges_c,
                          {("PATIENT", "ZC_PATIENT_STATUS")})

    def test_self_join_emits_no_edge(self):
        """Self-joins on the same base table produce a self-loop,
        which the renderer skips (no visible variance signal)."""
        view = {
            "view_name": "VW_SELF",
            "view_outputs": ["main"],
            "scopes": [{
                "id": "main", "kind": "main",
                "reads_from_tables": ["PAT_ENC"],
                "reads_from_scopes": [],
                "joins": [{"right_table": "PAT_ENC", "join_type": "INNER JOIN",
                            "on_expression": "A.PAT_ID = B.PARENT_PAT_ID",
                            "right_alias": "B"}],
                "columns": [],
            }],
        }
        nodes, edges = view_extended_tree(view)
        self.assertEqual(nodes, {"PAT_ENC"})
        self.assertEqual(edges, set())

    def test_exists_subquery_scope_skipped(self):
        """EXISTS/IN subqueries reference tables but are filter
        dependencies, not join data flow -- excluded from the shape."""
        view = {
            "view_name": "VW_EXISTS",
            "view_outputs": ["main"],
            "scopes": [
                {
                    "id": "main", "kind": "main",
                    "reads_from_tables": ["PAT_ENC"],
                    "reads_from_scopes": ["exists:0"],
                    "joins": [],
                    "columns": [],
                },
                {
                    "id": "exists:0", "kind": "exists",
                    "reads_from_tables": ["PHARMACY"],
                    "reads_from_scopes": [],
                    "joins": [],
                    "columns": [],
                },
            ],
        }
        nodes, _ = view_extended_tree(view)
        # PHARMACY (from the EXISTS subquery) must NOT appear; only
        # the main scope's base tables count.
        self.assertEqual(nodes, {"PAT_ENC"})


# ---------------------------------------------------------------------------
# Community substrate tests
# ---------------------------------------------------------------------------

class TestCommunitySubstrate(unittest.TestCase):

    def test_substrate_unions_nodes_and_edges_across_views(self):
        views = [_make_view_a(), _make_view_b(), _make_view_c(), _make_view_d()]
        nodes, edges, per_view = community_substrate(views)
        self.assertEqual(
            nodes,
            {"PAT_ENC", "PATIENT", "CLARITY_DEP", "ZC_PATIENT_STATUS"},
        )
        self.assertEqual(
            edges,
            {
                ("PATIENT", "PAT_ENC"),
                ("CLARITY_DEP", "PAT_ENC"),
                ("PATIENT", "ZC_PATIENT_STATUS"),
            },
        )
        # per_view round-trips: each view's set is correctly indexed.
        self.assertEqual(per_view["VW_A"][1], {("PATIENT", "PAT_ENC")})
        self.assertEqual(per_view["VW_A"], per_view["VW_B"])


# ---------------------------------------------------------------------------
# Layout tests
# ---------------------------------------------------------------------------

class TestHierarchicalLayout(unittest.TestCase):

    def test_root_is_most_connected_table(self):
        """In the four-view substrate, PAT_ENC and PATIENT both have
        degree 2. Alphabetic tie-break uses Python string sort
        (underscores after letters), so PATIENT < PAT_ENC."""
        nodes = {"PAT_ENC", "PATIENT", "CLARITY_DEP", "ZC_PATIENT_STATUS"}
        edges = {
            ("PATIENT", "PAT_ENC"),
            ("CLARITY_DEP", "PAT_ENC"),
            ("PATIENT", "ZC_PATIENT_STATUS"),
        }
        self.assertEqual(_pick_root(nodes, edges), "PATIENT")

    def test_layout_is_deterministic_across_runs(self):
        """Same input -> identical coordinate map. Critical because
        the substrate layout is computed once and reused across panels;
        any nondeterminism makes panels misalign across reruns."""
        nodes = {"PAT_ENC", "PATIENT", "CLARITY_DEP", "ZC_PATIENT_STATUS"}
        edges = {
            ("PATIENT", "PAT_ENC"),
            ("CLARITY_DEP", "PAT_ENC"),
            ("PATIENT", "ZC_PATIENT_STATUS"),
        }
        coords_a = hierarchical_layout(nodes, edges)
        coords_b = hierarchical_layout(nodes, edges)
        self.assertEqual(coords_a, coords_b)

    def test_root_at_column_zero(self):
        nodes = {"PAT_ENC", "PATIENT", "CLARITY_DEP", "ZC_PATIENT_STATUS"}
        edges = {
            ("PATIENT", "PAT_ENC"),
            ("CLARITY_DEP", "PAT_ENC"),
            ("PATIENT", "ZC_PATIENT_STATUS"),
        }
        coords = hierarchical_layout(nodes, edges, root="PAT_ENC")
        self.assertEqual(coords["PAT_ENC"][0], 0)
        # Direct neighbors land at column 1.
        self.assertEqual(coords["PATIENT"][0], 1)
        self.assertEqual(coords["CLARITY_DEP"][0], 1)
        # Two hops away.
        self.assertEqual(coords["ZC_PATIENT_STATUS"][0], 2)


# ---------------------------------------------------------------------------
# SVG / HTML rendering tests
# ---------------------------------------------------------------------------

class TestSVGRendering(unittest.TestCase):

    def test_panel_marks_lit_and_faded_correctly(self):
        """For view A inside the four-view substrate, PAT_ENC + PATIENT
        should be drawn lit (colored fill), and CLARITY_DEP +
        ZC_PATIENT_STATUS should be drawn faded (faded fill, dashed
        stroke)."""
        views = [_make_view_a(), _make_view_b(), _make_view_c(), _make_view_d()]
        nodes, edges, per_view = community_substrate(views)
        coords = hierarchical_layout(nodes, edges)
        v_nodes, v_edges = per_view["VW_A"]
        svg = render_view_shape_panel(
            view_name="VW_A",
            view_nodes=v_nodes, view_edges=v_edges,
            substrate_nodes=nodes, substrate_edges=edges,
            coords=coords,
        )
        # Sanity: the SVG mentions every substrate node by name.
        for table in nodes:
            self.assertIn(table, svg)
        # Lit fill appears (used by lit nodes); faded fill appears
        # (used by faded nodes).
        self.assertIn("#2c7fb8", svg)   # lit fill
        self.assertIn("#f0f0f0", svg)   # faded fill
        # Dashed stroke marker is present (faded nodes use it).
        self.assertIn("stroke-dasharray", svg)

    def test_write_community_shapes_creates_html(self):
        import tempfile
        from pathlib import Path

        views = [_make_view_a(), _make_view_b(), _make_view_c(), _make_view_d()]
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "community_05_PAT_ENC_shapes.html"
            written = write_community_shapes(
                views, out, community_label="Community 5",
            )
            self.assertTrue(written.exists())
            content = written.read_text(encoding="utf-8")
            # Header + meta + substrate section + grid of N panels.
            self.assertIn("Community 5", content)
            for view_name in ("VW_A", "VW_B", "VW_C", "VW_D"):
                self.assertIn(view_name, content)
            # Substrate panel + 4 view panels + 0 (each panel is one
            # <svg> element). Substrate-section anchor distinguishes
            # the reference panel from the per-view grid.
            self.assertIn("Shared substrate", content)
            self.assertEqual(content.count("<svg "), 5)


if __name__ == "__main__":
    unittest.main()
