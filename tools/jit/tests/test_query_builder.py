"""TDD tests for the query builder pipeline.

Tests are written FIRST, implementation follows. Each test class covers
one layer of the pipeline: FK graph → path finder → SQL generator →
conversation engine.

Run from the repo root:
    python -m pytest tools/jit/tests/test_query_builder.py -v
"""

from __future__ import annotations

import os
import unittest


SCHEMA_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "..",
    "data", "schemas", "clarity_schema.yaml"
)


# ---------------------------------------------------------------------------
# Layer 1: FK Graph from clarity_schema.yaml
# ---------------------------------------------------------------------------

class TestFkGraph(unittest.TestCase):
    """The FK graph represents legal joins between Clarity tables."""

    def _build(self):
        from tools.jit.query_graph import build_fk_graph
        return build_fk_graph(SCHEMA_PATH)

    def test_patient_node_exists(self):
        g = self._build()
        self.assertIn("PATIENT", g)

    def test_referral_has_edge_to_patient(self):
        """REFERRAL.PAT_ID -> PATIENT.PAT_ID is an FK relationship."""
        g = self._build()
        self.assertTrue(g.has_edge("REFERRAL", "PATIENT"))

    def test_edge_has_join_metadata(self):
        """Each FK edge should carry the column names for the ON clause."""
        g = self._build()
        edge = g.edges["REFERRAL", "PATIENT"]
        self.assertIn("fk_column", edge)
        self.assertIn("pk_column", edge)

    def test_pat_enc_dx_connects_to_pat_enc_and_edg(self):
        g = self._build()
        self.assertTrue(g.has_edge("PAT_ENC_DX", "PAT_ENC"))
        self.assertTrue(g.has_edge("PAT_ENC_DX", "CLARITY_EDG"))

    def test_zc_tables_are_leaves(self):
        """ZC tables should have in-edges but no out-edges to non-ZC tables."""
        g = self._build()
        for node in g.nodes:
            if node.startswith("ZC_"):
                non_zc_targets = [t for t in g.successors(node)
                                  if not t.startswith("ZC_")]
                self.assertEqual(
                    non_zc_targets, [],
                    f"ZC table {node} has outgoing edges to {non_zc_targets}"
                )

    def test_multiple_paths_patient_to_edg(self):
        """There should be multiple paths from PATIENT to CLARITY_EDG
        (via PAT_ENC_DX, PROBLEM_LIST, etc.)."""
        import networkx as nx
        g = self._build()
        # Use undirected view for path finding (FKs are navigable both ways)
        ug = g.to_undirected()
        paths = list(nx.all_simple_paths(ug, "PATIENT", "CLARITY_EDG", cutoff=5))
        self.assertGreater(len(paths), 1,
                           "Should have multiple paths from PATIENT to CLARITY_EDG")

    def test_node_has_description(self):
        g = self._build()
        self.assertIn("description", g.nodes["PATIENT"])


# ---------------------------------------------------------------------------
# Layer 2: Path Finder
# ---------------------------------------------------------------------------

class TestPathFinder(unittest.TestCase):
    """Find legal join paths between two tables."""

    def _find(self, source, target, max_hops=5):
        from tools.jit.query_graph import build_fk_graph, find_join_paths
        g = build_fk_graph(SCHEMA_PATH)
        return find_join_paths(g, source, target, max_hops=max_hops)

    def test_direct_fk_one_hop(self):
        """REFERRAL -> PATIENT is a direct FK, should find a 1-hop path."""
        paths = self._find("REFERRAL", "PATIENT")
        self.assertGreater(len(paths), 0)
        # At least one path should be length 2 (REFERRAL, PATIENT)
        shortest = min(paths, key=len)
        self.assertEqual(len(shortest), 2)

    def test_multi_hop_patient_to_edg(self):
        """PATIENT to CLARITY_EDG requires at least 2 hops."""
        paths = self._find("PATIENT", "CLARITY_EDG")
        self.assertGreater(len(paths), 0)
        for path in paths:
            self.assertGreaterEqual(len(path), 3)

    def test_paths_are_distinct(self):
        """Multiple paths to CLARITY_EDG should go through different tables."""
        paths = self._find("PATIENT", "CLARITY_EDG")
        # Convert to tuples of table names for dedup check
        path_tuples = [tuple(s["table"] for s in p) for p in paths]
        self.assertEqual(len(path_tuples), len(set(path_tuples)),
                         "Paths should be distinct")

    def test_each_path_step_has_join_info(self):
        """Each path should include join metadata for SQL generation."""
        from tools.jit.query_graph import build_fk_graph, find_join_paths
        g = build_fk_graph(SCHEMA_PATH)
        paths = find_join_paths(g, "REFERRAL", "PATIENT")
        self.assertGreater(len(paths), 0)
        # Each path is a list of JoinStep or similar with table + ON info
        for path in paths:
            for step in path:
                self.assertIn("table", step)

    def test_nonexistent_table_returns_empty(self):
        paths = self._find("PATIENT", "NONEXISTENT_TABLE")
        self.assertEqual(paths, [])

    def test_max_hops_limits_depth(self):
        """With max_hops=2, shouldn't find PATIENT->CLARITY_EDG (needs 3+ hops)."""
        paths = self._find("PATIENT", "CLARITY_EDG", max_hops=2)
        for path in paths:
            self.assertLessEqual(len(path), 3)  # source + max 2 hops


# ---------------------------------------------------------------------------
# Layer 3: Step-by-step SQL Generator
# ---------------------------------------------------------------------------

class TestSqlGenerator(unittest.TestCase):
    """Generate one COUNT(*) SQL per join step."""

    def _generate(self, source, target):
        from tools.jit.query_graph import build_fk_graph, find_join_paths
        from tools.jit.query_builder import generate_step_sql
        g = build_fk_graph(SCHEMA_PATH)
        paths = find_join_paths(g, source, target)
        self.assertGreater(len(paths), 0)
        return generate_step_sql(paths[0])

    def test_first_step_is_base_table(self):
        """Step 1 should be SELECT COUNT(*) FROM the source table."""
        steps = self._generate("PATIENT", "PAT_ENC")
        self.assertGreater(len(steps), 0)
        self.assertIn("PATIENT", steps[0]["sql"])
        self.assertIn("COUNT", steps[0]["sql"].upper())

    def test_each_step_adds_one_join(self):
        """Each subsequent step adds exactly one JOIN."""
        steps = self._generate("PATIENT", "CLARITY_EDG")
        for i, step in enumerate(steps):
            if i == 0:
                self.assertNotIn("JOIN", step["sql"].upper())
            else:
                # Count JOINs — should have exactly i JOINs
                join_count = step["sql"].upper().count("JOIN")
                self.assertEqual(join_count, i,
                                 f"Step {i} should have {i} JOINs, got {join_count}")

    def test_join_has_on_clause(self):
        """Each JOIN should include an ON clause."""
        steps = self._generate("REFERRAL", "PATIENT")
        for step in steps[1:]:
            self.assertIn(" ON ", step["sql"].upper())

    def test_steps_have_description(self):
        """Each step should have a human-readable description."""
        steps = self._generate("PATIENT", "PAT_ENC")
        for step in steps:
            self.assertIn("description", step)
            self.assertIsInstance(step["description"], str)
            self.assertGreater(len(step["description"]), 0)

    def test_filter_applied_at_right_step(self):
        """When a filter is provided, it should appear in the SQL."""
        from tools.jit.query_graph import build_fk_graph, find_join_paths
        from tools.jit.query_builder import generate_step_sql
        g = build_fk_graph(SCHEMA_PATH)
        paths = find_join_paths(g, "PATIENT", "CLARITY_EDG")
        steps = generate_step_sql(
            paths[0],
            filters={"CLARITY_EDG": "CURRENT_ICD10_LIST LIKE 'E10%'"}
        )
        # The filter should appear in the last step (where CLARITY_EDG is joined)
        last_sql = steps[-1]["sql"].upper()
        self.assertIn("WHERE", last_sql)
        self.assertIn("E10", last_sql)


if __name__ == "__main__":
    unittest.main()
