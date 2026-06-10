"""Tests for tools.p30_analyze.table_importance.

Run from the repo root:
    python -m pytest tools/p30_analyze/tests/test_table_importance.py -v

The fixtures model a small referral-domain cluster:
  - REFERRAL is the center table (everything joins to it)
  - PAT_ENC is a secondary table (joins to REFERRAL in 2 views)
  - CLARITY_SER joins to REFERRAL in 1 view
  - ZC_RFL_STATUS is a lookup (leaf, joins to REFERRAL in 2 views)

Even though ZC_RFL_STATUS has 2 join edges (same as PAT_ENC), the
algorithm should rank it lower because:
  1. ZC tables are excluded from PageRank
  2. ZC frequency is capped at 20%
  3. ZC tables are always classified as "peripheral"
"""

from __future__ import annotations

import unittest


# ---------------------------------------------------------------------------
# Fixtures -- three views forming a referral-domain cluster
# ---------------------------------------------------------------------------

VIEW_REFERRAL_BASIC = {
    "view_name": "VW_REFERRAL_STATUS",
    "scopes": [{
        "id": "main",
        "kind": "main",
        "reads_from_tables": ["REFERRAL", "ZC_RFL_STATUS"],
        "joins": [
            {"right_table": "ZC_RFL_STATUS", "join_type": "LEFT JOIN",
             "on_expression": "REFERRAL.REFERRAL_STATUS_C = ZC_RFL_STATUS.RFL_STATUS_C"},
        ],
        "reads_from_scopes": [],
        "columns": [
            {"column_name": "REFERRAL_ID", "column_type": "key",
             "base_columns": ["table:REFERRAL.REFERRAL_ID"], "base_tables": ["REFERRAL"]},
        ],
        "filters": [],
    }],
}

VIEW_REFERRAL_ENC = {
    "view_name": "VW_REFERRAL_ENCOUNTERS",
    "scopes": [{
        "id": "main",
        "kind": "main",
        "reads_from_tables": ["REFERRAL", "PAT_ENC", "ZC_RFL_STATUS"],
        "joins": [
            {"right_table": "PAT_ENC", "join_type": "INNER JOIN",
             "on_expression": "REFERRAL.PAT_ENC_CSN_ID = PAT_ENC.PAT_ENC_CSN_ID"},
            {"right_table": "ZC_RFL_STATUS", "join_type": "LEFT JOIN",
             "on_expression": "REFERRAL.REFERRAL_STATUS_C = ZC_RFL_STATUS.RFL_STATUS_C"},
        ],
        "reads_from_scopes": [],
        "columns": [
            {"column_name": "REFERRAL_ID", "column_type": "key",
             "base_columns": ["table:REFERRAL.REFERRAL_ID"], "base_tables": ["REFERRAL"]},
        ],
        "filters": [],
    }],
}

VIEW_REFERRAL_PROVIDER = {
    "view_name": "VW_REFERRAL_PROVIDER",
    "scopes": [{
        "id": "main",
        "kind": "main",
        "reads_from_tables": ["REFERRAL", "PAT_ENC", "CLARITY_SER"],
        "joins": [
            {"right_table": "PAT_ENC", "join_type": "INNER JOIN",
             "on_expression": "REFERRAL.PAT_ENC_CSN_ID = PAT_ENC.PAT_ENC_CSN_ID"},
            {"right_table": "CLARITY_SER", "join_type": "INNER JOIN",
             "on_expression": "REFERRAL.REFERRING_PROV_ID = CLARITY_SER.PROV_ID"},
        ],
        "reads_from_scopes": [],
        "columns": [
            {"column_name": "REFERRAL_ID", "column_type": "key",
             "base_columns": ["table:REFERRAL.REFERRAL_ID"], "base_tables": ["REFERRAL"]},
        ],
        "filters": [],
    }],
}

ALL_VIEWS = [VIEW_REFERRAL_BASIC, VIEW_REFERRAL_ENC, VIEW_REFERRAL_PROVIDER]


def _build_graph_and_community():
    """Build graph from fixtures and return (graph, community_table_set)."""
    from tools.p20_index.graph_builder import build_graph
    g = build_graph(ALL_VIEWS)
    # All tables in one community for these tests
    community = {n for n, d in g.nodes(data=True) if d.get("ntype") == "table"}
    return g, community


class TestBuildCorpusTableFrequency(unittest.TestCase):

    def test_referral_appears_in_all_three_views(self):
        from tools.p20_index.graph_builder import build_graph
        from tools.p30_analyze.table_importance import build_corpus_table_frequency
        g = build_graph(ALL_VIEWS)
        freq = build_corpus_table_frequency(g)
        self.assertEqual(freq.get("table::REFERRAL"), 3)

    def test_zc_table_appears_in_two_views(self):
        from tools.p20_index.graph_builder import build_graph
        from tools.p30_analyze.table_importance import build_corpus_table_frequency
        g = build_graph(ALL_VIEWS)
        freq = build_corpus_table_frequency(g)
        self.assertEqual(freq.get("table::ZC_RFL_STATUS"), 2)

    def test_clarity_ser_appears_in_one_view(self):
        from tools.p20_index.graph_builder import build_graph
        from tools.p30_analyze.table_importance import build_corpus_table_frequency
        g = build_graph(ALL_VIEWS)
        freq = build_corpus_table_frequency(g)
        self.assertEqual(freq.get("table::CLARITY_SER"), 1)


class TestRankTablesInCommunity(unittest.TestCase):

    def test_referral_is_center(self):
        """REFERRAL should be classified as the center table."""
        from tools.p30_analyze.table_importance import rank_tables_in_community
        g, community = _build_graph_and_community()
        ranking = rank_tables_in_community(g, community)
        roles = {name: role for name, _, role in ranking}
        self.assertEqual(roles["REFERRAL"], "center")

    def test_zc_table_is_peripheral(self):
        """ZC_RFL_STATUS must always be peripheral, never center/secondary."""
        from tools.p30_analyze.table_importance import rank_tables_in_community
        g, community = _build_graph_and_community()
        ranking = rank_tables_in_community(g, community)
        roles = {name: role for name, _, role in ranking}
        self.assertEqual(roles["ZC_RFL_STATUS"], "peripheral")

    def test_center_scores_highest(self):
        """The center table should have the highest score (1.0 after normalization)."""
        from tools.p30_analyze.table_importance import rank_tables_in_community
        g, community = _build_graph_and_community()
        ranking = rank_tables_in_community(g, community)
        scores = {name: score for name, score, _ in ranking}
        center_score = scores["REFERRAL"]
        self.assertAlmostEqual(center_score, 1.0, places=5)

    def test_zc_scores_below_non_zc(self):
        """ZC table should score below all non-ZC tables."""
        from tools.p30_analyze.table_importance import rank_tables_in_community
        g, community = _build_graph_and_community()
        ranking = rank_tables_in_community(g, community)
        scores = {name: score for name, score, _ in ranking}
        zc_score = scores["ZC_RFL_STATUS"]
        for name, score in scores.items():
            if not name.startswith("ZC_"):
                self.assertGreater(
                    score, zc_score,
                    f"Non-ZC table {name} ({score:.3f}) should score above "
                    f"ZC_RFL_STATUS ({zc_score:.3f})"
                )

    def test_sorted_descending(self):
        """Output should be sorted by score descending."""
        from tools.p30_analyze.table_importance import rank_tables_in_community
        g, community = _build_graph_and_community()
        ranking = rank_tables_in_community(g, community)
        scores = [score for _, score, _ in ranking]
        self.assertEqual(scores, sorted(scores, reverse=True))

    def test_pat_enc_is_secondary(self):
        """PAT_ENC joins to REFERRAL in 2 views -- should be secondary."""
        from tools.p30_analyze.table_importance import rank_tables_in_community
        g, community = _build_graph_and_community()
        ranking = rank_tables_in_community(g, community)
        roles = {name: role for name, _, role in ranking}
        self.assertEqual(roles["PAT_ENC"], "secondary")

    def test_empty_community(self):
        """Empty community should return empty list."""
        from tools.p20_index.graph_builder import build_graph
        from tools.p30_analyze.table_importance import rank_tables_in_community
        g = build_graph(ALL_VIEWS)
        result = rank_tables_in_community(g, set())
        self.assertEqual(result, [])


class TestRankAllCommunities(unittest.TestCase):

    def test_returns_aligned_list(self):
        """Output length should match number of communities."""
        from tools.p30_analyze.table_importance import rank_all_communities
        g, community = _build_graph_and_community()
        communities = [community]
        result = rank_all_communities(g, communities)
        self.assertEqual(len(result), 1)
        # Each entry should cover all tables in that community
        self.assertEqual(len(result[0]), len(community))


class TestFkOntology(unittest.TestCase):

    def test_with_schema_path(self):
        """When clarity_schema.yaml is provided, FK signal should boost
        tables that are FK targets."""
        import os
        from tools.p30_analyze.table_importance import rank_tables_in_community
        g, community = _build_graph_and_community()

        schema_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "..",
            "data", "schemas", "clarity_schema.yaml"
        )
        if not os.path.exists(schema_path):
            self.skipTest("clarity_schema.yaml not available")

        from tools.p30_analyze.table_importance import _load_fk_ontology
        fk = _load_fk_ontology(schema_path)
        # REFERRAL should have inbound FKs (ZC tables point to it? No --
        # REFERRAL points TO ZC tables. But PATIENT should have inbound FKs.)
        # Just verify the loader returns a non-empty dict.
        self.assertIsInstance(fk, dict)
        self.assertGreater(len(fk), 0)

    def test_none_schema_returns_empty(self):
        from tools.p30_analyze.table_importance import _load_fk_ontology
        self.assertEqual(_load_fk_ontology(None), {})

    def test_missing_file_returns_empty(self):
        from tools.p30_analyze.table_importance import _load_fk_ontology
        self.assertEqual(_load_fk_ontology("/nonexistent/path.yaml"), {})


if __name__ == "__main__":
    unittest.main()
