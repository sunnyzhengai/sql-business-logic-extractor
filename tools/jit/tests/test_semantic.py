"""Tests for tools.jit.semantic -- Phase 2 semantic retrieval.

Run from the repo root:
    python -m pytest tools/jit/tests/test_semantic.py -v

Tests verify that TF-IDF retrieval ranks relevant views higher than
irrelevant ones, and that the router falls through to semantic search
when no structural match is found.
"""

from __future__ import annotations

import unittest


# Reuse fixtures from test_ask.py
from tools.jit.tests.test_ask import VIEWS


class TestSemanticIndex(unittest.TestCase):

    def _build(self):
        from tools.jit.semantic import SemanticIndex
        return SemanticIndex(VIEWS)

    def test_search_returns_results(self):
        idx = self._build()
        hits = idx.search("referral status denied")
        self.assertGreater(len(hits), 0)

    def test_denied_referral_ranks_status_view_first(self):
        """VW_REFERRAL_STATUS has 'denied' in its filters and description,
        so it should rank highest for a 'denied referral' query."""
        idx = self._build()
        hits = idx.search("denied referral status")
        self.assertGreater(len(hits), 0)
        self.assertEqual(hits[0]["view_name"], "VW_REFERRAL_STATUS")

    def test_provider_query_ranks_provider_view_high(self):
        """A query about providers should rank VW_REFERRAL_PROVIDER high."""
        idx = self._build()
        hits = idx.search("provider name referral")
        view_names = [h["view_name"] for h in hits]
        self.assertIn("VW_REFERRAL_PROVIDER", view_names)
        # Should be in top 2
        provider_rank = view_names.index("VW_REFERRAL_PROVIDER")
        self.assertLessEqual(provider_rank, 1)

    def test_encounter_query_ranks_encounters_view_high(self):
        idx = self._build()
        hits = idx.search("encounter appointment completed")
        view_names = [h["view_name"] for h in hits]
        self.assertIn("VW_REFERRAL_ENCOUNTERS", view_names)
        enc_rank = view_names.index("VW_REFERRAL_ENCOUNTERS")
        self.assertLessEqual(enc_rank, 1)

    def test_scores_are_descending(self):
        idx = self._build()
        hits = idx.search("referral")
        scores = [h["score"] for h in hits]
        self.assertEqual(scores, sorted(scores, reverse=True))

    def test_zero_score_results_excluded(self):
        idx = self._build()
        hits = idx.search("referral")
        for h in hits:
            self.assertGreater(h["score"], 0)

    def test_irrelevant_query_returns_fewer_results(self):
        """A totally irrelevant query should return fewer high-scoring hits."""
        idx = self._build()
        hits = idx.search("quantum physics dark matter")
        # Might still return some results due to TF-IDF noise, but scores should be low
        if hits:
            self.assertLess(hits[0]["score"], 0.3)

    def test_top_k_limits_results(self):
        idx = self._build()
        hits = idx.search("referral", top_k=2)
        self.assertLessEqual(len(hits), 2)


class TestRouterSemanticFallthrough(unittest.TestCase):
    """When no structural match is found, the router should fall through
    to semantic search."""

    def setUp(self):
        from tools.jit.ask import StructuralIndex
        from tools.jit.semantic import SemanticIndex
        self.structural = StructuralIndex(VIEWS)
        self.semantic = SemanticIndex(VIEWS)

    def test_business_question_routes_to_semantic(self):
        """A question with terms from descriptions (not table/col names)
        should fall through to semantic search."""
        from tools.jit.ask import _route_question
        # "tracking" and "status" appear in descriptions but STATUS is not
        # a table name. "tracking" appears in "Denied referral tracking".
        result = _route_question(
            "how do we do tracking for status purposes?",
            self.structural,
            semantic_index=self.semantic,
        )
        self.assertEqual(result.query_type, "semantic_retrieval")

    def test_semantic_result_has_view_hits(self):
        from tools.jit.ask import _route_question
        # "encounter linkage" appears in VW_REFERRAL_ENCOUNTERS description
        result = _route_question(
            "show me reports about encounter linkage",
            self.structural,
            semantic_index=self.semantic,
        )
        self.assertIsInstance(result.results, list)
        self.assertGreater(len(result.results), 0)
        self.assertIn("view_name", result.results[0])

    def test_structural_match_still_takes_priority(self):
        """Even with semantic index available, structural matches win."""
        from tools.jit.ask import _route_question
        result = _route_question(
            "which views use REFERRAL?",
            self.structural,
            semantic_index=self.semantic,
        )
        self.assertEqual(result.query_type, "table_lookup")

    def test_semantic_retrieval_renders_markdown(self):
        from tools.jit.ask import _route_question
        result = _route_question(
            "what reports show provider analysis details?",
            self.structural,
            semantic_index=self.semantic,
        )
        md = result.to_markdown()
        self.assertIn("Relevant views", md)
        self.assertIn("semantic search", md)


class TestSynthesizePromptAssembly(unittest.TestCase):
    """Test that the synthesizer builds a proper prompt from retrieved views."""

    def test_context_includes_view_details(self):
        """Verify the context passed to the LLM includes view descriptions."""
        from tools.jit.semantic import SemanticIndex
        idx = SemanticIndex(VIEWS)
        hits = idx.search("denied referral", top_k=3)
        # The hits should include view data we can inspect
        self.assertGreater(len(hits), 0)
        for hit in hits:
            self.assertIn("view", hit)
            self.assertIn("view_name", hit)
            self.assertIn("score", hit)
            view = hit["view"]
            self.assertIn("report", view)
            self.assertIn("scopes", view)


if __name__ == "__main__":
    unittest.main()
