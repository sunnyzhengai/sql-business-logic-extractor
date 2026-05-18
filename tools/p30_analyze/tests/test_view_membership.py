"""Tests for tools.p30_analyze.view_membership.

Run from the repo root:
    python -m unittest tools.p30_analyze.tests.test_view_membership
"""

from __future__ import annotations

import unittest


# Fixture: same shape as the validate_graph_pivot tests use.
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

# An "outlier" view that has Patient as driver but reaches into the claims
# community via a single peripheral table.
SAMPLE_VIEW_OUTLIER = {
    "view_name": "VW_PATIENT_CLAIM_SUMMARY",
    "scopes": [{
        "id": "main",
        "kind": "main",
        "reads_from_tables": ["EPIC.PATIENT", "CLAIM_C"],
        "joins": [
            {"right_table": "CLAIM_C", "join_type": "LEFT JOIN", "on_expression": ""},
        ],
        "reads_from_scopes": [],
        "columns": [],
        "filters": [],
    }],
}


class TestComputeViewMembershipStrength(unittest.TestCase):
    def test_view_with_all_tables_in_one_community_has_full_strength(self):
        from tools.p20_index.graph_builder import build_graph
        from tools.p30_analyze.view_membership import compute_view_membership_strength

        g = build_graph([SAMPLE_VIEW_CLINIC])
        # Put every non-bridge table in one community.
        communities = [{
            "table::PAT_ENC", "table::PAT_ENC_DX", "table::ZC_DX_TYPE",
            "table::PATIENT",
        }]
        strength = compute_view_membership_strength(g, communities)
        self.assertEqual(strength["VW_CLINIC_DX"], {0: 1.0})

    def test_view_split_across_two_communities(self):
        from tools.p20_index.graph_builder import build_graph
        from tools.p30_analyze.view_membership import compute_view_membership_strength

        g = build_graph([SAMPLE_VIEW_CLINIC])
        communities = [
            {"table::PATIENT", "table::PAT_ENC"},      # 2 tables in community 0
            {"table::PAT_ENC_DX", "table::ZC_DX_TYPE"}  # 2 tables in community 1
        ]
        strength = compute_view_membership_strength(g, communities)
        s = strength["VW_CLINIC_DX"]
        # 2 of 4 tables in each community.
        self.assertAlmostEqual(s[0], 0.5)
        self.assertAlmostEqual(s[1], 0.5)

    def test_view_with_only_bridge_tables_has_empty_strength(self):
        """If every table the view touches is a bridge (excluded from any
        community), the view's strength dict is empty."""
        from tools.p20_index.graph_builder import build_graph
        from tools.p30_analyze.view_membership import compute_view_membership_strength

        g = build_graph([SAMPLE_VIEW_OUTLIER])
        # No communities -- everything was deemed bridge.
        communities: list[set] = []
        strength = compute_view_membership_strength(g, communities)
        # View touches PATIENT + CLAIM_C, neither in any community.
        self.assertEqual(strength["VW_PATIENT_CLAIM_SUMMARY"], {})

    def test_weak_member_pattern(self):
        """The outlier case from the design discussion: PATIENT is a bridge,
        CLAIM_C is in the claims community. View has 1/1 non-bridge table
        in claims -> 100% by the metric (because PATIENT was excluded as a
        bridge, only CLAIM_C contributes). This shows the metric is FAIRLY
        ENFORCING what bridges mean -- the view IS a member, just on a thin
        signal. The "weakness" surfaces in the absolute count being tiny."""
        from tools.p20_index.graph_builder import build_graph
        from tools.p30_analyze.view_membership import compute_view_membership_strength

        g = build_graph([SAMPLE_VIEW_OUTLIER])
        communities = [{"table::CLAIM_C"}]
        strength = compute_view_membership_strength(g, communities)
        # PATIENT is excluded because it's NOT in any community (we didn't
        # add it). CLAIM_C is in community 0. So 1/1 non-bridge = 100%.
        # This is correct behavior -- the metric reports membership relative
        # to ASSIGNED tables, not raw table count.
        self.assertEqual(strength["VW_PATIENT_CLAIM_SUMMARY"], {0: 1.0})


class TestViewDriverTable(unittest.TestCase):
    def test_driver_is_most_common_left_in_joins(self):
        from tools.p20_index.graph_builder import build_graph
        from tools.p30_analyze.view_membership import view_driver_table

        g = build_graph([SAMPLE_VIEW_CLINIC])
        # SAMPLE_VIEW_CLINIC's main scope: reads_from_tables starts with
        # EPIC.PATIENT, then has joins to PAT_ENC, PAT_ENC_DX, ZC_DX_TYPE.
        # build_graph uses the FIRST table seen as the left of every JOIN,
        # so PATIENT is the driver.
        self.assertEqual(view_driver_table(g, "VW_CLINIC_DX"), "PATIENT")

    def test_view_with_no_joins_returns_none(self):
        from tools.p20_index.graph_builder import build_graph
        from tools.p30_analyze.view_membership import view_driver_table

        single_table_view = {
            "view_name": "VW_SIMPLE",
            "scopes": [{
                "id": "main", "kind": "main",
                "reads_from_tables": ["PATIENT"],
                "joins": [], "reads_from_scopes": [],
                "columns": [], "filters": [],
            }],
        }
        g = build_graph([single_table_view])
        self.assertIsNone(view_driver_table(g, "VW_SIMPLE"))


class TestClassifyViewsByStrength(unittest.TestCase):
    def test_strong_above_threshold_weak_below(self):
        from tools.p30_analyze.view_membership import classify_views_by_strength

        # 3 community slots; we only populate the strength data we want to test.
        communities = [set(), set(), set()]
        view_strength = {
            "VW_STRONG":   {0: 0.8, 1: 0.2},   # primary=0 with 0.8 -> strong
            "VW_WEAK":     {0: 0.3, 1: 0.2, 2: 0.1},  # primary=0 with 0.3 -> weak
            "VW_BORDERLINE": {0: 0.5},          # primary=0 with 0.5 -> strong (== threshold)
            "VW_NO_COMMUNITY": {},               # touches only bridges
        }
        result = classify_views_by_strength(view_strength, communities, threshold=0.5)
        self.assertEqual(result[0]["strong"], ["VW_BORDERLINE", "VW_STRONG"])
        self.assertEqual(result[0]["weak"], ["VW_WEAK"])
        # VW_NO_COMMUNITY appears nowhere.
        for buckets in result.values():
            self.assertNotIn("VW_NO_COMMUNITY", buckets["strong"])
            self.assertNotIn("VW_NO_COMMUNITY", buckets["weak"])


if __name__ == "__main__":
    unittest.main()
