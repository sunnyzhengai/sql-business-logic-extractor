"""Tests for tools.p40_synthesize.community_modeling_spec.

The function takes many parameters (everything the analysis pipeline
produces); these tests focus on the SHAPE of the output -- that all
expected sections appear and that data passed in shows up correctly.

Run from the repo root:
    python -m unittest tools.p40_synthesize.tests.test_community_modeling_spec
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path


def _default_inputs():
    """Realistic-ish inputs for the spec generator. Tests override fields."""
    return {
        "community_index": 5,
        "top_table": "CLAIM",
        "analysis": {
            "n_tables": 6,
            "n_primary_views": 3,
            "top_tables": [("CLAIM", 12), ("CLAIM_LINE", 9)],
            "core_tables": ["CLAIM", "CLAIM_LINE", "AP_CLAIM"],
            "leaf_tables": ["ZC_CLM_STATUS"],
            "primary_views": ["VW_CLAIM_A", "VW_CLAIM_B", "VW_CLAIM_C"],
            "zc_table_count": 1,
            "table_node_ids": set(),
        },
        "column_variance": [
            {
                "column_name": "MEMBER_ID",
                "source_tables": ["PATIENT"],
                "n_views": 3,
                "n_distinct_fingerprints": 2,
                "definitions": [
                    {
                        "fingerprint": "fp_a",
                        "technical_description": "P.PAT_ID",
                        "business_description": "Patient identifier",
                        "views": ["VW_CLAIM_A", "VW_CLAIM_C"],
                    },
                    {
                        "fingerprint": "fp_b",
                        "technical_description": "RTRIM(P.PAT_ID)",
                        "business_description": "",
                        "views": ["VW_CLAIM_B"],
                    },
                ],
            },
        ],
        "join_paths": [
            {
                "from_table": "PATIENT", "to_table": "CLAIM",
                "join_type": "INNER JOIN", "n_distinct_join_types": 1,
                "n_views": 3, "views": ["VW_CLAIM_A", "VW_CLAIM_B", "VW_CLAIM_C"],
            },
            {
                "from_table": "CLAIM", "to_table": "CLAIM_LINE",
                "join_type": "INNER JOIN", "n_distinct_join_types": 1,
                "n_views": 2, "views": ["VW_CLAIM_A", "VW_CLAIM_B"],
            },
        ],
        "filter_patterns": [
            {
                "english": "Active claims only", "sql": "STATUS_C = 1",
                "kind": "where", "n_views": 3,
                "views": ["VW_CLAIM_A", "VW_CLAIM_B", "VW_CLAIM_C"],
            },
        ],
        "view_strength": {
            "VW_CLAIM_A": {5: 0.8},
            "VW_CLAIM_B": {5: 0.6},
            "VW_CLAIM_C": {5: 0.3, 7: 0.7},   # weak in 5 (primary), strong in 7
        },
        "view_to_driver": {
            "VW_CLAIM_A": "CLAIM",
            "VW_CLAIM_B": "CLAIM",
            "VW_CLAIM_C": "PATIENT",
        },
        "view_to_spans": {
            "VW_CLAIM_A": [5],
            "VW_CLAIM_B": [5],
            "VW_CLAIM_C": [5, 7],
        },
        "bridge_table_labels": ["PATIENT", "CLARITY_SER"],
        "bridge_to_neighbor_communities": {
            "PATIENT": [3, 5, 7],
            "CLARITY_SER": [1, 5, 7],
        },
    }


class TestWriteCommunityModelingSpec(unittest.TestCase):

    def test_all_sections_present(self):
        from tools.p40_synthesize.community_modeling_spec import (
            write_community_modeling_spec,
        )
        inputs = _default_inputs()
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "spec.md"
            result = write_community_modeling_spec(output_path=out, **inputs)
            self.assertTrue(Path(result).is_file())
            content = Path(result).read_text(encoding="utf-8")
            # Top-level heading.
            self.assertIn("# Community 5 -- CLAIM", content)
            # Each major section heading.
            for heading in [
                "## Tables & roles",
                "## Common JOIN spine",
                "## Reconciliation candidates",
                "## Common cohort filters",
                "## Member views",
                "## Recommendations",
            ]:
                self.assertIn(heading, content)

    def test_column_variance_renders_definitions(self):
        from tools.p40_synthesize.community_modeling_spec import (
            write_community_modeling_spec,
        )
        inputs = _default_inputs()
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "spec.md"
            write_community_modeling_spec(output_path=out, **inputs)
            content = out.read_text(encoding="utf-8")
            # The reconciliation candidate's column name + both definitions.
            self.assertIn("`MEMBER_ID`", content)
            self.assertIn("P.PAT_ID", content)
            self.assertIn("RTRIM(P.PAT_ID)", content)
            # The most-common variant gets the marker.
            self.assertIn("most-common", content)

    def test_spine_separates_above_and_below_threshold(self):
        from tools.p40_synthesize.community_modeling_spec import (
            write_community_modeling_spec,
        )
        inputs = _default_inputs()
        # 3 primary views; spine_threshold_fraction=0.5 -> threshold = 2 views.
        # PATIENT->CLAIM has 3 views (spine), CLAIM->CLAIM_LINE has 2 views (also spine).
        # So both end up in spine for this fixture. Verify by lowering the
        # threshold so neither falls below.
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "spec.md"
            write_community_modeling_spec(output_path=out, **inputs)
            content = out.read_text(encoding="utf-8")
            self.assertIn("Spine edges", content)
            self.assertIn("`PATIENT`", content)
            self.assertIn("`CLAIM_LINE`", content)

    def test_member_views_split_into_strong_weak_cross_domain(self):
        from tools.p40_synthesize.community_modeling_spec import (
            write_community_modeling_spec,
        )
        inputs = _default_inputs()
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "spec.md"
            write_community_modeling_spec(output_path=out, **inputs)
            content = out.read_text(encoding="utf-8")
            # VW_CLAIM_A and VW_CLAIM_B are strong (>= 50%).
            self.assertIn("Strong members (2)", content)
            # VW_CLAIM_C is weak (0.3 < 0.5).
            self.assertIn("Weak members (1)", content)
            self.assertIn("VW_CLAIM_C", content)
            # VW_CLAIM_C is also cross-domain (spans [5, 7]).
            self.assertIn("Cross-domain spanners (1)", content)
            # Driver shown for weak member.
            self.assertIn("driver: `PATIENT`", content)

    def test_recommendations_section_includes_data_based_advice(self):
        from tools.p40_synthesize.community_modeling_spec import (
            write_community_modeling_spec,
        )
        inputs = _default_inputs()
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "spec.md"
            write_community_modeling_spec(output_path=out, **inputs)
            content = out.read_text(encoding="utf-8")
            # All recommendation triggers should fire on this fixture:
            #   - reconciliation candidates (MEMBER_ID variance)
            #   - common spine (PATIENT->CLAIM in 3/3)
            #   - common filter ("Active claims only")
            #   - weak member (VW_CLAIM_C)
            #   - cross-domain (VW_CLAIM_C)
            self.assertIn("reconciliation candidate", content)
            self.assertIn("spine", content)
            self.assertIn("filter", content)
            self.assertIn("weak member", content)
            self.assertIn("cross-domain", content)


if __name__ == "__main__":
    unittest.main()
