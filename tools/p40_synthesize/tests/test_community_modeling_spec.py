"""Tests for tools.p40_synthesize.community_modeling_spec (Phase 3e-iv).

Lean version: the spec consolidates tables, joins, starter SQL, and
member-view list. These tests verify each of those sections renders.

Run from the repo root:
    python -m unittest tools.p40_synthesize.tests.test_community_modeling_spec
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path


def _default_inputs():
    """Inputs for the simplified spec generator."""
    return {
        "community_index": 5,
        "top_table": "CLAIM",
        "analysis": {
            "n_tables": 6,
            "n_primary_views": 3,
            "top_tables": [("CLAIM", 12)],
            "core_tables": ["CLAIM", "CLAIM_LINE", "AP_CLAIM"],
            "leaf_tables": ["ZC_CLM_STATUS"],
            "primary_views": ["VW_CLAIM_A", "VW_CLAIM_B", "VW_CLAIM_C"],
            "zc_table_count": 1,
            "table_node_ids": set(),
        },
        "join_paths": [
            {
                "from_table": "CLAIM", "to_table": "CLAIM_LINE",
                "join_type": "INNER JOIN", "n_distinct_join_types": 1,
                "on_expression": "CLAIM.claim_id = CLAIM_LINE.claim_id",
                "n_distinct_on_expressions": 1,
                "n_views": 3, "views": ["VW_CLAIM_A", "VW_CLAIM_B", "VW_CLAIM_C"],
            },
            {
                "from_table": "CLAIM", "to_table": "PATIENT",
                "join_type": "INNER JOIN", "n_distinct_join_types": 1,
                "on_expression": "CLAIM.pat_id = PATIENT.pat_id",
                "n_distinct_on_expressions": 1,
                "n_views": 3, "views": ["VW_CLAIM_A", "VW_CLAIM_B", "VW_CLAIM_C"],
            },
        ],
        "bridge_table_labels": ["PATIENT", "CLARITY_SER"],
        "bridge_to_neighbor_communities": {
            "PATIENT": [3, 5, 7],
            "CLARITY_SER": [1, 5, 7],
        },
    }


class TestWriteCommunityModelingSpec(unittest.TestCase):

    def test_all_sections_render(self):
        from tools.p40_synthesize.community_modeling_spec import (
            write_community_modeling_spec,
        )
        inputs = _default_inputs()
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "spec.md"
            result = write_community_modeling_spec(output_path=out, **inputs)
            self.assertTrue(Path(result).is_file())
            content = Path(result).read_text(encoding="utf-8")
            # Heading + 4 lean sections.
            self.assertIn("# Community 5 -- CLAIM", content)
            self.assertIn("## Tables", content)
            self.assertIn("## Joins", content)
            self.assertIn("## Starter SQL", content)
            self.assertIn("## Replaces these views (3)", content)

    def test_starter_sql_includes_create_view_and_real_on_clause(self):
        from tools.p40_synthesize.community_modeling_spec import (
            write_community_modeling_spec,
        )
        inputs = _default_inputs()
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "spec.md"
            write_community_modeling_spec(output_path=out, **inputs)
            content = out.read_text(encoding="utf-8")
            # The starter SQL block has a CREATE VIEW + real ON clauses
            # extracted from the input (not "TODO: verify").
            self.assertIn("CREATE VIEW dbo.model_claim AS", content)
            self.assertIn("FROM CLAIM", content)
            self.assertIn("CLAIM.claim_id = CLAIM_LINE.claim_id", content)
            self.assertIn("CLAIM.pat_id = PATIENT.pat_id", content)

    def test_tables_section_groups_core_bridges_lookups(self):
        from tools.p40_synthesize.community_modeling_spec import (
            write_community_modeling_spec,
        )
        inputs = _default_inputs()
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "spec.md"
            write_community_modeling_spec(output_path=out, **inputs)
            content = out.read_text(encoding="utf-8")
            # Core, Conformed dimensions, Lookups all labeled.
            self.assertIn("**Core**", content)
            self.assertIn("**Conformed dimensions**", content)
            self.assertIn("**Lookups**", content)
            # PATIENT shows up under Conformed dimensions because it's a
            # bridge that connects to community 5.
            self.assertIn("PATIENT", content)

    def test_replaced_views_lists_members(self):
        from tools.p40_synthesize.community_modeling_spec import (
            write_community_modeling_spec,
        )
        inputs = _default_inputs()
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "spec.md"
            write_community_modeling_spec(output_path=out, **inputs)
            content = out.read_text(encoding="utf-8")
            for view in ["VW_CLAIM_A", "VW_CLAIM_B", "VW_CLAIM_C"]:
                self.assertIn(view, content)

    def test_handles_community_with_no_join_paths(self):
        from tools.p40_synthesize.community_modeling_spec import (
            write_community_modeling_spec,
        )
        inputs = _default_inputs()
        inputs["join_paths"] = []  # no joins discovered
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "spec.md"
            result = write_community_modeling_spec(output_path=out, **inputs)
            self.assertTrue(Path(result).is_file())
            content = Path(result).read_text(encoding="utf-8")
            # The SQL section should explain there's no spine to model.
            self.assertIn("no JOIN spine", content)


if __name__ == "__main__":
    unittest.main()
