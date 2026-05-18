"""Tests for tools.shared.view_filter -- infrastructure-view filtering.

Run from the repo root:
    python -m unittest tools.shared.tests.test_view_filter
"""

from __future__ import annotations

import unittest

from tools.shared.view_filter import (
    DEFAULT_INFRASTRUCTURE_PATTERNS,
    filter_business_views,
    is_infrastructure_view,
)


# Minimal valid views for these tests. Only the fields the filter inspects
# (`view_name`, `scopes[].reads_from_tables`) need to be populated.
_BUSINESS_VIEW = {
    "view_name": "VW_CLINIC_DX",
    "scopes": [{
        "id": "main", "kind": "main",
        "reads_from_tables": ["EPIC.PATIENT", "PAT_ENC"],
        "joins": [], "reads_from_scopes": [],
        "columns": [], "filters": [],
    }],
}

_COLLIBRA_VIEW = {
    "view_name": "VW_COLLIBRA_INGEST",
    "scopes": [],
}

_SYS_SCHEMA_VIEW = {
    "view_name": "VW_TABLE_INVENTORY",
    "scopes": [{
        "id": "main", "kind": "main",
        "reads_from_tables": ["sys.tables"],
        "joins": [], "reads_from_scopes": [],
        "columns": [], "filters": [],
    }],
}


class TestIsInfrastructureView(unittest.TestCase):
    """Single-view predicate -- both heuristics."""

    def test_collibra_in_name_is_infrastructure(self):
        self.assertTrue(
            is_infrastructure_view(_COLLIBRA_VIEW, DEFAULT_INFRASTRUCTURE_PATTERNS)
        )

    def test_name_match_is_case_insensitive(self):
        view = {"view_name": "VW_metadata_DUMP", "scopes": []}
        self.assertTrue(
            is_infrastructure_view(view, DEFAULT_INFRASTRUCTURE_PATTERNS)
        )

    def test_reads_from_sys_schema_is_infrastructure(self):
        self.assertTrue(
            is_infrastructure_view(_SYS_SCHEMA_VIEW, DEFAULT_INFRASTRUCTURE_PATTERNS)
        )

    def test_reads_from_information_schema_is_infrastructure(self):
        view = {
            "view_name": "VW_SOMETHING",
            "scopes": [{
                "id": "main", "kind": "main",
                "reads_from_tables": ["INFORMATION_SCHEMA.COLUMNS"],
                "joins": [], "reads_from_scopes": [],
                "columns": [], "filters": [],
            }],
        }
        self.assertTrue(
            is_infrastructure_view(view, DEFAULT_INFRASTRUCTURE_PATTERNS)
        )

    def test_normal_business_view_is_not_infrastructure(self):
        self.assertFalse(
            is_infrastructure_view(_BUSINESS_VIEW, DEFAULT_INFRASTRUCTURE_PATTERNS)
        )

    def test_custom_patterns_override(self):
        """Passing your own patterns replaces the default list entirely."""
        view = {"view_name": "VW_FOO", "scopes": []}
        # With default patterns ("collibra", "metadata", ...), VW_FOO doesn't match.
        self.assertFalse(
            is_infrastructure_view(view, DEFAULT_INFRASTRUCTURE_PATTERNS)
        )
        # With custom patterns containing "foo", it does.
        self.assertTrue(is_infrastructure_view(view, ["foo"]))


class TestFilterBusinessViews(unittest.TestCase):
    """Bulk split: (kept, excluded_view_names)."""

    def test_splits_kept_and_excluded(self):
        kept, excluded = filter_business_views(
            [_BUSINESS_VIEW, _COLLIBRA_VIEW, _SYS_SCHEMA_VIEW]
        )
        self.assertEqual(len(kept), 1)
        self.assertEqual(kept[0]["view_name"], "VW_CLINIC_DX")
        self.assertCountEqual(
            excluded, ["VW_COLLIBRA_INGEST", "VW_TABLE_INVENTORY"]
        )

    def test_empty_list_returns_empty_pair(self):
        kept, excluded = filter_business_views([])
        self.assertEqual(kept, [])
        self.assertEqual(excluded, [])

    def test_custom_patterns_param(self):
        kept, excluded = filter_business_views(
            [_BUSINESS_VIEW, {"view_name": "VW_AUDIT", "scopes": []}],
            name_patterns=["audit"],
        )
        self.assertEqual([v["view_name"] for v in kept], ["VW_CLINIC_DX"])
        self.assertEqual(excluded, ["VW_AUDIT"])


if __name__ == "__main__":
    unittest.main()
