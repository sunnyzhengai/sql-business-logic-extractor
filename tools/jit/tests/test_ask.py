"""Tests for tools.jit.ask -- Phase 1 structural queries.

Run from the repo root:
    python -m pytest tools/jit/tests/test_ask.py -v

Fixtures model a small referral domain with 3 views:
  - VW_REFERRAL_STATUS: REFERRAL + ZC_RFL_STATUS, filters on denied
  - VW_REFERRAL_ENCOUNTERS: REFERRAL + PAT_ENC + ZC_RFL_STATUS
  - VW_REFERRAL_PROVIDER: REFERRAL + PAT_ENC + CLARITY_SER
"""

from __future__ import annotations

import unittest


# ---------------------------------------------------------------------------
# Fixtures -- three views with corpus-style structure
# ---------------------------------------------------------------------------

VIEWS = [
    {
        "view_name": "VW_REFERRAL_STATUS",
        "report": {
            "business_description": "Lists denied referrals with their status.",
            "technical_description": "Row-level extraction from REFERRAL + ZC_RFL_STATUS.",
            "primary_purpose": "Denied referral tracking",
            "key_metrics": ["referral_status"],
        },
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
                 "business_description": "Unique referral identifier",
                 "base_columns": ["table:REFERRAL.REFERRAL_ID"]},
                {"column_name": "PAT_ID", "column_type": "key",
                 "business_description": "Patient receiving referral",
                 "base_columns": ["table:REFERRAL.PAT_ID"]},
                {"column_name": "referral_status", "column_type": "passthrough",
                 "business_description": "Status name from lookup",
                 "base_columns": ["table:ZC_RFL_STATUS.NAME"]},
            ],
            "filters": [
                {"expression": "REFERRAL.REFERRAL_STATUS_C = 5",
                 "english": "referral status is Denied",
                 "inline_comments": [], "zc_lookups": [], "columns": []},
            ],
        }],
    },
    {
        "view_name": "VW_REFERRAL_ENCOUNTERS",
        "report": {
            "business_description": "Referral details with encounter context.",
            "technical_description": "Row-level: REFERRAL + PAT_ENC + ZC_RFL_STATUS.",
            "primary_purpose": "Referral-encounter linkage",
            "key_metrics": ["REFERRAL_ID", "PAT_ENC_CSN_ID"],
        },
        "scopes": [{
            "id": "main",
            "kind": "main",
            "reads_from_tables": ["REFERRAL", "PAT_ENC", "ZC_RFL_STATUS"],
            "joins": [
                {"right_table": "PAT_ENC", "join_type": "INNER JOIN",
                 "on_expression": ""},
                {"right_table": "ZC_RFL_STATUS", "join_type": "LEFT JOIN",
                 "on_expression": ""},
            ],
            "reads_from_scopes": [],
            "columns": [
                {"column_name": "REFERRAL_ID", "column_type": "key",
                 "business_description": "Unique referral identifier",
                 "base_columns": ["table:REFERRAL.REFERRAL_ID"]},
                {"column_name": "PAT_ENC_CSN_ID", "column_type": "key",
                 "business_description": "Encounter serial number",
                 "base_columns": ["table:PAT_ENC.PAT_ENC_CSN_ID"]},
            ],
            "filters": [
                {"expression": "PAT_ENC.APPT_STATUS_C = 2",
                 "english": "appointment status is Completed",
                 "inline_comments": [], "zc_lookups": [], "columns": []},
            ],
        }],
    },
    {
        "view_name": "VW_REFERRAL_PROVIDER",
        "report": {
            "business_description": "Referrals with provider details.",
            "technical_description": "Row-level: REFERRAL + PAT_ENC + CLARITY_SER.",
            "primary_purpose": "Referral provider analysis",
            "key_metrics": ["REFERRAL_ID", "PROV_NAME"],
        },
        "scopes": [{
            "id": "main",
            "kind": "main",
            "reads_from_tables": ["REFERRAL", "PAT_ENC", "CLARITY_SER"],
            "joins": [
                {"right_table": "PAT_ENC", "join_type": "INNER JOIN",
                 "on_expression": ""},
                {"right_table": "CLARITY_SER", "join_type": "INNER JOIN",
                 "on_expression": ""},
            ],
            "reads_from_scopes": [],
            "columns": [
                {"column_name": "REFERRAL_ID", "column_type": "key",
                 "business_description": "Unique referral identifier",
                 "base_columns": ["table:REFERRAL.REFERRAL_ID"]},
                {"column_name": "PROV_NAME", "column_type": "passthrough",
                 "business_description": "Provider full name",
                 "base_columns": ["table:CLARITY_SER.PROV_NAME"]},
            ],
            "filters": [],
        }],
    },
]


def _build_index():
    from tools.jit.ask import StructuralIndex
    return StructuralIndex(VIEWS)


# ---------------------------------------------------------------------------
# StructuralIndex tests
# ---------------------------------------------------------------------------

class TestStructuralIndex(unittest.TestCase):

    def test_table_to_views_referral_in_all_three(self):
        idx = _build_index()
        views = idx.table_to_views.get("REFERRAL", set())
        self.assertEqual(len(views), 3)

    def test_table_to_views_clarity_ser_in_one(self):
        idx = _build_index()
        views = idx.table_to_views.get("CLARITY_SER", set())
        self.assertEqual(views, {"VW_REFERRAL_PROVIDER"})

    def test_column_to_views_referral_id_in_all_three(self):
        idx = _build_index()
        entries = idx.column_to_views.get("REFERRAL_ID", [])
        view_names = {e[0] for e in entries}
        self.assertEqual(len(view_names), 3)

    def test_column_to_views_prov_name_in_one(self):
        idx = _build_index()
        entries = idx.column_to_views.get("PROV_NAME", [])
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0][0], "VW_REFERRAL_PROVIDER")

    def test_all_table_names(self):
        idx = _build_index()
        expected = {"REFERRAL", "ZC_RFL_STATUS", "PAT_ENC", "CLARITY_SER"}
        self.assertEqual(idx.all_table_names, expected)


class TestFindByTable(unittest.TestCase):

    def test_referral_returns_three_views(self):
        idx = _build_index()
        results = idx.find_by_table("REFERRAL")
        self.assertEqual(len(results), 3)

    def test_each_result_has_required_fields(self):
        idx = _build_index()
        results = idx.find_by_table("REFERRAL")
        for r in results:
            self.assertIn("view_name", r)
            self.assertIn("business_description", r)
            self.assertIn("table_role", r)

    def test_driver_role_for_referral(self):
        """REFERRAL is the first table in reads_from_tables -> driver."""
        idx = _build_index()
        results = idx.find_by_table("REFERRAL")
        roles = {r["view_name"]: r["table_role"] for r in results}
        for vname in roles:
            self.assertEqual(roles[vname], "driver")

    def test_lookup_role_for_zc(self):
        idx = _build_index()
        results = idx.find_by_table("ZC_RFL_STATUS")
        for r in results:
            self.assertEqual(r["table_role"], "lookup")

    def test_nonexistent_table_returns_empty(self):
        idx = _build_index()
        self.assertEqual(idx.find_by_table("NONEXISTENT"), [])


class TestFindByColumn(unittest.TestCase):

    def test_referral_id_found_in_three_views(self):
        idx = _build_index()
        results = idx.find_by_column("REFERRAL_ID")
        self.assertEqual(len(results), 3)

    def test_column_definition_populated(self):
        idx = _build_index()
        results = idx.find_by_column("PROV_NAME")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["column_definition"], "Provider full name")

    def test_nonexistent_column_returns_empty(self):
        idx = _build_index()
        self.assertEqual(idx.find_by_column("FAKE_COL"), [])


class TestDescribeView(unittest.TestCase):

    def test_returns_full_view_detail(self):
        idx = _build_index()
        result = idx.describe_view("VW_REFERRAL_STATUS")
        self.assertIsNotNone(result)
        self.assertEqual(result["view_name"], "VW_REFERRAL_STATUS")
        self.assertIn("REFERRAL", [t["name"] for t in result["tables"]])
        self.assertEqual(len(result["columns"]), 3)
        self.assertEqual(len(result["filters"]), 1)

    def test_case_insensitive_lookup(self):
        idx = _build_index()
        result = idx.describe_view("vw_referral_status")
        self.assertIsNotNone(result)

    def test_nonexistent_view_returns_none(self):
        idx = _build_index()
        self.assertIsNone(idx.describe_view("NONEXISTENT_VIEW"))


class TestFindByFilter(unittest.TestCase):

    def test_denied_finds_referral_status_view(self):
        idx = _build_index()
        results = idx.find_by_filter("denied")
        view_names = {r["view_name"] for r in results}
        self.assertIn("VW_REFERRAL_STATUS", view_names)

    def test_completed_finds_encounters_view(self):
        idx = _build_index()
        results = idx.find_by_filter("completed")
        view_names = {r["view_name"] for r in results}
        self.assertIn("VW_REFERRAL_ENCOUNTERS", view_names)

    def test_matching_filters_populated(self):
        idx = _build_index()
        results = idx.find_by_filter("denied")
        for r in results:
            if r["view_name"] == "VW_REFERRAL_STATUS":
                self.assertGreater(len(r["matching_filters"]), 0)

    def test_no_match_returns_empty(self):
        idx = _build_index()
        self.assertEqual(idx.find_by_filter("xyznonexistent"), [])


# ---------------------------------------------------------------------------
# Router tests
# ---------------------------------------------------------------------------

class TestRouter(unittest.TestCase):

    def setUp(self):
        import tools.jit.ask as mod
        self.idx = _build_index()
        mod._INDEX = self.idx

    def test_table_question_routes_to_table_lookup(self):
        from tools.jit.ask import ask
        result = ask("which views use the REFERRAL table?")
        self.assertEqual(result.query_type, "table_lookup")
        self.assertEqual(result.match_term, "REFERRAL")

    def test_view_question_routes_to_view_detail(self):
        from tools.jit.ask import ask
        result = ask("what does VW_REFERRAL_STATUS do?")
        self.assertEqual(result.query_type, "view_detail")

    def test_column_question_routes_to_column_lookup(self):
        from tools.jit.ask import ask
        result = ask("which views produce PROV_NAME?")
        self.assertEqual(result.query_type, "column_lookup")
        self.assertEqual(result.match_term, "PROV_NAME")

    def test_filter_question_routes_to_filter_lookup(self):
        from tools.jit.ask import ask
        result = ask("which views filter on denied status?")
        self.assertEqual(result.query_type, "filter_lookup")

    def test_unknown_question_returns_no_match(self):
        from tools.jit.ask import ask
        result = ask("what is the meaning of life?")
        self.assertEqual(result.query_type, "no_match")


class TestMarkdownFormatting(unittest.TestCase):

    def setUp(self):
        import tools.jit.ask as mod
        self.idx = _build_index()
        mod._INDEX = self.idx

    def test_table_lookup_renders_markdown(self):
        from tools.jit.ask import ask
        result = ask("which views use REFERRAL?")
        md = result.to_markdown()
        self.assertIn("## Views using `REFERRAL`", md)
        self.assertIn("VW_REFERRAL_STATUS", md)
        self.assertIn("3 found", md)

    def test_view_detail_renders_markdown(self):
        from tools.jit.ask import ask
        result = ask("tell me about VW_REFERRAL_STATUS")
        md = result.to_markdown()
        self.assertIn("## VW_REFERRAL_STATUS", md)
        self.assertIn("Denied referral tracking", md)
        self.assertIn("REFERRAL_ID", md)

    def test_column_lookup_renders_markdown(self):
        from tools.jit.ask import ask
        result = ask("where is PROV_NAME used?")
        md = result.to_markdown()
        self.assertIn("## Views producing `PROV_NAME`", md)
        self.assertIn("Provider full name", md)

    def test_no_match_renders_helpful_message(self):
        from tools.jit.ask import ask
        result = ask("random gibberish")
        md = result.to_markdown()
        self.assertIn("couldn't find", md)


if __name__ == "__main__":
    unittest.main()
