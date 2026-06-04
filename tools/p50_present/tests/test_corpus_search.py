"""Tests for tools.p50_present.corpus_search (unified entity + text
search across the parsed corpus)."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.p50_present.corpus_search import (
    build_search_index,
    write_corpus_search,
)


# ---------------------------------------------------------------------------
# Mock corpus with realistic-shaped content -- name, description, columns,
# filters with English, inline comments, and a ZC lookup.
# ---------------------------------------------------------------------------

def _corpus_for_search() -> list[dict]:
    return [
        {
            "view_name": "V_AR_AGING_BUCKETS",
            "report": {
                "business_description": "Accounts receivable aging buckets "
                "for the patient billing area. Used by finance dashboards.",
                "technical_description": "...",
            },
            "scopes": [{
                "id": "main", "kind": "main",
                "reads_from_tables": ["ARPB", "HSP_ACCOUNT"],
                "reads_from_scopes": [],
                "joins": [{"right_table": "HSP_ACCOUNT", "right_alias": "h",
                            "join_type": "INNER JOIN", "on_expression": "",
                            "columns": []}],
                "columns": [
                    {"column_name": "pat_id", "technical_description": "P.PAT_ID",
                     "business_description": "Patient identifier"},
                    {"column_name": "aging_bucket", "technical_description": "...",
                     "business_description": "Aging bucket category"},
                ],
                "filters": [{
                    "expression": "STATUS_C = 1",
                    "english": "Status is active",
                    "inline_comments": ["/* only open AR */"],
                    "zc_lookups": [{"column": "STATUS_C", "code": "1",
                                     "name": "Active", "zc_table": "ZC_STATUS"}],
                    "kind": "where", "subquery_scope_ids": [], "columns": [],
                }],
            }],
            "view_outputs": ["main"],
        },
        {
            "view_name": "V_ASTHMA_REGISTRY",
            "report": {
                "business_description": "Pediatric asthma cohort. Lists "
                "patients with active diagnoses for asthma or reactive "
                "airway disease.",
                "technical_description": "...",
            },
            "scopes": [{
                "id": "main", "kind": "main",
                "reads_from_tables": ["PATIENT", "PAT_ENC", "CLARITY_EDG"],
                "reads_from_scopes": [],
                "joins": [{"right_table": "PAT_ENC", "right_alias": "e",
                            "join_type": "INNER JOIN", "on_expression": "",
                            "columns": []}],
                "columns": [
                    {"column_name": "pat_id", "technical_description": "",
                     "business_description": ""},
                ],
                "filters": [{
                    "expression": "DX_ID = 1234",
                    "english": "Diagnosis is asthma, unspecified",
                    "inline_comments": [],
                    "zc_lookups": [], "kind": "where",
                    "subquery_scope_ids": [], "columns": [],
                }],
            }],
            "view_outputs": ["main"],
        },
        {
            "view_name": "VW_DAILY_CHARGES",
            "report": {
                "business_description": "Daily ARPB charge summary.",
                "technical_description": "...",
            },
            "scopes": [{
                "id": "main", "kind": "main",
                "reads_from_tables": ["ARPB", "PROC"],
                "reads_from_scopes": [], "joins": [], "columns": [],
                "filters": [],
            }],
            "view_outputs": ["main"],
        },
    ]


# ---------------------------------------------------------------------------
# Index building
# ---------------------------------------------------------------------------

class TestBuildSearchIndex(unittest.TestCase):

    def test_tables_collected_with_used_by_views(self):
        idx = build_search_index(_corpus_for_search())
        by_name = {t["name"]: t for t in idx["tables"]}
        # ARPB is used by V_AR_AGING_BUCKETS and VW_DAILY_CHARGES.
        self.assertIn("ARPB", by_name)
        self.assertEqual(by_name["ARPB"]["n_views"], 2)
        self.assertEqual(
            sorted(by_name["ARPB"]["used_by_views"]),
            ["VW_DAILY_CHARGES", "V_AR_AGING_BUCKETS"],
        )

    def test_view_index_captures_searchable_fields(self):
        idx = build_search_index(_corpus_for_search())
        v = next(v for v in idx["views"] if v["name"] == "V_AR_AGING_BUCKETS")
        # Description from report.business_description, fallback chain.
        self.assertIn("aging buckets", v["description"].lower())
        # Column names indexed.
        self.assertIn("aging_bucket", v["column_names"])
        # Filter text indexed (both raw SQL and English).
        self.assertTrue(any("STATUS_C = 1" in f for f in v["filter_text"]))
        self.assertTrue(any("active" in f.lower() for f in v["filter_text"]))
        # Inline comments captured.
        self.assertTrue(any("open AR" in c for c in v["comments"]))
        # ZC-lookup names indexed -- so a search for "Active" hits.
        self.assertIn("Active", v["zc_lookups"])

    def test_skips_views_without_a_name(self):
        idx = build_search_index([{"view_name": "", "scopes": []}])
        self.assertEqual(idx["views"], [])
        self.assertEqual(idx["tables"], [])


# ---------------------------------------------------------------------------
# HTML output
# ---------------------------------------------------------------------------

class TestWriteCorpusSearch(unittest.TestCase):

    def test_writes_html_with_embedded_index(self):
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "corpus_search.html"
            written = write_corpus_search(
                _corpus_for_search(), out,
                view_links={
                    "V_AR_AGING_BUCKETS":
                        "community_shapes/community_00_arpb_shapes.html#view-V_AR_AGING_BUCKETS",
                    "V_ASTHMA_REGISTRY":
                        "community_shapes/community_01_patient_shapes.html#view-V_ASTHMA_REGISTRY",
                    "VW_DAILY_CHARGES":
                        "community_shapes/community_00_arpb_shapes.html#view-VW_DAILY_CHARGES",
                },
            )
            self.assertTrue(written.exists())
            content = written.read_text(encoding="utf-8")

            # Search box present and the help text explains unified routing.
            self.assertIn('id="q"', content)
            self.assertIn("Type anything", content)
            # The embedded JSON index is present.
            self.assertIn('id="search-index"', content)
            self.assertIn('id="view-links"', content)
            # The actual data made it in.
            self.assertIn("ARPB", content)
            self.assertIn("V_ASTHMA_REGISTRY", content)
            # Nav link back to the corpus map.
            self.assertIn("corpus_map.html", content)

    def test_index_json_round_trips(self):
        """The embedded JSON should be valid -- a bad escape would make
        the page non-functional. Round-trip by extracting and parsing."""
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "corpus_search.html"
            write_corpus_search(_corpus_for_search(), out)
            content = out.read_text(encoding="utf-8")
            # Extract the search-index script block contents.
            start = content.index('id="search-index" type="application/json">')
            start = content.index(">", start) + 1
            end = content.index("</script>", start)
            payload = content[start:end]
            parsed = json.loads(payload)
            self.assertIn("tables", parsed)
            self.assertIn("views", parsed)


if __name__ == "__main__":
    unittest.main()
