"""Tests for tools.p50_present.community_matrix.

Verifies the renderer produces the three matrices in order, that
grain classification works (Clarity defaults + prefix fallback),
that the alignment + grain-changer footers compute correctly, and
that build_view_data correctly assembles the input from ViewV1
dicts.

Run from repo root:
    python -m pytest tools/p50_present/tests/test_community_matrix.py
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

# Synthetic 4-view community mimicking a tight Patient Access cluster
# plus one finer-grain outlier. Values chosen so the test can assert
# specific alignment / grain-changer numbers.
SAMPLE_VIEW_DATA = {
    "VW_A_ENC_CLOSE": {
        "tables": ["PATIENT", "PAT_ENC", "CLARITY_SER", "ZC_APPT_STATUS"],
        "filters": ["Encounter status = Closed"],
        "base_columns": ["PATIENT.PAT_ID", "PAT_ENC.ENC_DATE", "PAT_ENC.STATUS_C"],
    },
    "VW_B_DEPT_CLOSE": {
        "tables": ["PATIENT", "PAT_ENC", "CLARITY_DEP", "ZC_APPT_STATUS"],
        "filters": ["Encounter status = Closed"],
        "base_columns": ["PATIENT.PAT_ID", "PAT_ENC.ENC_DATE", "PAT_ENC.STATUS_C", "PAT_ENC.DEPT_ID"],
    },
    "VW_C_CANCEL": {
        "tables": ["PATIENT", "PAT_ENC", "CLARITY_SER", "CLARITY_DEP", "ZC_APPT_STATUS"],
        "filters": ["Encounter status = Cancelled"],
        "base_columns": ["PATIENT.PAT_ID", "PAT_ENC.ENC_DATE", "PAT_ENC.STATUS_C"],
    },
    "VW_D_BP_CONTROL": {  # the grain-changer: brings FLOWSHEET + PAT_ENC_DX
        "tables": ["PATIENT", "PAT_ENC", "FLOWSHEET", "PAT_ENC_DX"],
        "filters": ["BP measurement is most recent"],
        "base_columns": ["PATIENT.PAT_ID", "PAT_ENC.ENC_DATE", "FLOWSHEET.MEAS_VALUE"],
    },
}


class TestClassifyTableGrain(unittest.TestCase):

    def test_known_cohort_table(self):
        from tools.p50_present.community_matrix import _classify_table_grain
        info = _classify_table_grain("PAT_ENC")
        self.assertEqual(info["category"], "fact")
        self.assertEqual(info["level"], 0)
        self.assertEqual(info["label"], "cohort")

    def test_known_grain_expander(self):
        from tools.p50_present.community_matrix import _classify_table_grain
        info = _classify_table_grain("FLOWSHEET")
        self.assertEqual(info["category"], "fact")
        self.assertEqual(info["level"], +1)
        self.assertIn("measurement", info["label"])

    def test_known_dim(self):
        from tools.p50_present.community_matrix import _classify_table_grain
        info = _classify_table_grain("PATIENT")
        self.assertEqual(info["category"], "dim")
        self.assertIsNone(info["level"])

    def test_zc_prefix_fallback(self):
        """Unknown ZC_* tables default to code-lookup classification."""
        from tools.p50_present.community_matrix import _classify_table_grain
        info = _classify_table_grain("ZC_NEW_LOOKUP_NOT_IN_DICT")
        self.assertEqual(info["category"], "code")

    def test_clarity_prefix_fallback(self):
        """Unknown CLARITY_* tables default to dim classification."""
        from tools.p50_present.community_matrix import _classify_table_grain
        info = _classify_table_grain("CLARITY_NEW_DIM_NOT_IN_DICT")
        self.assertEqual(info["category"], "dim")

    def test_unknown_table_falls_back_to_question_mark(self):
        from tools.p50_present.community_matrix import _classify_table_grain
        info = _classify_table_grain("V_CUSTOM_BI_TABLE")
        self.assertEqual(info["label"], "?")

    def test_schema_prefix_stripped(self):
        """Tables qualified with schema (Clarity.dbo.PAT_ENC) should
        resolve to the bare PAT_ENC classification."""
        from tools.p50_present.community_matrix import _classify_table_grain
        info = _classify_table_grain("Clarity.dbo.PAT_ENC")
        self.assertEqual(info["category"], "fact")
        self.assertEqual(info["level"], 0)


class TestViewGrainChange(unittest.TestCase):

    def test_cohort_only_view_returns_zero(self):
        from tools.p50_present.community_matrix import _view_grain_change
        # PAT_ENC is the cohort; PATIENT + CLARITY_SER are dims.
        n = _view_grain_change(["PATIENT", "PAT_ENC", "CLARITY_SER"])
        self.assertEqual(n, 0)

    def test_finer_grain_join_returns_positive_count(self):
        from tools.p50_present.community_matrix import _view_grain_change
        # PAT_ENC + FLOWSHEET + PAT_ENC_DX -> output level = +1,
        # count of finer-grain facts = 2.
        n = _view_grain_change(["PATIENT", "PAT_ENC", "FLOWSHEET", "PAT_ENC_DX"])
        self.assertEqual(n, 2)

    def test_coarser_anchor_returns_negative_offset(self):
        from tools.p50_present.community_matrix import _view_grain_change
        # Only PAT_PCP (level -1) as the fact -> anchor is one level coarser.
        n = _view_grain_change(["PATIENT", "PAT_PCP", "CLARITY_SER"])
        self.assertEqual(n, -1)

    def test_no_facts_returns_zero(self):
        from tools.p50_present.community_matrix import _view_grain_change
        # Only dims -- no grain change.
        n = _view_grain_change(["PATIENT", "CLARITY_SER", "CLARITY_DEP"])
        self.assertEqual(n, 0)


class TestWriteCommunityMatrix(unittest.TestCase):

    def test_writes_file_with_three_matrices(self):
        from tools.p50_present.community_matrix import write_community_matrix
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "test_matrix.md"
            primary_views = sorted(SAMPLE_VIEW_DATA.keys())
            result = write_community_matrix(
                community_index=0,
                top_table="PAT_ENC",
                primary_views=primary_views,
                view_data=SAMPLE_VIEW_DATA,
                output_path=out,
            )
            self.assertTrue(Path(result).is_file())
            content = Path(result).read_text(encoding="utf-8")
            # Three matrices, in priority order.
            self.assertIn("## 1. Table matrix", content)
            self.assertIn("## 2. Filter / cohort matrix", content)
            self.assertIn("## 3. Base column matrix", content)
            # Top-level heading uses community index + top table.
            self.assertIn("# Community 0 -- PAT_ENC", content)

    def test_table_matrix_has_grain_column_and_changers_footer(self):
        from tools.p50_present.community_matrix import write_community_matrix
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "test.md"
            write_community_matrix(
                community_index=1,
                top_table="PAT_ENC",
                primary_views=sorted(SAMPLE_VIEW_DATA.keys()),
                view_data=SAMPLE_VIEW_DATA,
                output_path=out,
            )
            content = out.read_text(encoding="utf-8")
            # Grain column shows up in the table-matrix header. Padding
            # may insert variable whitespace, so test for the cells
            # individually rather than a fixed substring.
            self.assertRegex(content, r"\|\s*table\s*\|\s*grain\s*\|")
            self.assertIn("grain-changers joined", content)
            # PAT_ENC labeled as cohort; FLOWSHEET as grain expander.
            self.assertIn("cohort", content)
            self.assertIn("↑ per measurement", content)

    def test_filter_and_base_column_matrices_have_no_grain_column(self):
        from tools.p50_present.community_matrix import write_community_matrix
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "test.md"
            write_community_matrix(
                community_index=1, top_table="PAT_ENC",
                primary_views=sorted(SAMPLE_VIEW_DATA.keys()),
                view_data=SAMPLE_VIEW_DATA, output_path=out,
            )
            content = out.read_text(encoding="utf-8")
            # Filter matrix header has no "grain" column (test against
            # padding-tolerant regex). The cell right after the feature
            # label should be R1 -- no grain cell in between.
            self.assertRegex(content, r"\|\s*filter / cohort definition\s*\|\s*R1\s*\|")
            self.assertRegex(content, r"\|\s*base column \(TABLE\.COLUMN\)\s*\|\s*R1\s*\|")

    def test_legend_lists_all_views(self):
        from tools.p50_present.community_matrix import write_community_matrix
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "test.md"
            primary_views = sorted(SAMPLE_VIEW_DATA.keys())
            write_community_matrix(
                community_index=0, top_table="PAT_ENC",
                primary_views=primary_views, view_data=SAMPLE_VIEW_DATA,
                output_path=out,
            )
            content = out.read_text(encoding="utf-8")
            # Every view appears in the legend.
            for vn in primary_views:
                self.assertIn(vn, content)

    def test_empty_community_writes_stub(self):
        from tools.p50_present.community_matrix import write_community_matrix
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "test.md"
            result = write_community_matrix(
                community_index=5, top_table="ORPHAN",
                primary_views=[], view_data={}, output_path=out,
            )
            self.assertTrue(Path(result).is_file())
            content = out.read_text(encoding="utf-8")
            self.assertIn("no primary views", content)

    def test_truncation_note_appears_for_oversized_community(self):
        from tools.p50_present.community_matrix import write_community_matrix
        # Build a 25-view community to exceed the default cap of 20.
        big_view_data = {
            f"VW_{i:02d}": {
                "tables": ["PATIENT", "PAT_ENC"], "filters": [], "base_columns": [],
            } for i in range(25)
        }
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "test.md"
            write_community_matrix(
                community_index=0, top_table="PAT_ENC",
                primary_views=sorted(big_view_data.keys()),
                view_data=big_view_data, output_path=out,
                max_views_in_matrix=20,
            )
            content = out.read_text(encoding="utf-8")
            self.assertIn("25 views; showing the first 20", content)


class TestBuildViewData(unittest.TestCase):

    def test_assembles_three_dimensions_from_viewv1_dict(self):
        """build_view_data() should produce the same shape the renderer
        consumes -- tables (from view_to_tables map), filters (from
        scope filters), base_columns (paired TABLE.COLUMN)."""
        from tools.p50_present.community_matrix import build_view_data

        # Minimal ViewV1-shaped dict.
        view = {
            "view_name": "VW_TEST",
            "scopes": [
                {
                    "id": "main",
                    "filters": [
                        {"english": "Patient is active", "expression": "p.status = 'A'"},
                        {"english": "Encounter is closed", "expression": "e.status_c = 2"},
                    ],
                    "columns": [
                        {
                            "column_name": "pat_id",
                            "base_columns": ("PAT_ID",),
                            "base_tables": ("PATIENT",),
                        },
                        {
                            "column_name": "enc_date",
                            "base_columns": ("ENC_DATE",),
                            "base_tables": ("PAT_ENC",),
                        },
                    ],
                },
            ],
        }
        # view_to_tables_map is normally computed by p30_analyze; here we
        # mimic its output for the one view under test.
        view_to_tables = {"VW_TEST": {"PATIENT", "PAT_ENC"}}

        result = build_view_data([view], view_to_tables)
        self.assertIn("VW_TEST", result)
        data = result["VW_TEST"]

        self.assertEqual(sorted(data["tables"]), ["PATIENT", "PAT_ENC"])
        self.assertIn("Patient is active", data["filters"])
        self.assertIn("Encounter is closed", data["filters"])
        # Base columns paired with their base table -> TABLE.COLUMN form.
        self.assertIn("PATIENT.PAT_ID", data["base_columns"])
        self.assertIn("PAT_ENC.ENC_DATE", data["base_columns"])

    def test_deduplicates_filters_across_scopes(self):
        from tools.p50_present.community_matrix import build_view_data
        # A view with the same filter english in two scopes.
        view = {
            "view_name": "VW_DUP",
            "scopes": [
                {"id": "main", "filters": [{"english": "Status = Closed"}], "columns": []},
                {"id": "cte_x", "filters": [{"english": "Status = Closed"}], "columns": []},
            ],
        }
        result = build_view_data([view], {"VW_DUP": set()})
        self.assertEqual(result["VW_DUP"]["filters"].count("Status = Closed"), 1)

    def test_strips_schema_prefix_from_table_in_base_columns(self):
        """When base_tables contains schema-qualified names like
        Clarity.dbo.PAT_ENC, the base_columns entry should use the
        bare table name."""
        from tools.p50_present.community_matrix import build_view_data
        view = {
            "view_name": "VW_SCHEMA",
            "scopes": [{
                "id": "main",
                "filters": [],
                "columns": [{
                    "column_name": "pat_id",
                    "base_columns": ("PAT_ID",),
                    "base_tables": ("Clarity.dbo.PATIENT",),
                }],
            }],
        }
        result = build_view_data([view], {"VW_SCHEMA": set()})
        self.assertIn("PATIENT.PAT_ID", result["VW_SCHEMA"]["base_columns"])


class TestNoiseGuards(unittest.TestCase):
    """The renderer-side hygiene guards that drop extractor noise."""

    def test_is_real_table_name_accepts_normal_identifiers(self):
        from tools.p50_present.community_matrix import _is_real_table_name
        self.assertTrue(_is_real_table_name("PAT_ENC"))
        self.assertTrue(_is_real_table_name("PATIENT"))
        self.assertTrue(_is_real_table_name("V_CCHP_UMAuthorization_Fact"))
        self.assertTrue(_is_real_table_name("dbo.PATIENT"))

    def test_is_real_table_name_rejects_cte_fragments(self):
        from tools.p50_present.community_matrix import _is_real_table_name
        # CTE-definition fragments that leaked through as "tables"
        self.assertFalse(_is_real_table_name("DAY_OF_MONTH = 1) AS DD"))
        self.assertFalse(_is_real_table_name("MYPT_ID WHERE 1 = 1"))
        self.assertFalse(_is_real_table_name("HSP_ACCOUNT_ID IS NULL WHERE 1 = 1"))
        self.assertFalse(_is_real_table_name("CROSS APPLY DateDim"))

    def test_is_real_filter_accepts_real_predicates(self):
        from tools.p50_present.community_matrix import _is_real_filter
        self.assertTrue(_is_real_filter("Coverage Type C = 2"))
        self.assertTrue(_is_real_filter("Encounter Date >= 2024-01-01"))
        self.assertTrue(_is_real_filter("Status in ('A', 'B')"))

    def test_is_real_filter_rejects_tautology(self):
        from tools.p50_present.community_matrix import _is_real_filter
        self.assertFalse(_is_real_filter("1 = 1"))
        self.assertFalse(_is_real_filter("1=1"))
        self.assertFalse(_is_real_filter("(1 = 1)"))
        self.assertFalse(_is_real_filter("  1 = 1  "))

    def test_is_real_filter_rejects_self_equality(self):
        """JOIN ON keys like `a.PAT_ID = b.PAT_ID` render as
        `Patient Identifier = Patient Identifier` and aren't cohort
        definitions."""
        from tools.p50_present.community_matrix import _is_real_filter
        self.assertFalse(_is_real_filter("Patient Identifier = Patient Identifier"))
        self.assertFalse(_is_real_filter("Coverage Identifier = Coverage Identifier"))

    def test_is_real_filter_keeps_compound_predicates(self):
        """Compounds combining a self-equality with a real predicate keep
        signal -- don't drop them wholesale."""
        from tools.p50_present.community_matrix import _is_real_filter
        compound = "Patient Identifier = Patient Identifier and Coverage Type C = 2"
        self.assertTrue(_is_real_filter(compound))


class TestBuildViewDataAppliesGuards(unittest.TestCase):

    def test_filters_drop_tautology_and_self_equality(self):
        """build_view_data should NOT include 1=1 or X=X in filters."""
        from tools.p50_present.community_matrix import build_view_data
        view = {
            "view_name": "VW_GUARD",
            "scopes": [{
                "id": "main",
                "filters": [
                    {"english": "1 = 1"},
                    {"english": "Coverage Type C = 2"},
                    {"english": "Patient Identifier = Patient Identifier"},
                ],
                "columns": [],
            }],
        }
        result = build_view_data([view], {"VW_GUARD": set()})
        filters = result["VW_GUARD"]["filters"]
        self.assertIn("Coverage Type C = 2", filters)
        self.assertNotIn("1 = 1", filters)
        self.assertNotIn("Patient Identifier = Patient Identifier", filters)

    def test_tables_drop_cte_fragments(self):
        """Tables containing SQL operators / keywords get dropped."""
        from tools.p50_present.community_matrix import build_view_data
        view = {"view_name": "VW_T", "scopes": []}
        view_to_tables = {"VW_T": {
            "PAT_ENC",
            "table::DAY_OF_MONTH = 1) AS DD",
            "table::PATIENT",
            "table::CROSS APPLY DateDim",
        }}
        result = build_view_data([view], view_to_tables)
        tables = result["VW_T"]["tables"]
        # Real tables stay, including the bare 'PAT_ENC' (no prefix)
        # and the schema-stripped 'PATIENT'.
        self.assertIn("PAT_ENC", tables)
        self.assertIn("PATIENT", tables)
        # CTE-fragment "tables" got filtered out.
        for t in tables:
            self.assertNotIn("=", t)
            self.assertNotIn("(", t)


class TestAlignedRender(unittest.TestCase):
    """Column-aligned pipe-table rendering for raw-md readability."""

    def test_columns_padded_to_max_width(self):
        from tools.p50_present.community_matrix import _render_aligned_pipe_table
        header = ["table", "grain", "R1", "coverage"]
        rows = [
            ["**LONG_TABLE_NAME**", "cohort", "✓", "1/1  ●"],
            ["short", "?",          " ", "0/1"],
        ]
        out = _render_aligned_pipe_table(header, rows)
        # Every output line should be the same number of pipe chars.
        pipe_counts = [line.count("|") for line in out]
        self.assertEqual(len(set(pipe_counts)), 1,
                         f"pipe counts not uniform: {pipe_counts}")
        # Body rows should be the same length (column alignment).
        body_lengths = [len(out[2]), len(out[3])]
        self.assertEqual(body_lengths[0], body_lengths[1])


if __name__ == "__main__":
    unittest.main()
