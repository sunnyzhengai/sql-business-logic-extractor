"""Unit tests for tools.operate.validate_graph_pivot.

Run from the repo root:
    python -m unittest tools.operate.tests.test_validate_graph_pivot
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path


# ----------------------------------------------------------------------------
# Fixture corpora -- two synthetic views with overlapping tables.
# ----------------------------------------------------------------------------
#
# View A: clinic-side diagnosis path
#   main: FROM PATIENT JOIN PAT_ENC JOIN PAT_ENC_DX JOIN ZC_DX_TYPE
# View B: inpatient-side diagnosis path
#   main: FROM PATIENT JOIN PAT_ENC_HSP JOIN HSP_DX JOIN ZC_DX_TYPE
#
# Expected: PATIENT and ZC_DX_TYPE are shared "bridge" tables; the clinic and
# inpatient paths form two table neighborhoods. Even with only two views we
# should get a clean graph with the right schema.

SAMPLE_VIEW_CLINIC = {
    "view_name": "VW_CLINIC_DX",
    "scopes": [
        {
            "id": "main",
            "kind": "main",
            "reads_from_tables": ["EPIC.PATIENT", "PAT_ENC", "PAT_ENC_DX", "ZC_DX_TYPE"],
            "joins": [
                {"right_table": "PAT_ENC", "join_type": "INNER JOIN", "on_expression": ""},
                {"right_table": "PAT_ENC_DX", "join_type": "INNER JOIN", "on_expression": ""},
                {"right_table": "ZC_DX_TYPE", "join_type": "LEFT JOIN", "on_expression": ""},
            ],
            "reads_from_scopes": [],
            "columns": [
                {
                    "column_name": "PAT_ID",
                    "column_type": "key",
                    "base_columns": ["table:PATIENT.PAT_ID"],
                    "base_tables": ["PATIENT"],
                },
                {
                    "column_name": "DX_NAME",
                    "column_type": "label",
                    "base_columns": ["table:ZC_DX_TYPE.NAME"],
                    "base_tables": ["ZC_DX_TYPE"],
                },
            ],
            "filters": [],
        },
    ],
}

SAMPLE_VIEW_INPATIENT = {
    "view_name": "VW_INPATIENT_DX",
    "scopes": [
        {
            "id": "main",
            "kind": "main",
            "reads_from_tables": ["EPIC.PATIENT", "PAT_ENC_HSP", "HSP_DX", "ZC_DX_TYPE"],
            "joins": [
                {"right_table": "PAT_ENC_HSP", "join_type": "INNER JOIN", "on_expression": ""},
                {"right_table": "HSP_DX", "join_type": "INNER JOIN", "on_expression": ""},
                {"right_table": "ZC_DX_TYPE", "join_type": "LEFT JOIN", "on_expression": ""},
            ],
            "reads_from_scopes": [],
            "columns": [
                {
                    "column_name": "PAT_ID",
                    "column_type": "key",
                    "base_columns": ["table:PATIENT.PAT_ID"],
                    "base_tables": ["PATIENT"],
                },
                {
                    "column_name": "DX_NAME",
                    "column_type": "label",
                    "base_columns": ["table:ZC_DX_TYPE.NAME"],
                    "base_tables": ["ZC_DX_TYPE"],
                },
            ],
            "filters": [],
        },
    ],
}


# NOTE: corpus loading is tested in tools.shared.tests.test_corpus_io
# (moved there in Phase 2a). The `load_corpus` function lives in
# tools.shared.corpus_io and is imported here for the end-to-end test
# below.


# NOTE: graph-construction tests live in
# tools.p20_index.tests.test_graph_builder (moved there in Phase 2b
# when build_graph was promoted from this file to p20_index).


# NOTE: analysis tests (projection, bridges, communities,
# primary-community) live in tools.p30_analyze.tests.test_p30_analyze
# (moved there in Phase 2c). The end-to-end orchestration test below
# exercises them all together via run_validation.


class TestEndToEndOrchestration(unittest.TestCase):
    """Verify the full run_validation pipeline produces all artifacts."""

    def test_run_validation_writes_all_artifacts(self):
        from tools.operate.validate_graph_pivot import run_validation
        with tempfile.TemporaryDirectory() as d:
            corpus_path = Path(d) / "corpus.jsonl"
            output_dir = Path(d) / "out"
            with corpus_path.open("w", encoding="utf-8") as f:
                f.write(json.dumps({"schema_version": 3, "n_views": 2}) + "\n")
                f.write(json.dumps(SAMPLE_VIEW_CLINIC) + "\n")
                f.write(json.dumps(SAMPLE_VIEW_INPATIENT) + "\n")
            result = run_validation(corpus_path, output_dir)
            # All file-existence assertions must happen INSIDE the with block;
            # otherwise the TemporaryDirectory context cleans up the files
            # before we can check them.
            self.assertTrue(Path(result["graph_html"]).is_file())
            self.assertTrue(Path(result["communities_index_html"]).is_file())
            self.assertTrue(Path(result["communities_md"]).is_file())
            self.assertTrue(Path(result["validation_report"]).is_file())
            # The communities/ dir should contain at least one per-community HTML.
            community_htmls = list((output_dir / "communities").glob("community_*.html"))
            self.assertGreater(len(community_htmls), 0)
            # Phase 5: per-community shape HTMLs (side-by-side join graphs).
            shape_htmls = list((output_dir / "community_shapes").glob("community_*_shapes.html"))
            self.assertGreater(len(shape_htmls), 0)
            self.assertEqual(len(result["community_shapes"]), len(shape_htmls))
            # Sanity-check the report content.
            report = Path(result["validation_report"]).read_text(encoding="utf-8")
            self.assertIn("Verdict", report)
            self.assertIn("Bridge tables", report)
            self.assertIn("Cross-domain views", report)

    def test_run_validation_excludes_infrastructure_views(self):
        """End-to-end: a corpus with one business view and one Collibra view
        should report 1 business view + 1 excluded."""
        from tools.operate.validate_graph_pivot import run_validation
        with tempfile.TemporaryDirectory() as d:
            corpus_path = Path(d) / "corpus.jsonl"
            output_dir = Path(d) / "out"
            collibra_view = {
                "view_name": "VW_COLLIBRA_TABLE_INVENTORY",
                "scopes": [{
                    "id": "main", "kind": "main",
                    "reads_from_tables": ["sys.tables"],
                    "joins": [], "reads_from_scopes": [],
                    "columns": [], "filters": [],
                }],
            }
            with corpus_path.open("w", encoding="utf-8") as f:
                f.write(json.dumps({"schema_version": 3, "n_views": 2}) + "\n")
                f.write(json.dumps(SAMPLE_VIEW_CLINIC) + "\n")
                f.write(json.dumps(collibra_view) + "\n")
            result = run_validation(corpus_path, output_dir)
            self.assertEqual(result["n_views_total"], 2)
            self.assertEqual(result["n_views_business"], 1)
            self.assertEqual(result["n_views_excluded"], 1)


if __name__ == "__main__":
    unittest.main()
