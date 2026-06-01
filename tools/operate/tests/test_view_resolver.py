"""Tests for tools.operate.view_resolver -- loading external view
SQL files into shape-ready ViewV1 dicts."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tools.operate.view_resolver import (
    load_external_views,
    parse_view_for_shape,
)


class TestParseViewForShape(unittest.TestCase):

    def test_simple_join_view_parses_with_expected_shape(self):
        with tempfile.TemporaryDirectory() as td:
            sql = Path(td) / "V_TEST.sql"
            sql.write_text(
                "CREATE VIEW V_TEST AS "
                "SELECT t.x FROM PAT_ENC t "
                "INNER JOIN PATIENT p ON t.PAT_ID = p.PAT_ID"
            )
            view = parse_view_for_shape(sql)
            self.assertIsNotNone(view)
            self.assertEqual(view["view_name"], "V_TEST")
            self.assertEqual(len(view["scopes"]), 1)
            main = view["scopes"][0]
            self.assertIn("PAT_ENC", main["reads_from_tables"])
            self.assertIn("PATIENT", main["reads_from_tables"])
            self.assertEqual(len(main["joins"]), 1)
            self.assertEqual(main["joins"][0]["right_table"], "PATIENT")

    def test_unparseable_returns_none(self):
        """Garbage SQL is tolerated -- the function returns None rather
        than raising, so a bad single file doesn't break a batch."""
        with tempfile.TemporaryDirectory() as td:
            sql = Path(td) / "broken.sql"
            sql.write_text("this is not SQL at all !@#$%")
            view = parse_view_for_shape(sql)
            self.assertIsNone(view)


class TestLoadExternalViews(unittest.TestCase):

    def test_loads_every_sql_file_under_overrides(self):
        with tempfile.TemporaryDirectory() as td:
            d1 = Path(td) / "views_a"
            d2 = Path(td) / "views_b"
            d1.mkdir()
            d2.mkdir()
            (d1 / "V_ONE.sql").write_text(
                "CREATE VIEW V_ONE AS SELECT 1 AS x FROM PAT_ENC"
            )
            (d2 / "V_TWO.sql").write_text(
                "CREATE VIEW V_TWO AS SELECT 2 AS y FROM PATIENT"
            )
            views = load_external_views(
                project_root=td,
                overrides=("views_a", "views_b"),
            )
            self.assertIn("V_ONE", views)
            self.assertIn("V_TWO", views)
            self.assertEqual(views["V_ONE"]["scopes"][0]["reads_from_tables"],
                              ["PAT_ENC"])

    def test_missing_folders_silently_skipped(self):
        """Tolerant to partial inputs -- the validate_graph_pivot
        pipeline shouldn't break for users without the data/views_*
        folders."""
        with tempfile.TemporaryDirectory() as td:
            # Neither overrides exists.
            views = load_external_views(
                project_root=td,
                overrides=("does_not_exist_a", "does_not_exist_b"),
            )
            self.assertEqual(views, {})

    def test_view_source_dirs_accepts_absolute_paths(self):
        """The view_source_dirs param lets the caller pass absolute
        paths directly, bypassing the cwd-relative resolution that
        breaks under Fabric notebooks where cwd may not be the repo
        root."""
        with tempfile.TemporaryDirectory() as td:
            # Two absolute paths in completely unrelated locations.
            d1 = Path(td) / "anywhere" / "viewsA"
            d2 = Path(td) / "elsewhere" / "viewsB"
            d1.mkdir(parents=True)
            d2.mkdir(parents=True)
            (d1 / "V_ALPHA.sql").write_text(
                "CREATE VIEW V_ALPHA AS SELECT 1 AS x FROM TBL_A"
            )
            (d2 / "V_BETA.sql").write_text(
                "CREATE VIEW V_BETA AS SELECT 2 AS y FROM TBL_B"
            )
            views = load_external_views(
                view_source_dirs=[str(d1), str(d2)],
            )
            self.assertIn("V_ALPHA", views)
            self.assertIn("V_BETA", views)

    def test_view_source_dirs_missing_dir_silently_skipped(self):
        """Passing an absolute path that doesn't exist is tolerated."""
        with tempfile.TemporaryDirectory() as td:
            d = Path(td) / "real"
            d.mkdir()
            (d / "V_REAL.sql").write_text(
                "CREATE VIEW V_REAL AS SELECT 1 FROM TBL"
            )
            views = load_external_views(view_source_dirs=[
                str(d),
                "/does/not/exist",
            ])
            self.assertIn("V_REAL", views)
            # No crash from the bogus path.

    def test_first_match_wins_on_duplicate_view_name(self):
        with tempfile.TemporaryDirectory() as td:
            d1 = Path(td) / "views_a"
            d2 = Path(td) / "views_b"
            d1.mkdir()
            d2.mkdir()
            (d1 / "V_DUP.sql").write_text(
                "CREATE VIEW V_DUP AS SELECT 1 AS x FROM PAT_ENC"
            )
            (d2 / "V_DUP.sql").write_text(
                "CREATE VIEW V_DUP AS SELECT 2 AS y FROM PATIENT"
            )
            views = load_external_views(
                project_root=td,
                overrides=("views_a", "views_b"),
            )
            # First-folder definition wins -- PAT_ENC, not PATIENT.
            self.assertEqual(
                views["V_DUP"]["scopes"][0]["reads_from_tables"],
                ["PAT_ENC"],
            )


if __name__ == "__main__":
    unittest.main()
