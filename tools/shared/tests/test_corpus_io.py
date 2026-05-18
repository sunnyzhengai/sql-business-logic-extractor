"""Tests for tools.shared.corpus_io.

Run from the repo root:
    python -m unittest tools.shared.tests.test_corpus_io
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.shared.corpus_io import load_corpus


_SAMPLE_VIEW_A = {
    "view_name": "VW_A",
    "scopes": [{"id": "main", "kind": "main", "reads_from_tables": [],
                "joins": [], "reads_from_scopes": [],
                "columns": [], "filters": []}],
}


class TestLoadCorpus(unittest.TestCase):

    def test_separates_header_and_views(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "corpus.jsonl"
            with path.open("w", encoding="utf-8") as f:
                f.write(json.dumps({"schema_version": 3, "n_views": 1}) + "\n")
                f.write(json.dumps(_SAMPLE_VIEW_A) + "\n")
            header, views = load_corpus(path)
        self.assertEqual(header.get("schema_version"), 3)
        self.assertEqual(header.get("n_views"), 1)
        self.assertEqual(len(views), 1)
        self.assertEqual(views[0]["view_name"], "VW_A")

    def test_empty_corpus_ok(self):
        """A jsonl with only a header line returns an empty views list."""
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "corpus.jsonl"
            with path.open("w", encoding="utf-8") as f:
                f.write(json.dumps({"schema_version": 3, "n_views": 0}) + "\n")
            header, views = load_corpus(path)
        self.assertEqual(header.get("n_views"), 0)
        self.assertEqual(views, [])

    def test_skips_blank_lines(self):
        """Blank lines in the jsonl shouldn't break the loader."""
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "corpus.jsonl"
            with path.open("w", encoding="utf-8") as f:
                f.write(json.dumps({"schema_version": 3, "n_views": 1}) + "\n")
                f.write("\n")        # blank line
                f.write(json.dumps(_SAMPLE_VIEW_A) + "\n")
                f.write("   \n")     # whitespace-only line
            header, views = load_corpus(path)
        self.assertEqual(len(views), 1)


if __name__ == "__main__":
    unittest.main()
