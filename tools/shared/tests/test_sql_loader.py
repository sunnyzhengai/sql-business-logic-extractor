"""Tests for tools.shared.sql_loader -- the canonical raw-SQL reader."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path


class TestReadSqlRobust(unittest.TestCase):
    """The encoding-detection read path."""

    def _write(self, content: bytes) -> str:
        """Write `content` bytes to a temp file, return its path."""
        f = tempfile.NamedTemporaryFile(suffix=".sql", delete=False)
        f.write(content)
        f.close()
        return f.name

    def test_plain_utf8_passes_through(self):
        from tools.shared.sql_loader import read_sql_robust
        path = self._write(b"SELECT 1 FROM tbl")
        self.assertEqual(read_sql_robust(path), "SELECT 1 FROM tbl")

    def test_utf16_le_bom_decoded(self):
        from tools.shared.sql_loader import read_sql_robust
        content = "SELECT 1 FROM tbl"
        bom = b"\xff\xfe" + content.encode("utf-16-le")
        path = self._write(bom)
        self.assertEqual(read_sql_robust(path), "SELECT 1 FROM tbl")

    def test_utf16_be_bom_decoded(self):
        from tools.shared.sql_loader import read_sql_robust
        content = "SELECT 1 FROM tbl"
        bom = b"\xfe\xff" + content.encode("utf-16-be")
        path = self._write(bom)
        self.assertEqual(read_sql_robust(path), "SELECT 1 FROM tbl")

    def test_utf8_bom_stripped(self):
        from tools.shared.sql_loader import read_sql_robust
        content = "SELECT 1 FROM tbl"
        bom = b"\xef\xbb\xbf" + content.encode("utf-8")
        path = self._write(bom)
        self.assertEqual(read_sql_robust(path), "SELECT 1 FROM tbl")


class TestLoadCleanSql(unittest.TestCase):
    """The read + preprocess one-call entry point."""

    def _write(self, content: str, encoding: str = "utf-8") -> str:
        f = tempfile.NamedTemporaryFile(suffix=".sql", delete=False)
        if encoding == "utf-16-le-bom":
            f.write(b"\xff\xfe" + content.encode("utf-16-le"))
        else:
            f.write(content.encode(encoding))
        f.close()
        return f.name

    def test_ssms_preamble_stripped_with_utf8(self):
        """A typical SSMS-shape view returns just the body SQL."""
        from tools.shared.sql_loader import load_clean_sql
        sql = (
            "USE [DB]\nGO\n"
            "/****** Object:  View [dbo].[v_test]    Script Date: 5/25/2026 ******/\n"
            "SET ANSI_NULLS ON\nGO\n"
            "CREATE VIEW [dbo].[v_test] AS\n"
            "SELECT a FROM tbl\n"
        )
        path = self._write(sql)
        clean, meta = load_clean_sql(path)
        self.assertNotIn("USE ", clean)
        self.assertNotIn(" GO ", " " + clean + " ")
        self.assertIn("SELECT a FROM tbl", clean)
        # Metadata extracted from the Object header.
        self.assertEqual(meta.get("schema"), "dbo")
        self.assertEqual(meta.get("name"), "v_test")

    def test_ssms_preamble_stripped_with_utf16(self):
        """The SAME view encoded as UTF-16 LE BOM gets decoded then
        preprocessed identically -- the single trap that bit Yang's
        team gets handled in one call."""
        from tools.shared.sql_loader import load_clean_sql
        sql = (
            "USE [DB]\nGO\n"
            "SET ANSI_NULLS ON\nGO\n"
            "CREATE VIEW [dbo].[v_test_u16] AS\n"
            "SELECT a FROM tbl\n"
        )
        path = self._write(sql, encoding="utf-16-le-bom")
        clean, _meta = load_clean_sql(path)
        self.assertIn("SELECT a FROM tbl", clean)


if __name__ == "__main__":
    unittest.main()
