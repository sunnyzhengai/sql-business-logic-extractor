"""Tests for tools.shared.table_names -- the small string-handling helpers
that strip qualifiers, detect ZC lookups, and recognize scope refs.

Run from the repo root:
    python -m unittest tools.shared.tests.test_table_names
"""

from __future__ import annotations

import unittest

from tools.shared.table_names import (
    bare_table_name,
    is_cte_or_scope_reference,
    is_zc_table,
)


class TestBareTableName(unittest.TestCase):
    """`bare_table_name` strips database / schema qualifiers."""

    def test_strips_three_part_qualifier(self):
        self.assertEqual(bare_table_name("Clarity.dbo.PATIENT"), "PATIENT")

    def test_strips_two_part_qualifier(self):
        self.assertEqual(bare_table_name("EPIC.PATIENT"), "PATIENT")

    def test_bare_name_unchanged(self):
        self.assertEqual(bare_table_name("PATIENT"), "PATIENT")

    def test_empty_string_returns_empty(self):
        self.assertEqual(bare_table_name(""), "")

    def test_strips_whitespace(self):
        self.assertEqual(bare_table_name("  PATIENT  "), "PATIENT")

    def test_scope_ref_passed_through_unchanged(self):
        """Names with colons are scope refs; bare_table_name doesn't try
        to strip them -- callers use is_cte_or_scope_reference to detect."""
        self.assertEqual(bare_table_name("cte:foo"), "cte:foo")


class TestIsZcTable(unittest.TestCase):
    """`is_zc_table` flags Epic code-lookup tables."""

    def test_zc_prefix_detected(self):
        self.assertTrue(is_zc_table("ZC_STATUS"))

    def test_zc_prefix_case_insensitive(self):
        self.assertTrue(is_zc_table("zc_status"))

    def test_non_zc_table_not_flagged(self):
        self.assertFalse(is_zc_table("PATIENT"))

    def test_zc_in_middle_not_flagged(self):
        """Only prefix counts; ZC elsewhere isn't a lookup."""
        self.assertFalse(is_zc_table("MY_ZC_TABLE"))


class TestIsCteOrScopeReference(unittest.TestCase):
    """`is_cte_or_scope_reference` flags non-table scope identifiers."""

    def test_cte_prefix_detected(self):
        self.assertTrue(is_cte_or_scope_reference("cte:my_cte"))

    def test_derived_prefix_detected(self):
        self.assertTrue(is_cte_or_scope_reference("derived:subquery_3"))

    def test_real_table_not_flagged(self):
        self.assertFalse(is_cte_or_scope_reference("PATIENT"))

    def test_empty_string_not_flagged(self):
        self.assertFalse(is_cte_or_scope_reference(""))


if __name__ == "__main__":
    unittest.main()
