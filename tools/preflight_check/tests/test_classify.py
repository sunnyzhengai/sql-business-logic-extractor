"""Tests for tools/preflight_check.

Asserts the four classification buckets behave correctly:
- clean SELECT -> 'clean', empty rules_fired
- view requiring a registry rule -> 'needs_rule', rule id reported
- structurally bad SQL -> 'unknown_failure' with redacted error
- the redaction strips literals (so the CSV is shareable)
"""

import re
import tempfile
from pathlib import Path

from tools.preflight_check.batch import classify_view


def _write(content: str) -> Path:
    f = tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False, encoding="utf-8")
    f.write(content)
    f.close()
    return Path(f.name)


def test_clean_select_classifies_clean():
    p = _write("SELECT P.PAT_ID FROM Clarity.dbo.PATIENT P WHERE P.STATUS_C = 1")
    row = classify_view(p)
    assert row["status"] == "clean"
    assert row["rules_fired"] == ""
    assert row["error_message"] == ""


def test_create_view_explicit_cols_classifies_needs_rule():
    p = _write(
        "CREATE VIEW dbo.foo (\n"
        "    A,\n"
        "    B\n"
        ")\n"
        "AS\n"
        "SELECT C.A, C.B FROM Clarity.dbo.SOURCE C"
    )
    row = classify_view(p)
    assert row["status"] == "needs_rule"
    assert "create_view_explicit_column_list" in row["rules_fired"]


def test_unknown_failure_reports_redacted_error():
    p = _write("SELECT NOT_A_VALID )))) STATEMENT 'secret value 12345'")
    row = classify_view(p)
    assert row["status"] == "unknown_failure"
    # Redaction: the secret value should NOT appear in the error_message
    assert "secret value" not in row["error_message"]
    assert "12345" not in row["error_message"]


def test_each_row_has_all_required_fields():
    """The CSV writer needs every key present on every row -- guards
    against KeyError when one row's classification path forgets a key."""
    required = {"view_name", "status", "rules_fired",
                  "error_line", "error_col", "error_message"}
    for sql in (
        "SELECT 1",                                       # clean
        "CREATE VIEW dbo.x (A) AS SELECT 1 AS A",         # needs_rule
        "SELECT )))",                                     # unknown_failure
    ):
        row = classify_view(_write(sql))
        assert set(row.keys()) >= required, \
            f"missing keys: {required - set(row.keys())}"
