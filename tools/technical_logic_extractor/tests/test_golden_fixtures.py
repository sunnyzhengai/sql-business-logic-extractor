"""Golden-file regression tests for Tool 2.

For each subdirectory under fixtures/ that contains an `input.sql` file:
    - Run Tool 2's batch processor on it
    - Compare the produced rows to the checked-in `expected.csv`
    - Fail if they differ

Re-baseline by setting `UPDATE_GOLDEN=1` in the environment, e.g.:
    UPDATE_GOLDEN=1 python3 -m pytest tools/technical_logic_extractor/tests/test_golden_fixtures.py

This catches regressions in Tool 2's output (column types, resolved
expressions, base columns/tables, filter cleanup) on real production
SQL views. New fixtures are added by dropping a folder under fixtures/
with an input.sql; running with UPDATE_GOLDEN=1 generates the expected.csv
that you then commit.
"""

from __future__ import annotations

import csv
import os
from pathlib import Path

import pytest

from tools.technical_logic_extractor.batch import _process_view


FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"
UPDATE_GOLDEN = os.environ.get("UPDATE_GOLDEN") == "1"

FIELDNAMES = [
    "view_name", "column_name", "column_type",
    "resolved_expression", "base_tables", "base_columns", "filters",
]


def _discover_fixtures() -> list[Path]:
    """Each subdir of fixtures/ that has an input.sql is one test case."""
    if not FIXTURES_DIR.is_dir():
        return []
    out = []
    for child in sorted(FIXTURES_DIR.iterdir()):
        if child.is_dir() and (child / "input.sql").is_file():
            out.append(child)
    return out


def _normalize(rows: list[dict]) -> list[dict]:
    """Sort rows for stable comparison; the dict iteration order in the
    underlying engine isn't strictly guaranteed."""
    keep = lambda r: {k: r.get(k, "") for k in FIELDNAMES}
    return sorted([keep(r) for r in rows], key=lambda r: (r["view_name"], r["column_name"]))


def _run_tool2(input_sql: Path) -> list[dict]:
    rows = _process_view(input_sql, dialect="tsql")
    # _process_view returns a list of dicts already shaped for the CSV.
    return _normalize(rows)


def _load_expected(csv_path: Path) -> list[dict]:
    with csv_path.open(encoding="utf-8-sig", newline="") as f:
        return _normalize(list(csv.DictReader(f)))


def _write_expected(csv_path: Path, rows: list[dict]) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


@pytest.mark.parametrize("fixture_dir", _discover_fixtures(),
                         ids=lambda p: p.name)
def test_golden_fixture(fixture_dir: Path) -> None:
    input_sql = fixture_dir / "input.sql"
    expected_csv = fixture_dir / "expected.csv"

    actual = _run_tool2(input_sql)

    if UPDATE_GOLDEN or not expected_csv.exists():
        _write_expected(expected_csv, actual)
        if not UPDATE_GOLDEN:
            pytest.fail(
                f"Golden file was missing -- created {expected_csv.name}. "
                f"Re-run tests to verify. (Set UPDATE_GOLDEN=1 to regenerate "
                f"existing goldens.)"
            )
        return

    expected = _load_expected(expected_csv)

    if actual != expected:
        # Build a focused diff message for failures.
        lines = [f"Fixture {fixture_dir.name} output diverged from golden:"]
        actual_keys = {(r["view_name"], r["column_name"]) for r in actual}
        expected_keys = {(r["view_name"], r["column_name"]) for r in expected}
        missing = expected_keys - actual_keys
        extra = actual_keys - expected_keys
        if missing:
            lines.append(f"  Rows MISSING from actual: {sorted(missing)}")
        if extra:
            lines.append(f"  Rows EXTRA in actual:     {sorted(extra)}")
        # Show first row whose content differs
        actual_idx = {(r["view_name"], r["column_name"]): r for r in actual}
        expected_idx = {(r["view_name"], r["column_name"]): r for r in expected}
        for k in sorted(actual_keys & expected_keys):
            if actual_idx[k] != expected_idx[k]:
                lines.append(f"  First diff on row {k}:")
                for field in FIELDNAMES:
                    a = actual_idx[k].get(field, "")
                    e = expected_idx[k].get(field, "")
                    if a != e:
                        lines.append(f"    {field}:")
                        lines.append(f"      expected: {e[:120]}")
                        lines.append(f"      actual:   {a[:120]}")
                break
        lines.append(
            "\nIf this change is intentional, regenerate goldens with:\n"
            "  UPDATE_GOLDEN=1 python3 -m pytest "
            "tools/technical_logic_extractor/tests/test_golden_fixtures.py"
        )
        pytest.fail("\n".join(lines))
