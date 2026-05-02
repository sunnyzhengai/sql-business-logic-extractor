"""Golden-file regression tests for Tool 1 -- Column Lineage Extractor.

For each subdirectory under fixtures/ that contains an `input.sql` file,
runs Tool 1's batch processor and asserts the output matches the
checked-in `expected.csv`.

Re-baseline:
    UPDATE_GOLDEN=1 python3 -m pytest tools/column_lineage_extractor/tests/test_golden_fixtures.py
"""

from __future__ import annotations

import csv
import os
from pathlib import Path

import pytest

from tools.column_lineage_extractor.batch import _process_view


FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"
UPDATE_GOLDEN = os.environ.get("UPDATE_GOLDEN") == "1"

FIELDNAMES = [
    "view_name",
    "referenced_database", "referenced_schema", "referenced_table",
    "referenced_column", "reference_type", "confidence",
]


def _discover_fixtures() -> list[Path]:
    if not FIXTURES_DIR.is_dir():
        return []
    return sorted(c for c in FIXTURES_DIR.iterdir()
                    if c.is_dir() and (c / "input.sql").is_file())


def _normalize(rows: list[dict]) -> list[dict]:
    keep = lambda r: {k: r.get(k, "") for k in FIELDNAMES}
    return sorted(
        [keep(r) for r in rows],
        key=lambda r: (r["view_name"], r["referenced_database"], r["referenced_schema"],
                        r["referenced_table"], r["referenced_column"]),
    )


def _run_tool(input_sql: Path) -> list[dict]:
    rows = _process_view(input_sql, dialect="tsql")
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
    actual = _run_tool(input_sql)

    if UPDATE_GOLDEN or not expected_csv.exists():
        _write_expected(expected_csv, actual)
        if not UPDATE_GOLDEN:
            pytest.fail(
                f"Golden file was missing -- created {expected_csv.name}. "
                "Re-run tests to verify."
            )
        return

    expected = _load_expected(expected_csv)
    if actual == expected:
        return

    lines = [f"Fixture {fixture_dir.name} output diverged from golden:"]
    actual_keys = {(r["view_name"], r["referenced_table"], r["referenced_column"]) for r in actual}
    expected_keys = {(r["view_name"], r["referenced_table"], r["referenced_column"]) for r in expected}
    if expected_keys - actual_keys:
        lines.append(f"  MISSING from actual: {sorted(expected_keys - actual_keys)}")
    if actual_keys - expected_keys:
        lines.append(f"  EXTRA in actual:     {sorted(actual_keys - expected_keys)}")
    a_idx = {(r["view_name"], r["referenced_table"], r["referenced_column"]): r for r in actual}
    e_idx = {(r["view_name"], r["referenced_table"], r["referenced_column"]): r for r in expected}
    for k in sorted(actual_keys & expected_keys):
        if a_idx[k] != e_idx[k]:
            lines.append(f"  First diff on row {k}:")
            for field in FIELDNAMES:
                if a_idx[k].get(field) != e_idx[k].get(field):
                    lines.append(f"    {field}:")
                    lines.append(f"      expected: {e_idx[k].get(field, '')[:120]}")
                    lines.append(f"      actual:   {a_idx[k].get(field, '')[:120]}")
            break
    lines.append("\nRe-baseline with: UPDATE_GOLDEN=1 python3 -m pytest "
                  "tools/column_lineage_extractor/tests/test_golden_fixtures.py")
    pytest.fail("\n".join(lines))
