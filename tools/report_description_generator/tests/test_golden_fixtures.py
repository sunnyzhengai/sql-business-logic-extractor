"""Golden-file regression tests for Tool 4 -- Report Description Generator.

For each fixture under fixtures/, runs Tool 4's batch processor in
ENGINEERED mode (use_llm=False) with an empty schema. Asserts output
matches the checked-in expected.csv.

Tool 4 produces ONE row per view (the report-level summary), unlike
Tool 1/2/3 which produce one row per column. The fixture pattern is
the same; the underlying _process_view returns a single dict per view.

LLM mode intentionally not tested here -- LLM summaries are non-
deterministic. License gate is covered by test_generate_report_description.py.

Re-baseline:
    UPDATE_GOLDEN=1 python3 -m pytest tools/report_description_generator/tests/test_golden_fixtures.py
"""

from __future__ import annotations

import csv
import os
from pathlib import Path

import pytest

from tools.report_description_generator.batch import _process_view


FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"
UPDATE_GOLDEN = os.environ.get("UPDATE_GOLDEN") == "1"

FIELDNAMES = [
    "view_name", "query_summary", "primary_purpose",
    "key_metrics", "source_tables", "column_count", "use_llm",
]


def _discover_fixtures() -> list[Path]:
    if not FIXTURES_DIR.is_dir():
        return []
    return sorted(c for c in FIXTURES_DIR.iterdir()
                    if c.is_dir() and (c / "input.sql").is_file())


def _normalize(rows: list[dict]) -> list[dict]:
    keep = lambda r: {k: str(r.get(k, "")) for k in FIELDNAMES}
    return sorted([keep(r) for r in rows], key=lambda r: r["view_name"])


def _run_tool(input_sql: Path) -> list[dict]:
    # _process_view for Tool 4 returns ONE dict (per view), not a list.
    one = _process_view(input_sql, schema={}, use_llm=False,
                          llm_client=None, dialect="tsql")
    return _normalize([one])


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
    a = actual[0] if actual else {}
    e = expected[0] if expected else {}
    for field in FIELDNAMES:
        if a.get(field) != e.get(field):
            lines.append(f"  {field}:")
            lines.append(f"    expected: {str(e.get(field, ''))[:200]}")
            lines.append(f"    actual:   {str(a.get(field, ''))[:200]}")
    lines.append("\nRe-baseline with: UPDATE_GOLDEN=1 python3 -m pytest "
                  "tools/report_description_generator/tests/test_golden_fixtures.py")
    pytest.fail("\n".join(lines))
