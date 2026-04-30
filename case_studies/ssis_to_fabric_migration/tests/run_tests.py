#!/usr/bin/env python3
"""Run the archived standalone manifest builder against every view in
tests/views/ and print a per-view summary so regressions are visible.

Usage:
    python3 case_studies/ssis_to_fabric_migration/tests/run_tests.py

Note: as of the 4-tool restructure, the standalone manifest builder lives
at docs/archive/build_manifest_standalone.py -- it's the predecessor of
tools/column_lineage_extractor. These case-study tests pin its behavior
so the fixtures stay reproducible while we transition to the productized
tools.
"""

import csv
import io
import sys
from pathlib import Path

# Make the archived standalone module importable. Path: project_root/docs/archive/
HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent.parent.parent
ARCHIVE_DIR = PROJECT_ROOT / "docs" / "archive"
sys.path.insert(0, str(ARCHIVE_DIR))

from build_manifest_standalone import extract_view_refs  # noqa: E402


VIEWS_DIR = HERE / "views"
SSMS_TEST_STEM = "09_ssms_utf16_boilerplate"


def _generate_utf16_view() -> Path:
    """Re-encode the SSMS marker SQL as UTF-16 LE with a BOM in /tmp so the
    runner exercises the encoding handler against a real BOM'd file. Returns
    the path of the generated file."""
    src = VIEWS_DIR / f"{SSMS_TEST_STEM}.sql"
    text = src.read_text(encoding="utf-8")
    out = Path("/tmp") / f"{SSMS_TEST_STEM}.utf16.sql"
    out.write_bytes(b"\xff\xfe" + text.encode("utf-16-le"))
    return out


def _run_one(view_path: Path) -> dict:
    rows = extract_view_refs(view_path, dialect="tsql")
    parse_errors = [r for r in rows if r["reference_type"] == "parse_error"]
    columns = sorted({(r["referenced_database"], r["referenced_schema"],
                       r["referenced_table"], r["referenced_column"])
                      for r in rows if r["reference_type"] == "column"})
    tables = sorted({(r["referenced_database"], r["referenced_schema"],
                      r["referenced_table"])
                     for r in rows if r["reference_type"] == "table"})
    return {
        "name": view_path.stem,
        "parse_errors": parse_errors,
        "columns": columns,
        "tables": tables,
        "row_count": len(rows),
    }


def _format_row(parts: tuple) -> str:
    db, schema, table, *col = parts
    qual = ".".join(p for p in (db, schema, table) if p) or "(unknown)"
    if col:
        return f"{qual}.{col[0]}"
    return qual


def main() -> int:
    if not VIEWS_DIR.is_dir():
        print(f"Error: {VIEWS_DIR} not found", file=sys.stderr)
        return 1

    fixtures: list[Path] = sorted(VIEWS_DIR.glob("*.sql"))

    # Generate the UTF-16 LE binary version of test 09 at runtime.
    utf16_path = _generate_utf16_view()
    fixtures.append(utf16_path)

    fail_count = 0
    print(f"Running {len(fixtures)} fixtures from {VIEWS_DIR.relative_to(VIEWS_DIR.parent.parent)}")
    print()

    for path in fixtures:
        result = _run_one(path)
        status = "FAIL" if result["parse_errors"] else "PASS"
        if result["parse_errors"]:
            fail_count += 1
        print(f"[{status}] {result['name']}  —  {result['row_count']} rows "
              f"({len(result['columns'])} columns, {len(result['tables'])} tables)")
        if result["parse_errors"]:
            for err in result["parse_errors"]:
                print(f"        ERROR: {err['referenced_column']}")
            continue
        for c in result["columns"]:
            print(f"        col   {_format_row(c)}")
        for t in result["tables"]:
            print(f"        table {_format_row(t)}")
        print()

    print()
    print(f"{len(fixtures) - fail_count}/{len(fixtures)} passed, {fail_count} failed")
    return 0 if fail_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
