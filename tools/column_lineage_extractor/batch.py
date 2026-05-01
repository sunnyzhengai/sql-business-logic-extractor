#!/usr/bin/env python3
"""Folder/batch mode for Tool 1 -- Column Lineage Extractor.

Walks a folder of *.sql view files, runs `extract_columns` on each, and
emits a single CSV the ETL team can consume. This is the productized
replacement for the legacy `build_manifest_standalone.py` script that
lived in view-migration/scripts/.

CSV columns:
    view_file, view_name,
    referenced_database, referenced_schema, referenced_table, referenced_column,
    reference_type, confidence

Notebook usage:
    from tools.column_lineage_extractor.batch import build_manifest
    build_manifest(input_dir='/lakehouse/default/Files/views',
                   output_csv='/lakehouse/default/Files/manifest.csv',
                   dialect='tsql')

CLI usage:
    python -m tools.column_lineage_extractor.batch <input_dir> [-o manifest.csv] [-d tsql]
"""

import argparse
import csv
import sys
from pathlib import Path
from typing import Optional

from sql_logic_extractor.products import extract_columns
from sqlglot import exp, parse_one

from sql_logic_extractor.resolve import preprocess_ssms


def _read_sql_file(path: Path) -> str:
    """Read a SQL file, handling SSMS's default UTF-16 LE BOM and other
    common encodings. SSMS scripts views as UTF-16 LE by default."""
    raw = path.read_bytes()
    if raw.startswith(b"\xff\xfe"):
        return raw.decode("utf-16-le")[1:]
    if raw.startswith(b"\xfe\xff"):
        return raw.decode("utf-16-be")[1:]
    if raw.startswith(b"\xef\xbb\xbf"):
        return raw.decode("utf-8")[1:]
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("utf-16-le", errors="replace")


def _table_level_rows(sql: str, dialect: str, view_file: str, view_name: str,
                      seen: set) -> list[dict]:
    """One row per Table node referenced (catches SELECT *, EXISTS, etc.)
    that the column-level extractor doesn't surface as a specific column."""
    clean_sql, _ = preprocess_ssms(sql)
    if not clean_sql.strip():
        return []
    try:
        parsed = parse_one(clean_sql, dialect=dialect)
    except Exception:
        return []

    cte_names = {(c.alias_or_name or "").lower() for c in parsed.find_all(exp.CTE)}
    self_name = None
    if isinstance(parsed, exp.Create) and isinstance(parsed.this, exp.Table):
        self_name = parsed.this.name.lower() if parsed.this.name else None

    rows: list[dict] = []
    for t in parsed.find_all(exp.Table):
        nm = t.name.lower()
        if nm in cte_names or nm == self_name:
            continue
        db = t.args["catalog"].name if t.args.get("catalog") else ""
        schema = t.args["db"].name if t.args.get("db") else ""
        key = (view_file, db, schema, t.name, "*")
        if key in seen:
            continue
        seen.add(key)
        rows.append({
            "view_file": view_file,
            "view_name": view_name,
            "referenced_database": db,
            "referenced_schema": schema,
            "referenced_table": t.name,
            "referenced_column": "*",
            "reference_type": "table",
            "confidence": "high" if (db or schema) else "medium",
        })
    return rows


def rows_from_inventory(view_path: Path, view_name: str, inventory) -> tuple[list[dict], set]:
    """Shape a ColumnInventory into manifest rows. Used by _process_view
    AND by tools/batch_all.py (which feeds a pre-computed inventory to
    avoid re-running the engine). Returns (rows, seen_set) so the caller
    can extend with table-level rows without re-deduping."""
    rows: list[dict] = []
    seen: set[tuple] = set()
    for c in inventory.columns:
        key = (view_path.name, c.database or "", c.schema or "", c.table, c.column)
        if key in seen:
            continue
        seen.add(key)
        rows.append({
            "view_file": view_path.name,
            "view_name": view_name,
            "referenced_database": c.database or "",
            "referenced_schema": c.schema or "",
            "referenced_table": c.table,
            "referenced_column": c.column,
            "reference_type": "column",
            "confidence": "high" if (c.database or c.schema) else "medium",
        })
    return rows, seen


def _process_view(view_path: Path, dialect: str = "tsql") -> list[dict]:
    """Run Tool 1 on one file, shape the output for the manifest CSV."""
    sql = _read_sql_file(view_path)
    if not sql.strip():
        return [_error_row(view_path, "EMPTY: file is empty after decoding")]

    try:
        inv = extract_columns(sql, dialect=dialect)
    except Exception as e:
        return [_error_row(view_path, f"PARSE ERROR: {e}")]

    view_name = view_path.stem
    rows, seen = rows_from_inventory(view_path, view_name, inv)
    rows.extend(_table_level_rows(sql, dialect, view_path.name, view_name, seen))
    return rows


def _error_row(view_path: Path, msg: str) -> dict:
    return {
        "view_file": view_path.name, "view_name": view_path.stem,
        "referenced_database": "", "referenced_schema": "",
        "referenced_table": "", "referenced_column": msg,
        "reference_type": "parse_error", "confidence": "low",
    }


def build_manifest(input_dir: str, output_csv: str = "column_lineage_extractor.csv",
                   dialect: str = "tsql") -> int:
    in_dir = Path(input_dir)
    if not in_dir.is_dir():
        print(f"Error: {in_dir} is not a directory")
        return 1
    sql_files = sorted(in_dir.glob("*.sql"))
    if not sql_files:
        print(f"Error: no .sql files in {in_dir}")
        return 1

    fieldnames = ["view_file", "view_name", "referenced_database",
                  "referenced_schema", "referenced_table", "referenced_column",
                  "reference_type", "confidence"]

    all_rows: list[dict] = []
    for path in sql_files:
        print(f"Parsing: {path.name}")
        all_rows.extend(_process_view(path, dialect=dialect))

    out = Path(output_csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    err = sum(1 for r in all_rows if r["reference_type"] == "parse_error")
    print(f"\nWrote {len(all_rows)} rows from {len(sql_files)} view(s) -> {out}")
    if err:
        print(f"  ({err} view(s) failed to parse -- see 'parse_error' rows)")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build a column-inventory manifest CSV from a folder of SQL views."
    )
    parser.add_argument("input_dir", help="Folder containing view *.sql files")
    parser.add_argument("-o", "--output", default="column_lineage_extractor.csv")
    parser.add_argument("-d", "--dialect", default="tsql")
    args = parser.parse_args()
    return build_manifest(args.input_dir, args.output, args.dialect)


def _is_notebook() -> bool:
    return "ipykernel" in sys.argv[0] or "ipykernel" in " ".join(sys.argv[1:])


if __name__ == "__main__":
    if _is_notebook():
        print("Notebook environment detected -- call build_manifest("
              "input_dir=..., output_csv=..., dialect='tsql') from a cell.")
    else:
        sys.exit(main())
