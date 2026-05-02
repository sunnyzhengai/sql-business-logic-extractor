#!/usr/bin/env python3
"""Folder/batch mode for Tool 2 -- Technical Logic Extractor.

Walks a folder of *.sql view files, runs `extract_technical_lineage` on
each, and emits a single CSV with one row per non-trivial output column.
This is the governance-review companion to Tool 1's manifest.

A column is *truly trivial* (skipped) when the resolver attaches no
filters to it AND the column type is a simple passthrough. Everything
else is kept, including passthrough columns whose filters carry business
logic (WHERE STATUS = 'Active' makes a passthrough column non-trivial).

CSV columns:
    view_name, column_name, column_type,
    resolved_expression, base_tables, base_columns, filters

Notebook usage:
    from tools.technical_logic_extractor.batch import build_transformations
    build_transformations(input_dir='/lakehouse/default/Files/views',
                           output_csv='/lakehouse/default/Files/transformations.csv',
                           dialect='tsql')

CLI usage:
    python -m tools.technical_logic_extractor.batch <input_dir> [-o transformations.csv] [-d tsql]
"""

import argparse
import csv
import sys
from pathlib import Path

from sql_logic_extractor.products import extract_technical_lineage
from sql_logic_extractor.business_logic import build_alias_map, clean_filter_sql


def _read_sql_file(path: Path) -> str:
    """Read SQL handling SSMS's default UTF-16 LE BOM and other encodings."""
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


def _filter_text(f) -> str:
    if isinstance(f, dict):
        return (f.get("expression") or "").strip()
    return str(f or "").strip()


def rows_from_lineage(view_path: Path, view_name: str, lineage,
                        alias_map: dict, dialect: str = "tsql") -> list[dict]:
    """Shape a TechnicalLineage into transformation rows. Used by
    _process_view AND tools/batch_all.py.

    Skips truly-trivial passthroughs (no filters, no transformation).
    Cleans filter expressions in two ways: strips JOIN correlation keys
    (t1.K = t2.K) and resolves aliases to real table names using
    alias_map. A column whose filters were ALL correlation keys lands
    in the skipped pile -- correctly, since it's then truly trivial.
    """
    rows: list[dict] = []
    for col in lineage.resolved_columns:
        col_type = col.get("type", "unknown")
        col_filters = col.get("filters", []) or []

        cleaned = []
        seen = set()
        for f in col_filters:
            text = _filter_text(f)
            if not text:
                continue
            cleaned_text = clean_filter_sql(text, alias_map, dialect=dialect)
            if cleaned_text and cleaned_text not in seen:
                seen.add(cleaned_text)
                cleaned.append(cleaned_text)

        if col_type == "passthrough" and not cleaned:
            continue

        rows.append({
            "view_name": view_name,
            "column_name": col.get("name", ""),
            "column_type": col_type,
            "resolved_expression": col.get("resolved_expression", ""),
            "base_tables": ", ".join(col.get("base_tables", []) or []),
            "base_columns": ", ".join(col.get("base_columns", []) or []),
            "filters": "; ".join(cleaned),
        })
    return rows


def _process_view(view_path: Path, dialect: str = "tsql") -> list[dict]:
    """Run Tool 2 on one file, shape into transformation rows. Skip
    truly-trivial passthrough columns (no filters, no transformation)."""
    sql = _read_sql_file(view_path)
    if not sql.strip():
        return [_error_row(view_path, "EMPTY: file is empty after decoding")]

    try:
        lineage = extract_technical_lineage(sql, dialect=dialect)
    except Exception as e:
        return [_error_row(view_path, f"PARSE ERROR: {e}")]

    alias_map = build_alias_map(sql, dialect=dialect)
    return rows_from_lineage(view_path, view_path.stem, lineage, alias_map, dialect)


def _error_row(view_path: Path, msg: str) -> dict:
    return {
        "view_name": view_path.stem,
        "column_name": "", "column_type": "parse_error",
        "resolved_expression": msg, "base_tables": "",
        "base_columns": "", "filters": "",
    }


def build_transformations(input_dir: str, output_csv: str = "technical_logic_extractor.csv",
                           dialect: str = "tsql") -> int:
    in_dir = Path(input_dir)
    if not in_dir.is_dir():
        print(f"Error: {in_dir} is not a directory")
        return 1
    sql_files = sorted(in_dir.glob("*.sql"))
    if not sql_files:
        print(f"Error: no .sql files in {in_dir}")
        return 1

    fieldnames = ["view_name", "column_name", "column_type",
                  "resolved_expression", "base_tables", "base_columns", "filters"]

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

    err = sum(1 for r in all_rows if r["column_type"] == "parse_error")
    print(f"\nWrote {len(all_rows)} non-trivial column rows from {len(sql_files)} view(s) -> {out}")
    if err:
        print(f"  ({err} view(s) failed to parse -- see 'parse_error' rows)")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build a transformations CSV from a folder of SQL views."
    )
    parser.add_argument("input_dir", help="Folder containing view *.sql files")
    parser.add_argument("-o", "--output", default="technical_logic_extractor.csv")
    parser.add_argument("-d", "--dialect", default="tsql")
    args = parser.parse_args()
    return build_transformations(args.input_dir, args.output, args.dialect)


def _is_notebook() -> bool:
    return "ipykernel" in sys.argv[0] or "ipykernel" in " ".join(sys.argv[1:])


if __name__ == "__main__":
    if _is_notebook():
        print("Notebook environment detected -- call build_transformations("
              "input_dir=..., output_csv=..., dialect='tsql') from a cell.")
    else:
        sys.exit(main())
