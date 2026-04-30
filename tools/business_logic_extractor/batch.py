#!/usr/bin/env python3
"""Folder/batch mode for Tool 3 -- Business Logic Extractor.

Walks a folder of *.sql view files, runs `extract_business_logic` on each
view (engineered mode by default; LLM mode opt-in via --use-llm), and
emits a single CSV with one row per output column's English definition.

CSV columns:
    view_file, view_name, column_name, column_type,
    english_definition, business_domain,
    base_columns, base_tables, resolved_expression,
    english_definition_with_filters, use_llm

Notebook usage:
    from tools.business_logic_extractor.batch import build_business_logic
    build_business_logic(input_dir='/lakehouse/default/Files/views',
                         schema_path='/lakehouse/default/Files/schemas/clarity.yaml',
                         output_csv='/lakehouse/default/Files/outputs/business_logic.csv',
                         use_llm=False, dialect='tsql')

CLI usage:
    python -m tools.business_logic_extractor.batch <input_dir> --schema <yaml> [-o out.csv] [--use-llm]
"""

import argparse
import csv
import json
import sys
from pathlib import Path

from sql_logic_extractor.business_logic import load_schema
from sql_logic_extractor.products import extract_business_logic


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


def _process_view(view_path: Path, schema: dict, *, use_llm: bool, llm_client,
                  dialect: str) -> list[dict]:
    sql = _read_sql_file(view_path)
    if not sql.strip():
        return [_error_row(view_path, "EMPTY", use_llm)]

    try:
        bl = extract_business_logic(sql, schema, use_llm=use_llm,
                                      llm_client=llm_client, dialect=dialect)
    except Exception as e:
        return [_error_row(view_path, f"ERROR: {type(e).__name__}: {e}", use_llm)]

    view_name = view_path.stem
    rows: list[dict] = []
    for t in bl.column_translations:
        rows.append({
            "view_file": view_path.name,
            "view_name": view_name,
            "column_name": t.get("column_name", ""),
            "column_type": t.get("column_type", "unknown"),
            "english_definition": t.get("english_definition", ""),
            "business_domain": t.get("business_domain", ""),
            "base_columns": ", ".join(t.get("base_columns", []) or []),
            "base_tables": ", ".join(t.get("base_tables", []) or []),
            "resolved_expression": t.get("resolved_expression", ""),
            "english_definition_with_filters": t.get("english_definition_with_filters", ""),
            "use_llm": "true" if use_llm else "false",
        })
    return rows


def _error_row(view_path: Path, msg: str, use_llm: bool) -> dict:
    return {
        "view_file": view_path.name, "view_name": view_path.stem,
        "column_name": "", "column_type": "parse_error",
        "english_definition": msg, "business_domain": "",
        "base_columns": "", "base_tables": "", "resolved_expression": "",
        "english_definition_with_filters": "",
        "use_llm": "true" if use_llm else "false",
    }


def build_business_logic(input_dir: str, schema_path: str | None = None,
                          output_csv: str = "business_logic.csv",
                          *, use_llm: bool = False, llm_client=None,
                          dialect: str = "tsql") -> int:
    """Folder mode entry point. Returns 0 on success, 1 on usage error."""
    in_dir = Path(input_dir)
    if not in_dir.is_dir():
        print(f"Error: {in_dir} is not a directory")
        return 1
    sql_files = sorted(in_dir.glob("*.sql"))
    if not sql_files:
        print(f"Error: no .sql files in {in_dir}")
        return 1

    schema = load_schema(schema_path) if schema_path else {}

    fieldnames = ["view_file", "view_name", "column_name", "column_type",
                  "english_definition", "business_domain",
                  "base_columns", "base_tables", "resolved_expression",
                  "english_definition_with_filters", "use_llm"]

    all_rows: list[dict] = []
    for path in sql_files:
        print(f"Parsing: {path.name}")
        all_rows.extend(_process_view(path, schema, use_llm=use_llm,
                                        llm_client=llm_client, dialect=dialect))

    out = Path(output_csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    err = sum(1 for r in all_rows if r["column_type"] == "parse_error")
    mode = "LLM" if use_llm else "engineered"
    print(f"\nWrote {len(all_rows)} business-logic rows from {len(sql_files)} view(s) "
          f"({mode} mode) -> {out}")
    if err:
        print(f"  ({err} view(s) failed -- see 'parse_error' rows)")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build a business-logic CSV from a folder of SQL views."
    )
    parser.add_argument("input_dir", help="Folder containing view *.sql files")
    parser.add_argument("--schema", default=None, help="Schema YAML/JSON (data dictionary)")
    parser.add_argument("-o", "--output", default="business_logic.csv")
    parser.add_argument("-d", "--dialect", default="tsql")
    parser.add_argument("--use-llm", action="store_true",
                        help="Use LLM mode (requires business_logic_llm license feature)")
    args = parser.parse_args()
    return build_business_logic(args.input_dir, args.schema, args.output,
                                  use_llm=args.use_llm, dialect=args.dialect)


def _is_notebook() -> bool:
    return "ipykernel" in sys.argv[0] or "ipykernel" in " ".join(sys.argv[1:])


if __name__ == "__main__":
    if _is_notebook():
        print("Notebook environment detected -- call build_business_logic("
              "input_dir=..., schema_path=..., output_csv=..., use_llm=False) from a cell.")
    else:
        sys.exit(main())
