#!/usr/bin/env python3
"""Folder/batch mode for Tool 4 -- Report Description Generator.

Walks a folder of *.sql view files, runs `generate_report_description` on
each (engineered mode by default; LLM mode opt-in via --use-llm), and
emits a single CSV with one row per view.

CSV columns:
    view_file, view_name, query_summary, primary_purpose,
    key_metrics, source_tables, column_count, use_llm

Notebook usage:
    from tools.report_description_generator.batch import build_report_descriptions
    build_report_descriptions(input_dir='/lakehouse/default/Files/views',
                               schema_path='/lakehouse/default/Files/schemas/clarity.yaml',
                               output_csv='/lakehouse/default/Files/outputs/report_descriptions.csv',
                               use_llm=False, dialect='tsql')

CLI usage:
    python -m tools.report_description_generator.batch <input_dir> --schema <yaml> [-o out.csv] [--use-llm]
"""

import argparse
import csv
import sys
from pathlib import Path

from sql_logic_extractor.business_logic import load_schema
from sql_logic_extractor.products import generate_report_description


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
                  dialect: str) -> dict:
    """Run Tool 4 on one view; return one CSV row."""
    sql = _read_sql_file(view_path)
    if not sql.strip():
        return _error_row(view_path, "EMPTY", use_llm)

    try:
        desc = generate_report_description(sql, schema, use_llm=use_llm,
                                              llm_client=llm_client, dialect=dialect)
    except Exception as e:
        return _error_row(view_path, f"ERROR: {type(e).__name__}: {e}", use_llm)

    base_tables = sorted({t for col in desc.business_logic.lineage.resolved_columns
                            for t in (col.get("base_tables", []) or [])})
    return {
        "view_file": view_path.name,
        "view_name": view_path.stem,
        "query_summary": desc.query_summary,
        "primary_purpose": desc.primary_purpose,
        "key_metrics": ", ".join(desc.key_metrics or []),
        "source_tables": ", ".join(base_tables),
        "column_count": len(desc.business_logic.column_translations),
        "use_llm": "true" if use_llm else "false",
    }


def _error_row(view_path: Path, msg: str, use_llm: bool) -> dict:
    return {
        "view_file": view_path.name, "view_name": view_path.stem,
        "query_summary": msg, "primary_purpose": "parse_error",
        "key_metrics": "", "source_tables": "", "column_count": 0,
        "use_llm": "true" if use_llm else "false",
    }


def build_report_descriptions(input_dir: str, schema_path: str | None = None,
                                output_csv: str = "report_descriptions.csv",
                                *, use_llm: bool = False, llm_client=None,
                                dialect: str = "tsql") -> int:
    in_dir = Path(input_dir)
    if not in_dir.is_dir():
        print(f"Error: {in_dir} is not a directory")
        return 1
    sql_files = sorted(in_dir.glob("*.sql"))
    if not sql_files:
        print(f"Error: no .sql files in {in_dir}")
        return 1

    schema = load_schema(schema_path) if schema_path else {}

    fieldnames = ["view_file", "view_name", "query_summary", "primary_purpose",
                  "key_metrics", "source_tables", "column_count", "use_llm"]

    all_rows: list[dict] = []
    for path in sql_files:
        print(f"Parsing: {path.name}")
        all_rows.append(_process_view(path, schema, use_llm=use_llm,
                                        llm_client=llm_client, dialect=dialect))

    out = Path(output_csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    err = sum(1 for r in all_rows if r["primary_purpose"] == "parse_error")
    mode = "LLM" if use_llm else "engineered"
    print(f"\nWrote {len(all_rows)} report description(s) from {len(sql_files)} view(s) "
          f"({mode} mode) -> {out}")
    if err:
        print(f"  ({err} view(s) failed -- see 'parse_error' rows)")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate report descriptions for a folder of SQL views."
    )
    parser.add_argument("input_dir", help="Folder containing view *.sql files")
    parser.add_argument("--schema", default=None, help="Schema YAML/JSON")
    parser.add_argument("-o", "--output", default="report_descriptions.csv")
    parser.add_argument("-d", "--dialect", default="tsql")
    parser.add_argument("--use-llm", action="store_true",
                        help="Use LLM mode (requires report_description_llm license)")
    args = parser.parse_args()
    return build_report_descriptions(args.input_dir, args.schema, args.output,
                                        use_llm=args.use_llm, dialect=args.dialect)


def _is_notebook() -> bool:
    return "ipykernel" in sys.argv[0] or "ipykernel" in " ".join(sys.argv[1:])


if __name__ == "__main__":
    if _is_notebook():
        print("Notebook environment detected -- call build_report_descriptions("
              "input_dir=..., schema_path=..., output_csv=..., use_llm=False) from a cell.")
    else:
        sys.exit(main())
