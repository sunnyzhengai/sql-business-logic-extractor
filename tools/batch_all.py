#!/usr/bin/env python3
"""All-tools batch runner -- single resolver pass per view, four CSV outputs.

For accuracy-critical use cases that need ALL FOUR tools' outputs across
a folder of views, this runs the engine pipeline ONCE per view and shapes
all four tools' output rows from the same parse. Same accuracy as running
the four individual batch CLIs separately, materially faster on real-
world workloads (~3x on multi-tool runs since the resolver is the heavy
step and each individual batch reruns it).

Output: four CSV files in --output-dir, named after their tool's folder:
    column_lineage_extractor.csv
    technical_logic_extractor.csv
    business_logic_extractor.csv
    report_description_generator.csv

Notebook usage:
    from tools.batch_all import run_all
    run_all(
        input_dir='/lakehouse/default/Files/views',
        output_dir='/lakehouse/default/Files/outputs',
        schema_path='/lakehouse/default/Files/schemas/clarity.yaml',  # optional
        use_llm=False,
        dialect='tsql',
    )

CLI usage:
    python -m tools.batch_all <input_dir> --output-dir <out> [--schema <yaml>] [-d tsql] [--use-llm]

When NOT to use this:
    - A customer who only paid for Tool 1 should use that tool's batch.py
      directly. batch_all calls the full pipeline (Tools 1-4) and would
      hit the license gate at Tool 2 / Tool 3 / Tool 4 if those features
      aren't unlocked.
    - When you only need one tool's output, calling that tool's batch
      directly is conceptually simpler (and not slower in single-tool
      mode -- batch_all's savings come from sharing the parse across
      tools, which doesn't apply if you only want one tool's output).
"""

import argparse
import csv
import sys
from pathlib import Path

from sql_logic_extractor.business_logic import build_alias_map, load_schema
from sql_logic_extractor.products import generate_report_description

# Re-use each tool's row-shaping function and CSV field schema.
from tools.column_lineage_extractor.batch import (
    _read_sql_file, _table_level_rows, rows_from_inventory,
)
from tools.technical_logic_extractor.batch import rows_from_lineage
from tools.business_logic_extractor.batch import rows_from_business_logic
from tools.report_description_generator.batch import row_from_report_description


# Field schemas matched to each tool's existing batch.py output:
TOOL1_FIELDS = ["view_name", "referenced_database",
                "referenced_schema", "referenced_table", "referenced_column",
                "reference_type", "confidence"]

TOOL2_FIELDS = ["view_name", "column_name", "column_type",
                "resolved_expression", "base_tables", "base_columns", "filters"]

TOOL3_FIELDS = ["view_name", "column_name", "column_type",
                "english_definition", "business_domain",
                "resolved_expression",
                "english_definition_with_filters", "use_llm"]

TOOL4_FIELDS = ["view_name", "query_summary", "primary_purpose",
                "key_metrics", "source_tables", "column_count", "use_llm"]


def _error_rows_all_tools(view_path: Path, msg: str, use_llm: bool) -> dict:
    """When a view fails to parse / resolve, emit a single error row in
    each tool's CSV so the user can see which view broke and why."""
    base = {"view_name": view_path.stem}
    return {
        "tool1": [{**base, "referenced_database": "", "referenced_schema": "",
                    "referenced_table": "", "referenced_column": msg,
                    "reference_type": "parse_error", "confidence": "low"}],
        "tool2": [{**base, "column_name": "", "column_type": "parse_error",
                    "resolved_expression": msg, "base_tables": "",
                    "base_columns": "", "filters": ""}],
        "tool3": [{"view_name": view_path.stem,
                    "column_name": "", "column_type": "parse_error",
                    "english_definition": msg, "business_domain": "",
                    "resolved_expression": "",
                    "english_definition_with_filters": "",
                    "use_llm": "true" if use_llm else "false"}],
        "tool4": [{**base, "query_summary": msg, "primary_purpose": "parse_error",
                    "key_metrics": "", "source_tables": "", "column_count": 0,
                    "use_llm": "true" if use_llm else "false"}],
    }


def _process_view_all_tools(view_path: Path, schema: dict, *,
                              use_llm: bool, llm_client, dialect: str) -> dict:
    """Run the engine ONCE per view, shape rows for all four tools.

    Returns a dict with keys 'tool1', 'tool2', 'tool3', 'tool4', each
    holding a list of rows ready for CSV writing."""
    sql = _read_sql_file(view_path)
    if not sql.strip():
        return _error_rows_all_tools(view_path, "EMPTY: file is empty after decoding", use_llm)

    try:
        # Single cascading call -- internally runs L1 (extract) + L2
        # (normalize) + L3 (resolve) + L4 (translate) + summarize. The
        # resulting `desc` carries the full nested chain that each tool
        # needs to shape its output.
        desc = generate_report_description(sql, schema, use_llm=use_llm,
                                              llm_client=llm_client, dialect=dialect)
    except Exception as e:
        return _error_rows_all_tools(view_path, f"ERROR: {type(e).__name__}: {e}", use_llm)

    bl = desc.business_logic
    lineage = bl.lineage
    inventory = lineage.inventory
    view_name = view_path.stem
    alias_map = build_alias_map(sql, dialect=dialect)

    # Tool 1: inventory rows + table-walk rows (table-walk needs sql + dialect
    # but doesn't run the resolver -- it's a separate sqlglot AST walk).
    t1_rows, seen = rows_from_inventory(view_path, view_name, inventory)
    t1_rows.extend(_table_level_rows(sql, dialect, view_name, seen))

    return {
        "tool1": t1_rows,
        "tool2": rows_from_lineage(view_path, view_name, lineage, alias_map, dialect),
        "tool3": rows_from_business_logic(view_path, view_name, bl, use_llm),
        "tool4": [row_from_report_description(view_path, view_name, desc, use_llm)],
    }


def _write_csv(path: str, fieldnames: list[str], rows: list[dict]) -> None:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def run_all(input_dir: str, output_dir: str, *,
              schema_path: str | None = None,
              use_llm: bool = False, llm_client=None,
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

    # Accumulate rows per tool across all views.
    accum: dict[str, list[dict]] = {"tool1": [], "tool2": [], "tool3": [], "tool4": []}
    for path in sql_files:
        print(f"Parsing: {path.name}")
        per_view = _process_view_all_tools(path, schema, use_llm=use_llm,
                                              llm_client=llm_client, dialect=dialect)
        for k in accum:
            accum[k].extend(per_view[k])

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(out_dir / "column_lineage_extractor.csv",        TOOL1_FIELDS, accum["tool1"])
    _write_csv(out_dir / "technical_logic_extractor.csv",       TOOL2_FIELDS, accum["tool2"])
    _write_csv(out_dir / "business_logic_extractor.csv",        TOOL3_FIELDS, accum["tool3"])
    _write_csv(out_dir / "report_description_generator.csv",    TOOL4_FIELDS, accum["tool4"])

    err = sum(1 for r in accum["tool1"] if r.get("reference_type") == "parse_error")
    mode = "LLM" if use_llm else "engineered"
    print(f"\nAll 4 tools written to {out_dir} ({mode} mode for Tools 3 + 4)")
    print(f"  Tool 1 (column_lineage_extractor.csv):     {len(accum['tool1'])} rows")
    print(f"  Tool 2 (technical_logic_extractor.csv):    {len(accum['tool2'])} rows")
    print(f"  Tool 3 (business_logic_extractor.csv):     {len(accum['tool3'])} rows")
    print(f"  Tool 4 (report_description_generator.csv): {len(accum['tool4'])} rows")
    if err:
        print(f"  ({err} view(s) failed -- see 'parse_error' rows in each CSV)")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run all 4 tools on a folder in a single pass per view."
    )
    parser.add_argument("input_dir", help="Folder containing view *.sql files")
    parser.add_argument("--output-dir", default=".", help="Folder to write the 4 CSVs into")
    parser.add_argument("--schema", default=None, help="Schema YAML/JSON for Tools 3 + 4")
    parser.add_argument("-d", "--dialect", default="tsql")
    parser.add_argument("--use-llm", action="store_true",
                        help="Use LLM mode for Tools 3 + 4 (requires _llm license features)")
    args = parser.parse_args()
    return run_all(args.input_dir, args.output_dir,
                    schema_path=args.schema, use_llm=args.use_llm, dialect=args.dialect)


def _is_notebook() -> bool:
    return "ipykernel" in sys.argv[0] or "ipykernel" in " ".join(sys.argv[1:])


if __name__ == "__main__":
    if _is_notebook():
        print("Notebook environment detected -- call run_all("
              "input_dir=..., output_dir=..., schema_path=..., use_llm=False) from a cell.")
    else:
        sys.exit(main())
