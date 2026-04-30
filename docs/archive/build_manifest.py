#!/usr/bin/env python3
"""Build the view-migration manifest CSV from a folder of SQL view DDL files.

For each *.sql file in INPUT_DIR, extract every (database, schema, table, column)
referenced by the view and emit a flat CSV that can be handed to the ETL team.

Pipeline:  parse → resolve (sql_logic_extractor) → flatten → CSV.

Usage:
    python build_manifest.py <input_dir> [-o manifest.csv] [-d tsql]

Example:
    python build_manifest.py ../sample/views -o ../manifest.csv

Dependencies:
    sqlglot (already required by sql_logic_extractor)
    sql_logic_extractor package on PYTHONPATH

The script emits one CSV row per distinct
(view, referenced_database, referenced_schema, referenced_table, referenced_column)
tuple. CTE names are filtered out so the ETL team never sees query-internal
identifiers. Both column-level references (from the resolver) and table-level
references (from a direct AST walk, for SELECT * / EXISTS / etc.) are captured.
"""

import argparse
import csv
import sys
from pathlib import Path
from typing import Optional

# Add the parent project root to sys.path so `sql_logic_extractor` imports work
# without a pip install or PYTHONPATH= prefix. Script lives at:
#   <repo>/view-migration/scripts/build_manifest.py
# Project root is two levels up.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from sqlglot import exp, parse_one

from sql_logic_extractor.resolve import resolve_query, resolved_to_dict


def _qualify_table(t: exp.Table) -> tuple[Optional[str], Optional[str], str]:
    """(database, schema, table) for a sqlglot Table node.

    sqlglot's naming follows MySQL roots: in catalog.db.table the "catalog" is
    the database and "db" is the schema. The mapping below restates that into
    SQL-Server-friendly terms.
    """
    return (
        t.args["catalog"].name if t.args.get("catalog") else None,
        t.args["db"].name if t.args.get("db") else None,
        t.name,
    )


def _build_table_qualifier_map(parsed: exp.Expression) -> dict[str, tuple]:
    """Walk every Table node, return {alias_or_name: (db, schema, table)}.

    Used to recover database/schema info that the resolver drops when it
    normalises Table.Column lineage to bare table names. CTE references are
    filtered out — they're query-internal, not real database objects.
    """
    cte_names = {cte.alias_or_name for cte in parsed.find_all(exp.CTE)}
    mapping: dict[str, tuple] = {}
    for t in parsed.find_all(exp.Table):
        if t.name in cte_names:
            continue
        full = _qualify_table(t)
        alias = t.alias_or_name
        if alias and alias != t.name:
            mapping[alias] = full
        # Bare-name fallback (the resolver emits bare table names, not aliases).
        mapping[t.name] = full
    return mapping


def _split_base_column(ref: str) -> tuple[Optional[str], str]:
    """'Table.Column' → ('Table', 'Column'). 'Column' → (None, 'Column').

    For 3-part inputs like 'a.b.c' (rare but possible), keep only the last
    table-name segment ('b') as the table — the qualifier_map stores db/schema
    separately.
    """
    if "." not in ref:
        return None, ref
    lhs, col = ref.rsplit(".", 1)
    return lhs.rsplit(".", 1)[-1], col


def _extract_view_refs(view_path: Path, dialect: str = "tsql") -> list[dict]:
    """Parse one view file, return a list of manifest rows."""
    sql = view_path.read_text(encoding="utf-8", errors="replace")

    try:
        parsed = parse_one(sql, dialect=dialect)
    except Exception as e:
        return [_error_row(view_path, f"PARSE ERROR: {e}")]

    qualifier_map = _build_table_qualifier_map(parsed)
    cte_names = {cte.alias_or_name for cte in parsed.find_all(exp.CTE)}

    # The view's own name appears as a Table node in CREATE VIEW <name> AS ...
    # — exclude it so the manifest only contains things the view *references*.
    self_name: Optional[str] = None
    if isinstance(parsed, exp.Create) and isinstance(parsed.this, exp.Table):
        self_name = parsed.this.name

    try:
        resolved = resolve_query(sql, dialect=dialect)
    except Exception as e:
        return [_error_row(view_path, f"RESOLVE ERROR: {e}")]

    rd = resolved_to_dict(resolved)

    # The resolver pulls the view's own schema/name from the CREATE VIEW header.
    view_schema = rd.get("schema")
    view_name = rd.get("name") or view_path.stem
    view_full = f"{view_schema}.{view_name}" if view_schema else view_name

    rows: list[dict] = []
    seen: set[tuple] = set()

    for col in rd.get("columns", []):
        for base_col in col.get("base_columns", []) or []:
            tbl, col_name = _split_base_column(base_col)
            if tbl and tbl in cte_names:
                continue
            db, schema, qualified_tbl = (
                qualifier_map.get(tbl, (None, None, tbl)) if tbl
                else (None, None, None)
            )
            row_key = (view_path.name, db, schema, qualified_tbl, col_name)
            if row_key in seen:
                continue
            seen.add(row_key)
            rows.append({
                "view_file": view_path.name,
                "view_name": view_full,
                "referenced_database": db or "",
                "referenced_schema": schema or "",
                "referenced_table": qualified_tbl or "",
                "referenced_column": col_name,
                "reference_type": "column",
                "confidence": "high" if (db or schema) else "medium",
            })

    # Also emit one row per Table node — catches references that don't surface
    # column lineage (SELECT *, EXISTS, COUNT(*), etc.).
    for tbl_node in parsed.find_all(exp.Table):
        if tbl_node.name in cte_names:
            continue
        if tbl_node.name == self_name:
            continue
        db, schema, name = _qualify_table(tbl_node)
        row_key = (view_path.name, db, schema, name, "*")
        if row_key in seen:
            continue
        seen.add(row_key)
        rows.append({
            "view_file": view_path.name,
            "view_name": view_full,
            "referenced_database": db or "",
            "referenced_schema": schema or "",
            "referenced_table": name,
            "referenced_column": "*",
            "reference_type": "table",
            "confidence": "high" if (db or schema) else "medium",
        })

    return rows


def _error_row(view_path: Path, msg: str) -> dict:
    return {
        "view_file": view_path.name,
        "view_name": view_path.stem,
        "referenced_database": "",
        "referenced_schema": "",
        "referenced_table": "",
        "referenced_column": msg,
        "reference_type": "parse_error",
        "confidence": "low",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("input_dir", help="Folder containing view *.sql files")
    parser.add_argument("-o", "--output", default="manifest.csv",
                        help="Output CSV path (default: manifest.csv)")
    parser.add_argument("-d", "--dialect", default="tsql",
                        help="sqlglot dialect (default: tsql)")
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    if not input_dir.is_dir():
        print(f"Error: {input_dir} is not a directory", file=sys.stderr)
        return 1

    sql_files = sorted(input_dir.glob("*.sql"))
    if not sql_files:
        print(f"Error: no .sql files in {input_dir}", file=sys.stderr)
        return 1

    fieldnames = [
        "view_file", "view_name",
        "referenced_database", "referenced_schema",
        "referenced_table", "referenced_column",
        "reference_type", "confidence",
    ]

    all_rows: list[dict] = []
    for path in sql_files:
        print(f"Parsing: {path.name}", file=sys.stderr)
        all_rows.extend(_extract_view_refs(path, dialect=args.dialect))

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # UTF-8-with-BOM so Excel opens it cleanly without character mangling.
    with out_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    err_count = sum(1 for r in all_rows if r["reference_type"] == "parse_error")
    print(file=sys.stderr)
    print(f"Wrote {len(all_rows)} rows from {len(sql_files)} view(s) → {out_path}",
          file=sys.stderr)
    if err_count:
        print(f"  ({err_count} view(s) failed to parse — see 'parse_error' rows in CSV)",
              file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
