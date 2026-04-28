#!/usr/bin/env python3
"""Extract view metadata for the SSIS-to-Fabric migration workstream.

ONE wrapper, ONE pass over a folder of SQL views, TWO output CSVs:

1. manifest.csv — every (database, schema, table, column) the views reference,
   for handoff to the ETL team. Columns:
       view_file, view_name, referenced_database, referenced_schema,
       referenced_table, referenced_column, reference_type, confidence

2. transformations.csv — every NON-trivial output column with full lineage,
   for governance / steward review. Columns:
       view_file, view_name, column_name, column_type, resolved_expression,
       base_tables, base_columns, filters

A column is *truly trivial* (skipped in transformations.csv) only when:
    column_type is 'passthrough' AND the resolver attached no filters.
Passthrough WITH filters stays — the WHERE clause encodes business logic.

Built on top of the parent project's `sql_logic_extractor` package
(extract → normalize → resolve), which handles the full slew of SQL shapes
we care about: SSMS USE/GO/SET boilerplate, CTE chain flattening, alias
resolution, self-joins, EXISTS subqueries, and propagating WHERE-clause
predicates to per-column lineage. UTF-16 LE BOM (SSMS scripted views) is
handled in this wrapper before the SQL hits the resolver.

================================================================================
Required files (5 total — minimum for the full lineage path):
    sql_logic_extractor/__init__.py
    sql_logic_extractor/extract.py
    sql_logic_extractor/normalize.py
    sql_logic_extractor/resolve.py
    cli/extract_view_metadata.py    ← this file

This script auto-injects the project root onto sys.path so imports work
without a pip install.

================================================================================
Fabric Notebook usage (one-time per fresh session):

    !cd /lakehouse/default/Files && \\
        rm -rf repo && \\
        git clone https://github.com/sunnyzhengai/sql-business-logic-extractor.git repo
    %pip install sqlglot

Then in code:

    import sys
    sys.path.insert(0, '/lakehouse/default/Files/repo')
    sys.path.insert(0, '/lakehouse/default/Files/repo/cli')
    from extract_view_metadata import extract_to_csvs

    extract_to_csvs(
        input_dir='/lakehouse/default/Files/views',
        manifest_csv='/lakehouse/default/Files/manifest.csv',
        transformations_csv='/lakehouse/default/Files/transformations.csv',
        dialect='tsql',
    )

================================================================================
Local CLI:

    python3 cli/extract_view_metadata.py <input_dir> \\
        [--manifest manifest.csv] [--transformations transformations.csv] \\
        [-d tsql]
"""

import argparse
import csv
import sys
from pathlib import Path
from typing import Optional

# Add the parent repo root to sys.path so `sql_logic_extractor` imports
# work without a pip install. This script lives at <repo>/cli/.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from sqlglot import exp, parse_one

from sql_logic_extractor.resolve import resolve_query, resolved_to_dict, preprocess_ssms


# ---------------------------------------------------------------------------
# Encoding handling (the resolver expects str — we have to decode bytes here)
# ---------------------------------------------------------------------------

def _read_sql(path: Path) -> str:
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


# ---------------------------------------------------------------------------
# Per-table qualifier recovery (resolver gives Table.Column; we want db/schema)
# ---------------------------------------------------------------------------

def _qualify_table(t: exp.Table) -> tuple[Optional[str], Optional[str], str]:
    """Return (database, schema, table) from a sqlglot Table node. In
    sqlglot's MySQL-rooted naming, `catalog` is the SQL Server database and
    `db` is the SQL Server schema."""
    return (
        t.args["catalog"].name if t.args.get("catalog") else None,
        t.args["db"].name if t.args.get("db") else None,
        t.name,
    )


def _build_qualifier_map(parsed: exp.Expression, cte_names: set[str]) -> dict[str, tuple]:
    """{table_name_lower: (db, schema, table)} — the resolver normalises
    Table.Column lineage to bare table names; this map lets us look up the
    db/schema portions for the manifest. CTE names are excluded — they're
    query-internal."""
    mapping: dict[str, tuple] = {}
    for t in parsed.find_all(exp.Table):
        if t.name in cte_names:
            continue
        full = _qualify_table(t)
        mapping[t.name.lower()] = full
    return mapping


# ---------------------------------------------------------------------------
# Per-view extraction — runs the resolver, then shapes both output flavours
# ---------------------------------------------------------------------------

MANIFEST_FIELDS = ["view_file", "view_name", "referenced_database",
                   "referenced_schema", "referenced_table", "referenced_column",
                   "reference_type", "confidence"]
TRANSFORMATIONS_FIELDS = ["view_file", "view_name", "column_name", "column_type",
                          "resolved_expression", "base_tables", "base_columns",
                          "filters"]


def _split_table_column(ref: str) -> tuple[Optional[str], str]:
    """`Table.Column` → (Table, Column). `Column` (bare) → (None, Column)."""
    if "." not in ref:
        return None, ref
    lhs, col = ref.rsplit(".", 1)
    return lhs.rsplit(".", 1)[-1], col


def _filter_text(f) -> str:
    if isinstance(f, dict):
        return (f.get("expression") or "").strip()
    return str(f or "").strip()


def extract_view(view_path: Path, dialect: str = "tsql") -> tuple[list[dict], list[dict]]:
    """Run the resolver on one view; return (manifest_rows, transformation_rows)."""
    sql = _read_sql(view_path)
    if not sql.strip():
        err = _manifest_error_row(view_path, "EMPTY: file is empty after decoding")
        return [err], [_transformation_error_row(view_path, "EMPTY")]

    # We re-parse with sqlglot to recover db/schema qualifiers (the resolver
    # output strips them). Run the same SSMS preprocessor the resolver uses
    # so our parse handles USE/GO/SET-stripped scripts too — otherwise
    # parse_one stops at the first non-DDL line.
    clean_sql, _ = preprocess_ssms(sql)
    if not clean_sql.strip():
        clean_sql = sql.strip()
    try:
        parsed = parse_one(clean_sql, dialect=dialect)
    except Exception as e:
        return ([_manifest_error_row(view_path, f"PARSE ERROR: {e}")],
                [_transformation_error_row(view_path, f"PARSE ERROR: {e}")])

    cte_names = {cte.alias_or_name for cte in parsed.find_all(exp.CTE)}
    qualifier_map = _build_qualifier_map(parsed, cte_names)

    self_name: Optional[str] = None
    if isinstance(parsed, exp.Create) and isinstance(parsed.this, exp.Table):
        self_name = parsed.this.name

    try:
        resolved = resolve_query(sql, dialect=dialect)
    except Exception as e:
        return ([_manifest_error_row(view_path, f"RESOLVE ERROR: {e}")],
                [_transformation_error_row(view_path, f"RESOLVE ERROR: {e}")])
    rd = resolved_to_dict(resolved)

    view_schema = rd.get("schema")
    view_short = rd.get("name") or view_path.stem
    view_full = f"{view_schema}.{view_short}" if view_schema else view_short

    manifest_rows = _build_manifest_rows(view_path, view_full, rd, parsed,
                                          qualifier_map, cte_names, self_name)
    transformation_rows = _build_transformation_rows(view_path, view_full, rd)
    return manifest_rows, transformation_rows


def _build_manifest_rows(view_path, view_full, rd, parsed, qualifier_map,
                          cte_names, self_name) -> list[dict]:
    rows: list[dict] = []
    seen: set[tuple] = set()

    # Column-level: iterate every base_column the resolver attributed.
    for col in rd.get("columns", []):
        for base_col in col.get("base_columns", []) or []:
            tbl, col_name = _split_table_column(base_col)
            if tbl in cte_names:
                continue
            db, schema, qualified_tbl = (
                qualifier_map.get(tbl.lower(), (None, None, tbl)) if tbl
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

    # Table-level: catches SELECT *, EXISTS, COUNT(*), and any table referenced
    # for its existence rather than a specific column.
    for t in parsed.find_all(exp.Table):
        if t.name in cte_names:
            continue
        if t.name == self_name:
            continue
        db, schema, name = _qualify_table(t)
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


def _build_transformation_rows(view_path, view_full, rd) -> list[dict]:
    rows: list[dict] = []
    for col in rd.get("columns", []):
        col_type = col.get("type", "unknown")
        col_filters = col.get("filters", []) or []

        # Truly-trivial passthrough (no filters, no transformation) is skipped.
        # Passthrough WITH filters stays — the WHERE clause encodes meaning.
        if col_type == "passthrough" and not col_filters:
            continue

        rows.append({
            "view_file": view_path.name,
            "view_name": view_full,
            "column_name": col.get("name", ""),
            "column_type": col_type,
            "resolved_expression": col.get("resolved_expression", ""),
            "base_tables": ", ".join(col.get("base_tables", []) or []),
            "base_columns": ", ".join(col.get("base_columns", []) or []),
            "filters": "; ".join(filter(None, (_filter_text(f) for f in col_filters))),
        })
    return rows


def _manifest_error_row(view_path: Path, msg: str) -> dict:
    return {
        "view_file": view_path.name, "view_name": view_path.stem,
        "referenced_database": "", "referenced_schema": "",
        "referenced_table": "", "referenced_column": msg,
        "reference_type": "parse_error", "confidence": "low",
    }


def _transformation_error_row(view_path: Path, msg: str) -> dict:
    return {
        "view_file": view_path.name, "view_name": view_path.stem,
        "column_name": "", "column_type": "parse_error",
        "resolved_expression": msg, "base_tables": "",
        "base_columns": "", "filters": "",
    }


# ---------------------------------------------------------------------------
# Folder walk + CSV writers
# ---------------------------------------------------------------------------

def extract_to_csvs(input_dir: str,
                    manifest_csv: str = "manifest.csv",
                    transformations_csv: str = "transformations.csv",
                    dialect: str = "tsql") -> int:
    """Notebook-callable entry point. Walks input_dir for *.sql files, runs
    each through the resolver, writes both CSVs. Returns 0 on success, 1 on
    usage error."""
    in_dir = Path(input_dir)
    if not in_dir.is_dir():
        print(f"Error: {in_dir} is not a directory")
        return 1
    sql_files = sorted(in_dir.glob("*.sql"))
    if not sql_files:
        print(f"Error: no .sql files in {in_dir}")
        return 1

    all_manifest: list[dict] = []
    all_transforms: list[dict] = []
    for path in sql_files:
        print(f"Parsing: {path.name}")
        m, t = extract_view(path, dialect=dialect)
        all_manifest.extend(m)
        all_transforms.extend(t)

    _write_csv(manifest_csv, MANIFEST_FIELDS, all_manifest)
    _write_csv(transformations_csv, TRANSFORMATIONS_FIELDS, all_transforms)

    err = sum(1 for r in all_manifest if r["reference_type"] == "parse_error")
    print(f"\nWrote {len(all_manifest)} manifest rows + "
          f"{len(all_transforms)} transformation rows from {len(sql_files)} view(s)")
    print(f"  manifest:        {manifest_csv}")
    print(f"  transformations: {transformations_csv}")
    if err:
        print(f"  ({err} view(s) failed to parse — see 'parse_error' rows in CSVs)")
    return 0


def _write_csv(path: str, fieldnames: list[str], rows: list[dict]) -> None:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    # UTF-8 with BOM so Excel opens it cleanly on the ETL team's side.
    with out.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# CLI / notebook entry points
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract manifest + transformations CSVs from a folder of SQL views."
    )
    parser.add_argument("input_dir", help="Folder containing view *.sql files")
    parser.add_argument("--manifest", default="manifest.csv",
                        help="Manifest CSV path (default: manifest.csv)")
    parser.add_argument("--transformations", default="transformations.csv",
                        help="Transformations CSV path (default: transformations.csv)")
    parser.add_argument("-d", "--dialect", default="tsql",
                        help="sqlglot dialect (default: tsql)")
    args = parser.parse_args()
    return extract_to_csvs(args.input_dir, args.manifest, args.transformations, args.dialect)


def _is_notebook() -> bool:
    return "ipykernel" in sys.argv[0] or "ipykernel" in " ".join(sys.argv[1:])


if __name__ == "__main__":
    if _is_notebook():
        print("Notebook environment detected — call extract_to_csvs("
              "input_dir=..., manifest_csv=..., transformations_csv=..., dialect='tsql') "
              "from a cell.")
    else:
        sys.exit(main())
