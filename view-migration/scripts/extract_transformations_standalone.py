"""Extract transformed (non-passthrough) columns from a folder of SQL views.

Single-file, depends only on sqlglot. Designed for Fabric Notebook paste-and-run
or local CLI use. Reuses the SSMS encoding / boilerplate handling from the
manifest builder.

Output CSV columns:
    view_file               filename of the view
    view_name               the view's own dbo.<name> from CREATE VIEW
    column_name             the output column / alias
    transformation_type     case | window | aggregate | function | calculated
                            | scalar_subquery | literal | other
    expression              the SQL of the SELECT-item (truncated if long)

Passthrough columns (e.g. `R.STATUS_C` or `[ID] = R.PAT_ID`) are skipped —
they're direct mappings that don't need governance/curation. Only columns
where the SELECT-item expression is something other than a bare Column node
are emitted.

================================================================================
Fabric Notebook usage
================================================================================

Cell 1: %pip install sqlglot

Cell 2 (paste this whole file's contents):
    <paste here>
    # Bottom prints "Notebook environment detected — call ..."

Cell 3:
    extract_transformations(
        input_dir='/lakehouse/default/Files/views',
        output_csv='/lakehouse/default/Files/transformations.csv',
        dialect='tsql',
    )

Cell 4 (preview):
    import pandas as pd
    pd.read_csv('/lakehouse/default/Files/transformations.csv').head(20)

================================================================================
Local CLI
================================================================================

    python3 extract_transformations_standalone.py <input_dir> [-o transformations.csv] [-d tsql]
"""

import argparse
import csv
import re
import sys
from pathlib import Path
from typing import Optional

from sqlglot import exp, parse_one


# ---------------------------------------------------------------------------
# SSMS encoding + boilerplate handling (shared with build_manifest_standalone)
# ---------------------------------------------------------------------------

def _read_sql(path: Path) -> str:
    """Read a SQL file, handling SSMS's default UTF-16 LE BOM."""
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


_GO_RE = re.compile(r"^\s*GO\s*$", re.IGNORECASE)
_USE_RE = re.compile(r"^\s*USE\s+", re.IGNORECASE)
_SET_RE = re.compile(
    r"^\s*SET\s+(ANSI_NULLS|QUOTED_IDENTIFIER|NOCOUNT|TRANSACTION|ARITHABORT|"
    r"NUMERIC_ROUNDABORT|CONCAT_NULL_YIELDS_NULL|ANSI_PADDING|ANSI_WARNINGS|"
    r"DATEFORMAT|DATEFIRST)\b",
    re.IGNORECASE,
)
_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)


def _strip_ssms_boilerplate(sql: str) -> str:
    sql = _BLOCK_COMMENT_RE.sub("", sql)
    cleaned: list[str] = []
    for line in sql.split("\n"):
        if _GO_RE.match(line) or _USE_RE.match(line) or _SET_RE.match(line):
            continue
        cleaned.append(line)
    return "\n".join(cleaned).strip()


# ---------------------------------------------------------------------------
# Transformation classification
# ---------------------------------------------------------------------------

# When unwrapping an Alias, the inner expression tells us the kind of
# transformation. Order matters — check more specific subclasses first.
def _classify(node: exp.Expression) -> str:
    """Return a short label describing the transformation in this expression.
    Returns 'passthrough' for a bare Column node — the caller should skip it."""
    if isinstance(node, exp.Column):
        return "passthrough"
    if isinstance(node, exp.Literal) or isinstance(node, exp.Null) or isinstance(node, exp.Boolean):
        return "literal"
    if isinstance(node, (exp.Window,)):
        return "window"
    if isinstance(node, exp.Case):
        return "case"
    if isinstance(node, exp.Subquery):
        return "scalar_subquery"
    # Aggregate functions — sqlglot has a base AggFunc class for SUM, AVG, COUNT, MIN, MAX, etc.
    if isinstance(node, exp.AggFunc):
        return "aggregate"
    # CAST / CONVERT
    if isinstance(node, exp.Cast):
        return "function"
    # Generic function call (CONCAT, ISNULL, COALESCE, custom UDFs, etc.)
    if isinstance(node, exp.Func):
        return "function"
    # Arithmetic, string concat, comparisons combined into expressions
    if isinstance(node, (exp.Add, exp.Sub, exp.Mul, exp.Div, exp.Mod,
                         exp.DPipe, exp.SafeConcat)):
        return "calculated"
    return "other"


# ---------------------------------------------------------------------------
# Per-view extraction
# ---------------------------------------------------------------------------

def _select_body(parsed: exp.Expression) -> Optional[exp.Select]:
    """Find the top-level SELECT inside a parsed CREATE VIEW (or bare SELECT)."""
    if isinstance(parsed, exp.Create) and parsed.expression is not None:
        return parsed.expression
    if isinstance(parsed, exp.Select):
        return parsed
    # Some views wrap the SELECT in extra layers (e.g. UNION / CTE-only) — best effort
    sel = parsed.find(exp.Select)
    return sel


def _expression_text(node: exp.Expression, max_len: int = 500) -> str:
    """Render a node's SQL for the CSV, collapsing whitespace and truncating
    if absurdly long so Excel doesn't choke."""
    sql = node.sql(dialect="tsql").strip()
    sql = re.sub(r"\s+", " ", sql)
    if len(sql) > max_len:
        sql = sql[:max_len] + " ..."
    return sql


def extract_transformations(view_path: Path, dialect: str = "tsql") -> list[dict]:
    """Parse one view; return a list of CSV rows, one per transformed column."""
    sql = _read_sql(view_path)
    sql = _strip_ssms_boilerplate(sql)
    if not sql:
        return [_error_row(view_path, "EMPTY: file contained only SSMS boilerplate")]

    try:
        parsed = parse_one(sql, dialect=dialect)
    except Exception as e:
        return [_error_row(view_path, f"PARSE ERROR: {e}")]

    # View's own name from CREATE VIEW header.
    view_name = view_path.stem
    if isinstance(parsed, exp.Create) and isinstance(parsed.this, exp.Table):
        t = parsed.this
        schema = t.args["db"].name if t.args.get("db") else None
        view_name = f"{schema}.{t.name}" if schema else t.name

    select = _select_body(parsed)
    if select is None:
        return [_error_row(view_path, "NO SELECT: could not find a SELECT clause")]

    rows: list[dict] = []
    for item in select.expressions:
        # Each select item is either an Alias (most common) or a bare expression.
        if isinstance(item, exp.Alias):
            output_name = item.alias_or_name
            inner = item.this
        else:
            output_name = item.alias_or_name or _expression_text(item, max_len=80)
            inner = item

        kind = _classify(inner)
        if kind == "passthrough":
            continue
        rows.append({
            "view_file": view_path.name,
            "view_name": view_name,
            "column_name": output_name,
            "transformation_type": kind,
            "expression": _expression_text(item),
        })
    return rows


def _error_row(view_path: Path, msg: str) -> dict:
    return {
        "view_file": view_path.name,
        "view_name": view_path.stem,
        "column_name": "",
        "transformation_type": "parse_error",
        "expression": msg,
    }


# ---------------------------------------------------------------------------
# Folder walk + CSV output
# ---------------------------------------------------------------------------

def extract_transformations_to_csv(input_dir: str, output_csv: str = "transformations.csv",
                                    dialect: str = "tsql") -> int:
    in_dir = Path(input_dir)
    if not in_dir.is_dir():
        print(f"Error: {in_dir} is not a directory")
        return 1
    sql_files = sorted(in_dir.glob("*.sql"))
    if not sql_files:
        print(f"Error: no .sql files in {in_dir}")
        return 1

    fieldnames = ["view_file", "view_name", "column_name",
                  "transformation_type", "expression"]

    all_rows: list[dict] = []
    for path in sql_files:
        print(f"Parsing: {path.name}")
        all_rows.extend(extract_transformations(path, dialect=dialect))

    out = Path(output_csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    err = sum(1 for r in all_rows if r["transformation_type"] == "parse_error")
    print(f"\nWrote {len(all_rows)} transformed-column rows from {len(sql_files)} view(s) → {out}")
    if err:
        print(f"  ({err} view(s) failed to parse — see 'parse_error' rows in CSV)")
    return 0


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract transformed (non-passthrough) columns from SQL views into a CSV."
    )
    parser.add_argument("input_dir", help="Folder containing view *.sql files")
    parser.add_argument("-o", "--output", default="transformations.csv",
                        help="Output CSV path (default: transformations.csv)")
    parser.add_argument("-d", "--dialect", default="tsql",
                        help="sqlglot dialect (default: tsql)")
    args = parser.parse_args()
    return extract_transformations_to_csv(args.input_dir, args.output, args.dialect)


def _is_notebook() -> bool:
    return "ipykernel" in sys.argv[0] or "ipykernel" in " ".join(sys.argv[1:])


if __name__ == "__main__":
    if _is_notebook():
        print("Notebook environment detected — call extract_transformations_to_csv("
              "input_dir=..., output_csv=..., dialect='tsql') from a cell.")
    else:
        sys.exit(main())
