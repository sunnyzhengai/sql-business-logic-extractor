#!/usr/bin/env python3
"""Walk a folder of SQL view files and produce SSMS-ready @Tables and
@Columns INSERT VALUES lists for extract_clarity_metadata.sql.

Why this exists:
    extract_clarity_metadata.sql ships with hard-coded @Tables / @Columns
    pre-populated for the two reference views. When you scale to your
    full view set (10, 50, 100 views), you don't want to maintain that
    list by hand. This helper reads every .sql file in a folder, runs
    Tool 1 (column lineage extractor) on each, dedupes the (table,
    column) pairs, and prints them in the exact `('TABLE', 'COLUMN'),`
    syntax SSMS expects.

Usage (from the repo root):

    python3 data/schemas/build_metadata_extract_sql.py /path/to/views/

Or in a Fabric notebook cell:

    import sys
    sys.path.insert(0, '/lakehouse/default/Files')
    from data.schemas.build_metadata_extract_sql import build
    build('/lakehouse/default/Files/views')

Output is printed to stdout. Copy the two INSERT blocks into
extract_clarity_metadata.sql, replacing the existing @Tables and
@Columns INSERT lines.

Note: The output is a SUPERSET -- it includes every (table, column)
the views reference, including any spurious matches from Tool 1's
heuristic (e.g. CTE-aliased columns may be attributed to the wrong
base table). Extra rows are harmless: SSMS just won't find a match
in CLARITY_TBL/COL or sys.* and silently drops them.
"""

import sys
from pathlib import Path

# Repo root is two levels up from this file (data/schemas/this.py).
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from sql_logic_extractor.products import extract_columns


def _read_sql_file(path: Path) -> str:
    """SSMS exports often UTF-16 LE w/ BOM; handle that and other variants."""
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


def collect_pairs(views_dir: str, dialect: str = "tsql") -> tuple[list[str], list[tuple[str, str]]]:
    """Return (sorted unique table list, sorted unique (table, column) pairs)."""
    in_dir = Path(views_dir)
    if not in_dir.is_dir():
        raise SystemExit(f"Not a directory: {in_dir}")
    sql_files = sorted(in_dir.glob("*.sql"))
    if not sql_files:
        raise SystemExit(f"No .sql files in {in_dir}")

    tables: set[str] = set()
    pairs: set[tuple[str, str]] = set()
    skipped: list[tuple[str, str]] = []
    for path in sql_files:
        try:
            inv = extract_columns(_read_sql_file(path), dialect=dialect)
        except Exception as e:
            skipped.append((path.name, f"{type(e).__name__}: {e}"))
            continue
        for c in inv.columns:
            if not c.table or not c.column:
                continue
            tables.add(c.table)
            pairs.add((c.table, c.column))

    if skipped:
        print("-- WARNING: failed to parse:", file=sys.stderr)
        for name, err in skipped:
            print(f"--   {name}: {err}", file=sys.stderr)

    return sorted(tables), sorted(pairs)


def emit_sql(tables: list[str], pairs: list[tuple[str, str]]) -> str:
    """Format as SSMS-pasteable INSERT blocks."""
    lines: list[str] = []
    lines.append("-- ==========================================================")
    lines.append("-- @Tables INSERT block -- paste into extract_clarity_metadata.sql")
    lines.append("-- ==========================================================")
    lines.append("INSERT INTO @Tables (TABLE_NAME) VALUES")
    table_lines = [f"    ('{t}')" for t in tables]
    lines.append(",\n".join(table_lines) + ";")
    lines.append("")
    lines.append("-- ==========================================================")
    lines.append(f"-- @Columns INSERT block ({len(pairs)} pairs)")
    lines.append("-- ==========================================================")
    lines.append("INSERT INTO @Columns (TABLE_NAME, COLUMN_NAME) VALUES")
    pair_lines = [f"    ('{t}', '{c}')" for t, c in pairs]
    lines.append(",\n".join(pair_lines) + ";")
    return "\n".join(lines)


def build(views_dir: str, dialect: str = "tsql") -> None:
    tables, pairs = collect_pairs(views_dir, dialect=dialect)
    print(emit_sql(tables, pairs))
    print(f"\n-- Summary: {len(tables)} unique tables, {len(pairs)} unique columns",
          file=sys.stderr)


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: build_metadata_extract_sql.py <views_dir> [dialect]", file=sys.stderr)
        return 2
    build(sys.argv[1], dialect=sys.argv[2] if len(sys.argv) > 2 else "tsql")
    return 0


if __name__ == "__main__":
    sys.exit(main())
