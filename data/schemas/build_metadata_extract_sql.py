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

import re
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


# Regex fallback for views Tool 1 can't parse (e.g. heavy T-SQL hints,
# unusual procedural syntax). Catches qualified column refs in three
# common T-SQL forms:
#   1. ALIAS.COLUMN          -- bare identifiers
#   2. ALIAS.[COLUMN]        -- bracket-quoted column
#   3. [ALIAS].[COLUMN]      -- both bracket-quoted
# This OVER-collects (matches table aliases, not real names) but extras
# are harmless: the SSMS metadata query just won't find a row for them.
_QUAL_COL_RE = re.compile(
    r"(?<!\w)"
    r"(?:\[(?P<t1>\w+)\]|(?P<t2>\w+))"
    r"\.(?:\[(?P<c1>\w+)\]|(?P<c2>\w+))"
    r"(?!\w)"
)

# Keywords that look like identifiers but aren't table aliases.
_NOT_AN_ALIAS = {
    "DBO", "SYS", "INFORMATION_SCHEMA",
    "SELECT", "FROM", "WHERE", "JOIN", "ON", "AND", "OR", "AS", "WHEN",
    "THEN", "ELSE", "END", "CASE", "WITH", "ORDER", "BY", "GROUP", "HAVING",
    "INNER", "OUTER", "LEFT", "RIGHT", "FULL", "CROSS", "ROW_NUMBER",
    "OVER", "PARTITION", "BETWEEN", "IN", "EXISTS", "NOT", "NULL", "IS",
    "ROW", "RANK", "DENSE_RANK",
}


def _regex_fallback_pairs(sql: str) -> set[tuple[str, str]]:
    """Extract (table, column) pairs from raw SQL when sqlglot can't parse.
    Filters out obvious non-table tokens but otherwise lets extras through;
    the downstream SSMS query drops misses silently."""
    pairs: set[tuple[str, str]] = set()
    for m in _QUAL_COL_RE.finditer(sql):
        t = (m.group("t1") or m.group("t2") or "").upper()
        c = m.group("c1") or m.group("c2") or ""
        if not t or not c or t in _NOT_AN_ALIAS:
            continue
        pairs.add((t, c))
    return pairs


def collect_pairs(views_dir: str, dialect: str = "tsql") -> tuple[list[str], list[tuple[str, str]]]:
    """Return (sorted unique table list, sorted unique (table, column) pairs).

    For any view Tool 1 can't parse, falls back to a regex extraction so
    those views still contribute identifiers to the metadata extract.
    """
    in_dir = Path(views_dir)
    if not in_dir.is_dir():
        raise SystemExit(f"Not a directory: {in_dir}")
    sql_files = sorted(in_dir.glob("*.sql"))
    if not sql_files:
        raise SystemExit(f"No .sql files in {in_dir}")

    tables: set[str] = set()
    pairs: set[tuple[str, str]] = set()
    parsed_ok = 0
    fell_back: list[str] = []

    for path in sql_files:
        sql = _read_sql_file(path)
        try:
            inv = extract_columns(sql, dialect=dialect)
            for c in inv.columns:
                if not c.table or not c.column:
                    continue
                tables.add(c.table)
                pairs.add((c.table, c.column))
            parsed_ok += 1
        except Exception:
            # Fallback: regex extraction. ALIAS.COLUMN qualified pairs only.
            recovered = _regex_fallback_pairs(sql)
            for t, c in recovered:
                tables.add(t)
                pairs.add((t, c))
            fell_back.append(f"{path.name} ({len(recovered)} pairs via regex)")

    if fell_back:
        print(f"-- NOTE: {len(fell_back)} view(s) used regex fallback "
              f"(sqlglot couldn't parse): ", file=sys.stderr)
        for entry in fell_back:
            print(f"--   {entry}", file=sys.stderr)
        print("-- The fallback pairs may include aliases that are not real "
              "table names; harmless -- they just won't match in CLARITY_TBL "
              "or sys.* and get dropped.", file=sys.stderr)

    print(f"-- Parsed cleanly: {parsed_ok} / {len(sql_files)} views",
          file=sys.stderr)
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
