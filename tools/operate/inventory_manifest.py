#!/usr/bin/env python3
"""Tool 15 -- inventory manifest batch driver.

Reads a v3 corpus.jsonl and writes manifest files describing every
table and (table, column) pair referenced across the corpus. Used to
narrow the SSMS metadata extracts (extract_clarity_metadata.sql,
extract_zc_values.sql) to just the tables your views actually touch.

Notebook usage:

    from tools.operate.inventory_manifest import build_inventory_manifest
    build_inventory_manifest(
        corpus_path='/lakehouse/default/Files/outputs/corpus.jsonl',
        output_dir='/lakehouse/default/Files/outputs/inventory',
    )

CLI:
    python -m tools.operate.inventory_manifest <corpus.jsonl> [-o out_dir]

Outputs (written to `output_dir`):
  - used_tables.txt           -- newline-separated bare table names
  - used_zc_tables.txt        -- newline-separated ZC_* tables only
  - used_columns.csv          -- table_name,column_name pairs
  - tables_values_clause.sql  -- paste-ready VALUES list for SSMS:
                                   ('TABLE_A'),
                                   ('TABLE_B'),
                                   ...
  - zc_tables_values_clause.sql -- same shape, ZC tables only

Historical note
---------------
This module was previously `tools.inventory_manifest.batch` ("Tool 15
-- inventory manifest"). It was renamed to `tools.operate.inventory_manifest`
as part of the 2026-05 codebase restructure (see `tools/PHASES.md`)
which placed steward-artifact generators under p40_synthesize.

It cuts the upstream SSMS metadata extracts from "scan everything in
CLARITY" to "scan only what your views touch" -- typically a 10-100x
reduction in result-set size and runtime.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path


def _bare(name: str) -> str:
    """Strip database/schema qualifiers, return the bare table name."""
    return (name or "").split(".")[-1].strip()


def _walk_corpus(corpus_path: Path):
    """Yield each ViewV1 dict from a v3 corpus.jsonl (skips header)."""
    with corpus_path.open("r", encoding="utf-8") as f:
        next(f, None)
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def _collect(corpus_path: Path):
    """Walk the corpus and accumulate sets of (tables, columns, zc_tables).

    Filters out CTE / derived-table aliases that show up in the
    inventory as if they were base tables -- per view, we know which
    scope IDs exist (cte:X, derived:Y), so any inventory `table`
    matching a same-view CTE/derived alias gets skipped.

    `zc_tables` is the union of:
      (a) ZC_* tables explicitly READ in any view (joined or in FROM)
      (b) ZC_* tables INFERRED from `<X>_C` column references anywhere
          in the corpus (e.g., `WHERE COVERAGE_TYPE_C = 2` implies
          `ZC_COVERAGE_TYPE` even when the view doesn't JOIN that table).

    The (b) rule matters because Epic Clarity views frequently filter
    on numeric codes without joining the ZC table to dereference the
    name -- they only join when they need to project the label. Without
    this rule, the zc_values.csv that drives ZC code-to-name annotation
    misses every code-only filter predicate.
    """
    tables: set[str] = set()
    columns: set[tuple[str, str]] = set()

    for view in _walk_corpus(corpus_path):
        # Per-view: collect CTE / derived-table names so we can skip
        # them when they leak into the inventory as fake tables.
        scope_aliases: set[str] = set()
        for scope in view.get("scopes") or []:
            sid = scope.get("id") or ""
            if ":" in sid:
                _kind, alias = sid.split(":", 1)
                if alias:
                    scope_aliases.add(alias.upper())

        # Inventory: Tool 1's flat list of every (table, column) referenced
        # anywhere in the view (SELECT, JOIN, WHERE, EXISTS subquery, etc.).
        for inv in view.get("inventory") or []:
            t = _bare(inv.get("table") or "")
            c = (inv.get("column") or "").strip()
            if not t or t.upper() in scope_aliases:
                continue
            tables.add(t)
            if c:
                columns.add((t, c))

        # Scopes: catch tables that are read but whose columns weren't
        # individually inventoried (rare, but defensive).
        for scope in view.get("scopes") or []:
            for t in scope.get("reads_from_tables") or []:
                bare = _bare(t)
                if bare and bare.upper() not in scope_aliases:
                    tables.add(bare)
            # Joins also reference tables (right side)
            for j in scope.get("joins") or []:
                rt = _bare(j.get("right_table") or "")
                # Skip cross-scope refs ("cte:X", "derived:Y") and same-view CTE aliases
                if rt and ":" not in rt and rt.upper() not in scope_aliases:
                    tables.add(rt)

    # Rule (a): ZC tables explicitly present as joined / FROM tables.
    zc_tables: set[str] = {t for t in tables if t.upper().startswith("ZC_")}

    # Rule (b): ZC tables INFERRED from <X>_C column references. Even when
    # the view doesn't JOIN ZC_<X>, a predicate `<X>_C = <N>` implies the
    # ZC table -- it's the lookup target for the code value.
    for _t, col in columns:
        col_upper = col.upper()
        if col_upper.endswith("_C") and len(col_upper) > 2:
            zc_tables.add("ZC_" + col_upper[:-2])

    return tables, columns, zc_tables


def _write_lines(path: Path, items) -> None:
    path.write_text("\n".join(items) + "\n", encoding="utf-8")


def _write_values_clause(path: Path, items, header: str) -> None:
    """Emit a paste-ready T-SQL VALUES list:

        -- header comment
        ('NAME_1'),
        ('NAME_2'),
        ...

    Single quotes inside a name are doubled per T-SQL escape rules.
    """
    lines = [f"-- {header}"]
    if not items:
        lines.append("-- (no tables found in corpus)")
    else:
        for i, name in enumerate(sorted(items)):
            escaped = name.replace("'", "''")
            comma = "," if i < len(items) - 1 else ""
            lines.append(f"    ('{escaped}'){comma}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_inventory_manifest(
    corpus_path: str,
    output_dir: str = "inventory",
) -> int:
    corpus = Path(corpus_path)
    if not corpus.is_file():
        print(f"Error: {corpus} not found", file=sys.stderr)
        return 1

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    tables, columns, zc_tables = _collect(corpus)

    _write_lines(out / "used_tables.txt", sorted(tables))
    _write_lines(out / "used_zc_tables.txt", sorted(zc_tables))

    with (out / "used_columns.csv").open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["table_name", "column_name"])
        for t, c in sorted(columns):
            w.writerow([t, c])

    _write_values_clause(
        out / "tables_values_clause.sql",
        tables,
        "Paste between INSERT INTO @TableList (TABLE_NAME) VALUES and ;",
    )
    _write_values_clause(
        out / "zc_tables_values_clause.sql",
        zc_tables,
        "Paste between INSERT INTO @TableList (TABLE_NAME) VALUES and ;",
    )

    print(f"\ninventory_manifest:")
    print(f"  unique tables:                {len(tables)}")
    print(f"  unique ZC_* tables:           {len(zc_tables)}")
    print(f"  unique (table, column) pairs: {len(columns)}")
    print(f"  -> {out / 'used_tables.txt'}")
    print(f"  -> {out / 'used_zc_tables.txt'}")
    print(f"  -> {out / 'used_columns.csv'}")
    print(f"  -> {out / 'tables_values_clause.sql'}")
    print(f"  -> {out / 'zc_tables_values_clause.sql'}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract used tables/columns/ZCs from a v3 corpus.jsonl."
    )
    parser.add_argument("corpus", help="Path to corpus.jsonl")
    parser.add_argument("-o", "--output", default="inventory",
                          help="Output directory (default: inventory/)")
    args = parser.parse_args()
    return build_inventory_manifest(args.corpus, args.output)


if __name__ == "__main__":
    sys.exit(main())
