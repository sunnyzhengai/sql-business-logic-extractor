#!/usr/bin/env python3
"""Tool 10 -- corpus-level term extractor (Phase D, scope-correct).

For each *.sql in a folder, runs the scope-correct resolver, walks the
main-output scope's columns, and emits Terms (the governance comparison
unit). Each Term's `filters` are scope-local (declared in the column's
own scope only), NOT the cross-scope flattened union the legacy
extractor produced.

CTE-internal columns are NOT emitted as Terms by default -- they're not
user-visible and don't participate in cross-view governance comparison.
Pass `all_scopes=True` to include them (each Term then has its scope ID
in `view_name` for disambiguation).

Output:
    terms.json     -- list of Term records, one per qualifying column
    terms.csv      -- the same data flattened for spreadsheet review

Notebook usage:

    from tools.term_extraction.batch import extract_corpus_terms
    extract_corpus_terms(
        input_dir='/lakehouse/default/Files/views_healthy',
        output_path='/lakehouse/default/Files/outputs/terms.json',
    )

CLI:
    python -m tools.term_extraction.batch <input_dir> [-o terms.json]
"""

import argparse
import csv
import json
import sys
import time
from pathlib import Path

from sql_logic_extractor.business_logic import load_schema
from sql_logic_extractor.extract import SQLBusinessLogicExtractor, to_dict
from sql_logic_extractor.resolve import LineageResolver, preprocess_ssms
from sql_logic_extractor.term_extraction import (
    Term,
    extract_terms,
    load_default_synonyms,
)


def _read_sql_file(path: Path) -> str:
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


def _scoped_terms_for_view(
    view_name: str,
    sql: str,
    *,
    dialect: str,
    synonyms,
    all_scopes: bool,
) -> list[Term]:
    """Build Terms from a view's scope tree.

    Per-scope: pick up that scope's columns, build the dict shape the
    Term extractor expects, and pass the SCOPE's own filters as
    query_filters (no cross-scope inheritance).

    By default, only the scopes named in `view_outputs` (typically
    `main`) contribute Terms. With `all_scopes=True`, every scope
    contributes -- useful for governance audits that care about
    intermediate CTE shapes.
    """
    # Strip SSMS script boilerplate (SET ANSI_NULLS, GO, header comments)
    # so sqlglot sees a parseable CREATE VIEW / SELECT statement.
    clean_sql, _meta = preprocess_ssms(sql)
    if not clean_sql or not clean_sql.strip():
        clean_sql = sql.strip()

    extractor = SQLBusinessLogicExtractor(dialect=dialect)
    logic = to_dict(extractor.extract(clean_sql))
    tree = LineageResolver(logic).resolve_all_scoped()

    target_scope_ids = (
        {s.id for s in tree.scopes}
        if all_scopes
        else set(tree.view_outputs or ["main"])
    )

    terms: list[Term] = []
    for scope in tree.scopes:
        if scope.id not in target_scope_ids:
            continue

        col_dicts = [{
            "column_name": c.name,
            "column_type": c.type,
            "resolved_expression": c.resolved_expression or c.expression,
            "base_tables": list(c.base_tables),
            "base_columns": list(c.base_columns),
        } for c in scope.columns]
        scope_filters = tuple(f.expression for f in scope.filters)

        # Disambiguate intermediate-scope terms by suffixing the scope ID
        # to view_name so cross-view comparison still groups by the actual
        # view but downstream readers can tell them apart.
        scope_view_name = view_name if scope.id == "main" else f"{view_name}#{scope.id}"
        terms.extend(extract_terms(
            view_name=scope_view_name,
            column_translations=col_dicts,
            query_filters=scope_filters,
            synonyms=synonyms,
        ))

    return terms


def extract_corpus_terms(
    input_dir: str,
    output_path: str = "terms.json",
    *,
    schema_path: str | None = None,
    dialect: str = "tsql",
    all_scopes: bool = False,
) -> int:
    """Walk a folder of views, build scope-correct Terms, write JSON + CSV."""
    in_dir = Path(input_dir)
    if not in_dir.is_dir():
        print(f"Error: {in_dir} is not a directory", file=sys.stderr)
        return 1
    sql_files = sorted(in_dir.glob("*.sql"))
    if not sql_files:
        print(f"Error: no .sql files in {in_dir}", file=sys.stderr)
        return 1

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    csv_out = out.with_suffix(".csv")

    # schema_path is accepted for API compatibility; the scope-correct
    # path doesn't use it (column English isn't part of the Term, and
    # filter scoping doesn't depend on the data dictionary).
    if schema_path:
        load_schema(schema_path)
    synonyms = load_default_synonyms()

    all_terms: list[Term] = []
    n_views_ok = 0
    n_views_failed = 0
    t_start = time.time()

    for i, path in enumerate(sql_files, 1):
        view_name = path.stem
        try:
            sql = _read_sql_file(path)
            terms = _scoped_terms_for_view(
                view_name, sql,
                dialect=dialect, synonyms=synonyms, all_scopes=all_scopes,
            )
            all_terms.extend(terms)
            n_views_ok += 1
            print(f"[{i}/{len(sql_files)}] {view_name}: {len(terms)} term(s)",
                  flush=True)
        except Exception as e:
            n_views_failed += 1
            print(f"[{i}/{len(sql_files)}] {view_name}: ERROR "
                  f"{type(e).__name__}: {str(e)[:100]}", flush=True)

    out.write_text(json.dumps([t.to_dict() for t in all_terms], indent=2))

    csv_fields = [
        "view_name", "column_name", "column_type",
        "name_tokens", "is_passthrough", "has_filters", "name_is_structural",
        "base_tables", "base_columns",
        "resolved_expression",
        "filters", "author_notes",
    ]
    with csv_out.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields)
        writer.writeheader()
        for t in all_terms:
            writer.writerow({
                "view_name":            t.view_name,
                "column_name":          t.column_name,
                "column_type":          t.column_type,
                "name_tokens":          ", ".join(sorted(t.name_tokens)),
                "is_passthrough":       t.is_passthrough,
                "has_filters":          t.has_filters,
                "name_is_structural":   t.name_is_structural,
                "base_tables":          ", ".join(t.base_tables),
                "base_columns":         ", ".join(t.base_columns),
                "resolved_expression":  t.resolved_expression,
                "filters":              " | ".join(t.filters),
                "author_notes":         " | ".join(t.author_notes),
            })

    elapsed = time.time() - t_start
    print(f"\nExtracted {len(all_terms)} term(s) from "
          f"{n_views_ok}/{len(sql_files)} views in {elapsed:.1f}s")
    if n_views_failed:
        print(f"  ({n_views_failed} view(s) failed -- see ERROR lines above)")
    print(f"  -> {out}")
    print(f"  -> {csv_out}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract scope-correct Terms from a folder of SQL views."
    )
    parser.add_argument("input_dir", help="Folder containing view *.sql files")
    parser.add_argument("-o", "--output", default="terms.json",
                          help="Output JSON path (also writes <stem>.csv)")
    parser.add_argument("--schema", default=None,
                          help="(Accepted for API compatibility; not used.)")
    parser.add_argument("-d", "--dialect", default="tsql")
    parser.add_argument("--all-scopes", action="store_true",
                          help="Emit Terms from intermediate scopes (CTEs, "
                                "subqueries) too. Default: only view outputs.")
    args = parser.parse_args()
    return extract_corpus_terms(
        args.input_dir, args.output,
        schema_path=args.schema, dialect=args.dialect,
        all_scopes=args.all_scopes,
    )


def _is_notebook() -> bool:
    return "ipykernel" in sys.argv[0] or "ipykernel" in " ".join(sys.argv[1:])


if __name__ == "__main__":
    if _is_notebook():
        print("Notebook environment detected -- call extract_corpus_terms("
              "input_dir=..., output_path=...) from a cell.")
    else:
        sys.exit(main())
