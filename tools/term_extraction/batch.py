#!/usr/bin/env python3
"""Tool 10 -- corpus-level term extractor.

For each *.sql in a folder, runs the resolver, walks the per-column
translations, and emits Terms (the governance comparison unit).

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
from sql_logic_extractor.products import (
    extract_business_logic,
    extract_technical_lineage,
)
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


def extract_corpus_terms(
    input_dir: str,
    output_path: str = "terms.json",
    *,
    schema_path: str | None = None,
    dialect: str = "tsql",
) -> int:
    """Walk a folder of views, build Terms, write JSON + CSV."""
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

    schema = load_schema(schema_path) if schema_path else {}
    synonyms = load_default_synonyms()

    all_terms: list[Term] = []
    n_views_ok = 0
    n_views_failed = 0
    t_start = time.time()

    for i, path in enumerate(sql_files, 1):
        view_name = path.stem
        try:
            sql = _read_sql_file(path)
            # Tool 3 path gives us per-column translations with author_notes,
            # base_tables/columns, and filters already attached. Use it
            # rather than raw lineage so terms inherit Tool 3's enrichment.
            bl = extract_business_logic(sql, schema, use_llm=False, dialect=dialect)
            lineage = bl.lineage
            terms = extract_terms(
                view_name=view_name,
                column_translations=bl.column_translations,
                query_filters=lineage.query_filters,
                synonyms=synonyms,
            )
            all_terms.extend(terms)
            n_views_ok += 1
            print(f"[{i}/{len(sql_files)}] {view_name}: {len(terms)} term(s)",
                  flush=True)
        except Exception as e:
            n_views_failed += 1
            print(f"[{i}/{len(sql_files)}] {view_name}: ERROR "
                  f"{type(e).__name__}: {str(e)[:100]}", flush=True)

    # Write JSON (full structured records).
    out.write_text(json.dumps([t.to_dict() for t in all_terms], indent=2))

    # Write CSV (flattened, for spreadsheet review).
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
        description="Extract Terms from a folder of SQL views."
    )
    parser.add_argument("input_dir", help="Folder containing view *.sql files")
    parser.add_argument("-o", "--output", default="terms.json",
                          help="Output JSON path (also writes <stem>.csv)")
    parser.add_argument("--schema", default=None, help="Schema YAML/JSON for Tool 3 enrichment")
    parser.add_argument("-d", "--dialect", default="tsql")
    args = parser.parse_args()
    return extract_corpus_terms(args.input_dir, args.output,
                                   schema_path=args.schema, dialect=args.dialect)


def _is_notebook() -> bool:
    return "ipykernel" in sys.argv[0] or "ipykernel" in " ".join(sys.argv[1:])


if __name__ == "__main__":
    if _is_notebook():
        print("Notebook environment detected -- call extract_corpus_terms("
              "input_dir=..., output_path=...) from a cell.")
    else:
        sys.exit(main())
