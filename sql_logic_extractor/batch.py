#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL Business Logic Extractor -- Batch Processing

Scan a directory of SQL files (views, procs, functions) and produce
combined Collibra-compatible exports plus per-object resolved output.

Usage (CLI):
    python -m sql_logic_extractor.batch ./sql_files/ -o ./output/
    python -m sql_logic_extractor.batch ./sql_files/*.sql --dialect tsql

Usage (API):
    from sql_logic_extractor.batch import batch_process
    report = batch_process("./sql_files/", dialect="tsql", output_dir="./output/")
"""

import csv
import glob as globmod
import io
import json
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Optional

from .collibra import (
    CollibraConfig,
    GLOSSARY_COLUMNS,
    DICTIONARY_COLUMNS,
    _build_glossary_rows,
    _build_lineage_edges,
    _build_dictionary_rows,
)
from .resolve import resolve_query, resolved_to_dict, ResolvedQuery
from .translate import translate_resolved


# ---------------------------------------------------------------------------
# Batch result
# ---------------------------------------------------------------------------

@dataclass
class BatchResult:
    """Summary of a batch processing run."""
    total: int = 0
    succeeded: int = 0
    failed: int = 0
    skipped: int = 0
    errors: list[dict] = field(default_factory=list)   # [{file, error, category, action}]
    objects: list[dict] = field(default_factory=list)   # [{file, name, schema, columns}]
    elapsed_seconds: float = 0.0


# ---------------------------------------------------------------------------
# Failure classification
# ---------------------------------------------------------------------------

# Each category maps to (human label, recommended action)
_FAILURE_CATEGORIES = {
    "parse_error": (
        "SQL Parse Error",
        "Check the SQL dialect setting (--dialect). If correct, this SQL uses "
        "syntax not yet supported by the parser. Save the file for review.",
    ),
    "empty_input": (
        "Empty or Blank File",
        "No action needed. File contains no SQL.",
    ),
    "comments_only": (
        "Comments Only",
        "No action needed. File contains only comments, no executable SQL.",
    ),
    "no_columns": (
        "No Output Columns",
        "This is likely DML (INSERT/UPDATE/DELETE/MERGE) or DDL (CREATE TABLE), "
        "not a query. No business definitions to extract.",
    ),
    "recursion": (
        "Recursion / Infinite Loop",
        "The SQL has circular CTE references or extremely deep nesting that "
        "exceeded the resolver's depth limit. Simplify the query or break it "
        "into smaller parts.",
    ),
    "encoding": (
        "File Encoding Error",
        "File is not valid UTF-8. Re-save with UTF-8 encoding.",
    ),
    "unknown": (
        "Unexpected Error",
        "An unclassified error occurred. This pattern may need a new handler. "
        "Save the file and error details for review.",
    ),
}


def _classify_error(error: Exception, sql: str = "") -> tuple[str, str, str]:
    """Classify an error into category, label, and recommended action.

    Returns (category_key, label, action).
    """
    msg = str(error).lower()
    etype = type(error).__name__

    if isinstance(error, ValueError):
        if "empty" in msg:
            return "empty_input", *_FAILURE_CATEGORIES["empty_input"]
        if "parse" in msg or "unparsable" in msg:
            return "parse_error", *_FAILURE_CATEGORIES["parse_error"]

    if isinstance(error, RecursionError) or "recursion" in msg or "depth" in msg:
        return "recursion", *_FAILURE_CATEGORIES["recursion"]

    if isinstance(error, (UnicodeDecodeError, UnicodeError)):
        return "encoding", *_FAILURE_CATEGORIES["encoding"]

    if "parse" in msg or "token" in msg or "unexpected" in msg or "invalid expression" in msg:
        return "parse_error", *_FAILURE_CATEGORIES["parse_error"]

    return "unknown", *_FAILURE_CATEGORIES["unknown"]


# ---------------------------------------------------------------------------
# Core batch logic
# ---------------------------------------------------------------------------

def batch_process(
    path: str,
    dialect: str = "tsql",
    output_dir: str = None,
    collibra_config: CollibraConfig = None,
    pattern: str = "*.sql",
) -> BatchResult:
    """Process all SQL files matching pattern under path.

    Args:
        path: Directory or glob pattern (e.g. "./views/*.sql")
        dialect: SQL dialect for parsing
        output_dir: Where to write output files (optional)
        collibra_config: Collibra export settings (optional)
        pattern: Glob pattern for finding files when path is a directory

    Returns:
        BatchResult with success/failure counts and per-object details.
    """
    config = collibra_config or CollibraConfig(dialect=dialect)
    config.dialect = dialect

    # Collect input files
    if os.path.isdir(path):
        files = sorted(globmod.glob(os.path.join(path, "**", pattern), recursive=True))
    else:
        files = sorted(globmod.glob(path))

    if not files:
        print(f"No files found matching: {path}", file=sys.stderr)
        return BatchResult()

    result = BatchResult(total=len(files))
    start = time.time()

    # Accumulators for combined Collibra exports
    all_glossary_rows = []
    all_lineage_edges = []
    all_dictionary_rows = []
    all_resolved = []

    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(os.path.join(output_dir, "per_object"), exist_ok=True)

    for i, fpath in enumerate(files, 1):
        fname = os.path.basename(fpath)
        try:
            with open(fpath) as fh:
                sql = fh.read()

            if not sql.strip():
                result.skipped += 1
                cat, label, action = _FAILURE_CATEGORIES["empty_input"][0], *_FAILURE_CATEGORIES["empty_input"]
                result.errors.append({
                    "file": fname, "category": "empty_input",
                    "label": label, "action": action, "error": "empty file",
                })
                _progress(i, len(files), fname, error="empty file")
                continue

            # Skip files that are only comments (no actual SQL)
            lines = [l.strip() for l in sql.strip().splitlines()
                     if l.strip() and not l.strip().startswith("--")]
            if not lines:
                result.skipped += 1
                cat, label, action = "comments_only", *_FAILURE_CATEGORIES["comments_only"]
                result.errors.append({
                    "file": fname, "category": cat,
                    "label": label, "action": action, "error": "comments only",
                })
                _progress(i, len(files), fname, error="comments only, no SQL")
                continue

            # Process
            resolved = resolve_query(sql.strip(), dialect=dialect)

            resolved_dict = resolved_to_dict(resolved)
            obj_name = resolved_dict.get("name", os.path.splitext(fname)[0])
            obj_schema = resolved_dict.get("schema", config.schema)
            obj_type = resolved_dict.get("object_type", "")
            col_count = len(resolved.columns)

            if col_count == 0:
                result.skipped += 1
                label, action = _FAILURE_CATEGORIES["no_columns"]
                result.errors.append({
                    "file": fname, "category": "no_columns",
                    "label": label, "action": action,
                    "error": "no output columns",
                })
                _progress(i, len(files), obj_name or fname, error="no output columns (DML/DDL?)")
                continue

            translated = translate_resolved(sql.strip(), dialect=dialect)

            # Accumulate Collibra rows
            glossary_rows = _build_glossary_rows(translated, resolved, config, obj_name)
            lineage_edges = _build_lineage_edges(resolved, config, obj_name)
            dictionary_rows = _build_dictionary_rows(resolved, translated, config)

            all_glossary_rows.extend(glossary_rows)
            all_lineage_edges.extend(lineage_edges)
            all_dictionary_rows.extend(dictionary_rows)

            # Per-object output
            if output_dir:
                per_obj = {
                    "name": obj_name,
                    "schema": obj_schema,
                    "type": obj_type,
                    "source_file": fname,
                    "columns": resolved_dict.get("columns", []),
                    "translations": translated,
                }
                obj_path = os.path.join(
                    output_dir, "per_object", f"{obj_name}.json"
                )
                with open(obj_path, "w") as fh:
                    json.dump(per_obj, fh, indent=2)

            result.succeeded += 1
            result.objects.append({
                "file": fname,
                "name": obj_name,
                "schema": obj_schema,
                "type": obj_type,
                "columns": col_count,
            })

            _progress(i, len(files), obj_name, col_count)

        except Exception as e:
            result.failed += 1
            cat, label, action = _classify_error(e, sql if 'sql' in dir() else "")
            result.errors.append({
                "file": fname, "category": cat,
                "label": label, "action": action,
                "error": str(e)[:500],
            })
            _progress(i, len(files), fname, error=str(e))

    result.elapsed_seconds = round(time.time() - start, 2)

    # Write combined Collibra exports
    if output_dir:
        _write_combined_exports(
            output_dir, all_glossary_rows, all_lineage_edges, all_dictionary_rows,
        )
        _write_summary(output_dir, result)

    return result


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------

def _write_combined_exports(
    output_dir: str,
    glossary_rows: list[dict],
    lineage_edges: list[dict],
    dictionary_rows: list[dict],
):
    """Write combined Collibra import files."""
    # Glossary CSV
    if glossary_rows:
        path = os.path.join(output_dir, "collibra_glossary.csv")
        with open(path, "w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=GLOSSARY_COLUMNS)
            writer.writeheader()
            writer.writerows(glossary_rows)

    # Lineage JSON
    if lineage_edges:
        path = os.path.join(output_dir, "collibra_lineage.json")
        with open(path, "w") as fh:
            json.dump({"edges": lineage_edges}, fh, indent=2)

    # Dictionary CSV
    if dictionary_rows:
        path = os.path.join(output_dir, "collibra_dictionary.csv")
        with open(path, "w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=DICTIONARY_COLUMNS)
            writer.writeheader()
            writer.writerows(dictionary_rows)


def _write_summary(output_dir: str, result: BatchResult):
    """Write summary report and, if there are failures, a separate action items file."""
    # Group errors by category for the summary
    errors_by_category = {}
    for err in result.errors:
        cat = err.get("category", "unknown")
        if cat not in errors_by_category:
            errors_by_category[cat] = {
                "label": err.get("label", cat),
                "action": err.get("action", ""),
                "count": 0,
                "files": [],
            }
        errors_by_category[cat]["count"] += 1
        errors_by_category[cat]["files"].append({
            "file": err["file"],
            "error": err.get("error", ""),
        })

    summary = {
        "total_files": result.total,
        "succeeded": result.succeeded,
        "failed": result.failed,
        "skipped": result.skipped,
        "elapsed_seconds": result.elapsed_seconds,
        "objects": result.objects,
    }
    if errors_by_category:
        summary["errors_by_category"] = errors_by_category

    path = os.path.join(output_dir, "batch_summary.json")
    with open(path, "w") as fh:
        json.dump(summary, fh, indent=2)

    # Write a separate action items file for failures that need attention
    actionable = {
        cat: info for cat, info in errors_by_category.items()
        if cat not in ("empty_input", "comments_only", "no_columns")
    }
    if actionable:
        _write_action_items(output_dir, actionable, result)


def _write_action_items(output_dir: str, actionable: dict, result: BatchResult):
    """Write a human-readable action items report for failures that need review."""
    lines = [
        "BATCH PROCESSING -- ACTION ITEMS",
        "=" * 50,
        "",
        f"Run: {result.succeeded} succeeded, {result.failed} failed, "
        f"{result.skipped} skipped out of {result.total}",
        "",
    ]

    priority = 1
    needs_review = []

    for cat, info in actionable.items():
        lines.append(f"--- [{priority}] {info['label']} ({info['count']} file(s)) ---")
        lines.append(f"Action: {info['action']}")
        lines.append("Files:")
        for f in info["files"]:
            lines.append(f"  - {f['file']}")
            if f.get("error"):
                # Truncate long errors for readability
                err_short = f["error"][:200]
                lines.append(f"    Error: {err_short}")
            needs_review.append(f["file"])
        lines.append("")
        priority += 1

    if needs_review:
        lines.append("=" * 50)
        lines.append("FILES NEEDING REVIEW (copy this list):")
        for f in needs_review:
            lines.append(f"  {f}")

    path = os.path.join(output_dir, "action_items.txt")
    with open(path, "w") as fh:
        fh.write("\n".join(lines) + "\n")

    # Also save the raw SQL of failed files for pattern analysis
    failed_patterns = []
    for err in result.errors:
        if err.get("category") in ("empty_input", "comments_only", "no_columns"):
            continue
        failed_patterns.append({
            "file": err["file"],
            "category": err.get("category"),
            "error": err.get("error", ""),
        })

    if failed_patterns:
        path = os.path.join(output_dir, "failed_patterns.json")
        with open(path, "w") as fh:
            json.dump(failed_patterns, fh, indent=2)


def _progress(i: int, total: int, name: str, columns: int = 0, error: str = None):
    """Print progress line."""
    if error:
        print(f"  [{i}/{total}] FAIL  {name}: {error}", file=sys.stderr)
    else:
        print(f"  [{i}/{total}] OK    {name} ({columns} columns)")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Batch process SQL files and generate Collibra exports",
    )
    parser.add_argument(
        "path",
        help="Directory of SQL files or glob pattern (e.g. './views/*.sql')",
    )
    parser.add_argument("--output-dir", "-o", default="./batch_output",
                        help="Output directory (default: ./batch_output)")
    parser.add_argument("--dialect", "-d", default="tsql",
                        help="SQL dialect (default: tsql)")
    parser.add_argument("--pattern", "-p", default="*.sql",
                        help="File pattern when path is a directory (default: *.sql)")
    parser.add_argument("--glossary-domain", default=None,
                        help="Collibra glossary domain path")
    parser.add_argument("--catalog-domain", default=None,
                        help="Collibra catalog domain path")
    parser.add_argument("--database", default="",
                        help="Database name for lineage references")
    parser.add_argument("--schema", default="dbo",
                        help="Default schema (default: dbo)")

    args = parser.parse_args()

    config = CollibraConfig(dialect=args.dialect, schema=args.schema, database=args.database)
    if args.glossary_domain:
        config.glossary_domain = args.glossary_domain
    if args.catalog_domain:
        config.catalog_domain = args.catalog_domain

    print(f"Scanning: {args.path}")
    print(f"Output:   {args.output_dir}")
    print(f"Dialect:  {args.dialect}")
    print()

    result = batch_process(
        path=args.path,
        dialect=args.dialect,
        output_dir=args.output_dir,
        collibra_config=config,
        pattern=args.pattern,
    )

    print()
    print(f"Done in {result.elapsed_seconds}s: "
          f"{result.succeeded} succeeded, {result.failed} failed, "
          f"{result.skipped} skipped out of {result.total}")

    # Group and display errors by category
    if result.errors:
        from collections import Counter
        cats = Counter(e.get("category", "unknown") for e in result.errors)
        print(f"\nIssues by category:")
        for cat, count in cats.most_common():
            label = _FAILURE_CATEGORIES.get(cat, ("Unknown",))[0]
            print(f"  {label}: {count} file(s)")

        # Show actionable errors (not benign skips)
        actionable = [e for e in result.errors
                      if e.get("category") not in ("empty_input", "comments_only", "no_columns")]
        if actionable:
            print(f"\n  ** {len(actionable)} file(s) need review -- see action_items.txt **")

    if result.succeeded > 0:
        print(f"\nOutput files:")
        print(f"  {args.output_dir}/collibra_glossary.csv")
        print(f"  {args.output_dir}/collibra_lineage.json")
        print(f"  {args.output_dir}/collibra_dictionary.csv")
        print(f"  {args.output_dir}/batch_summary.json")
        print(f"  {args.output_dir}/per_object/<name>.json")
        if any(e.get("category") not in ("empty_input", "comments_only", "no_columns")
               for e in result.errors):
            print(f"  {args.output_dir}/action_items.txt")
            print(f"  {args.output_dir}/failed_patterns.json")


if __name__ == "__main__":
    main()
