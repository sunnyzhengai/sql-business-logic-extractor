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
    errors: list[dict] = field(default_factory=list)   # [{file, error}]
    objects: list[dict] = field(default_factory=list)   # [{file, name, schema, columns}]
    elapsed_seconds: float = 0.0


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
                continue

            # Process
            resolved = resolve_query(sql.strip(), dialect=dialect)
            translated = translate_resolved(sql.strip(), dialect=dialect)

            resolved_dict = resolved_to_dict(resolved)
            obj_name = resolved_dict.get("name", os.path.splitext(fname)[0])
            obj_schema = resolved_dict.get("schema", config.schema)
            obj_type = resolved_dict.get("object_type", "")
            col_count = len(resolved.columns)

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
            result.errors.append({"file": fname, "error": str(e)})
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
    """Write a summary report."""
    summary = {
        "total_files": result.total,
        "succeeded": result.succeeded,
        "failed": result.failed,
        "skipped": result.skipped,
        "elapsed_seconds": result.elapsed_seconds,
        "objects": result.objects,
    }
    if result.errors:
        summary["errors"] = result.errors

    path = os.path.join(output_dir, "batch_summary.json")
    with open(path, "w") as fh:
        json.dump(summary, fh, indent=2)


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

    if result.errors:
        print(f"\nFailed files:")
        for err in result.errors:
            print(f"  {err['file']}: {err['error']}")

    if result.succeeded > 0:
        print(f"\nOutput files:")
        print(f"  {args.output_dir}/collibra_glossary.csv")
        print(f"  {args.output_dir}/collibra_lineage.json")
        print(f"  {args.output_dir}/collibra_dictionary.csv")
        print(f"  {args.output_dir}/batch_summary.json")
        print(f"  {args.output_dir}/per_object/<name>.json")


if __name__ == "__main__":
    main()
