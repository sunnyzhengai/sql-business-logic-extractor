#!/usr/bin/env python3
"""Cohort extractor entry-point.

Reads a v3 corpus.jsonl, renders each scope as a cohort + filters
(population-level governance description), writes JSON + MD.

Notebook usage:

    from tools.p40_synthesize.cohort_extract import extract_cohorts
    extract_cohorts(
        corpus_path='/lakehouse/default/Files/outputs/corpus.jsonl',
        output_dir='/lakehouse/default/Files/outputs/cohorts',
    )

CLI:
    python -m tools.p40_synthesize.cohort_extract <corpus.jsonl> [-o out_dir]
                                                   [--table-descriptions YAML]
                                                   [--dim-filter PATH]

Historical note
---------------
This module was previously `tools.cohort_extract.batch` ("Tool 14 --
cohort extractor batch driver"). It was renamed to
`tools.p40_synthesize.cohort_extract` as part of the 2026-05 codebase
restructure (see `tools/PHASES.md`) which placed steward-artifact
generators under p40_synthesize. The pure-function renderer lives at
`tools.p40_synthesize.cohort_render` (formerly `tools.cohort_extract.render`).

As of Phase 1e of the restructure, the `dim_filter` dependency lives
at `tools.shared.dim_filter` (formerly `tools.view_shape_compare.dim_filter`).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from tools.shared.dim_filter import (
    DEFAULT_DIM_FILTER_PATH,
    DimFilter,
    load_default_dim_filter,
)

# Relative import: cohort_render.py lives next to this file inside p40_synthesize.
# Renamed from the original `render.py` so the file name is descriptive at the
# phase-folder level (where multiple tools sit side by side).
from .cohort_render import (
    TableDescriptions,
    cohorts_to_markdown,
    view_to_cohorts,
)


_DEFAULT_TABLE_DESCRIPTIONS_PATH = (
    Path(__file__).resolve().parents[2]
    / "data" / "dictionaries" / "table_short_descriptions.yaml"
)


def _read_corpus(corpus_path: Path):
    with corpus_path.open("r", encoding="utf-8") as f:
        header_line = next(f, None)
        if not header_line:
            return
        header = json.loads(header_line)
        if "schema_version" not in header:
            raise ValueError(
                f"{corpus_path} is not a corpus.jsonl: header lacks schema_version"
            )
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def _load_table_descriptions(
    schema_path: str | None,
    yaml_overlay_path: str | None,
) -> TableDescriptions:
    """Build the table-description lookup from two sources, in priority order:

      1. clarity_schema.json (`tables[*].short_description`, sourced from
         the `TABLE_SHORT_DESCRIPTION` column of clarity_metadata.csv).
      2. The YAML overlay -- for custom tables not in the Clarity schema.

    Schema wins on conflict. Either source may be absent."""
    sources: list[TableDescriptions] = []

    # YAML loaded FIRST (so schema overrides it via merge).
    yaml_p = Path(yaml_overlay_path) if yaml_overlay_path else _DEFAULT_TABLE_DESCRIPTIONS_PATH
    if yaml_p.is_file():
        try:
            sources.append(TableDescriptions.from_yaml(yaml_p))
        except Exception as e:
            print(f"WARNING: could not load table descriptions YAML from {yaml_p}: {e}",
                   file=sys.stderr)

    if schema_path:
        schema_p = Path(schema_path)
        if schema_p.is_file():
            try:
                sources.append(TableDescriptions.from_schema_path(schema_p))
            except Exception as e:
                print(f"WARNING: could not load schema descriptions from {schema_p}: {e}",
                       file=sys.stderr)

    if not sources:
        return TableDescriptions.empty()
    return TableDescriptions.merge(*sources)


def _dim_predicates(dim_filter: DimFilter) -> list:
    """Convert DimFilter into a list of predicates the cohort renderer
    can apply directly (avoids tight coupling between modules)."""
    return [
        lambda name, df=dim_filter: df.is_dim(name),
    ]


def extract_cohorts(
    corpus_path: str,
    output_dir: str = "cohorts",
    *,
    schema_path: str | None = None,
    table_descriptions_path: str | None = None,
    dim_filter_path: str | None = None,
) -> int:
    """Render each scope of each view as a cohort + filters.

    `schema_path` is the clarity_schema.json built from
    clarity_metadata.csv -- this is the PRIMARY source for table
    short_descriptions used in the cohort phrase. `table_descriptions_path`
    is an optional YAML overlay for tables not in the Clarity schema
    (custom fact tables / views); the schema wins on conflict.
    """
    corpus = Path(corpus_path)
    if not corpus.is_file():
        print(f"Error: {corpus} not found", file=sys.stderr)
        return 1

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    descriptions = _load_table_descriptions(schema_path, table_descriptions_path)
    dim_filter = (
        DimFilter.from_file(dim_filter_path) if dim_filter_path
        else load_default_dim_filter()
    )
    dim_preds = _dim_predicates(dim_filter)

    json_views: list[dict] = []
    md_blocks: list[str] = ["# Cohorts per view\n"]
    n_views = 0
    n_skipped = 0

    for view in _read_corpus(corpus):
        view_name = view.get("view_name") or ""
        scopes = view.get("scopes") or []
        if not scopes:
            n_skipped += 1
            continue
        cohorts = view_to_cohorts(view, descriptions, dim_preds)
        json_views.append({"view_name": view_name, "cohorts": cohorts})
        md_blocks.append(cohorts_to_markdown(view_name, cohorts))
        n_views += 1

    json_doc = {
        "schema_version": 1,
        "n_views": n_views,
        "n_skipped": n_skipped,
        "views": json_views,
    }
    (out_dir / "cohorts.json").write_text(
        json.dumps(json_doc, indent=2), encoding="utf-8"
    )
    (out_dir / "cohorts.md").write_text(
        "\n".join(md_blocks), encoding="utf-8"
    )

    n_with_cohort = sum(1 for v in json_views
                          for c in v["cohorts"] if c["cohort"])
    n_total_cohorts = sum(len(v["cohorts"]) for v in json_views)

    print(f"\ncohort_extract:")
    print(f"  views rendered: {n_views} (skipped {n_skipped} with no scopes)")
    print(f"  cohorts emitted: {n_total_cohorts} "
          f"({n_with_cohort} with a populated cohort phrase)")
    print(f"  -> {out_dir / 'cohorts.json'}")
    print(f"  -> {out_dir / 'cohorts.md'}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Render each view's scopes as cohort + filters."
    )
    parser.add_argument("corpus", help="Path to corpus.jsonl")
    parser.add_argument("-o", "--output", default="cohorts",
                          help="Output directory (default: cohorts/)")
    parser.add_argument("--schema", default=None,
                          help="Path to clarity_schema.json (built from "
                                "clarity_metadata.csv). PRIMARY source for "
                                "table_short_description and column "
                                "short_description.")
    parser.add_argument("--table-descriptions", default=None,
                          help="YAML overlay for tables NOT in the Clarity "
                                "schema (custom fact tables / views). "
                                "Defaults to data/dictionaries/table_short_descriptions.yaml.")
    parser.add_argument("--dim-filter", default=None,
                          help="dim_tables.txt to suppress enrichment joins. "
                                "Defaults to data/dictionaries/dim_tables.txt.")
    args = parser.parse_args()
    return extract_cohorts(
        args.corpus, args.output,
        schema_path=args.schema,
        table_descriptions_path=args.table_descriptions,
        dim_filter_path=args.dim_filter,
    )


if __name__ == "__main__":
    sys.exit(main())
