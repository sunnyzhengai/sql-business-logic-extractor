#!/usr/bin/env python3
"""Tool 13 -- dataset extractor batch driver.

Reads a v3 corpus.jsonl, walks each view's scope tree, and emits two
files describing each view as a chain of datasets (one per scope):

  datasets.json -- programmatic, structured (one entry per view, each
                   containing an ordered list of datasets)
  datasets.md   -- human-readable; one section per view with the same
                   dataset chain rendered as markdown.

Notebook usage:

    from tools.dataset_extract.batch import extract_datasets
    extract_datasets(
        corpus_path='/lakehouse/default/Files/outputs/corpus.jsonl',
        output_dir='/lakehouse/default/Files/outputs/datasets',
    )

CLI:
    python -m tools.dataset_extract.batch <corpus.jsonl> [-o out_dir]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .render import (
    datasets_to_json_dict,
    datasets_to_markdown,
    view_to_datasets,
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


def extract_datasets(
    corpus_path: str,
    output_dir: str = "datasets",
) -> int:
    corpus = Path(corpus_path)
    if not corpus.is_file():
        print(f"Error: {corpus} not found", file=sys.stderr)
        return 1

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    json_views: list[dict] = []
    md_blocks: list[str] = ["# Datasets per view\n"]
    n_views = 0
    n_skipped = 0

    for view in _read_corpus(corpus):
        view_name = view.get("view_name") or ""
        scopes = view.get("scopes") or []
        if not scopes:
            n_skipped += 1
            continue
        datasets = view_to_datasets(view)
        json_views.append(datasets_to_json_dict(view_name, datasets))
        md_blocks.append(datasets_to_markdown(view_name, datasets))
        n_views += 1

    json_doc = {
        "schema_version": 1,
        "n_views": n_views,
        "n_skipped": n_skipped,
        "views": json_views,
    }
    (out_dir / "datasets.json").write_text(
        json.dumps(json_doc, indent=2), encoding="utf-8"
    )
    (out_dir / "datasets.md").write_text(
        "\n".join(md_blocks), encoding="utf-8"
    )

    print(f"\ndataset_extract:")
    print(f"  views rendered: {n_views} (skipped {n_skipped} with no scopes)")
    print(f"  -> {out_dir / 'datasets.json'}")
    print(f"  -> {out_dir / 'datasets.md'}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Render each view in a corpus.jsonl as a chain of datasets."
    )
    parser.add_argument("corpus", help="Path to corpus.jsonl")
    parser.add_argument("-o", "--output", default="datasets",
                          help="Output directory (default: datasets/)")
    args = parser.parse_args()
    return extract_datasets(args.corpus, args.output)


if __name__ == "__main__":
    sys.exit(main())
