#!/usr/bin/env python3
"""Tool 12 -- view-shape comparison batch driver.

Reads a v3 corpus.jsonl and emits a clustering report flagging views
with similar / identical / overlapping table+join shapes.

Notebook usage:

    from tools.view_shape_compare.batch import compare_view_shapes
    compare_view_shapes(
        corpus_path='/lakehouse/default/Files/outputs/corpus.jsonl',
        output_dir='/lakehouse/default/Files/outputs/view_shapes',
    )

CLI:
    python -m tools.view_shape_compare.batch <corpus.jsonl> [-o out_dir]

Outputs (written to `output_dir`):
  - clusters.csv          -- one row per cluster, with member views + flags
  - cross_pairs.csv       -- asymmetric cross-cluster relationships
  - features.csv          -- per-view shape features (driver, facts, joins)
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

from .clusters import build_clusters
from .dim_filter import DimFilter, load_default_dim_filter
from .features import ViewShape, view_shape_from_dict


def _read_corpus(corpus_path: Path):
    """Yield each ViewV1 dict from a v3 corpus.jsonl. Skips the header."""
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


def compare_view_shapes(
    corpus_path: str,
    output_dir: str = "view_shapes",
    *,
    dim_filter_path: str | None = None,
    min_members: int = 2,
) -> int:
    """Build the shape report from a v3 corpus.jsonl.

    `dim_filter_path` defaults to the project's
    data/dictionaries/dim_tables.txt. Pass an alternate path to use a
    custom dim list.
    """
    corpus = Path(corpus_path)
    if not corpus.is_file():
        print(f"Error: {corpus} not found", file=sys.stderr)
        return 1

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if dim_filter_path:
        dim_filter = DimFilter.from_file(dim_filter_path)
    else:
        dim_filter = load_default_dim_filter()

    shapes: list[ViewShape] = []
    n_skipped = 0
    for view_dict in _read_corpus(corpus):
        shape = view_shape_from_dict(view_dict, dim_filter)
        if shape is None:
            n_skipped += 1
            continue
        shapes.append(shape)

    report = build_clusters(shapes, min_members=min_members)

    _write_features(out_dir / "features.csv", shapes)
    _write_clusters(out_dir / "clusters.csv", report.clusters)
    _write_cross_pairs(out_dir / "cross_pairs.csv", report.cross_pairs)

    print(f"\nview_shape_compare:")
    print(f"  views inspected:    {len(shapes)} (skipped {n_skipped} with no main scope)")
    print(f"  clusters emitted:   {len(report.clusters)}")
    print(f"  cross-cluster pairs:{len(report.cross_pairs)}")
    print(f"  -> {out_dir / 'clusters.csv'}")
    print(f"  -> {out_dir / 'cross_pairs.csv'}")
    print(f"  -> {out_dir / 'features.csv'}")
    return 0


def _write_features(path: Path, shapes):
    fields = [
        "view_name", "driver_table", "n_facts", "n_dims", "n_joins",
        "fact_tables", "all_tables", "fact_joins",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for s in shapes:
            w.writerow({
                "view_name": s.view_name,
                "driver_table": s.driver_table,
                "n_facts": len(s.fact_tables),
                "n_dims": len(s.all_tables) - len(s.fact_tables),
                "n_joins": len(s.all_joins),
                "fact_tables": ", ".join(sorted(s.fact_tables)),
                "all_tables": ", ".join(sorted(s.all_tables)),
                "fact_joins": "; ".join(
                    f"{rt} {jt} ON {on}" for rt, jt, on in sorted(s.fact_joins)
                ),
            })


def _write_clusters(path: Path, clusters):
    fields = [
        "cluster_id", "n_members", "all_tables", "fact_tables",
        "flags", "members",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for i, c in enumerate(clusters, 1):
            all_tables_sig, _all_joins_sig = c.cluster_key
            sample = c.members[0]
            w.writerow({
                "cluster_id": i,
                "n_members": len(c.members),
                "all_tables": ", ".join(all_tables_sig),
                "fact_tables": ", ".join(sorted(sample.fact_tables)),
                "flags": ", ".join(sorted(c.flags)),
                "members": "; ".join(m.view_name for m in c.members),
            })


def _write_cross_pairs(path: Path, pairs):
    fields = ["view_a", "view_b", "flag", "detail"]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for p in pairs:
            w.writerow({"view_a": p.a, "view_b": p.b, "flag": p.flag, "detail": p.detail})


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Cluster views by table+join shape from a v3 corpus.jsonl."
    )
    parser.add_argument("corpus", help="Path to corpus.jsonl")
    parser.add_argument("-o", "--output", default="view_shapes",
                          help="Output directory (default: view_shapes/)")
    parser.add_argument("--dim-filter", default=None,
                          help="Path to dim_tables.txt (default: project default)")
    parser.add_argument("--min-members", type=int, default=2,
                          help="Minimum cluster size (default: 2). Set to 1 "
                                "to include singleton clusters.")
    args = parser.parse_args()
    return compare_view_shapes(
        args.corpus, args.output,
        dim_filter_path=args.dim_filter,
        min_members=args.min_members,
    )


if __name__ == "__main__":
    sys.exit(main())
