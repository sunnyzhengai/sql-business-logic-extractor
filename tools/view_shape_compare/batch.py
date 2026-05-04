#!/usr/bin/env python3
"""Tool 12 -- view-shape comparison batch driver.

Reads a v3 corpus.jsonl and emits side-by-side pair findings between
views that share table+join structure.

Notebook usage:

    from tools.view_shape_compare.batch import compare_view_shapes
    compare_view_shapes(
        corpus_path='/lakehouse/default/Files/outputs/corpus.jsonl',
        output_dir='/lakehouse/default/Files/outputs/view_shapes',
    )

CLI:
    python -m tools.view_shape_compare.batch <corpus.jsonl> [-o out_dir]

Outputs (written to `output_dir`):
  - pairs.json    -- one entry per (view_a, view_b) pair with a finding,
                     each broken down side-by-side: shared / only-in-A /
                     only-in-B for fact tables, dim tables, and joins.
                     Multiple flags per pair are accumulated.
  - features.json -- per-view shape: driver, fact tables, dim tables,
                     joins. Reference for triaging specific pairs.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from dataclasses import asdict
from pathlib import Path

from .clusters import (
    FLAG_DIM_EXTENSION,
    FLAG_FACT_IDENTICAL,
    FLAG_FACT_OVERLAP,
    FLAG_FACT_SUBSET,
    FLAG_FACT_SUPERSET,
    FLAG_JOIN_SUBSET,
    FLAG_SAME_DRIVER,
    FLAG_SAME_FACTS_DIFFERENT_JOINS,
    FLAG_TABLE_IDENTICAL,
    build_clusters,
)
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


# Triage priority: lower number = read first. Used to sort the JSON
# output so the highest-value findings come first per file.
_FLAG_PRIORITY = {
    FLAG_TABLE_IDENTICAL:               1,
    FLAG_DIM_EXTENSION:                 2,
    FLAG_SAME_FACTS_DIFFERENT_JOINS:    3,
    FLAG_JOIN_SUBSET:                   4,
    FLAG_FACT_IDENTICAL:                5,
    FLAG_FACT_SUBSET:                   6,
    FLAG_FACT_SUPERSET:                 6,
    FLAG_FACT_OVERLAP:                  7,
    FLAG_SAME_DRIVER:                   8,
}


def _format_join(j) -> str:
    """Render a (right_table, type, on) triple as a one-line string."""
    rt, jt, on = j
    return f"{rt} {jt} ON {on}" if on else f"{rt} {jt}"


def _scope_to_dict(sf) -> dict:
    """Render one ScopeFeature as JSON, with the scope id/kind so the
    reader can see WHERE each table/join came from in the view."""
    return {
        "id": sf.id,
        "kind": sf.kind,
        "fact_tables": sorted(sf.fact_tables),
        "dim_tables": sorted(sf.tables - sf.fact_tables),
        "fact_joins": [_format_join(j) for j in sorted(sf.fact_joins)],
        "all_joins": [_format_join(j) for j in sorted(sf.joins)],
    }


def _pair_diff(a: ViewShape, b: ViewShape, flags: list[str]) -> dict:
    """Side-by-side breakdown for one pair. Each axis shows what's
    shared, what's only in A, and what's only in B. Per-scope detail
    follows so readers can see CTE structure on both sides."""
    a_dims = a.all_tables - a.fact_tables
    b_dims = b.all_tables - b.fact_tables
    a_fjoins = set(a.fact_joins)
    b_fjoins = set(b.fact_joins)
    a_alljoins = set(a.all_joins)
    b_alljoins = set(b.all_joins)
    return {
        "view_a": a.view_name,
        "view_b": b.view_name,
        "flags": sorted(set(flags)),
        "fact_tables": {
            "shared": sorted(a.fact_tables & b.fact_tables),
            "only_a": sorted(a.fact_tables - b.fact_tables),
            "only_b": sorted(b.fact_tables - a.fact_tables),
        },
        "dim_tables": {
            "shared": sorted(a_dims & b_dims),
            "only_a": sorted(a_dims - b_dims),
            "only_b": sorted(b_dims - a_dims),
        },
        "fact_joins": {
            "shared": sorted(_format_join(j) for j in a_fjoins & b_fjoins),
            "only_a": sorted(_format_join(j) for j in a_fjoins - b_fjoins),
            "only_b": sorted(_format_join(j) for j in b_fjoins - a_fjoins),
        },
        "all_joins": {
            "shared": sorted(_format_join(j) for j in a_alljoins & b_alljoins),
            "only_a": sorted(_format_join(j) for j in a_alljoins - b_alljoins),
            "only_b": sorted(_format_join(j) for j in b_alljoins - a_alljoins),
        },
        "drivers": {
            "a": a.driver_table,
            "b": b.driver_table,
            "same": bool(a.driver_table) and a.driver_table == b.driver_table,
        },
        # Full scope tree on each side. Reader sees which CTE each
        # table/join came from, so two views with similar aggregates
        # but different scope decomposition are still inspectable.
        "scopes_a": [_scope_to_dict(s) for s in a.scopes],
        "scopes_b": [_scope_to_dict(s) for s in b.scopes],
    }


def _pair_priority(pair: dict) -> tuple:
    """Sort key: best (lowest) flag priority first, then alphabetic by
    (view_a, view_b) for stable cross-run diffs."""
    if not pair["flags"]:
        best = 99
    else:
        best = min(_FLAG_PRIORITY.get(f, 99) for f in pair["flags"])
    return (best, pair["view_a"], pair["view_b"])


def compare_view_shapes(
    corpus_path: str,
    output_dir: str = "view_shapes",
    *,
    dim_filter_path: str | None = None,
    min_members: int = 2,
) -> int:
    """Build the side-by-side pair-finding report from a v3 corpus.jsonl.

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

    dim_filter = (
        DimFilter.from_file(dim_filter_path) if dim_filter_path
        else load_default_dim_filter()
    )

    shapes: list[ViewShape] = []
    n_skipped = 0
    for view_dict in _read_corpus(corpus):
        shape = view_shape_from_dict(view_dict, dim_filter)
        if shape is None:
            n_skipped += 1
            continue
        shapes.append(shape)

    report = build_clusters(shapes, min_members=min_members)

    # Pair-level findings. Three sources, all merged into one stream:
    #   1. table_identical members (every C(n,2) pair within a cluster)
    #   2. cross-cluster pair flags (one CrossClusterPair per (a,b,flag);
    #      multiple can apply to the same pair, so merge by (a,b) key).
    #   3. cluster pairs that are NOT in cross_pairs (only flag is
    #      table_identical -- captured in step 1).
    pairs_by_key: dict[tuple[str, str], list[str]] = defaultdict(list)

    for cluster in report.clusters:
        members = cluster.members
        for i in range(len(members)):
            for j in range(i + 1, len(members)):
                a_name = members[i].view_name
                b_name = members[j].view_name
                key = tuple(sorted([a_name, b_name]))
                pairs_by_key[key].append(FLAG_TABLE_IDENTICAL)

    for cp in report.cross_pairs:
        key = tuple(sorted([cp.a, cp.b]))
        pairs_by_key[key].append(cp.flag)

    shapes_by_name = {s.view_name: s for s in shapes}
    pair_dicts: list[dict] = []
    for (na, nb), flags in pairs_by_key.items():
        a = shapes_by_name.get(na)
        b = shapes_by_name.get(nb)
        if a is None or b is None:
            continue
        pair_dicts.append(_pair_diff(a, b, flags))
    pair_dicts.sort(key=_pair_priority)

    pairs_doc = {
        "schema_version": 1,
        "n_views": len(shapes),
        "n_views_skipped": n_skipped,
        "n_pairs": len(pair_dicts),
        "flag_priority_doc": (
            "Sort key: pairs are ordered by best (lowest-numbered) flag. "
            "1=table_identical, 2=dim_extension, 3=same_facts_different_joins, "
            "4=join_subset, 5=fact_identical, 6=fact_subset/superset, "
            "7=fact_overlap, 8=same_driver."
        ),
        "pairs": pair_dicts,
    }
    (out_dir / "pairs.json").write_text(
        json.dumps(pairs_doc, indent=2), encoding="utf-8"
    )

    features_doc = {
        "schema_version": 1,
        "n_views": len(shapes),
        "views": [_feature_dict(s) for s in
                   sorted(shapes, key=lambda s: s.view_name)],
    }
    (out_dir / "features.json").write_text(
        json.dumps(features_doc, indent=2), encoding="utf-8"
    )

    flag_counts: dict[str, int] = defaultdict(int)
    for p in pair_dicts:
        for f in p["flags"]:
            flag_counts[f] += 1

    print(f"\nview_shape_compare:")
    print(f"  views inspected: {len(shapes)} (skipped {n_skipped} with no main scope)")
    print(f"  pairs emitted:   {len(pair_dicts)}")
    print(f"  flag breakdown:  ")
    for flag in sorted(flag_counts, key=lambda f: _FLAG_PRIORITY.get(f, 99)):
        print(f"    {flag}: {flag_counts[flag]}")
    print(f"  -> {out_dir / 'pairs.json'}")
    print(f"  -> {out_dir / 'features.json'}")
    return 0


def _feature_dict(s: ViewShape) -> dict:
    return {
        "view_name": s.view_name,
        "driver_table": s.driver_table,
        "n_scopes": len(s.scopes),
        "n_facts": len(s.fact_tables),
        "n_dims": len(s.all_tables) - len(s.fact_tables),
        "n_joins": len(s.all_joins),
        # Aggregate (union across all scopes)
        "fact_tables": sorted(s.fact_tables),
        "dim_tables": sorted(s.all_tables - s.fact_tables),
        "fact_joins": [_format_join(j) for j in sorted(s.fact_joins)],
        "all_joins": [_format_join(j) for j in sorted(s.all_joins)],
        # Per-scope decomposition (ordered as the corpus stores them:
        # main first, then CTEs/derived/subqueries)
        "scopes": [_scope_to_dict(sf) for sf in s.scopes],
    }


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
