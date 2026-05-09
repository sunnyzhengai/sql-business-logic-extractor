#!/usr/bin/env python3
"""Tool 16 -- view-similarity clustering batch driver.

Reads a v3 corpus.jsonl, builds per-view structural signatures, and
emits clusters at four hierarchical levels (L1=subject, L2=grain,
L3=projections, L4=rows).

Notebook usage:

    from tools.similarity.batch import extract_similarity_clusters
    extract_similarity_clusters(
        corpus_path='/lakehouse/default/Files/outputs/corpus.jsonl',
        output_dir='/lakehouse/default/Files/outputs/similarity',
    )

CLI:

    python -m tools.similarity.batch <corpus.jsonl> [-o out_dir]

Outputs (written to `output_dir`):
  - clusters_L1.json + clusters_L1.md   subject-area clusters
  - clusters_L2.json + clusters_L2.md   grain clusters (within each L1)
  - clusters_L3.json + clusters_L3.md   projection clusters (within each L2)
  - clusters_L4.json + clusters_L4.md   row-set duplicates (within each L3)
  - features.json                       per-view structural signature
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

from .clusters import Cluster, SimilarityReport, build_clusters
from .signatures import ViewSignature, build_view_signature


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


def _signature_to_dict(s: ViewSignature) -> dict:
    return {
        "view_name": s.view_name,
        "driver": s.driver,
        "all_tables": sorted(s.all_tables),
        "joined_set": sorted(s.joined_set),
        "join_types": [{"table": t, "type": jt} for t, jt in s.join_types],
        "n_projections": len(s.projections),
        "projections": sorted(s.projections),
        "n_filters": len(s.filters),
        "filters": sorted(s.filters),
    }


def _cluster_to_dict(c: Cluster) -> dict:
    return {
        "cluster_id": c.cluster_id,
        "level": c.level,
        "n_members": len(c.members),
        "members": list(c.members),
        "signature": c.signature,
        "join_type_consistency": c.join_type_consistency,
        "join_types_per_member": c.join_types_per_member,
        "next_level_split": c.next_level_split,
    }


def _cluster_to_md(c: Cluster) -> str:
    """Render one cluster as a markdown section."""
    sig = c.signature
    lines = [
        f"### `{c.cluster_id}` -- {c.level} ({len(c.members)} members)"
    ]

    # Signature summary
    if c.level == "L1":
        drivers = sig.get("drivers") or []
        subject = sig.get("subject_tables") or []
        lines.append(f"- **Drivers across members:** {', '.join(drivers) if drivers else '(none)'}")
        lines.append(f"- **Subject tables (intersection):** {', '.join(subject) if subject else '(none)'}")
    else:
        lines.append(f"- **Driver:** `{sig.get('driver', '')}`")
        if "joined_set" in sig:
            joined = sig["joined_set"]
            lines.append(f"- **Joined tables:** {', '.join(joined) if joined else '(none)'}")
        if "projections" in sig and c.level in ("L3", "L4"):
            n = len(sig["projections"])
            sample = ", ".join(sig["projections"][:5])
            more = f" (+{n - 5} more)" if n > 5 else ""
            lines.append(f"- **Projections ({n}):** {sample}{more}")
        if "filters" in sig and c.level == "L4":
            n = len(sig["filters"])
            lines.append(f"- **Filters ({n}):**")
            for f in sig["filters"][:10]:
                lines.append(f"    - `{f[:160]}`")
            if n > 10:
                lines.append(f"    - ... +{n - 10} more")

    lines.append(f"- **Members:**")
    for m in c.members:
        lines.append(f"    - `{m}`")

    # Join-type consistency
    if c.join_type_consistency != "n/a":
        lines.append(f"- **Join-type consistency:** {c.join_type_consistency}")
        if c.join_type_consistency == "mixed":
            lines.append(f"  Per-member join types:")
            for view_name, types in c.join_types_per_member.items():
                lines.append(f"    - `{view_name}`: {', '.join(types) if types else '(none)'}")

    # Next-level split (skip for L4)
    splits = c.next_level_split
    if splits:
        next_level = {"L1": "L2", "L2": "L3", "L3": "L4"}.get(c.level, "")
        lines.append(f"- **Splits into {len(splits)} {next_level} sub-cluster(s):**")
        for sp in splits:
            ms = sp.get("members") or []
            lines.append(f"    - {len(ms)} member(s): " + ", ".join(f"`{m}`" for m in ms[:5])
                          + (f" (+{len(ms) - 5} more)" if len(ms) > 5 else ""))

    return "\n".join(lines)


def _write_level_outputs(out_dir: Path, level: str, clusters: list[Cluster]) -> None:
    json_doc = {
        "schema_version": 1,
        "level": level,
        "n_clusters": len(clusters),
        "clusters": [_cluster_to_dict(c) for c in clusters],
    }
    (out_dir / f"clusters_{level}.json").write_text(
        json.dumps(json_doc, indent=2), encoding="utf-8"
    )

    md_lines = [
        f"# Similarity clusters at {level}",
        "",
        f"{len(clusters)} cluster(s) with at least 2 members.",
        "",
    ]
    for c in clusters:
        md_lines.append(_cluster_to_md(c))
        md_lines.append("")
    (out_dir / f"clusters_{level}.md").write_text(
        "\n".join(md_lines), encoding="utf-8"
    )


def _write_features(out_dir: Path, sigs: tuple[ViewSignature, ...]) -> None:
    doc = {
        "schema_version": 1,
        "n_views": len(sigs),
        "views": [_signature_to_dict(s) for s in
                   sorted(sigs, key=lambda s: s.view_name)],
    }
    (out_dir / "features.json").write_text(
        json.dumps(doc, indent=2), encoding="utf-8"
    )


def extract_similarity_clusters(
    corpus_path: str,
    output_dir: str = "similarity",
    *,
    min_members: int = 2,
) -> int:
    corpus = Path(corpus_path)
    if not corpus.is_file():
        print(f"Error: {corpus} not found", file=sys.stderr)
        return 1

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    signatures: list[ViewSignature] = []
    for view in _read_corpus(corpus):
        signatures.append(build_view_signature(view))

    report = build_clusters(signatures, min_members=min_members)

    for level in ("L1", "L2", "L3", "L4"):
        _write_level_outputs(out_dir, level, report.by_level[level])
    _write_features(out_dir, report.signatures)

    print(f"\nsimilarity:")
    print(f"  views inspected:      {len(signatures)}")
    print(f"  L1 clusters (subject):    {len(report.by_level['L1'])}")
    print(f"  L2 clusters (grain):      {len(report.by_level['L2'])}")
    print(f"  L3 clusters (projections):{len(report.by_level['L3'])}")
    print(f"  L4 clusters (row-set):    {len(report.by_level['L4'])}")
    for level in ("L4", "L3", "L2", "L1"):
        print(f"  -> {out_dir / f'clusters_{level}.md'}")
    print(f"  -> {out_dir / 'features.json'}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Cluster views by 4-level structural similarity from a v3 corpus.jsonl."
    )
    parser.add_argument("corpus", help="Path to corpus.jsonl")
    parser.add_argument("-o", "--output", default="similarity",
                          help="Output directory (default: similarity/)")
    parser.add_argument("--min-members", type=int, default=2,
                          help="Minimum cluster size (default: 2). Set to 1 "
                                "to include singleton clusters in output.")
    args = parser.parse_args()
    return extract_similarity_clusters(
        args.corpus, args.output, min_members=args.min_members,
    )


if __name__ == "__main__":
    sys.exit(main())
