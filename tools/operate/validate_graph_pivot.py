"""Graph-pivot validation orchestrator (ops sidecar to the pipeline).

This module wires the pipeline phases (p10_extract -> p20_index ->
p30_analyze -> p40_synthesize -> p50_present) together to produce a
*validation experiment*: load a corpus, build the graph, run analysis,
emit artifacts, and write a PASS / INCONCLUSIVE / REVIEW NEEDED verdict.

It is NOT itself a pipeline phase. It lives in `tools/operate/` because
its audience is BI devs and admins answering "is the pipeline producing
sensible output on this corpus?" -- not stewards making governance
decisions. The validation experiment was the artifact that got the
graph-pivot architecture decision made (PASS verdict on the user's
real 130-view corpus in May 2026).

What this script does
---------------------
1. Loads corpus.jsonl                              (tools.shared.corpus_io)
2. Filters infrastructure views                    (tools.shared.view_filter)
3. Builds the unified typed graph                  (tools.p20_index.graph_builder)
4. Extracts the table-projection subgraph          (tools.p30_analyze.projection)
5. Detects bridge tables; projects them out        (tools.p30_analyze.bridges)
6. Runs Louvain community detection                (tools.p30_analyze.communities)
7. Assigns views to primary communities + spans    (tools.p30_analyze.primary_community)
8. Summarizes each community                        (tools.p30_analyze.community_analysis)
9. Writes per-community HTML + overview + index    (tools.p50_present.community_html)
10. Writes the steward-style markdown summary       (tools.p40_synthesize.community_summary)
11. Writes the validation report (verdict + recs)  (this module, write_validation_report)

The orchestrator (`run_validation`) does steps 1-11 in order. The
verdict logic (`write_validation_report`) is the only piece of unique
"diagnostic" code that stays here -- it's specific to this operational
question ("is the pivot justified?"), not part of the production
pipeline.

How to run
----------
From the repo root, on a small local sample:
    python -m tools.operate.validate_graph_pivot \\
        my_notes/bi_complex_sample/corpus.jsonl /tmp/graph_pivot_validation

In a Fabric notebook, against a real corpus:
    from tools.operate.validate_graph_pivot import run_validation
    run_validation(
        corpus_path="/lakehouse/default/Files/corpus/corpus.jsonl",
        output_dir="/lakehouse/default/Files/graph_pivot_validation",
    )

Output artifacts (in `output_dir`):
  - graph.html                                 corpus overview, colored by community
  - communities/community_NN_<top>.html        per-community focused HTMLs
  - communities/index.html                     linking page for the above
  - communities.md                             per-community steward-readable markdown
  - validation_report.md                       PASS / INCONCLUSIVE / REVIEW NEEDED verdict

Historical note
---------------
Before Phase 2 of the 2026-05 restructure, this file was monolithic
(~1100 lines): graph construction, projection, bridges, communities,
primary-community, community-analysis, HTML renderers, and the
markdown writer were all defined inline. Each piece was extracted
to its production home over Phases 2a-2d:

  Phase 2a  shared/{corpus_io, table_names, view_filter}
  Phase 2b  p20_index/graph_builder (replacing graph_explore-era code)
  Phase 2c  p30_analyze/{projection, bridges, communities,
                         primary_community, community_analysis}
  Phase 2d  p40_synthesize/community_summary
            p50_present/community_html

After Phase 2e (docstring polish, this commit), the orchestrator is
~290 lines (down from ~1110): module docstring, imports, the verdict
writer, run_validation, and the CLI. Each piece of pipeline work it
calls is tested in its own phase folder; the only test here is the
end-to-end TestEndToEndOrchestration that exercises the full chain.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable

# Shared utilities (extracted from this file in Phase 2a of the restructure).
from tools.shared.corpus_io import load_corpus
from tools.shared.view_filter import (
    DEFAULT_INFRASTRUCTURE_PATTERNS,
    filter_business_views,
)

# Graph construction lives in p20_index (Phase 2b).
from tools.p20_index.graph_builder import build_graph

# Analysis layer lives in p30_analyze (Phase 2c).
from tools.p30_analyze.bridges import (
    detect_bridge_tables,
    project_without_bridges,
)
from tools.p30_analyze.column_variance import (
    analyze_column_variance,
    count_reconciliation_candidates,
)
from tools.p30_analyze.communities import detect_table_communities
from tools.p30_analyze.community_analysis import analyze_community
from tools.p30_analyze.filter_patterns import (
    analyze_filter_patterns,
    count_filter_patterns,
)
from tools.p30_analyze.join_paths import analyze_join_paths, count_join_edges
from tools.p30_analyze.primary_community import assign_views_to_communities
from tools.p30_analyze.projection import extract_table_projection

# Per-view membership strength + driver detection (Phase 3a).
# view_to_tables added in Phase 3b as a shared helper.
from tools.p30_analyze.view_membership import (
    compute_view_membership_strength,
    view_driver_table,
    view_to_tables,
)

# Synthesis (markdown) lives in p40_synthesize (Phase 2d).
from tools.p40_synthesize.community_summary import write_communities_markdown
# Per-community modeling spec generator (Phase 3e-iii).
from tools.p40_synthesize.community_modeling_spec import write_community_modeling_spec

# Community HTML renderers moved to p50_present in Phase 2d. _safe_filename
# is exposed publicly there (renamed `safe_filename`) for the orchestrator.
from tools.p50_present.community_html import (
    render_communities_index_html,
    render_community_html,
    render_overview_html,
    safe_filename as _safe_filename,
)

# Per-view HTML rendering added Phase 3a.
from tools.p50_present.view_html import render_view_html, view_html_filename


def write_validation_report(
    header: dict,
    g,
    table_g,
    communities: list[set],
    analyses: list[dict],
    n_bridge_tables: int,
    n_excluded_views: int,
    n_cross_domain_views: int,
    output_path: str | Path,
) -> str:
    """Write the verdict / recommendation document.

    This is the artifact that decides whether we pivot the codebase. It is
    intentionally short and human-readable; the underlying data lives in
    communities.md and the per-community HTMLs.
    """
    n_views = header.get("n_views", "unknown")
    n_tables = sum(1 for _, a in g.nodes(data=True) if a.get("ntype") == "table")
    n_communities = len(communities)
    avg_community_size = (sum(len(c) for c in communities) / n_communities
                            if n_communities else 0)

    # The validation criterion: a "healthy" pivot looks like:
    #   - Multiple communities (more than 1, less than n_views)
    #   - Each community has 3-30 tables (recognizable subject area)
    #   - The largest community is not >70% of all tables (no degenerate clustering)
    largest_size = max((len(c) for c in communities), default=0)
    largest_pct = (100.0 * largest_size / n_tables) if n_tables else 0
    healthy_count_range = 2 <= n_communities <= max(2, n_views)
    healthy_size_range = 3 <= avg_community_size <= 30
    not_degenerate = largest_pct < 70

    if healthy_count_range and healthy_size_range and not_degenerate:
        verdict = "PASS"
        recommendation = ("The graph pivot is justified. Communities correspond to "
                          "table neighborhoods of plausible size. Proceed with the "
                          "codebase restructure and full pipeline build-out.")
    elif n_communities <= 1:
        verdict = "INCONCLUSIVE -- too few communities"
        recommendation = ("Louvain found only one community. Either the corpus is "
                          "too small to surface modular structure, or our table "
                          "projection is collapsing real structure. Try running "
                          "with a larger corpus or higher resolution before deciding.")
    elif largest_pct >= 70:
        verdict = "INCONCLUSIVE -- one giant community"
        recommendation = (f"The largest community contains {largest_pct:.0f}% of all "
                          "tables. This usually means PATIENT (or a similar superhub) "
                          "is dragging everything together. Consider downweighting "
                          "edges to high-degree hubs before re-running.")
    else:
        verdict = "REVIEW NEEDED"
        recommendation = ("The structure is non-trivial but does not fit the healthy "
                          "shape we expected. Inspect communities.md and graph.html "
                          "manually; decide whether the structure is healthcare-meaningful "
                          "or noise.")

    lines = []
    lines.append("# Graph-pivot validation report")
    lines.append("")
    lines.append("## Summary statistics")
    lines.append("")
    lines.append(f"- Views ingested: **{n_views}**")
    lines.append(f"- Infrastructure views excluded: **{n_excluded_views}**")
    lines.append(f"- Distinct tables: **{n_tables}**")
    lines.append(f"- Bridge tables (shared dimensions): **{n_bridge_tables}**")
    lines.append(f"- Communities found: **{n_communities}**")
    lines.append(f"- Average community size: **{avg_community_size:.1f}** tables")
    lines.append(f"- Largest community: **{largest_size}** tables "
                  f"(**{largest_pct:.0f}%** of all tables)")
    lines.append(f"- Cross-domain views (span 2+ communities): **{n_cross_domain_views}**")
    lines.append("")
    lines.append("## Verdict")
    lines.append("")
    lines.append(f"**{verdict}**")
    lines.append("")
    lines.append(recommendation)
    lines.append("")
    lines.append("## What to inspect")
    lines.append("")
    lines.append("- `graph.html` -- visualize the table graph, colored by community. ")
    lines.append("  Do the colored groupings correspond to recognizable subject areas?")
    lines.append("  (Epic clinic, inpatient, claims, billing, registry, etc.)")
    lines.append("- `communities.md` -- per-community detail: which tables, which views.")
    lines.append("  Look at the top 3-5 tables of each community and ask: does this name")
    lines.append("  a coherent business domain in your shop?")
    lines.append("")
    lines.append("## How to interpret the verdict")
    lines.append("")
    lines.append("- **PASS** -> proceed with the codebase restructure. Confidence is high.")
    lines.append("- **INCONCLUSIVE** -> investigate the named issue, possibly re-run, then ")
    lines.append("  re-evaluate. Do NOT commit to the restructure yet.")
    lines.append("- **REVIEW NEEDED** -> the algorithms ran cleanly but the result looks")
    lines.append("  unusual. Open the artifacts manually and decide.")

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines), encoding="utf-8")
    return str(out)


# ============================================================================
# SECTION 7 -- Orchestrator
# ============================================================================


def run_validation(
    corpus_path: str | Path,
    output_dir: str | Path,
    resolution: float = 1.0,
    bridge_percentile: float = 90.0,
    exclude_patterns: Iterable[str] | None = None,
) -> dict:
    """Run the full validation pipeline. Returns a dict of output paths + stats.

    This is the entry point you call from a Fabric notebook:

        from tools.operate.validate_graph_pivot import run_validation
        result = run_validation(
            corpus_path="/lakehouse/.../corpus.jsonl",
            output_dir="/lakehouse/.../validation_out",
            resolution=1.0,            # try 0.5 for fewer, broader communities
            bridge_percentile=90.0,    # top 10% by degree are flagged as bridges
            exclude_patterns=None,     # uses DEFAULT_INFRASTRUCTURE_PATTERNS
        )
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    communities_dir = output_dir / "communities"
    communities_dir.mkdir(parents=True, exist_ok=True)

    print(f"[1/8] Loading corpus from {corpus_path}...")
    header, all_views = load_corpus(corpus_path)
    print(f"      Loaded {len(all_views)} views (header says {header.get('n_views', '?')})")

    print("[2/8] Filtering infrastructure views...")
    views, excluded_views = filter_business_views(all_views, exclude_patterns)
    print(f"      Kept {len(views)} business views; excluded {len(excluded_views)}")
    if excluded_views:
        print(f"      Excluded: {', '.join(excluded_views[:10])}"
              f"{'...' if len(excluded_views) > 10 else ''}")

    print("[3/8] Building typed graph...")
    g = build_graph(views)
    n_table = sum(1 for _, a in g.nodes(data=True) if a.get("ntype") == "table")
    print(f"      Graph: {g.number_of_nodes()} nodes, {g.number_of_edges()} edges, "
          f"{n_table} distinct tables")

    print("[4/8] Extracting table-projection subgraph...")
    table_g = extract_table_projection(g)
    print(f"      Projection: {table_g.number_of_nodes()} tables, "
          f"{table_g.number_of_edges()} weighted edges")

    print(f"[5/8] Detecting bridge tables (top {100 - bridge_percentile:.0f}% by degree)...")
    bridge_nodes = detect_bridge_tables(table_g, percentile=bridge_percentile)
    bridge_labels = sorted(g.nodes[b].get("label", b) for b in bridge_nodes if b in g)
    print(f"      Bridge tables: {len(bridge_nodes)}")
    if bridge_labels:
        preview = ", ".join(bridge_labels[:8])
        print(f"      Examples: {preview}{'...' if len(bridge_labels) > 8 else ''}")
    projection_for_louvain = project_without_bridges(table_g, bridge_nodes)

    print(f"[6/8] Running Louvain community detection (resolution={resolution})...")
    communities = detect_table_communities(projection_for_louvain, resolution=resolution)
    print(f"      Found {len(communities)} communities, "
          f"sizes (top 10): {sorted([len(c) for c in communities], reverse=True)[:10]}")

    print("[7/8] Assigning views to primary communities + computing membership strength...")
    community_to_primary, view_to_spans = assign_views_to_communities(g, communities)
    cross_domain = [v for v, spans in view_to_spans.items() if len(spans) > 1]
    print(f"      Cross-domain views: {len(cross_domain)}")

    # Phase 3a: per-view membership strength + driver-table detection.
    # Used downstream by both the markdown summary (strong/weak split) and
    # the per-view HTMLs (driver gets a starred shape).
    view_strength = compute_view_membership_strength(g, communities)
    # Phase 3b: also compute view->tables map for Option B rendering
    # (view nodes embedded in each community HTML with click-to-highlight).
    tables_per_view = view_to_tables(g)
    # Phase 3e-i: per-community column-variance analysis. Identifies
    # (column_name, source_tables) groups with >= 2 distinct fingerprints
    # across the community's primary views -- the modeling team's
    # reconciliation candidates. Stashed here; rendered into the
    # community modeling spec in Phase 3e-iii.
    column_variance = analyze_column_variance(views, community_to_primary)
    n_reconciliation = count_reconciliation_candidates(column_variance)
    print(f"      Reconciliation candidates (columns with multi-definition variance): {n_reconciliation}")
    # Phase 3e-ii: per-community common JOIN edges + filter patterns.
    join_paths = analyze_join_paths(g, community_to_primary)
    n_join_edges = count_join_edges(join_paths)
    print(f"      Distinct JOIN edges across communities: {n_join_edges}")
    filter_patterns = analyze_filter_patterns(views, community_to_primary)
    n_common_filters = count_filter_patterns(filter_patterns, min_views=2)
    print(f"      Common filters (>=2 views in a community): {n_common_filters}")
    # Driver detection -- only do it for views that have a primary community,
    # since those are the ones we'll render and report on.
    views_to_describe = set(view_to_spans.keys())
    view_to_driver = {v: view_driver_table(g, v) for v in views_to_describe}
    n_weak = sum(
        1 for v, strengths in view_strength.items()
        if strengths and max(strengths.values()) < 0.5
    )
    print(f"      Weak members (< 50% of tables in primary community): {n_weak}")

    # Per-community analysis using primary-view assignments only.
    analyses = []
    for community_index, member_set in enumerate(communities):
        analyses.append(analyze_community(
            g, member_set, community_to_primary.get(community_index, set()),
        ))

    print("[8/8] Writing artifacts...")
    # Per-community HTMLs -- Phase 3b: each community HTML now includes
    # view nodes (one per primary-member-view) that you can click to
    # highlight the view's subgraph in place.
    community_html_files: list[tuple[int, str, str, int, int]] = []
    for community_index, (community_set, analysis) in enumerate(zip(communities, analyses)):
        # Name the per-community HTML after its top table (most connected within the community).
        top_label = analysis["top_tables"][0][0] if analysis["top_tables"] else f"community_{community_index}"
        safe = _safe_filename(top_label)
        fname = f"community_{community_index:02d}_{safe}.html"
        # The list of views to embed: this community's primary views.
        primary_views = sorted(community_to_primary.get(community_index, set()))
        render_community_html(
            table_g, community_index, community_set, bridge_nodes,
            communities_dir / fname,
            primary_views=primary_views,
            view_to_tables_map=tables_per_view,
        )
        community_html_files.append((
            community_index, top_label, fname,
            analysis["n_tables"], analysis["n_primary_views"],
        ))

    # Phase 3a: per-view HTMLs (one per view, colored by community membership,
    # driver table highlighted). Indexed under communities_dir/views/.
    views_dir = communities_dir / "views"
    views_dir.mkdir(parents=True, exist_ok=True)
    # Build the per-community list of (view_name, html_filename) tuples for
    # the index page. Order within each community: strong members first
    # (descending by strength), then weak (ascending by strength).
    view_html_files_by_community: dict[int, list[tuple[str, str]]] = {}
    for community_index, primary_set in community_to_primary.items():
        # Order primary views: strong first (high to low fraction),
        # then weak (low to high) -- weak members surface at the bottom.
        scored = [
            (v, view_strength.get(v, {}).get(community_index, 0.0))
            for v in primary_set
        ]
        strong = sorted([(v, f) for v, f in scored if f >= 0.5],
                         key=lambda kv: kv[1], reverse=True)
        weak = sorted([(v, f) for v, f in scored if f < 0.5],
                       key=lambda kv: kv[1])
        ordered = [v for v, _ in strong + weak]
        # Render each view's HTML; record its filename for the index.
        files_for_this_community: list[tuple[str, str]] = []
        for view_name in ordered:
            fname = view_html_filename(view_name)
            render_view_html(
                g=g,
                view_name=view_name,
                communities=communities,
                bridge_tables=bridge_nodes,
                output_path=views_dir / fname,
                driver_label=view_to_driver.get(view_name),
            )
            # The index lives one level above views/, so its links are
            # relative paths.
            files_for_this_community.append((view_name, f"views/{fname}"))
        view_html_files_by_community[community_index] = files_for_this_community

    index_html = render_communities_index_html(
        community_html_files,
        communities_dir / "index.html",
        view_html_files=view_html_files_by_community,
    )
    overview_html = render_overview_html(table_g, communities, bridge_nodes,
                                            output_dir / "graph.html")

    # Build bridge -> communities-it-touches map for the markdown report.
    bridge_to_neighbor_communities: dict[str, list[int]] = {}
    label_for_node = lambda n: g.nodes[n].get("label", n) if n in g else n
    for bridge in bridge_nodes:
        touched: set[int] = set()
        if bridge in table_g:
            for neighbor in table_g.neighbors(bridge):
                for community_index, member_set in enumerate(communities):
                    if neighbor in member_set:
                        touched.add(community_index)
        bridge_to_neighbor_communities[label_for_node(bridge)] = sorted(touched)

    communities_md = write_communities_markdown(
        communities, analyses,
        bridge_table_labels=bridge_labels,
        bridge_to_neighbor_communities=bridge_to_neighbor_communities,
        view_to_spans=view_to_spans,
        excluded_infrastructure_views=excluded_views,
        output_path=output_dir / "communities.md",
        # Phase 3a: strength + driver -> strong / weak split per community.
        view_strength=view_strength,
        view_to_driver=view_to_driver,
    )

    report_md = write_validation_report(
        header, g, table_g, communities, analyses,
        n_bridge_tables=len(bridge_nodes),
        n_excluded_views=len(excluded_views),
        n_cross_domain_views=len(cross_domain),
        output_path=output_dir / "validation_report.md",
    )

    # Phase 3e-iii: per-community modeling specs. One markdown per
    # community, the handoff artifact for the data modeling team.
    specs_dir = output_dir / "modeling_specs"
    specs_dir.mkdir(parents=True, exist_ok=True)
    spec_paths: list[str] = []
    for community_index, analysis in enumerate(analyses):
        top_label = (analysis["top_tables"][0][0]
                     if analysis["top_tables"]
                     else f"community_{community_index}")
        safe = _safe_filename(top_label)
        fname = f"community_{community_index:02d}_{safe}.md"
        spec_path = write_community_modeling_spec(
            community_index=community_index,
            top_table=top_label,
            analysis=analysis,
            join_paths=join_paths.get(community_index, []),
            bridge_table_labels=bridge_labels,
            bridge_to_neighbor_communities=bridge_to_neighbor_communities,
            output_path=specs_dir / fname,
        )
        spec_paths.append(spec_path)

    print(f"      graph.html (overview)     -> {overview_html}")
    print(f"      communities/index.html    -> {index_html}")
    print(f"      communities.md            -> {communities_md}")
    print(f"      modeling_specs/           -> {specs_dir} ({len(spec_paths)} spec(s))")
    print(f"      validation_report.md      -> {report_md}")

    return {
        "graph_html": overview_html,
        "communities_index_html": index_html,
        "communities_md": communities_md,
        "modeling_specs": spec_paths,
        "validation_report": report_md,
        "n_views_total": len(all_views),
        "n_views_business": len(views),
        "n_views_excluded": len(excluded_views),
        "n_tables": n_table,
        "n_bridge_tables": len(bridge_nodes),
        "n_communities": len(communities),
        "n_cross_domain_views": len(cross_domain),
    }


# ============================================================================
# CLI ENTRY POINT
# ============================================================================
#
# Allows running this from the shell:
#   python -m tools.operate.validate_graph_pivot CORPUS_PATH OUTPUT_DIR


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate whether the graph pivot is justified for this corpus."
    )
    parser.add_argument("corpus_path", help="Path to corpus.jsonl")
    parser.add_argument("output_dir", help="Directory to write artifacts into")
    parser.add_argument(
        "--resolution", type=float, default=1.0,
        help="Louvain resolution (default 1.0). Lower (e.g. 0.5) -> fewer, "
             "broader communities. Higher (e.g. 1.5) -> more, finer ones.",
    )
    parser.add_argument(
        "--bridge-percentile", type=float, default=90.0,
        help="Tables in the top (100 - bridge_percentile) %% by degree are "
             "classified as bridges (dimensions / shared lookups) and "
             "excluded from community detection. Default 90 means top 10%%.",
    )
    parser.add_argument(
        "--exclude-pattern", action="append", default=None,
        help="Case-insensitive substring; views whose name matches are excluded "
             "as infrastructure. Repeatable. If not supplied, uses defaults: "
             f"{', '.join(DEFAULT_INFRASTRUCTURE_PATTERNS)}.",
    )
    args = parser.parse_args()

    run_validation(
        args.corpus_path,
        args.output_dir,
        resolution=args.resolution,
        bridge_percentile=args.bridge_percentile,
        exclude_patterns=args.exclude_pattern,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
