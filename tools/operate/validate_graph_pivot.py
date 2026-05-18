"""Validation experiment for the graph-pivot architecture decision.

Background
----------
We have been debating whether to pivot the codebase away from per-view JSON
clusters and toward a unified GRAPH representation of the entire corpus. In a
graph, tables/columns become nodes, joins become edges, and similar views
naturally cluster together as densely-connected communities in the graph.

The hypothesis we are testing here is:

  When we build a single graph from all views, and apply a community-
  detection algorithm (Louvain), the resulting communities should
  correspond to RECOGNIZABLE healthcare-BI subject areas (Epic modules:
  inpatient, clinic, claims, etc.).

If that hypothesis holds, we commit to the graph pivot. If it does not,
we revisit the architecture before changing the codebase.

What this script does
---------------------
1. Reads a corpus.jsonl file (one view per JSON line; first line is a header).
2. Builds a typed networkx graph from the corpus:
     - View nodes, Scope nodes (main / cte / subquery), Table nodes, Column nodes
     - JOIN edges between tables, with view + scope provenance attached
     - Containment edges (View -> Scope, Scope -> Column, etc.)
3. Derives a "table co-occurrence" projection: a smaller, undirected graph in
   which two tables are connected if they appear in the same scope of any view.
   Edge weights = number of co-appearances. This projection is what we run
   community detection on, because:
     - It is small (tables, not all 1000s of columns).
     - It captures the cohort-shape information we care about.
     - It is the natural input to Louvain (which expects an undirected weighted graph).
4. Runs Louvain community detection on the table projection.
5. Emits these artifacts in the output directory:
     - communities/community_NN_<top-table>.html  -- one interactive HTML per
                                   community, showing that community's tables
                                   plus bridge tables in muted gray for context
     - communities/index.html   -- linkable index of all per-community HTMLs
     - communities.md           -- per-community summary: primary member views,
                                   top tables, leaf tables, bridge tables.
                                   PLUS a Shared Dimensions section and
                                   a Cross-Domain Views section
     - validation_report.md     -- summary verdict: does the community structure
                                   look healthcare-meaningful? Confidence level.
                                   Recommendation: pivot, revise, or revisit.

Refinements layered on top of the initial validation:

  * BRIDGE-TABLE DETECTION. Dimension/lookup tables like PATIENT, CLARITY_SER,
    CLARITY_DEP appear in nearly every view and connect everything in the graph,
    distorting community detection. We DON'T hard-code a dimension list -- we
    let the graph reveal them: tables in the top N percent by degree are
    classified as BRIDGES and excluded from community detection (but kept in
    the rendered graph and reported separately).

  * INFRASTRUCTURE-VIEW EXCLUSION. Views that extract metadata for cataloging
    (Collibra, Atlas) are infrastructure, not business logic. We exclude views
    whose name matches default patterns (collibra/metadata/catalog/ingest) or
    which read from sys.* / INFORMATION_SCHEMA system schemas. Override via
    --exclude-pattern on the CLI.

  * PRIMARY COMMUNITY PER VIEW. A view is assigned to ONE primary community
    (the one containing the most of its tables). Views that span multiple
    communities are reported separately as Cross-Domain Views -- a finding
    in their own right, not noise.

How to run
----------
From the repo root, on a small local sample:
    python -m tools.operate.validate_graph_pivot \\
        my_notes/bi_complex_sample/corpus.jsonl /tmp/graph_pivot_validation

Or in a Fabric notebook, against the real 130-view corpus:
    from tools.operate.validate_graph_pivot import run_validation
    run_validation(
        corpus_path="/lakehouse/default/Files/corpus/corpus.jsonl",
        output_dir="/lakehouse/default/Files/graph_pivot_validation",
    )

The output directory will be created if it doesn't exist.
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Iterable

# Shared utilities (extracted from this file in Phase 2a of the
# 2026-05 restructure -- see Historical note in each module).
from tools.shared.corpus_io import load_corpus
from tools.shared.view_filter import (
    DEFAULT_INFRASTRUCTURE_PATTERNS,
    filter_business_views,
    is_infrastructure_view,
)

# Graph construction moved to p20_index in Phase 2b of the restructure.
# This module orchestrates the validation experiment by calling into
# the production graph builder; it no longer defines the graph itself.
from tools.p20_index.graph_builder import build_graph

# Analysis layer moved to p30_analyze in Phase 2c of the restructure.
# The orchestrator calls into each module; the validation diagnostic
# now consumes the production analysis code rather than defining it.
from tools.p30_analyze.bridges import (
    detect_bridge_tables,
    project_without_bridges,
)
from tools.p30_analyze.communities import detect_table_communities
from tools.p30_analyze.community_analysis import analyze_community
from tools.p30_analyze.primary_community import assign_views_to_communities
from tools.p30_analyze.projection import extract_table_projection


# ============================================================================
# SECTION 5 -- Rendering (interactive HTML)
# ============================================================================


# Community-color palette. Distinct, colorblind-friendlier than rainbow.
# Cycles past 12 communities (acceptable for validation visualization).
_COMMUNITY_PALETTE: tuple[str, ...] = (
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b",
    "#e377c2", "#7f7f7f", "#bcbd22", "#17becf", "#aec7e8", "#ffbb78",
)

_BRIDGE_COLOR = "#bbbbbb"  # muted gray for bridge/dimension tables (shared context)


def _community_color(community_index: int) -> str:
    """Return a stable color for a community index. Cycles past the palette length."""
    return _COMMUNITY_PALETTE[community_index % len(_COMMUNITY_PALETTE)]


def _safe_filename(s: str) -> str:
    """Convert an arbitrary string to a safe filename fragment.

    Lowercases and replaces anything outside [a-z0-9_] with underscore.
    Used to name per-community HTML files after their top table.
    """
    return "".join(c.lower() if c.isalnum() else "_" for c in s)[:40]


def _compute_static_positions(g, scale: float = 1000.0) -> dict[str, tuple[float, float]]:
    """Compute a deterministic static layout for a graph (no animation).

    Uses networkx's spring_layout with a fixed seed. Returns a dict
    mapping node_id -> (x, y) where x and y are in pyvis pixel coordinates
    (scale of `scale` from the unit square spring_layout returns).

    Why a static layout: pyvis with physics-on animates the graph for
    several seconds while vis.js's force simulation converges, which the
    user found distracting on small graphs. Pre-computing positions in
    networkx + setting physics=False gives the user an instant, stable,
    community-centric layout.
    """
    import networkx as nx

    n = g.number_of_nodes()
    if n == 0:
        return {}
    if n <= 200:
        # kamada_kawai gives a nice readable layout for small graphs;
        # spring_layout is also fine but can overlap nodes in small graphs.
        try:
            raw = nx.kamada_kawai_layout(g)
        except Exception:
            # kamada_kawai requires connectivity; fall back to spring on disconnected
            raw = nx.spring_layout(g, seed=42, k=0.5, iterations=80)
    else:
        # For larger graphs, spring_layout is faster and good enough.
        raw = nx.spring_layout(g, seed=42, k=0.4, iterations=50)

    # spring_layout returns coords in [-1, 1] for spring or [0, 1] for kamada_kawai.
    # Normalize to a pyvis-friendly pixel range centered around 0.
    return {node: (float(x) * scale, float(y) * scale) for node, (x, y) in raw.items()}


def render_community_html(
    table_g, community_index: int, community_tables: set[str],
    bridge_tables: set[str], output_path: str | Path,
) -> str:
    """Render ONE community as interactive HTML, with bridges shown muted.

    Each per-community HTML contains:
      - All tables in this community (colored with the community color)
      - All bridge tables connected to any community-table (colored muted gray)
      - Edges between any pair of the above

    The layout is pre-computed with networkx and frozen (physics=off,
    fixed=true on every node). This gives stewards an instant, readable,
    non-animated view -- the previous behavior animated even small graphs
    for several seconds, which was distracting.
    """
    from pyvis.network import Network

    # Collect the nodes to render: community tables + bridges connected to them.
    nodes_to_render: set[str] = set(community_tables)
    for ct in community_tables:
        if ct not in table_g:
            continue
        for neighbor in table_g.neighbors(ct):
            if neighbor in bridge_tables:
                nodes_to_render.add(neighbor)

    # Build the subgraph as a regular nx.Graph (undirected) to keep pyvis happy.
    sub = table_g.subgraph(nodes_to_render).copy()
    color_for_community = _community_color(community_index)
    positions = _compute_static_positions(sub)

    net = Network(
        height="900px", width="100%",
        directed=False, notebook=False,
        cdn_resources="in_line",
    )

    for node, attrs in sub.nodes(data=True):
        is_zc = attrs.get("is_zc", False)
        is_bridge = node in bridge_tables
        if is_bridge:
            color = _BRIDGE_COLOR
            shape = "diamond"
            size = 18
        else:
            color = color_for_community
            shape = "box" if is_zc else "dot"
            size = 15 if is_zc else 25
        label = attrs.get("label", str(node))
        title_lines = [f"Table: {label}"]
        if is_bridge:
            title_lines.append("Role: BRIDGE (high-degree dimension/shared lookup)")
        if is_zc:
            title_lines.append("Type: ZC lookup")
        x, y = positions.get(node, (0.0, 0.0))
        # `physics=False` + `fixed=True` together pin the node to (x, y).
        # Without `fixed=True`, vis.js still nudges nodes during interaction.
        net.add_node(
            node, label=label, color=color, shape=shape, size=size,
            title="\n".join(title_lines),
            x=x, y=y, physics=False, fixed=True,
        )

    for u, v, attrs in sub.edges(data=True):
        w = attrs.get("weight", 1)
        width = min(1 + w / 2, 8)
        net.add_edge(u, v, value=w, width=width, title=f"co-occurrences: {w}")

    # Disable the simulation globally so the canvas does not "settle" on load.
    net.toggle_physics(False)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    net.write_html(str(out), notebook=False)
    return str(out)


def render_overview_html(
    table_g, communities: list[set], bridge_tables: set[str],
    output_path: str | Path,
) -> str:
    """Render the FULL table graph, colored by community. Useful as an overview.

    For corpora with many tables this will be dense. Per-community HTMLs are
    a better daily-driver; this is the "see the whole landscape" view.

    Layout is pre-computed with networkx so densely-connected nodes
    (community members) end up near each other in space, giving a
    community-centric visual without animation.
    """
    from pyvis.network import Network

    # Map each node to its community color (or bridge color).
    node_to_color: dict[str, str] = {}
    for community_index, member_set in enumerate(communities):
        c = _community_color(community_index)
        for node in member_set:
            node_to_color[node] = c
    for node in bridge_tables:
        node_to_color[node] = _BRIDGE_COLOR

    fallback = "#cccccc"
    # Larger pixel scale for the overview since it has more nodes to spread out.
    positions = _compute_static_positions(table_g, scale=2000.0)

    net = Network(
        height="900px", width="100%",
        directed=False, notebook=False,
        cdn_resources="in_line",
    )

    for node, attrs in table_g.nodes(data=True):
        is_zc = attrs.get("is_zc", False)
        is_bridge = node in bridge_tables
        label = attrs.get("label", str(node))
        title_lines = [f"Table: {label}"]
        if is_bridge:
            title_lines.append("Role: BRIDGE")
        if is_zc:
            title_lines.append("Type: ZC lookup")
        x, y = positions.get(node, (0.0, 0.0))
        net.add_node(
            node, label=label,
            color=node_to_color.get(node, fallback),
            shape="diamond" if is_bridge else ("box" if is_zc else "dot"),
            size=18 if is_bridge else (15 if is_zc else 25),
            title="\n".join(title_lines),
            x=x, y=y, physics=False, fixed=True,
        )

    for u, v, attrs in table_g.edges(data=True):
        w = attrs.get("weight", 1)
        net.add_edge(u, v, value=w, width=min(1 + w / 2, 8),
                     title=f"co-occurrences: {w}")

    net.toggle_physics(False)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    net.write_html(str(out), notebook=False)
    return str(out)


def render_communities_index_html(
    community_html_files: list[tuple[int, str, str, int, int]],
    output_path: str | Path,
) -> str:
    """Write a small index.html listing all per-community HTMLs.

    Each entry in `community_html_files` is:
       (community_index, top_table_label, html_filename, n_tables, n_views)
    """
    parts: list[str] = []
    parts.append("<!doctype html><html><head><meta charset='utf-8'>")
    parts.append("<title>Graph-pivot communities</title>")
    parts.append("<style>")
    parts.append("body { font-family: -apple-system, system-ui, sans-serif; "
                 "max-width: 800px; margin: 40px auto; padding: 0 20px; }")
    parts.append("h1 { color: #333; }")
    parts.append("table { border-collapse: collapse; width: 100%; margin: 20px 0; }")
    parts.append("th, td { text-align: left; padding: 8px 12px; "
                 "border-bottom: 1px solid #ddd; }")
    parts.append("th { background: #f4f4f4; }")
    parts.append("a { color: #1f77b4; text-decoration: none; }")
    parts.append("a:hover { text-decoration: underline; }")
    parts.append(".color-swatch { display: inline-block; width: 14px; height: 14px; "
                 "border-radius: 3px; vertical-align: middle; margin-right: 6px; }")
    parts.append("</style></head><body>")
    parts.append("<h1>Graph-pivot communities</h1>")
    parts.append("<p>Click a community to see its interactive graph. Bridge tables "
                 "(dimensions / shared lookups) are shown in muted gray.</p>")
    parts.append("<table><thead><tr><th>#</th><th>Top table</th><th>Tables</th>"
                 "<th>Member views</th><th>Open</th></tr></thead><tbody>")
    for community_index, top_table, fname, n_tables, n_views in community_html_files:
        c = _community_color(community_index)
        parts.append("<tr>")
        parts.append(f"<td><span class='color-swatch' style='background:{c}'></span>"
                     f"{community_index}</td>")
        parts.append(f"<td><code>{top_table}</code></td>")
        parts.append(f"<td>{n_tables}</td>")
        parts.append(f"<td>{n_views}</td>")
        parts.append(f"<td><a href='{fname}'>open &rarr;</a></td>")
        parts.append("</tr>")
    parts.append("</tbody></table></body></html>")

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(parts), encoding="utf-8")
    return str(out)


# ============================================================================
# SECTION 6 -- Markdown reporting
# ============================================================================


def write_communities_markdown(
    communities: list[set],
    analyses: list[dict],
    bridge_table_labels: list[str],
    bridge_to_neighbor_communities: dict[str, list[int]],
    view_to_spans: dict[str, list[int]],
    excluded_infrastructure_views: list[str],
    output_path: str | Path,
) -> str:
    """Write the per-community summary to markdown.

    Top of file: overall summary + shared dimensions + excluded infra views
    + cross-domain views. Then one section per community with primary views.

    Cross-domain views: any view in `view_to_spans` whose spans list has
    length > 1. These are reported separately so they don't pollute the
    primary-view lists of individual communities.
    """
    lines: list[str] = []
    lines.append("# Communities discovered by Louvain on the table-projection graph")
    lines.append("")
    lines.append(f"Total communities: {len(communities)}")
    lines.append("")
    lines.append("Each community is a set of tables that frequently co-appear in scopes")
    lines.append("of the same views. Communities should correspond to recognizable")
    lines.append("subject areas (e.g., Epic clinic encounters, claims, billing).")
    lines.append("")
    lines.append("Each view is assigned to its **primary community** -- the one")
    lines.append("containing the most of its tables. Views spanning multiple")
    lines.append("communities are listed separately under **Cross-Domain Views**.")
    lines.append("")

    # ----- Shared dimensions (bridge tables) -----
    lines.append("## Shared Dimensions (bridge tables)")
    lines.append("")
    if bridge_table_labels:
        lines.append("These tables have very high degree -- they connect to many other")
        lines.append("tables across the corpus. They are typically dimension tables")
        lines.append("(PATIENT, CLARITY_SER, CLARITY_DEP, etc.) that almost every view")
        lines.append("joins through. They are excluded from community detection because")
        lines.append("they would otherwise drag everything into one giant cluster.")
        lines.append("")
        for label in sorted(bridge_table_labels):
            neighbors = bridge_to_neighbor_communities.get(label, [])
            n_neighbors = len(neighbors)
            lines.append(f"- `{label}` -- bridges {n_neighbors} communities")
    else:
        lines.append("_(none detected at the current bridge-percentile threshold)_")
    lines.append("")

    # ----- Cross-domain views -----
    cross_domain = sorted([
        (v, spans) for v, spans in view_to_spans.items() if len(spans) > 1
    ])
    lines.append(f"## Cross-Domain Views ({len(cross_domain)})")
    lines.append("")
    lines.append("Views whose tables span 2+ communities. These are NOT noise -- they")
    lines.append("are reports that reach across business domains, and stewards should")
    lines.append("decide whether they should be split, consolidated, or kept as-is.")
    lines.append("")
    if cross_domain:
        for view_name, spans in cross_domain[:50]:  # cap at 50 to keep the file scannable
            spans_str = ", ".join(str(c) for c in spans)
            lines.append(f"- `{view_name}` spans communities: {spans_str}")
        if len(cross_domain) > 50:
            lines.append(f"- ... and {len(cross_domain) - 50} more")
    else:
        lines.append("_(none)_")
    lines.append("")

    # ----- Excluded infrastructure views -----
    if excluded_infrastructure_views:
        lines.append(f"## Excluded Infrastructure Views ({len(excluded_infrastructure_views)})")
        lines.append("")
        lines.append("These views were filtered out before community detection because")
        lines.append("they match infrastructure heuristics (metadata/catalog/ingest in")
        lines.append("the name, or reading from sys.* / INFORMATION_SCHEMA). Inspect to")
        lines.append("ensure no business-critical views were excluded by accident.")
        lines.append("")
        for v in sorted(excluded_infrastructure_views):
            lines.append(f"- `{v}`")
        lines.append("")

    # ----- Per-community sections -----
    for community_index, analysis in enumerate(analyses):
        lines.append(f"## Community {community_index} -- "
                     f"{analysis['n_tables']} tables, "
                     f"{analysis['n_primary_views']} primary views")
        lines.append("")
        lines.append(f"- ZC/lookup tables in this community: {analysis['zc_table_count']}")
        lines.append("")
        lines.append("### Top tables (by total JOIN traversal in + out)")
        for table_name, degree in analysis["top_tables"]:
            lines.append(f"- `{table_name}` -- {degree} joins")
        lines.append("")
        lines.append("### Core tables (cohort-shaping, out_degree >= 1)")
        if analysis["core_tables"]:
            for t in analysis["core_tables"]:
                lines.append(f"- `{t}`")
        else:
            lines.append("- _(none -- this community has only leaf tables)_")
        lines.append("")
        lines.append("### Leaf tables (decorative; in only, out zero)")
        if analysis["leaf_tables"]:
            for t in analysis["leaf_tables"]:
                lines.append(f"- `{t}`")
        else:
            lines.append("- _(none)_")
        lines.append("")
        lines.append(f"### Primary views ({len(analysis['primary_views'])})")
        for v in analysis["primary_views"]:
            lines.append(f"- `{v}`")
        lines.append("")

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines), encoding="utf-8")
    return str(out)


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

    print("[7/8] Assigning views to primary communities + finding cross-domain views...")
    community_to_primary, view_to_spans = assign_views_to_communities(g, communities)
    cross_domain = [v for v, spans in view_to_spans.items() if len(spans) > 1]
    print(f"      Cross-domain views: {len(cross_domain)}")

    # Per-community analysis using primary-view assignments only.
    analyses = []
    for community_index, member_set in enumerate(communities):
        analyses.append(analyze_community(
            g, member_set, community_to_primary.get(community_index, set()),
        ))

    print("[8/8] Writing artifacts...")
    # Per-community HTMLs
    community_html_files: list[tuple[int, str, str, int, int]] = []
    for community_index, (community_set, analysis) in enumerate(zip(communities, analyses)):
        # Name the per-community HTML after its top table (most connected within the community).
        top_label = analysis["top_tables"][0][0] if analysis["top_tables"] else f"community_{community_index}"
        safe = _safe_filename(top_label)
        fname = f"community_{community_index:02d}_{safe}.html"
        render_community_html(
            table_g, community_index, community_set, bridge_nodes,
            communities_dir / fname,
        )
        community_html_files.append((
            community_index, top_label, fname,
            analysis["n_tables"], analysis["n_primary_views"],
        ))
    index_html = render_communities_index_html(community_html_files,
                                                  communities_dir / "index.html")
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
    )

    report_md = write_validation_report(
        header, g, table_g, communities, analyses,
        n_bridge_tables=len(bridge_nodes),
        n_excluded_views=len(excluded_views),
        n_cross_domain_views=len(cross_domain),
        output_path=output_dir / "validation_report.md",
    )

    print(f"      graph.html (overview)     -> {overview_html}")
    print(f"      communities/index.html    -> {index_html}")
    print(f"      communities.md            -> {communities_md}")
    print(f"      validation_report.md      -> {report_md}")

    return {
        "graph_html": overview_html,
        "communities_index_html": index_html,
        "communities_md": communities_md,
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
