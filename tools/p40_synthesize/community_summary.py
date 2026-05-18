"""Per-community markdown summary -- the steward-facing artifact.

Takes the analysis findings produced by `p30_analyze` and renders them
as a plain-markdown report a non-technical steward can read in 5
minutes. Currently emits a single combined `communities.md` file with
four sections:

  1. Shared Dimensions   -- the bridge tables (high-degree dimensions
                            auto-detected by p30_analyze.bridges) with
                            a count of how many communities each bridges.
  2. Cross-Domain Views  -- views whose tables span 2+ communities;
                            steward finding in its own right.
  3. Excluded Infrastructure Views -- views filtered out by
                            tools.shared.view_filter; included for
                            transparency (so users can verify nothing
                            important was filtered).
  4. Per-community sections -- one section per community: top tables,
                            core (cohort-shaping) tables, leaf
                            (decorative) tables, primary member views.

Output is plain markdown. Stewards print these; BI devs paste excerpts
into deck slides. No HTML, no JSON-only.

Historical note
---------------
This module was previously `tools.operate.validate_graph_pivot.write_communities_markdown`.
In Phase 2d of the 2026-05 restructure it moved here -- it produces
the steward-facing artifact (the GOVERN layer's primary output), not
a validation-internal thing.
"""

from __future__ import annotations

from pathlib import Path


def write_communities_markdown(
    communities: list[set],
    analyses: list[dict],
    bridge_table_labels: list[str],
    bridge_to_neighbor_communities: dict[str, list[int]],
    view_to_spans: dict[str, list[int]],
    excluded_infrastructure_views: list[str],
    output_path: str | Path,
) -> str:
    """Write the per-community summary to a markdown file.

    Parameters
    ----------
    communities : list of sets of table-node IDs (from
        p30_analyze.communities.detect_table_communities)
    analyses : list of per-community summary dicts (from
        p30_analyze.community_analysis.analyze_community), index-aligned
        with `communities`
    bridge_table_labels : list of table labels classified as bridges
        (from p30_analyze.bridges.detect_bridge_tables; labels, not node IDs)
    bridge_to_neighbor_communities : map of bridge label -> sorted list
        of community indices the bridge connects to
    view_to_spans : view name -> sorted list of community indices the
        view touches (from p30_analyze.primary_community.assign_views_to_communities)
    excluded_infrastructure_views : list of view names filtered out by
        tools.shared.view_filter; reported for transparency
    output_path : str or Path

    Returns
    -------
    The output path as a string (for callers that want to log/echo it).
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
        # Cap at 50 to keep the file scannable; rest get a summary line.
        for view_name, spans in cross_domain[:50]:
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
