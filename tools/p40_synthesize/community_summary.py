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
    view_strength: dict[str, dict[int, float]] | None = None,
    view_to_driver: dict[str, str | None] | None = None,
    strength_threshold: float = 0.5,
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
    view_strength : (optional) map view_name -> {community_idx -> fraction}
        from `p30_analyze.view_membership.compute_view_membership_strength`.
        When provided, per-community sections split primary views into
        STRONG and WEAK members. None falls back to a single "Primary
        views" list (pre-Phase-3a behavior).
    view_to_driver : (optional) map view_name -> driver-table label or
        None. From `p30_analyze.view_membership.view_driver_table`.
        Used in the weak-members list to annotate each weak view with
        the table that actually drives it.
    strength_threshold : fraction. Views with primary-community
        membership >= threshold are STRONG; below are WEAK. Default 0.5.

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

        # If membership-strength data was provided, split into strong / weak.
        # Otherwise fall back to a single flat "Primary views" list (pre-3a).
        if view_strength is not None:
            _write_primary_views_split(
                lines, community_index, analysis["primary_views"],
                view_strength, view_to_driver, strength_threshold,
            )
        else:
            lines.append(f"### Primary views ({len(analysis['primary_views'])})")
            for v in analysis["primary_views"]:
                lines.append(f"- `{v}`")
            lines.append("")

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines), encoding="utf-8")
    return str(out)


def _write_primary_views_split(
    lines: list[str],
    community_index: int,
    primary_views: list[str],
    view_strength: dict[str, dict[int, float]],
    view_to_driver: dict[str, str | None] | None,
    strength_threshold: float,
) -> None:
    """Append "Strong members" + "Weak members" subsections to `lines`.

    Strong = primary-community membership fraction >= threshold.
    Weak   = primary-community membership fraction < threshold.

    For each weak view, the line includes:
      - the membership fraction (e.g., "1/4 tables here = 25%")
      - the view's driver table label, if known -- this is the key
        outlier-detection signal: "primary is claims, but driver is PATIENT"

    Modifies `lines` in place (appends; doesn't replace).
    """
    strong: list[tuple[str, float]] = []
    weak: list[tuple[str, float]] = []
    for v in primary_views:
        strengths = view_strength.get(v, {})
        primary_fraction = strengths.get(community_index, 0.0)
        if primary_fraction >= strength_threshold:
            strong.append((v, primary_fraction))
        else:
            weak.append((v, primary_fraction))
    # Sort: strong descending by fraction (most "in" the community first);
    #       weak ascending (most outlier-y first).
    strong.sort(key=lambda pair: pair[1], reverse=True)
    weak.sort(key=lambda pair: pair[1])

    lines.append(f"### Strong members ({len(strong)})  -- "
                  f"primary-community membership >= {int(strength_threshold * 100)}%")
    if strong:
        for view_name, frac in strong:
            lines.append(f"- `{view_name}`  ({frac:.0%} of its tables here)")
    else:
        lines.append("- _(none)_")
    lines.append("")

    lines.append(f"### Weak members ({len(weak)})  -- "
                  f"only some tables here; primary may be misleading")
    if weak:
        lines.append("These views are formally in this community (their primary)")
        lines.append("but only a small fraction of their tables actually fall here.")
        lines.append("Often the view is driven by a non-community table; review.")
        lines.append("")
        for view_name, frac in weak:
            driver = (view_to_driver or {}).get(view_name)
            driver_note = f"  driver: `{driver}`" if driver else "  driver: _(unknown)_"
            lines.append(f"- `{view_name}`  ({frac:.0%} here);{driver_note}")
    else:
        lines.append("- _(none -- all primary views are strong members)_")
    lines.append("")
