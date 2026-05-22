"""Per-community modeling spec generator -- the data-modeling-team handoff.

For each community, produces a single markdown document that
consolidates everything the modeling team needs to design a certified
Fabric data model that replaces the community's silo'd views:

  - Subject-area description (top tables + auto-named heading)
  - Tables & their roles (core / dimensions / lookups / bridges)
  - Common spine (most-traversed JOIN edges across views)
  - Reconciliation candidates (columns with multi-definition variance)
  - Common cohort filters (cross-view frequency)
  - Member views, split into strong / weak / cross-domain spanners
  - Recommendations (auto-generated from the data above)

One spec per community at `<output_dir>/modeling_specs/community_<NN>_<top_table>.md`.
Stewards skim communities.md to pick a community; modelers read the
spec for that community to scope their work.

Historical note
---------------
Added in Phase 3e-iii of the 2026-05 restructure. It depends on:
  - p30_analyze.community_analysis  (top/core/leaf tables + primary views)
  - p30_analyze.column_variance     (reconciliation candidates)
  - p30_analyze.join_paths          (common spine)
  - p30_analyze.filter_patterns     (common cohort filters)
  - p30_analyze.view_membership     (strong/weak per view + drivers)
  - p30_analyze.primary_community   (cross-domain spans)
"""

from __future__ import annotations

from pathlib import Path


def write_community_modeling_spec(
    community_index: int,
    top_table: str,
    analysis: dict,
    column_variance: list[dict],
    join_paths: list[dict],
    filter_patterns: list[dict],
    view_strength: dict[str, dict[int, float]],
    view_to_driver: dict[str, str | None],
    view_to_spans: dict[str, list[int]],
    bridge_table_labels: list[str],
    bridge_to_neighbor_communities: dict[str, list[int]],
    output_path: str | Path,
    spine_threshold_fraction: float = 0.5,
    filter_min_views: int = 2,
    strong_member_threshold: float = 0.5,
) -> str:
    """Write a single community modeling spec markdown.

    Parameters
    ----------
    community_index : int            -- community number (used in heading)
    top_table       : str            -- the community's highest-degree table
                                         (used as the auto-name)
    analysis        : dict           -- from p30_analyze.community_analysis.analyze_community
    column_variance : list[dict]     -- from p30_analyze.column_variance.analyze_column_variance
                                         (the per-community list, not the whole map)
    join_paths      : list[dict]     -- from p30_analyze.join_paths.analyze_join_paths (per-community)
    filter_patterns : list[dict]     -- from p30_analyze.filter_patterns.analyze_filter_patterns (per-community)
    view_strength   : dict           -- from p30_analyze.view_membership.compute_view_membership_strength
    view_to_driver  : dict           -- from p30_analyze.view_membership.view_driver_table results
    view_to_spans   : dict           -- from p30_analyze.primary_community.assign_views_to_communities
    bridge_table_labels             : labels of all bridge tables in the corpus
    bridge_to_neighbor_communities  : bridge label -> communities it connects
    output_path                     : where to write the markdown
    spine_threshold_fraction        : a JOIN edge is "spine" if used by
                                       >= this fraction of primary views (default 0.5)
    filter_min_views                : a filter pattern is reported only if
                                       it appears in >= this many views (default 2)
    strong_member_threshold         : view is "strong" if >= this fraction
                                       of its tables are in this community (default 0.5)

    Returns the output path as a string.
    """
    primary_views = analysis["primary_views"]
    n_primary = len(primary_views)
    n_tables = analysis["n_tables"]

    lines: list[str] = []

    # ----- Heading + subject area summary -----
    lines.append(f"# Community {community_index} -- {top_table}")
    lines.append("")
    lines.append(
        f"**{n_tables} tables, {n_primary} primary member views.** "
        f"Top table by JOIN traversal: `{top_table}`."
    )
    lines.append("")
    lines.append(
        "This document is the modeling-team handoff for community "
        f"{community_index}. It consolidates the structural and definitional "
        "findings from the analysis pipeline -- use it to scope a certified "
        "data model that replaces this community's silo'd views."
    )
    lines.append("")

    # ----- Tables & roles -----
    lines.append("## Tables & roles")
    lines.append("")
    lines.append("### Core tables (cohort-shaping)")
    lines.append("")
    if analysis["core_tables"]:
        for t in analysis["core_tables"]:
            lines.append(f"- `{t}`")
    else:
        lines.append("- _(none -- this community has only leaf tables)_")
    lines.append("")

    lines.append("### Leaf tables (decorative -- lookups, ZC, etc.)")
    lines.append("")
    if analysis["leaf_tables"]:
        for t in analysis["leaf_tables"]:
            lines.append(f"- `{t}`")
    else:
        lines.append("- _(none)_")
    lines.append("")

    # Bridges that connect TO this community.
    bridges_connecting_here = [
        label for label in bridge_table_labels
        if community_index in bridge_to_neighbor_communities.get(label, [])
    ]
    lines.append("### Conformed dimensions (bridge tables connecting to this community)")
    lines.append("")
    if bridges_connecting_here:
        lines.append(
            "These are high-degree dimensions used widely across the corpus. "
            "They are SHARED with other communities -- treat them as conformed "
            "dimensions in the data model, not as community-specific."
        )
        lines.append("")
        for label in sorted(bridges_connecting_here):
            other_comms = [
                c for c in bridge_to_neighbor_communities.get(label, [])
                if c != community_index
            ]
            lines.append(
                f"- `{label}` -- also connects to {len(other_comms)} other "
                f"communities"
            )
    else:
        lines.append(
            "- _(none -- this community is structurally isolated from "
            "the rest of the corpus)_"
        )
    lines.append("")

    # ----- Common spine (JOIN edges) -----
    lines.append("## Common JOIN spine")
    lines.append("")
    if join_paths:
        threshold_count = max(1, int(spine_threshold_fraction * n_primary)) if n_primary else 1
        spine = [j for j in join_paths if j["n_views"] >= threshold_count]
        peripheral = [j for j in join_paths if j["n_views"] < threshold_count]

        lines.append(
            f"JOIN edges across the {n_primary} primary views, "
            f"sorted by how many views use them. "
            f"Spine threshold: used by >= {threshold_count} views "
            f"(>= {int(spine_threshold_fraction * 100)}%)."
        )
        lines.append("")

        lines.append(f"### Spine edges ({len(spine)})")
        lines.append("")
        if spine:
            lines.append("| from | to | join type | views |")
            lines.append("|---|---|---|---|")
            for j in spine[:20]:
                jt = j["join_type"]
                if j["n_distinct_join_types"] > 1:
                    jt = f"{jt} _(+{j['n_distinct_join_types'] - 1} other)_"
                lines.append(
                    f"| `{j['from_table']}` | `{j['to_table']}` | {jt} | {j['n_views']}/{n_primary} |"
                )
            if len(spine) > 20:
                lines.append(f"\n... and {len(spine) - 20} more spine edges.")
        else:
            lines.append("- _(none -- no JOIN edge is used by enough views to qualify)_")
        lines.append("")

        lines.append(f"### Peripheral edges ({len(peripheral)})")
        lines.append("")
        if peripheral:
            lines.append(
                "Edges used by fewer than the spine threshold. May be view-specific "
                "or rare flows the modeling team can deprioritize."
            )
            lines.append("")
            for j in peripheral[:15]:
                lines.append(
                    f"- `{j['from_table']}` → `{j['to_table']}` "
                    f"({j['n_views']}/{n_primary} view{'s' if j['n_views'] != 1 else ''})"
                )
            if len(peripheral) > 15:
                lines.append(f"\n... and {len(peripheral) - 15} more peripheral edges.")
        else:
            lines.append("- _(none)_")
    else:
        lines.append(
            "_(no JOIN edges -- this community's views don't share table-to-table "
            "joins; the cohort may be defined by single-table reads.)_"
        )
    lines.append("")

    # ----- Reconciliation candidates -----
    lines.append("## Reconciliation candidates (columns with multi-definition variance)")
    lines.append("")
    if column_variance:
        lines.append(
            f"**{len(column_variance)} column(s)** in this community appear with "
            "multiple distinct SQL definitions across the member views. The "
            "modeling team must canonicalize each before designing the model."
        )
        lines.append("")
        for vc in column_variance[:20]:
            source_str = ", ".join(f"`{t}`" for t in vc["source_tables"]) or "_(no source)_"
            lines.append(
                f"### `{vc['column_name']}`  (from {source_str})"
            )
            lines.append("")
            lines.append(
                f"Used by {vc['n_views']} view(s); "
                f"**{vc['n_distinct_fingerprints']} distinct definition(s)**."
            )
            lines.append("")
            for i, defn in enumerate(vc["definitions"], 1):
                marker = "  ←  most-common" if i == 1 else ""
                lines.append(f"**Definition {i} ({len(defn['views'])} view(s)){marker}**")
                lines.append("")
                lines.append("```sql")
                lines.append(defn["technical_description"] or "(no expression captured)")
                lines.append("```")
                if defn["business_description"]:
                    lines.append(f"_English:_ {defn['business_description']}")
                    lines.append("")
                lines.append(f"_Views:_ {', '.join('`' + v + '`' for v in defn['views'])}")
                lines.append("")
        if len(column_variance) > 20:
            lines.append(f"... and {len(column_variance) - 20} more reconciliation candidates.")
            lines.append("")
    else:
        lines.append(
            "_(none -- no column has multiple distinct definitions across the "
            "community's member views.)_"
        )
    lines.append("")

    # ----- Common filters -----
    lines.append("## Common cohort filters")
    lines.append("")
    common_filters = [f for f in filter_patterns if f["n_views"] >= filter_min_views]
    if common_filters:
        lines.append(
            f"Filters that appear in **{filter_min_views}+ views** of this community. "
            "Consider these as candidate default scopes / parameters for the "
            "certified data model."
        )
        lines.append("")
        for f in common_filters[:20]:
            english = f["english"] or "_(no English translation)_"
            lines.append(f"- **{english}** ({f['n_views']}/{n_primary} views, kind={f['kind']})")
            if f["sql"] and f["sql"] != f["english"]:
                lines.append(f"  - SQL: `{f['sql']}`")
        if len(common_filters) > 20:
            lines.append(f"\n... and {len(common_filters) - 20} more.")
    else:
        lines.append(
            f"_(none -- no filter is shared by {filter_min_views}+ views in this community.)_"
        )
    lines.append("")

    # ----- Member views split -----
    lines.append("## Member views (retirement candidates)")
    lines.append("")
    strong: list[tuple[str, float]] = []
    weak: list[tuple[str, float]] = []
    for v in primary_views:
        fraction = view_strength.get(v, {}).get(community_index, 0.0)
        if fraction >= strong_member_threshold:
            strong.append((v, fraction))
        else:
            weak.append((v, fraction))
    strong.sort(key=lambda kv: kv[1], reverse=True)
    weak.sort(key=lambda kv: kv[1])

    # Cross-domain spanners among this community's primaries.
    cross_domain = sorted(
        v for v in primary_views
        if len(view_to_spans.get(v, [])) > 1
    )

    lines.append(
        f"### Strong members ({len(strong)})  -- >= {int(strong_member_threshold * 100)}% "
        "of tables in this community"
    )
    lines.append("")
    lines.append(
        "These views are firmly in this community. They are the primary "
        "retirement candidates -- the certified data model should be able "
        "to serve their use cases."
    )
    lines.append("")
    if strong:
        for view_name, frac in strong:
            lines.append(f"- `{view_name}`  ({frac:.0%} of its tables here)")
    else:
        lines.append("- _(none)_")
    lines.append("")

    lines.append(f"### Weak members ({len(weak)})  -- below the threshold")
    lines.append("")
    if weak:
        lines.append(
            "These views are formally in this community (primary) but only a "
            "minority of their tables actually fall here. Often the view is "
            "driven by a non-community table; verify before treating as a "
            "retirement candidate."
        )
        lines.append("")
        for view_name, frac in weak:
            driver = view_to_driver.get(view_name)
            driver_note = f", driver: `{driver}`" if driver else ""
            lines.append(f"- `{view_name}` ({frac:.0%} here{driver_note})")
    else:
        lines.append("- _(none -- all primary members are strong)_")
    lines.append("")

    lines.append(f"### Cross-domain spanners ({len(cross_domain)})")
    lines.append("")
    if cross_domain:
        lines.append(
            "These primary members ALSO touch other communities. They are "
            "candidates for splitting (consolidate two reports into two "
            "narrower models) or for a higher-level cross-domain model."
        )
        lines.append("")
        for v in cross_domain:
            spans = view_to_spans.get(v, [])
            other = [c for c in spans if c != community_index]
            lines.append(
                f"- `{v}` (also spans communities {', '.join(str(c) for c in other)})"
            )
    else:
        lines.append("- _(none -- all primary members are single-domain)_")
    lines.append("")

    # ----- Auto-generated recommendations -----
    lines.append("## Recommendations")
    lines.append("")
    recs = _generate_recommendations(
        community_index=community_index,
        top_table=top_table,
        n_primary=n_primary,
        column_variance=column_variance,
        join_paths=join_paths,
        common_filters=common_filters,
        weak=weak,
        cross_domain=cross_domain,
        spine_threshold_fraction=spine_threshold_fraction,
    )
    if recs:
        for r in recs:
            lines.append(f"- {r}")
    else:
        lines.append("- _(no auto-generated recommendations -- review the sections above directly.)_")
    lines.append("")

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines), encoding="utf-8")
    return str(out)


def _generate_recommendations(
    community_index: int,
    top_table: str,
    n_primary: int,
    column_variance: list[dict],
    join_paths: list[dict],
    common_filters: list[dict],
    weak: list[tuple[str, float]],
    cross_domain: list[str],
    spine_threshold_fraction: float,
) -> list[str]:
    """Generate a few heuristic recommendations from the analysis data.

    These are templated suggestions -- meant to start a conversation
    with the modeling team, not to be authoritative. Each item points
    back to a section above so the modeler can verify.
    """
    recs: list[str] = []

    if column_variance:
        recs.append(
            f"**{len(column_variance)} reconciliation candidate(s)** need "
            f"steward attention BEFORE modeling. See the Reconciliation "
            f"Candidates section -- each lists the variants and which views "
            f"use them; the most-common variant is flagged."
        )

    if join_paths:
        # Compute the "spine" size for the recommendation message.
        threshold_count = max(1, int(spine_threshold_fraction * n_primary)) if n_primary else 1
        spine = [j for j in join_paths if j["n_views"] >= threshold_count]
        if spine:
            top_edge = spine[0]
            recs.append(
                f"The common JOIN spine has **{len(spine)} edge(s)**. The "
                f"most-used edge `{top_edge['from_table']}` → "
                f"`{top_edge['to_table']}` is in {top_edge['n_views']}/"
                f"{n_primary} views -- it's the natural starting point for "
                f"the fact-table design."
            )

    if common_filters:
        top_filter = common_filters[0]
        recs.append(
            f"The most-common cohort filter (`{top_filter['english'] or top_filter['sql']}`) "
            f"appears in {top_filter['n_views']}/{n_primary} views. Consider "
            f"treating it as a default scope (or a parameter with this as "
            f"the default) in the model."
        )

    if weak:
        recs.append(
            f"**{len(weak)} weak member view(s)** in this community may not "
            f"actually belong here -- their primary table is in this community "
            f"but most of their tables aren't. Review with the BI dev before "
            f"committing them to the retirement list."
        )

    if cross_domain:
        recs.append(
            f"**{len(cross_domain)} cross-domain view(s)** touch this "
            f"community AND others. Decide per-view whether to split "
            f"(separate models per domain) or keep as a higher-level "
            f"integration view."
        )

    return recs
