"""Per-view community-membership strength + driver-table detection.

After p30_analyze.primary_community assigns each view to ONE primary
community (the one containing the most of its tables), some views end
up "barely" in their primary -- only one or two of their tables touch
that community, and the rest are spread elsewhere or are bridge
tables.

These weak members are the outlier suspects: views that got pulled
into a community on the strength of a peripheral lookup, not by virtue
of their core cohort logic. Surfacing them lets the user (or BI dev)
investigate whether the assignment is correct.

This module computes:

  - `compute_view_membership_strength(g, communities)`
      For each view, the fraction of its non-bridge tables that fall
      into each community. The community with the highest fraction is
      the view's primary (consistent with p30_analyze.primary_community).
      A "strong" member has >= 50% of its tables in the primary; a
      "weak" member has < 50%.

  - `view_driver_table(g, view_name)`
      Returns the LIKELY driver table for a view -- the table that
      appears as the LEFT side of the most JOIN edges in that view's
      main scope. None if no joins exist. Useful context for weak
      members: "VW_FOO is in claims because of claimC, but its driver
      is actually PATIENT" makes the outlier visible.

  - `classify_views_by_strength(view_strength, communities, threshold)`
      Returns a dict: community_idx -> {strong: [view_names...], weak: [view_names...]}.
      Convenience wrapper used by p40_synthesize.community_summary
      to produce the strong/weak split per community section.
"""

from __future__ import annotations

from collections import Counter, defaultdict


def compute_view_membership_strength(
    g, communities: list[set],
) -> dict[str, dict[int, float]]:
    """For each view, compute the fraction of its non-bridge tables in each community.

    Tables are counted UNIQUELY per view (so a table both read and joined
    is counted once). Tables that don't belong to any community (e.g.,
    bridge tables, since they were excluded before community detection)
    are excluded from the denominator -- they don't pull a view in any
    particular direction.

    Parameters
    ----------
    g           : nx.MultiDiGraph from p20_index.graph_builder.build_graph
    communities : list of sets of table-node IDs (from p30_analyze.communities.detect_table_communities)

    Returns
    -------
    view_strength : dict[view_name, dict[community_idx, fraction]]
        Each inner dict's values sum to <= 1.0 (less than 1.0 if the
        view has tables in NO community, e.g., the view touches only
        bridge tables -- in that case the inner dict is empty).
    """
    # Reverse map: table_node_id -> community_index, for fast lookup.
    table_to_community: dict[str, int] = {}
    for community_index, member_set in enumerate(communities):
        for table_id in member_set:
            table_to_community[table_id] = community_index

    # For each view, collect the UNIQUE set of table node IDs it touches.
    # We look at three edge relations to cover all ways a view can touch a
    # table: READS_FROM_TABLE (FROM clause), JOIN (JOIN clauses), and
    # BELONGS_TO (a column from this view references the table).
    view_to_tables: dict[str, set[str]] = defaultdict(set)

    for u, v, attrs in g.edges(data=True):
        relation = attrs.get("relation")
        if relation not in ("READS_FROM_TABLE", "JOIN", "BELONGS_TO"):
            continue
        view_name = attrs.get("view")
        if not view_name:
            continue
        # For each of these edge types, one endpoint is a table. Add any
        # endpoint that is a table node to the view's set.
        for endpoint in (u, v):
            attrs_endpoint = g.nodes[endpoint]
            if attrs_endpoint.get("ntype") == "table":
                view_to_tables[view_name].add(endpoint)

    # Compute strength per (view, community).
    view_strength: dict[str, dict[int, float]] = {}
    for view_name, tables in view_to_tables.items():
        # Exclude tables that are not in any community (bridges).
        non_bridge_tables = [t for t in tables if t in table_to_community]
        if not non_bridge_tables:
            # The view touches only bridges -- it doesn't belong anywhere.
            view_strength[view_name] = {}
            continue

        community_counts: Counter = Counter()
        for table_id in non_bridge_tables:
            community_counts[table_to_community[table_id]] += 1

        total = len(non_bridge_tables)
        view_strength[view_name] = {
            community_idx: count / total
            for community_idx, count in community_counts.items()
        }

    return view_strength


def view_driver_table(g, view_name: str) -> str | None:
    """Return the likely driver table for a view (its main scope's "from" table).

    Heuristic: the table that appears most often as the LEFT endpoint of
    JOIN edges originating from the view's main scope. This works
    because `build_graph` consistently records joins as
    (from_table -> right_table) within a scope -- from_table is the
    scope's first-seen table (typically the FROM clause).

    Returns the table's `label` attribute (e.g., "PATIENT"), or None if
    the view has no JOIN edges (single-table views, or views whose only
    tables are read from a CTE).

    This is used for weak-member reporting: "VW_FOO is in claims via
    claimC, but its driver is PATIENT" makes outlier patterns obvious.
    """
    # Count how many times each table appears as the LEFT side of a JOIN.
    left_counts: Counter = Counter()
    for u, v, attrs in g.edges(data=True):
        if attrs.get("relation") != "JOIN":
            continue
        if attrs.get("view") != view_name:
            continue
        # Only count joins from the main scope. JOINs in CTEs aren't the
        # view's "driver" in the user-facing sense.
        if attrs.get("scope") != "main":
            continue
        left_counts[u] += 1

    if not left_counts:
        return None

    # Most common left endpoint wins.
    most_common_node = left_counts.most_common(1)[0][0]
    # Return the human-readable label, not the namespaced node ID.
    return g.nodes[most_common_node].get("label", most_common_node)


def classify_views_by_strength(
    view_strength: dict[str, dict[int, float]],
    communities: list[set],
    threshold: float = 0.5,
) -> dict[int, dict[str, list[str]]]:
    """Split each community's views into "strong" and "weak" members.

    Parameters
    ----------
    view_strength : output of `compute_view_membership_strength`
    communities   : list of community member-sets (used to know which
                     community indices exist; unused otherwise)
    threshold     : fraction; views with primary-community strength >=
                     this are "strong", below are "weak". Default 0.5.

    Returns
    -------
    A dict: community_idx -> {"strong": [view_names...], "weak": [view_names...]}.
    Strong/weak lists are sorted alphabetically. A view appears in
    exactly one community's bucket (its primary's), like the existing
    primary-community assignment.
    """
    result: dict[int, dict[str, list[str]]] = {
        i: {"strong": [], "weak": []} for i in range(len(communities))
    }

    for view_name, strengths in view_strength.items():
        if not strengths:
            # View touches only bridges -- no primary community to report against.
            continue
        # Primary = the community with the highest membership fraction.
        primary_community, primary_fraction = max(strengths.items(), key=lambda kv: kv[1])
        bucket = "strong" if primary_fraction >= threshold else "weak"
        result[primary_community][bucket].append(view_name)

    # Sort each bucket alphabetically for stable output.
    for buckets in result.values():
        buckets["strong"].sort()
        buckets["weak"].sort()

    return result
