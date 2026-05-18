"""Assign each view to a primary community + identify cross-domain spans.

A view's tables can span multiple communities (especially if it joins
across subject areas -- e.g., a clinical view that also reaches into
billing tables). To make per-community reporting clean, each view is
assigned to ONE primary community (the one containing the most of its
tables). Views that touch multiple communities are also reported
separately as "cross-domain views" -- a steward finding in its own
right: should this view be split? Is it serving two audiences?

Algorithm
---------
For each view:
  1. Count how many of its tables fall in each community.
  2. The community with the highest count = the view's PRIMARY.
  3. The set of all communities the view touches = its "spans".

Bridges (excluded by `p30_analyze.bridges`) are intentionally NOT in
any community. So when a view joins through PATIENT -- a bridge --
PATIENT doesn't drag the view into the "patient community" (because
there is no such community after bridge exclusion). The view's
primary community is determined by its actual cohort-shaping tables.

Historical note
---------------
This was previously `tools.operate.validate_graph_pivot.assign_views_to_communities`.
In Phase 2c of the 2026-05 restructure it moved here -- it's part of
the GOVERN layer, not specific to the validation diagnostic.
"""

from __future__ import annotations

from collections import Counter, defaultdict


def assign_views_to_communities(
    g, communities: list[set]
) -> tuple[dict[int, set[str]], dict[str, list[int]]]:
    """Assign each view a PRIMARY community plus any communities it spans.

    Parameters
    ----------
    g           : the full nx.MultiDiGraph (from p20_index.graph_builder.build_graph)
    communities : list of sets of table-node IDs (from
                  p30_analyze.communities.detect_table_communities)

    Returns
    -------
    community_to_primary_views : dict[int, set[str]]
        community_index -> set of view names whose PRIMARY community is this one.
        Each view appears in EXACTLY ONE community's primary set.

    view_to_spans : dict[str, list[int]]
        view_name -> sorted list of community indices the view touches.
        Length 1 = single-domain view. Length > 1 = cross-domain view
        (a finding in its own right).
    """
    # Reverse map: table_node_id -> community_index, for fast lookup.
    table_to_community: dict[str, int] = {}
    for community_index, member_set in enumerate(communities):
        for table_id in member_set:
            table_to_community[table_id] = community_index

    # For each view, accumulate a count of tables per community.
    # view_table_counts[view_name][community_index] = count
    view_table_counts: dict[str, Counter] = defaultdict(Counter)

    for u, v, attrs in g.edges(data=True):
        relation = attrs.get("relation")
        if relation not in ("READS_FROM_TABLE", "JOIN", "BELONGS_TO"):
            continue
        view_name = attrs.get("view")
        if not view_name:
            continue
        # Either endpoint might be a table in a community; count both,
        # but de-dupe per (view, table) pair so a single join doesn't
        # double-count by being seen from both directions in MultiDiGraph.
        for endpoint in (u, v):
            community_index = table_to_community.get(endpoint)
            if community_index is not None:
                view_table_counts[view_name][community_index] += 1

    community_to_primary_views: dict[int, set[str]] = defaultdict(set)
    view_to_spans: dict[str, list[int]] = {}

    for view_name, counter in view_table_counts.items():
        if not counter:
            continue
        # Counter.most_common(1) returns [(community, count)] -- the max.
        primary_community = counter.most_common(1)[0][0]
        community_to_primary_views[primary_community].add(view_name)
        view_to_spans[view_name] = sorted(counter.keys())

    return community_to_primary_views, view_to_spans
