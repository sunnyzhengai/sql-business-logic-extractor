"""Per-community common join-edge aggregation (the "common spine").

For each community, walk the JOIN edges in the unified graph and count
how many DISTINCT views inside the community use each (from_table,
to_table) pair. Edges used by many views = the "common spine" the
data modeling team will lift into the certified data model.

Definition of a join edge
-------------------------
build_graph (p20_index) records JOINs as directed edges
   `from_table -> right_table`
within each scope, where `from_table` is the first table in the
scope's `reads_from_tables` list (typically the FROM-clause table)
and `right_table` is the JOIN's right-hand side.

So all JOIN edges in a single scope fan out from one center. This is
"star-shaped" not "chain-shaped" -- which matches what the modeling
team needs: "PATIENT was the driver; it joined to COVERAGE, CLAIM,
CLAIM_LINE." The chain shape (PATIENT -> COVERAGE -> CLAIM ...) would
require traversing SQL ON-clauses to reconstruct, which is a different
problem.

What this module produces
-------------------------
Per community, a sorted list of join-edge records:
  {
      "from_table": "PATIENT",
      "to_table":   "COVERAGE",
      "join_type":  "INNER JOIN",          # most-common variant
      "n_views":    11,
      "views":      ["VW_A", "VW_B", ...],  # sorted list
  }
Sorted by view-count descending (the most-used joins first); the
markdown report in Phase 3e-iii will typically show the top 5-10
per community.
"""

from __future__ import annotations

from collections import Counter, defaultdict


def analyze_join_paths(g, community_to_primary: dict[int, set[str]]) -> dict[int, list[dict]]:
    """For each community, return its common JOIN edges across primary views.

    Parameters
    ----------
    g : nx.MultiDiGraph from p20_index.graph_builder.build_graph
    community_to_primary : map of community_idx -> set of view names whose
        primary community is this one.

    Returns
    -------
    community_idx -> list of join-edge records sorted by view-count desc.
        Each record has: from_table, to_table, join_type, n_views, views.
    """
    # Reverse the primary-community map: view_name -> community_idx.
    view_to_community: dict[str, int] = {}
    for community_idx, view_set in community_to_primary.items():
        for view_name in view_set:
            view_to_community[view_name] = community_idx

    # Walk every JOIN edge in the graph. For each edge:
    #   key   = (community_idx, from_label, to_label)
    #   data  = { "view_set": set, "join_types": [...], "on_expressions": [...] }
    # We collect join_types AND on_expressions as lists so we can report
    # the most-common variant per edge (some edges might be INNER in some
    # views and LEFT in others -- a finding worth noting; same for ON clauses).
    grouped: dict[tuple[int, str, str], dict] = defaultdict(
        lambda: {"view_set": set(), "join_types": [], "on_expressions": []}
    )

    for u, v, attrs in g.edges(data=True):
        if attrs.get("relation") != "JOIN":
            continue
        view_name = attrs.get("view")
        community_idx = view_to_community.get(view_name)
        if community_idx is None:
            continue
        from_label = g.nodes[u].get("label", u)
        to_label = g.nodes[v].get("label", v)
        join_type = attrs.get("join_type") or "JOIN"
        on_expression = attrs.get("on_expression") or ""
        key = (community_idx, from_label, to_label)
        grouped[key]["view_set"].add(view_name)
        grouped[key]["join_types"].append(join_type)
        if on_expression:
            grouped[key]["on_expressions"].append(on_expression)

    # Materialize per-community lists of join-edge records.
    result: dict[int, list[dict]] = {idx: [] for idx in community_to_primary}
    for (community_idx, from_label, to_label), data in grouped.items():
        # Pick the most-common join_type as the representative; expose the
        # full counter behind it so callers can detect "INNER in some,
        # LEFT in others" if they care.
        join_type_counts = Counter(data["join_types"])
        most_common_type, _ = join_type_counts.most_common(1)[0]
        n_distinct_join_types = len(join_type_counts)
        # Most-common ON expression -- empty string if none of the views
        # carried one (some corpus extractors don't capture ON clauses).
        on_expr_counts = Counter(data["on_expressions"])
        most_common_on = on_expr_counts.most_common(1)[0][0] if on_expr_counts else ""
        n_distinct_on_expressions = len(on_expr_counts)
        result.setdefault(community_idx, []).append({
            "from_table": from_label,
            "to_table": to_label,
            "join_type": most_common_type,
            "n_distinct_join_types": n_distinct_join_types,
            "on_expression": most_common_on,
            "n_distinct_on_expressions": n_distinct_on_expressions,
            "n_views": len(data["view_set"]),
            "views": sorted(data["view_set"]),
        })

    # Sort each community's edges by importance (most views first; ties
    # broken alphabetically for determinism).
    for community_idx in result:
        result[community_idx].sort(key=lambda r: (
            -r["n_views"], r["from_table"], r["to_table"],
        ))

    return result


def count_join_edges(joins: dict[int, list[dict]]) -> int:
    """Total distinct (community, from, to) join edges across all communities."""
    return sum(len(records) for records in joins.values())
