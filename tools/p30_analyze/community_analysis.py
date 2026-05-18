"""Per-community summary -- top tables, leaf tables, cohort-shaping tables.

Once communities are identified and views assigned to primaries, we need
a per-community summary that downstream synthesis (p40_synthesize)
turns into steward-readable artifacts and downstream presentation
(p50_present) uses for rendering.

The summary distinguishes:

  - top_tables    -- tables in the community ranked by total JOIN
                     traversal (in_degree + out_degree). Useful for
                     naming the community ("this is the encounter cluster
                     because PAT_ENC has the most joins").
  - core_tables   -- cohort-shaping tables (out_degree >= 1). These
                     drive the cohort; downstream views read from them.
  - leaf_tables   -- decorative tables (in_degree > 0 but out_degree = 0).
                     Typically lookup tables (ZC_*) that contribute
                     labels but don't shape membership.

Historical note
---------------
This was previously `tools.operate.validate_graph_pivot.analyze_community`.
In Phase 2c of the 2026-05 restructure it moved here -- per-community
analysis is the natural finishing step of p30_analyze, before
synthesis hands off to markdown/HTML rendering.
"""

from __future__ import annotations


def analyze_community(g, community_tables: set[str],
                       primary_views: set[str]) -> dict:
    """Summarize one community for downstream synthesis + presentation.

    Parameters
    ----------
    g                : nx.MultiDiGraph (the full graph from p20_index)
    community_tables : set of table node IDs in this community
    primary_views    : set of view names whose primary community is this one

    Returns
    -------
    A summary dict with these keys:
      - n_tables          : int    -- number of tables in this community
      - n_primary_views   : int    -- views whose PRIMARY community is this one
      - top_tables        : list of (table_label, in+out_degree) -- ranked
      - leaf_tables       : sorted list of table labels (decorative/lookups)
      - core_tables       : sorted list of table labels (cohort-shaping)
      - primary_views     : sorted list of view names
      - zc_table_count    : int    -- how many tables in the community are ZC_*
      - table_node_ids    : set    -- the raw node IDs (for downstream renderers)
    """
    # Compute degrees inside the FULL graph (which includes JOIN edges with
    # direction information). A "leaf" in our usage = a table that other tables
    # join TO but never join FROM. These are typically lookup tables (ZC_*).

    table_in_degrees: dict[str, int] = {}
    table_out_degrees: dict[str, int] = {}
    for table_id in community_tables:
        # In-degree of a table = how many JOIN edges point INTO this table.
        # Out-degree = how many JOIN edges point OUT of this table.
        in_count = 0
        out_count = 0
        for _, _, attrs in g.in_edges(table_id, data=True):
            if attrs.get("relation") == "JOIN":
                in_count += 1
        for _, _, attrs in g.out_edges(table_id, data=True):
            if attrs.get("relation") == "JOIN":
                out_count += 1
        table_in_degrees[table_id] = in_count
        table_out_degrees[table_id] = out_count

    def label(table_id: str) -> str:
        # `g.nodes[id].get("label", id)` reads the visible label set during
        # graph construction; fall back to the raw ID if missing.
        return g.nodes[table_id].get("label", table_id)

    leaf_tables = [label(t) for t in community_tables
                   if table_out_degrees[t] == 0 and table_in_degrees[t] > 0]
    core_tables = [label(t) for t in community_tables
                   if table_out_degrees[t] >= 1]

    # Top tables by total traversal (in + out).
    top_tables_pairs = sorted(
        community_tables,
        key=lambda t: table_in_degrees[t] + table_out_degrees[t],
        reverse=True,
    )
    top_tables = [
        (label(t), table_in_degrees[t] + table_out_degrees[t])
        for t in top_tables_pairs
    ]

    zc_count = sum(1 for t in community_tables if g.nodes[t].get("is_zc"))

    return {
        "n_tables": len(community_tables),
        "n_primary_views": len(primary_views),
        "top_tables": top_tables[:15],   # cap at 15 for readability
        "leaf_tables": sorted(leaf_tables),
        "core_tables": sorted(core_tables),
        "primary_views": sorted(primary_views),
        "zc_table_count": zc_count,
        # The full set of community-tables (raw node IDs, not labels) for
        # downstream use by the per-community renderer.
        "table_node_ids": set(community_tables),
    }
