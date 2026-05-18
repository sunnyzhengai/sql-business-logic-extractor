"""Bridge-table detection (auto-classify high-degree dimension nodes).

"Bridge tables" are dimensions / shared lookups -- tables that almost
every view in the corpus joins through. PATIENT is the canonical
example in healthcare BI: it appears in hundreds of views, connecting
clinical encounters, claims, billing, registry tables, etc.

If we run community detection naively over the table-projection graph,
PATIENT (and friends like CLARITY_SER, CLARITY_DEP, ZC_*) pull every
table into one giant cluster -- bridges dominate the topology. We
exclude them from community detection by detecting them upfront
(`detect_bridge_tables`) and then projecting them out
(`project_without_bridges`).

We do NOT hard-code a dimension list. The GRAPH reveals which tables
are bridges, by degree percentile. If your corpus puts FOO_TABLE in
every view, FOO_TABLE gets classified as a bridge automatically.

Historical note
---------------
These functions were previously `tools.operate.validate_graph_pivot.detect_bridge_tables`
and `project_without_bridges`. In Phase 2c of the 2026-05 restructure
they moved here -- bridge detection is part of the GOVERN layer
(p30_analyze), not specific to the validation diagnostic.
"""

from __future__ import annotations


def detect_bridge_tables(table_g, percentile: float = 90.0) -> set[str]:
    """Identify high-degree "bridge" tables (dimensions / shared lookups).

    Detection: any table whose degree is at or above the given percentile
    of the degree distribution is classified as a bridge. With
    percentile=90, we flag the top 10% by degree.

    Parameters
    ----------
    table_g    : nx.Graph from `extract_table_projection`
    percentile : 90.0 default. Lower -> more tables classified as bridges
                 (more aggressive exclusion). Higher -> fewer bridges,
                 letting Louvain keep more structure.

    Returns
    -------
    bridge_node_ids : set of table node-IDs to exclude from community
                      detection. Empty set if `table_g` has no nodes.

    Notes
    -----
    We additionally require `degree > 1` so that an isolated 2-node
    component (where both nodes are 100th percentile by definition)
    is not over-eagerly flagged as a bridge.
    """
    import numpy as np  # stdlib has statistics.quantiles in 3.8+, but numpy is clearer here

    # Degree = number of distinct neighbors. We want a node-level statistic
    # so we use the simple .degree() view (not the multi-edge count).
    degrees = {node: deg for node, deg in table_g.degree()}
    if not degrees:
        return set()

    # numpy.percentile expects a list/array; degrees.values() is a view, so
    # list() materializes it for the percentile computation.
    cutoff = np.percentile(list(degrees.values()), percentile)
    bridges = {node for node, d in degrees.items() if d >= cutoff and d > 1}
    return bridges


def project_without_bridges(table_g, bridge_nodes: set[str]):
    """Return a copy of `table_g` with bridge tables removed.

    This is what we feed to Louvain. The original `table_g` is preserved
    because downstream visualization still wants bridge tables in the
    rendered graph (shown muted gray so users see how the rest of the
    structure relates to them).
    """
    g = table_g.copy()
    g.remove_nodes_from(bridge_nodes)
    return g
