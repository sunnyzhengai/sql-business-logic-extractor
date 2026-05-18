"""Table-only projection of the corpus graph (input to community detection).

Most community-detection algorithms (Louvain, etc.) expect an
*undirected, weighted, single-node-type* graph. Our unified graph from
p20_index is a `nx.MultiDiGraph` with mixed node types (view, scope,
table, column) and several edge relations. To run Louvain we project
down to tables only and let edge weights summarize the structural
relationship between them.

The weight on an edge between two tables = number of distinct scopes
across the corpus in which those two tables co-occur (i.e., are joined
together in the same query block). Tables that are *joined together
often* end up close in the graph; tables that share only one or two
co-occurrences are loosely connected; tables that never appear in the
same scope have no edge at all.

Historical note
---------------
This module was previously `tools.operate.validate_graph_pivot.extract_table_projection`.
In Phase 2c of the 2026-05 restructure it moved here -- the projection
is part of the GOVERN layer (input to p30_analyze.communities), not
specific to the validation diagnostic. validate_graph_pivot still
imports it from here.
"""

from __future__ import annotations

from collections import Counter


def extract_table_projection(g):
    """Project the full graph to an undirected, weighted, table-only Graph.

    Edge weight = number of times two tables co-appear in a scope across the
    corpus (i.e., the number of `CO_OCCURS_IN_SCOPE` edges between them in
    the input MultiDiGraph). This is what Louvain consumes.

    Parameters
    ----------
    g : nx.MultiDiGraph from p20_index.graph_builder.build_graph

    Returns
    -------
    table_g : nx.Graph -- undirected, weighted; only `table` nodes; edges
              have a `weight` attribute (positive integer).
    """
    import networkx as nx
    table_g = nx.Graph()

    # First: copy table nodes (we want all tables, even those with no co-occurrences).
    for node, attrs in g.nodes(data=True):
        if attrs.get("ntype") == "table":
            table_g.add_node(node, **attrs)

    # Second: aggregate CO_OCCURS_IN_SCOPE edges into weighted undirected edges.
    # Iterate over every edge in the original MultiDiGraph and accumulate weights
    # in a Counter, then write them into the projection graph at the end.
    weights: Counter = Counter()
    for u, v, attrs in g.edges(data=True):
        if attrs.get("relation") != "CO_OCCURS_IN_SCOPE":
            continue
        # Normalize edge direction so (A, B) and (B, A) collapse into the same key.
        key = tuple(sorted([u, v]))
        weights[key] += 1

    for (u, v), w in weights.items():
        if u in table_g and v in table_g:
            table_g.add_edge(u, v, weight=w)

    return table_g
