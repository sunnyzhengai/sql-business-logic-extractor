"""Recursively expand view-of-view references to base tables.

When a BI shop has built foundation views (V_* / F_* / etc.) that
downstream report views read from, the graph stores those foundation
views as TABLE nodes (because reads_from_tables doesn't distinguish
"table" from "view-treated-as-table"). Two report views that BOTH
read from a foundation view appear to "share a table" in graph
terms -- but the substrate similarity is mostly illusion because the
foundation view itself joins many base tables.

For honest similarity scoring AND community detection, we want the
matrix and the Louvain projection to see the BASE TABLES that each
view ultimately reads from, traversing through any foundation-view
layers. This module provides:

  - expand_view_to_base_tables(view_to_tables_map, view_names) --
    recursive walk that replaces foundation-view neighbors with
    their base tables.
  - build_expanded_table_projection(g, views) -- builds a table-
    projection graph where co-occurrence edges connect BASE tables
    only, ready for Louvain.

Cycle-safe via per-traversal visited sets.
"""

from __future__ import annotations

from collections import Counter


def _bare(t: str) -> str:
    """Strip the 'table::' graph-node prefix if present."""
    return t[len("table::"):] if t.startswith("table::") else t


def expand_view_to_base_tables(
    view_to_tables: dict[str, set[str]],
    view_names: set[str],
    *,
    max_iterations: int = 100,
) -> dict[str, set[str]]:
    """For each view, return its set of BASE tables (recursive).

    A "base table" is anything in `view_to_tables[view_name]` that is
    NOT itself a key in `view_names`. View entries get expanded by
    substituting their own current expansion -- iterated to a fixed
    point so cycles converge instead of looping.

    Foundation views that aren't in the corpus (referenced but not
    extracted) are treated as base tables -- we have no further
    information about them.

    Implementation: fixed-point iteration rather than recursion-with-
    caching. The recursive approach can cache an INCOMPLETE answer for
    one cycle participant before its peer finishes; the iterative
    approach lets all participants stabilize together. After the loop
    settles, a final pass strips any view-name entries that survived
    (would only happen with cycles).

    Parameters
    ----------
    view_to_tables : dict view_name -> set of table identifiers (may
        include "table::" prefix from the graph).
    view_names : set of all view names in the corpus -- used to decide
        whether a "table" neighbor is actually a view to expand.
    max_iterations : safety bound on the fixed-point loop. Real
        view-of-view depth in practice is single digits; 100 is paranoid.

    Returns
    -------
    dict view_name -> set of base table identifiers (same prefix
    convention as input).
    """
    # Seed each view's result with its own direct neighbors.
    result: dict[str, set[str]] = {
        vn: set(view_to_tables.get(vn, set())) for vn in view_names
    }

    for _ in range(max_iterations):
        changed = False
        for vn in view_names:
            current = result[vn]
            new: set[str] = set()
            for t in current:
                bare = _bare(t)
                if bare in view_names:
                    # Substitute the view-reference with that view's
                    # CURRENT expansion. With self-references (VW_A
                    # reads VW_A), this just re-adds VW_A and converges
                    # next iteration when the final filter strips it.
                    new.update(result[bare])
                else:
                    new.add(t)
            if new != current:
                result[vn] = new
                changed = True
        if not changed:
            break

    # Final filter: drop any view-name entries that survived (only
    # happens with cycles -- the cycle's view names stay in everyone's
    # set after the iteration converges).
    for vn in view_names:
        result[vn] = {t for t in result[vn] if _bare(t) not in view_names}
    return result


def build_expanded_table_projection(
    g,
    views: list[dict],
    view_to_tables_map: dict[str, set[str]] | None = None,
):
    """Build a Louvain-ready table projection using expanded base tables.

    Drop-in alternative to `tools.p30_analyze.projection.extract_table_projection`.
    The original uses `CO_OCCURS_IN_SCOPE` edges from the graph; this
    rebuilds co-occurrence at the VIEW level (every pair of tables a
    view reads from co-occurs), using the EXPANDED base-table set for
    each view rather than the raw foundation-view-laden set.

    Why view-level instead of scope-level: foundation-view expansion
    happens at the view boundary -- when we expand `view A reads
    foundation B reads tables [X, Y, Z]`, the X/Y/Z tables aren't in
    any scope of A; they're in scopes of B. Treating the expansion at
    view-level produces a coherent co-occurrence semantics
    (`A effectively reads X, Y, Z` -> X-Y, X-Z, Y-Z all co-occur in A).

    Parameters
    ----------
    g : nx.MultiDiGraph from p20_index.graph_builder.build_graph.
    views : list of ViewV1 dicts (business views).
    view_to_tables_map : optional precomputed view -> tables map. If
        None, computes via tools.p30_analyze.view_membership.view_to_tables.

    Returns
    -------
    (table_g, expanded_view_to_tables) tuple.
      - table_g : nx.Graph weighted projection ready for Louvain.
      - expanded_view_to_tables : dict that callers (matrix renderer)
        can reuse so the SAME expansion drives both clustering and
        display.
    """
    import networkx as nx

    if view_to_tables_map is None:
        from tools.p30_analyze.view_membership import view_to_tables
        view_to_tables_map = view_to_tables(g)

    view_names = {v["view_name"] for v in views if v.get("view_name")}
    expanded = expand_view_to_base_tables(view_to_tables_map, view_names)

    table_g = nx.Graph()
    # Copy table nodes that survived expansion (i.e. real base tables).
    surviving_tables: set[str] = set()
    for tables in expanded.values():
        surviving_tables.update(tables)
    for node, attrs in g.nodes(data=True):
        if attrs.get("ntype") == "table" and node in surviving_tables:
            table_g.add_node(node, **attrs)

    # Co-occurrence at view level over the expanded set.
    weights: Counter = Counter()
    for view_name, tables in expanded.items():
        tables_list = sorted(tables)
        for i, t1 in enumerate(tables_list):
            for t2 in tables_list[i + 1:]:
                key = tuple(sorted([t1, t2]))
                weights[key] += 1

    for (u, v), w in weights.items():
        if u in table_g and v in table_g:
            table_g.add_edge(u, v, weight=w)

    return table_g, expanded
