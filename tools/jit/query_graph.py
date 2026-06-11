"""FK graph and join path finder for the query builder.

Builds a NetworkX DiGraph from clarity_schema.yaml where:
  - Each table is a node (with description, primary_key, columns)
  - Each FK relationship is a directed edge from child → parent
    (e.g., REFERRAL → PATIENT via PAT_ID)
  - Edges carry join metadata: fk_column, pk_column, cardinality

The graph represents all **legal joins** in Clarity. The path finder
uses it to enumerate join paths between any two tables.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional


def build_fk_graph(schema_path: str | Path):
    """Build a directed graph of FK relationships from clarity_schema.yaml.

    Nodes = tables. Edges = FK relationships (child → parent).
    Each edge has: fk_column, pk_column, cardinality.
    Each node has: description, primary_key.

    Returns a networkx.DiGraph.
    """
    import yaml
    import networkx as nx

    with open(schema_path, encoding="utf-8") as f:
        schema = yaml.safe_load(f)

    g = nx.DiGraph()

    # Add table nodes
    for table in schema.get("tables") or []:
        name = table.get("name", "").upper()
        if not name:
            continue
        g.add_node(name,
                   description=table.get("description", ""),
                   primary_key=table.get("primary_key", ""),
                   is_enum=table.get("is_enum_table", False))

    # Add FK edges from column-level foreign_key declarations
    for table in schema.get("tables") or []:
        from_table = table.get("name", "").upper()
        for col in table.get("columns") or []:
            fk = col.get("foreign_key")
            if not fk:
                continue
            to_table = (fk.get("table") or "").upper()
            fk_col = col.get("name", "")
            pk_col = fk.get("column", "")
            cardinality = fk.get("cardinality", "")

            if not to_table or to_table not in g:
                continue

            # Avoid duplicate edges (same from→to can have multiple FKs,
            # e.g., REFERRAL has PCP_PROV_ID and REFERRING_PROV_ID both → CLARITY_SER).
            # Use the fk_column as part of the edge key.
            # For DiGraph (no multi-edges), keep the first one; for path finding
            # the important thing is that the edge exists.
            if not g.has_edge(from_table, to_table):
                g.add_edge(from_table, to_table,
                           fk_column=fk_col,
                           pk_column=pk_col,
                           cardinality=cardinality)
            # Store additional FK columns as a list for tables with multiple
            # FKs to the same parent (e.g., REFERRAL → CLARITY_SER via 2 columns)
            else:
                existing = g.edges[from_table, to_table]
                alt_joins = existing.get("alt_joins", [])
                alt_joins.append({"fk_column": fk_col, "pk_column": pk_col})
                existing["alt_joins"] = alt_joins

    # Also process explicit relationships section if present.
    # Skip entries where a ZC table is listed as from_table pointing to a
    # non-ZC table — these are modeled backwards in the YAML (the FK
    # actually lives on the non-ZC table, and the column-level FK
    # declarations above already captured the correct direction).
    for rel in schema.get("relationships") or []:
        from_table = (rel.get("from_table") or "").upper()
        to_table = (rel.get("to_table") or "").upper()
        if not from_table or not to_table:
            continue
        if from_table not in g or to_table not in g:
            continue
        # Skip reversed ZC→non-ZC entries
        if from_table.startswith("ZC_") and not to_table.startswith("ZC_"):
            continue
        if not g.has_edge(from_table, to_table):
            fk_col = rel.get("fk_column", "")
            pk_col = rel.get("target_column", "")
            if not pk_col:
                # Infer PK column from the target table's primary_key
                pk_col = g.nodes[to_table].get("primary_key", "")
            g.add_edge(from_table, to_table,
                       fk_column=fk_col,
                       pk_column=pk_col,
                       cardinality=rel.get("cardinality", ""))

    return g


def find_join_paths(g, source: str, target: str,
                    max_hops: int = 5) -> list[list[dict]]:
    """Find all legal join paths between two tables.

    Uses the undirected view of the FK graph (FKs are navigable in both
    directions via JOIN). Returns paths as lists of step dicts, each with:
      - table: the table name at this step
      - join_from: the previous table (None for the first step)
      - fk_column: the FK column used for the join
      - pk_column: the PK column used for the join
      - direction: "child_to_parent" or "parent_to_child"

    Parameters
    ----------
    g          : DiGraph from build_fk_graph
    source     : starting table name (upper)
    target     : ending table name (upper)
    max_hops   : maximum number of joins (default 5)

    Returns
    -------
    List of paths, each path is a list of step dicts.
    Empty list if no path exists.
    """
    import networkx as nx

    source = source.upper()
    target = target.upper()

    if source not in g or target not in g:
        return []

    ug = g.to_undirected()

    try:
        raw_paths = list(nx.all_simple_paths(ug, source, target, cutoff=max_hops))
    except nx.NetworkXError:
        return []

    if not raw_paths:
        return []

    # Convert raw node-list paths into step dicts with join metadata
    result = []
    for raw_path in raw_paths:
        steps = []
        for i, table in enumerate(raw_path):
            step = {"table": table}
            if i == 0:
                step["join_from"] = None
                step["fk_column"] = None
                step["pk_column"] = None
                step["direction"] = None
            else:
                prev = raw_path[i - 1]
                step["join_from"] = prev
                # Determine direction: is the edge prev→table or table→prev?
                if g.has_edge(prev, table):
                    edge = g.edges[prev, table]
                    step["fk_column"] = edge.get("fk_column", "")
                    step["pk_column"] = edge.get("pk_column", "")
                    step["direction"] = "child_to_parent"
                elif g.has_edge(table, prev):
                    edge = g.edges[table, prev]
                    step["fk_column"] = edge.get("fk_column", "")
                    step["pk_column"] = edge.get("pk_column", "")
                    step["direction"] = "parent_to_child"
                else:
                    step["fk_column"] = ""
                    step["pk_column"] = ""
                    step["direction"] = "unknown"
            steps.append(step)
        result.append(steps)

    # Sort by path length (shortest first)
    result.sort(key=len)
    return result
