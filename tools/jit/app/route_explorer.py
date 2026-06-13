"""Route Explorer — show all known paths to a destination table.

When the user's question involves a concept category (diagnosis, medication,
etc.), this module finds all known routes from the current graph to the
target tables, previews the count for each, and renders them as alternative
branches in the graph.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional

import yaml

DATA_DIR = Path(__file__).resolve().parents[1] / "data"


def get_routes_for_question(
    question_tokens: set[str],
    existing_tables: set[str],
    learned_terms: dict,
    route_catalog_path: Path | None = None,
) -> list[dict]:
    """Find applicable route groups based on the question.

    Returns list of route groups, each with:
    - category: str (e.g., "diagnosis")
    - reason: why this category was identified
    - routes: list of route dicts from the catalog
    """
    route_path = route_catalog_path or DATA_DIR / "route_catalog.yaml"
    with open(route_path) as f:
        catalog = yaml.safe_load(f)

    results = []

    # Check learned terms for category matches
    for key, term in learned_terms.items():
        aliases = set(a.lower() for a in term.get("aliases", []))
        aliases.add(term.get("term", "").lower())

        if question_tokens & aliases:
            category = term.get("category", "")
            if category and category in catalog:
                cat_routes = catalog[category]
                results.append({
                    "category": category,
                    "reason": f"matched term '{term.get('term', key)}'",
                    "routes": cat_routes.get("routes", []),
                })

    # Deduplicate by category
    seen = set()
    deduped = []
    for r in results:
        if r["category"] not in seen:
            deduped.append(r)
            seen.add(r["category"])

    return deduped


def preview_route_counts(
    routes: list[dict],
    conn: sqlite3.Connection,
    filter_expression: str = "",
) -> list[dict]:
    """Run each route's SQL to preview how many rows it produces.

    Parameters
    ----------
    routes          : route dicts from the catalog (each has "path" list)
    conn            : database connection
    filter_expression : optional WHERE clause to apply to CLARITY_EDG
                       (e.g., "CURRENT_ICD10_LIST LIKE 'E11%'")

    Returns routes with added "preview_count" and "preview_sql" fields.
    """
    results = []

    for route in routes:
        path = route.get("path", [])
        if len(path) < 2:
            continue

        # Build the JOIN chain
        from_table = path[0]
        sql = f"SELECT COUNT(DISTINCT {from_table}.PAT_ID) FROM {from_table}"

        for i in range(1, len(path)):
            prev = path[i - 1]
            curr = path[i]
            # Use common FK patterns
            join_col = _infer_join_column(prev, curr)
            sql += f"\nJOIN {curr} ON {curr}.{join_col} = {prev}.{join_col}"

        if filter_expression:
            sql += f"\nWHERE {filter_expression}"

        count = None
        try:
            cur = conn.cursor()
            cur.execute(sql)
            count = cur.fetchone()[0]
        except Exception:
            pass

        results.append({
            **route,
            "preview_count": count,
            "preview_sql": sql,
        })

    return results


def build_route_graph_nodes(
    route_groups: list[dict],
    existing_tables: set[str],
) -> tuple[list[dict], list[dict]]:
    """Build graph nodes and edges for route visualization.

    Returns (nodes, edges) where each node has:
    - id, label, title, color, size, is_route_node, route_index
    and each edge has:
    - source, target, label, dashes, route_index
    """
    nodes = []
    edges = []
    added_nodes = set()

    # Color palette for different routes
    route_colors = ["#93c5fd", "#86efac", "#fde68a", "#fca5a5"]

    for gi, group in enumerate(route_groups):
        for ri, route in enumerate(group.get("routes", [])):
            path = route.get("path", [])
            color = route_colors[ri % len(route_colors)]
            count = route.get("preview_count")
            count_str = f"{count:,}" if count is not None else "?"

            for i, table in enumerate(path):
                node_id = table

                if node_id not in added_nodes:
                    is_existing = table in existing_tables
                    is_destination = (i == len(path) - 1)

                    if is_existing:
                        node_color = "#3b82f6"
                        size = 30
                    elif is_destination:
                        node_color = "#f59e0b"  # amber for destination
                        size = 28
                    else:
                        node_color = color
                        size = 22

                    label = table
                    if is_destination and count is not None:
                        label = f"{table}\n({count_str})"

                    nodes.append({
                        "id": node_id,
                        "label": label,
                        "title": f"{table} — Route {ri+1}: {route.get('name', '')}",
                        "color": node_color,
                        "size": size,
                        "is_existing": is_existing,
                        "route_index": ri,
                    })
                    added_nodes.add(node_id)

                # Edge
                if i > 0:
                    prev = path[i - 1]
                    join_col = _infer_join_column(prev, table)
                    is_existing_edge = (prev in existing_tables and table in existing_tables)

                    edges.append({
                        "source": prev,
                        "target": table,
                        "label": join_col,
                        "dashes": not is_existing_edge,
                        "color": color if not is_existing_edge else "#475569",
                        "route_index": ri,
                        "route_name": route.get("name", ""),
                    })

    return nodes, edges


def _infer_join_column(table_a: str, table_b: str) -> str:
    """Infer the FK join column between two tables based on naming conventions."""
    known_joins = {
        ("PATIENT", "PROBLEM_LIST"): "PAT_ID",
        ("PATIENT", "PAT_ENC"): "PAT_ID",
        ("PATIENT", "HSP_ACCOUNT"): "PAT_ID",
        ("PAT_ENC", "PAT_ENC_DX"): "PAT_ENC_CSN_ID",
        ("PAT_ENC", "PAT_ENC_HSP"): "PAT_ENC_CSN_ID",
        ("PAT_ENC_HSP", "HSP_ADMIT_DIAG"): "PAT_ENC_CSN_ID",
        ("HSP_ACCOUNT", "HSP_ACCT_DX_LIST"): "HSP_ACCOUNT_ID",
        ("PROBLEM_LIST", "CLARITY_EDG"): "DX_ID",
        ("PAT_ENC_DX", "CLARITY_EDG"): "DX_ID",
        ("HSP_ADMIT_DIAG", "CLARITY_EDG"): "DX_ID",
        ("HSP_ACCT_DX_LIST", "CLARITY_EDG"): "DX_ID",
    }
    key = (table_a.upper(), table_b.upper())
    if key in known_joins:
        return known_joins[key]
    # Reverse
    key_rev = (table_b.upper(), table_a.upper())
    if key_rev in known_joins:
        return known_joins[key_rev]
    return "PAT_ID"  # fallback
