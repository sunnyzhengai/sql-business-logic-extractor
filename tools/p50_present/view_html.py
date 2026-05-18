"""Per-view focused HTML rendering.

Renders ONE view's tables and joins as a focused interactive HTML.
Useful for understanding outliers: a view that got assigned to an
unexpected community can be opened, and you immediately see WHICH
tables in the view fall in which community.

Color scheme on the per-view HTML:
  - Tables in the view's PRIMARY community  -> primary community color
  - Tables in OTHER communities             -> their respective community colors
                                                (so cross-domain reach is visible)
  - Bridge tables (dimensions / lookups)    -> muted gray
  - The view's "driver" table (heuristic)   -> heavier border to call it out

The view's own join edges (filtered from the full MultiDiGraph by
`view=<view_name>`) are rendered between the table nodes. Other views'
joins are not shown -- the point is to see THIS view in isolation,
just colored by community membership.

Historical note
---------------
This module was added in Phase 3a (2026-05). Per-view HTML rendering
is a NEW deliverable -- it didn't exist before the restructure. It
joins community_html.py and render.py in p50_present/ as the third
rendering primitive (one-community, one-view, one-graph).
"""

from __future__ import annotations

from pathlib import Path

from tools.p50_present.community_html import (
    BRIDGE_COLOR,
    community_color,
    _compute_static_positions,
    _safe_filename,
)


def render_view_html(
    g,
    view_name: str,
    communities: list[set],
    bridge_tables: set[str],
    output_path: str | Path,
    driver_label: str | None = None,
) -> str:
    """Render ONE view's tables and joins as a focused interactive HTML.

    Parameters
    ----------
    g            : the full nx.MultiDiGraph from p20_index.graph_builder
    view_name    : name of the view to render (the function filters g
                   to nodes and edges that belong to this view)
    communities  : list of community sets (used to color each table by
                   its community membership)
    bridge_tables: set of table node IDs flagged as bridges (will be
                   shown in muted gray)
    output_path  : where to write the HTML
    driver_label : (optional) the human-readable label of the view's
                   driver table; receives a heavier border in the render.
                   Pass `view_driver_table(g, view_name)` from
                   tools.p30_analyze.view_membership to compute.

    Returns
    -------
    The path written, as a string.
    """
    from pyvis.network import Network
    import networkx as nx

    # ---- Identify the tables this view touches --------------------------------
    # Walk the view's edges (READS_FROM_TABLE / JOIN / BELONGS_TO) and collect
    # every table-endpoint. Same logic as compute_view_membership_strength.
    tables_in_view: set[str] = set()
    view_joins: list[tuple[str, str, dict]] = []
    for u, v, attrs in g.edges(data=True):
        relation = attrs.get("relation")
        if attrs.get("view") != view_name:
            continue
        if relation == "JOIN":
            # Capture join edges for rendering as graph edges.
            view_joins.append((u, v, attrs))
            tables_in_view.add(u)
            tables_in_view.add(v)
        elif relation in ("READS_FROM_TABLE", "BELONGS_TO"):
            for endpoint in (u, v):
                if g.nodes[endpoint].get("ntype") == "table":
                    tables_in_view.add(endpoint)

    if not tables_in_view:
        # View has no table relationships -- write a stub HTML so the
        # index link doesn't 404, and return early.
        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(
            f"<!doctype html><html><body>"
            f"<h1>{view_name}</h1>"
            f"<p>This view has no detectable table references "
            f"(no FROM / JOIN / BELONGS_TO edges in the corpus graph).</p>"
            f"</body></html>",
            encoding="utf-8",
        )
        return str(out)

    # ---- Map each table to its community color --------------------------------
    table_to_community: dict[str, int] = {}
    for community_index, member_set in enumerate(communities):
        for table_id in member_set:
            table_to_community[table_id] = community_index

    # ---- Build a small subgraph for layout -----------------------------------
    sub = nx.Graph()
    for t in tables_in_view:
        sub.add_node(t, **g.nodes[t])
    # Add join edges (collapsing duplicates -- same (u,v) joined multiple
    # times in different scopes still draws as one visual edge).
    join_pairs: set[tuple[str, str]] = set()
    for u, v, attrs in view_joins:
        pair = tuple(sorted([u, v]))
        if pair not in join_pairs:
            sub.add_edge(u, v, join_type=attrs.get("join_type", "JOIN"),
                          scope=attrs.get("scope", "main"))
            join_pairs.add(pair)

    positions = _compute_static_positions(sub)

    # ---- Render with pyvis ---------------------------------------------------
    net = Network(
        height="800px", width="100%",
        directed=False, notebook=False,
        cdn_resources="in_line",
    )

    for node, attrs in sub.nodes(data=True):
        label = attrs.get("label", str(node))
        is_zc = attrs.get("is_zc", False)
        is_bridge = node in bridge_tables
        community_idx = table_to_community.get(node)

        # Color: bridge tables muted; everything else colored by its community.
        if is_bridge:
            color = BRIDGE_COLOR
        elif community_idx is not None:
            color = community_color(community_idx)
        else:
            # Table is not in any community AND not flagged as a bridge.
            # Shouldn't usually happen, but render with a neutral color.
            color = "#cccccc"

        # Shape: bridges are diamonds; ZC tables are boxes; driver gets a star;
        # rest are dots.
        if driver_label and label == driver_label:
            shape = "star"
            size = 32
        elif is_bridge:
            shape = "diamond"
            size = 18
        elif is_zc:
            shape = "box"
            size = 15
        else:
            shape = "dot"
            size = 25

        # Tooltip explains the table's role in this view.
        title_lines = [f"Table: {label}"]
        if community_idx is not None:
            title_lines.append(f"Community: {community_idx}")
        if is_bridge:
            title_lines.append("Role: BRIDGE (dimension / shared lookup)")
        if is_zc:
            title_lines.append("Type: ZC lookup")
        if driver_label and label == driver_label:
            title_lines.append("Role: DRIVER (most common left side of JOINs in main scope)")

        x, y = positions.get(node, (0.0, 0.0))
        net.add_node(
            node, label=label, color=color, shape=shape, size=size,
            title="\n".join(title_lines),
            x=x, y=y, physics=False, fixed=True,
        )

    for u, v, attrs in sub.edges(data=True):
        join_type = attrs.get("join_type", "JOIN")
        scope = attrs.get("scope", "main")
        net.add_edge(u, v, label=join_type,
                     title=f"join_type: {join_type}\nscope: {scope}")

    net.toggle_physics(False)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    net.write_html(str(out), notebook=False)
    return str(out)


def view_html_filename(view_name: str) -> str:
    """Return the conventional filename for a per-view HTML.

    Centralized so the orchestrator and the index renderer use the
    same filename. Sanitizes the view name to be filesystem-safe.
    """
    return f"view_{_safe_filename(view_name)}.html"
