"""Graph panel — render join paths and funnels as HTML for Streamlit.

Uses pyvis for interactive table graphs. Renders join paths in the
correct SQL join order (not random FK edges).
"""

from __future__ import annotations


def render_join_path(
    path: list[str],
    join_labels: dict[tuple[str, str], str] | None = None,
    highlight_index: int | None = None,
    fk_graph=None,
    height: str = "300px",
) -> str:
    """Render a SQL join path as a linear graph.

    Parameters
    ----------
    path            : table names in join order, e.g., ["PATIENT", "PROBLEM_LIST", "CLARITY_EDG"]
    join_labels     : optional (from_table, to_table) → label for the edge
    highlight_index : index in path to highlight (current step)
    fk_graph        : networkx DiGraph for FK column labels
    height          : CSS height

    Returns
    -------
    HTML string (self-contained)
    """
    from pyvis.network import Network

    net = Network(height=height, width="100%", directed=True,
                  cdn_resources="in_line")
    net.toggle_physics(False)

    join_labels = join_labels or {}

    # Lay out nodes left-to-right
    x_spacing = 250
    for i, table in enumerate(path):
        is_highlight = (highlight_index is not None and i == highlight_index)
        color = "#2563eb" if is_highlight else "#e2e8f0"
        font_color = "#ffffff" if is_highlight else "#1e293b"
        border = "#1d4ed8" if is_highlight else "#94a3b8"

        net.add_node(
            table, label=table,
            x=i * x_spacing, y=0,
            color={"background": color, "border": border,
                   "highlight": {"background": "#3b82f6", "border": "#1d4ed8"}},
            font={"size": 13, "face": "Inter, sans-serif", "color": font_color},
            size=30,
            shape="box",
            borderWidth=2,
        )

    # Add edges in path order
    for i in range(len(path) - 1):
        t_from = path[i]
        t_to = path[i + 1]

        # Get FK label from graph or from provided labels
        label = join_labels.get((t_from, t_to), "")
        if not label and fk_graph is not None:
            if fk_graph.has_edge(t_from, t_to):
                label = fk_graph.edges[t_from, t_to].get("fk_column", "")
            elif fk_graph.has_edge(t_to, t_from):
                label = fk_graph.edges[t_to, t_from].get("fk_column", "")

        net.add_edge(t_from, t_to, label=label, arrows="to",
                     color="#64748b", width=2,
                     font={"size": 10, "face": "monospace", "color": "#64748b"})

    return net.generate_html()


def render_multi_path(
    paths: list[list[str]],
    fk_graph=None,
    highlight_tables: list[str] | None = None,
    height: str = "350px",
) -> str:
    """Render multiple join paths that share a common root.

    Used when showing all tables involved in a multi-step query.
    """
    from pyvis.network import Network

    net = Network(height=height, width="100%", directed=True,
                  cdn_resources="in_line")
    net.toggle_physics(False)

    highlight_set = set(t.upper() for t in (highlight_tables or []))

    # Collect all unique tables and edges
    all_tables = []
    seen_tables = set()
    edges = []

    for path in paths:
        for table in path:
            t = table.upper()
            if t not in seen_tables:
                all_tables.append(t)
                seen_tables.add(t)
        for i in range(len(path) - 1):
            edges.append((path[i].upper(), path[i + 1].upper()))

    # Layout: use a tree-like arrangement
    # First table at top, branch downward
    positions = {}
    y_level = {}

    # BFS-style positioning
    if all_tables:
        root = all_tables[0]
        positions[root] = (300, 0)
        y_level[root] = 0

        placed = {root}
        queue = [root]
        while queue:
            current = queue.pop(0)
            cy = y_level[current]
            children = []
            for e_from, e_to in edges:
                if e_from == current and e_to not in placed:
                    children.append(e_to)
                    placed.add(e_to)
                elif e_to == current and e_from not in placed:
                    children.append(e_from)
                    placed.add(e_from)

            for ci, child in enumerate(children):
                cx = positions[current][0] + (ci - len(children) / 2) * 220
                positions[child] = (int(cx), (cy + 1) * 120)
                y_level[child] = cy + 1
                queue.append(child)

        # Place any remaining unpositioned tables
        for i, t in enumerate(all_tables):
            if t not in positions:
                positions[t] = (i * 200, 300)

    # Add nodes
    for table in all_tables:
        is_highlight = table in highlight_set
        color = "#2563eb" if is_highlight else "#f1f5f9"
        font_color = "#ffffff" if is_highlight else "#1e293b"
        border = "#1d4ed8" if is_highlight else "#cbd5e1"
        x, y = positions.get(table, (0, 0))

        net.add_node(
            table, label=table,
            x=x, y=y,
            color={"background": color, "border": border},
            font={"size": 12, "face": "Inter, sans-serif", "color": font_color},
            size=25, shape="box", borderWidth=2,
        )

    # Add edges
    seen_edges = set()
    for e_from, e_to in edges:
        edge_key = (e_from, e_to)
        if edge_key in seen_edges:
            continue
        seen_edges.add(edge_key)

        label = ""
        if fk_graph is not None:
            if fk_graph.has_edge(e_from, e_to):
                label = fk_graph.edges[e_from, e_to].get("fk_column", "")
            elif fk_graph.has_edge(e_to, e_from):
                label = fk_graph.edges[e_to, e_from].get("fk_column", "")

        net.add_edge(e_from, e_to, label=label, arrows="to",
                     color="#94a3b8", width=2,
                     font={"size": 9, "face": "monospace", "color": "#94a3b8"})

    return net.generate_html()


def render_funnel(steps: list[dict]) -> str:
    """Render a query funnel as clean HTML.

    Parameters
    ----------
    steps : list of {"label": str, "count": int|float, "approved": bool}
    """
    if not steps:
        return ""

    max_count = max(s.get("count", 0) for s in steps) or 1

    rows = []
    for i, step in enumerate(steps):
        count = step.get("count", 0)
        label = step.get("label", "")
        approved = step.get("approved", False)

        if isinstance(count, float):
            count_str = f"{count}%"
            width_pct = 50
        else:
            count_str = f"{count:,}"
            width_pct = max(15, int(count / max_count * 100))

        # Use theme-safe colors
        bg = "#2563eb" if approved else "#e5e7eb"
        fg = "#ffffff" if approved else "#111827"
        arrow = "-> " if i > 0 else ""

        rows.append(f'''
        <div style="display: flex; align-items: center; margin: 6px 0; gap: 12px;">
            <div style="background: {bg}; color: {fg}; width: {width_pct}%;
                        padding: 8px 14px; border-radius: 6px; min-width: 70px;
                        font-weight: 600; font-size: 14px; text-align: right;">
                {count_str}
            </div>
            <div style="color: #111827; font-size: 13px;">
                {arrow}{label}
            </div>
        </div>
        ''')

    return f'''
    <div style="padding: 8px 0; background: #ffffff;">
        {"".join(rows)}
    </div>
    '''
