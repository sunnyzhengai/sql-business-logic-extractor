"""Render networkx graphs (built by `tools.p20_index.graph_builder`) in
formats useful for governance review.

  render_pyvis(g, output_path)   interactive HTML (drag/zoom/click).
                                  Best for in-notebook exploration and
                                  for sharing with non-Fabric reviewers.
  export_graphml(g, output_path)  GraphML for Gephi or other desktop
                                  viewers. Best for offline study.
  render_inline(g)                matplotlib in-notebook static image.
                                  Best for graphs <50 nodes.

All three lazy-import their backend so importing
`tools.p50_present.render` is cheap.

Historical note
---------------
This module was previously `tools.graph_explore.render`. It was renamed
to `tools.p50_present.render` as part of the 2026-05 codebase restructure
(see `tools/PHASES.md`). The build counterpart lives at
`tools.p20_index.graph_builder` (formerly `tools.graph_explore.build`).
"""

from __future__ import annotations

from pathlib import Path


# Color + shape per node type. Tweakable; pyvis and matplotlib both
# read from the same dict.
_NODE_STYLE = {
    "view":   {"color": "#1f77b4", "shape": "diamond"},
    "scope":  {"color": "#2ca02c", "shape": "box"},
    "column": {"color": "#ff7f0e", "shape": "ellipse"},
    "table":  {"color": "#d62728", "shape": "ellipse"},
    "filter": {"color": "#9467bd", "shape": "triangle"},
}
# ZC tables get a muted color so they don't visually dominate
_TABLE_ZC_COLOR = "#aaaaaa"


def _node_color(attrs: dict) -> str:
    ntype = attrs.get("ntype", "")
    style = _NODE_STYLE.get(ntype, {"color": "#cccccc"})
    if ntype == "table" and attrs.get("is_zc"):
        return _TABLE_ZC_COLOR
    return style["color"]


# ---------- pyvis (interactive HTML) -------------------------------------

def render_pyvis(
    g,
    output_html_path: str | Path,
    *,
    height: str = "800px",
    width: str = "100%",
    physics: bool = True,
) -> str:
    """Render `g` as interactive HTML via pyvis. Returns the path written.

    `physics=True` runs a force-directed layout in the browser; turn
    off for very dense graphs where the simulation never settles.
    """
    from pyvis.network import Network

    net = Network(
        height=height, width=width,
        directed=True, notebook=False,
        cdn_resources="in_line",
    )

    for node, attrs in g.nodes(data=True):
        net.add_node(
            node,
            label=attrs.get("label", str(node)),
            title=attrs.get("title", ""),
            color=_node_color(attrs),
            shape=_NODE_STYLE.get(attrs.get("ntype", ""), {}).get("shape", "ellipse"),
        )

    seen_edges: set = set()
    for u, v, attrs in g.edges(data=True):
        rel = attrs.get("relation", "")
        # MultiDiGraph can produce parallel edges with the same label;
        # let pyvis collapse them by giving each a unique title but
        # the same label.
        key = (u, v, rel)
        if key in seen_edges:
            continue
        seen_edges.add(key)
        title = rel
        if rel == "JOINS" and attrs.get("join_type"):
            title = f"{rel} ({attrs['join_type']})"
        net.add_edge(u, v, label=rel, title=title, arrows="to")

    if not physics:
        # vis.js shorthand for "stop animating"
        net.toggle_physics(False)

    out = Path(output_html_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    net.write_html(str(out), notebook=False)
    return str(out)


# ---------- GraphML export (Gephi etc.) ----------------------------------

def export_graphml(g, output_path: str | Path) -> str:
    """Write `g` to a GraphML file for offline graph viewers (Gephi,
    yEd, etc.). Handles attribute-type coercion for non-string values
    by stringifying them, since GraphML is strict about types."""
    import networkx as nx
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    # Stringify problematic attrs so write_graphml doesn't reject them.
    g_clean = nx.MultiDiGraph()
    for n, a in g.nodes(data=True):
        g_clean.add_node(n, **{k: (str(v) if v is None or isinstance(v, (list, dict))
                                   else v)
                                for k, v in a.items()})
    for u, v, a in g.edges(data=True):
        g_clean.add_edge(u, v, **{k: (str(val) if val is None or
                                       isinstance(val, (list, dict))
                                       else val)
                                   for k, val in a.items()})
    nx.write_graphml(g_clean, str(out))
    return str(out)


# ---------- matplotlib (quick inline) ------------------------------------

def render_inline(g, *, max_nodes: int = 100, figsize=(12, 8)) -> None:
    """Quick static matplotlib render. Recommended only for graphs
    smaller than `max_nodes` -- past that threshold layouts get
    unreadable. Prints a warning if you exceed it.

    Best used inside a notebook cell where the figure renders directly.
    """
    import matplotlib.pyplot as plt
    import networkx as nx

    n = g.number_of_nodes()
    if n > max_nodes:
        print(
            f"WARNING: graph has {n} nodes (> {max_nodes}). The matplotlib "
            f"layout will be hard to read; try render_pyvis() instead."
        )

    plt.figure(figsize=figsize)
    pos = nx.spring_layout(g, k=0.6, seed=42)
    node_colors = [_node_color(g.nodes[n]) for n in g.nodes]
    labels = {n: g.nodes[n].get("label", str(n)) for n in g.nodes}
    nx.draw(
        g, pos,
        node_color=node_colors,
        with_labels=True,
        labels=labels,
        node_size=400,
        font_size=8,
        arrows=True,
    )
    plt.tight_layout()
    plt.show()
