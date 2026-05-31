"""Corpus landscape map -- Stage 1 of the cross-corpus shape view.

One HTML per corpus, NOT per community. Shows every table in the
corpus laid out via force-directed (spring) layout, colored by its
Louvain community membership. The shapes themselves (clusters of
densely-connected tables in similar colors) ARE the communities --
that's the navigation aid the modeler uses to see how singletons
relate to larger communities and how the whole substrate is laid out.

Deliberately imperfect:

  - No labels by default. With 200+ tables in a corpus, labels would
    overlap into noise. Hover for native browser tooltip showing
    table name + community.
  - Thin grey edges to keep visual clutter low. Communities surface
    through node clustering and color, not edge density.
  - Spring layout with a fixed seed for determinism across runs.

The bottom of the page lists every community with a colored swatch
and a hyperlink to that community's existing
`community_NN_<top>_shapes.html` file -- the map becomes the entry
point to drill into per-community detail.

Public entry points
-------------------
- build_corpus_substrate(views)
- force_directed_layout(nodes, edges, ...)
- render_corpus_overview_svg(...)
- write_corpus_map(views, communities, output_path, ...)
"""

from __future__ import annotations

import html
from collections import defaultdict
from pathlib import Path

from tools.p50_present.community_html import (
    BRIDGE_COLOR,
    community_color,
)
from tools.p50_present.community_matrix import _is_real_table_name


# ===========================================================================
# Helpers
# ===========================================================================

def _bare_table(qualified: str) -> str:
    """Strip schema / brackets; reject SQL fragments. Mirrors the
    same guard view_shape uses so corpus_map and view_shape agree on
    which strings are valid table identifiers."""
    if not qualified:
        return ""
    bare = qualified.split(".")[-1].strip().strip("[]").strip()
    if not _is_real_table_name(qualified):
        return ""
    if not _is_real_table_name(bare):
        return ""
    return bare


# ===========================================================================
# Substrate: union of all tables + join edges across the corpus
# ===========================================================================

def build_corpus_substrate(
    views: list[dict],
) -> tuple[set[str], set[tuple[str, str]]]:
    """Return (nodes, edges) where nodes are deduped bare table names
    across the entire corpus and edges are the union of (table_a,
    table_b) JOIN edges from every view's scopes.

    Deduplication is by bare name -- two views' PAT_ENC nodes are the
    SAME corpus-level node. This is the right level for the landscape
    map (we want to see WHERE PAT_ENC sits in the corpus, not how
    often it appears).
    """
    nodes: set[str] = set()
    edges: set[tuple[str, str]] = set()

    for view in views:
        for scope in view.get("scopes") or []:
            # Bare base-table names this scope reads.
            scope_tables: list[str] = []
            for t in (scope.get("reads_from_tables") or []):
                bare = _bare_table(t)
                if bare:
                    nodes.add(bare)
                    scope_tables.append(bare)

            # JOIN edges. The corpus's joins[].right_table is the
            # right side; the LEFT side is implicit, so we connect
            # the right side to the FIRST base-table in this scope
            # (mirrors graph_builder.py's star-join convention).
            first_table = scope_tables[0] if scope_tables else None
            for join in (scope.get("joins") or []):
                right = _bare_table(join.get("right_table") or "")
                if not right or right == first_table:
                    continue
                nodes.add(right)
                if first_table:
                    edges.add(tuple(sorted([first_table, right])))

    return nodes, edges


# ===========================================================================
# Force-directed layout (networkx spring_layout, seeded)
# ===========================================================================

def force_directed_layout(
    nodes: set[str],
    edges: set[tuple[str, str]],
    *,
    width: int = 1200,
    height: int = 800,
    seed: int = 42,
) -> dict[str, tuple[int, int]]:
    """Compute pixel positions for every node via networkx's
    spring_layout (Fruchterman-Reingold).

    Determinism: fixed `seed` so re-renders give identical coords
    until the underlying corpus changes.

    The raw layout produces (x, y) in roughly [-1, 1] range; we
    rescale to ([margin, width-margin], [margin, height-margin]).
    Empty corpus returns an empty dict.
    """
    if not nodes:
        return {}

    import networkx as nx

    g = nx.Graph()
    for n in nodes:
        g.add_node(n)
    for a, b in edges:
        g.add_edge(a, b)

    # k controls the optimal distance between nodes. Smaller k =
    # denser packing. Default scales with sqrt(N).
    # iterations defaults to 50; bumped slightly for clearer clusters.
    raw = nx.spring_layout(g, seed=seed, iterations=80, k=None)

    if not raw:
        return {}

    xs = [p[0] for p in raw.values()]
    ys = [p[1] for p in raw.values()]
    x_min, x_max = min(xs), max(xs)
    y_min, y_max = min(ys), max(ys)
    x_span = max(x_max - x_min, 1e-6)
    y_span = max(y_max - y_min, 1e-6)

    margin = 40
    avail_w = width - margin * 2
    avail_h = height - margin * 2

    coords: dict[str, tuple[int, int]] = {}
    for node, (rx, ry) in raw.items():
        px = margin + int((rx - x_min) / x_span * avail_w)
        py = margin + int((ry - y_min) / y_span * avail_h)
        coords[node] = (px, py)
    return coords


# ===========================================================================
# Community lookup
# ===========================================================================

def _table_to_community(
    communities: list[set[str]],
    bare_name: str,
) -> int | None:
    """Return the community index a table belongs to, or None if it's
    not assigned (bridge / unassigned). The communities list uses
    full graph-node ids like 'table::PAT_ENC'; we check both that
    and the bare name for tolerance.
    """
    if not bare_name:
        return None
    candidates = (f"table::{bare_name}", bare_name)
    for i, member_set in enumerate(communities):
        for c in candidates:
            if c in member_set:
                return i
    return None


# ===========================================================================
# SVG rendering
# ===========================================================================

_NODE_RADIUS = 5         # small; the map has hundreds of nodes
_EDGE_STROKE = "#dddddd"  # very pale -- structure surfaces through clusters
_EDGE_WIDTH = "0.8"


def render_corpus_overview_svg(
    nodes: set[str],
    edges: set[tuple[str, str]],
    coords: dict[str, tuple[int, int]],
    communities: list[set[str]],
    *,
    width: int = 1200,
    height: int = 800,
    title: str = "Corpus landscape",
) -> str:
    """Render the corpus-level SVG: small colored circles for every
    table, pale grey edges for every JOIN edge, with a <title>
    tooltip naming each node and its community."""
    parts: list[str] = []
    parts.append(
        f'<svg width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" '
        f'xmlns="http://www.w3.org/2000/svg" '
        f'style="background:#ffffff; border:1px solid #d0d0d0; '
        f'border-radius:4px; display:block;">'
    )

    # Title bar (top-left, small).
    parts.append(
        f'<text x="14" y="22" font-family="sans-serif" font-size="14" '
        f'font-weight="bold" fill="#333">{html.escape(title)}</text>'
    )

    # 1. Edges first so nodes sit on top.
    for a, b in sorted(edges):
        if a not in coords or b not in coords:
            continue
        x1, y1 = coords[a]
        x2, y2 = coords[b]
        parts.append(
            f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" '
            f'stroke="{_EDGE_STROKE}" stroke-width="{_EDGE_WIDTH}" />'
        )

    # 2. Nodes.
    for n in sorted(nodes):
        if n not in coords:
            continue
        x, y = coords[n]
        community_idx = _table_to_community(communities, n)
        if community_idx is None:
            fill = BRIDGE_COLOR
            community_label = "bridge / unassigned"
        else:
            fill = community_color(community_idx)
            community_label = f"community {community_idx}"
        tooltip = html.escape(f"{n}  -  {community_label}")
        parts.append(
            f'<g>'
            f'<title>{tooltip}</title>'
            f'<circle cx="{x}" cy="{y}" r="{_NODE_RADIUS}" '
            f'fill="{fill}" stroke="#ffffff" stroke-width="0.8" />'
            f'</g>'
        )

    parts.append("</svg>")
    return "".join(parts)


# ===========================================================================
# HTML wrapper
# ===========================================================================

_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<title>{title}</title>
<style>
  body {{ font-family: sans-serif; margin: 24px; color: #333; background: #fafafa; }}
  h1 {{ font-size: 18px; margin: 0 0 6px; }}
  p.meta {{ color: #666; font-size: 13px; margin: 0 0 18px; }}
  section {{ margin-bottom: 24px; }}
  section h2 {{ font-size: 14px; margin: 0 0 8px; color: #555; }}
  .community-list {{ list-style: none; padding: 0; margin: 0;
                      display: grid;
                      grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
                      gap: 6px; }}
  .community-list li {{ display: flex; align-items: center; gap: 8px;
                         font-size: 13px; padding: 4px 8px;
                         background: #fff; border: 1px solid #e0e0e0;
                         border-radius: 4px; }}
  .swatch {{ display: inline-block; width: 14px; height: 14px;
              border-radius: 3px; flex-shrink: 0; }}
  .community-list a {{ color: #1f77b4; text-decoration: none; flex-grow: 1; }}
  .community-list a:hover {{ text-decoration: underline; }}
  .no-link {{ color: #888; font-style: italic; }}
</style>
</head>
<body>
<h1>{title}</h1>
<p class="meta">{meta}</p>
<section>
  {svg}
</section>
<section>
  <h2>Communities (click a name to drill into its shapes)</h2>
  <ul class="community-list">
{community_items}
  </ul>
</section>
</body>
</html>
"""


def write_corpus_map(
    views: list[dict],
    communities: list[set[str]],
    output_path: str | Path,
    *,
    title: str = "Corpus landscape",
    community_files: dict[int, tuple[str, str]] | None = None,
    width: int = 1200,
    height: int = 800,
) -> Path:
    """Render the corpus-level landscape HTML.

    Parameters
    ----------
    views : list of ViewV1 dicts (the full corpus).
    communities : list of sets of table-node-ids (output of
        Louvain detection in p30_analyze).
    output_path : where to write the HTML.
    community_files : optional dict community_index -> (filename,
        top_table_label) for each community's shapes HTML. The
        filename should be RELATIVE to output_path's directory --
        the rendered links will use it verbatim. Communities not in
        this dict still get listed but without a hyperlink.
    width, height : pixel canvas dimensions.

    Returns
    -------
    Path to the written file.
    """
    output_path = Path(output_path)
    nodes, edges = build_corpus_substrate(views)
    coords = force_directed_layout(nodes, edges, width=width, height=height)
    svg = render_corpus_overview_svg(
        nodes, edges, coords, communities,
        width=width, height=height, title=title,
    )

    n_with_community = sum(
        1 for n in nodes
        if _table_to_community(communities, n) is not None
    )
    n_bridge = len(nodes) - n_with_community
    meta = (
        f"{len(nodes)} table(s) &middot; {len(edges)} JOIN edge(s) &middot; "
        f"{len(communities)} community(ies) &middot; "
        f"{n_bridge} bridge/unassigned"
    )

    community_files = community_files or {}
    items: list[str] = []
    for i in range(len(communities)):
        color = community_color(i)
        if i in community_files:
            fname, label = community_files[i]
            link_html = (
                f'<a href="{html.escape(fname)}" target="_blank">'
                f'Community {i:02d} -- {html.escape(label)}</a>'
            )
        else:
            link_html = (
                f'<span class="no-link">Community {i:02d} (no shapes file)</span>'
            )
        items.append(
            f'<li><span class="swatch" style="background:{color};"></span>'
            f'{link_html}</li>'
        )

    html_body = _HTML_TEMPLATE.format(
        title=html.escape(title),
        meta=meta,
        svg=svg,
        community_items="\n".join(items),
    )
    output_path.write_text(html_body, encoding="utf-8")
    return output_path
