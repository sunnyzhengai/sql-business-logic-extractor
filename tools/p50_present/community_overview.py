"""Per-community overview HTML -- substrate + view stripes.

The "big picture for one community" artifact. Sits between
`corpus_map.html` (whole-corpus orientation) and
`community_shapes/community_NN_*_shapes.html` (detailed per-view
unfolding) in the navigation hierarchy.

What this answers for a BI developer:

  - What's the CORE of this community? -- tables used by most views
    surface as bigger, more saturated nodes; outliers fade to the
    periphery
  - Which views are siblings? -- the per-view stripes below show
    each view's subset against the same shared substrate, so two
    nearly-identical views look nearly-identical
  - What does each view add on top of the core? -- the strip's
    lit nodes/edges minus the others is the per-view delta

How it works:

  1. UNION every primary view's tables and JOIN edges into a
     community-level substrate. Track per-element FREQUENCY (how
     many views use it).
  2. Lay the substrate out ONCE via spring_layout, seeded.
  3. Render the substrate at large size with node radius and
     opacity scaled by frequency -- the modeler reads the core
     instantly.
  4. Render N small "stripes," one per view, REUSING the same
     coords -- each stripe lights its own subset against the
     same faded substrate underneath.
  5. Stripes link to the per-view shape HTML for full unfolding
     detail.

Public entry points
-------------------
- build_community_substrate(views)   -> (nodes, edges, node_freq,
                                          edge_freq, per_view)
- frequency_layout(nodes, edges)     -> dict node -> (x, y)
- render_substrate_svg(...)          -> SVG string
- render_view_stripe_svg(...)        -> SVG string
- write_community_overview(views, output_path, ...)
"""

from __future__ import annotations

import html
from collections import defaultdict
from pathlib import Path

from tools.p50_present.community_matrix import _is_real_table_name


# ===========================================================================
# Helpers
# ===========================================================================

def _bare_table(qualified: str) -> str:
    """Strip schema/brackets and reject SQL fragments. Mirrors the
    same guard view_shape and corpus_map use."""
    if not qualified:
        return ""
    bare = qualified.split(".")[-1].strip().strip("[]").strip()
    if not _is_real_table_name(qualified):
        return ""
    if not _is_real_table_name(bare):
        return ""
    return bare


def _anchor_id(view_name: str) -> str:
    """CSS-safe anchor id from a view name (same convention as
    view_shape.write_community_shapes)."""
    safe = []
    for ch in (view_name or ""):
        safe.append(ch if (ch.isalnum() or ch in "-_") else "_")
    return "view-" + "".join(safe)


# ===========================================================================
# Build community substrate with per-element frequency
# ===========================================================================

def build_community_substrate(
    views: list[dict],
) -> tuple[
    set[str],
    set[tuple[str, str]],
    dict[str, int],
    dict[tuple[str, str], int],
    dict[str, tuple[set[str], set[tuple[str, str]]]],
]:
    """Walk the views, build the deduped community substrate, and
    compute per-element frequency (how many views use each table /
    edge).

    Returns
    -------
    (nodes, edges, node_freq, edge_freq, per_view)
        nodes      : set of bare table names across the community
        edges      : set of (a, b) tuples, canonicalized (sorted)
        node_freq  : dict table -> number of views containing it
        edge_freq  : dict edge  -> number of views containing it
        per_view   : dict view_name -> (its nodes, its edges)
    """
    nodes: set[str] = set()
    edges: set[tuple[str, str]] = set()
    node_freq: dict[str, int] = defaultdict(int)
    edge_freq: dict[tuple[str, str], int] = defaultdict(int)
    per_view: dict[str, tuple[set[str], set[tuple[str, str]]]] = {}

    for v in views:
        name = v.get("view_name") or ""
        if not name:
            continue

        v_nodes: set[str] = set()
        v_edges: set[tuple[str, str]] = set()

        for scope in v.get("scopes") or []:
            scope_tables: list[str] = []
            for t in (scope.get("reads_from_tables") or []):
                bare = _bare_table(t)
                if not bare:
                    continue
                v_nodes.add(bare)
                scope_tables.append(bare)
            # Star-shaped edge convention: connect the first table
            # in reads_from_tables to each JOIN's right_table. This
            # mirrors graph_builder.py / corpus_map.py.
            first_table = scope_tables[0] if scope_tables else None
            for join in (scope.get("joins") or []):
                right = _bare_table(join.get("right_table") or "")
                if not right or right == first_table:
                    continue
                v_nodes.add(right)
                if first_table:
                    v_edges.add(tuple(sorted([first_table, right])))

        # Bump per-element frequency once per view (not per occurrence)
        # so 'used by N/M views' counts views, not appearances.
        for n in v_nodes:
            node_freq[n] += 1
        for e in v_edges:
            edge_freq[e] += 1

        nodes |= v_nodes
        edges |= v_edges
        per_view[name] = (v_nodes, v_edges)

    return nodes, edges, dict(node_freq), dict(edge_freq), per_view


# ===========================================================================
# Layout (force-directed, seeded)
# ===========================================================================

def frequency_layout(
    nodes: set[str],
    edges: set[tuple[str, str]],
    *,
    width: int = 800,
    height: int = 400,
    seed: int = 42,
) -> dict[str, tuple[int, int]]:
    """Spring layout the community substrate, scale to pixel coords.

    Deterministic via fixed seed -- re-renders give identical
    positions until the substrate changes. Shared by the main
    substrate AND every per-view stripe so visual comparison is
    position-locked.
    """
    if not nodes:
        return {}

    import networkx as nx

    g = nx.Graph()
    for n in nodes:
        g.add_node(n)
    for a, b in edges:
        g.add_edge(a, b)
    raw = nx.spring_layout(g, seed=seed, iterations=80, k=None)
    if not raw:
        return {}

    xs = [p[0] for p in raw.values()]
    ys = [p[1] for p in raw.values()]
    x_min, x_max = min(xs), max(xs)
    y_min, y_max = min(ys), max(ys)
    x_span = max(x_max - x_min, 1e-6)
    y_span = max(y_max - y_min, 1e-6)

    margin = 30
    avail_w = width - margin * 2
    avail_h = height - margin * 2

    coords: dict[str, tuple[int, int]] = {}
    for node, (rx, ry) in raw.items():
        px = margin + int((rx - x_min) / x_span * avail_w)
        py = margin + int((ry - y_min) / y_span * avail_h)
        coords[node] = (px, py)
    return coords


# ===========================================================================
# SVG rendering
# ===========================================================================

# Visual constants for the BIG substrate panel (one per community).
_SUB_MIN_R = 5
_SUB_MAX_R = 14
_SUB_MIN_OPACITY = 0.32   # tables used by only 1 view sit faintly
_SUB_MAX_OPACITY = 1.0
_SUB_EDGE_MIN_W = 0.6
_SUB_EDGE_MAX_W = 2.5
_FADED_FILL = "#dcdcdc"   # for view-stripe non-members


def _freq_ratio(freq: int, n_views: int) -> float:
    """Map a frequency count to [0..1]. Single-view community treats
    its only-one-view tables as frequency 1.0 (no fading)."""
    if n_views <= 1:
        return 1.0
    return max(0.0, min(1.0, (freq - 1) / (n_views - 1)))


def render_substrate_svg(
    nodes: set[str],
    edges: set[tuple[str, str]],
    coords: dict[str, tuple[int, int]],
    node_freq: dict[str, int],
    edge_freq: dict[tuple[str, str], int],
    n_views: int,
    *,
    base_color: str = "#2c7fb8",
    width: int = 800,
    height: int = 400,
    title: str = "",
) -> str:
    """Render the BIG community substrate. Each table is a circle
    sized AND opacity-scaled by view-count frequency. Labels visible.

    A node used by every view is large, fully saturated, and clearly
    in the "core." A node used by one view is small, faint, and
    visually peripheral. Same scaling on edges (stroke-width +
    opacity)."""
    parts: list[str] = [
        f'<svg width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" '
        f'xmlns="http://www.w3.org/2000/svg" '
        f'style="background:#ffffff; border:1px solid #d0d0d0; '
        f'border-radius:4px; display:block;">',
    ]
    if title:
        parts.append(
            f'<text x="14" y="22" font-family="sans-serif" font-size="13" '
            f'font-weight="bold" fill="#333">{html.escape(title)}</text>'
        )

    # 1. Edges first (under nodes).
    for a, b in sorted(edges):
        if a not in coords or b not in coords:
            continue
        x1, y1 = coords[a]
        x2, y2 = coords[b]
        r = _freq_ratio(edge_freq.get((a, b), 0), n_views)
        stroke_w = _SUB_EDGE_MIN_W + (_SUB_EDGE_MAX_W - _SUB_EDGE_MIN_W) * r
        opacity = _SUB_MIN_OPACITY + (_SUB_MAX_OPACITY - _SUB_MIN_OPACITY) * r
        parts.append(
            f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" '
            f'stroke="{base_color}" stroke-width="{stroke_w:.2f}" '
            f'stroke-opacity="{opacity:.2f}" />'
        )

    # 2. Nodes + labels.
    for n in sorted(nodes):
        if n not in coords:
            continue
        x, y = coords[n]
        freq = node_freq.get(n, 0)
        r = _freq_ratio(freq, n_views)
        radius = _SUB_MIN_R + (_SUB_MAX_R - _SUB_MIN_R) * r
        opacity = _SUB_MIN_OPACITY + (_SUB_MAX_OPACITY - _SUB_MIN_OPACITY) * r
        tooltip = html.escape(f"{n}  -  used by {freq} of {n_views} view(s)")
        parts.append(
            f'<g>'
            f'<title>{tooltip}</title>'
            f'<circle cx="{x}" cy="{y}" r="{radius:.1f}" '
            f'fill="{base_color}" fill-opacity="{opacity:.2f}" '
            f'stroke="#ffffff" stroke-width="0.8" />'
            f'<text x="{x + radius + 3:.1f}" y="{y + 4}" '
            f'font-family="sans-serif" font-size="10" '
            f'fill="#333" fill-opacity="{opacity:.2f}">'
            f'{html.escape(n)}</text>'
            f'</g>'
        )

    parts.append("</svg>")
    return "".join(parts)


# Visual constants for SMALL per-view stripes (one per view).
_STRIPE_NODE_R = 4
_STRIPE_LIT_OPACITY = 1.0
_STRIPE_FADED_OPACITY = 0.18
_STRIPE_LIT_EDGE_W = 1.5
_STRIPE_FADED_EDGE_W = 0.6


def render_view_stripe_svg(
    nodes: set[str],
    edges: set[tuple[str, str]],
    coords: dict[str, tuple[int, int]],
    view_nodes: set[str],
    view_edges: set[tuple[str, str]],
    *,
    base_color: str = "#2c7fb8",
    width: int = 280,
    height: int = 160,
    title: str = "",
    href: str | None = None,
) -> str:
    """Render a small per-view stripe REUSING the substrate coords
    (rescaled to fit in `width` x `height`). Lit nodes/edges = this
    view's subset; faded = the rest of the substrate.

    `href` wraps the whole SVG in <a> so the stripe is clickable as
    a drill-down link to the per-view shape HTML.
    """
    # Compute the substrate's pixel bounding box so we can scale to
    # the stripe size.
    if not coords:
        return ""
    xs = [p[0] for p in coords.values()]
    ys = [p[1] for p in coords.values()]
    x_min, x_max = min(xs), max(xs)
    y_min, y_max = min(ys), max(ys)
    x_span = max(x_max - x_min, 1)
    y_span = max(y_max - y_min, 1)
    margin = 8
    title_pad = 14 if title else 0
    avail_w = width - margin * 2
    avail_h = height - margin * 2 - title_pad

    def _xy(x: int, y: int) -> tuple[float, float]:
        nx = margin + (x - x_min) / x_span * avail_w
        ny = margin + title_pad + (y - y_min) / y_span * avail_h
        return nx, ny

    parts: list[str] = [
        f'<svg width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" '
        f'xmlns="http://www.w3.org/2000/svg" '
        f'style="background:#ffffff; border:1px solid #e0e0e0; '
        f'border-radius:4px; display:block;">',
    ]
    if title:
        parts.append(
            f'<text x="{width // 2}" y="11" text-anchor="middle" '
            f'font-family="sans-serif" font-size="10" font-weight="bold" '
            f'fill="#333">{html.escape(title)}</text>'
        )

    # Edges (faded first, lit on top).
    for a, b in sorted(edges):
        if a not in coords or b not in coords:
            continue
        in_view = (a, b) in view_edges
        x1, y1 = _xy(*coords[a])
        x2, y2 = _xy(*coords[b])
        stroke = base_color if in_view else _FADED_FILL
        sw = _STRIPE_LIT_EDGE_W if in_view else _STRIPE_FADED_EDGE_W
        op = _STRIPE_LIT_OPACITY if in_view else _STRIPE_FADED_OPACITY
        parts.append(
            f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" '
            f'y2="{y2:.1f}" stroke="{stroke}" stroke-width="{sw}" '
            f'stroke-opacity="{op}" />'
        )

    # Nodes (faded first, lit on top so they sit visually above).
    for n in sorted(nodes):
        if n not in coords:
            continue
        in_view = n in view_nodes
        x, y = _xy(*coords[n])
        fill = base_color if in_view else _FADED_FILL
        op = _STRIPE_LIT_OPACITY if in_view else _STRIPE_FADED_OPACITY
        parts.append(
            f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{_STRIPE_NODE_R}" '
            f'fill="{fill}" fill-opacity="{op}" stroke="#ffffff" '
            f'stroke-width="0.6" />'
        )

    parts.append("</svg>")
    svg = "".join(parts)
    if href:
        # Wrap in an anchor so the whole stripe is clickable.
        return (
            f'<a href="{html.escape(href)}" target="_blank" '
            f'style="text-decoration:none;">{svg}</a>'
        )
    return svg


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
  section h2 .hint {{ font-weight: normal; color: #888; font-size: 12px;
                       margin-left: 8px; }}
  .core-note {{ background: #fff; border: 1px solid #e0e0e0; border-radius: 4px;
                 padding: 8px 12px; font-size: 12px; color: #555;
                 margin-bottom: 12px; }}
  .stripes {{ display: flex; flex-wrap: wrap; gap: 12px; align-items: flex-start; }}
  .stripes a {{ display: block; }}
</style>
</head>
<body>
<h1>{title}</h1>
<p class="meta">{meta}</p>
<section>
  <h2>Community substrate
    <span class="hint">node size + opacity = number of views using
      that table; same for edge weight</span>
  </h2>
  <div class="core-note">
    Tables in the <strong>core</strong> (used by most views) appear
    bigger and more saturated. Tables used by only one view sit
    smaller and faded -- often candidates for review or
    consolidation. Hover a node for its view-count.
  </div>
  {substrate_svg}
</section>
<section>
  <h2>Per-view stripes
    <span class="hint">click any stripe to drill into that view's
      unfolded shape</span>
  </h2>
  <div class="stripes">
{stripes}
  </div>
</section>
</body>
</html>
"""


def write_community_overview(
    views: list[dict],
    output_path: str | Path,
    *,
    community_label: str = "",
    base_color: str = "#2c7fb8",
    shape_file_relpath_by_view: dict[str, str] | None = None,
    substrate_width: int = 900,
    substrate_height: int = 460,
    stripe_width: int = 280,
    stripe_height: int = 160,
) -> Path:
    """Write the per-community overview HTML.

    Parameters
    ----------
    views : list of ViewV1 dicts (the primary views of THIS community).
    output_path : where to write the HTML.
    community_label : header label, e.g. "Community 5 -- PAT_ENC".
    base_color : community-level hue (typically from
        community_color(community_index) for consistency with other
        artifacts).
    shape_file_relpath_by_view : optional dict
        view_name -> relative URL (e.g.,
        '../community_shapes/community_05_pat_enc_shapes.html#view-VW_FOO').
        Each stripe becomes a hyperlink when its view is in this map.
    substrate_width / substrate_height : pixel canvas for the big
        substrate panel at the top.
    stripe_width / stripe_height : pixel canvas for each per-view
        stripe in the grid.
    """
    output_path = Path(output_path)
    nodes, edges, node_freq, edge_freq, per_view = build_community_substrate(views)
    coords = frequency_layout(
        nodes, edges,
        width=substrate_width,
        height=substrate_height,
    )

    n_views = len(per_view)
    substrate_svg = render_substrate_svg(
        nodes, edges, coords, node_freq, edge_freq, n_views,
        base_color=base_color,
        width=substrate_width,
        height=substrate_height,
        title=community_label or "Community substrate",
    )

    # Stripes -- stable sort by view name so re-renders diff cleanly.
    stripes_html: list[str] = []
    for view_name in sorted(per_view):
        v_nodes, v_edges = per_view[view_name]
        href = None
        if shape_file_relpath_by_view:
            href = shape_file_relpath_by_view.get(view_name)
        stripes_html.append(render_view_stripe_svg(
            nodes, edges, coords, v_nodes, v_edges,
            base_color=base_color,
            width=stripe_width,
            height=stripe_height,
            title=f"{view_name}  ({len(v_nodes)}/{len(nodes)})",
            href=href,
        ))

    # Core stats: tables used by every view vs. tables used by 1.
    core_tables = [n for n, f in node_freq.items() if f == n_views]
    outlier_tables = [n for n, f in node_freq.items() if f == 1]
    meta = (
        f"{n_views} view(s) &middot; {len(nodes)} table(s) &middot; "
        f"{len(edges)} edge(s) &middot; "
        f"<strong>core</strong> (used by all views): {len(core_tables)} table(s) "
        f"&middot; <strong>outliers</strong> (used by 1 view): "
        f"{len(outlier_tables)} table(s)"
    )

    title = (
        f"Community overview -- {community_label}" if community_label
        else "Community overview"
    )
    html_body = _HTML_TEMPLATE.format(
        title=html.escape(title),
        meta=meta,
        substrate_svg=substrate_svg,
        stripes="\n".join(stripes_html),
    )
    output_path.write_text(html_body, encoding="utf-8")
    return output_path
