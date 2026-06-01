"""Per-community overview HTML -- interactive substrate + view stripes.

The "big picture for one community" artifact. Sits between
`corpus_map.html` (whole-corpus orientation) and
`community_shapes/community_NN_*_shapes.html` (per-view unfolding)
in the navigation hierarchy.

What this answers for a BI developer:

  - What's the CORE of this community? -- tables used by most views
    surface as bigger, more saturated nodes; outliers fade to the
    periphery
  - Which views are siblings? -- the per-view stripes below show
    each view's subset against the same shared substrate
  - What does each view ADD on top of the core? -- click a stripe
    and the substrate spotlights that view's nodes/edges with
    labels; the rest dims out

Interaction model (v2):

  - The substrate is the PRIMARY canvas. Labels are hidden by
    default when there are too many tables (>30 nodes) -- the
    cloud's SHAPE still communicates community structure, and
    hover tooltips name individual nodes.
  - Per-view stripes are CLICKABLE filters, NOT external links.
    Clicking a stripe spotlights that view's subset on the
    substrate: lit nodes/edges + labels appear ONLY for the
    selected view's tables; everything else dims out. Click
    `Show all` to reset.
  - A small `Open detail` link under each stripe still routes to
    the per-view shape HTML for callers who want full unfolding
    (the substrate spotlight is for orientation; the shape HTML
    is for forensics).

Public entry points
-------------------
- build_community_substrate(views)   -> (nodes, edges, node_freq,
                                          edge_freq, per_view)
- frequency_layout(nodes, edges)     -> dict node -> (x, y)
- render_substrate_svg(...)          -> SVG string with data attrs
- render_view_stripe_svg(...)        -> SVG string (just the thumb)
- write_community_overview(views, output_path, ...)
"""

from __future__ import annotations

import html
import json
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
    edge)."""
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
            first_table = scope_tables[0] if scope_tables else None
            for join in (scope.get("joins") or []):
                right = _bare_table(join.get("right_table") or "")
                if not right or right == first_table:
                    continue
                v_nodes.add(right)
                if first_table:
                    v_edges.add(tuple(sorted([first_table, right])))

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
    Deterministic via fixed seed."""
    if not nodes:
        return {}

    import networkx as nx

    g = nx.Graph()
    for n in nodes:
        g.add_node(n)
    for a, b in edges:
        g.add_edge(a, b)
    raw = nx.spring_layout(g, seed=seed, iterations=120, k=None)
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


def adaptive_canvas_size(n_nodes: int) -> tuple[int, int]:
    """Scale the substrate canvas up for big communities. Hundreds
    of tables can't be readable in 800x460; give them more room
    so the spring layout has space to spread the cloud out."""
    if n_nodes <= 30:
        return (820, 460)
    if n_nodes <= 80:
        return (1100, 660)
    if n_nodes <= 180:
        return (1400, 880)
    return (1700, 1080)


# ===========================================================================
# SVG rendering -- substrate has stable data attrs for JS targeting
# ===========================================================================

_SUB_MIN_R = 5
_SUB_MAX_R = 14
_SUB_MIN_OPACITY = 0.32
_SUB_MAX_OPACITY = 1.0
_SUB_EDGE_MIN_W = 0.6
_SUB_EDGE_MAX_W = 2.5
_FADED_FILL = "#dcdcdc"


def _freq_ratio(freq: int, n_views: int) -> float:
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
    width: int = 820,
    height: int = 460,
    title: str = "",
    label_threshold: int = 30,
) -> str:
    """Render the BIG community substrate with stable `data-table` /
    `data-edge` attributes so JS can update colors / opacities when
    a view is spotlighted.

    Labels are EMITTED for every node but their default opacity is
    set based on `label_threshold`:
      - n_nodes <= threshold: all labels visible (small community)
      - n_nodes >  threshold: labels start hidden (the cloud would
        be unreadable otherwise); a hover-only tooltip from <title>
        names individual nodes, and clicking a per-view stripe
        un-hides the labels for that view's tables.
    """
    parts: list[str] = [
        f'<svg id="substrate-svg" width="{width}" height="{height}" '
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

    labels_default_visible = len(nodes) <= label_threshold

    # Edges (under nodes). Each line carries data-edge="A||B" so JS
    # can re-color it on view-spotlight.
    for a, b in sorted(edges):
        if a not in coords or b not in coords:
            continue
        x1, y1 = coords[a]
        x2, y2 = coords[b]
        r = _freq_ratio(edge_freq.get((a, b), 0), n_views)
        sw = _SUB_EDGE_MIN_W + (_SUB_EDGE_MAX_W - _SUB_EDGE_MIN_W) * r
        op = _SUB_MIN_OPACITY + (_SUB_MAX_OPACITY - _SUB_MIN_OPACITY) * r
        edge_key = f"{a}||{b}"
        parts.append(
            f'<line data-edge="{html.escape(edge_key)}" '
            f'x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" '
            f'stroke="{base_color}" stroke-width="{sw:.2f}" '
            f'stroke-opacity="{op:.2f}" />'
        )

    # Nodes + labels. Each <g> carries data-table="<bare_name>" so
    # JS can find every part of the node together.
    for n in sorted(nodes):
        if n not in coords:
            continue
        x, y = coords[n]
        freq = node_freq.get(n, 0)
        r = _freq_ratio(freq, n_views)
        radius = _SUB_MIN_R + (_SUB_MAX_R - _SUB_MIN_R) * r
        opacity = _SUB_MIN_OPACITY + (_SUB_MAX_OPACITY - _SUB_MIN_OPACITY) * r
        tooltip = html.escape(f"{n}  -  used by {freq} of {n_views} view(s)")
        label_opacity = opacity if labels_default_visible else 0.0
        parts.append(
            f'<g data-table="{html.escape(n)}">'
            f'<title>{tooltip}</title>'
            f'<circle cx="{x}" cy="{y}" r="{radius:.1f}" '
            f'fill="{base_color}" fill-opacity="{opacity:.2f}" '
            f'stroke="#ffffff" stroke-width="0.8" />'
            f'<text x="{x + radius + 3:.1f}" y="{y + 4}" '
            f'font-family="sans-serif" font-size="10" '
            f'fill="#333" fill-opacity="{label_opacity:.2f}">'
            f'{html.escape(n)}</text>'
            f'</g>'
        )

    parts.append("</svg>")
    return "".join(parts)


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
) -> str:
    """Render a small per-view stripe REUSING the substrate coords
    (rescaled to fit in `width` x `height`). Lit nodes/edges = this
    view's subset; faded = the rest of the substrate.

    NOTE v2: no longer wrapped in <a>. The PARENT div in the HTML
    template carries the click handler that triggers in-page
    spotlight. The fallback "Open detail" link is rendered
    SEPARATELY below the thumbnail by write_community_overview.
    """
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
    return "".join(parts)


# ===========================================================================
# HTML wrapper -- interactive
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
  .controls {{ background: #fff; border: 1px solid #e0e0e0; border-radius: 4px;
                padding: 8px 12px; margin-bottom: 12px; display: flex;
                gap: 12px; align-items: center; font-size: 13px; }}
  .controls button {{ font-family: sans-serif; font-size: 13px; padding: 4px 10px;
                       cursor: pointer; }}
  #spotlight-banner {{ color: #2c7fb8; font-weight: bold; }}
  #spotlight-banner:empty {{ display: none; }}
  .stripes {{ display: flex; flex-wrap: wrap; gap: 12px; align-items: flex-start; }}
  .stripe {{ cursor: pointer; padding: 4px; border: 1px solid transparent;
             border-radius: 4px; background: #fff; }}
  .stripe:hover {{ border-color: #b0d4eb; background: #f5fafd; }}
  .stripe.active {{ border-color: #2c7fb8; background: #eaf4fb; }}
  .stripe .open-detail {{ display: block; font-size: 10px; color: #888;
                           text-decoration: none; padding: 2px 4px;
                           text-align: right; }}
  .stripe .open-detail:hover {{ color: #2c7fb8; text-decoration: underline; }}
</style>
</head>
<body>
<h1>{title}</h1>
<p class="meta">{meta}</p>
<section>
  <h2>Community substrate
    <span class="hint">node size + opacity = number of views using
      that table; click a stripe below to spotlight one view</span>
  </h2>
  <div class="core-note">
    Tables in the <strong>core</strong> (used by most views) appear
    bigger and more saturated. Tables used by only one view sit
    smaller and faded -- often candidates for review or
    consolidation. Hover any node for its view-count.
  </div>
  <div class="controls">
    <span id="spotlight-banner"></span>
    <button id="show-all-btn" type="button">Show all (frequency view)</button>
  </div>
  {substrate_svg}
</section>
<section>
  <h2>Per-view stripes
    <span class="hint">click any stripe to spotlight that view's
      tables on the substrate above</span>
  </h2>
  <div class="stripes">
{stripes}
  </div>
</section>
<script id="overview-data" type="application/json">{view_data_json}</script>
<script>
(function() {{
  var DATA = JSON.parse(document.getElementById('overview-data').textContent);
  // DATA = {{view_name: {{nodes: ["PAT_ENC", ...], edges: ["A||B", ...]}}}}
  var BASE_COLOR = {base_color_js};
  var FADED_FILL = "#dcdcdc";
  var FADED_OPACITY = 0.12;
  var LIT_OPACITY = 1.0;
  var N_VIEWS = {n_views};
  var FREQ = JSON.parse({freq_json});
  // FREQ.nodes[table] = view_count; FREQ.edges[a||b] = view_count.

  var substrate = document.getElementById('substrate-svg');
  var banner = document.getElementById('spotlight-banner');
  var activeView = null;   // null = frequency view (default)

  function freqRatio(count) {{
    if (N_VIEWS <= 1) return 1.0;
    return Math.max(0.0, Math.min(1.0, (count - 1) / (N_VIEWS - 1)));
  }}

  function applyFrequency() {{
    // Restore the default frequency-based coloring on every node + edge.
    substrate.querySelectorAll('[data-table]').forEach(function(g) {{
      var name = g.getAttribute('data-table');
      var f = (FREQ.nodes && FREQ.nodes[name]) || 0;
      var r = freqRatio(f);
      var minOp = 0.32, maxOp = 1.0;
      var op = minOp + (maxOp - minOp) * r;
      var circle = g.querySelector('circle');
      var text = g.querySelector('text');
      if (circle) {{
        circle.setAttribute('fill', BASE_COLOR);
        circle.setAttribute('fill-opacity', op.toFixed(2));
      }}
      if (text) {{
        text.setAttribute('fill', '#333');
        // Labels visible by default only when the community is small
        // enough that they don't collide into a wall of text.
        var smallCommunity = substrate.querySelectorAll('[data-table]').length <= 30;
        text.setAttribute('fill-opacity', smallCommunity ? op.toFixed(2) : '0');
      }}
    }});
    substrate.querySelectorAll('[data-edge]').forEach(function(line) {{
      var key = line.getAttribute('data-edge');
      var f = (FREQ.edges && FREQ.edges[key]) || 0;
      var r = freqRatio(f);
      var minW = 0.6, maxW = 2.5;
      var minOp = 0.32, maxOp = 1.0;
      line.setAttribute('stroke', BASE_COLOR);
      line.setAttribute('stroke-width', (minW + (maxW - minW) * r).toFixed(2));
      line.setAttribute('stroke-opacity', (minOp + (maxOp - minOp) * r).toFixed(2));
    }});
  }}

  function spotlight(viewName) {{
    activeView = viewName;
    var d = DATA[viewName] || {{nodes: [], edges: []}};
    var nodeSet = {{}};
    d.nodes.forEach(function(n) {{ nodeSet[n] = true; }});
    var edgeSet = {{}};
    d.edges.forEach(function(e) {{ edgeSet[e] = true; }});

    substrate.querySelectorAll('[data-table]').forEach(function(g) {{
      var name = g.getAttribute('data-table');
      var inView = nodeSet[name];
      var circle = g.querySelector('circle');
      var text = g.querySelector('text');
      if (inView) {{
        if (circle) {{
          circle.setAttribute('fill', BASE_COLOR);
          circle.setAttribute('fill-opacity', LIT_OPACITY.toString());
        }}
        if (text) {{
          text.setAttribute('fill', '#1a1a1a');
          text.setAttribute('fill-opacity', LIT_OPACITY.toString());
        }}
      }} else {{
        if (circle) {{
          circle.setAttribute('fill', FADED_FILL);
          circle.setAttribute('fill-opacity', FADED_OPACITY.toString());
        }}
        if (text) {{
          text.setAttribute('fill-opacity', '0');
        }}
      }}
    }});
    substrate.querySelectorAll('[data-edge]').forEach(function(line) {{
      var key = line.getAttribute('data-edge');
      var inView = edgeSet[key];
      if (inView) {{
        line.setAttribute('stroke', BASE_COLOR);
        line.setAttribute('stroke-width', '2.4');
        line.setAttribute('stroke-opacity', '1.0');
      }} else {{
        line.setAttribute('stroke', FADED_FILL);
        line.setAttribute('stroke-width', '0.6');
        line.setAttribute('stroke-opacity', FADED_OPACITY.toString());
      }}
    }});

    // Update the banner + active-stripe styling.
    banner.textContent = 'Highlighting: ' + viewName
        + '  (' + d.nodes.length + ' of '
        + substrate.querySelectorAll('[data-table]').length + ' tables)';
    document.querySelectorAll('.stripe').forEach(function(el) {{
      el.classList.toggle('active', el.getAttribute('data-view-stripe') === viewName);
    }});
  }}

  function showAll() {{
    activeView = null;
    applyFrequency();
    banner.textContent = '';
    document.querySelectorAll('.stripe').forEach(function(el) {{
      el.classList.remove('active');
    }});
  }}

  document.getElementById('show-all-btn').addEventListener('click', showAll);

  document.querySelectorAll('.stripe').forEach(function(el) {{
    el.addEventListener('click', function(ev) {{
      // Don't trigger highlight if the user clicked the "Open detail"
      // fallback link.
      if (ev.target.closest('.open-detail')) return;
      spotlight(el.getAttribute('data-view-stripe'));
    }});
  }});

  // Initial render: frequency view (no spotlight).
  applyFrequency();
}})();
</script>
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
    label_threshold: int = 30,
    stripe_width: int = 280,
    stripe_height: int = 160,
) -> Path:
    """Write the per-community overview HTML.

    The substrate canvas size is determined adaptively from the
    node count (see `adaptive_canvas_size`) so big communities get
    a larger canvas. The fixed-size `substrate_width` / `_height`
    parameters from v1 are gone; pass `label_threshold` to control
    when labels default to hidden (large communities) vs visible
    (small communities).
    """
    output_path = Path(output_path)
    nodes, edges, node_freq, edge_freq, per_view = build_community_substrate(views)
    sub_w, sub_h = adaptive_canvas_size(len(nodes))
    coords = frequency_layout(nodes, edges, width=sub_w, height=sub_h)

    n_views = len(per_view)
    substrate_svg = render_substrate_svg(
        nodes, edges, coords, node_freq, edge_freq, n_views,
        base_color=base_color,
        width=sub_w,
        height=sub_h,
        title=community_label or "Community substrate",
        label_threshold=label_threshold,
    )

    # Per-view stripes -- stable sort for diff-friendly output.
    stripes_html: list[str] = []
    for view_name in sorted(per_view):
        v_nodes, v_edges = per_view[view_name]
        thumb_svg = render_view_stripe_svg(
            nodes, edges, coords, v_nodes, v_edges,
            base_color=base_color,
            width=stripe_width,
            height=stripe_height,
            title=f"{view_name}  ({len(v_nodes)}/{len(nodes)})",
        )
        # Optional fallback link to the per-view shape HTML. NOT the
        # primary click action -- that triggers in-page spotlight via
        # the JS handler on the parent .stripe div.
        detail_link = ""
        if shape_file_relpath_by_view:
            href = shape_file_relpath_by_view.get(view_name)
            if href:
                detail_link = (
                    f'<a class="open-detail" target="_blank" '
                    f'href="{html.escape(href)}">Open detail &raquo;</a>'
                )
        stripes_html.append(
            f'<div class="stripe" '
            f'data-view-stripe="{html.escape(view_name)}">'
            f'{thumb_svg}{detail_link}</div>'
        )

    # Embed per-view subset data as JSON for the spotlight JS.
    view_data = {
        name: {
            "nodes": sorted(per_view[name][0]),
            "edges": [f"{a}||{b}" for a, b in sorted(per_view[name][1])],
        }
        for name in per_view
    }
    view_data_json = json.dumps(view_data, separators=(",", ":"))
    # Frequency data also embedded so the JS reset path can restore
    # the default coloring without rebuilding.
    freq_data = {
        "nodes": dict(node_freq),
        "edges": {f"{a}||{b}": v for (a, b), v in edge_freq.items()},
    }
    freq_json = json.dumps(json.dumps(freq_data, separators=(",", ":")))

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
        view_data_json=view_data_json,
        freq_json=freq_json,
        base_color_js=json.dumps(base_color),
        n_views=n_views,
    )
    output_path.write_text(html_body, encoding="utf-8")
    return output_path
