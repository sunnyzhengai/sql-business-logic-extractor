"""Per-view structural shape rendering (the "side-by-side compare" artifact).

Goal: help a Fabric modeler eyeball the variance/coverage across the
views inside one community. Each view is reduced to its "extended
tree" -- the set of base tables it touches plus the join edges between
them -- with CTE / derived subquery wrappers flattened away, so two
views that express the same relational shape with different SQL
patterns (one inline, one CTE-wrapped) end up with identical trees.

For each community we then compute the UNION of all primary views'
extended trees: that is the "shared substrate" -- all tables and all
joins anyone in the community touches. We lay out the substrate ONCE
with a deterministic hierarchical algorithm (left-to-right BFS from
the most-connected table), then render N panels -- one per view --
each re-using the exact same node positions. In each panel, the
view's own subset is LIT (solid stroke, full color, bold label) and
the complement is FADED (light grey, dashed, low opacity).

The output is a single HTML file per community with the N panels
arranged in a CSS grid. A steward scanning left-to-right sees at a
glance: "View A uses 4 tables, View B is the same 4 + ENCOUNTER, View
C drops ZC_STATUS and adds DEPARTMENT." The variance/coverage read is
immediate; modeling decisions ("which of these tables belong in the
certified model?") stay with the human.

Public entry points
-------------------
- view_extended_tree(view)        -> (nodes, edges)
- community_substrate(views)      -> (nodes, edges, per_view)
- hierarchical_layout(nodes, edges, root)
- render_view_shape_panel(...)    -> SVG string
- write_community_shapes(views, output_path, ...)

What is intentionally NOT here (yet)
------------------------------------
- Coloring lookup vs fact tables differently (Yang: "for now don't
  differentiate, modeler can read the tree themselves"; revisit when
  we see real graphs and decide it's actionable).
- EXISTS / IN subquery tables (filter dependencies, not join data
  flow). They reference tables but those tables don't shape the join
  graph; surfacing them would over-promise structure.
- Interactivity / pyvis. Pyvis defeats deterministic layout; SVG with
  fixed coords compares cleanly side-by-side.
"""

from __future__ import annotations

import html
from collections import defaultdict, deque
from pathlib import Path


# ---------------------------------------------------------------------------
# Helpers for the corpus dict shape
# ---------------------------------------------------------------------------

def _bare_table(qualified: str) -> str:
    """Strip schema/database prefix and bracket quoting.

    Examples:
        Clarity.dbo.PAT_ENC -> PAT_ENC
        [PAT_ENC]           -> PAT_ENC
        PAT_ENC             -> PAT_ENC

    Mirrors tools.shared.table_names.bare_table_name but local so this
    module has no upstream dependency on the graph builder's helpers.
    """
    if not qualified:
        return ""
    return qualified.split(".")[-1].strip().strip("[]").strip()


def _scope_bare_name(scope_id: str) -> str:
    """Return the bare scope name (the part after the last `:`).

    `cte:EncDept`     -> `EncDept`
    `derived:t0`      -> `t0`
    `union:0`         -> `0`
    `main`            -> `main`

    Used to match a join's `right_table` (which the corpus stores
    without the prefix) against a scope ID (which has the prefix).
    """
    return (scope_id or "").split(":")[-1].strip()


def _build_scope_lookup(view: dict) -> dict[str, dict]:
    """Map bare scope name (lowercased) -> scope dict for this view.

    `right_table` in `joins` and entries in `reads_from_scopes` both
    refer to scopes by their bare name; this lookup is how we resolve
    those references back to the full scope dict so we can recurse
    into the CTE's own joins / sources.
    """
    out: dict[str, dict] = {}
    for s in view.get("scopes") or []:
        bare = _scope_bare_name(s.get("id") or "")
        if bare:
            out[bare.lower()] = s
        # Also key by the full scope ID so callers can look up by id.
        full = s.get("id") or ""
        if full:
            out[full.lower()] = s
    return out


# ---------------------------------------------------------------------------
# Per-view extended tree
# ---------------------------------------------------------------------------

def _scope_driver(
    scope: dict,
    scope_lookup: dict[str, dict],
    visited: set[str] | None = None,
) -> str:
    """Return the bare base-table name that "drives" this scope.

    The driver is the FROM-clause table -- conventionally the first
    entry in `scope.reads_from_tables`. If the scope reads only from
    other scopes (e.g., a CTE that selects from another CTE), we
    recursively descend into the first scope reference to find the
    base table at the bottom of the chain.

    Returns the empty string if no base-table driver can be found
    (e.g., a scope with no sources, or a cycle -- shouldn't happen in
    valid SQL but we defend against it via `visited`).
    """
    if visited is None:
        visited = set()
    scope_id = scope.get("id") or ""
    if scope_id in visited:
        return ""
    visited.add(scope_id)

    # Prefer a direct base-table source.
    for t in (scope.get("reads_from_tables") or []):
        bare = _bare_table(t)
        if bare:
            return bare

    # No direct base table -- descend into the first scope reference.
    for ref in (scope.get("reads_from_scopes") or []):
        bare_ref = _scope_bare_name(ref)
        inner = scope_lookup.get(bare_ref.lower())
        if inner is not None:
            d = _scope_driver(inner, scope_lookup, visited)
            if d:
                return d

    return ""


def _is_scope_ref(name: str, scope_lookup: dict[str, dict]) -> bool:
    """True if `name` (a join's right_table, bare) matches a scope in
    this view rather than a base table."""
    return bool(name) and name.lower() in scope_lookup


def _join_left_table(
    join: dict,
    right_bare: str,
    scope_driver: str,
    scope_lookup: dict[str, dict],
) -> str:
    """Find the LEFT side of a join.

    Why this is non-trivial: a scope with N joins doesn't necessarily
    chain them all off the FROM-clause table. `SELECT ... FROM A
    JOIN B ON A.x = B.x JOIN C ON B.y = C.y` connects C to B, not C
    to A. Picking the scope's driver as the left for every join would
    produce the wrong shape (A-B and A-C instead of A-B and B-C).

    The corpus's JoinV1.columns already carries every column reference
    inside the ON clause with its resolved real table. We pick the
    left side as: the first column reference whose table is NOT the
    right side. If multiple candidates exist, prefer the scope's
    driver (the FROM table) -- that mirrors the SQL author's usual
    intent that the FROM-clause table is the join chain's anchor.

    Fallback: if JoinV1.columns is empty (old corpus pre-dating the
    columns field, or a column lookup that didn't resolve), return
    the scope driver so we still get a connected graph.
    """
    candidates: list[str] = []
    for cref in (join.get("columns") or []):
        col_table_raw = (cref.get("table") or "").strip()
        if not col_table_raw:
            continue
        # If the ref points at a CTE/derived scope, resolve to its
        # driver -- same flattening rule as elsewhere in this module.
        if col_table_raw.lower() in scope_lookup:
            col_table_raw = _scope_driver(
                scope_lookup[col_table_raw.lower()], scope_lookup,
            )
        bare = _bare_table(col_table_raw)
        if not bare:
            continue
        if bare.lower() == right_bare.lower():
            continue
        candidates.append(bare)

    if not candidates:
        return scope_driver
    # Prefer the scope driver if it appears in the candidates -- that's
    # the SQL author's likely intent for the join's anchor table.
    if scope_driver and scope_driver in candidates:
        return scope_driver
    return candidates[0]


def view_extended_tree(view: dict) -> tuple[set[str], set[tuple[str, str]]]:
    """Flatten a view to its base-table-only join graph.

    Walks every scope in the view (transitively via CTE/derived
    references), emitting one undirected edge per JOIN. CTE/derived
    wrappers are collapsed: a join from main to a CTE becomes a join
    from main's driver to the CTE's driver, and the CTE's INTERNAL
    joins are pulled into the same edge set. The result is the
    "extended tree" -- one connected graph of base tables, regardless
    of how the SQL author wrapped sub-expressions.

    Edges are returned as undirected frozensets-as-tuples (sorted
    alphabetically) so two equivalent shapes compared across views
    dedupe correctly. Self-loops (PAT_ENC self-join) are dropped --
    they don't contribute a visible variance/coverage signal.

    Parameters
    ----------
    view : ViewV1 dict (as produced by corpus.jsonl)

    Returns
    -------
    (nodes, edges)
        nodes : set of bare base-table names
        edges : set of (table_a, table_b) tuples, sorted alphabetically
                so (PAT_ENC, PATIENT) and (PATIENT, PAT_ENC) are the
                same edge
    """
    nodes: set[str] = set()
    edges: set[tuple[str, str]] = set()

    scope_lookup = _build_scope_lookup(view)

    # Walk every scope (not just main / view_outputs). The corpus
    # structure has one ScopeV1 per CTE / derived / etc., so iterating
    # over all scopes already covers the transitive closure -- no need
    # for a separate recursion. Each scope contributes:
    #   1. Its base-table sources -> nodes
    #   2. Its joins, with the right side either base or scope-ref
    #   3. Scope refs -> emit edge (this_driver -> ref_driver) so the
    #      CTE consumption is represented even when there's no JOIN
    #      keyword (e.g., `FROM EncDept ED` without an explicit JOIN)
    for scope in view.get("scopes") or []:
        # Skip filter-only scopes (EXISTS / IN) -- they're filter
        # predicates, not join sources. Including them would surface
        # tables that condition rows but don't flow data.
        if (scope.get("kind") or "") in ("exists", "in"):
            continue

        # Base-table sources contribute nodes regardless of joins.
        for t in (scope.get("reads_from_tables") or []):
            bare = _bare_table(t)
            if bare:
                nodes.add(bare)

        # The driver of THIS scope is the left side of every join
        # emitted from inside it.
        left = _scope_driver(scope, scope_lookup)
        if not left:
            continue

        # Process explicit JOIN clauses. right_table may be a base
        # table OR a bare scope name (the corpus drops the cte:/
        # derived: prefix on right_table). When it's a scope ref, the
        # natural extended-tree edge is left -> ref_scope's driver
        # (because the CTE's result, at the join boundary, IS its own
        # driver table).
        for join in (scope.get("joins") or []):
            right_raw = join.get("right_table") or ""
            right_bare = _bare_table(right_raw)
            if not right_bare:
                continue
            if _is_scope_ref(right_bare, scope_lookup):
                # Join to a CTE / derived scope. Resolve to its driver.
                inner = scope_lookup[right_bare.lower()]
                right = _scope_driver(inner, scope_lookup)
                if not right:
                    continue
            else:
                right = right_bare
            # Re-derive the LEFT side from this join's own column refs
            # rather than always using the scope's driver -- otherwise
            # join chains (FROM A JOIN B JOIN C ON B.y=C.y) get
            # mis-rooted at A.
            join_left = _join_left_table(join, right, left, scope_lookup)
            nodes.add(join_left)
            nodes.add(right)
            if join_left == right:
                # Self-join -- the on-clause carries the disambiguation
                # but as a shape edge it's degenerate.
                continue
            edges.add(tuple(sorted([join_left, right])))

        # Scope references that aren't via explicit JOIN (e.g.,
        # `FROM EncDept ED` listed in `reads_from_scopes` but the
        # parser didn't classify it as a join). The CTE's tables and
        # internal edges still belong to this view's shape; the OUTER
        # scope's driver is connected to the CTE's driver so the tree
        # stays unified rather than splitting into disconnected
        # subgraphs.
        for ref in (scope.get("reads_from_scopes") or []):
            bare_ref = _scope_bare_name(ref)
            inner = scope_lookup.get(bare_ref.lower())
            if inner is None:
                continue
            # EXISTS / IN subqueries are filter dependencies, not data
            # flow. Skip the inner scope's driver and its internal
            # joins (the outer loop already skips them when iterating
            # scopes, but they can still leak in via main's
            # reads_from_scopes if main references them).
            if (inner.get("kind") or "") in ("exists", "in"):
                continue
            ref_driver = _scope_driver(inner, scope_lookup)
            if not ref_driver or ref_driver == left:
                continue
            nodes.add(ref_driver)
            edges.add(tuple(sorted([left, ref_driver])))

    return nodes, edges


# ---------------------------------------------------------------------------
# Community substrate (union across all views)
# ---------------------------------------------------------------------------

def community_substrate(
    views: list[dict],
) -> tuple[set[str], set[tuple[str, str]], dict[str, tuple[set[str], set[tuple[str, str]]]]]:
    """Union nodes and edges across all `views`, plus return per-view
    breakdowns so the panel renderer can quickly check membership.

    Parameters
    ----------
    views : list of ViewV1 dicts (typically: all primary views of one
        community).

    Returns
    -------
    (substrate_nodes, substrate_edges, per_view)
        substrate_nodes : union of all views' nodes
        substrate_edges : union of all views' edges
        per_view : dict view_name -> (its nodes, its edges)
    """
    substrate_nodes: set[str] = set()
    substrate_edges: set[tuple[str, str]] = set()
    per_view: dict[str, tuple[set[str], set[tuple[str, str]]]] = {}

    for v in views:
        name = v.get("view_name") or ""
        if not name:
            continue
        nodes, edges = view_extended_tree(v)
        per_view[name] = (nodes, edges)
        substrate_nodes |= nodes
        substrate_edges |= edges

    return substrate_nodes, substrate_edges, per_view


# ---------------------------------------------------------------------------
# Hierarchical layout (left-to-right BFS, deterministic across runs)
# ---------------------------------------------------------------------------

def _pick_root(
    nodes: set[str],
    edges: set[tuple[str, str]],
) -> str:
    """Choose the layout root: the table with the highest degree in
    the substrate (most-connected = most-relevant anchor). Ties broken
    alphabetically so the choice is deterministic across runs.

    Falls back to the alphabetically first node when there are no
    edges -- the layout will be a single column of disconnected
    tables.
    """
    if not nodes:
        return ""
    degree: dict[str, int] = defaultdict(int)
    for a, b in edges:
        degree[a] += 1
        degree[b] += 1
    return min(nodes, key=lambda n: (-degree.get(n, 0), n))


def hierarchical_layout(
    nodes: set[str],
    edges: set[tuple[str, str]],
    root: str | None = None,
) -> dict[str, tuple[int, int]]:
    """Compute (col, row) integer coordinates for each node via BFS.

    The root sits at column 0. Each node's column = its hop distance
    from the root. Within a column, nodes are sorted alphabetically
    and assigned ascending row indices. Disconnected components are
    appended after the main BFS in alphabetic order; their internal
    structure is also BFS-laid from their own most-connected node.

    Returns
    -------
    dict node_name -> (col, row). Pixel coordinates are caller's
    responsibility (multiply by panel column/row spacing).
    """
    if root is None or root not in nodes:
        root = _pick_root(nodes, edges)
    coords: dict[str, tuple[int, int]] = {}
    if not nodes:
        return coords

    # Adjacency for BFS. Sorted neighbor lists -> deterministic order.
    adj: dict[str, set[str]] = defaultdict(set)
    for a, b in edges:
        adj[a].add(b)
        adj[b].add(a)

    # Per-column buckets so we can sort+assign rows after BFS.
    columns: dict[int, list[str]] = defaultdict(list)
    visited: set[str] = set()

    def _bfs(start: str, base_col: int = 0) -> None:
        """Visit a connected component starting at `start`; record
        each node's column in the per-column bucket."""
        q: deque[tuple[str, int]] = deque([(start, base_col)])
        visited.add(start)
        while q:
            node, col = q.popleft()
            columns[col].append(node)
            for neighbor in sorted(adj[node]):
                if neighbor not in visited:
                    visited.add(neighbor)
                    q.append((neighbor, col + 1))

    _bfs(root)

    # Handle disconnected components, if any. Each gets appended to
    # the right of the main BFS so it doesn't visually intrude.
    while True:
        remaining = nodes - visited
        if not remaining:
            break
        next_root = _pick_root(remaining, {(a, b) for (a, b) in edges
                                            if a in remaining and b in remaining})
        if not next_root:
            # No edges among remaining -- just pile them in the next
            # column in alphabetic order.
            next_col = (max(columns) if columns else 0) + 2
            for n in sorted(remaining):
                columns[next_col].append(n)
                visited.add(n)
                next_col += 0   # all in the same column, stacked
            break
        offset = (max(columns) if columns else 0) + 2
        _bfs(next_root, base_col=offset)

    # Assign row index within each column alphabetically.
    for col, members in columns.items():
        for row, name in enumerate(sorted(set(members))):
            coords[name] = (col, row)

    return coords


# ---------------------------------------------------------------------------
# SVG panel renderer
# ---------------------------------------------------------------------------

# Layout constants. Tuned so two typical panels fit comfortably
# side-by-side on a ~1100px-wide browser (compare-mode default).
_COL_SPACING = 105       # horizontal pixels between BFS columns
_ROW_SPACING = 64        # vertical pixels between rows (incl. label space)
_NODE_RADIUS = 13        # circle radius (smaller now that text is external)
_LABEL_OFFSET = 14       # pixels below circle center to baseline of label
_PAD_X = 30              # canvas padding (left + right margin)
_PAD_Y = 24              # canvas padding (top + bottom margin)
_TITLE_HEIGHT = 24       # vertical room for the panel title

# Color scheme: minimal, two states per element (lit / faded). Labels
# are rendered OUTSIDE the circles in dark text on the white panel
# background -- always readable, no white-on-blue collision.
_LIT_NODE_FILL = "#2c7fb8"
_LIT_NODE_STROKE = "#1a5d8a"
_LIT_LABEL_COLOR = "#1a1a1a"
_FADED_NODE_FILL = "#f0f0f0"
_FADED_NODE_STROKE = "#bdbdbd"
_FADED_LABEL_COLOR = "#9e9e9e"
_LIT_EDGE_COLOR = "#2c7fb8"
_FADED_EDGE_COLOR = "#dcdcdc"


def render_view_shape_panel(
    view_name: str,
    view_nodes: set[str],
    view_edges: set[tuple[str, str]],
    substrate_nodes: set[str],
    substrate_edges: set[tuple[str, str]],
    coords: dict[str, tuple[int, int]],
    *,
    title_suffix: str = "",
) -> str:
    """Render ONE view's panel as an SVG string.

    Substrate edges are drawn at their shared layout position; ones
    present in `view_edges` are lit, the rest faded. Same for nodes.
    A small title bar identifies the view. Caller stacks N of these
    in a CSS grid for the side-by-side view.

    Parameters
    ----------
    view_name : the view's display name (used in the panel title).
    view_nodes / view_edges : what THIS view actually uses.
    substrate_nodes / substrate_edges : the union -- what gets drawn
        at all (lit or faded).
    coords : shared layout map (col, row) per node.
    title_suffix : optional small text appended to the title (e.g.,
        " (4/7 tables)" for coverage hint).
    """
    if not coords:
        # Empty community -- emit a placeholder svg so the CSS layout
        # doesn't collapse.
        return (
            '<svg width="240" height="60" viewBox="0 0 240 60" '
            'xmlns="http://www.w3.org/2000/svg">'
            f'<text x="120" y="35" text-anchor="middle" fill="#888" '
            f'font-family="sans-serif" font-size="13">'
            f'{html.escape(view_name)} (empty)</text></svg>'
        )

    max_col = max(c for (c, _) in coords.values())
    max_row = max(r for (_, r) in coords.values())
    width = _PAD_X * 2 + (max_col + 1) * _COL_SPACING
    height = _PAD_Y * 2 + _TITLE_HEIGHT + (max_row + 1) * _ROW_SPACING

    # Pixel coords per node. Title bar sits in the top _TITLE_HEIGHT
    # band; the first row of nodes starts below that.
    pixel: dict[str, tuple[int, int]] = {}
    for node, (col, row) in coords.items():
        x = _PAD_X + col * _COL_SPACING + _NODE_RADIUS + 30
        y = _PAD_Y + _TITLE_HEIGHT + row * _ROW_SPACING + _NODE_RADIUS
        pixel[node] = (x, y)

    parts: list[str] = []
    # Set explicit width and height attributes so the SVG renders at
    # its natural pixel size in the browser (no auto-scaling-to-
    # container, which is what made dense panels unreadable when
    # squeezed into narrow grid cells).
    parts.append(
        f'<svg width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" '
        f'xmlns="http://www.w3.org/2000/svg" '
        f'style="background:#ffffff; border:1px solid #d0d0d0; '
        f'border-radius:4px; display:block;">'
    )

    # Title bar.
    title = html.escape(view_name + title_suffix)
    parts.append(
        f'<text x="{width // 2}" y="18" text-anchor="middle" '
        f'font-family="sans-serif" font-size="13" font-weight="bold" '
        f'fill="#333">{title}</text>'
    )

    # Edges first (so nodes draw on top). Faded edges first, then lit
    # -- lit colors should sit visually above the faded ones.
    def _edge_path(a: str, b: str) -> str | None:
        if a not in pixel or b not in pixel:
            return None
        x1, y1 = pixel[a]
        x2, y2 = pixel[b]
        return f"M{x1},{y1} L{x2},{y2}"

    for (a, b) in sorted(substrate_edges - view_edges):
        path = _edge_path(a, b)
        if path is None:
            continue
        parts.append(
            f'<path d="{path}" stroke="{_FADED_EDGE_COLOR}" '
            f'stroke-width="1.2" stroke-dasharray="4,4" fill="none" />'
        )
    for (a, b) in sorted(view_edges):
        path = _edge_path(a, b)
        if path is None:
            continue
        parts.append(
            f'<path d="{path}" stroke="{_LIT_EDGE_COLOR}" '
            f'stroke-width="2.4" fill="none" />'
        )

    # Nodes. Labels render BELOW the circle in dark text on the
    # white panel background -- always readable regardless of fill
    # color, and unbounded so long table names don't get truncated by
    # the circle radius. The <title> element gives the full table
    # name as a native browser hover tooltip.
    def _node_svg(name: str, lit: bool) -> str:
        x, y = pixel[name]
        label = html.escape(name)
        if lit:
            fill, stroke, stroke_dash = (
                _LIT_NODE_FILL, _LIT_NODE_STROKE, ""
            )
            text_color = _LIT_LABEL_COLOR
            text_weight = "bold"
        else:
            fill, stroke = _FADED_NODE_FILL, _FADED_NODE_STROKE
            stroke_dash = ' stroke-dasharray="3,3"'
            text_color = _FADED_LABEL_COLOR
            text_weight = "normal"
        return (
            f'<g>'
            f'<title>{label}</title>'
            f'<circle cx="{x}" cy="{y}" r="{_NODE_RADIUS}" '
            f'fill="{fill}" stroke="{stroke}" stroke-width="1.5"{stroke_dash} />'
            f'<text x="{x}" y="{y + _NODE_RADIUS + _LABEL_OFFSET}" '
            f'text-anchor="middle" font-family="sans-serif" '
            f'font-size="11" font-weight="{text_weight}" '
            f'fill="{text_color}">{label}</text>'
            f'</g>'
        )

    for n in sorted(substrate_nodes - view_nodes):
        if n in pixel:
            parts.append(_node_svg(n, lit=False))
    for n in sorted(view_nodes):
        if n in pixel:
            parts.append(_node_svg(n, lit=True))

    parts.append("</svg>")
    return "".join(parts)


# ---------------------------------------------------------------------------
# HTML wrapper: N panels in a CSS grid
# ---------------------------------------------------------------------------

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
  /* Compare picker: two dropdowns + a Show-all toggle. Default state
     hides everything except the first two views so the page opens
     "ready to compare" rather than "wall of panels". */
  .controls {{ background: #fff; border: 1px solid #e0e0e0; border-radius: 4px;
               padding: 10px 14px; display: flex; flex-wrap: wrap; gap: 16px;
               align-items: center; }}
  .controls label {{ font-size: 13px; }}
  .controls select {{ font-family: sans-serif; font-size: 13px; padding: 3px 6px;
                       min-width: 220px; max-width: 360px; }}
  .controls button {{ font-family: sans-serif; font-size: 13px; padding: 4px 10px;
                       cursor: pointer; }}
  /* Each per-view panel sits inside .panel-grid as flex-wrapping. JS
     toggles each panel's display based on the dropdown selection. */
  .panel-grid {{ display: flex; flex-wrap: wrap; gap: 16px; align-items: flex-start; }}
  .panel {{ scroll-margin-top: 12px; }}
  .panel.hidden {{ display: none; }}
</style>
</head>
<body>
<h1>{title}</h1>
<p class="meta">{meta}</p>
<section class="controls">
  <label>Left:&nbsp;<select id="cmp-a">{view_options_a}</select></label>
  <label>Right:&nbsp;<select id="cmp-b">{view_options_b}</select></label>
  <button id="cmp-all" type="button">Show all</button>
  <button id="cmp-pair" type="button">Show selected pair</button>
</section>
<section>
  <div class="panel-grid">
{panels}
  </div>
</section>
<script>
(function() {{
  // Pick exactly two panels to display (the L/R dropdown values);
  // everything else is hidden via the .hidden class.
  function showPair() {{
    var a = document.getElementById('cmp-a').value;
    var b = document.getElementById('cmp-b').value;
    document.querySelectorAll('[data-view]').forEach(function(el) {{
      var v = el.getAttribute('data-view');
      el.classList.toggle('hidden', v !== a && v !== b);
    }});
  }}
  // Drop the .hidden class everywhere -- the scroll-through view.
  function showAll() {{
    document.querySelectorAll('[data-view]').forEach(function(el) {{
      el.classList.remove('hidden');
    }});
  }}
  document.getElementById('cmp-a').addEventListener('change', showPair);
  document.getElementById('cmp-b').addEventListener('change', showPair);
  document.getElementById('cmp-all').addEventListener('click', showAll);
  document.getElementById('cmp-pair').addEventListener('click', showPair);
  // Initial state: show only the first two views (the default
  // dropdown values). Single-view communities just show the one.
  showPair();
}})();
</script>
</body>
</html>
"""


def write_community_shapes(
    views: list[dict],
    output_path: str | Path,
    *,
    community_label: str = "",
) -> Path:
    """Render one HTML file with N panels (one per view) plus a
    shared-substrate reference at the top.

    Parameters
    ----------
    views : ViewV1 dicts to compare. Typically all primary views of
        ONE community.
    output_path : where to write the HTML file.
    community_label : optional header label (e.g., "Community 5").

    Returns
    -------
    Path to the written file.
    """
    output_path = Path(output_path)
    nodes, edges, per_view = community_substrate(views)
    coords = hierarchical_layout(nodes, edges)

    # Per-view panels. Sort by view_name so output order is stable
    # across reruns (dict-iteration order could flip otherwise and the
    # diff looks noisy). Each panel is wrapped in a `data-view` div so
    # the compare-picker JS can toggle visibility by view name. The
    # substrate-only reference panel is intentionally NOT rendered
    # here -- every per-view panel already shows the substrate as the
    # faded grey background, so a separate "everything lit" panel
    # would be visually redundant.
    sorted_view_names = sorted(per_view)
    panels: list[str] = []
    view_options: list[str] = []
    for i, view_name in enumerate(sorted_view_names):
        v_nodes, v_edges = per_view[view_name]
        suffix = f"  ({len(v_nodes)}/{len(nodes)} tables)"
        panel_svg = render_view_shape_panel(
            view_name=view_name,
            view_nodes=v_nodes,
            view_edges=v_edges,
            substrate_nodes=nodes,
            substrate_edges=edges,
            coords=coords,
            title_suffix=suffix,
        )
        escaped = html.escape(view_name)
        panels.append(
            f'<div class="panel" data-view="{escaped}" id="{_anchor_id(view_name)}">'
            f'{panel_svg}</div>'
        )
        # Default selection: first view -> Left, second view -> Right.
        # Single-view communities just default both to the same one.
        # The JS hides everything except those two on initial load.
        view_options.append(
            f'<option value="{escaped}">{escaped}</option>'
        )

    options_html = "".join(view_options)

    # Pre-select the first two views as the default compare pair by
    # marking them with `selected` in the dropdowns. JS reads the
    # values on load and hides the other panels.
    default_a = sorted_view_names[0] if sorted_view_names else ""
    default_b = (sorted_view_names[1] if len(sorted_view_names) > 1
                  else default_a)
    options_a = options_html.replace(
        f'<option value="{html.escape(default_a)}">',
        f'<option value="{html.escape(default_a)}" selected>',
        1,
    )
    options_b = options_html.replace(
        f'<option value="{html.escape(default_b)}">',
        f'<option value="{html.escape(default_b)}" selected>',
        1,
    )

    title = (
        f"View shapes -- {community_label}" if community_label
        else "View shapes"
    )
    meta = (
        f"{len(views)} view(s)  &middot;  {len(nodes)} substrate table(s)  "
        f"&middot;  {len(edges)} substrate edge(s)  &middot;  "
        f"Use the Left/Right dropdowns to pick a pair; "
        f"<b>Show all</b> to scroll through all panels."
    )
    html_body = _HTML_TEMPLATE.format(
        title=html.escape(title),
        meta=meta,
        view_options_a=options_a,
        view_options_b=options_b,
        panels="\n".join(panels),
    )

    output_path.write_text(html_body, encoding="utf-8")
    return output_path


def _anchor_id(view_name: str) -> str:
    """Convert a view name into a CSS-safe anchor id.

    View names like `Reporting.V_CCHP_HomeHealth_Population.View` need
    the dots / colons / brackets stripped so the resulting `#anchor`
    target is a valid HTML id (and not interpreted as a CSS selector).
    """
    safe = []
    for ch in (view_name or ""):
        if ch.isalnum() or ch in "-_":
            safe.append(ch)
        else:
            safe.append("_")
    return "view-" + "".join(safe)
