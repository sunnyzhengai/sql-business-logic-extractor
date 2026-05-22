"""Interactive HTML rendering of communities + the corpus graph.

Three rendering modes, all consuming the table-projection graph from
p30_analyze.projection and the communities from p30_analyze.communities:

  render_community_html(table_g, community_index, tables, bridges, out)
      One community at a time. Bridge tables shown muted gray. The
      most useful steward-meeting artifact (focused, readable).

  render_overview_html(table_g, communities, bridges, out)
      The whole corpus, colored by community. Useful for the
      "see the landscape" overview slide.

  render_communities_index_html(community_html_files, out)
      A small index.html that links to all per-community HTMLs in a
      table format with color swatches.

All HTML is self-contained (`cdn_resources="in_line"`) so it works
offline on locked-down healthcare laptops -- no external dependencies
at view time. Layouts are pre-computed with networkx
(`_compute_static_positions`) and pinned (`physics=False, fixed=True`)
so the canvas doesn't animate on load.

Historical note
---------------
These renderers were previously in `tools.operate.validate_graph_pivot`
(Section 5). In Phase 2d of the 2026-05 restructure they moved here --
HTML rendering is the VISUALIZE layer's responsibility, not validation-
specific. The companion file in this folder, `render.py`, holds the
older per-view renderers from the graph_explore era; this new module
is specifically for community-level renders.
"""

from __future__ import annotations

from pathlib import Path


# Community-color palette. Distinct, colorblind-friendlier than rainbow.
# Cycles past 12 communities (acceptable for validation visualization).
COMMUNITY_PALETTE: tuple[str, ...] = (
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b",
    "#e377c2", "#7f7f7f", "#bcbd22", "#17becf", "#aec7e8", "#ffbb78",
)

# Muted gray for bridge/dimension tables (shared context across communities).
BRIDGE_COLOR = "#bbbbbb"


def community_color(community_index: int) -> str:
    """Return a stable color for a community index. Cycles past the palette length.

    Useful as a public helper for callers that want to color something
    consistently with the renders (e.g., a slide background, a Power BI
    visual matching the HTML).
    """
    return COMMUNITY_PALETTE[community_index % len(COMMUNITY_PALETTE)]


def _safe_filename(s: str) -> str:
    """Convert an arbitrary string to a safe filename fragment.

    Lowercases and replaces anything outside [a-z0-9_] with underscore.
    Used to name per-community HTML files after their top table.
    """
    return "".join(c.lower() if c.isalnum() else "_" for c in s)[:40]


def _compute_static_positions(g, scale: float = 1000.0) -> dict[str, tuple[float, float]]:
    """Compute a deterministic static layout for a graph (no animation).

    Uses networkx's layout algorithms with a fixed seed. Returns a dict
    mapping node_id -> (x, y) in pyvis pixel coordinates (the unit
    square that spring_layout returns is scaled up by `scale`).

    Why a static layout: pyvis with physics-on animates the graph for
    several seconds while vis.js's force simulation converges, which
    users find distracting on small graphs. Pre-computing positions
    in networkx + setting physics=False gives an instant, stable,
    community-centric layout.
    """
    import networkx as nx

    n = g.number_of_nodes()
    if n == 0:
        return {}
    if n <= 200:
        # kamada_kawai gives a nice readable layout for small graphs;
        # spring_layout is also fine but can overlap nodes in small graphs.
        try:
            raw = nx.kamada_kawai_layout(g)
        except Exception:
            # kamada_kawai requires connectivity; fall back to spring on disconnected
            raw = nx.spring_layout(g, seed=42, k=0.5, iterations=80)
    else:
        # For larger graphs, spring_layout is faster and good enough.
        raw = nx.spring_layout(g, seed=42, k=0.4, iterations=50)

    # spring_layout returns coords in [-1, 1] for spring or [0, 1] for kamada_kawai.
    # Normalize to a pyvis-friendly pixel range centered around 0.
    return {node: (float(x) * scale, float(y) * scale) for node, (x, y) in raw.items()}


# Color for view nodes (Phase 3b): pale yellow -- stands out from any
# community color in the palette, and visually marks "this is a VIEW,
# not a table." Click a view node to see vis.js highlight its connections.
VIEW_NODE_COLOR = "#fff3a0"

# Marker comment that lets us detect whether we've already injected the
# subgraph-isolation script into a given pyvis HTML. Used by
# inject_subgraph_isolation_script() to be idempotent across multiple
# renders to the same path.
_ISOLATION_SCRIPT_MARKER = "<!-- subgraph-isolation-injected -->"

# The custom JS that adds the two-way subgraph-isolation behavior on
# node click. Inserted before </body> in the pyvis-generated HTML.
#
# Behavior:
#   - User clicks a node (table OR view) -> that node + its 1-hop
#     neighbors stay at full opacity; everything else fades to ~15%.
#   - User clicks empty canvas (deselects) -> all opacities restore.
#   - Works for both directions because the underlying network is the
#     same: clicking a view shows the tables it connects to; clicking
#     a table shows the views connecting to it (since view-uses edges
#     are bidirectional in vis.js).
#
# We hook into the existing `network` variable that pyvis declares
# globally in its template. This is fragile only if pyvis renames the
# variable -- which they don't tend to do across minor versions.
_ISOLATION_SCRIPT = """
<!-- subgraph-isolation-injected -->
<script>
(function () {
  // pyvis declares `network` as a global. Wait for it to be ready.
  if (typeof network === "undefined") { return; }

  var DIM_OPACITY = 0.15;

  function setAllOpacity(o) {
    var nodeUpdates = network.body.data.nodes.getIds().map(function (id) {
      return { id: id, opacity: o };
    });
    network.body.data.nodes.update(nodeUpdates);
    // Edges -- vis.js stores opacity inside `color.opacity`.
    var edgeUpdates = network.body.data.edges.getIds().map(function (id) {
      return { id: id, color: { opacity: o } };
    });
    network.body.data.edges.update(edgeUpdates);
  }

  network.on("selectNode", function (params) {
    if (!params.nodes || params.nodes.length === 0) { return; }
    var selectedNode = params.nodes[0];
    var keepNodes = new Set([selectedNode]);
    network.getConnectedNodes(selectedNode).forEach(function (id) {
      keepNodes.add(id);
    });
    var keepEdges = new Set(network.getConnectedEdges(selectedNode));

    // Dim everything, then restore the kept set.
    var nodeUpdates = network.body.data.nodes.getIds().map(function (id) {
      return { id: id, opacity: keepNodes.has(id) ? 1.0 : DIM_OPACITY };
    });
    network.body.data.nodes.update(nodeUpdates);

    var edgeUpdates = network.body.data.edges.getIds().map(function (id) {
      return { id: id, color: { opacity: keepEdges.has(id) ? 1.0 : DIM_OPACITY } };
    });
    network.body.data.edges.update(edgeUpdates);
  });

  network.on("deselectNode", function () {
    setAllOpacity(1.0);
  });

  // If the user clicks empty canvas (no selection), also reset.
  network.on("click", function (params) {
    if (params.nodes.length === 0 && params.edges.length === 0) {
      setAllOpacity(1.0);
    }
  });
})();
</script>
"""


def inject_subgraph_isolation_script(html_path: str | Path) -> None:
    """Inject the two-way subgraph-isolation JS into a pyvis-generated HTML.

    The script makes clicked nodes + their neighbors stay vivid while
    everything else fades to ~15% opacity. Idempotent: calling twice on
    the same file does nothing the second time.

    Called at the end of `render_community_html`, `render_overview_html`,
    and `render_view_html` so every rendered HTML has the behavior.
    """
    p = Path(html_path)
    text = p.read_text(encoding="utf-8")
    if _ISOLATION_SCRIPT_MARKER in text:
        # Already injected -- nothing to do.
        return
    # Insert before the closing </body>. pyvis always emits one;
    # if it doesn't (defensive), append at end.
    if "</body>" in text:
        text = text.replace("</body>", _ISOLATION_SCRIPT + "\n</body>", 1)
    else:
        text = text + _ISOLATION_SCRIPT
    p.write_text(text, encoding="utf-8")


def render_community_html(
    table_g, community_index: int, community_tables: set[str],
    bridge_tables: set[str], output_path: str | Path,
    primary_views: list[str] | None = None,
    view_to_tables_map: dict[str, set[str]] | None = None,
) -> str:
    """Render ONE community as interactive HTML, with bridges shown muted.

    Each per-community HTML contains:
      - All tables in this community (colored with the community color)
      - All bridge tables connected to any community-table (muted gray)
      - Edges between any pair of the above (the co-occurrence subgraph)
      - (Phase 3b) View nodes for each view whose PRIMARY community is
        this one, with edges from view-node to each table the view
        touches. Click a view node to highlight its subgraph.

    The layout is pre-computed with networkx and frozen (physics=off,
    fixed=true on every node). This gives stewards an instant, readable,
    non-animated view -- the previous behavior animated even small
    graphs for several seconds, which was distracting.

    Parameters
    ----------
    table_g            : table-projection graph (nx.Graph)
    community_index    : index into the communities list (drives color)
    community_tables   : set of node IDs in this community
    bridge_tables      : set of bridge-table node IDs (muted)
    output_path        : where to write the HTML
    primary_views      : (Phase 3b) list of view names whose PRIMARY
                         community is this one. None disables view nodes
                         (preserves pre-3b rendering for callers that
                         haven't opted in).
    view_to_tables_map : (Phase 3b) {view_name -> set of table node IDs}.
                         Used to draw edges from each view-node to the
                         tables it touches. None disables view nodes.

    Returns the path written.
    """
    from pyvis.network import Network
    import networkx as nx

    # Collect the table nodes to render: community tables + bridges connected to them.
    table_nodes_to_render: set[str] = set(community_tables)
    for ct in community_tables:
        if ct not in table_g:
            continue
        for neighbor in table_g.neighbors(ct):
            if neighbor in bridge_tables:
                table_nodes_to_render.add(neighbor)

    # Build the subgraph as a regular nx.Graph (undirected) to keep pyvis happy.
    sub = table_g.subgraph(table_nodes_to_render).copy()
    color_for_community = community_color(community_index)

    # Phase 3b: add view nodes + view->table edges to the same nx.Graph
    # so the layout positions them sensibly alongside the tables.
    show_views = primary_views is not None and view_to_tables_map is not None
    if show_views:
        for view_name in primary_views:
            view_node_id = f"view::{view_name}"
            # Each view is one node in the layout graph. We give it the
            # view name as its label so pyvis renders it visibly.
            sub.add_node(view_node_id, ntype="view", label=view_name)
            # Connect the view to every table it touches that's also in
            # our render set (community tables + relevant bridges).
            for table_id in view_to_tables_map.get(view_name, set()):
                if table_id in table_nodes_to_render:
                    sub.add_edge(view_node_id, table_id, relation="VIEW_USES")

    positions = _compute_static_positions(sub)

    net = Network(
        height="900px", width="100%",
        directed=False, notebook=False,
        cdn_resources="in_line",
        # selectConnectedEdges + hover lets vis.js highlight neighbors
        # when a node is clicked. The user picks a view name -> the
        # subgraph of that view's tables stays vivid while everything
        # else dims.
        select_menu=show_views,
    )

    # Render each node. View nodes get a distinct shape (hexagon) and
    # the VIEW_NODE_COLOR; tables keep their existing visual treatment.
    for node, attrs in sub.nodes(data=True):
        ntype = attrs.get("ntype")
        label = attrs.get("label", str(node))
        x, y = positions.get(node, (0.0, 0.0))

        if ntype == "view":
            title_lines = [
                f"View: {label}",
                "Click to highlight this view's tables.",
            ]
            net.add_node(
                node, label=label, color=VIEW_NODE_COLOR,
                shape="hexagon", size=20,
                title="\n".join(title_lines),
                x=x, y=y, physics=False, fixed=True,
            )
            continue

        # Table node (existing behavior).
        is_zc = attrs.get("is_zc", False)
        is_bridge = node in bridge_tables
        if is_bridge:
            color = BRIDGE_COLOR
            shape = "diamond"
            size = 18
        else:
            color = color_for_community
            shape = "box" if is_zc else "dot"
            size = 15 if is_zc else 25
        title_lines = [f"Table: {label}"]
        if is_bridge:
            title_lines.append("Role: BRIDGE (high-degree dimension/shared lookup)")
        if is_zc:
            title_lines.append("Type: ZC lookup")
        net.add_node(
            node, label=label, color=color, shape=shape, size=size,
            title="\n".join(title_lines),
            x=x, y=y, physics=False, fixed=True,
        )

    # Render edges. Co-occurrence edges (between tables) keep their
    # weight-based width. VIEW_USES edges (Phase 3b) are dashed + thin
    # so they don't visually dominate the table-to-table relationships.
    for u, v, attrs in sub.edges(data=True):
        relation = attrs.get("relation")
        if relation == "VIEW_USES":
            net.add_edge(
                u, v, width=1, color={"color": "#cccccc"}, dashes=True,
                title="view uses this table",
            )
        else:
            w = attrs.get("weight", 1)
            width = min(1 + w / 2, 8)
            net.add_edge(u, v, value=w, width=width,
                         title=f"co-occurrences: {w}")

    # Disable the simulation globally so the canvas does not "settle" on load.
    net.toggle_physics(False)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    net.write_html(str(out), notebook=False)
    inject_subgraph_isolation_script(out)
    return str(out)


def render_overview_html(
    table_g, communities: list[set], bridge_tables: set[str],
    output_path: str | Path,
) -> str:
    """Render the FULL table graph, colored by community. Useful as an overview.

    For corpora with many tables this will be dense. Per-community HTMLs
    are a better daily-driver; this is the "see the whole landscape" view.

    Layout is pre-computed with networkx so densely-connected nodes
    (community members) end up near each other in space, giving a
    community-centric visual without animation.

    Returns the path written.
    """
    from pyvis.network import Network

    # Map each node to its community color (or bridge color).
    node_to_color: dict[str, str] = {}
    for community_index, member_set in enumerate(communities):
        c = community_color(community_index)
        for node in member_set:
            node_to_color[node] = c
    for node in bridge_tables:
        node_to_color[node] = BRIDGE_COLOR

    fallback = "#cccccc"
    # Larger pixel scale for the overview since it has more nodes to spread out.
    positions = _compute_static_positions(table_g, scale=2000.0)

    net = Network(
        height="900px", width="100%",
        directed=False, notebook=False,
        cdn_resources="in_line",
    )

    for node, attrs in table_g.nodes(data=True):
        is_zc = attrs.get("is_zc", False)
        is_bridge = node in bridge_tables
        label = attrs.get("label", str(node))
        title_lines = [f"Table: {label}"]
        if is_bridge:
            title_lines.append("Role: BRIDGE")
        if is_zc:
            title_lines.append("Type: ZC lookup")
        x, y = positions.get(node, (0.0, 0.0))
        net.add_node(
            node, label=label,
            color=node_to_color.get(node, fallback),
            shape="diamond" if is_bridge else ("box" if is_zc else "dot"),
            size=18 if is_bridge else (15 if is_zc else 25),
            title="\n".join(title_lines),
            x=x, y=y, physics=False, fixed=True,
        )

    for u, v, attrs in table_g.edges(data=True):
        w = attrs.get("weight", 1)
        net.add_edge(u, v, value=w, width=min(1 + w / 2, 8),
                     title=f"co-occurrences: {w}")

    net.toggle_physics(False)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    net.write_html(str(out), notebook=False)
    inject_subgraph_isolation_script(out)
    return str(out)


def render_communities_index_html(
    community_html_files: list[tuple[int, str, str, int, int]],
    output_path: str | Path,
    view_html_files: dict[int, list[tuple[str, str]]] | None = None,
) -> str:
    """Write a small index.html listing all per-community HTMLs.

    `community_html_files` is a list of tuples:
        (community_index, top_table_label, html_filename, n_tables, n_views)

    `view_html_files` (optional, added Phase 3a) is a per-community map:
        community_index -> [(view_name, html_filename), ...]
        sorted in the order they should appear (typically: strong members
        first by descending strength, then weak members).
        When provided, each community row gets an expandable
        `<details>` block listing every member view with its
        per-view HTML link. When None, the original compact view (just
        the community-level row, no view links) is preserved.

    Style is inline so the file works offline without external CSS.
    """
    parts: list[str] = []
    parts.append("<!doctype html><html><head><meta charset='utf-8'>")
    parts.append("<title>Graph-pivot communities</title>")
    parts.append("<style>")
    parts.append("body { font-family: -apple-system, system-ui, sans-serif; "
                 "max-width: 900px; margin: 40px auto; padding: 0 20px; }")
    parts.append("h1 { color: #333; }")
    parts.append("table { border-collapse: collapse; width: 100%; margin: 20px 0; }")
    parts.append("th, td { text-align: left; padding: 8px 12px; "
                 "border-bottom: 1px solid #ddd; vertical-align: top; }")
    parts.append("th { background: #f4f4f4; }")
    parts.append("a { color: #1f77b4; text-decoration: none; }")
    parts.append("a:hover { text-decoration: underline; }")
    parts.append(".color-swatch { display: inline-block; width: 14px; height: 14px; "
                 "border-radius: 3px; vertical-align: middle; margin-right: 6px; }")
    parts.append("details summary { cursor: pointer; color: #555; }")
    parts.append(".view-list { margin: 8px 0 0 16px; padding: 0; list-style: none; "
                 "font-size: 0.95em; }")
    parts.append(".view-list li { margin: 4px 0; }")
    parts.append("</style></head><body>")
    parts.append("<h1>Graph-pivot communities</h1>")
    parts.append("<p>Click a community to see its interactive graph. Bridge tables "
                 "(dimensions / shared lookups) are shown in muted gray. Expand a "
                 "row's <em>member views</em> to drill into any single view's subgraph.</p>")
    parts.append("<table><thead><tr><th>#</th><th>Top table</th><th>Tables</th>"
                 "<th>Member views</th><th>Open</th></tr></thead><tbody>")
    for community_index, top_table, fname, n_tables, n_views in community_html_files:
        c = community_color(community_index)
        parts.append("<tr>")
        parts.append(f"<td><span class='color-swatch' style='background:{c}'></span>"
                     f"{community_index}</td>")
        parts.append(f"<td><code>{top_table}</code></td>")
        parts.append(f"<td>{n_tables}</td>")

        # Member-views cell -- either an expandable <details> with view links,
        # or just the count if no per-view HTMLs are provided.
        if view_html_files is not None:
            views_for_community = view_html_files.get(community_index, [])
            parts.append("<td>")
            parts.append(f"<details><summary>{n_views} views</summary>")
            parts.append("<ul class='view-list'>")
            for view_name, view_fname in views_for_community:
                parts.append(f"<li><a href='{view_fname}'><code>{view_name}</code></a></li>")
            parts.append("</ul>")
            parts.append("</details>")
            parts.append("</td>")
        else:
            parts.append(f"<td>{n_views}</td>")

        parts.append(f"<td><a href='{fname}'>open &rarr;</a></td>")
        parts.append("</tr>")
    parts.append("</tbody></table></body></html>")

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(parts), encoding="utf-8")
    return str(out)


# Re-export the safe-filename helper for callers (validate_graph_pivot
# uses it to name per-community HTML files).
safe_filename = _safe_filename
