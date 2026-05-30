"""Per-view structural shape rendering (v4 -- query-unfolding model).

Replaces the v3 deduped-flatten model. Goals from Yang's review of v3:

  1. Each SQL occurrence of a table is its OWN node. Self-joins
     produce two distinct nodes; using PAT_ENC inside a CTE AND in
     main produces two nodes (one per usage site).
  2. CTEs and subqueries appear as nested clusters with their own
     bounding box and label, not flattened into the base-table
     graph. The structure is the artifact.
  3. Layout reads like the SQL unfolds: start at the FROM-clause
     table on the left, JOIN'd tables fan to the right in join
     order, sub-queries / CTEs hang below as their own sub-trees
     connecting in at their consume point.

This model trades the cheap lit/faded comparison of v3 for SQL-
structural fidelity. Side-by-side compare still works -- the panels
are full trees rather than masked subsets of a shared substrate.
The v3 overlay tri-color mode is dropped because there's no longer
a shared layout for two views to project onto.

Public entry points
-------------------
- build_view_shape(view)     -> ViewShape  (corpus dict -> tree model)
- layout_shape(shape)        -> dict node_id -> (x, y)  pixel coords
- render_view_shape_panel(shape, coords) -> SVG string
- write_community_shapes(views, output_path, *, community_label)

Reverting
---------
The v3 model (deduped substrate, lit/faded panels, tri-color overlay)
is tagged `view_shape_v3` (commit 380ce57). To restore:
    git checkout view_shape_v3 -- tools/p50_present/view_shape.py
v1 and v2 are also tagged (`view_shape_v1`, `view_shape_v2`).
"""

from __future__ import annotations

import html
import json
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

# Reuse the matrix renderer's "is this a real table identifier?"
# heuristic so the shape graph and the table matrix accept/reject the
# same set of names. The extractor occasionally records SQL fragments
# (filter clauses, '1=1' tautologies) in reads_from_tables; without
# this guard those would surface as ghost nodes.
from tools.p50_present.community_matrix import _is_real_table_name


# ===========================================================================
# Helpers for reading the corpus dict shape
# ===========================================================================

def _bare_table(qualified: str) -> str:
    """Strip schema/database prefix and bracket quoting; reject
    anything that isn't a real table identifier.

    Returns "" for empty / unparseable / SQL-fragment inputs so every
    caller can filter with the existing `if bare: ...` idiom.
    """
    if not qualified:
        return ""
    bare = qualified.split(".")[-1].strip().strip("[]").strip()
    if not _is_real_table_name(qualified):
        return ""
    if not _is_real_table_name(bare):
        return ""
    return bare


def _scope_bare_name(scope_id: str) -> str:
    """Return the bare scope name (the part after the last `:`).

    `cte:EncDept`   -> `EncDept`
    `derived:t0`    -> `t0`
    `union:0`       -> `0`
    `main`          -> `main`
    """
    return (scope_id or "").split(":")[-1].strip()


def _build_scope_lookup(view: dict) -> dict[str, dict]:
    """Map both bare scope name AND full scope id (lowercased) -> the
    scope dict for this view. join.right_table refers by bare name
    while reads_from_scopes uses the full id, so we key by both.
    """
    out: dict[str, dict] = {}
    for s in view.get("scopes") or []:
        bare = _scope_bare_name(s.get("id") or "")
        if bare:
            out[bare.lower()] = s
        full = s.get("id") or ""
        if full:
            out[full.lower()] = s
    return out


def _is_scope_ref(name: str, scope_lookup: dict[str, dict]) -> bool:
    """True if `name` matches a scope (CTE / derived / etc.) in this
    view rather than a base table."""
    return bool(name) and name.lower() in scope_lookup


# ===========================================================================
# Data model: each view's shape is a forest of ShapeScopes
# ===========================================================================

@dataclass
class ShapeNode:
    """One table occurrence in the SQL.

    `id` is unique within the ViewShape. `table` is the bare base
    table name (e.g. PAT_ENC). `alias` is the SQL author's alias
    (e.g. 'A'); empty when unaliased. `scope_id` is the corpus scope
    this node belongs to. `role` is 'from' for the FROM-clause
    driver, 'join' for a JOIN'd table.
    """
    id: str
    table: str
    alias: str
    scope_id: str
    role: str  # 'from' | 'join'


@dataclass(frozen=True)
class ShapeEdge:
    """One join edge between two ShapeNodes.

    `kind` distinguishes intra-scope joins (kind='join', with a
    real `join_type`) from cross-scope consumption edges
    (kind='cross_scope', `join_type` is the outer scope's join_type
    when the cross-scope ref came via JOIN, or "" when it came via
    `reads_from_scopes` without an explicit JOIN).
    """
    source_id: str
    target_id: str
    join_type: str
    scope_id: str
    kind: str  # 'join' | 'cross_scope'


@dataclass
class ShapeScope:
    """One SQL scope unfolded as a tree.

    `nodes` are ordered by the SQL author's FROM/JOIN order so the
    layout can preserve "read left-to-right". `driver_node_id` is
    the node external consumers attach to (typically the first node,
    the FROM-clause driver).
    """
    id: str
    kind: str       # 'main' | 'cte' | 'derived' | 'join_subq' | 'union' | ...
    label: str      # display label (e.g. 'main', 'CTE: ActiveEnc')
    nodes: list[ShapeNode] = field(default_factory=list)
    edges: list[ShapeEdge] = field(default_factory=list)
    driver_node_id: str = ""


@dataclass
class ViewShape:
    """Complete unfolded shape of one view.

    `root_scope_ids` lists the scopes whose nodes are user-visible
    (typically ['main'] but for top-level UNION views it's
    ['union:0', 'union:1', ...]). `cross_scope_edges` link a
    consuming node (in some outer scope) to the driver_node of a
    referenced inner scope.
    """
    view_name: str
    scopes: list[ShapeScope] = field(default_factory=list)
    root_scope_ids: list[str] = field(default_factory=list)
    cross_scope_edges: list[ShapeEdge] = field(default_factory=list)

    def scope_by_id(self, scope_id: str) -> ShapeScope | None:
        for s in self.scopes:
            if s.id == scope_id:
                return s
        return None


# ===========================================================================
# Build: walk a corpus view dict and produce a ViewShape
# ===========================================================================

# Scope kinds that are filter dependencies (not data flow); excluded
# from the shape entirely. EXISTS/IN subqueries condition rows but
# don't contribute to the join structure that a modeler is asking
# about. Same convention as v3.
_FILTER_SCOPE_KINDS = ("exists", "in")


def build_view_shape(view: dict) -> ViewShape:
    """Convert a corpus ViewV1 dict into a ViewShape forest.

    Strategy:
      - One ShapeScope per corpus scope (filter scopes excluded).
      - Within a scope: the FIRST entry in reads_from_tables becomes
        the FROM-driver ShapeNode. Each JOIN clause adds one more
        ShapeNode and one ShapeEdge from the prior in-scope node to
        the new one. Self-joins produce two distinct nodes (one per
        SQL alias).
      - When a JOIN's right side is another scope (CTE / derived /
        join-subquery), no node is added in the outer scope -- instead
        a cross_scope edge points from the most-recent outer node to
        the inner scope's driver.
      - `reads_from_scopes` entries (CTE refs that aren't via JOIN,
        e.g. `FROM combined C` without a JOIN keyword) produce a
        cross_scope edge from the FROM driver to the inner scope's
        driver.

    The result preserves SQL author intent: each table occurrence is
    its own node, scopes are kept as nested clusters, edges are
    ordered.
    """
    shape = ViewShape(view_name=view.get("view_name") or "")
    scope_lookup = _build_scope_lookup(view)

    # Counter for unique node IDs across the whole view.
    counter = {"n": 0}

    def _new_node(table: str, alias: str, scope_id: str, role: str) -> ShapeNode:
        counter["n"] += 1
        node_id = f"n{counter['n']}"
        return ShapeNode(
            id=node_id, table=table, alias=alias,
            scope_id=scope_id, role=role,
        )

    # ----- wrapper detection: which scopes are containers of others? ------
    # resolve.py emits nested set-op branches with ids like
    # `cte:foo/union:0`. A scope is a "wrapper" if any other scope's
    # id starts with `{this_id}/` -- its flat reads_from_tables /
    # joins are merged-from-branches duplication (per fc904a6) and
    # rendering them would re-introduce the per-branch ordering bug.
    # Skip these here; the branch sub-scopes handle the rendering.
    all_scope_ids = [
        s.get("id") or "" for s in view.get("scopes") or [] if s.get("id")
    ]
    wrapper_ids: set[str] = set()
    for sid in all_scope_ids:
        for other in all_scope_ids:
            if other != sid and other.startswith(sid + "/"):
                wrapper_ids.add(sid)
                break

    # ----- per-scope tree construction ------------------------------------
    for raw_scope in view.get("scopes") or []:
        kind = (raw_scope.get("kind") or "").lower()
        if kind in _FILTER_SCOPE_KINDS:
            continue
        if (raw_scope.get("id") or "") in wrapper_ids:
            # Wrapper scope -- delegate rendering to its child branches.
            continue

        sscope = _build_scope(raw_scope, scope_lookup, _new_node, shape)
        if sscope is not None:
            shape.scopes.append(sscope)

    # ----- root scopes: view_outputs OR fall back to 'main'/first ---------
    declared_outputs = list(view.get("view_outputs") or [])
    if declared_outputs:
        shape.root_scope_ids = declared_outputs
    elif any(s.id == "main" for s in shape.scopes):
        shape.root_scope_ids = ["main"]
    elif shape.scopes:
        shape.root_scope_ids = [shape.scopes[0].id]

    return shape


def _build_scope(
    raw_scope: dict,
    scope_lookup: dict[str, dict],
    _new_node,
    shape: ViewShape,
) -> ShapeScope | None:
    """Build one ShapeScope from a corpus scope dict.

    Returns None when the scope has nothing renderable (no real
    base-table sources AND no scope refs to draw).
    """
    scope_id = raw_scope.get("id") or ""
    kind = (raw_scope.get("kind") or "").lower()
    sscope = ShapeScope(
        id=scope_id,
        kind=kind,
        label=_scope_label(scope_id, kind),
    )

    # FROM-clause driver: first non-CTE-named entry in reads_from_tables.
    # We pull the alias from the corpus's `sources` field when we can
    # so the node's alias matches what the SQL author wrote.
    from_table = ""
    from_alias = ""
    for t in (raw_scope.get("reads_from_tables") or []):
        bare = _bare_table(t)
        if bare:
            from_table = bare
            break
    if not from_table:
        # No base-table FROM driver (might be a scope that reads only
        # from another scope; e.g., `SELECT * FROM cte_foo` with no
        # base tables of its own). For unfolding purposes we don't
        # create a phantom node here -- the inner scope's nodes will
        # be the visible structure.
        from_alias = ""

    if from_table:
        from_node = _new_node(from_table, from_alias, scope_id, "from")
        sscope.nodes.append(from_node)
        sscope.driver_node_id = from_node.id

    # JOIN clauses. Each contributes either a new in-scope node and
    # join edge, or a cross-scope edge into an inner scope. We chain
    # joins off the MOST-RECENT in-scope node (mirrors SQL author's
    # "subsequent joins continue off the running result set" mental
    # model). When the previous node was a cross-scope reference, the
    # next join still chains off the outer scope's previous in-scope
    # node -- the cross-scope ref doesn't become an in-scope node.
    last_in_scope_node_id = sscope.driver_node_id

    for join in (raw_scope.get("joins") or []):
        right_raw = join.get("right_table") or ""
        right_alias = (join.get("right_alias") or "")
        join_type = (join.get("join_type") or "JOIN")

        # Check scope-ref FIRST -- the corpus drops the cte:/derived:/
        # join: prefix on join.right_table (so right_table='sub' may
        # actually refer to scope 'join:sub'). _bare_table would
        # happily accept 'sub' as a base-table identifier, which would
        # create a phantom in-scope node. The scope_lookup check has
        # to win.
        scope_ref_name = (right_raw or "").split(".")[-1].strip().strip("[]")
        if _is_scope_ref(scope_ref_name, scope_lookup):
            inner_scope_id = scope_lookup[scope_ref_name.lower()].get("id") or ""
            inner_kind = (scope_lookup[scope_ref_name.lower()].get("kind") or "").lower()
            if inner_kind in _FILTER_SCOPE_KINDS:
                continue
            if last_in_scope_node_id and inner_scope_id:
                # Cross-scope edge: outer node -> inner scope's driver
                # (resolved to a real node after all scopes are built).
                shape.cross_scope_edges.append(ShapeEdge(
                    source_id=last_in_scope_node_id,
                    target_id=f"scope:{inner_scope_id}",
                    join_type=join_type,
                    scope_id=scope_id,
                    kind="cross_scope",
                ))
            continue

        right_bare = _bare_table(right_raw)
        if right_bare:
            # Plain base table on the right -- add a new in-scope node.
            new_node = _new_node(right_bare, right_alias, scope_id, "join")
            sscope.nodes.append(new_node)
            if last_in_scope_node_id:
                sscope.edges.append(ShapeEdge(
                    source_id=last_in_scope_node_id,
                    target_id=new_node.id,
                    join_type=join_type,
                    scope_id=scope_id,
                    kind="join",
                ))
            else:
                # No anchor yet (rare -- scope has no FROM table at
                # all). The new node IS the anchor; later joins
                # chain off it.
                sscope.driver_node_id = new_node.id
            last_in_scope_node_id = new_node.id
            continue

    # `reads_from_scopes` entries that aren't covered by an explicit
    # JOIN (typically: `FROM derived_alias` style). Emit one cross-
    # scope edge per ref from the FROM driver to the referenced scope.
    for ref in (raw_scope.get("reads_from_scopes") or []):
        bare_ref = _scope_bare_name(ref)
        inner = scope_lookup.get(bare_ref.lower())
        if inner is None:
            continue
        inner_kind = (inner.get("kind") or "").lower()
        if inner_kind in _FILTER_SCOPE_KINDS:
            continue
        inner_scope_id = inner.get("id") or ""
        # Skip if this scope ref was already covered by a JOIN-based
        # cross-scope edge above. Match on (source, target_scope).
        target_marker = f"scope:{inner_scope_id}"
        already = any(
            e.scope_id == scope_id and e.target_id == target_marker
            for e in shape.cross_scope_edges
        )
        if already:
            continue
        if sscope.driver_node_id and inner_scope_id:
            shape.cross_scope_edges.append(ShapeEdge(
                source_id=sscope.driver_node_id,
                target_id=target_marker,
                join_type="",   # implicit FROM-style reference
                scope_id=scope_id,
                kind="cross_scope",
            ))

    if not sscope.nodes:
        return None
    return sscope


def _scope_label(scope_id: str, kind: str) -> str:
    """Human-readable label for a scope's cluster box.

    Nested ids like `cte:foo/union:0` get a path-style label
    (`CTE: foo · UNION branch 0`) so the modeler can read the
    nesting at a glance without consulting the id directly.
    """
    if not scope_id:
        return kind or "scope"
    parts = scope_id.split("/")
    if len(parts) > 1:
        labels = []
        for p in parts:
            seg_kind = p.split(":")[0] if ":" in p else (p or "scope")
            labels.append(_label_one_segment(p, seg_kind))
        return " · ".join(labels)
    return _label_one_segment(scope_id, kind)


def _label_one_segment(seg_id: str, kind: str) -> str:
    """Label one path segment (no slashes inside)."""
    bare = _scope_bare_name(seg_id)
    if kind == "main" or seg_id == "main":
        return "main"
    if kind == "cte":
        return f"CTE: {bare}"
    if kind == "derived":
        return f"Derived: {bare}"
    if kind in ("join", "join_subq"):
        return f"JOIN subquery: {bare}"
    if kind.startswith("union"):
        return f"UNION branch {bare}"
    return f"{kind or 'scope'}: {bare}" if bare else (kind or seg_id)


# ===========================================================================
# Resolve cross-scope edge targets (scope marker -> real driver node id)
# ===========================================================================

def _resolve_cross_scope_edges(shape: ViewShape) -> list[ShapeEdge]:
    """Map each cross_scope edge's target marker ("scope:<id>") to the
    referenced scope's driver_node_id.

    When the target is a "wrapper" scope (one that has branch
    children from a nested set-op), fan the edge out to EACH child's
    driver -- the modeler should see that the consumer reads from
    all UNION branches, not just the first.
    """
    resolved: list[ShapeEdge] = []
    for e in shape.cross_scope_edges:
        if not e.target_id.startswith("scope:"):
            resolved.append(e)
            continue

        inner_id = e.target_id[len("scope:"):]
        # Identify child branches (id starts with `{inner_id}/`).
        children = [
            s for s in shape.scopes if s.id.startswith(inner_id + "/")
        ]
        if children:
            for child in children:
                if not child.driver_node_id:
                    continue
                resolved.append(ShapeEdge(
                    source_id=e.source_id,
                    target_id=child.driver_node_id,
                    join_type=e.join_type,
                    scope_id=e.scope_id,
                    kind=e.kind,
                ))
            continue

        inner = shape.scope_by_id(inner_id)
        if inner is None or not inner.driver_node_id:
            continue
        resolved.append(ShapeEdge(
            source_id=e.source_id,
            target_id=inner.driver_node_id,
            join_type=e.join_type,
            scope_id=e.scope_id,
            kind=e.kind,
        ))
    return resolved


# ===========================================================================
# Layout: each scope is a VERTICAL tree-list (one node per row, depth
# = horizontal indent); scopes stack vertically.
# ===========================================================================

# Layout constants. Tree-list orientation puts each node on its own
# row so labels never compete for horizontal space across siblings.
# Labels render to the RIGHT of each circle -- the modeler reads
# "indent | circle | TABLE_NAME" like a file-tree view.
_INDENT_WIDTH = 30       # horizontal pixels per BFS depth level
_ROW_HEIGHT = 30         # vertical pixels per node row
_LABEL_LEFT_PAD = 8      # gap between circle right edge and label baseline x
_LABEL_BUDGET = 220      # pixels reserved on the right for the label text
_NODE_RADIUS = 11
_PAD_X = 24
_PAD_Y = 20
_TITLE_HEIGHT = 28
_SCOPE_LABEL_HEIGHT = 22  # vertical room for the scope's cluster label
_SCOPE_PAD = 12           # inner padding between cluster border and nodes
_SCOPE_GAP = 24           # vertical pixels between scope clusters


def layout_shape(shape: ViewShape) -> tuple[
    dict[str, tuple[int, int]],
    dict[str, tuple[int, int, int, int]],
    int,
    int,
]:
    """Compute pixel positions for every node + bounding box for every
    scope cluster (vertical tree-list orientation).

    Returns
    -------
    (node_coords, scope_boxes, total_width, total_height)
        node_coords  : dict node_id -> (x, y) pixel center of the circle
        scope_boxes  : dict scope_id -> (x, y, w, h) cluster box
        total_width  : pixel width of the whole layout
        total_height : pixel height of the whole layout
    """
    node_coords: dict[str, tuple[int, int]] = {}
    scope_boxes: dict[str, tuple[int, int, int, int]] = {}

    # Order scopes: root scopes first (typically 'main'), then the rest
    # by id so output is deterministic across runs. Branch sub-scopes
    # of the form `cte:foo/union:0` sort lexically so branches stay
    # grouped under their parent visually.
    root_ids = list(shape.root_scope_ids)
    rest = [s.id for s in shape.scopes if s.id not in root_ids]
    rest.sort()
    scope_order = [s for s in (root_ids + rest)
                   if shape.scope_by_id(s) is not None]

    current_y = _PAD_Y + _TITLE_HEIGHT
    max_x = _PAD_X

    for scope_id in scope_order:
        sscope = shape.scope_by_id(scope_id)
        if sscope is None or not sscope.nodes:
            continue
        col_of, row_of = _scope_internal_layout(sscope)
        max_col = max(col_of.values()) if col_of else 0
        n_rows = (max(row_of.values()) + 1) if row_of else 0

        box_x = _PAD_X
        box_y = current_y
        box_w = (_SCOPE_PAD * 2
                  + max_col * _INDENT_WIDTH
                  + _NODE_RADIUS * 2
                  + _LABEL_LEFT_PAD
                  + _LABEL_BUDGET)
        box_h = (_SCOPE_LABEL_HEIGHT
                  + _SCOPE_PAD * 2
                  + n_rows * _ROW_HEIGHT)

        for node in sscope.nodes:
            col = col_of[node.id]
            row = row_of[node.id]
            x = (box_x + _SCOPE_PAD
                 + col * _INDENT_WIDTH
                 + _NODE_RADIUS)
            y = (box_y + _SCOPE_LABEL_HEIGHT + _SCOPE_PAD
                 + row * _ROW_HEIGHT
                 + _NODE_RADIUS)
            node_coords[node.id] = (x, y)

        scope_boxes[scope_id] = (box_x, box_y, box_w, box_h)
        max_x = max(max_x, box_x + box_w)
        current_y = box_y + box_h + _SCOPE_GAP

    total_width = max_x + _PAD_X
    total_height = current_y - _SCOPE_GAP + _PAD_Y
    if not scope_boxes:
        total_width = 240
        total_height = 80

    return node_coords, scope_boxes, total_width, total_height


def _scope_internal_layout(
    sscope: ShapeScope,
) -> tuple[dict[str, int], dict[str, int]]:
    """Tree-list orientation: pre-order DFS from the scope's driver
    node, one node per row, depth = horizontal indent column.

    For SQL views the result reads top-to-bottom like a chain of
    JOINs, with parallel branches indented under their shared parent.
    Each node sits on its own row so labels (rendered to the right
    of the circle) never compete with siblings for horizontal space.
    """
    col_of: dict[str, int] = {}
    row_of: dict[str, int] = {}
    if not sscope.nodes:
        return col_of, row_of
    if not sscope.driver_node_id:
        for i, n in enumerate(sscope.nodes):
            col_of[n.id] = 0
            row_of[n.id] = i
        return col_of, row_of

    # Child map: source -> [target_id, target_id, ...] in SQL join order.
    children: dict[str, list[str]] = defaultdict(list)
    for e in sscope.edges:
        children[e.source_id].append(e.target_id)

    visited: set[str] = set()
    order: list[str] = []

    def _dfs(node_id: str, depth: int) -> None:
        if node_id in visited:
            return
        visited.add(node_id)
        col_of[node_id] = depth
        row_of[node_id] = len(order)
        order.append(node_id)
        for child in children[node_id]:
            _dfs(child, depth + 1)

    _dfs(sscope.driver_node_id, 0)

    # Disconnected nodes (orphans the resolver gave us): append at
    # depth 0 in declaration order so they're still visible.
    for n in sscope.nodes:
        if n.id not in visited:
            visited.add(n.id)
            col_of[n.id] = 0
            row_of[n.id] = len(order)
            order.append(n.id)

    return col_of, row_of


# ===========================================================================
# SVG renderer
# ===========================================================================

# Color scheme. We don't differentiate fact / dim / lookup tables
# (Yang: defer until real graphs surface the need). One color for all
# in-scope nodes; scope clusters get a faint background tint.
_NODE_FILL = "#2c7fb8"
_NODE_STROKE = "#1a5d8a"
_LABEL_COLOR = "#1a1a1a"
_EDGE_COLOR = "#5a5a5a"          # intra-scope joins
_CROSS_EDGE_COLOR = "#999"        # cross-scope (CTE consume) edges
_CLUSTER_FILL = "#f7fafc"
_CLUSTER_BORDER = "#cbd5e0"
_CLUSTER_LABEL = "#4a5568"


def render_view_shape_panel(
    shape: ViewShape,
    *,
    title_suffix: str = "",
) -> str:
    """Render ONE view's unfolded shape as an SVG string.

    Cluster boxes are drawn first (so nodes/edges sit on top); then
    cross-scope edges (dashed grey, on top of cluster boundaries);
    then intra-scope edges; then nodes with their labels below.
    """
    coords, boxes, width, height = layout_shape(shape)
    if not coords:
        return (
            f'<svg width="240" height="60" viewBox="0 0 240 60" '
            f'xmlns="http://www.w3.org/2000/svg">'
            f'<text x="120" y="35" text-anchor="middle" fill="#888" '
            f'font-family="sans-serif" font-size="13">'
            f'{html.escape(shape.view_name)} (empty)</text></svg>'
        )

    parts: list[str] = []
    parts.append(
        f'<svg width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" '
        f'xmlns="http://www.w3.org/2000/svg" '
        f'style="background:#ffffff; border:1px solid #d0d0d0; '
        f'border-radius:4px; display:block;">'
    )

    # Title bar.
    title = html.escape(shape.view_name + title_suffix)
    parts.append(
        f'<text x="{width // 2}" y="18" text-anchor="middle" '
        f'font-family="sans-serif" font-size="13" font-weight="bold" '
        f'fill="#333">{title}</text>'
    )

    # 1. Scope cluster boxes (background layer).
    for sscope in shape.scopes:
        box = boxes.get(sscope.id)
        if box is None:
            continue
        x, y, w, h = box
        parts.append(
            f'<rect x="{x}" y="{y}" width="{w}" height="{h}" '
            f'fill="{_CLUSTER_FILL}" stroke="{_CLUSTER_BORDER}" '
            f'stroke-width="1" stroke-dasharray="5,3" rx="6" ry="6" />'
        )
        # Cluster label, top-left of the box.
        parts.append(
            f'<text x="{x + 10}" y="{y + 16}" '
            f'font-family="sans-serif" font-size="11" font-weight="bold" '
            f'fill="{_CLUSTER_LABEL}">{html.escape(sscope.label)}</text>'
        )

    # 2. Cross-scope edges (dashed grey arrows between clusters).
    for e in _resolve_cross_scope_edges(shape):
        src = coords.get(e.source_id)
        tgt = coords.get(e.target_id)
        if src is None or tgt is None:
            continue
        parts.append(
            f'<path d="M{src[0]},{src[1]} L{tgt[0]},{tgt[1]}" '
            f'stroke="{_CROSS_EDGE_COLOR}" stroke-width="1.4" '
            f'stroke-dasharray="6,3" fill="none" />'
        )

    # 3. Intra-scope join edges. With tree-list orientation, edges
    # connect a parent (lower y) to its child (higher y) -- a step-
    # down + step-right when the child is indented further. Use a
    # right-angled path so the visual matches the file-tree convention.
    for sscope in shape.scopes:
        for e in sscope.edges:
            src = coords.get(e.source_id)
            tgt = coords.get(e.target_id)
            if src is None or tgt is None:
                continue
            path = _tree_list_edge_path(src, tgt)
            parts.append(
                f'<path d="{path}" stroke="{_EDGE_COLOR}" '
                f'stroke-width="1.6" fill="none" />'
            )
            jt = (e.join_type or "").upper().strip()
            if jt and jt not in ("JOIN", "INNER JOIN"):
                mx = (src[0] + tgt[0]) // 2 + 4
                my = (src[1] + tgt[1]) // 2
                parts.append(
                    f'<text x="{mx}" y="{my}" '
                    f'font-family="sans-serif" font-size="9" '
                    f'fill="#666">{html.escape(jt)}</text>'
                )

    # 4. Nodes. Labels render to the RIGHT of each circle so each row
    # gets the full width of the panel for its table name -- no more
    # horizontal collisions between sibling labels.
    for sscope in shape.scopes:
        for n in sscope.nodes:
            xy = coords.get(n.id)
            if xy is None:
                continue
            x, y = xy
            label = n.table
            if n.alias and n.alias.upper() != n.table.upper():
                label = f"{n.table} ({n.alias})"
            label_html = html.escape(label)
            tooltip = html.escape(
                f"{n.table}"
                + (f" alias={n.alias}" if n.alias else "")
                + f" -- role={n.role} -- scope={n.scope_id}"
            )
            parts.append(
                f'<g>'
                f'<title>{tooltip}</title>'
                f'<circle cx="{x}" cy="{y}" r="{_NODE_RADIUS}" '
                f'fill="{_NODE_FILL}" stroke="{_NODE_STROKE}" '
                f'stroke-width="1.5" />'
                f'<text x="{x + _NODE_RADIUS + _LABEL_LEFT_PAD}" '
                f'y="{y + 4}" '
                f'font-family="sans-serif" font-size="11" '
                f'font-weight="bold" '
                f'fill="{_LABEL_COLOR}">{label_html}</text>'
                f'</g>'
            )

    parts.append("</svg>")
    return "".join(parts)


def _tree_list_edge_path(src: tuple[int, int], tgt: tuple[int, int]) -> str:
    """Right-angled path from parent (src) to child (tgt) suitable
    for tree-list rendering: down from the parent to the child's row,
    then right (or left) to the child's column. Mimics the
    file-tree connector style."""
    sx, sy = src
    tx, ty = tgt
    if sx == tx:
        # Same indent column -- straight vertical line.
        return f"M{sx},{sy} L{tx},{ty}"
    # Step down to the child's row, then over to the child's column.
    return f"M{sx},{sy} L{sx},{ty} L{tx},{ty}"


# ===========================================================================
# HTML wrapper: compare picker (pair + show-all; overlay dropped)
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
  .controls {{ background: #fff; border: 1px solid #e0e0e0; border-radius: 4px;
               padding: 10px 14px; display: flex; flex-wrap: wrap; gap: 16px;
               align-items: center; }}
  .controls label {{ font-size: 13px; }}
  .controls select {{ font-family: sans-serif; font-size: 13px; padding: 3px 6px;
                       min-width: 220px; max-width: 360px; }}
  .controls button {{ font-family: sans-serif; font-size: 13px; padding: 4px 10px;
                       cursor: pointer; }}
  .controls button.active {{ background: #2c7fb8; color: #fff; border-color: #1a5d8a; }}
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
  <button id="cmp-pair" type="button" class="active">Show selected pair</button>
  <button id="cmp-all" type="button">Show all</button>
</section>
<section>
  <div class="panel-grid">
{panels}
  </div>
</section>
<script>
(function() {{
  function setActive(buttonId) {{
    ['cmp-pair', 'cmp-all'].forEach(function(id) {{
      document.getElementById(id).classList.toggle('active', id === buttonId);
    }});
  }}

  // Two panels visible; left dropdown choice -> order 1, right -> order 2,
  // so the on-screen layout matches the dropdown labels regardless of
  // alphabetic DOM order.
  function showPair() {{
    var a = document.getElementById('cmp-a').value;
    var b = document.getElementById('cmp-b').value;
    document.querySelectorAll('[data-view]').forEach(function(el) {{
      var v = el.getAttribute('data-view');
      if (v === a) {{
        el.classList.remove('hidden'); el.style.order = '1';
      }} else if (v === b) {{
        el.classList.remove('hidden'); el.style.order = '2';
      }} else {{
        el.classList.add('hidden'); el.style.order = '';
      }}
    }});
    setActive('cmp-pair');
  }}

  function showAll() {{
    document.querySelectorAll('[data-view]').forEach(function(el) {{
      el.classList.remove('hidden'); el.style.order = '';
    }});
    setActive('cmp-all');
  }}

  document.getElementById('cmp-a').addEventListener('change', showPair);
  document.getElementById('cmp-b').addEventListener('change', showPair);
  document.getElementById('cmp-pair').addEventListener('click', showPair);
  document.getElementById('cmp-all').addEventListener('click', showAll);

  showPair();  // Initial render: pair mode.
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
    """Render one HTML file per community with N unfolded shape panels.

    No overlay mode in v4. The deduped-substrate that overlay
    depended on doesn't exist in the unfolding model -- each view's
    panel is its own complete tree, not a masked subset of a shared
    layout. Comparison is now eyeballed across two side-by-side
    panels.
    """
    output_path = Path(output_path)

    # Build shapes once per view; sort by view_name for stable output.
    shape_by_name: dict[str, ViewShape] = {}
    for v in views:
        name = v.get("view_name") or ""
        if not name:
            continue
        shape_by_name[name] = build_view_shape(v)
    sorted_view_names = sorted(shape_by_name)

    # Per-view panels, each wrapped in `<div data-view="...">` so the
    # compare picker JS can toggle visibility by view name.
    panels: list[str] = []
    view_options: list[str] = []
    for view_name in sorted_view_names:
        shape = shape_by_name[view_name]
        n_nodes = sum(len(s.nodes) for s in shape.scopes)
        n_scopes = len(shape.scopes)
        suffix = f"  ({n_nodes} nodes, {n_scopes} scope(s))"
        panel_svg = render_view_shape_panel(shape, title_suffix=suffix)
        escaped = html.escape(view_name)
        panels.append(
            f'<div class="panel" data-view="{escaped}" '
            f'id="{_anchor_id(view_name)}">{panel_svg}</div>'
        )
        view_options.append(
            f'<option value="{escaped}">{escaped}</option>'
        )

    options_html = "".join(view_options)
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
    total_nodes = sum(
        sum(len(s.nodes) for s in shape_by_name[n].scopes)
        for n in sorted_view_names
    )
    meta = (
        f"{len(sorted_view_names)} view(s)  &middot;  "
        f"{total_nodes} total occurrence node(s)  &middot;  "
        f"Each table occurrence is its own node; CTEs and subqueries "
        f"appear as their own clusters."
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
    """Convert a view name into a CSS-safe anchor id."""
    safe = []
    for ch in (view_name or ""):
        if ch.isalnum() or ch in "-_":
            safe.append(ch)
        else:
            safe.append("_")
    return "view-" + "".join(safe)
