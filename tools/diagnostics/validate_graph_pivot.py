"""Validation experiment for the graph-pivot architecture decision.

Background
----------
We have been debating whether to pivot the codebase away from per-view JSON
clusters and toward a unified GRAPH representation of the entire corpus. In a
graph, tables/columns become nodes, joins become edges, and similar views
naturally cluster together as densely-connected communities in the graph.

The hypothesis we are testing here is:

  When we build a single graph from all views, and apply a community-
  detection algorithm (Louvain), the resulting communities should
  correspond to RECOGNIZABLE healthcare-BI subject areas (Epic modules:
  inpatient, clinic, claims, etc.).

If that hypothesis holds, we commit to the graph pivot. If it does not,
we revisit the architecture before changing the codebase.

What this script does
---------------------
1. Reads a corpus.jsonl file (one view per JSON line; first line is a header).
2. Builds a typed networkx graph from the corpus:
     - View nodes, Scope nodes (main / cte / subquery), Table nodes, Column nodes
     - JOIN edges between tables, with view + scope provenance attached
     - Containment edges (View -> Scope, Scope -> Column, etc.)
3. Derives a "table co-occurrence" projection: a smaller, undirected graph in
   which two tables are connected if they appear in the same scope of any view.
   Edge weights = number of co-appearances. This projection is what we run
   community detection on, because:
     - It is small (tables, not all 1000s of columns).
     - It captures the cohort-shape information we care about.
     - It is the natural input to Louvain (which expects an undirected weighted graph).
4. Runs Louvain community detection on the table projection.
5. Emits three artifacts in the output directory:
     - graph.html               -- interactive pyvis render of the table projection,
                                   colored by community
     - communities.md           -- one section per community, with member views,
                                   top tables, and leaf tables (decorative lookups)
     - validation_report.md     -- summary verdict: does the community structure
                                   look healthcare-meaningful? Confidence level.
                                   Recommendation: pivot, revise, or revisit.

How to run
----------
From the repo root, on a small local sample:
    python -m tools.diagnostics.validate_graph_pivot \\
        my_notes/bi_complex_sample/corpus.jsonl /tmp/graph_pivot_validation

Or in a Fabric notebook, against the real 130-view corpus:
    from tools.diagnostics.validate_graph_pivot import run_validation
    run_validation(
        corpus_path="/lakehouse/default/Files/corpus/corpus.jsonl",
        output_dir="/lakehouse/default/Files/graph_pivot_validation",
    )

The output directory will be created if it doesn't exist.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Iterable


# ============================================================================
# SECTION 1 -- Corpus loading
# ============================================================================
#
# corpus.jsonl is a JSON-lines file (each line is one JSON object). The first
# line is a HEADER object describing the schema version and number of views.
# Every subsequent line is one ViewV1 dict.


def load_corpus(corpus_path: str | Path) -> tuple[dict, list[dict]]:
    """Read a corpus.jsonl file and split it into (header, list-of-views).

    The header is the first line and contains metadata about the corpus
    (schema version, view count). We do not strictly need it for graph
    construction, but it's useful for the validation report.

    Returns
    -------
    header  : dict  -- the first-line metadata object
    views   : list  -- one ViewV1 dict per remaining line
    """
    path = Path(corpus_path)
    header: dict = {}
    views: list[dict] = []

    # Open with UTF-8 because corpora may contain SSMS-exported text. Python's
    # default `open()` mode is text mode, which gives us strings back.
    with path.open(encoding="utf-8") as f:
        first_line = f.readline().strip()
        if first_line:
            # The header line is regular JSON; loads() parses a JSON string.
            header = json.loads(first_line)

        for line in f:
            line = line.strip()
            if not line:
                # Skip blank lines defensively; jsonl files shouldn't have them
                # but real-world files sometimes do.
                continue
            views.append(json.loads(line))

    return header, views


# ============================================================================
# SECTION 2 -- Graph construction
# ============================================================================
#
# We build a single MultiDiGraph for the whole corpus. Node IDs are namespaced
# strings (e.g., "table::PATIENT", "view::VW_FOO") to ensure uniqueness across
# multiple views being merged into the same graph. Tables are GLOBAL: one node
# represents PATIENT regardless of how many views touch it.


def _bare_table_name(qualified_name: str) -> str:
    """Strip schema/database prefixes from a fully-qualified table name.

    Examples:
        EPIC.PATIENT     -> PATIENT
        Clarity.dbo.ZC_X -> ZC_X
        PATIENT          -> PATIENT
        cte:foo          -> cte:foo   (no change; we filter these out elsewhere)
    """
    if not qualified_name:
        return ""
    # rsplit splits from the right; "Clarity.dbo.X".rsplit(".", 1) -> ["Clarity.dbo", "X"]
    # We take the last segment regardless of how many dots there were.
    return qualified_name.split(".")[-1].strip()


def _is_zc_table(bare_name: str) -> bool:
    """Heuristic: ZC_* tables are Epic code-lookup tables (decorative).

    These tables are 'leaves' in the join graph -- nothing joins from them
    onward. They contribute attributes (status labels, category names) without
    shaping the cohort. We tag them so we can visually distinguish them.
    """
    return bare_name.upper().startswith("ZC_")


def _is_cte_or_scope_reference(name: str) -> bool:
    """Detect scope references that should NOT be treated as table nodes.

    The corpus uses prefixes like 'cte:X' or 'derived:Y' for non-table scopes.
    A real table name will never contain a colon, so this is a safe filter.
    """
    return ":" in (name or "")


def build_graph(views: Iterable[dict]):
    """Build a typed networkx MultiDiGraph from a list of ViewV1 dicts.

    Schema (each node has an `ntype` attribute identifying its type):
      - View:   ntype="view",   id = f"view::{view_name}"
      - Scope:  ntype="scope",  id = f"scope::{view_name}::{scope_id}"
      - Table:  ntype="table",  id = f"table::{bare_table_name}"  (GLOBAL)
      - Column: ntype="column", id = f"col::{view_name}::{scope_id}::{column_name}"

    Edge relations (carried in the `relation` attribute):
      - HAS_SCOPE        View   -> Scope
      - READS_FROM_TABLE Scope  -> Table
      - JOIN             Table  -> Table  (with view+scope+join_type provenance)
      - CONTAINS_COLUMN  Scope  -> Column
      - BELONGS_TO       Column -> Table  (when base_columns says "table:X.Y")
      - REFERENCES_SCOPE Scope  -> Scope  (CTE references another CTE)

    Why MultiDiGraph: a single (Table, Table) pair may be joined in many views.
    We want to keep each instance so we can attribute joins back to their views.
    """
    import networkx as nx
    g = nx.MultiDiGraph()

    for view in views:
        view_name = view.get("view_name") or "UNKNOWN_VIEW"
        view_id = f"view::{view_name}"
        g.add_node(view_id, ntype="view", label=view_name, title=f"View: {view_name}")

        # Pre-compute the set of scope NAMES in this view (stripped of any
        # `cte:` or `derived:` prefix). The corpus uses prefixes consistently
        # in `reads_from_scopes`, but joins[].right_table emits the bare name.
        # We need this set to distinguish "joins to a CTE" (a scope reference)
        # from "joins to a real table" (an actual JOIN edge).
        scope_names_in_view = _collect_scope_names(view)

        for scope in view.get("scopes") or []:
            _add_scope_to_graph(g, view_name, view_id, scope, scope_names_in_view)

    return g


def _collect_scope_names(view: dict) -> set[str]:
    """Return the set of bare scope names defined in this view.

    Strips `cte:` / `derived:` / `exists:` / `union:` prefixes so the result
    matches what shows up in `joins[].right_table` (which is always the bare
    name, no prefix).
    """
    names: set[str] = set()
    for scope in view.get("scopes") or []:
        scope_id = scope.get("id") or ""
        # Strip any "prefix:" portion. After the rightmost colon is the real name.
        bare = scope_id.split(":")[-1].strip()
        if bare:
            names.add(bare)
    return names


def _add_scope_to_graph(g, view_name: str, view_id: str, scope: dict,
                         scope_names_in_view: set[str]) -> None:
    """Add one scope (and its tables/joins/columns) to the growing graph.

    Broken out from build_graph() so each function stays small and readable.
    Modifies `g` in place (networkx convention).
    """
    scope_raw_id = scope.get("id") or "?"
    scope_kind = scope.get("kind") or "main"
    scope_node_id = f"scope::{view_name}::{scope_raw_id}"

    g.add_node(
        scope_node_id,
        ntype="scope",
        label=scope_raw_id,
        kind=scope_kind,
        view=view_name,
        title=f"Scope: {scope_raw_id}  kind={scope_kind}  view={view_name}",
    )
    g.add_edge(view_id, scope_node_id, relation="HAS_SCOPE")

    # ----- Tables this scope reads from (FROM clause + all joined tables) -----
    # We collect the set of bare table names seen in this scope. The same set is
    # used downstream to build co-occurrence edges (every pair of tables seen
    # together in one scope contributes a co-occurrence).
    scope_table_set: set[str] = set()

    for table_name in scope.get("reads_from_tables") or []:
        bare = _bare_table_name(table_name)
        if not bare or _is_cte_or_scope_reference(bare):
            # Skip CTE/scope references -- they're handled by REFERENCES_SCOPE below.
            continue
        if bare in scope_names_in_view:
            # The "table" name actually matches a CTE/scope name in this view.
            # Treat as a scope reference, not a table.
            target_scope_id = f"scope::{view_name}::cte:{bare}"
            if target_scope_id not in g:
                target_scope_id = f"scope::{view_name}::{bare}"
            g.add_edge(scope_node_id, target_scope_id,
                        relation="REFERENCES_SCOPE", view=view_name)
            continue
        table_node_id = _ensure_table_node(g, bare)
        g.add_edge(scope_node_id, table_node_id,
                    relation="READS_FROM_TABLE", view=view_name, scope=scope_raw_id)
        scope_table_set.add(bare)

    # ----- JOIN edges (table -> table) -----
    # The corpus gives us right_table per join but not the left table explicitly.
    # We approximate the left side as "whatever table was already in the scope
    # before this join was applied." For a simple co-occurrence-style graph,
    # this is enough: we connect right_table to the FIRST table we saw in the
    # scope (typically the FROM-clause table), and we also add a co-occurrence
    # edge to every other table in scope further down (see Section 3).
    from_table = next(iter(scope_table_set), None)  # arbitrary "first" element

    for join in scope.get("joins") or []:
        right = _bare_table_name(join.get("right_table") or "")
        if not right or _is_cte_or_scope_reference(right):
            continue
        if right in scope_names_in_view:
            # Joining to a CTE/scope by bare name (the corpus drops the
            # `cte:` prefix on join right-sides). Record as a scope reference,
            # not as a table-to-table JOIN.
            target_scope_id = f"scope::{view_name}::cte:{right}"
            if target_scope_id not in g:
                target_scope_id = f"scope::{view_name}::{right}"
            g.add_edge(scope_node_id, target_scope_id,
                        relation="REFERENCES_SCOPE", view=view_name,
                        join_type=join.get("join_type") or "JOIN")
            continue
        right_id = _ensure_table_node(g, right)
        scope_table_set.add(right)

        if from_table and from_table != right:
            left_id = f"table::{from_table}"
            g.add_edge(
                left_id, right_id,
                relation="JOIN",
                view=view_name,
                scope=scope_raw_id,
                join_type=join.get("join_type") or "JOIN",
                on_expression=join.get("on_expression") or "",
            )

    # ----- Cross-scope references (CTE/derived references) -----
    for ref in scope.get("reads_from_scopes") or []:
        if not ref:
            continue
        target_scope_id = f"scope::{view_name}::{ref}"
        g.add_edge(scope_node_id, target_scope_id,
                    relation="REFERENCES_SCOPE", view=view_name)

    # ----- Columns in this scope (with role) -----
    for col in scope.get("columns") or []:
        col_name = col.get("column_name") or ""
        if not col_name:
            continue
        col_node_id = f"col::{view_name}::{scope_raw_id}::{col_name}"
        g.add_node(
            col_node_id,
            ntype="column",
            label=col_name,
            view=view_name,
            scope=scope_raw_id,
            column_type=col.get("column_type") or "",
            title=f"Column: {col_name}  view={view_name}  scope={scope_raw_id}",
        )
        g.add_edge(scope_node_id, col_node_id,
                    relation="CONTAINS_COLUMN", view=view_name, scope=scope_raw_id,
                    role="select")  # default role; we don't yet distinguish where/groupby

        # base_columns are strings like "table:PATIENT.PAT_ID" or "cte:foo.bar".
        # Only the "table:" form gives us a back-edge to a real table.
        for base in col.get("base_columns") or []:
            if not base.startswith("table:"):
                continue
            # Strip the "table:" prefix and split off the column name.
            body = base[len("table:"):]
            parts = body.rsplit(".", 1)
            if len(parts) != 2:
                continue
            tbl, _ref_col = parts
            bare = _bare_table_name(tbl)
            if not bare or _is_cte_or_scope_reference(bare):
                continue
            table_id = _ensure_table_node(g, bare)
            g.add_edge(col_node_id, table_id,
                        relation="BELONGS_TO", view=view_name, scope=scope_raw_id)

    # ----- Add co-occurrence edges among ALL tables in this scope ------
    # This is the secret sauce for community detection: tables that frequently
    # appear together in the same scope (i.e. are joined together in real views)
    # will end up in the same community.
    #
    # We add a "CO_OCCURS_IN_SCOPE" edge for each unordered table pair.
    table_list = sorted(scope_table_set)  # sorted for deterministic edge order
    for i in range(len(table_list)):
        for j in range(i + 1, len(table_list)):
            a = f"table::{table_list[i]}"
            b = f"table::{table_list[j]}"
            g.add_edge(a, b,
                        relation="CO_OCCURS_IN_SCOPE",
                        view=view_name,
                        scope=scope_raw_id)


def _ensure_table_node(g, bare_table: str) -> str:
    """Add a table node to the graph if not already present. Return its id.

    Idempotent: calling this with the same table multiple times only creates
    the node once. This is how we make tables GLOBAL across the corpus.
    """
    table_id = f"table::{bare_table}"
    if table_id not in g:
        is_zc = _is_zc_table(bare_table)
        g.add_node(
            table_id,
            ntype="table",
            label=bare_table,
            is_zc=is_zc,
            title=f"Table: {bare_table}" + ("  (ZC lookup)" if is_zc else ""),
        )
    return table_id


# ============================================================================
# SECTION 3 -- Table projection + community detection
# ============================================================================
#
# Louvain (and most community-detection algorithms) work on UNDIRECTED, WEIGHTED
# graphs. Our full graph is a MultiDiGraph with mixed node types. We need to
# project it down to a clean undirected weighted graph of TABLES ONLY.


def extract_table_projection(g):
    """Project the full graph to an undirected, weighted, table-only Graph.

    Edge weight = number of times two tables co-appear in a scope across the
    corpus. This is what Louvain consumes.

    Returns
    -------
    table_g : nx.Graph -- undirected, weighted
    """
    import networkx as nx
    table_g = nx.Graph()

    # First: copy table nodes (we want all tables, even those with no co-occurrences).
    for node, attrs in g.nodes(data=True):
        if attrs.get("ntype") == "table":
            table_g.add_node(node, **attrs)

    # Second: aggregate CO_OCCURS_IN_SCOPE edges into weighted undirected edges.
    # Iterate over every edge in the original MultiDiGraph and accumulate weights
    # in a Counter, then write them into the projection graph at the end.
    weights: Counter = Counter()
    for u, v, attrs in g.edges(data=True):
        if attrs.get("relation") != "CO_OCCURS_IN_SCOPE":
            continue
        # Normalize edge direction so (A, B) and (B, A) collapse into the same key.
        key = tuple(sorted([u, v]))
        weights[key] += 1

    for (u, v), w in weights.items():
        if u in table_g and v in table_g:
            table_g.add_edge(u, v, weight=w)

    return table_g


def detect_table_communities(table_g, resolution: float = 1.0) -> list[set]:
    """Run Louvain community detection on the weighted table graph.

    Parameters
    ----------
    table_g     : nx.Graph from extract_table_projection
    resolution  : 1.0 default. Higher -> more, smaller communities.
                  Lower  -> fewer, larger communities.

    Returns
    -------
    communities : list of sets of node IDs. Each set is one community.
    """
    from networkx.algorithms import community as nx_community

    # The seed is fixed so re-runs give the same partitioning. Louvain is
    # stochastic by default; a deterministic seed makes results reproducible.
    communities = nx_community.louvain_communities(
        table_g, weight="weight", resolution=resolution, seed=42,
    )
    # Sort communities by size (largest first) so the report is naturally ordered.
    communities.sort(key=len, reverse=True)
    return communities


def assign_views_to_communities(g, communities: list[set]) -> dict[int, set[str]]:
    """For each community, find which views' scopes use those tables.

    Returns a dict: community_index -> set of view names.

    A view belongs to a community if it has at least one READS_FROM_TABLE or JOIN
    edge to a table in that community. A view can belong to multiple communities
    (cross-module views, which are themselves a finding).
    """
    # Reverse map: table_node_id -> community_index, for fast lookup.
    table_to_community: dict[str, int] = {}
    for community_index, member_set in enumerate(communities):
        for table_id in member_set:
            table_to_community[table_id] = community_index

    # Walk every edge that connects a view (via its scope) to a table.
    community_views: dict[int, set[str]] = defaultdict(set)
    for u, v, attrs in g.edges(data=True):
        relation = attrs.get("relation")
        if relation not in ("READS_FROM_TABLE", "JOIN", "BELONGS_TO"):
            continue
        view_name = attrs.get("view")
        if not view_name:
            continue

        # Map the target table back to its community.
        for endpoint in (u, v):
            if endpoint in table_to_community:
                community_views[table_to_community[endpoint]].add(view_name)
                # Don't break -- the join might span two communities (also a finding).

    return community_views


# ============================================================================
# SECTION 4 -- Per-community analysis
# ============================================================================


def analyze_community(g, community_tables: set[str],
                       member_views: set[str]) -> dict:
    """Summarize one community: top tables, leaf tables, member views, etc.

    Returns a dict with these keys:
      - n_tables       : int        -- number of tables in this community
      - n_views        : int        -- number of views that touch this community
      - top_tables     : list       -- (table_name, in_degree) sorted by traversal
      - leaf_tables    : list       -- tables with out_degree 0 (decorative/lookups)
      - core_tables    : list       -- tables with out_degree >= 1 (cohort-shaping)
      - member_views   : list       -- sorted list of view names
      - zc_table_count : int        -- how many tables in the community are ZC_*
    """
    # Compute degrees inside the FULL graph (which includes JOIN edges with
    # direction information). A "leaf" in our usage = a table that other tables
    # join TO but never join FROM. These are typically lookup tables (ZC_*).

    table_in_degrees: dict[str, int] = {}
    table_out_degrees: dict[str, int] = {}
    for table_id in community_tables:
        # In-degree of a table = how many JOIN edges point INTO this table.
        # Out-degree = how many JOIN edges point OUT of this table.
        in_count = 0
        out_count = 0
        for _, _, attrs in g.in_edges(table_id, data=True):
            if attrs.get("relation") == "JOIN":
                in_count += 1
        for _, _, attrs in g.out_edges(table_id, data=True):
            if attrs.get("relation") == "JOIN":
                out_count += 1
        table_in_degrees[table_id] = in_count
        table_out_degrees[table_id] = out_count

    def label(table_id: str) -> str:
        return g.nodes[table_id].get("label", table_id)

    leaf_tables = [label(t) for t in community_tables
                   if table_out_degrees[t] == 0 and table_in_degrees[t] > 0]
    core_tables = [label(t) for t in community_tables
                   if table_out_degrees[t] >= 1]

    # Top tables by total traversal (in + out).
    top_tables_pairs = sorted(
        community_tables,
        key=lambda t: table_in_degrees[t] + table_out_degrees[t],
        reverse=True,
    )
    top_tables = [
        (label(t), table_in_degrees[t] + table_out_degrees[t])
        for t in top_tables_pairs
    ]

    zc_count = sum(1 for t in community_tables if g.nodes[t].get("is_zc"))

    return {
        "n_tables": len(community_tables),
        "n_views": len(member_views),
        "top_tables": top_tables[:15],   # cap at 15 for readability
        "leaf_tables": sorted(leaf_tables),
        "core_tables": sorted(core_tables),
        "member_views": sorted(member_views),
        "zc_table_count": zc_count,
    }


# ============================================================================
# SECTION 5 -- Rendering (interactive HTML)
# ============================================================================


def render_table_graph_html(table_g, communities: list[set],
                              output_path: str | Path) -> str:
    """Render the table-projection graph as an interactive HTML, colored by community.

    Uses pyvis (already a project dependency). The HTML is self-contained
    (CDN-free) so it can be downloaded and opened in any browser without
    further internet access -- important for healthcare laptops with
    restricted egress.

    Returns the path written.
    """
    from pyvis.network import Network

    # Build a color palette. We use a fixed list of distinct, colorblind-safer
    # colors. For more than 12 communities we cycle, which is fine for validation.
    palette = [
        "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b",
        "#e377c2", "#7f7f7f", "#bcbd22", "#17becf", "#aec7e8", "#ffbb78",
    ]

    # Map each node to its community color.
    node_to_color: dict[str, str] = {}
    for community_index, member_set in enumerate(communities):
        color = palette[community_index % len(palette)]
        for node in member_set:
            node_to_color[node] = color
    fallback_color = "#cccccc"

    net = Network(
        height="900px", width="100%",
        directed=False, notebook=False,
        cdn_resources="in_line",   # inline JS so the HTML works offline
    )

    for node, attrs in table_g.nodes(data=True):
        is_zc = attrs.get("is_zc", False)
        net.add_node(
            node,
            label=attrs.get("label", str(node)),
            title=attrs.get("title", ""),
            color=node_to_color.get(node, fallback_color),
            # ZC tables get a smaller, square shape to visually distinguish them
            # from cohort-shaping tables. Useful at a glance.
            shape="box" if is_zc else "dot",
            size=15 if is_zc else 25,
        )

    for u, v, attrs in table_g.edges(data=True):
        w = attrs.get("weight", 1)
        # Edge width scales with weight (capped so very high-weight edges don't
        # dominate the visualization).
        width = min(1 + w / 2, 8)
        net.add_edge(u, v, value=w, width=width,
                     title=f"co-occurrences: {w}")

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    net.write_html(str(out), notebook=False)
    return str(out)


# ============================================================================
# SECTION 6 -- Markdown reporting
# ============================================================================


def write_communities_markdown(
    communities: list[set],
    analyses: list[dict],
    output_path: str | Path,
) -> str:
    """Write one section per community to a markdown file.

    The shape of the file is intentionally simple: a heading per community,
    then a small set of bullet lists. Easy to scan in 5 minutes; easy to
    paste into a slide if one community stands out.
    """
    lines: list[str] = []
    lines.append("# Communities discovered by Louvain on the table-projection graph")
    lines.append("")
    lines.append(f"Total communities: {len(communities)}")
    lines.append("")
    lines.append("Each community is a set of tables that frequently co-appear in scopes")
    lines.append("of the same views. Communities should correspond to recognizable")
    lines.append("subject areas (e.g., Epic clinic encounters, claims, billing, etc.).")
    lines.append("")

    for community_index, analysis in enumerate(analyses):
        lines.append(f"## Community {community_index} -- "
                     f"{analysis['n_tables']} tables, {analysis['n_views']} views")
        lines.append("")
        lines.append(f"- ZC/lookup tables in this community: {analysis['zc_table_count']}")
        lines.append("")
        lines.append("### Top tables (by total JOIN traversal in + out)")
        for table_name, degree in analysis["top_tables"]:
            lines.append(f"- `{table_name}` -- {degree} joins")
        lines.append("")
        lines.append("### Core tables (cohort-shaping, out_degree >= 1)")
        if analysis["core_tables"]:
            for t in analysis["core_tables"]:
                lines.append(f"- `{t}`")
        else:
            lines.append("- _(none -- this community has only leaf tables)_")
        lines.append("")
        lines.append("### Leaf tables (decorative; in only, out zero)")
        if analysis["leaf_tables"]:
            for t in analysis["leaf_tables"]:
                lines.append(f"- `{t}`")
        else:
            lines.append("- _(none)_")
        lines.append("")
        lines.append(f"### Member views ({len(analysis['member_views'])})")
        for v in analysis["member_views"]:
            lines.append(f"- `{v}`")
        lines.append("")

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines), encoding="utf-8")
    return str(out)


def write_validation_report(
    header: dict,
    g,
    table_g,
    communities: list[set],
    analyses: list[dict],
    output_path: str | Path,
) -> str:
    """Write the verdict / recommendation document.

    This is the artifact that decides whether we pivot the codebase. It is
    intentionally short and human-readable; the underlying data lives in
    communities.md and graph.html.
    """
    n_views = header.get("n_views", "unknown")
    n_tables = sum(1 for _, a in g.nodes(data=True) if a.get("ntype") == "table")
    n_communities = len(communities)
    avg_community_size = (sum(len(c) for c in communities) / n_communities
                            if n_communities else 0)

    # The validation criterion: a "healthy" pivot looks like:
    #   - Multiple communities (more than 1, less than n_views)
    #   - Each community has 3-30 tables (recognizable subject area)
    #   - The largest community is not >70% of all tables (no degenerate clustering)
    largest_size = max((len(c) for c in communities), default=0)
    largest_pct = (100.0 * largest_size / n_tables) if n_tables else 0
    healthy_count_range = 2 <= n_communities <= max(2, n_views)
    healthy_size_range = 3 <= avg_community_size <= 30
    not_degenerate = largest_pct < 70

    if healthy_count_range and healthy_size_range and not_degenerate:
        verdict = "PASS"
        recommendation = ("The graph pivot is justified. Communities correspond to "
                          "table neighborhoods of plausible size. Proceed with the "
                          "codebase restructure and full pipeline build-out.")
    elif n_communities <= 1:
        verdict = "INCONCLUSIVE -- too few communities"
        recommendation = ("Louvain found only one community. Either the corpus is "
                          "too small to surface modular structure, or our table "
                          "projection is collapsing real structure. Try running "
                          "with a larger corpus or higher resolution before deciding.")
    elif largest_pct >= 70:
        verdict = "INCONCLUSIVE -- one giant community"
        recommendation = (f"The largest community contains {largest_pct:.0f}% of all "
                          "tables. This usually means PATIENT (or a similar superhub) "
                          "is dragging everything together. Consider downweighting "
                          "edges to high-degree hubs before re-running.")
    else:
        verdict = "REVIEW NEEDED"
        recommendation = ("The structure is non-trivial but does not fit the healthy "
                          "shape we expected. Inspect communities.md and graph.html "
                          "manually; decide whether the structure is healthcare-meaningful "
                          "or noise.")

    lines = []
    lines.append("# Graph-pivot validation report")
    lines.append("")
    lines.append("## Summary statistics")
    lines.append("")
    lines.append(f"- Views ingested: **{n_views}**")
    lines.append(f"- Distinct tables: **{n_tables}**")
    lines.append(f"- Communities found: **{n_communities}**")
    lines.append(f"- Average community size: **{avg_community_size:.1f}** tables")
    lines.append(f"- Largest community: **{largest_size}** tables "
                  f"(**{largest_pct:.0f}%** of all tables)")
    lines.append("")
    lines.append("## Verdict")
    lines.append("")
    lines.append(f"**{verdict}**")
    lines.append("")
    lines.append(recommendation)
    lines.append("")
    lines.append("## What to inspect")
    lines.append("")
    lines.append("- `graph.html` -- visualize the table graph, colored by community. ")
    lines.append("  Do the colored groupings correspond to recognizable subject areas?")
    lines.append("  (Epic clinic, inpatient, claims, billing, registry, etc.)")
    lines.append("- `communities.md` -- per-community detail: which tables, which views.")
    lines.append("  Look at the top 3-5 tables of each community and ask: does this name")
    lines.append("  a coherent business domain in your shop?")
    lines.append("")
    lines.append("## How to interpret the verdict")
    lines.append("")
    lines.append("- **PASS** -> proceed with the codebase restructure. Confidence is high.")
    lines.append("- **INCONCLUSIVE** -> investigate the named issue, possibly re-run, then ")
    lines.append("  re-evaluate. Do NOT commit to the restructure yet.")
    lines.append("- **REVIEW NEEDED** -> the algorithms ran cleanly but the result looks")
    lines.append("  unusual. Open the artifacts manually and decide.")

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines), encoding="utf-8")
    return str(out)


# ============================================================================
# SECTION 7 -- Orchestrator
# ============================================================================


def run_validation(corpus_path: str | Path, output_dir: str | Path,
                    resolution: float = 1.0) -> dict:
    """Run the full validation pipeline. Returns a dict of output paths.

    This is the entry point you call from a Fabric notebook:

        from tools.diagnostics.validate_graph_pivot import run_validation
        run_validation("/lakehouse/.../corpus.jsonl", "/lakehouse/.../validation_out")
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"[1/6] Loading corpus from {corpus_path}...")
    header, views = load_corpus(corpus_path)
    print(f"      Loaded {len(views)} views (header says {header.get('n_views', '?')})")

    print("[2/6] Building typed graph...")
    g = build_graph(views)
    n_table = sum(1 for _, a in g.nodes(data=True) if a.get("ntype") == "table")
    print(f"      Graph: {g.number_of_nodes()} nodes, {g.number_of_edges()} edges, "
          f"{n_table} distinct tables")

    print("[3/6] Extracting table-projection subgraph...")
    table_g = extract_table_projection(g)
    print(f"      Projection: {table_g.number_of_nodes()} tables, "
          f"{table_g.number_of_edges()} weighted edges")

    print("[4/6] Running Louvain community detection...")
    communities = detect_table_communities(table_g, resolution=resolution)
    print(f"      Found {len(communities)} communities, "
          f"sizes: {sorted([len(c) for c in communities], reverse=True)}")

    print("[5/6] Analyzing communities...")
    community_views = assign_views_to_communities(g, communities)
    analyses = []
    for community_index, member_set in enumerate(communities):
        analyses.append(analyze_community(
            g, member_set, community_views.get(community_index, set()),
        ))

    print("[6/6] Writing artifacts...")
    graph_html = render_table_graph_html(table_g, communities,
                                          output_dir / "graph.html")
    communities_md = write_communities_markdown(communities, analyses,
                                                  output_dir / "communities.md")
    report_md = write_validation_report(header, g, table_g, communities, analyses,
                                          output_dir / "validation_report.md")
    print(f"      graph.html             -> {graph_html}")
    print(f"      communities.md         -> {communities_md}")
    print(f"      validation_report.md   -> {report_md}")

    return {
        "graph_html": graph_html,
        "communities_md": communities_md,
        "validation_report": report_md,
        "n_views": len(views),
        "n_tables": n_table,
        "n_communities": len(communities),
    }


# ============================================================================
# CLI ENTRY POINT
# ============================================================================
#
# Allows running this from the shell:
#   python -m tools.diagnostics.validate_graph_pivot CORPUS_PATH OUTPUT_DIR


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate whether the graph pivot is justified for this corpus."
    )
    parser.add_argument("corpus_path", help="Path to corpus.jsonl")
    parser.add_argument("output_dir", help="Directory to write artifacts into")
    parser.add_argument("--resolution", type=float, default=1.0,
                          help="Louvain resolution (default 1.0; >1 = more communities)")
    args = parser.parse_args()

    run_validation(args.corpus_path, args.output_dir, resolution=args.resolution)
    return 0


if __name__ == "__main__":
    sys.exit(main())
