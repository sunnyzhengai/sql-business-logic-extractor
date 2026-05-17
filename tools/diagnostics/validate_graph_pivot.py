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
5. Emits these artifacts in the output directory:
     - communities/community_NN_<top-table>.html  -- one interactive HTML per
                                   community, showing that community's tables
                                   plus bridge tables in muted gray for context
     - communities/index.html   -- linkable index of all per-community HTMLs
     - communities.md           -- per-community summary: primary member views,
                                   top tables, leaf tables, bridge tables.
                                   PLUS a Shared Dimensions section and
                                   a Cross-Domain Views section
     - validation_report.md     -- summary verdict: does the community structure
                                   look healthcare-meaningful? Confidence level.
                                   Recommendation: pivot, revise, or revisit.

Refinements layered on top of the initial validation:

  * BRIDGE-TABLE DETECTION. Dimension/lookup tables like PATIENT, CLARITY_SER,
    CLARITY_DEP appear in nearly every view and connect everything in the graph,
    distorting community detection. We DON'T hard-code a dimension list -- we
    let the graph reveal them: tables in the top N percent by degree are
    classified as BRIDGES and excluded from community detection (but kept in
    the rendered graph and reported separately).

  * INFRASTRUCTURE-VIEW EXCLUSION. Views that extract metadata for cataloging
    (Collibra, Atlas) are infrastructure, not business logic. We exclude views
    whose name matches default patterns (collibra/metadata/catalog/ingest) or
    which read from sys.* / INFORMATION_SCHEMA system schemas. Override via
    --exclude-pattern on the CLI.

  * PRIMARY COMMUNITY PER VIEW. A view is assigned to ONE primary community
    (the one containing the most of its tables). Views that span multiple
    communities are reported separately as Cross-Domain Views -- a finding
    in their own right, not noise.

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
# SECTION 1B -- Infrastructure-view filtering
# ============================================================================
#
# Views that exist purely to extract metadata for catalogs (Collibra, Atlas,
# Purview, ...) join to dozens of tables to harvest schema/usage info. They
# are not business logic. If we leave them in, they pollute the community
# detection by connecting tables that have no business-domain relationship.


# Default substrings (case-insensitive) that mark an infrastructure view.
# Users can append more via the --exclude-pattern CLI argument.
DEFAULT_INFRASTRUCTURE_PATTERNS: tuple[str, ...] = (
    "collibra",
    "metadata",
    "catalog",
    "ingest",
)

# System-schema prefixes -- any view that reads from one of these is almost
# certainly infrastructure. We match on the SQL-qualified source name.
SYSTEM_SCHEMA_PREFIXES: tuple[str, ...] = (
    "sys.",
    "information_schema.",
    "INFORMATION_SCHEMA.",
)


def is_infrastructure_view(view: dict, name_patterns: Iterable[str]) -> bool:
    """Return True if a view looks like metadata/catalog infrastructure.

    Two heuristics:
      1. View name contains one of the configured substring patterns
         (case-insensitive match).
      2. Any scope of the view reads from a system schema (sys.*, etc.).

    These are HEURISTICS. They will both miss some infrastructure views and
    occasionally catch a legitimate business view. The CLI lets users add
    or remove patterns to tune for their corpus.
    """
    name_lower = (view.get("view_name") or "").lower()
    for pat in name_patterns:
        if pat and pat.lower() in name_lower:
            return True

    for scope in view.get("scopes") or []:
        for table_name in scope.get("reads_from_tables") or []:
            t_lower = (table_name or "").lower()
            for prefix in SYSTEM_SCHEMA_PREFIXES:
                if t_lower.startswith(prefix.lower()):
                    return True
    return False


def filter_business_views(views: list[dict],
                            name_patterns: Iterable[str] | None = None,
                            ) -> tuple[list[dict], list[str]]:
    """Split a view list into (business_views, excluded_view_names).

    The excluded list is reported in validation_report.md so the user can
    verify nothing important was filtered out by accident.
    """
    if name_patterns is None:
        name_patterns = DEFAULT_INFRASTRUCTURE_PATTERNS
    patterns = list(name_patterns)

    kept: list[dict] = []
    excluded: list[str] = []
    for v in views:
        if is_infrastructure_view(v, patterns):
            excluded.append(v.get("view_name") or "?")
        else:
            kept.append(v)
    return kept, excluded


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


def detect_bridge_tables(table_g, percentile: float = 90.0) -> set[str]:
    """Identify high-degree "bridge" tables (dimensions / shared lookups).

    Bridge tables are connected to many other tables. They are typically
    dimension tables (PATIENT, CLARITY_SER, CLARITY_DEP) that almost every
    view joins through. If we leave them in the projection, Louvain pulls
    everything into one giant blob because all paths go through them.

    Detection: any table whose degree is at or above the given percentile
    of the degree distribution is classified as a bridge. With percentile=90,
    we flag the top 10% by degree.

    We do NOT hard-code which tables are dimensions. The graph reveals them:
    if PATIENT is in 100 views and FOOBAR_TABLE is in 2, PATIENT's degree
    is dramatically higher and it gets classified as a bridge automatically.

    Returns a set of table-node IDs to exclude from community detection.
    """
    import numpy as np  # stdlib has statistics.quantiles in 3.8+, but numpy is clearer here

    # Degree = number of distinct neighbors. We want a node-level statistic
    # so we use the simple .degree() view (not the multi-edge count).
    degrees = {node: deg for node, deg in table_g.degree()}
    if not degrees:
        return set()

    # Use numpy.percentile to find the cutoff. degrees.values() is a view;
    # list() materializes it for percentile computation.
    cutoff = np.percentile(list(degrees.values()), percentile)
    bridges = {node for node, d in degrees.items() if d >= cutoff and d > 1}
    return bridges


def project_without_bridges(table_g, bridge_nodes: set[str]):
    """Return a copy of `table_g` with bridge tables removed.

    This is what we feed to Louvain. The original `table_g` is preserved
    because we still want bridge tables in the rendered graph (shown muted
    so the user can see how the rest of the structure relates to them).
    """
    import networkx as nx
    g = table_g.copy()
    g.remove_nodes_from(bridge_nodes)
    return g


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


def assign_views_to_communities(
    g, communities: list[set]
) -> tuple[dict[int, set[str]], dict[str, list[int]]]:
    """Assign each view a PRIMARY community plus any communities it spans.

    Two outputs:

      community_to_primary_views[community_index] -> set of view names whose
          primary community is this one. Each view appears in EXACTLY ONE
          community's primary set.

      view_to_spans[view_name] -> sorted list of community indices the view
          touches (its tables span these communities). Length 1 = single-domain
          view. Length > 1 = cross-domain view (its own finding).

    Algorithm:
      - For each view, count how many of its tables fall in each community.
      - The community with the highest count = the view's PRIMARY.
      - All communities with at least one of the view's tables = the spans.
      - Bridge tables are intentionally NOT in any community, so they don't
        contribute to the count -- joining through PATIENT no longer drags a
        view into the "patient community" (there is no such community now).
    """
    # Reverse map: table_node_id -> community_index, for fast lookup.
    table_to_community: dict[str, int] = {}
    for community_index, member_set in enumerate(communities):
        for table_id in member_set:
            table_to_community[table_id] = community_index

    # For each view, accumulate a count of tables per community.
    # view_table_counts[view_name][community_index] = count
    view_table_counts: dict[str, Counter] = defaultdict(Counter)

    for u, v, attrs in g.edges(data=True):
        relation = attrs.get("relation")
        if relation not in ("READS_FROM_TABLE", "JOIN", "BELONGS_TO"):
            continue
        view_name = attrs.get("view")
        if not view_name:
            continue
        # Either endpoint might be a table in a community; count both,
        # but de-dupe per (view, table) pair so a single join doesn't
        # double-count by being seen from both directions in MultiDiGraph.
        for endpoint in (u, v):
            community_index = table_to_community.get(endpoint)
            if community_index is not None:
                view_table_counts[view_name][community_index] += 1

    community_to_primary_views: dict[int, set[str]] = defaultdict(set)
    view_to_spans: dict[str, list[int]] = {}

    for view_name, counter in view_table_counts.items():
        if not counter:
            continue
        # Counter.most_common(1) returns [(community, count)] -- the max.
        primary_community = counter.most_common(1)[0][0]
        community_to_primary_views[primary_community].add(view_name)
        view_to_spans[view_name] = sorted(counter.keys())

    return community_to_primary_views, view_to_spans


# ============================================================================
# SECTION 4 -- Per-community analysis
# ============================================================================


def analyze_community(g, community_tables: set[str],
                       primary_views: set[str]) -> dict:
    """Summarize one community: top tables, leaf tables, primary views, etc.

    Returns a dict with these keys:
      - n_tables          : int    -- number of tables in this community
      - n_primary_views   : int    -- views whose PRIMARY community is this one
      - top_tables        : list   -- (table_name, in_degree) sorted by traversal
      - leaf_tables       : list   -- tables with out_degree 0 (decorative/lookups)
      - core_tables       : list   -- tables with out_degree >= 1 (cohort-shaping)
      - primary_views     : list   -- sorted list of view names (primary assignment)
      - zc_table_count    : int    -- how many tables in the community are ZC_*
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
        "n_primary_views": len(primary_views),
        "top_tables": top_tables[:15],   # cap at 15 for readability
        "leaf_tables": sorted(leaf_tables),
        "core_tables": sorted(core_tables),
        "primary_views": sorted(primary_views),
        "zc_table_count": zc_count,
        # The full set of community-tables (raw node IDs, not labels) for
        # downstream use by the per-community renderer.
        "table_node_ids": set(community_tables),
    }


# ============================================================================
# SECTION 5 -- Rendering (interactive HTML)
# ============================================================================


# Community-color palette. Distinct, colorblind-friendlier than rainbow.
# Cycles past 12 communities (acceptable for validation visualization).
_COMMUNITY_PALETTE: tuple[str, ...] = (
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b",
    "#e377c2", "#7f7f7f", "#bcbd22", "#17becf", "#aec7e8", "#ffbb78",
)

_BRIDGE_COLOR = "#bbbbbb"  # muted gray for bridge/dimension tables (shared context)


def _community_color(community_index: int) -> str:
    """Return a stable color for a community index. Cycles past the palette length."""
    return _COMMUNITY_PALETTE[community_index % len(_COMMUNITY_PALETTE)]


def _safe_filename(s: str) -> str:
    """Convert an arbitrary string to a safe filename fragment.

    Lowercases and replaces anything outside [a-z0-9_] with underscore.
    Used to name per-community HTML files after their top table.
    """
    return "".join(c.lower() if c.isalnum() else "_" for c in s)[:40]


def _compute_static_positions(g, scale: float = 1000.0) -> dict[str, tuple[float, float]]:
    """Compute a deterministic static layout for a graph (no animation).

    Uses networkx's spring_layout with a fixed seed. Returns a dict
    mapping node_id -> (x, y) where x and y are in pyvis pixel coordinates
    (scale of `scale` from the unit square spring_layout returns).

    Why a static layout: pyvis with physics-on animates the graph for
    several seconds while vis.js's force simulation converges, which the
    user found distracting on small graphs. Pre-computing positions in
    networkx + setting physics=False gives the user an instant, stable,
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


def render_community_html(
    table_g, community_index: int, community_tables: set[str],
    bridge_tables: set[str], output_path: str | Path,
) -> str:
    """Render ONE community as interactive HTML, with bridges shown muted.

    Each per-community HTML contains:
      - All tables in this community (colored with the community color)
      - All bridge tables connected to any community-table (colored muted gray)
      - Edges between any pair of the above

    The layout is pre-computed with networkx and frozen (physics=off,
    fixed=true on every node). This gives stewards an instant, readable,
    non-animated view -- the previous behavior animated even small graphs
    for several seconds, which was distracting.
    """
    from pyvis.network import Network

    # Collect the nodes to render: community tables + bridges connected to them.
    nodes_to_render: set[str] = set(community_tables)
    for ct in community_tables:
        if ct not in table_g:
            continue
        for neighbor in table_g.neighbors(ct):
            if neighbor in bridge_tables:
                nodes_to_render.add(neighbor)

    # Build the subgraph as a regular nx.Graph (undirected) to keep pyvis happy.
    sub = table_g.subgraph(nodes_to_render).copy()
    color_for_community = _community_color(community_index)
    positions = _compute_static_positions(sub)

    net = Network(
        height="900px", width="100%",
        directed=False, notebook=False,
        cdn_resources="in_line",
    )

    for node, attrs in sub.nodes(data=True):
        is_zc = attrs.get("is_zc", False)
        is_bridge = node in bridge_tables
        if is_bridge:
            color = _BRIDGE_COLOR
            shape = "diamond"
            size = 18
        else:
            color = color_for_community
            shape = "box" if is_zc else "dot"
            size = 15 if is_zc else 25
        label = attrs.get("label", str(node))
        title_lines = [f"Table: {label}"]
        if is_bridge:
            title_lines.append("Role: BRIDGE (high-degree dimension/shared lookup)")
        if is_zc:
            title_lines.append("Type: ZC lookup")
        x, y = positions.get(node, (0.0, 0.0))
        # `physics=False` + `fixed=True` together pin the node to (x, y).
        # Without `fixed=True`, vis.js still nudges nodes during interaction.
        net.add_node(
            node, label=label, color=color, shape=shape, size=size,
            title="\n".join(title_lines),
            x=x, y=y, physics=False, fixed=True,
        )

    for u, v, attrs in sub.edges(data=True):
        w = attrs.get("weight", 1)
        width = min(1 + w / 2, 8)
        net.add_edge(u, v, value=w, width=width, title=f"co-occurrences: {w}")

    # Disable the simulation globally so the canvas does not "settle" on load.
    net.toggle_physics(False)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    net.write_html(str(out), notebook=False)
    return str(out)


def render_overview_html(
    table_g, communities: list[set], bridge_tables: set[str],
    output_path: str | Path,
) -> str:
    """Render the FULL table graph, colored by community. Useful as an overview.

    For corpora with many tables this will be dense. Per-community HTMLs are
    a better daily-driver; this is the "see the whole landscape" view.

    Layout is pre-computed with networkx so densely-connected nodes
    (community members) end up near each other in space, giving a
    community-centric visual without animation.
    """
    from pyvis.network import Network

    # Map each node to its community color (or bridge color).
    node_to_color: dict[str, str] = {}
    for community_index, member_set in enumerate(communities):
        c = _community_color(community_index)
        for node in member_set:
            node_to_color[node] = c
    for node in bridge_tables:
        node_to_color[node] = _BRIDGE_COLOR

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
    return str(out)


def render_communities_index_html(
    community_html_files: list[tuple[int, str, str, int, int]],
    output_path: str | Path,
) -> str:
    """Write a small index.html listing all per-community HTMLs.

    Each entry in `community_html_files` is:
       (community_index, top_table_label, html_filename, n_tables, n_views)
    """
    parts: list[str] = []
    parts.append("<!doctype html><html><head><meta charset='utf-8'>")
    parts.append("<title>Graph-pivot communities</title>")
    parts.append("<style>")
    parts.append("body { font-family: -apple-system, system-ui, sans-serif; "
                 "max-width: 800px; margin: 40px auto; padding: 0 20px; }")
    parts.append("h1 { color: #333; }")
    parts.append("table { border-collapse: collapse; width: 100%; margin: 20px 0; }")
    parts.append("th, td { text-align: left; padding: 8px 12px; "
                 "border-bottom: 1px solid #ddd; }")
    parts.append("th { background: #f4f4f4; }")
    parts.append("a { color: #1f77b4; text-decoration: none; }")
    parts.append("a:hover { text-decoration: underline; }")
    parts.append(".color-swatch { display: inline-block; width: 14px; height: 14px; "
                 "border-radius: 3px; vertical-align: middle; margin-right: 6px; }")
    parts.append("</style></head><body>")
    parts.append("<h1>Graph-pivot communities</h1>")
    parts.append("<p>Click a community to see its interactive graph. Bridge tables "
                 "(dimensions / shared lookups) are shown in muted gray.</p>")
    parts.append("<table><thead><tr><th>#</th><th>Top table</th><th>Tables</th>"
                 "<th>Member views</th><th>Open</th></tr></thead><tbody>")
    for community_index, top_table, fname, n_tables, n_views in community_html_files:
        c = _community_color(community_index)
        parts.append("<tr>")
        parts.append(f"<td><span class='color-swatch' style='background:{c}'></span>"
                     f"{community_index}</td>")
        parts.append(f"<td><code>{top_table}</code></td>")
        parts.append(f"<td>{n_tables}</td>")
        parts.append(f"<td>{n_views}</td>")
        parts.append(f"<td><a href='{fname}'>open &rarr;</a></td>")
        parts.append("</tr>")
    parts.append("</tbody></table></body></html>")

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(parts), encoding="utf-8")
    return str(out)


# ============================================================================
# SECTION 6 -- Markdown reporting
# ============================================================================


def write_communities_markdown(
    communities: list[set],
    analyses: list[dict],
    bridge_table_labels: list[str],
    bridge_to_neighbor_communities: dict[str, list[int]],
    view_to_spans: dict[str, list[int]],
    excluded_infrastructure_views: list[str],
    output_path: str | Path,
) -> str:
    """Write the per-community summary to markdown.

    Top of file: overall summary + shared dimensions + excluded infra views
    + cross-domain views. Then one section per community with primary views.

    Cross-domain views: any view in `view_to_spans` whose spans list has
    length > 1. These are reported separately so they don't pollute the
    primary-view lists of individual communities.
    """
    lines: list[str] = []
    lines.append("# Communities discovered by Louvain on the table-projection graph")
    lines.append("")
    lines.append(f"Total communities: {len(communities)}")
    lines.append("")
    lines.append("Each community is a set of tables that frequently co-appear in scopes")
    lines.append("of the same views. Communities should correspond to recognizable")
    lines.append("subject areas (e.g., Epic clinic encounters, claims, billing).")
    lines.append("")
    lines.append("Each view is assigned to its **primary community** -- the one")
    lines.append("containing the most of its tables. Views spanning multiple")
    lines.append("communities are listed separately under **Cross-Domain Views**.")
    lines.append("")

    # ----- Shared dimensions (bridge tables) -----
    lines.append("## Shared Dimensions (bridge tables)")
    lines.append("")
    if bridge_table_labels:
        lines.append("These tables have very high degree -- they connect to many other")
        lines.append("tables across the corpus. They are typically dimension tables")
        lines.append("(PATIENT, CLARITY_SER, CLARITY_DEP, etc.) that almost every view")
        lines.append("joins through. They are excluded from community detection because")
        lines.append("they would otherwise drag everything into one giant cluster.")
        lines.append("")
        for label in sorted(bridge_table_labels):
            neighbors = bridge_to_neighbor_communities.get(label, [])
            n_neighbors = len(neighbors)
            lines.append(f"- `{label}` -- bridges {n_neighbors} communities")
    else:
        lines.append("_(none detected at the current bridge-percentile threshold)_")
    lines.append("")

    # ----- Cross-domain views -----
    cross_domain = sorted([
        (v, spans) for v, spans in view_to_spans.items() if len(spans) > 1
    ])
    lines.append(f"## Cross-Domain Views ({len(cross_domain)})")
    lines.append("")
    lines.append("Views whose tables span 2+ communities. These are NOT noise -- they")
    lines.append("are reports that reach across business domains, and stewards should")
    lines.append("decide whether they should be split, consolidated, or kept as-is.")
    lines.append("")
    if cross_domain:
        for view_name, spans in cross_domain[:50]:  # cap at 50 to keep the file scannable
            spans_str = ", ".join(str(c) for c in spans)
            lines.append(f"- `{view_name}` spans communities: {spans_str}")
        if len(cross_domain) > 50:
            lines.append(f"- ... and {len(cross_domain) - 50} more")
    else:
        lines.append("_(none)_")
    lines.append("")

    # ----- Excluded infrastructure views -----
    if excluded_infrastructure_views:
        lines.append(f"## Excluded Infrastructure Views ({len(excluded_infrastructure_views)})")
        lines.append("")
        lines.append("These views were filtered out before community detection because")
        lines.append("they match infrastructure heuristics (metadata/catalog/ingest in")
        lines.append("the name, or reading from sys.* / INFORMATION_SCHEMA). Inspect to")
        lines.append("ensure no business-critical views were excluded by accident.")
        lines.append("")
        for v in sorted(excluded_infrastructure_views):
            lines.append(f"- `{v}`")
        lines.append("")

    # ----- Per-community sections -----
    for community_index, analysis in enumerate(analyses):
        lines.append(f"## Community {community_index} -- "
                     f"{analysis['n_tables']} tables, "
                     f"{analysis['n_primary_views']} primary views")
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
        lines.append(f"### Primary views ({len(analysis['primary_views'])})")
        for v in analysis["primary_views"]:
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
    n_bridge_tables: int,
    n_excluded_views: int,
    n_cross_domain_views: int,
    output_path: str | Path,
) -> str:
    """Write the verdict / recommendation document.

    This is the artifact that decides whether we pivot the codebase. It is
    intentionally short and human-readable; the underlying data lives in
    communities.md and the per-community HTMLs.
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
    lines.append(f"- Infrastructure views excluded: **{n_excluded_views}**")
    lines.append(f"- Distinct tables: **{n_tables}**")
    lines.append(f"- Bridge tables (shared dimensions): **{n_bridge_tables}**")
    lines.append(f"- Communities found: **{n_communities}**")
    lines.append(f"- Average community size: **{avg_community_size:.1f}** tables")
    lines.append(f"- Largest community: **{largest_size}** tables "
                  f"(**{largest_pct:.0f}%** of all tables)")
    lines.append(f"- Cross-domain views (span 2+ communities): **{n_cross_domain_views}**")
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


def run_validation(
    corpus_path: str | Path,
    output_dir: str | Path,
    resolution: float = 1.0,
    bridge_percentile: float = 90.0,
    exclude_patterns: Iterable[str] | None = None,
) -> dict:
    """Run the full validation pipeline. Returns a dict of output paths + stats.

    This is the entry point you call from a Fabric notebook:

        from tools.diagnostics.validate_graph_pivot import run_validation
        result = run_validation(
            corpus_path="/lakehouse/.../corpus.jsonl",
            output_dir="/lakehouse/.../validation_out",
            resolution=1.0,            # try 0.5 for fewer, broader communities
            bridge_percentile=90.0,    # top 10% by degree are flagged as bridges
            exclude_patterns=None,     # uses DEFAULT_INFRASTRUCTURE_PATTERNS
        )
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    communities_dir = output_dir / "communities"
    communities_dir.mkdir(parents=True, exist_ok=True)

    print(f"[1/8] Loading corpus from {corpus_path}...")
    header, all_views = load_corpus(corpus_path)
    print(f"      Loaded {len(all_views)} views (header says {header.get('n_views', '?')})")

    print("[2/8] Filtering infrastructure views...")
    views, excluded_views = filter_business_views(all_views, exclude_patterns)
    print(f"      Kept {len(views)} business views; excluded {len(excluded_views)}")
    if excluded_views:
        print(f"      Excluded: {', '.join(excluded_views[:10])}"
              f"{'...' if len(excluded_views) > 10 else ''}")

    print("[3/8] Building typed graph...")
    g = build_graph(views)
    n_table = sum(1 for _, a in g.nodes(data=True) if a.get("ntype") == "table")
    print(f"      Graph: {g.number_of_nodes()} nodes, {g.number_of_edges()} edges, "
          f"{n_table} distinct tables")

    print("[4/8] Extracting table-projection subgraph...")
    table_g = extract_table_projection(g)
    print(f"      Projection: {table_g.number_of_nodes()} tables, "
          f"{table_g.number_of_edges()} weighted edges")

    print(f"[5/8] Detecting bridge tables (top {100 - bridge_percentile:.0f}% by degree)...")
    bridge_nodes = detect_bridge_tables(table_g, percentile=bridge_percentile)
    bridge_labels = sorted(g.nodes[b].get("label", b) for b in bridge_nodes if b in g)
    print(f"      Bridge tables: {len(bridge_nodes)}")
    if bridge_labels:
        preview = ", ".join(bridge_labels[:8])
        print(f"      Examples: {preview}{'...' if len(bridge_labels) > 8 else ''}")
    projection_for_louvain = project_without_bridges(table_g, bridge_nodes)

    print(f"[6/8] Running Louvain community detection (resolution={resolution})...")
    communities = detect_table_communities(projection_for_louvain, resolution=resolution)
    print(f"      Found {len(communities)} communities, "
          f"sizes (top 10): {sorted([len(c) for c in communities], reverse=True)[:10]}")

    print("[7/8] Assigning views to primary communities + finding cross-domain views...")
    community_to_primary, view_to_spans = assign_views_to_communities(g, communities)
    cross_domain = [v for v, spans in view_to_spans.items() if len(spans) > 1]
    print(f"      Cross-domain views: {len(cross_domain)}")

    # Per-community analysis using primary-view assignments only.
    analyses = []
    for community_index, member_set in enumerate(communities):
        analyses.append(analyze_community(
            g, member_set, community_to_primary.get(community_index, set()),
        ))

    print("[8/8] Writing artifacts...")
    # Per-community HTMLs
    community_html_files: list[tuple[int, str, str, int, int]] = []
    for community_index, (community_set, analysis) in enumerate(zip(communities, analyses)):
        # Name the per-community HTML after its top table (most connected within the community).
        top_label = analysis["top_tables"][0][0] if analysis["top_tables"] else f"community_{community_index}"
        safe = _safe_filename(top_label)
        fname = f"community_{community_index:02d}_{safe}.html"
        render_community_html(
            table_g, community_index, community_set, bridge_nodes,
            communities_dir / fname,
        )
        community_html_files.append((
            community_index, top_label, fname,
            analysis["n_tables"], analysis["n_primary_views"],
        ))
    index_html = render_communities_index_html(community_html_files,
                                                  communities_dir / "index.html")
    overview_html = render_overview_html(table_g, communities, bridge_nodes,
                                            output_dir / "graph.html")

    # Build bridge -> communities-it-touches map for the markdown report.
    bridge_to_neighbor_communities: dict[str, list[int]] = {}
    label_for_node = lambda n: g.nodes[n].get("label", n) if n in g else n
    for bridge in bridge_nodes:
        touched: set[int] = set()
        if bridge in table_g:
            for neighbor in table_g.neighbors(bridge):
                for community_index, member_set in enumerate(communities):
                    if neighbor in member_set:
                        touched.add(community_index)
        bridge_to_neighbor_communities[label_for_node(bridge)] = sorted(touched)

    communities_md = write_communities_markdown(
        communities, analyses,
        bridge_table_labels=bridge_labels,
        bridge_to_neighbor_communities=bridge_to_neighbor_communities,
        view_to_spans=view_to_spans,
        excluded_infrastructure_views=excluded_views,
        output_path=output_dir / "communities.md",
    )

    report_md = write_validation_report(
        header, g, table_g, communities, analyses,
        n_bridge_tables=len(bridge_nodes),
        n_excluded_views=len(excluded_views),
        n_cross_domain_views=len(cross_domain),
        output_path=output_dir / "validation_report.md",
    )

    print(f"      graph.html (overview)     -> {overview_html}")
    print(f"      communities/index.html    -> {index_html}")
    print(f"      communities.md            -> {communities_md}")
    print(f"      validation_report.md      -> {report_md}")

    return {
        "graph_html": overview_html,
        "communities_index_html": index_html,
        "communities_md": communities_md,
        "validation_report": report_md,
        "n_views_total": len(all_views),
        "n_views_business": len(views),
        "n_views_excluded": len(excluded_views),
        "n_tables": n_table,
        "n_bridge_tables": len(bridge_nodes),
        "n_communities": len(communities),
        "n_cross_domain_views": len(cross_domain),
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
    parser.add_argument(
        "--resolution", type=float, default=1.0,
        help="Louvain resolution (default 1.0). Lower (e.g. 0.5) -> fewer, "
             "broader communities. Higher (e.g. 1.5) -> more, finer ones.",
    )
    parser.add_argument(
        "--bridge-percentile", type=float, default=90.0,
        help="Tables in the top (100 - bridge_percentile) %% by degree are "
             "classified as bridges (dimensions / shared lookups) and "
             "excluded from community detection. Default 90 means top 10%%.",
    )
    parser.add_argument(
        "--exclude-pattern", action="append", default=None,
        help="Case-insensitive substring; views whose name matches are excluded "
             "as infrastructure. Repeatable. If not supplied, uses defaults: "
             f"{', '.join(DEFAULT_INFRASTRUCTURE_PATTERNS)}.",
    )
    args = parser.parse_args()

    run_validation(
        args.corpus_path,
        args.output_dir,
        resolution=args.resolution,
        bridge_percentile=args.bridge_percentile,
        exclude_patterns=args.exclude_pattern,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
