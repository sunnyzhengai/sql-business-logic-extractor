"""Build the unified networkx graph for the whole corpus.

This is the substrate that all downstream analysis runs on. Tables and
columns become GLOBAL nodes (one node per bare name, regardless of how
many views touch them). Every edge carries `view` and `scope`
provenance so we can attribute findings back to the source view + the
specific scope inside it (main vs CTE vs subquery).

Two entry points:

  build_graph(views)           -- in-memory list/iterable of ViewV1 dicts
  build_corpus_graph(path)     -- convenience: load_corpus(path) then build

Graph schema
------------
Each node has an `ntype` attribute identifying its type:

  view    -- id = f"view::{view_name}"
  scope   -- id = f"scope::{view_name}::{scope_id}"
  table   -- id = f"table::{bare_table_name}"  (GLOBAL across views)
  column  -- id = f"col::{view_name}::{scope_id}::{column_name}"

Each edge has a `relation` attribute. The relations and what carries them:

  HAS_SCOPE          view -> scope
  READS_FROM_TABLE   scope -> table   (with view, scope provenance)
  JOIN               table -> table   (with view, scope, join_type)
  CO_OCCURS_IN_SCOPE table -> table   (every pair within a scope; this is
                                       the input to community detection)
  CONTAINS_COLUMN    scope -> column  (with role -- currently always "select")
  BELONGS_TO         column -> table  (when base_columns references a table)
  REFERENCES_SCOPE   scope -> scope   (CTE references another CTE in same view)

Why MultiDiGraph: a single (Table, Table) pair may be joined in many views.
We want to keep each instance so we can attribute joins back to their
view + scope.

Historical note
---------------
This module was previously the `tools.graph_explore.build` set of three
functions (`build_view_graph`, `build_cluster_graph`, `build_corpus_graph`)
which produced a lighter graph schema with column-level edges
(DERIVED_FROM, REFERENCES_TABLE, HAS_FILTER, JOINS plural).

In Phase 2b of the 2026-05 restructure, the production graph builder
that powered the validation diagnostic (`tools.operate.validate_graph_pivot`)
was promoted here. That version has:
- CO_OCCURS_IN_SCOPE edges (the secret sauce for community detection)
- CTE-name detection that catches joins whose right_table strips the
  `cte:` prefix
- Per-view scope-name collection so CTE references aren't mistaken for
  real tables
- Single entry point `build_graph(views)` instead of three separate ones

The old, lighter graph_explore-era functions were deleted; git history
preserves them at the parent commit. The `build_corpus_graph(path)`
convenience function survives, now backed by the new `build_graph`.
"""

from __future__ import annotations

from typing import Iterable, Optional
from pathlib import Path

from tools.shared.corpus_io import load_corpus
from tools.shared.table_names import (
    bare_table_name,
    is_cte_or_scope_reference,
    is_zc_table,
)


def build_graph(views: Iterable[dict]):
    """Build a typed networkx MultiDiGraph from an iterable of ViewV1 dicts.

    See module docstring for the node/edge schema. This is the primary
    entry point used by every downstream phase (p30_analyze, p40_synthesize,
    p50_present) and by the validation diagnostic in `tools.operate`.

    Returns a `networkx.MultiDiGraph`.
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


def build_corpus_graph(corpus_path: str | Path,
                        view_filter: Optional[Iterable[str]] = None):
    """Convenience: load a corpus.jsonl file and build a graph from it.

    Equivalent to:
        header, views = load_corpus(corpus_path)
        if view_filter is not None:
            views = [v for v in views if v["view_name"] in view_filter]
        return build_graph(views)

    `view_filter` is an iterable of view names; if provided, only those
    views are included in the resulting graph. None means all views.
    """
    _, views = load_corpus(corpus_path)
    if view_filter is not None:
        wanted = set(view_filter)
        views = [v for v in views if v.get("view_name") in wanted]
    return build_graph(views)


# ---------------------------------------------------------------------------
# Internals -- module-private helpers used by build_graph()
# ---------------------------------------------------------------------------


def _collect_scope_names(view: dict) -> set[str]:
    """Return the set of bare scope names defined in this view.

    Strips `cte:` / `derived:` / `exists:` / `union:` prefixes so the result
    matches what shows up in `joins[].right_table` (which is always the bare
    name, no prefix). Used by `_add_scope_to_graph` to decide whether a
    `right_table` is an actual table or a reference to a sibling CTE/scope
    inside the same view.
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
    """Add one scope (and its tables / joins / columns / cross-refs) to `g`.

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
        bare = bare_table_name(table_name)
        if not bare or is_cte_or_scope_reference(bare):
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
    # edge to every other table in scope further down.
    from_table = next(iter(scope_table_set), None)  # arbitrary "first" element

    for join in scope.get("joins") or []:
        right = bare_table_name(join.get("right_table") or "")
        if not right or is_cte_or_scope_reference(right):
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

    # ----- Cross-scope references (CTE / derived references) -----
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
            bare = bare_table_name(tbl)
            if not bare or is_cte_or_scope_reference(bare):
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
        zc = is_zc_table(bare_table)
        g.add_node(
            table_id,
            ntype="table",
            label=bare_table,
            is_zc=zc,
            title=f"Table: {bare_table}" + ("  (ZC lookup)" if zc else ""),
        )
    return table_id
