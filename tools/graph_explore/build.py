"""Build networkx graphs from corpus.jsonl ViewV1 dicts.

Node ID conventions (ensure uniqueness when multiple views are combined):

  view:    `<view_name>`
  scope:   `<view_name>::<scope_id>`
  column:  `<view_name>::<scope_id>::<column_name>`
  filter:  `<view_name>::<scope_id>::filter_<index>`
  table:   `table::<bare_table_name>`     -- GLOBAL across views

Tables being global means a single node represents PATIENT regardless
of how many views touch it -- exactly what you want when looking for
shared subjects across views.

Each node carries an `ntype` attribute (one of "view"/"scope"/
"column"/"table"/"filter") and a `label` (short name shown in
visualizations). Tooltips live in `title`.

Edges carry a `relation` attribute. Common values:
  HAS_SCOPE          view -> scope
  READS_FROM_SCOPE   scope -> scope (CTE / derived references)
  READS_FROM_TABLE   scope -> table (FROM clause)
  JOINS              scope -> table (JOIN right-side)
  CONTAINS_COLUMN    scope -> column
  DERIVED_FROM       column -> column (cross-scope dataflow)
  REFERENCES_TABLE   column -> table (when base_columns is a base table)
  HAS_FILTER         scope -> filter
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Optional


def _bare(name: str) -> str:
    return (name or "").split(".")[-1].strip()


def _ensure_table_node(g, bare_table: str) -> str:
    """Add a global table node (idempotent) and return its id."""
    table_id = f"table::{bare_table}"
    if table_id not in g:
        is_zc = bare_table.upper().startswith("ZC_")
        g.add_node(
            table_id,
            ntype="table",
            label=bare_table,
            title=f"Table: {bare_table}" + ("  (ZC lookup)" if is_zc else ""),
            is_zc=is_zc,
        )
    return table_id


def build_view_graph(view_dict: dict):
    """One ViewV1 dict -> networkx MultiDiGraph.

    Nodes are typed (view / scope / column / table / filter) with
    label and tooltip attributes. Edges carry a `relation` attribute.
    """
    import networkx as nx
    g = nx.MultiDiGraph()
    view_name = view_dict.get("view_name") or "view"
    view_id = view_name
    g.add_node(
        view_id,
        ntype="view",
        label=view_name,
        title=f"View: {view_name}",
    )

    for scope in view_dict.get("scopes") or []:
        scope_raw_id = scope.get("id") or "?"
        scope_node_id = f"{view_name}::{scope_raw_id}"
        kind = scope.get("kind") or ""
        n_cols = len(scope.get("columns") or [])
        n_filters = len(scope.get("filters") or [])
        g.add_node(
            scope_node_id,
            ntype="scope",
            label=scope_raw_id,
            title=(
                f"Scope: {scope_raw_id} (kind={kind})\n"
                f"Columns: {n_cols}\n"
                f"Filters: {n_filters}"
            ),
            scope_kind=kind,
        )
        g.add_edge(view_id, scope_node_id, relation="HAS_SCOPE")

        # Tables read from in this scope (FROM clause + flat reads_from_tables)
        for t in scope.get("reads_from_tables") or []:
            bare = _bare(t)
            if not bare or ":" in bare:
                continue
            table_id = _ensure_table_node(g, bare)
            g.add_edge(scope_node_id, table_id, relation="READS_FROM_TABLE")

        # Cross-scope reads (CTE / derived references)
        for ref in scope.get("reads_from_scopes") or []:
            if not ref:
                continue
            target = f"{view_name}::{ref}"
            g.add_edge(scope_node_id, target, relation="READS_FROM_SCOPE")

        # Joins (right-side tables)
        for j in scope.get("joins") or []:
            rt = _bare(j.get("right_table") or "")
            if not rt or ":" in rt:
                continue
            table_id = _ensure_table_node(g, rt)
            g.add_edge(
                scope_node_id, table_id,
                relation="JOINS",
                join_type=j.get("join_type") or "JOIN",
            )

        # Columns in this scope
        for col in scope.get("columns") or []:
            col_name = col.get("column_name") or ""
            if not col_name:
                continue
            col_node_id = f"{view_name}::{scope_raw_id}::{col_name}"
            biz = (col.get("business_description") or "").replace("\n", " ")
            tech = (col.get("technical_description") or "").replace("\n", " ")
            tooltip = (
                f"Column: {col_name}\n"
                f"Type: {col.get('column_type', '')}\n"
                f"English: {biz[:200]}\n"
                f"SQL:     {tech[:200]}"
            )
            g.add_node(
                col_node_id,
                ntype="column",
                label=col_name,
                title=tooltip,
                column_type=col.get("column_type") or "",
            )
            g.add_edge(scope_node_id, col_node_id, relation="CONTAINS_COLUMN")

            # base_columns: scope-qualified ("cte:X.col") or table-qualified ("table:T.col")
            for bc in col.get("base_columns") or []:
                if bc.startswith("table:"):
                    body = bc[len("table:"):]
                    parts = body.rsplit(".", 1)
                    if len(parts) == 2:
                        tbl, ref_col = parts
                        bare = _bare(tbl)
                        if bare and ":" not in bare:
                            target = _ensure_table_node(g, bare)
                            g.add_edge(
                                col_node_id, target,
                                relation="REFERENCES_TABLE",
                                ref_column=ref_col,
                            )
                else:
                    # cte:X.col / derived:y.col -> dataflow edge to upstream column
                    ref_scope, _, ref_col = bc.partition(".")
                    if ref_scope and ref_col:
                        target = f"{view_name}::{ref_scope}::{ref_col}"
                        g.add_edge(col_node_id, target, relation="DERIVED_FROM")

        # Filters
        for i, f in enumerate(scope.get("filters") or []):
            filter_id = f"{view_name}::{scope_raw_id}::filter_{i}"
            kind_f = f.get("kind") or "where"
            expr = (f.get("expression") or "").replace("\n", " ")
            eng = (f.get("english") or "").replace("\n", " ")
            g.add_node(
                filter_id,
                ntype="filter",
                label=f"f{i}",
                title=(
                    f"Filter (kind={kind_f}):\n"
                    f"  SQL: {expr[:200]}\n"
                    f"  English: {eng[:200]}"
                ),
                filter_kind=kind_f,
            )
            g.add_edge(scope_node_id, filter_id, relation="HAS_FILTER")

    return g


def build_cluster_graph(view_dicts: Iterable[dict]):
    """Combine multiple views' graphs into one. Tables are merged by
    global ID, so cross-view edges through shared tables show up.
    Other nodes (views/scopes/columns/filters) stay per-view."""
    import networkx as nx
    g = nx.MultiDiGraph()
    for vd in view_dicts:
        sub = build_view_graph(vd)
        for node, attrs in sub.nodes(data=True):
            if node not in g:
                g.add_node(node, **attrs)
        for u, v, attrs in sub.edges(data=True):
            g.add_edge(u, v, **attrs)
    return g


def build_corpus_graph(
    corpus_path: str | Path,
    view_filter: Optional[Iterable[str]] = None,
):
    """Load corpus.jsonl, build a graph for all views or a filtered
    subset. `view_filter` is an iterable of view names; only those are
    included. None means all views."""
    p = Path(corpus_path)
    wanted = set(view_filter) if view_filter is not None else None
    views: list[dict] = []
    with p.open(encoding="utf-8") as f:
        next(f, None)  # skip header
        for line in f:
            line = line.strip()
            if not line:
                continue
            v = json.loads(line)
            if wanted is None or v.get("view_name") in wanted:
                views.append(v)
    return build_cluster_graph(views)
