"""JIT Phase 1: structural question-answering over the corpus.

This module provides the ``ask()`` entry point and the ``StructuralIndex``
that powers it. No LLM is needed at query time -- all answers come from
deterministic graph/corpus lookups.

Quick start in a notebook::

    from tools.jit.ask import build_index, ask
    build_index("/lakehouse/.../corpus.jsonl")
    ask("which views use the REFERRAL table?")

The index is built once (takes ~1-2s for a few hundred views) and cached
in a module-level global. Subsequent ``ask()`` calls are O(1) lookups.
"""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path
from typing import Optional

from tools.shared.table_names import bare_table_name, is_zc_table


# ---------------------------------------------------------------------------
# StructuralIndex -- the pre-computed reverse lookup tables
# ---------------------------------------------------------------------------

class StructuralIndex:
    """In-memory index over a corpus for fast structural lookups.

    Built once from corpus.jsonl (and optionally the graph for table
    importance). Provides O(1) lookups by table, column, view name, and
    filter pattern.
    """

    def __init__(self, views: list[dict],
                 table_scores: dict | None = None):
        self.views_by_name: dict[str, dict] = {}
        self.table_to_views: dict[str, set[str]] = defaultdict(set)
        self.column_to_views: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
        self.filter_to_views: dict[str, list[tuple[str, str]]] = defaultdict(list)
        self.all_table_names: set[str] = set()
        self.all_view_names: set[str] = set()
        self.table_scores = table_scores or {}

        for view in views:
            vname = view.get("view_name", "")
            self.views_by_name[vname] = view
            self.all_view_names.add(vname.upper())

            for scope in view.get("scopes") or []:
                scope_id = scope.get("id", "main")

                # Table reverse index
                for table in scope.get("reads_from_tables") or []:
                    bare = bare_table_name(table).upper()
                    if bare and ":" not in bare:
                        self.table_to_views[bare].add(vname)
                        self.all_table_names.add(bare)
                for join in scope.get("joins") or []:
                    rt = bare_table_name(join.get("right_table") or "").upper()
                    if rt and ":" not in rt:
                        self.table_to_views[rt].add(vname)
                        self.all_table_names.add(rt)

                # Column reverse index: (view_name, scope_id, definition)
                for col in scope.get("columns") or []:
                    col_name = (col.get("column_name") or "").upper()
                    if col_name:
                        defn = (col.get("business_description")
                                or col.get("technical_description")
                                or col.get("column_name", ""))
                        self.column_to_views[col_name].append(
                            (vname, scope_id, defn)
                        )

                # Filter reverse index: normalized expression -> (view, scope)
                for filt in scope.get("filters") or []:
                    expr = (filt.get("expression") or "").strip()
                    if expr:
                        key = _normalize_filter(expr)
                        self.filter_to_views[key].append((vname, scope_id))

    # ---- Query methods ----

    def find_by_table(self, table_name: str) -> list[dict]:
        """Find all views that reference a table. Returns enriched results."""
        bare = bare_table_name(table_name).upper()
        view_names = sorted(self.table_to_views.get(bare, set()))
        results = []
        for vname in view_names:
            view = self.views_by_name.get(vname)
            if not view:
                continue
            report = view.get("report") or {}
            results.append({
                "view_name": vname,
                "business_description": report.get("business_description", ""),
                "primary_purpose": report.get("primary_purpose", ""),
                "key_metrics": report.get("key_metrics", []),
                "table_role": self._table_role_in_view(bare, view),
            })
        return results

    def find_by_column(self, column_name: str) -> list[dict]:
        """Find all views that produce a column. Returns with definitions."""
        key = column_name.upper()
        entries = self.column_to_views.get(key, [])
        results = []
        seen = set()
        for vname, scope_id, defn in entries:
            if vname in seen:
                continue
            seen.add(vname)
            view = self.views_by_name.get(vname)
            report = (view or {}).get("report") or {}
            results.append({
                "view_name": vname,
                "scope": scope_id,
                "column_definition": defn,
                "business_description": report.get("business_description", ""),
            })
        return results

    def describe_view(self, view_name: str) -> dict | None:
        """Return full description of a view including columns and filters."""
        # Try exact match first, then case-insensitive
        view = self.views_by_name.get(view_name)
        if not view:
            for vname, v in self.views_by_name.items():
                if vname.upper() == view_name.upper():
                    view = v
                    break
        if not view:
            return None

        report = view.get("report") or {}
        tables = set()
        columns = []
        filters = []
        for scope in view.get("scopes") or []:
            for t in scope.get("reads_from_tables") or []:
                bare = bare_table_name(t)
                if bare and ":" not in bare:
                    tables.add(bare)
            for col in scope.get("columns") or []:
                columns.append({
                    "name": col.get("column_name", ""),
                    "scope": scope.get("id", "main"),
                    "definition": (col.get("business_description")
                                   or col.get("technical_description") or ""),
                    "type": col.get("column_type", ""),
                })
            for filt in scope.get("filters") or []:
                english = filt.get("english") or filt.get("expression") or ""
                if english:
                    filters.append({
                        "expression": filt.get("expression", ""),
                        "english": english,
                        "scope": scope.get("id", "main"),
                    })

        # Annotate tables with importance
        table_list = []
        for t in sorted(tables):
            score, role = self.table_scores.get(t.upper(), (0.0, ""))
            table_list.append({"name": t, "role": role, "importance": score})
        table_list.sort(key=lambda x: x["importance"], reverse=True)

        return {
            "view_name": view.get("view_name", ""),
            "business_description": report.get("business_description", ""),
            "technical_description": report.get("technical_description", ""),
            "primary_purpose": report.get("primary_purpose", ""),
            "key_metrics": report.get("key_metrics", []),
            "tables": table_list,
            "columns": columns,
            "filters": filters,
        }

    def find_by_filter(self, search_term: str) -> list[dict]:
        """Find views whose filters mention a term (e.g., 'denied', 'active').

        Searches both the raw SQL expression AND the English translation,
        since business terms like 'denied' typically appear in the English
        but not in the raw SQL (which has code values like STATUS_C = 5).
        """
        term = search_term.upper()
        results = []
        seen = set()
        for vname, view in self.views_by_name.items():
            matching_filters = []
            for scope in view.get("scopes") or []:
                for filt in scope.get("filters") or []:
                    expr = (filt.get("expression") or "").upper()
                    english = (filt.get("english") or "").upper()
                    if term in expr or term in english:
                        matching_filters.append(
                            filt.get("english") or filt.get("expression") or ""
                        )
            if matching_filters and vname not in seen:
                seen.add(vname)
                report = view.get("report") or {}
                results.append({
                    "view_name": vname,
                    "matching_filters": matching_filters[:5],
                    "business_description": report.get("business_description", ""),
                })
        return results

    # ---- Helpers ----

    def _table_role_in_view(self, bare_upper: str, view: dict) -> str:
        """Determine whether a table is the driver, joined, or lookup in a view."""
        # Check if it's the first table in reads_from_tables (= driver/FROM table)
        for scope in view.get("scopes") or []:
            tables = scope.get("reads_from_tables") or []
            if tables:
                first = bare_table_name(tables[0]).upper()
                if first == bare_upper:
                    return "driver"
        # Check if it's a ZC table
        if is_zc_table(bare_upper):
            return "lookup"
        return "joined"


def _normalize_filter(expr: str) -> str:
    """Normalize a filter expression for dedup/matching."""
    return re.sub(r"\s+", " ", expr.strip().upper())


# ---------------------------------------------------------------------------
# Module-level index + public API
# ---------------------------------------------------------------------------

_INDEX: Optional[StructuralIndex] = None


def build_index(corpus_path: str | Path,
                schema_path: str | None = None) -> StructuralIndex:
    """Build (or rebuild) the structural index from a corpus.

    Call once in a notebook session. Subsequent ``ask()`` calls use the
    cached index. Takes ~1-2s for a few hundred views.

    Parameters
    ----------
    corpus_path : path to corpus.jsonl
    schema_path : optional path to clarity_schema.yaml (enables FK-based
                  table importance in results)
    """
    global _INDEX
    from tools.shared.corpus_io import load_corpus

    _, views = load_corpus(corpus_path)

    # Build table importance scores if we can
    table_scores = None
    try:
        from tools.p20_index.graph_builder import build_graph
        from tools.p30_analyze.projection import extract_table_projection
        from tools.p30_analyze.bridges import detect_bridge_tables, project_without_bridges
        from tools.p30_analyze.communities import detect_table_communities
        from tools.p30_analyze.table_importance import (
            rank_all_communities, build_table_scores_lookup,
        )
        g = build_graph(views)
        table_g = extract_table_projection(g)
        bridges = detect_bridge_tables(table_g)
        table_g_no_bridges = project_without_bridges(table_g, bridges)
        communities = detect_table_communities(table_g_no_bridges)
        # Re-insert bridges so they get scored
        for bridge_id in bridges:
            best_comm, best_count = 0, 0
            for ci, comm in enumerate(communities):
                count = sum(1 for t in comm if table_g.has_edge(bridge_id, t))
                if count > best_count:
                    best_comm, best_count = ci, count
            if best_count > 0:
                communities[best_comm].add(bridge_id)
        rankings = rank_all_communities(g, communities, schema_path=schema_path)
        table_scores = build_table_scores_lookup(rankings)
    except Exception:
        table_scores = None

    _INDEX = StructuralIndex(views, table_scores=table_scores)
    n_views = len(_INDEX.views_by_name)
    n_tables = len(_INDEX.all_table_names)
    print(f"JIT index built: {n_views} views, {n_tables} tables")
    if table_scores:
        print(f"  Table importance: {len(table_scores)} tables scored")
    return _INDEX


def get_index() -> StructuralIndex:
    """Return the current index, or raise if not built yet."""
    if _INDEX is None:
        raise RuntimeError(
            "JIT index not built. Call build_index(corpus_path) first."
        )
    return _INDEX


# ---------------------------------------------------------------------------
# Router -- classify question and dispatch to the right query method
# ---------------------------------------------------------------------------

class _QueryResult:
    """Structured result from ask(). Renders as markdown in notebooks."""

    def __init__(self, question: str, query_type: str,
                 results: list[dict] | dict, match_term: str = ""):
        self.question = question
        self.query_type = query_type
        self.results = results
        self.match_term = match_term

    def _repr_markdown_(self) -> str:
        """Jupyter/notebook rich display."""
        return self.to_markdown()

    def __repr__(self) -> str:
        return self.to_markdown()

    def to_markdown(self) -> str:
        if self.query_type == "view_detail":
            return _format_view_detail(self.results)
        elif self.query_type == "table_lookup":
            return _format_table_lookup(self.match_term, self.results)
        elif self.query_type == "column_lookup":
            return _format_column_lookup(self.match_term, self.results)
        elif self.query_type == "filter_lookup":
            return _format_filter_lookup(self.match_term, self.results)
        elif self.query_type == "no_match":
            return (f"I couldn't find a specific table, column, or view name "
                    f"in your question: \"{self.question}\"\n\n"
                    f"Try asking about a specific table (e.g., REFERRAL), "
                    f"column (e.g., PAT_ID), or view name.")
        return str(self.results)


def _route_question(question: str, index: StructuralIndex) -> _QueryResult:
    """Classify a question and dispatch to the right query method.

    Phase 1 routing is keyword-based:
    1. If the question mentions a known view name -> describe_view
    2. If the question mentions a known table name -> find_by_table
    3. If the question mentions a known column name -> find_by_column
    4. If the question mentions filter-like terms -> find_by_filter
    5. Otherwise -> no_match (Phase 2 will add semantic search here)
    """
    q_upper = question.upper()
    # Extract words and multi-word tokens from the question
    tokens = set(re.findall(r"[A-Z][A-Z0-9_]+", q_upper))

    # 1. Check for view names (longest match first to avoid partial matches)
    for vname in sorted(index.all_view_names, key=len, reverse=True):
        if vname in q_upper:
            result = index.describe_view(vname)
            if result:
                return _QueryResult(question, "view_detail", result,
                                    match_term=vname)

    # 2. Check for table names (prefer longer matches, skip ZC unless explicit)
    best_table = None
    best_len = 0
    for token in tokens:
        if token in index.all_table_names and len(token) > best_len:
            best_table = token
            best_len = len(token)
    # Also check multi-word patterns like "REFERRAL table" or "the PAT_ENC"
    if not best_table:
        for tname in sorted(index.all_table_names, key=len, reverse=True):
            if tname in q_upper:
                best_table = tname
                break

    if best_table:
        results = index.find_by_table(best_table)
        return _QueryResult(question, "table_lookup", results,
                            match_term=best_table)

    # 3. Check for column names
    best_col = None
    best_col_len = 0
    for token in tokens:
        if token in index.column_to_views and len(token) > best_col_len:
            best_col = token
            best_col_len = len(token)
    if best_col:
        results = index.find_by_column(best_col)
        return _QueryResult(question, "column_lookup", results,
                            match_term=best_col)

    # 4. Check for filter-like business terms
    filter_keywords = ["denied", "active", "pending", "completed", "cancelled",
                       "approved", "expired", "valid", "invalid", "open",
                       "closed", "inpatient", "outpatient", "emergency"]
    for kw in filter_keywords:
        if kw.upper() in q_upper:
            results = index.find_by_filter(kw)
            if results:
                return _QueryResult(question, "filter_lookup", results,
                                    match_term=kw)

    return _QueryResult(question, "no_match", [])


# ---------------------------------------------------------------------------
# Answer formatters
# ---------------------------------------------------------------------------

def _format_view_detail(view: dict) -> str:
    """Format a describe_view result as markdown."""
    lines = [f"## {view['view_name']}", ""]

    if view.get("primary_purpose"):
        lines += [f"**Primary purpose:** {view['primary_purpose']}", ""]
    if view.get("business_description"):
        lines += [f"**Description:** {view['business_description']}", ""]

    # Tables with importance
    if view.get("tables"):
        lines.append("**Source tables:**")
        for t in view["tables"]:
            role_tag = f" ({t['role']})" if t.get("role") else ""
            imp = f" — importance {t['importance']:.0%}" if t.get("importance") else ""
            lines.append(f"  - `{t['name']}`{role_tag}{imp}")
        lines.append("")

    # Output columns (cap at 15)
    main_cols = [c for c in view.get("columns", []) if c.get("scope") == "main"]
    if not main_cols:
        main_cols = view.get("columns", [])
    if main_cols:
        lines.append(f"**Output columns ({len(main_cols)}):**")
        for c in main_cols[:15]:
            defn = f" — {c['definition']}" if c.get("definition") else ""
            lines.append(f"  - `{c['name']}`{defn}")
        if len(main_cols) > 15:
            lines.append(f"  - ... and {len(main_cols) - 15} more")
        lines.append("")

    # Filters
    if view.get("filters"):
        lines.append("**Filters:**")
        for f in view["filters"]:
            english = f.get("english", f.get("expression", ""))
            scope_tag = f" _{f['scope']}_" if f.get("scope") != "main" else ""
            lines.append(f"  - {english}{scope_tag}")
        lines.append("")

    # Key metrics
    if view.get("key_metrics"):
        lines.append(f"**Key metrics:** {', '.join(view['key_metrics'])}")
        lines.append("")

    return "\n".join(lines)


def _format_table_lookup(table_name: str, results: list[dict]) -> str:
    """Format find_by_table results as markdown."""
    if not results:
        return f"No views found that use table `{table_name}`."

    lines = [
        f"## Views using `{table_name}` ({len(results)} found)",
        "",
    ]
    for r in results:
        role_tag = f" [{r['table_role']}]" if r.get("table_role") else ""
        lines.append(f"### {r['view_name']}{role_tag}")
        if r.get("primary_purpose"):
            lines.append(f"**Purpose:** {r['primary_purpose']}")
        if r.get("business_description"):
            desc = r["business_description"]
            # Truncate long descriptions for the list view
            if len(desc) > 200:
                desc = desc[:200].rsplit(" ", 1)[0] + "..."
            lines.append(f"{desc}")
        if r.get("key_metrics"):
            lines.append(f"**Key metrics:** {', '.join(r['key_metrics'][:5])}")
        lines.append("")

    lines.append("---")
    lines.append(f"_Source: structural lookup on table `{table_name}` "
                 f"across {len(results)} views_")
    return "\n".join(lines)


def _format_column_lookup(column_name: str, results: list[dict]) -> str:
    """Format find_by_column results as markdown."""
    if not results:
        return f"No views found that produce column `{column_name}`."

    lines = [
        f"## Views producing `{column_name}` ({len(results)} found)",
        "",
    ]
    for r in results:
        lines.append(f"### {r['view_name']}")
        lines.append(f"**Definition in this view:** {r['column_definition']}")
        if r.get("scope") and r["scope"] != "main":
            lines.append(f"**Scope:** {r['scope']}")
        lines.append("")

    lines.append("---")
    lines.append(f"_Source: column lookup for `{column_name}` "
                 f"across {len(results)} views_")
    return "\n".join(lines)


def _format_filter_lookup(term: str, results: list[dict]) -> str:
    """Format find_by_filter results as markdown."""
    if not results:
        return f"No views found with filters matching \"{term}\"."

    lines = [
        f"## Views with \"{term}\" in filter logic ({len(results)} found)",
        "",
    ]
    for r in results:
        lines.append(f"### {r['view_name']}")
        if r.get("matching_filters"):
            lines.append("**Matching filters:**")
            for f in r["matching_filters"]:
                lines.append(f"  - {f}")
        if r.get("business_description"):
            desc = r["business_description"]
            if len(desc) > 200:
                desc = desc[:200].rsplit(" ", 1)[0] + "..."
            lines.append(f"{desc}")
        lines.append("")

    lines.append("---")
    lines.append(f"_Source: filter search for \"{term}\" "
                 f"across {len(results)} views_")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def ask(question: str) -> _QueryResult:
    """Ask a natural-language question about the ingested SQL corpus.

    The index must be built first via ``build_index(corpus_path)``.

    Parameters
    ----------
    question : natural-language question (e.g., "which views use REFERRAL?")

    Returns
    -------
    A ``_QueryResult`` that renders as markdown in Jupyter/notebook cells,
    with citations back to specific views, scopes, columns, and filters.

    Examples
    --------
    >>> ask("which views use the REFERRAL table?")
    ## Views using `REFERRAL` (12 found)
    ...

    >>> ask("what does VW_REFERRAL_STATUS do?")
    ## VW_REFERRAL_STATUS
    ...

    >>> ask("which views produce PAT_ID?")
    ## Views producing `PAT_ID` (45 found)
    ...
    """
    index = get_index()
    return _route_question(question, index)
