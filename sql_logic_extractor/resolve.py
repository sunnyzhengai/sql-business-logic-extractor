#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL Business Logic Extractor -- L3: Resolve Lineage

Traces every output column through CTEs, subqueries, and derived tables
all the way down to base table.column references.

Takes L1 extraction output and produces a fully resolved lineage
where every passthrough is inlined to show the complete transformation chain.

Pipeline: L1 (extract) -> L2 (normalize) -> L3 (resolve) -> L4 (translate) -> L5 (compare)
"""

import json
from dataclasses import dataclass, field
from typing import Optional

from .extract import SQLBusinessLogicExtractor, to_dict


# ---------------------------------------------------------------------------
# Resolved lineage structures
# ---------------------------------------------------------------------------

@dataclass
class ResolvedFilter:
    """A filter predicate with its nested subquery lineage (if any).

    When the filter contains an EXISTS/IN/scalar subquery, ``subqueries``
    carries the fully resolved inner query for each subquery expression
    that appears inside. This gives downstream graph consumers a direct
    Column -> Filter -> Subquery -> Tables/Columns edge.
    """
    expression: str
    subqueries: list["ResolvedQuery"] = field(default_factory=list)


@dataclass
class ResolvedColumn:
    """A fully resolved output column with its complete transformation chain."""
    name: str
    expression: str
    type: str                               # final type: calculated, aggregate, etc.
    base_columns: list[str] = field(default_factory=list)  # ultimate table.column refs
    base_tables: list[str] = field(default_factory=list)
    filters: list[ResolvedFilter] = field(default_factory=list)
    transformation_chain: list[dict] = field(default_factory=list)  # [{scope, name, expression, type}]
    resolved_expression: str = ""           # fully inlined expression


@dataclass
class ResolvedQuery:
    """A query with fully resolved lineage for every output column."""
    columns: list[ResolvedColumn] = field(default_factory=list)
    base_tables: list[str] = field(default_factory=list)  # union of extractor-level sources
    raw_sql: str = ""


# ---------------------------------------------------------------------------
# Scope-correct resolution (Phase D)
#
# These structures coexist with the flat ResolvedQuery / ResolvedColumn
# above. They DO NOT propagate filters across scope boundaries -- each
# scope owns the filters declared inside it and nothing else. Cross-scope
# dataflow is captured via scope-qualified `base_columns` and the
# `reads_from_*` edges on each scope.
#
# `kind` strings are sourced from the parser, not a closed enum. Common
# values: "main" | "cte" | "derived" | "subquery" | "exists" | "in" |
# "union:N" | "intersect:N" | "except:N" | "lateral". Consumers should
# accept any string; only specialize on names they recognize.
# ---------------------------------------------------------------------------

@dataclass
class ScopedFilter:
    """A predicate declared in one scope. `kind` distinguishes
    where/having/qualify/join_on/exists/in. `subquery_scope_ids` lists
    any subquery scopes referenced inside this predicate."""
    expression: str
    kind: str = "where"
    subquery_scope_ids: list[str] = field(default_factory=list)


@dataclass
class ScopedColumn:
    """One output column of one scope.

    `base_columns` are scope-qualified strings:
      - "table:Clarity.dbo.PATIENT.PAT_ID"  -- terminal base column
      - "cte:CTE1.PAT_ID"                    -- upstream CTE column
      - "derived:t0.x"                       -- derived-table column
    Consumers walk these as graph edges to reach base tables.

    `filters` are NOT inlined here -- they live on the owning ResolvedScope.
    """
    name: str
    expression: str
    type: str
    base_columns: list[str] = field(default_factory=list)
    base_tables: list[str] = field(default_factory=list)
    resolved_expression: str = ""
    transformation_chain: list[dict] = field(default_factory=list)


@dataclass
class ResolvedScope:
    """One structural scope (CTE, derived table, subquery, set-op branch,
    main SELECT, lateral). Carries only what is declared inside it."""
    id: str
    kind: str
    filters: list[ScopedFilter] = field(default_factory=list)
    columns: list[ScopedColumn] = field(default_factory=list)
    reads_from_scopes: list[str] = field(default_factory=list)
    reads_from_tables: list[str] = field(default_factory=list)
    raw_sql: str = ""


@dataclass
class ResolvedScopeTree:
    """Scope-correct decomposition of a query. The whole tree as a flat
    list of scopes plus an entry-point list (`view_outputs`) naming the
    scope(s) whose columns are the user-visible view output."""
    raw_sql: str = ""
    scopes: list[ResolvedScope] = field(default_factory=list)
    view_outputs: list[str] = field(default_factory=list)


def _dedupe_resolved_filters(filters: list) -> list:
    """Deduplicate ResolvedFilter objects by expression, keeping the first instance
    (which carries any nested subqueries found in that scope)."""
    seen: dict[str, "ResolvedFilter"] = {}
    for f in filters:
        if f.expression not in seen:
            seen[f.expression] = f
    return list(seen.values())


# ---------------------------------------------------------------------------
# Scope registry -- maps scope outputs to their definitions
# ---------------------------------------------------------------------------

class ScopeRegistry:
    """Builds a lookup of all named outputs across all scopes (CTEs, subqueries, main query).

    Each entry maps (scope_name, column_name) -> output definition dict from Layer 1.
    """

    def __init__(self):
        # {scope_name: {column_name: output_dict}}
        self._scopes: dict[str, dict[str, dict]] = {}
        # {scope_name: logic_dict}  (full Layer 1 extraction for that scope)
        self._scope_logic: dict[str, dict] = {}
        # Track which scope names are base tables vs CTEs/subqueries
        self._base_tables: set[str] = set()
        self._derived_scopes: set[str] = set()

    def register_query(self, logic: dict):
        """Register all scopes from a Layer 1 extraction."""
        # Register CTEs
        for cte in logic.get("ctes", []):
            name = cte.get("name", "")
            cte_logic = cte.get("logic")
            if name:
                # Always mark CTE names as derived scopes, even if extraction failed
                self._derived_scopes.add(name.lower())
                if cte_logic:
                    self._register_scope(name, cte_logic)
                    # Recursively register nested CTEs/subqueries
                    self._register_nested(cte_logic)

        # Register subqueries (derived tables in FROM)
        for sq in logic.get("subqueries", []):
            alias = sq.get("alias", "")
            sq_logic = sq.get("logic")
            if alias and sq_logic:
                self._register_scope(alias, sq_logic)
                self._derived_scopes.add(alias.lower())
                self._register_nested(sq_logic)

        # Register base tables from sources
        for src in logic.get("sources", []):
            if src.get("type") == "table":
                name = src.get("name", "")
                alias = src.get("alias", "")
                self._base_tables.add(name.lower())
                if alias:
                    self._base_tables.add(alias.lower())

        # Register the main query itself as "__main__"
        self._register_scope("__main__", logic)

    def _register_nested(self, logic: dict):
        """Recursively register nested CTEs and subqueries."""
        for cte in logic.get("ctes", []):
            name = cte.get("name", "")
            cte_logic = cte.get("logic")
            if name and cte_logic:
                self._register_scope(name, cte_logic)
                self._derived_scopes.add(name.lower())
                self._register_nested(cte_logic)

        for sq in logic.get("subqueries", []):
            alias = sq.get("alias", "")
            sq_logic = sq.get("logic")
            if alias and sq_logic:
                self._register_scope(alias, sq_logic)
                self._derived_scopes.add(alias.lower())
                self._register_nested(sq_logic)

        for src in logic.get("sources", []):
            if src.get("type") == "table":
                self._base_tables.add(src.get("name", "").lower())
                if src.get("alias"):
                    self._base_tables.add(src["alias"].lower())

    def _register_scope(self, scope_name: str, logic: dict):
        """Register a scope's outputs."""
        outputs = {}
        for out in logic.get("outputs", []):
            col_name = out.get("name", "")
            if col_name and col_name != "*":
                outputs[col_name.lower()] = out
        self._scopes[scope_name.lower()] = outputs
        self._scope_logic[scope_name.lower()] = logic

    def lookup(self, scope_name: str, column_name: str) -> Optional[dict]:
        """Look up an output definition in a scope."""
        scope = self._scopes.get(scope_name.lower(), {})
        return scope.get(column_name.lower())

    def get_filters(self, scope_name: str) -> list[dict]:
        """Get business-relevant filters for a scope as intermediate dicts.

        Each returned entry is ``{"expression": str, "subquery_logics": list[dict]}``
        where ``subquery_logics`` holds the raw L1 logic dicts for any EXISTS/IN/
        scalar subqueries whose expression text appears inside this filter.
        ``LineageResolver`` converts these into fully resolved ``ResolvedFilter``
        objects with nested ``ResolvedQuery`` children.
        """
        logic = self._scope_logic.get(scope_name.lower(), {})
        subqueries = logic.get("subqueries", []) or []
        out = []
        for f in logic.get("filters", []):
            scope = f.get("scope", "")
            expr = f.get("expression", "")
            if scope in ("where", "having", "qualify"):
                pass
            elif scope == "join":
                # Include non-equi join conditions (they carry business logic)
                # e.g., "d2.HOSP_DISCH_TIME > d1.HOSP_DISCH_TIME" is business logic
                # but "e.PAT_ID = p.PAT_ID" is just a key relationship
                expr_upper = expr.upper()
                if " = " in expr and not any(op in expr_upper for op in
                    [" > ", " < ", " >= ", " <= ", " <> ", " != ",
                     "BETWEEN", "LIKE", "DATEDIFF", "AND "]):
                    continue
            else:
                continue
            matched = []
            for sq in subqueries:
                if sq.get("context") not in ("exists", "in", "where"):
                    continue
                sq_expr = sq.get("expression", "")
                if sq_expr and sq_expr in expr:
                    matched.append(sq)
            out.append({"expression": expr, "subquery_logics": matched})
        return out

    def is_base_table(self, name: str) -> bool:
        """Check if a name refers to a base table (not a CTE/subquery)."""
        return name.lower() in self._base_tables and name.lower() not in self._derived_scopes

    def is_derived(self, name: str) -> bool:
        """Check if a name refers to a CTE or subquery."""
        return name.lower() in self._derived_scopes

    def get_alias_to_table(self, scope_name: str) -> dict[str, str]:
        """Get alias->table mapping for a scope."""
        logic = self._scope_logic.get(scope_name.lower(), {})
        mapping = {}
        for src in logic.get("sources", []):
            alias = src.get("alias", "")
            name = src.get("name", "")
            if alias and alias != name:
                mapping[alias.lower()] = name
            mapping[name.lower()] = name
        for join in logic.get("joins", []):
            ra = join.get("right_alias", "")
            rt = join.get("right_table", "")
            if ra and ra != rt:
                mapping[ra.lower()] = rt
        for cte in logic.get("ctes", []):
            cte_name = cte.get("name", "")
            mapping[cte_name.lower()] = cte_name
        for sq in logic.get("subqueries", []):
            sq_alias = sq.get("alias", "")
            if sq_alias:
                mapping[sq_alias.lower()] = sq_alias
        return mapping


# ---------------------------------------------------------------------------
# Lineage resolver
# ---------------------------------------------------------------------------

class LineageResolver:
    """Resolves output columns through all scopes to base table.column references."""

    def __init__(self, logic: dict):
        self.logic = logic
        self.registry = ScopeRegistry()
        self.registry.register_query(logic)
        self._resolve_cache: dict[tuple, ResolvedColumn] = {}
        # Per-scope structural dependencies (base tables/columns this scope's
        # rows rely on, independent of any particular output column). Populated
        # lazily by _scope_contribs and consumed by _apply_scope_contribs.
        self._scope_contribs_cache: dict[str, tuple[list[str], list[str]]] = {}

    def _scope_contribs(self, scope: str) -> tuple[list[str], list[str]]:
        """Base (tables, columns) this scope's rows structurally depend on.

        Row-existence model: a scope constrains its rows via its FROM driver,
        INNER joins, WHERE/HAVING/QUALIFY predicates, and EXISTS/IN subqueries.
        LEFT/RIGHT/FULL join right-sides are NOT mandatory for left-side rows,
        so they're excluded here -- they only contribute when a column's own
        passthrough recursion actually traverses that side.
        """
        key = scope.lower()
        if key in self._scope_contribs_cache:
            return self._scope_contribs_cache[key]
        # Placeholder prevents infinite recursion on cyclic scope graphs.
        self._scope_contribs_cache[key] = ([], [])

        logic = self.registry._scope_logic.get(key, {}) or {}
        alias_map = self.registry.get_alias_to_table(scope)
        tables: list[str] = []
        cols: list[str] = []

        joins = logic.get("joins", []) or []

        # Aliases/names that are JOIN right-sides -- used to tell drivers apart
        # from join participants when walking sources[].
        join_right_ids: set[str] = set()
        for j in joins:
            ra = (j.get("right_alias") or "").lower()
            rt = (j.get("right_table") or "").lower()
            if ra:
                join_right_ids.add(ra)
            if rt:
                join_right_ids.add(rt)

        # (1) Driver sources (FROM clause, not a join right-side).
        for src in logic.get("sources", []) or []:
            name = src.get("name") or ""
            alias = src.get("alias") or name
            stype = src.get("type") or ""
            if alias.lower() in join_right_ids or name.lower() in join_right_ids:
                continue
            if stype == "table":
                if name and self.registry.is_derived(name):
                    inner_t, inner_c = self._scope_contribs(name)
                    tables.extend(inner_t)
                    cols.extend(inner_c)
                elif name:
                    tables.append(name)
            elif stype == "subquery":
                a = src.get("alias") or ""
                if a and self.registry.is_derived(a):
                    inner_t, inner_c = self._scope_contribs(a)
                    tables.extend(inner_t)
                    cols.extend(inner_c)

        # (2) INNER/CROSS joins: right side contribs + join keys.
        # Skip LEFT / RIGHT / FULL -- they don't constrain non-right-side rows.
        for join in joins:
            jt = (join.get("join_type") or "").upper()
            if "LEFT" in jt or "RIGHT" in jt or "FULL" in jt:
                continue
            rt = join.get("right_table") or ""
            if rt:
                if self.registry.is_derived(rt):
                    inner_t, inner_c = self._scope_contribs(rt)
                    tables.extend(inner_t)
                    cols.extend(inner_c)
                elif rt.lower() in self.registry._base_tables:
                    tables.append(rt)
            for col in join.get("columns", []) or []:
                t, c = self._resolve_ref(col, scope, alias_map)
                tables.extend(t)
                cols.extend(c)

        # (3) WHERE/HAVING/QUALIFY filter columns (always constrain the scope's rows).
        for f in logic.get("filters", []) or []:
            if f.get("scope") not in ("where", "having", "qualify"):
                continue
            for col in f.get("columns", []) or []:
                t, c = self._resolve_ref(col, scope, alias_map)
                tables.extend(t)
                cols.extend(c)

        # (4) EXISTS/IN/WHERE subqueries -- union the resolved inner lineage.
        for sq in logic.get("subqueries", []) or []:
            if sq.get("context") not in ("exists", "in", "where"):
                continue
            inner_logic = sq.get("logic")
            if not inner_logic:
                continue
            try:
                inner = LineageResolver(inner_logic).resolve_all()
            except Exception:
                continue
            tables.extend(inner.base_tables)
            for c in inner.columns:
                tables.extend(c.base_tables)
                cols.extend(c.base_columns)

        tables = list(dict.fromkeys(tables))
        cols = list(dict.fromkeys(cols))
        self._scope_contribs_cache[key] = (tables, cols)
        return tables, cols

    def _resolve_ref(self, ref: dict, scope: str, alias_map: dict) -> tuple[list[str], list[str]]:
        """Resolve one column ref ``{table, column}`` to base (tables, columns).

        If the alias refers to a CTE/derived scope, walks the inner output when
        the column exists there; otherwise falls back to that scope's contribs
        (handles join keys whose inner output isn't directly lookup-able)."""
        src_table = (ref.get("table") or "").strip()
        src_col = (ref.get("column") or "").strip()
        if not src_col:
            return [], []
        real_table = alias_map.get(src_table.lower(), src_table) if src_table else ""
        if not real_table:
            real_table = self._find_source_for_column(src_col, scope, alias_map)
        if not real_table:
            return [], []

        if self.registry.is_derived(real_table):
            inner_out = self.registry.lookup(real_table, src_col)
            if inner_out:
                inner = self._resolve_output(inner_out, real_table, [])
                return list(inner.base_tables), list(inner.base_columns)
            return self._scope_contribs(real_table)

        # Only emit if this is a known base table. Anything else is an
        # unresolved alias -- usually a correlated reference to an outer
        # scope that this resolver doesn't see -- and would leak as noise.
        if real_table.lower() in self.registry._base_tables:
            return [real_table], [f"{real_table}.{src_col}"]
        return [], []

    def _apply_scope_contribs(self, col: ResolvedColumn, scope: str) -> ResolvedColumn:
        """Union the scope's structural dependencies into a resolved column."""
        t, c = self._scope_contribs(scope)
        if t:
            col.base_tables = list(dict.fromkeys(list(col.base_tables) + t))
        if c:
            col.base_columns = list(dict.fromkeys(list(col.base_columns) + c))
        return col

    def _get_scope_filters(self, scope: str) -> list[ResolvedFilter]:
        """Fetch filters for a scope and hydrate any matched subqueries into
        fully resolved ``ResolvedQuery`` children."""
        resolved: list[ResolvedFilter] = []
        for raw in self.registry.get_filters(scope):
            rf = ResolvedFilter(expression=raw["expression"])
            for sq in raw.get("subquery_logics", []) or []:
                inner_logic = sq.get("logic")
                if not inner_logic:
                    continue
                try:
                    rf.subqueries.append(LineageResolver(inner_logic).resolve_all())
                except Exception:
                    pass
            resolved.append(rf)
        return resolved

    # ----------------------------------------------------------------
    # Scope-correct resolution (Phase D, additive)
    # ----------------------------------------------------------------

    def resolve_all_scoped(self) -> ResolvedScopeTree:
        """Walk the scope graph and emit one ResolvedScope per structural
        unit. Filters DO NOT propagate across scope boundaries; cross-scope
        dataflow is preserved through scope-qualified base_columns and
        reads_from_* edges. Coexists with `resolve_all()`."""
        tree = ResolvedScopeTree(raw_sql=self.logic.get("raw_sql", ""))
        emitted: dict[str, ResolvedScope] = {}

        def emit(scope: ResolvedScope) -> None:
            if scope.id not in emitted:
                emitted[scope.id] = scope
                tree.scopes.append(scope)

        # CTE names are visible to sibling and descendant scopes. Pre-walk
        # the whole logic tree once so a deeply nested CTE-of-CTE knows
        # which names are CTE references (not base tables).
        known_ctes: dict[str, str] = {}
        self._collect_cte_names(self.logic, known_ctes)

        # Set-op view (top-level UNION / INTERSECT / EXCEPT): each branch
        # is its own scope; the "view output" is the first branch's columns
        # (positionally aligned, by SQL semantics).
        set_ops = self.logic.get("set_operations") or []
        if set_ops:
            op = set_ops[0]
            op_kind = (op.get("type") or "UNION").lower().replace(" ", "_")
            for i, branch_logic in enumerate(op.get("branches") or []):
                branch_id = f"{op_kind}:{i}"
                self._emit_scope_recursive(branch_logic, branch_id, op_kind, emit, known_ctes)
            tree.view_outputs = [f"{op_kind}:0"] if op.get("branches") else []
            return tree

        # Plain view (main SELECT, possibly with CTEs/subqueries).
        self._emit_scope_recursive(self.logic, "main", "main", emit, known_ctes)
        tree.view_outputs = ["main"]
        return tree

    def _collect_cte_names(self, logic: dict, out: dict[str, str]) -> None:
        """Walk a logic tree and accumulate {cte_name_lower: scope_id}.
        CTEs declared at any level are visible to their siblings and
        descendants, so the map is global to the whole resolution."""
        if not isinstance(logic, dict):
            return
        for cte in logic.get("ctes", []) or []:
            name = cte.get("name") or ""
            if name:
                out[name.lower()] = f"cte:{name}"
            sub = cte.get("logic")
            if sub:
                self._collect_cte_names(sub, out)
        for sq in logic.get("subqueries", []) or []:
            sub = sq.get("logic")
            if sub:
                self._collect_cte_names(sub, out)
        for op in logic.get("set_operations", []) or []:
            for branch in op.get("branches", []) or []:
                self._collect_cte_names(branch, out)

    def _emit_scope_recursive(
        self,
        logic: dict,
        scope_id: str,
        kind: str,
        emit,
        known_ctes: dict[str, str],
    ) -> None:
        """Build a ResolvedScope from a logic dict and emit it, then
        recursively emit its CTEs / derived sources / subqueries."""
        if not isinstance(logic, dict):
            return
        scope = ResolvedScope(
            id=scope_id,
            kind=kind,
            raw_sql=logic.get("raw_sql", ""),
        )

        # --- Filters declared in THIS scope only (no propagation) ---
        for f in logic.get("filters", []) or []:
            f_kind = f.get("scope") or "where"
            if f_kind == "join":
                # Drop pure equi-join keys; keep business-bearing join predicates.
                expr_upper = (f.get("expression") or "").upper()
                if " = " in (f.get("expression") or "") and not any(op in expr_upper for op in
                    [" > ", " < ", " >= ", " <= ", " <> ", " != ",
                     "BETWEEN", "LIKE", "DATEDIFF", "AND "]):
                    continue
                f_kind = "join_on"
            scope.filters.append(ScopedFilter(
                expression=f.get("expression") or "",
                kind=f_kind,
                # Subquery linkage filled in below once subquery scopes have IDs.
                subquery_scope_ids=[],
            ))

        # --- reads_from_tables / reads_from_scopes ---
        # CTE references resolve to globally-known CTE IDs (CTEs declared
        # anywhere in the query are visible to descendants). Derived-table
        # aliases resolve to the local scope's subqueries.
        cte_name_to_id = known_ctes
        derived_alias_to_id: dict[str, str] = {}
        for sq in logic.get("subqueries", []) or []:
            alias = sq.get("alias")
            if alias and (sq.get("context") in ("from", None) or False):
                derived_alias_to_id[alias.lower()] = f"derived:{alias}"

        lateral_count = 0
        for src in logic.get("sources", []) or []:
            stype = src.get("type") or ""
            sname = src.get("name") or ""
            salias = src.get("alias") or ""
            if stype == "table":
                if sname.lower() in cte_name_to_id:
                    scope.reads_from_scopes.append(cte_name_to_id[sname.lower()])
                else:
                    scope.reads_from_tables.append(sname)
            elif stype == "subquery":
                if salias:
                    scope.reads_from_scopes.append(f"derived:{salias}")
                    derived_alias_to_id[salias.lower()] = f"derived:{salias}"
            elif stype == "lateral":
                lat_id = f"lateral:{salias}" if salias else f"lateral:{lateral_count}"
                lateral_count += 1
                scope.reads_from_scopes.append(lat_id)

        for join in logic.get("joins", []) or []:
            rt = join.get("right_table") or ""
            ra = join.get("right_alias") or ""
            if rt:
                if rt.lower() in cte_name_to_id:
                    scope.reads_from_scopes.append(cte_name_to_id[rt.lower()])
                elif ra and ra.lower() in derived_alias_to_id:
                    scope.reads_from_scopes.append(derived_alias_to_id[ra.lower()])
                else:
                    scope.reads_from_tables.append(rt)

        # Dedupe edges, preserve order.
        scope.reads_from_tables = list(dict.fromkeys(scope.reads_from_tables))
        scope.reads_from_scopes = list(dict.fromkeys(scope.reads_from_scopes))

        # --- Columns local to this scope (no transitive resolution) ---
        alias_map = self._build_alias_map(logic, cte_name_to_id, derived_alias_to_id)
        for out in logic.get("outputs", []) or []:
            col = self._resolve_column_local(out, alias_map, cte_name_to_id, derived_alias_to_id)
            if col is not None:
                scope.columns.append(col)

        emit(scope)

        # --- Recurse into nested scopes ---
        for cte in logic.get("ctes", []) or []:
            cte_name = cte.get("name") or ""
            cte_logic = cte.get("logic")
            if cte_name and cte_logic:
                self._emit_scope_recursive(cte_logic, f"cte:{cte_name}", "cte", emit, known_ctes)

        # Subqueries: WHERE-EXISTS, IN, scalar, derived. Synthesize IDs.
        ctx_counters: dict[str, int] = {}
        for sq in logic.get("subqueries", []) or []:
            ctx = sq.get("context") or "subquery"
            sub_logic = sq.get("logic")
            alias = sq.get("alias")
            if alias and ctx in ("from", None):
                sub_id = f"derived:{alias}"
                sub_kind = "derived"
            else:
                idx = ctx_counters.get(ctx, 0)
                ctx_counters[ctx] = idx + 1
                sub_id = f"{ctx}:{idx}"
                sub_kind = ctx
            if sub_logic:
                self._emit_scope_recursive(sub_logic, sub_id, sub_kind, emit, known_ctes)

    def _build_alias_map(
        self,
        logic: dict,
        cte_name_to_id: dict[str, str],
        derived_alias_to_id: dict[str, str],
    ) -> dict[str, str]:
        """Map source aliases to scope-qualified prefixes used in
        scope-local base_columns. Each entry is {alias_lower: prefix}
        where prefix is "table:<name>" or "cte:<name>" or "derived:<alias>".
        """
        out: dict[str, str] = {}
        for src in logic.get("sources", []) or []:
            sname = src.get("name") or ""
            salias = src.get("alias") or sname
            stype = src.get("type") or ""
            if stype == "table":
                key = cte_name_to_id.get(sname.lower())
                prefix = key if key else f"table:{sname}"
            elif stype == "subquery":
                prefix = f"derived:{salias}" if salias else "derived:?"
            elif stype == "lateral":
                prefix = f"lateral:{salias}" if salias else "lateral:?"
            else:
                prefix = f"table:{sname}"
            if salias:
                out[salias.lower()] = prefix
            if sname:
                out[sname.lower()] = prefix
        for join in logic.get("joins", []) or []:
            rt = join.get("right_table") or ""
            ra = join.get("right_alias") or rt
            if rt.lower() in cte_name_to_id:
                prefix = cte_name_to_id[rt.lower()]
            elif ra.lower() in derived_alias_to_id:
                prefix = derived_alias_to_id[ra.lower()]
            else:
                prefix = f"table:{rt}"
            if ra:
                out[ra.lower()] = prefix
            if rt:
                out[rt.lower()] = prefix
        for cte_name, cte_id in cte_name_to_id.items():
            out.setdefault(cte_name, cte_id)
        for alias, der_id in derived_alias_to_id.items():
            out.setdefault(alias, der_id)
        return out

    def _resolve_column_local(
        self,
        out: dict,
        alias_map: dict[str, str],
        cte_name_to_id: dict[str, str],
        derived_alias_to_id: dict[str, str],
    ) -> Optional[ScopedColumn]:
        """Build a ScopedColumn with scope-qualified base_columns. Does
        NOT recurse into upstream scopes -- cross-scope dataflow is
        captured by the scope-qualified prefix, which a graph walker
        can follow."""
        name = out.get("name") or ""
        if name == "*":
            return ScopedColumn(name="*", expression="*", type="star")
        expr = out.get("expression") or ""
        col_type = out.get("type") or ""
        base_cols: list[str] = []
        base_tables: list[str] = []
        for src in out.get("source_columns", []) or []:
            t = (src.get("table") or "").strip()
            c = (src.get("column") or "").strip()
            if not c:
                continue
            prefix = alias_map.get(t.lower()) if t else None
            if not prefix:
                prefix = f"table:{t}" if t else "table:?"
            base_cols.append(f"{prefix}.{c}")
            if prefix.startswith("table:"):
                base_tables.append(prefix[len("table:"):])
        return ScopedColumn(
            name=name,
            expression=expr,
            type=col_type,
            base_columns=list(dict.fromkeys(base_cols)),
            base_tables=list(dict.fromkeys(base_tables)),
            resolved_expression=expr,
        )

    def resolve_all(self) -> ResolvedQuery:
        """Resolve every output column in the main query."""
        result = ResolvedQuery(raw_sql=self.logic.get("raw_sql", ""))

        # Query-level base tables: union of extractor ``sources`` for this scope.
        # Catches tables referenced by EXISTS-style subqueries that select a
        # literal and therefore have no column lineage pointing back.
        seen_tables: list[str] = []
        for src in self.logic.get("sources", []) or []:
            name = src.get("name") or ""
            if name and name not in seen_tables and src.get("type") == "table":
                seen_tables.append(name)
        result.base_tables = seen_tables

        for out in self.logic.get("outputs", []):
            name = out.get("name", "")
            if name == "*":
                # Expand * by finding the source scope(s) and resolving their columns
                expanded = self._expand_star("__main__")
                if expanded:
                    result.columns.extend(expanded)
                else:
                    result.columns.append(ResolvedColumn(
                        name="*", expression="*", type="star",
                    ))
                continue

            resolved = self._resolve_output(out, "__main__", [])
            result.columns.append(resolved)

        return result

    def _expand_star(self, scope: str) -> list[ResolvedColumn]:
        """Expand SELECT * by resolving all columns from the direct source scope(s).

        Only expands from derived scopes (CTEs/subqueries) that are direct
        sources of this scope. Avoids duplicating columns from nested CTEs.
        """
        logic = self.registry._scope_logic.get(scope.lower(), {})
        expanded = []
        seen_names = set()

        # Collect direct source refs -- only from sources and joins of THIS scope
        source_refs = []
        for src in logic.get("sources", []):
            ref = src.get("alias") or src.get("name", "")
            if ref and self.registry.is_derived(ref):
                source_refs.append(ref)
        for join in logic.get("joins", []):
            ref = join.get("right_alias") or join.get("right_table", "")
            if ref and self.registry.is_derived(ref):
                source_refs.append(ref)

        # If no derived sources found, try alias mapping
        if not source_refs:
            alias_map = self.registry.get_alias_to_table(scope)
            for src in logic.get("sources", []):
                ref = src.get("alias") or src.get("name", "")
                real_name = alias_map.get(ref.lower(), ref)
                if self.registry.is_derived(real_name):
                    source_refs.append(real_name)

        for ref in source_refs:
            scope_outputs = self.registry._scopes.get(ref.lower(), {})
            for col_name, out_def in scope_outputs.items():
                # Deduplicate by column name (first source wins)
                if col_name.lower() in seen_names:
                    continue
                seen_names.add(col_name.lower())
                resolved = self._resolve_output(out_def, ref, [])
                resolved.name = out_def.get("name", col_name)
                expanded.append(resolved)

        return expanded

    def _resolve_output(self, out: dict, scope: str, visited: list) -> ResolvedColumn:
        """Resolve a single output column, following references through scopes."""
        name = out.get("name", "")
        expr = out.get("expression", "")
        col_type = out.get("type", "")
        source_cols = out.get("source_columns", [])

        # Get alias mapping for this scope
        alias_map = self.registry.get_alias_to_table(scope)

        # Collect filters from this scope (with any nested subquery lineage)
        scope_filters = self._get_scope_filters(scope)

        # Build the chain entry for this level
        chain_entry = {
            "scope": scope if scope != "__main__" else "query",
            "name": name,
            "expression": expr,
            "type": col_type,
        }

        if col_type == "passthrough":
            # This column comes from a source -- trace it
            if source_cols:
                src = source_cols[0]
                src_table = src.get("table") or ""
                src_col = src.get("column") or ""

                # Resolve alias to real name
                real_table = alias_map.get(src_table.lower(), src_table) if src_table else ""

                # If no table qualifier, try to find which source has this column
                if not real_table:
                    real_table = self._find_source_for_column(src_col, scope, alias_map)

                # Check for circular reference
                cache_key = (real_table.lower(), src_col.lower())
                if cache_key in visited:
                    return self._apply_scope_contribs(ResolvedColumn(
                        name=name, expression=expr, type="passthrough",
                        base_columns=[f"{real_table}.{src_col}"],
                        base_tables=[real_table],
                        transformation_chain=[chain_entry],
                    ), scope)

                # Is this from a derived scope (CTE/subquery)?
                if self.registry.is_derived(real_table):
                    inner_out = self.registry.lookup(real_table, src_col)
                    if inner_out:
                        inner_resolved = self._resolve_output(
                            inner_out, real_table,
                            visited + [cache_key],
                        )
                        # Prepend our chain entry and merge filters (dedupe by expression)
                        merged_filters = _dedupe_resolved_filters(scope_filters + inner_resolved.filters)
                        return self._apply_scope_contribs(ResolvedColumn(
                            name=name,
                            expression=inner_resolved.resolved_expression or inner_resolved.expression,
                            type=inner_resolved.type,
                            base_columns=inner_resolved.base_columns,
                            base_tables=inner_resolved.base_tables,
                            filters=merged_filters,
                            transformation_chain=[chain_entry] + inner_resolved.transformation_chain,
                            resolved_expression=inner_resolved.resolved_expression,
                        ), scope)

                # Base table -- terminal
                qualified = f"{real_table}.{src_col}"
                return self._apply_scope_contribs(ResolvedColumn(
                    name=name, expression=expr, type="passthrough",
                    base_columns=[qualified],
                    base_tables=[real_table],
                    filters=scope_filters,
                    transformation_chain=[chain_entry],
                    resolved_expression=qualified,
                ), scope)

            # No source columns -- just return as-is
            return self._apply_scope_contribs(ResolvedColumn(
                name=name, expression=expr, type=col_type,
                filters=scope_filters,
                transformation_chain=[chain_entry],
            ), scope)

        # Non-passthrough (calculated, aggregate, case, window, etc.)
        # Resolve each source column reference
        all_base_cols = []
        all_base_tables = []
        all_filters = list(scope_filters)
        sub_chains = []

        for src in source_cols:
            src_table = src.get("table") or ""
            src_col = src.get("column") or ""
            real_table = alias_map.get(src_table.lower(), src_table) if src_table else ""
            if not real_table:
                real_table = self._find_source_for_column(src_col, scope, alias_map)
            qualified = f"{real_table}.{src_col}"

            cache_key = (real_table.lower(), src_col.lower())
            if cache_key in visited:
                all_base_cols.append(qualified)
                if real_table:
                    all_base_tables.append(real_table)
                continue

            if self.registry.is_derived(real_table):
                inner_out = self.registry.lookup(real_table, src_col)
                if inner_out:
                    inner_resolved = self._resolve_output(
                        inner_out, real_table,
                        visited + [cache_key],
                    )
                    all_base_cols.extend(inner_resolved.base_columns)
                    all_base_tables.extend(inner_resolved.base_tables)
                    all_filters.extend(inner_resolved.filters)
                    sub_chains.extend(inner_resolved.transformation_chain)
                else:
                    all_base_cols.append(qualified)
                    if real_table:
                        all_base_tables.append(real_table)
            else:
                all_base_cols.append(qualified)
                if real_table:
                    all_base_tables.append(real_table)

        # Deduplicate
        base_cols = list(dict.fromkeys(all_base_cols))
        base_tables = list(dict.fromkeys(all_base_tables))
        filters = _dedupe_resolved_filters(all_filters)

        # Build resolved expression by inlining
        resolved_expr = self._inline_expression(expr, scope, alias_map, visited)

        return self._apply_scope_contribs(ResolvedColumn(
            name=name,
            expression=expr,
            type=col_type,
            base_columns=base_cols,
            base_tables=base_tables,
            filters=filters,
            transformation_chain=[chain_entry] + sub_chains,
            resolved_expression=resolved_expr,
        ), scope)

    def _find_source_for_column(self, col_name: str, scope: str, alias_map: dict) -> str:
        """When a column has no table qualifier, find which source scope defines it."""
        # Check each source in the scope's logic
        logic = self.registry._scope_logic.get(scope.lower(), {})
        for src in logic.get("sources", []):
            src_name = src.get("name", "")
            src_alias = src.get("alias", "")
            # Use alias if available, otherwise name
            scope_ref = src_alias or src_name

            # Check if this source (as a derived scope) has this column
            if self.registry.is_derived(scope_ref):
                if self.registry.lookup(scope_ref, col_name):
                    return scope_ref
            elif self.registry.is_derived(src_name):
                if self.registry.lookup(src_name, col_name):
                    return src_name

        # Fallback: check derived scopes that are sources of this scope
        for src in logic.get("sources", []):
            src_name = src.get("alias") or src.get("name", "")
            real_name = alias_map.get(src_name.lower(), src_name)
            if self.registry.is_base_table(real_name) and not self.registry.is_derived(real_name):
                # It's a base table -- return it
                return real_name

        return ""

    def _inline_expression(self, expr: str, scope: str, alias_map: dict, visited: list) -> str:
        """Try to inline CTE/subquery column references in an expression.

        For example, if expr is "CASE WHEN los_days > 7 ..." and los_days comes from
        a CTE where it's DATEDIFF(...), replace los_days with the DATEDIFF expression.
        """
        try:
            import sqlglot
            from sqlglot import exp as sqlexp

            parsed = sqlglot.parse_one(expr)

            # Find all column references
            for col_node in parsed.find_all(sqlexp.Column):
                col_name = col_node.name
                col_table = col_node.table or ""
                real_table = alias_map.get(col_table.lower(), col_table) if col_table else ""

                # Check if this references a derived scope
                target_scope = real_table if real_table and self.registry.is_derived(real_table) else ""
                if not target_scope:
                    # Try without table qualifier -- check all derived scopes
                    for dscope in self.registry._derived_scopes:
                        if self.registry.lookup(dscope, col_name):
                            target_scope = dscope
                            break

                if target_scope:
                    inner_out = self.registry.lookup(target_scope, col_name)
                    if inner_out and inner_out.get("type") != "passthrough":
                        # Get the inner expression (strip alias)
                        inner_expr = inner_out.get("expression", "")
                        try:
                            inner_parsed = sqlglot.parse_one(inner_expr)
                            if isinstance(inner_parsed, sqlexp.Alias):
                                inner_parsed = inner_parsed.this
                            # Replace the column reference with the inner expression
                            col_node.replace(inner_parsed)
                        except Exception:
                            pass

            result = parsed.sql(pretty=False)
            # Strip alias if present
            try:
                reparsed = sqlglot.parse_one(result)
                if isinstance(reparsed, sqlexp.Alias):
                    result = reparsed.this.sql(pretty=False)
            except Exception:
                pass
            return result

        except Exception:
            return expr


# ---------------------------------------------------------------------------
# SSMS script preprocessor
# ---------------------------------------------------------------------------

import re


def _normalize_meta_key(raw_key: str) -> str:
    """Normalize header comment keys to consistent names."""
    key = re.sub(r"\s+", "_", raw_key.strip()).lower()
    aliases = {
        "created_by": "author",
        "modified_by": "modified_by",
        "updated_by": "modified_by",
        "revised_by": "modified_by",
        "desc": "description",
        "purpose": "description",
        "summary": "description",
        "notes": "notes",
        "report_name": "report_name",
        "report": "report_name",
        "view_name": "object_name",
        "proc_name": "object_name",
        "procedure_name": "object_name",
        "object_name": "object_name",
        "revision": "version",
        "rev": "version",
        "version": "version",
        "date": "date",
        "created_date": "created_date",
        "modified_date": "modified_date",
        "updated_date": "modified_date",
        "change_date": "modified_date",
        "ticket": "ticket",
        "jira": "ticket",
        "story": "ticket",
        "task": "ticket",
        "issue": "ticket",
        "cr": "ticket",
        "change_request": "ticket",
        "department": "department",
        "team": "team",
        "project": "project",
        "parameters": "parameters",
        "params": "parameters",
        "returns": "returns",
        "output": "returns",
        "dependencies": "dependencies",
        "depends_on": "dependencies",
        "history": "history",
        "change_log": "history",
        "changelog": "history",
    }
    return aliases.get(key, key)


def _add_meta(metadata: dict, key: str, value: str):
    """Add a metadata value, appending to list if key already exists."""
    if key in metadata and key in ("history", "notes"):
        # Append to list for multi-line fields
        if isinstance(metadata[key], list):
            metadata[key].append(value)
        else:
            metadata[key] = [metadata[key], value]
    else:
        metadata[key] = value


def _parse_header_comments(lines: list[str], metadata: dict):
    """Parse collected header comment lines for metadata key-value pairs and revision history."""
    _META_PATTERN = re.compile(
        r"^[*\s]*"
        r"(Author|Created\s*By|Modified\s*By|Updated\s*By|Revised\s*By"
        r"|Description|Desc|Purpose|Summary|Notes"
        r"|Report\s*Name|Report|View\s*Name|Proc(?:edure)?\s*Name|Object\s*Name"
        r"|Revision|Version|Rev"
        r"|Date|Created\s*Date|Modified\s*Date|Updated\s*Date|Change\s*Date"
        r"|Ticket|Jira|Story|Task|Issue|CR|Change\s*Request"
        r"|Department|Team|Project"
        r"|Parameters|Params"
        r"|Returns|Output"
        r"|Dependencies|Depends\s*On"
        r"|History|Change\s*Log|Changelog)"
        r"\s*[:=]\s*(.*)",
        re.IGNORECASE,
    )

    _REVISION_LINE = re.compile(
        r"^[*\s]*"
        r"(\d{1,4}[-/]\d{1,2}[-/]\d{1,4})"
        r"\s+"
        r"(\S+(?:\s+\S+)?)"
        r"\s{2,}"
        r"(.+)",
    )

    in_history = False
    revisions = []

    for line in lines:
        clean = line.lstrip("* ").strip()
        if not clean:
            continue

        meta_match = _META_PATTERN.match(clean)
        if meta_match:
            key = _normalize_meta_key(meta_match.group(1))
            val = meta_match.group(2).strip()
            if key == "history":
                in_history = True
                if val:
                    revisions.append(val)
            else:
                in_history = False
                if val:
                    _add_meta(metadata, key, val)
            continue

        # Inside a history/changelog section, look for revision lines
        if in_history:
            rev_match = _REVISION_LINE.match(clean)
            if rev_match:
                revisions.append({
                    "date": rev_match.group(1).strip(),
                    "author": rev_match.group(2).strip(),
                    "description": rev_match.group(3).strip(),
                })
            elif clean:
                revisions.append(clean)
            continue

    if revisions:
        metadata["revisions"] = revisions


def preprocess_ssms(sql: str) -> tuple[str, dict]:
    """Strip SSMS boilerplate from scripted views/procedures/functions.

    Returns (clean_sql, metadata) where metadata contains the object name,
    schema, type, script date, and any header comment metadata (author,
    description, report name, revision history, etc.).
    """
    metadata = {}

    # Pre-pass: apply the rule registry. Each rule is one T-SQL construct
    # sqlglot can't parse natively, declared in parsing_rules/rules.py
    # with a matching fixture. New rules go in the registry, never inline
    # here -- see parsing_rules/__init__.py for the contract.
    from .parsing_rules import apply_all
    sql, _fired_rules = apply_all(sql)

    lines = sql.split("\n")
    clean_lines = []
    body_started = False
    in_header_comment = False
    header_comment_lines = []

    # Known metadata keys in header comments (case-insensitive matching)
    # Matches patterns like:  Author: John Smith
    #                         Description: This view calculates ...
    #                         Report Name: Monthly Revenue
    #                         Revision: 2.1
    #                         Modified By: Jane Doe
    #                         Modified Date: 2024-01-15
    _META_PATTERN = re.compile(
        r"^[-*\s]*"  # leading dashes, stars, whitespace
        r"(Author|Created\s*By|Modified\s*By|Updated\s*By|Revised\s*By"
        r"|Description|Desc|Purpose|Summary|Notes"
        r"|Report\s*Name|Report|View\s*Name|Proc(?:edure)?\s*Name|Object\s*Name"
        r"|Revision|Version|Rev"
        r"|Date|Created\s*Date|Modified\s*Date|Updated\s*Date|Change\s*Date"
        r"|Ticket|Jira|Story|Task|Issue|CR|Change\s*Request"
        r"|Department|Team|Project"
        r"|Parameters|Params"
        r"|Returns|Output"
        r"|Dependencies|Depends\s*On"
        r"|History|Change\s*Log|Changelog)"
        r"\s*[:=]\s*(.*)",
        re.IGNORECASE,
    )

    # Revision history line: date + author + description
    # Pattern: 2024-01-15  John Smith  Added new column
    #      or: 01/15/2024  jsmith      Fixed join
    _REVISION_LINE = re.compile(
        r"^[-*\s]*"
        r"(\d{1,4}[-/]\d{1,2}[-/]\d{1,4})"  # date
        r"\s+"
        r"(\S+(?:\s+\S+)?)"  # author (1-2 words)
        r"\s{2,}"  # gap
        r"(.+)",  # description
    )

    for line in lines:
        stripped = line.strip()
        upper = stripped.upper()

        # Extract object info from the SSMS header comment
        # Pattern: /****** Object:  View [schema].[name]    Script Date: ... ******/
        obj_match = re.match(
            r"/\*+\s*Object:\s+(\w+)\s+\[([^\]]*)\]\.\[([^\]]*)\]"
            r"(?:\s+Script Date:\s*(.+?))?\s*\*+/",
            stripped,
        )
        if obj_match:
            metadata["object_type"] = obj_match.group(1)
            metadata["schema"] = obj_match.group(2)
            metadata["name"] = obj_match.group(3)
            if obj_match.group(4):
                metadata["script_date"] = obj_match.group(4).strip()
            continue

        # Track block comments that may contain metadata
        if not body_started:
            if "/*" in stripped and "*/" not in stripped:
                in_header_comment = True
                # Check if this line itself has metadata after /*
                after_open = stripped.split("/*", 1)[1].strip()
                if after_open:
                    header_comment_lines.append(after_open)
                continue
            if in_header_comment:
                if "*/" in stripped:
                    in_header_comment = False
                    before_close = stripped.split("*/", 1)[0].strip()
                    if before_close:
                        header_comment_lines.append(before_close)
                    # Parse all collected header comment lines
                    _parse_header_comments(header_comment_lines, metadata)
                    header_comment_lines = []
                else:
                    header_comment_lines.append(stripped)
                continue

            # Single-line comments before the body (-- Author: xxx)
            if stripped.startswith("--"):
                comment_text = stripped.lstrip("-").strip()
                meta_match = _META_PATTERN.match(comment_text)
                if meta_match:
                    key = _normalize_meta_key(meta_match.group(1))
                    val = meta_match.group(2).strip()
                    if val:
                        _add_meta(metadata, key, val)
                continue

        # Skip SSMS-scripted boilerplate: USE / GO / SET-option statements
        if upper in ("GO", "GO;"):
            continue
        if upper.startswith("USE "):
            # `USE [database]` is a session-level switch SSMS emits before the
            # actual DDL; sqlglot.parse_one only takes one statement so this
            # has to be stripped or the parser stops at USE.
            continue
        if upper.startswith("SET ") and any(
            kw in upper for kw in ("ANSI_NULLS", "QUOTED_IDENTIFIER", "NOCOUNT",
                                    "ARITHABORT", "CONCAT_NULL_YIELDS_NULL",
                                    "ANSI_PADDING", "ANSI_WARNINGS",
                                    "NUMERIC_ROUNDABORT", "DATEFORMAT", "DATEFIRST")
        ):
            continue

        # Strip CREATE/ALTER VIEW/PROCEDURE wrapper -- keep everything after AS
        if not body_started:
            # Match: CREATE VIEW [schema].[name] AS
            # or:    CREATE OR ALTER VIEW [schema].[name] AS
            # or:    ALTER VIEW [schema].[name] AS
            create_match = re.match(
                r"(?:CREATE\s+(?:OR\s+ALTER\s+)?|ALTER\s+)"
                r"(?:VIEW|PROCEDURE|PROC|FUNCTION)\s+"
                r"(?:\[?[\w]+\]?\.)?\[?[\w]+\]?\s*"
                r"(?:\(.*?\))?\s*"  # optional params for procs/functions
                r"(?:AS)?\s*$",
                stripped,
                re.IGNORECASE,
            )
            if create_match:
                # Extract name if we didn't get it from the header
                if "name" not in metadata:
                    name_match = re.search(
                        r"(?:\[([^\]]+)\]\.)?\[([^\]]+)\]",
                        stripped,
                    )
                    if name_match:
                        metadata["schema"] = name_match.group(1) or "dbo"
                        metadata["name"] = name_match.group(2)
                body_started = True
                continue

            # Also handle "AS" on its own line after CREATE VIEW
            if upper == "AS":
                body_started = True
                continue

            # Skip blank lines and comments before body
            if not stripped or stripped.startswith("--") or stripped.startswith("/*"):
                continue

        # Once we hit a SELECT or WITH, we're definitely in the body
        if not body_started and (upper.startswith("SELECT") or upper.startswith("WITH")):
            body_started = True

        if body_started or upper.startswith("SELECT") or upper.startswith("WITH"):
            body_started = True
            clean_lines.append(line)

    clean_sql = "\n".join(clean_lines).strip()

    # Remove trailing GO if it slipped through
    if clean_sql.upper().endswith("\nGO"):
        clean_sql = clean_sql[:-3].strip()

    return clean_sql, metadata


# ---------------------------------------------------------------------------
# Convenience functions
# ---------------------------------------------------------------------------

def resolve_query(sql: str, dialect: str = None) -> ResolvedQuery:
    """Parse, extract, and resolve lineage for a SQL query.

    Raises ValueError for empty/unparsable SQL.
    """
    if not sql or not sql.strip():
        raise ValueError("Empty SQL input")

    # Preprocess SSMS script boilerplate
    clean_sql, metadata = preprocess_ssms(sql)
    if not clean_sql or not clean_sql.strip():
        clean_sql = sql.strip()  # fallback if preprocessing removed everything

    extractor = SQLBusinessLogicExtractor(dialect=dialect)
    logic = to_dict(extractor.extract(clean_sql))

    # Attach metadata to logic if present
    if metadata:
        logic["_object"] = metadata

    resolver = LineageResolver(logic)
    resolved = resolver.resolve_all()

    # Store metadata on the result
    resolved._metadata = metadata if metadata else {}
    if metadata:
        resolved.raw_sql = f"-- {metadata.get('object_type', 'Object')}: [{metadata.get('schema', 'dbo')}].[{metadata.get('name', '?')}]\n{clean_sql}"

    return resolved


def _filter_to_dict(f: ResolvedFilter) -> dict:
    """Serialize a ResolvedFilter, nesting any resolved subquery lineage."""
    d: dict = {"expression": f.expression}
    if f.subqueries:
        d["subqueries"] = [resolved_to_dict(sq) for sq in f.subqueries]
    return d


def resolved_to_dict(resolved: ResolvedQuery) -> dict:
    """Convert resolved query to plain dict."""
    result = {}

    # Extract object metadata stored by resolve_query
    if hasattr(resolved, '_metadata') and resolved._metadata:
        for k, v in resolved._metadata.items():
            result[k] = v
    else:
        # Fallback: extract from raw_sql header
        meta_match = re.match(r"-- (\w+): \[([^\]]*)\]\.\[([^\]]*)\]", resolved.raw_sql or "")
        if meta_match:
            result["object_type"] = meta_match.group(1)
            result["schema"] = meta_match.group(2)
            result["name"] = meta_match.group(3)

    columns = []
    for col in resolved.columns:
        entry = {"name": col.name, "type": col.type}
        if col.resolved_expression:
            entry["resolved_expression"] = col.resolved_expression
        if col.expression != col.resolved_expression:
            entry["direct_expression"] = col.expression
        if col.base_columns:
            entry["base_columns"] = col.base_columns
        if col.base_tables:
            entry["base_tables"] = col.base_tables
        if col.filters:
            entry["filters"] = [_filter_to_dict(f) for f in col.filters]
        if col.transformation_chain and len(col.transformation_chain) > 1:
            entry["transformation_chain"] = col.transformation_chain
        columns.append(entry)
    result["columns"] = columns
    if resolved.base_tables:
        result["base_tables"] = resolved.base_tables
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _format_text(resolved) -> str:
    """Format resolved query as human-readable text."""
    lines = []
    result_dict = resolved_to_dict(resolved)
    if result_dict.get("name"):
        schema = result_dict.get("schema", "dbo")
        obj_type = result_dict.get("object_type", "Object")
        lines.append(f"{obj_type}: [{schema}].[{result_dict['name']}]")
        for key in ("author", "description", "report_name", "version",
                    "ticket", "department", "team", "project",
                    "created_date", "modified_date", "modified_by",
                    "dependencies", "parameters", "returns", "notes"):
            if key in result_dict:
                label = key.replace("_", " ").title()
                lines.append(f"  {label}: {result_dict[key]}")
        if result_dict.get("revisions"):
            lines.append("  Revisions:")
            for rev in result_dict["revisions"]:
                if isinstance(rev, dict):
                    lines.append(f"    {rev['date']}  {rev['author']}  {rev['description']}")
                else:
                    lines.append(f"    {rev}")
        lines.append("=" * 60)
    for col in resolved.columns:
        lines.append(f"\n{col.name} ({col.type})")
        if col.resolved_expression:
            lines.append(f"  Resolved: {col.resolved_expression}")
        if col.base_columns:
            lines.append(f"  Base columns: {', '.join(col.base_columns)}")
        if col.base_tables:
            lines.append(f"  Base tables: {', '.join(col.base_tables)}")
        if col.filters:
            lines.append("  Filters:")
            for flt in col.filters:
                lines.append(f"    - {flt.expression}")
                for i, sq in enumerate(flt.subqueries):
                    tbls = list(sq.base_tables) or sorted({t for c in sq.columns for t in c.base_tables})
                    cols_ = sorted({c_ for c in sq.columns for c_ in c.base_columns})
                    lines.append(f"        subquery #{i + 1}: tables={tbls}")
                    if cols_:
                        lines.append(f"                       columns={cols_}")
        if col.transformation_chain and len(col.transformation_chain) > 1:
            lines.append("  Chain:")
            for i, step in enumerate(col.transformation_chain):
                pad = "    " + "  " * i
                scope = step.get("scope", "")
                sname = step.get("name", "")
                stype = step.get("type", "")
                sexpr = step.get("expression", "")
                if stype == "passthrough":
                    lines.append(f"{pad}-> {scope}.{sname} (passthrough)")
                else:
                    lines.append(f"{pad}-> {scope}.{sname} = {sexpr} ({stype})")
    return "\n".join(lines)


def _get_output_filename(resolved, input_path=None):
    """Derive output filename from view name or input file."""
    meta = getattr(resolved, '_metadata', {})
    name = meta.get("name")
    if name:
        return f"parsed_{name}"
    if input_path:
        import os
        base = os.path.splitext(os.path.basename(input_path))[0]
        return f"parsed_{base}"
    return "parsed_output"


def _process_one(sql, dialect, text_mode, compact):
    """Process a single SQL string. Returns (resolved, output_text)."""
    resolved = resolve_query(sql.strip(), dialect=dialect)
    if text_mode:
        return resolved, _format_text(resolved)
    else:
        indent = None if compact else 2
        return resolved, json.dumps(resolved_to_dict(resolved), indent=indent)


def main():
    import argparse
    import sys
    import os
    import glob as globmod

    parser = argparse.ArgumentParser(description="Resolve SQL lineage to base table.column")
    parser.add_argument("sql", nargs="?", help="SQL query")
    parser.add_argument("--file", "-f", help="SQL file (or glob pattern like '*.sql')")
    parser.add_argument("--stdin", action="store_true")
    parser.add_argument("--dialect", "-d", default=None)
    parser.add_argument("--compact", action="store_true")
    parser.add_argument("--text", action="store_true", help="Human-readable output")
    parser.add_argument("--output-dir", "-o", default=None,
                        help="Output directory. Files named parsed_<view_name>.<ext>")

    args = parser.parse_args()

    # Collect input files
    input_files = []
    if args.file:
        # Support glob patterns
        matched = globmod.glob(args.file)
        if matched:
            input_files = sorted(matched)
        else:
            input_files = [args.file]

    if input_files and len(input_files) > 1 and not args.output_dir:
        # Multiple files require --output-dir
        print("Error: multiple input files require --output-dir / -o", file=sys.stderr)
        sys.exit(1)

    ext = ".txt" if args.text else ".json"

    # Create output dir if needed
    if args.output_dir:
        os.makedirs(args.output_dir, exist_ok=True)

    if input_files:
        for fpath in input_files:
            with open(fpath) as fh:
                sql = fh.read()
            resolved, output = _process_one(sql, args.dialect, args.text, args.compact)

            if args.output_dir:
                out_name = _get_output_filename(resolved, fpath) + ext
                out_path = os.path.join(args.output_dir, out_name)
                with open(out_path, "w") as fh:
                    fh.write(output + "\n")
                meta = getattr(resolved, '_metadata', {})
                view_name = meta.get('name', os.path.basename(fpath))
                print(f"  {view_name} -> {out_path}")
            else:
                print(output)

    elif args.stdin:
        sql = sys.stdin.read()
        resolved, output = _process_one(sql, args.dialect, args.text, args.compact)
        if args.output_dir:
            out_name = _get_output_filename(resolved) + ext
            out_path = os.path.join(args.output_dir, out_name)
            with open(out_path, "w") as fh:
                fh.write(output + "\n")
            print(f"  -> {out_path}")
        else:
            print(output)

    elif args.sql:
        resolved, output = _process_one(args.sql, args.dialect, args.text, args.compact)
        print(output)

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
