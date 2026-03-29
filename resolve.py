#!/usr/bin/env python3
"""
SQL Business Logic Extractor — Lineage Resolution

Traces every output column through CTEs, subqueries, and derived tables
all the way down to base table.column references.

Takes Layer 1 extraction output and produces a fully resolved lineage
where every passthrough is inlined to show the complete transformation chain.
"""

import json
from dataclasses import dataclass, field
from typing import Optional

from extract import SQLBusinessLogicExtractor, to_dict


# ---------------------------------------------------------------------------
# Resolved lineage structures
# ---------------------------------------------------------------------------

@dataclass
class ResolvedColumn:
    """A fully resolved output column with its complete transformation chain."""
    name: str
    expression: str
    type: str                               # final type: calculated, aggregate, etc.
    base_columns: list[str] = field(default_factory=list)  # ultimate table.column refs
    base_tables: list[str] = field(default_factory=list)
    filters: list[str] = field(default_factory=list)       # all filters in the chain
    transformation_chain: list[dict] = field(default_factory=list)  # [{scope, name, expression, type}]
    resolved_expression: str = ""           # fully inlined expression


@dataclass
class ResolvedQuery:
    """A query with fully resolved lineage for every output column."""
    columns: list[ResolvedColumn] = field(default_factory=list)
    raw_sql: str = ""


# ---------------------------------------------------------------------------
# Scope registry — maps scope outputs to their definitions
# ---------------------------------------------------------------------------

class ScopeRegistry:
    """Builds a lookup of all named outputs across all scopes (CTEs, subqueries, main query).

    Each entry maps (scope_name, column_name) → output definition dict from Layer 1.
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
            if name and cte_logic:
                self._register_scope(name, cte_logic)
                self._derived_scopes.add(name.lower())
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

    def get_filters(self, scope_name: str) -> list[str]:
        """Get all WHERE/HAVING filters for a scope."""
        logic = self._scope_logic.get(scope_name.lower(), {})
        filters = []
        for f in logic.get("filters", []):
            if f.get("scope") in ("where", "having", "qualify"):
                filters.append(f.get("expression", ""))
        return filters

    def is_base_table(self, name: str) -> bool:
        """Check if a name refers to a base table (not a CTE/subquery)."""
        return name.lower() in self._base_tables and name.lower() not in self._derived_scopes

    def is_derived(self, name: str) -> bool:
        """Check if a name refers to a CTE or subquery."""
        return name.lower() in self._derived_scopes

    def get_alias_to_table(self, scope_name: str) -> dict[str, str]:
        """Get alias→table mapping for a scope."""
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

    def resolve_all(self) -> ResolvedQuery:
        """Resolve every output column in the main query."""
        result = ResolvedQuery(raw_sql=self.logic.get("raw_sql", ""))

        for out in self.logic.get("outputs", []):
            name = out.get("name", "")
            if name == "*":
                result.columns.append(ResolvedColumn(
                    name="*", expression="*", type="star",
                ))
                continue

            resolved = self._resolve_output(out, "__main__", [])
            result.columns.append(resolved)

        return result

    def _resolve_output(self, out: dict, scope: str, visited: list) -> ResolvedColumn:
        """Resolve a single output column, following references through scopes."""
        name = out.get("name", "")
        expr = out.get("expression", "")
        col_type = out.get("type", "")
        source_cols = out.get("source_columns", [])

        # Get alias mapping for this scope
        alias_map = self.registry.get_alias_to_table(scope)

        # Collect filters from this scope
        scope_filters = self.registry.get_filters(scope)

        # Build the chain entry for this level
        chain_entry = {
            "scope": scope if scope != "__main__" else "query",
            "name": name,
            "expression": expr,
            "type": col_type,
        }

        if col_type == "passthrough":
            # This column comes from a source — trace it
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
                    return ResolvedColumn(
                        name=name, expression=expr, type="passthrough",
                        base_columns=[f"{real_table}.{src_col}"],
                        base_tables=[real_table],
                        transformation_chain=[chain_entry],
                    )

                # Is this from a derived scope (CTE/subquery)?
                if self.registry.is_derived(real_table):
                    inner_out = self.registry.lookup(real_table, src_col)
                    if inner_out:
                        inner_resolved = self._resolve_output(
                            inner_out, real_table,
                            visited + [cache_key],
                        )
                        # Prepend our chain entry and merge
                        # Deduplicate filters
                        merged_filters = list(dict.fromkeys(scope_filters + inner_resolved.filters))
                        return ResolvedColumn(
                            name=name,
                            expression=inner_resolved.resolved_expression or inner_resolved.expression,
                            type=inner_resolved.type,
                            base_columns=inner_resolved.base_columns,
                            base_tables=inner_resolved.base_tables,
                            filters=merged_filters,
                            transformation_chain=[chain_entry] + inner_resolved.transformation_chain,
                            resolved_expression=inner_resolved.resolved_expression,
                        )

                # Base table — terminal
                qualified = f"{real_table}.{src_col}"
                return ResolvedColumn(
                    name=name, expression=expr, type="passthrough",
                    base_columns=[qualified],
                    base_tables=[real_table],
                    filters=scope_filters,
                    transformation_chain=[chain_entry],
                    resolved_expression=qualified,
                )

            # No source columns — just return as-is
            return ResolvedColumn(
                name=name, expression=expr, type=col_type,
                filters=scope_filters,
                transformation_chain=[chain_entry],
            )

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
        filters = list(dict.fromkeys(all_filters))

        # Build resolved expression by inlining
        resolved_expr = self._inline_expression(expr, scope, alias_map, visited)

        return ResolvedColumn(
            name=name,
            expression=expr,
            type=col_type,
            base_columns=base_cols,
            base_tables=base_tables,
            filters=filters,
            transformation_chain=[chain_entry] + sub_chains,
            resolved_expression=resolved_expr,
        )

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
                # It's a base table — return it
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
                    # Try without table qualifier — check all derived scopes
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
# Convenience functions
# ---------------------------------------------------------------------------

def resolve_query(sql: str, dialect: str = None) -> ResolvedQuery:
    """Parse, extract, and resolve lineage for a SQL query."""
    extractor = SQLBusinessLogicExtractor(dialect=dialect)
    logic = to_dict(extractor.extract(sql))
    resolver = LineageResolver(logic)
    return resolver.resolve_all()


def resolved_to_dict(resolved: ResolvedQuery) -> dict:
    """Convert resolved query to plain dict."""
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
            entry["filters"] = col.filters
        if col.transformation_chain and len(col.transformation_chain) > 1:
            entry["transformation_chain"] = col.transformation_chain
        columns.append(entry)
    return {"columns": columns}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="Resolve SQL lineage to base table.column")
    parser.add_argument("sql", nargs="?", help="SQL query")
    parser.add_argument("--file", "-f", help="SQL file")
    parser.add_argument("--stdin", action="store_true")
    parser.add_argument("--dialect", "-d", default=None)
    parser.add_argument("--compact", action="store_true")
    parser.add_argument("--text", action="store_true", help="Human-readable output")

    args = parser.parse_args()

    if args.file:
        with open(args.file) as f:
            sql = f.read()
    elif args.stdin:
        sql = sys.stdin.read()
    elif args.sql:
        sql = args.sql
    else:
        parser.print_help()
        sys.exit(1)

    resolved = resolve_query(sql.strip(), dialect=args.dialect)

    if args.text:
        for col in resolved.columns:
            print(f"\n{col.name} ({col.type})")
            if col.resolved_expression:
                print(f"  Resolved: {col.resolved_expression}")
            if col.base_columns:
                print(f"  Base columns: {', '.join(col.base_columns)}")
            if col.base_tables:
                print(f"  Base tables: {', '.join(col.base_tables)}")
            if col.filters:
                print(f"  Filters:")
                for f in col.filters:
                    print(f"    - {f}")
            if col.transformation_chain and len(col.transformation_chain) > 1:
                print(f"  Chain:")
                for i, step in enumerate(col.transformation_chain):
                    indent = "    " + "  " * i
                    scope = step.get("scope", "")
                    sname = step.get("name", "")
                    stype = step.get("type", "")
                    sexpr = step.get("expression", "")
                    if stype == "passthrough":
                        print(f"{indent}→ {scope}.{sname} (passthrough)")
                    else:
                        print(f"{indent}→ {scope}.{sname} = {sexpr} ({stype})")
    else:
        indent = None if args.compact else 2
        print(json.dumps(resolved_to_dict(resolved), indent=indent))


if __name__ == "__main__":
    main()
