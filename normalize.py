#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL Business Logic Extractor -- L2: Normalize

Takes L1 extraction output and produces normalized, comparable
business definitions with:
  - Alias resolution (e.table -> real_table.column)
  - Canonical expression forms (sorted ANDs, lowercased functions)
  - AST-based signatures for equality/similarity matching
  - Pattern classification (date_calculation, classification, aggregation, etc.)

Pipeline: L1 (parse) → L2 (normalize) → L3 (resolve) → L4 (translate) → L5 (compare)
"""

import hashlib
import re
from dataclasses import dataclass, field
from typing import Optional

import sqlglot
from sqlglot import exp

from extract import (
    SQLBusinessLogicExtractor, QueryLogic, to_dict,
    ColumnRef, OutputColumn, Filter, CaseLogic, Aggregation, WindowFunc,
)


# ---------------------------------------------------------------------------
# Business Definition
# ---------------------------------------------------------------------------

@dataclass
class BusinessDefinition:
    """A single atomic, normalized business rule extracted from SQL."""
    id: str                                     # unique within a catalog
    name: str                                   # output column name / filter label
    category: str                               # date_calculation, classification, aggregation, filter_rule, etc.
    subcategory: Optional[str] = None           # more specific: e.g. "conditional_count", "running_total"
    pattern: str = ""                           # abstract pattern: DATEDIFF(DAY, <date>, <date>)
    normalized_expression: str = ""             # fully resolved, canonical SQL
    signature: str = ""                         # hash of normalized AST for comparison
    structural_signature: str = ""              # hash of pattern (ignores column names, keeps structure)
    source_tables: list[str] = field(default_factory=list)
    source_columns: list[str] = field(default_factory=list)  # fully qualified: TABLE.COLUMN
    filters_context: list[str] = field(default_factory=list)  # normalized filter expressions
    query_file: str = ""                        # which file/query this came from
    query_label: str = ""                       # user-supplied label or auto-generated


@dataclass
class DefinitionCatalog:
    """Collection of business definitions from one or more queries."""
    definitions: list[BusinessDefinition] = field(default_factory=list)
    query_count: int = 0
    query_labels: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Alias resolver
# ---------------------------------------------------------------------------

class AliasResolver:
    """Resolves table aliases to real table names within a query."""

    def __init__(self, logic: dict):
        self._alias_map: dict[str, str] = {}
        self._build_map(logic)

    def _build_map(self, logic: dict):
        for src in logic.get("sources", []):
            name = src.get("name", "")
            alias = src.get("alias")
            if alias and alias != name:
                self._alias_map[alias.lower()] = name
            # Also map name to itself for consistency
            self._alias_map[name.lower()] = name

        for join in logic.get("joins", []):
            rt = join.get("right_table", "")
            ra = join.get("right_alias")
            if ra and ra != rt:
                self._alias_map[ra.lower()] = rt

        # CTEs -- the CTE name IS the table name (don't resolve further)
        for cte in logic.get("ctes", []):
            cte_name = cte.get("name", "")
            self._alias_map[cte_name.lower()] = cte_name

        # Subquery sources -- treat aliases as opaque names (don't resolve to raw SQL)
        for src in logic.get("sources", []):
            if src.get("type") == "subquery" and src.get("alias"):
                alias = src["alias"]
                self._alias_map[alias.lower()] = alias

    def resolve_table(self, alias_or_name: str) -> str:
        if not alias_or_name:
            return alias_or_name
        return self._alias_map.get(alias_or_name.lower(), alias_or_name)

    def resolve_column(self, col: dict) -> str:
        """Return fully qualified TABLE.COLUMN string."""
        table = col.get("table", "")
        column = col.get("column", "")
        resolved_table = self.resolve_table(table) if table else ""
        if resolved_table:
            return f"{resolved_table}.{column}"
        return column

    def resolve_expression(self, expr_sql: str) -> str:
        """Replace aliases in a SQL expression string with real table names."""
        result = expr_sql
        # Sort by length descending to avoid partial replacements
        for alias, real in sorted(self._alias_map.items(), key=lambda x: -len(x[0])):
            if alias == real.lower():
                continue
            # Replace alias.column patterns (word boundary before alias, dot after)
            pattern = r'\b' + re.escape(alias) + r'\.'
            replacement = real + '.'
            result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
        return result


# ---------------------------------------------------------------------------
# Expression canonicalizer
# ---------------------------------------------------------------------------

def canonicalize_expression(sql_expr: str) -> str:
    """Normalize a SQL expression to a canonical form for comparison.
    Strips aliases so that `x AS foo` and `x AS bar` produce the same canonical form."""
    if not sql_expr or not sql_expr.strip():
        return ""
    try:
        parsed = sqlglot.parse_one(sql_expr)
        # Strip alias -- we only care about the expression, not its name
        if isinstance(parsed, exp.Alias):
            parsed = parsed.this
        canonical = parsed.sql(pretty=False, normalize=True)
        return canonical
    except Exception:
        result = sql_expr.strip()
        result = re.sub(r'\s+', ' ', result)
        return result


def abstract_pattern(sql_expr: str) -> str:
    """Replace specific column/table names with placeholders to get the structural pattern."""
    if not sql_expr or not sql_expr.strip():
        return ""
    try:
        parsed = sqlglot.parse_one(sql_expr)
        return _abstract_node(parsed)
    except Exception:
        return sql_expr


def _abstract_node(node) -> str:
    """Recursively abstract an AST node, replacing columns with <col> and literals with <val>."""
    if isinstance(node, exp.Column):
        return "<col>"
    if isinstance(node, exp.Var):
        return "<col>"
    if isinstance(node, exp.Identifier):
        return "<col>"
    if isinstance(node, exp.Literal):
        return "<val>"
    if isinstance(node, exp.Boolean):
        return "<val>"
    if isinstance(node, exp.Null):
        return "NULL"

    # For functions, keep the function name but abstract arguments
    if isinstance(node, exp.Func):
        func_name = node.sql_name() if hasattr(node, 'sql_name') else type(node).__name__.upper()
        args = []
        for key, val in node.args.items():
            if val is None:
                continue
            if isinstance(val, exp.Expression):
                args.append(_abstract_node(val))
            elif isinstance(val, list):
                for item in val:
                    if isinstance(item, exp.Expression):
                        args.append(_abstract_node(item))
        if args:
            return f"{func_name}({', '.join(args)})"
        return func_name

    # For binary operations, abstract both sides
    if isinstance(node, exp.Binary):
        left = _abstract_node(node.left)
        right = _abstract_node(node.right)
        op = type(node).__name__.upper()
        # Map to SQL operators
        op_map = {
            "SUB": "-", "ADD": "+", "MUL": "*", "DIV": "/",
            "EQ": "=", "NEQ": "<>", "GT": ">", "GTE": ">=",
            "LT": "<", "LTE": "<=", "AND": "AND", "OR": "OR",
            "LIKE": "LIKE", "IS": "IS",
        }
        op_str = op_map.get(op, op)
        return f"{left} {op_str} {right}"

    # For CASE, keep structure
    if isinstance(node, exp.Case):
        parts = ["CASE"]
        for if_ in node.find_all(exp.If):
            cond = _abstract_node(if_.this) if if_.this else "<cond>"
            result = _abstract_node(if_.args.get("true")) if if_.args.get("true") else "<result>"
            parts.append(f"WHEN {cond} THEN {result}")
        default = node.args.get("default")
        if default:
            parts.append(f"ELSE {_abstract_node(default)}")
        parts.append("END")
        return " ".join(parts)

    # For window functions
    if isinstance(node, exp.Window):
        func = _abstract_node(node.this) if node.this else "<func>"
        parts = [f"{func} OVER ("]
        pb = node.args.get("partition_by")
        if pb:
            parts.append("PARTITION BY <cols>")
        order = node.find(exp.Order)
        if order:
            parts.append("ORDER BY <cols>")
        parts.append(")")
        return " ".join(parts)

    # For aliases, just return the inner expression
    if isinstance(node, exp.Alias):
        return _abstract_node(node.this)

    # For parentheses/grouping
    if isinstance(node, exp.Paren):
        return f"({_abstract_node(node.this)})"

    # For subqueries
    if isinstance(node, exp.Subquery):
        return "<subquery>"

    # Fallback
    try:
        return node.sql(pretty=False)
    except Exception:
        return str(node)


# ---------------------------------------------------------------------------
# Signature computation
# ---------------------------------------------------------------------------

def compute_signature(normalized_expr: str) -> str:
    """Hash a normalized expression for exact matching."""
    cleaned = normalized_expr.strip().lower()
    cleaned = re.sub(r'\s+', ' ', cleaned)
    return hashlib.sha256(cleaned.encode()).hexdigest()[:16]


def compute_structural_signature(pattern: str) -> str:
    """Hash an abstracted pattern for structural matching."""
    cleaned = pattern.strip().lower()
    cleaned = re.sub(r'\s+', ' ', cleaned)
    return hashlib.sha256(cleaned.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Pattern classifier
# ---------------------------------------------------------------------------

def classify_expression(output: dict, logic: dict) -> tuple[str, Optional[str]]:
    """Classify an output column into a business logic category.
    Returns (category, subcategory)."""

    col_type = output.get("type", "")
    expr = output.get("expression", "").upper()

    if col_type == "passthrough":
        return ("passthrough", None)

    if col_type == "literal":
        return ("constant", None)

    if col_type == "star":
        return ("star", None)

    if col_type == "subquery":
        return ("subquery", None)

    if col_type == "case":
        # Analyze CASE branches for subcategory
        case_exprs = logic.get("case_expressions", [])
        matching = [c for c in case_exprs if c.get("output_name") == output.get("name")]
        if matching:
            branches = matching[0].get("branches", [])
            n = len(branches)
            has_else = matching[0].get("else_result") is not None
            # Check if it's a simple mapping (CASE col WHEN val THEN result)
            conditions = [b.get("condition", "") for b in branches]
            all_eq = all("=" in c or "WHEN" in c.upper() for c in conditions)
            if all_eq and n <= 10:
                return ("classification", "value_mapping")
            if any("LIKE" in c.upper() or "BETWEEN" in c.upper() for c in conditions):
                return ("classification", "pattern_matching")
            return ("classification", "conditional_logic")

    if col_type == "window":
        window_funcs = logic.get("window_functions", [])
        for wf in window_funcs:
            func = wf.get("function", "").upper()
            if func in ("ROWNUMBER", "ROW_NUMBER"):
                return ("window_function", "ranking")
            if func in ("LAG", "LEAD"):
                return ("window_function", "offset_comparison")
            if func in ("SUM", "COUNT", "AVG", "MIN", "MAX"):
                return ("window_function", "running_aggregate")
            if func in ("RANK", "DENSERANK", "DENSE_RANK", "NTILE", "PERCENTRANK"):
                return ("window_function", "ranking")
            return ("window_function", func.lower())
        return ("window_function", None)

    if col_type == "aggregate":
        # Try to determine subcategory
        if "COUNT" in expr and "CASE" in expr:
            return ("aggregation", "conditional_count")
        if "SUM" in expr and "CASE" in expr:
            return ("aggregation", "conditional_sum")
        if "COUNT" in expr:
            return ("aggregation", "count")
        if "SUM" in expr:
            return ("aggregation", "sum")
        if "AVG" in expr:
            return ("aggregation", "average")
        if "MIN" in expr:
            return ("aggregation", "minimum")
        if "MAX" in expr:
            return ("aggregation", "maximum")
        return ("aggregation", None)

    if col_type == "calculated":
        if "DATEDIFF" in expr or "DATE_DIFF" in expr or "TIMESTAMPDIFF" in expr:
            return ("date_calculation", "date_difference")
        if "DATEADD" in expr or "DATE_ADD" in expr:
            return ("date_calculation", "date_arithmetic")
        if "CAST" in expr or "CONVERT" in expr or "TRY_CAST" in expr:
            if "FLOAT" in expr or "DECIMAL" in expr or "INT" in expr:
                return ("type_conversion", "numeric_cast")
            if "DATE" in expr or "TIME" in expr:
                return ("type_conversion", "date_cast")
            return ("type_conversion", None)
        if "COALESCE" in expr or "ISNULL" in expr or "NVL" in expr or "IFNULL" in expr:
            return ("null_handling", "default_value")
        if "CONCAT" in expr or "||" in expr:
            return ("string_operation", "concatenation")
        if "UPPER" in expr or "LOWER" in expr or "TRIM" in expr or "SUBSTRING" in expr:
            return ("string_operation", "transformation")
        if "ROUND" in expr or "CEIL" in expr or "FLOOR" in expr or "ABS" in expr:
            return ("numeric_operation", "rounding")
        if any(op in expr for op in (" - ", " + ", " * ", " / ")):
            return ("arithmetic", "calculation")
        return ("calculated", None)

    return ("unknown", None)


def classify_filter(f: dict) -> tuple[str, Optional[str]]:
    """Classify a filter into a category."""
    scope = f.get("scope", "")
    expr = f.get("expression", "").upper()

    if scope == "join":
        return ("join_condition", None)
    if scope == "having":
        return ("post_aggregation_filter", None)
    if scope == "qualify":
        return ("window_filter", None)

    # WHERE filters
    if "IS NULL" in expr or "IS NOT NULL" in expr:
        return ("null_check", None)
    if "LIKE" in expr:
        return ("pattern_filter", None)
    if "BETWEEN" in expr:
        return ("range_filter", None)
    if " IN " in expr:
        return ("membership_filter", None)
    if "EXISTS" in expr:
        return ("existence_filter", None)
    if ">=" in expr or "<=" in expr or ">" in expr and "=" not in expr.replace(">=", "").replace("<>", "") or "<" in expr:
        return ("comparison_filter", None)
    if "=" in expr and "<>" not in expr:
        return ("equality_filter", None)
    if "<>" in expr or "!=" in expr:
        return ("inequality_filter", None)

    return ("filter", None)


# ---------------------------------------------------------------------------
# Normalizer: Layer 1 -> Layer 2
# ---------------------------------------------------------------------------

class BusinessLogicNormalizer:
    """Converts Layer 1 extraction output into normalized business definitions."""

    def __init__(self, query_file: str = "", query_label: str = ""):
        self.query_file = query_file
        self.query_label = query_label
        self._def_counter = 0

    def _next_id(self) -> str:
        self._def_counter += 1
        label = self.query_label or self.query_file or "q"
        return f"{label}:def-{self._def_counter:03d}"

    def normalize(self, logic: dict) -> list[BusinessDefinition]:
        """Convert Layer 1 output dict into a list of BusinessDefinitions."""
        resolver = AliasResolver(logic)
        defs = []

        # Extract definitions from output columns (non-passthrough)
        for output in logic.get("outputs", []):
            if output.get("type") in ("passthrough", "star"):
                continue
            bd = self._normalize_output(output, logic, resolver)
            if bd:
                defs.append(bd)

        # Extract definitions from filters (WHERE/HAVING only, not join)
        for f in logic.get("filters", []):
            if f.get("scope") in ("where", "having", "qualify"):
                bd = self._normalize_filter(f, logic, resolver)
                if bd:
                    defs.append(bd)

        # Recursively extract from CTEs
        for cte in logic.get("ctes", []):
            cte_logic = cte.get("logic")
            if cte_logic:
                sub_normalizer = BusinessLogicNormalizer(
                    query_file=self.query_file,
                    query_label=f"{self.query_label}:CTE:{cte.get('name', '')}",
                )
                sub_normalizer._def_counter = self._def_counter
                sub_defs = sub_normalizer.normalize(cte_logic)
                self._def_counter = sub_normalizer._def_counter
                defs.extend(sub_defs)

        # Recursively extract from subqueries
        for sq in logic.get("subqueries", []):
            sq_logic = sq.get("logic")
            if sq_logic:
                ctx = sq.get("context", "subquery")
                alias = sq.get("alias", "")
                sub_label = f"{self.query_label}:SUB:{alias or ctx}"
                sub_normalizer = BusinessLogicNormalizer(
                    query_file=self.query_file,
                    query_label=sub_label,
                )
                sub_normalizer._def_counter = self._def_counter
                sub_defs = sub_normalizer.normalize(sq_logic)
                self._def_counter = sub_normalizer._def_counter
                defs.extend(sub_defs)

        return defs

    def _normalize_output(self, output: dict, logic: dict, resolver: AliasResolver) -> Optional[BusinessDefinition]:
        """Create a BusinessDefinition from an output column."""
        name = output.get("name", "")
        expr = output.get("expression", "")
        category, subcategory = classify_expression(output, logic)

        # Resolve aliases in expression
        normalized_expr = resolver.resolve_expression(expr)
        normalized_expr = canonicalize_expression(normalized_expr)

        # Abstract pattern (strip alias before abstracting)
        try:
            parsed_expr = sqlglot.parse_one(expr)
            if isinstance(parsed_expr, exp.Alias):
                inner_expr = parsed_expr.this.sql(pretty=False)
            else:
                inner_expr = expr
        except Exception:
            inner_expr = expr
        pattern = abstract_pattern(inner_expr)

        # Resolve source columns
        source_cols = []
        source_tables = set()
        for col in output.get("source_columns", []):
            resolved = resolver.resolve_column(col)
            source_cols.append(resolved)
            table = resolver.resolve_table(col.get("table", ""))
            if table:
                source_tables.add(table)

        # Find applicable filters
        filters_ctx = self._find_filters_for_output(output, logic, resolver)

        sig = compute_signature(normalized_expr)
        struct_sig = compute_structural_signature(pattern)

        return BusinessDefinition(
            id=self._next_id(),
            name=name,
            category=category,
            subcategory=subcategory,
            pattern=pattern,
            normalized_expression=normalized_expr,
            signature=sig,
            structural_signature=struct_sig,
            source_tables=sorted(source_tables),
            source_columns=source_cols,
            filters_context=filters_ctx,
            query_file=self.query_file,
            query_label=self.query_label,
        )

    def _normalize_filter(self, f: dict, logic: dict, resolver: AliasResolver) -> Optional[BusinessDefinition]:
        """Create a BusinessDefinition from a filter."""
        expr = f.get("expression", "")
        scope = f.get("scope", "")
        category, subcategory = classify_filter(f)

        normalized_expr = resolver.resolve_expression(expr)
        normalized_expr = canonicalize_expression(normalized_expr)
        pattern = abstract_pattern(expr)

        source_cols = []
        source_tables = set()
        for col in f.get("columns", []):
            resolved = resolver.resolve_column(col)
            source_cols.append(resolved)
            table = resolver.resolve_table(col.get("table", ""))
            if table:
                source_tables.add(table)

        sig = compute_signature(normalized_expr)
        struct_sig = compute_structural_signature(pattern)

        name = f"filter:{scope}:{source_cols[0] if source_cols else 'unknown'}"

        return BusinessDefinition(
            id=self._next_id(),
            name=name,
            category=category,
            subcategory=subcategory,
            pattern=pattern,
            normalized_expression=normalized_expr,
            signature=sig,
            structural_signature=struct_sig,
            source_tables=sorted(source_tables),
            source_columns=source_cols,
            filters_context=[],
            query_file=self.query_file,
            query_label=self.query_label,
        )

    def _find_filters_for_output(self, output: dict, logic: dict, resolver: AliasResolver) -> list[str]:
        """Find all filters that apply to the source tables of an output column."""
        output_tables = set()
        for col in output.get("source_columns", []):
            table = resolver.resolve_table(col.get("table", ""))
            if table:
                output_tables.add(table.lower())

        filters = []
        for f in logic.get("filters", []):
            if f.get("scope") not in ("where", "having", "qualify"):
                continue
            for col in f.get("columns", []):
                table = resolver.resolve_table(col.get("table", ""))
                if table and table.lower() in output_tables:
                    normalized = resolver.resolve_expression(f.get("expression", ""))
                    normalized = canonicalize_expression(normalized)
                    if normalized not in filters:
                        filters.append(normalized)
                    break
        return filters


# ---------------------------------------------------------------------------
# Convenience: extract + normalize in one step
# ---------------------------------------------------------------------------

def extract_definitions(sql: str, query_file: str = "", query_label: str = "",
                        dialect: str = None) -> list[BusinessDefinition]:
    """Extract and normalize business definitions from a SQL query."""
    extractor = SQLBusinessLogicExtractor(dialect=dialect)
    logic = to_dict(extractor.extract(sql))
    normalizer = BusinessLogicNormalizer(query_file=query_file, query_label=query_label)
    return normalizer.normalize(logic)


def definitions_to_dict(defs: list[BusinessDefinition]) -> list[dict]:
    """Convert definitions to plain dicts, filtering empty fields."""
    result = []
    for d in defs:
        entry = {}
        for k, v in d.__dict__.items():
            if v is None or v == [] or v == "" or v is False:
                continue
            entry[k] = v
        result.append(entry)
    return result
