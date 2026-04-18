#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL Business Logic Extractor -- L1: Parse

Parses SQL queries and extracts transformations, filters, joins, aggregations,
window functions, CTEs, and column-level lineage into a structured format.

Pipeline: L1 (parse) → L2 (normalize) → L3 (resolve) → L4 (translate) → L5 (compare)

Requirements: pip install sqlglot
"""

import json
from dataclasses import dataclass, field, asdict
from typing import Optional
import sqlglot
from sqlglot import exp, errors


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ColumnRef:
    column: str
    table: Optional[str] = None
    schema: Optional[str] = None

    def qualified(self) -> str:
        parts = [p for p in (self.schema, self.table, self.column) if p]
        return ".".join(parts)


@dataclass
class OutputColumn:
    name: str
    expression: str
    type: str  # passthrough, calculated, aggregate, window, case, subquery, literal, star
    source_columns: list[ColumnRef] = field(default_factory=list)


@dataclass
class Filter:
    expression: str
    scope: str  # where, having, join, qualify
    columns: list[ColumnRef] = field(default_factory=list)


@dataclass
class JoinInfo:
    join_type: str
    right_table: str
    right_alias: Optional[str]
    on_expression: Optional[str]
    columns: list[ColumnRef] = field(default_factory=list)


@dataclass
class Aggregation:
    function: str
    expression: str
    source_columns: list[ColumnRef] = field(default_factory=list)
    group_by: list[str] = field(default_factory=list)


@dataclass
class WindowFunc:
    function: str
    expression: str
    partition_by: list[str] = field(default_factory=list)
    order_by: list[str] = field(default_factory=list)
    frame: Optional[str] = None
    source_columns: list[ColumnRef] = field(default_factory=list)


@dataclass
class CaseLogic:
    output_name: str
    expression: str
    branches: list[dict] = field(default_factory=list)
    else_result: Optional[str] = None
    source_columns: list[ColumnRef] = field(default_factory=list)


@dataclass
class CTEDef:
    name: str
    query: str
    logic: Optional[dict] = None


@dataclass
class SubqueryInfo:
    context: str  # select, from, where, exists, in
    expression: str
    alias: Optional[str] = None
    logic: Optional[dict] = None


@dataclass
class SourceTable:
    name: str
    alias: Optional[str] = None
    schema: Optional[str] = None
    type: str = "table"


@dataclass
class Lineage:
    output: str
    expression: str
    depends_on: list[str] = field(default_factory=list)
    filtered_by: dict = field(default_factory=dict)


@dataclass
class QueryLogic:
    sources: list[SourceTable] = field(default_factory=list)
    outputs: list[OutputColumn] = field(default_factory=list)
    filters: list[Filter] = field(default_factory=list)
    joins: list[JoinInfo] = field(default_factory=list)
    aggregations: list[Aggregation] = field(default_factory=list)
    window_functions: list[WindowFunc] = field(default_factory=list)
    case_expressions: list[CaseLogic] = field(default_factory=list)
    ctes: list[CTEDef] = field(default_factory=list)
    subqueries: list[SubqueryInfo] = field(default_factory=list)
    lineage: list[Lineage] = field(default_factory=list)
    set_operations: list[dict] = field(default_factory=list)
    order_by: list[str] = field(default_factory=list)
    limit: Optional[str] = None
    distinct: bool = False
    raw_sql: str = ""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sql(node, dialect=None) -> str:
    if node is None:
        return ""
    try:
        return node.sql(pretty=False, dialect=dialect)
    except Exception:
        return str(node)


def _extract_columns(node) -> list[ColumnRef]:
    cols = []
    if node is None:
        return cols
    for col in node.find_all(exp.Column):
        table = col.table if col.table else None
        schema = None
        if hasattr(col, "catalog") and col.catalog:
            schema = col.catalog
        cols.append(ColumnRef(column=col.name, table=table, schema=schema))
    return cols


def _is_simple_column(node) -> bool:
    if isinstance(node, exp.Column):
        return True
    if isinstance(node, exp.Alias):
        return _is_simple_column(node.this)
    return False


_AGG_TYPES = (
    exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max,
    exp.ArrayAgg, exp.GroupConcat, exp.Stddev, exp.Variance,
)


def _is_aggregate(node) -> bool:
    inner = node.this if isinstance(node, exp.Alias) else node
    if isinstance(inner, _AGG_TYPES):
        return True
    for _ in inner.find_all(*_AGG_TYPES):
        return True
    return False


def _is_window(node) -> bool:
    inner = node.this if isinstance(node, exp.Alias) else node
    if isinstance(inner, exp.Window):
        return True
    for _ in inner.find_all(exp.Window):
        return True
    return False


def _is_case(node) -> bool:
    inner = node.this if isinstance(node, exp.Alias) else node
    return isinstance(inner, exp.Case)


def _is_subquery_expr(node) -> bool:
    inner = node.this if isinstance(node, exp.Alias) else node
    if isinstance(inner, (exp.Subquery, exp.Select)):
        return True
    for _ in inner.find_all(exp.Subquery):
        return True
    return False


def _is_literal(node) -> bool:
    inner = node.this if isinstance(node, exp.Alias) else node
    return isinstance(inner, (exp.Literal, exp.Null, exp.Boolean))


def _is_star(node) -> bool:
    inner = node.this if isinstance(node, exp.Alias) else node
    return isinstance(inner, exp.Star)


def _get_alias(node) -> str:
    if isinstance(node, exp.Alias):
        return node.alias
    if isinstance(node, exp.Column):
        return node.name
    if isinstance(node, exp.Star):
        return "*"
    return _sql(node)


# ---------------------------------------------------------------------------
# Core extractor
# ---------------------------------------------------------------------------

class SQLBusinessLogicExtractor:
    def __init__(self, dialect: str = None):
        self.dialect = dialect

    def extract(self, sql: str) -> QueryLogic:
        if not sql or not sql.strip():
            raise ValueError("Empty or unparsable SQL")

        try:
            parsed = sqlglot.parse(sql, read=self.dialect)
        except (errors.ParseError, errors.TokenError) as e:
            raise ValueError(f"Failed to parse SQL: {e}")
        except Exception as e:
            raise ValueError(f"Failed to parse SQL: {e}")

        if not parsed or parsed[0] is None:
            raise ValueError("Empty or unparsable SQL")

        if len(parsed) > 1:
            results = []
            for stmt in parsed:
                if stmt is not None:
                    results.append(self._extract_statement(stmt, sql))
            return results[0] if results else QueryLogic(raw_sql=sql)

        return self._extract_statement(parsed[0], sql)

    def _extract_statement(self, tree, raw_sql: str) -> QueryLogic:
        logic = QueryLogic(raw_sql=raw_sql)

        if isinstance(tree, exp.Union):
            self._extract_set_operations(tree, logic)
            return logic

        if not isinstance(tree, exp.Select):
            select = tree.find(exp.Select)
            if select:
                return self._extract_select(select, logic)
            return logic

        return self._extract_select(tree, logic)

    def _extract_select(self, select: exp.Select, logic: QueryLogic) -> QueryLogic:
        self._extract_ctes(select, logic)
        self._extract_sources(select, logic)
        self._extract_joins(select, logic)
        self._extract_outputs(select, logic)
        self._extract_where(select, logic)
        self._extract_having(select, logic)
        self._extract_group_by(select, logic)
        self._extract_window_details(select, logic)
        self._extract_case_details(select, logic)
        self._extract_subqueries(select, logic)
        self._extract_order_by(select, logic)

        limit = select.find(exp.Limit)
        if limit:
            logic.limit = _sql(limit.this) if limit.this else _sql(limit)

        if select.find(exp.Distinct):
            logic.distinct = True

        qualify = select.find(exp.Qualify)
        if qualify:
            logic.filters.append(Filter(
                expression=_sql(qualify.this), scope="qualify",
                columns=_extract_columns(qualify),
            ))

        self._build_lineage(logic)
        return logic

    def _extract_ctes(self, select, logic: QueryLogic):
        root = select
        while root.parent and not isinstance(root, exp.With):
            root = root.parent
        with_clause = select.find(exp.With) if not isinstance(root, exp.With) else root
        if with_clause is None and select.parent:
            with_clause = select.parent.find(exp.With)
        if with_clause is None:
            return

        for cte in with_clause.find_all(exp.CTE):
            cte_name = cte.alias
            cte_sql = _sql(cte.this, dialect=self.dialect)
            sub_logic = None
            try:
                sub_logic = to_dict(SQLBusinessLogicExtractor(dialect=self.dialect).extract(cte_sql))
            except Exception:
                pass
            logic.ctes.append(CTEDef(name=cte_name, query=cte_sql, logic=sub_logic))

    def _extract_sources(self, select, logic: QueryLogic):
        from_clause = select.find(exp.From)
        if not from_clause:
            return

        # Collect subquery boundaries so we can skip tables inside them
        subquery_nodes = set()
        for subq in select.find_all(exp.Subquery):
            subquery_nodes.add(id(subq))
            for descendant in subq.walk():
                subquery_nodes.add(id(descendant[0]))

        for table in select.find_all(exp.Table):
            # Skip tables that are inside a subquery (they belong to the inner query)
            if id(table) in subquery_nodes:
                continue
            alias = table.alias or table.name
            schema = table.db if hasattr(table, "db") and table.db else None
            logic.sources.append(SourceTable(
                name=table.name,
                alias=alias if alias != table.name else None,
                schema=schema, type="table",
            ))

        # Derived tables (subqueries in FROM) -- extract recursively
        for subq in from_clause.find_all(exp.Subquery):
            alias = subq.alias or None
            inner_sql = _sql(subq.this, dialect=self.dialect)
            sub_logic = None
            try:
                sub_logic = to_dict(SQLBusinessLogicExtractor(dialect=self.dialect).extract(inner_sql))
            except Exception:
                pass
            logic.sources.append(SourceTable(
                name=inner_sql, alias=alias, type="subquery",
            ))
            if sub_logic:
                logic.subqueries.append(SubqueryInfo(
                    context="from", expression=inner_sql, alias=alias, logic=sub_logic,
                ))

        for lat in select.find_all(exp.Lateral):
            if id(lat) in subquery_nodes:
                continue
            logic.sources.append(SourceTable(
                name=_sql(lat.this), alias=lat.alias or None, type="lateral",
            ))

    def _extract_joins(self, select, logic: QueryLogic):
        for join in select.find_all(exp.Join):
            join_type = "JOIN"
            if join.side:
                join_type = f"{join.side} JOIN"
            if join.kind:
                join_type = f"{join.kind} {join_type}" if join.side else f"{join.kind} JOIN"
            join_type = join_type.upper().strip()

            right = join.this
            right_name, right_alias = "", None
            if isinstance(right, exp.Table):
                right_name = right.name
                right_alias = right.alias if right.alias != right.name else None
            elif isinstance(right, exp.Subquery):
                right_name = _sql(right.this)
                right_alias = right.alias
            else:
                right_name = _sql(right)

            on_expr = join.args.get("on")
            on_sql = _sql(on_expr) if on_expr else None
            cols = _extract_columns(on_expr) if on_expr else []

            logic.joins.append(JoinInfo(
                join_type=join_type, right_table=right_name,
                right_alias=right_alias, on_expression=on_sql, columns=cols,
            ))
            if on_expr:
                logic.filters.append(Filter(expression=on_sql, scope="join", columns=cols))

    def _extract_outputs(self, select, logic: QueryLogic):
        for expr in select.expressions:
            alias = _get_alias(expr)
            sql_str = _sql(expr)
            inner = expr.this if isinstance(expr, exp.Alias) else expr
            source_cols = _extract_columns(inner)

            if _is_star(expr):
                col_type = "star"
            elif _is_literal(expr):
                col_type = "literal"
            elif _is_subquery_expr(expr):
                col_type = "subquery"
            elif _is_window(expr):
                col_type = "window"
            elif _is_aggregate(expr):
                col_type = "aggregate"
            elif _is_case(expr):
                col_type = "case"
            elif _is_simple_column(expr):
                col_type = "passthrough"
            else:
                col_type = "calculated"

            logic.outputs.append(OutputColumn(
                name=alias, expression=sql_str, type=col_type, source_columns=source_cols,
            ))

            if col_type == "aggregate":
                for agg in inner.find_all(*_AGG_TYPES):
                    logic.aggregations.append(Aggregation(
                        function=type(agg).__name__.upper(),
                        expression=_sql(agg),
                        source_columns=_extract_columns(agg),
                    ))

    def _extract_where(self, select, logic: QueryLogic):
        where = select.find(exp.Where)
        if not where:
            return
        for cond in self._split_conditions(where.this):
            logic.filters.append(Filter(
                expression=_sql(cond), scope="where", columns=_extract_columns(cond),
            ))

    def _extract_having(self, select, logic: QueryLogic):
        having = select.find(exp.Having)
        if not having:
            return
        for cond in self._split_conditions(having.this):
            logic.filters.append(Filter(
                expression=_sql(cond), scope="having", columns=_extract_columns(cond),
            ))

    def _extract_group_by(self, select, logic: QueryLogic):
        group = select.find(exp.Group)
        if not group:
            return
        group_keys = [_sql(g) for g in group.expressions]
        for agg in logic.aggregations:
            agg.group_by = group_keys

    def _extract_window_details(self, select, logic: QueryLogic):
        for expr in select.expressions:
            inner = expr.this if isinstance(expr, exp.Alias) else expr
            for win in inner.find_all(exp.Window):
                func = win.this
                func_name = type(func).__name__.upper() if func else "UNKNOWN"

                partition = []
                pb = win.args.get("partition_by")
                if pb:
                    partition = [_sql(p) for p in (pb if isinstance(pb, list) else [pb])]

                order = []
                order_clause = win.find(exp.Order)
                if order_clause:
                    order = [_sql(o) for o in order_clause.expressions]

                frame_str = None
                spec = win.find(exp.WindowSpec)
                if spec:
                    frame_str = _sql(spec)

                logic.window_functions.append(WindowFunc(
                    function=func_name, expression=_sql(win),
                    partition_by=partition, order_by=order,
                    frame=frame_str, source_columns=_extract_columns(win),
                ))

    def _extract_case_details(self, select, logic: QueryLogic):
        for expr in select.expressions:
            alias = _get_alias(expr)
            inner = expr.this if isinstance(expr, exp.Alias) else expr
            for case in inner.find_all(exp.Case):
                branches = []
                for if_ in case.find_all(exp.If):
                    cond = if_.this
                    result = if_.args.get("true")
                    branches.append({
                        "condition": _sql(cond),
                        "result": _sql(result) if result else None,
                    })
                else_result = None
                default = case.args.get("default")
                if default:
                    else_result = _sql(default)
                logic.case_expressions.append(CaseLogic(
                    output_name=alias, expression=_sql(case),
                    branches=branches, else_result=else_result,
                    source_columns=_extract_columns(case),
                ))

    def _extract_subqueries(self, select, logic: QueryLogic):
        where = select.find(exp.Where)
        if where:
            seen = set()
            for exists in where.find_all(exp.Exists):
                subq = exists.find(exp.Subquery)
                if subq:
                    seen.add(id(subq))
                    self._add_subquery(subq, "exists", logic)
                else:
                    # Exists.this may be a Select directly (not wrapped in Subquery)
                    inner = exists.this
                    if isinstance(inner, exp.Select):
                        seen.add(id(inner))
                        self._add_subquery_from_select(inner, "exists", logic)
            for in_ in where.find_all(exp.In):
                subq = in_.find(exp.Subquery)
                if subq and id(subq) not in seen:
                    seen.add(id(subq))
                    self._add_subquery(subq, "in", logic)
            for subq in where.find_all(exp.Subquery):
                if id(subq) not in seen:
                    self._add_subquery(subq, "where", logic)

        for expr in select.expressions:
            inner = expr.this if isinstance(expr, exp.Alias) else expr
            if isinstance(inner, exp.Subquery):
                self._add_subquery(inner, "select", logic)
            else:
                for subq in inner.find_all(exp.Subquery):
                    self._add_subquery(subq, "select", logic)

    def _add_subquery(self, subq, context: str, logic: QueryLogic):
        alias = subq.alias or None
        inner_sql = _sql(subq.this)
        sub_logic = None
        try:
            sub_logic = to_dict(SQLBusinessLogicExtractor(dialect=self.dialect).extract(inner_sql))
        except Exception:
            pass
        logic.subqueries.append(SubqueryInfo(
            context=context, expression=inner_sql, alias=alias, logic=sub_logic,
        ))

    def _add_subquery_from_select(self, select_node, context: str, logic: QueryLogic):
        inner_sql = _sql(select_node, dialect=self.dialect)
        sub_logic = None
        try:
            sub_logic = to_dict(SQLBusinessLogicExtractor(dialect=self.dialect).extract(inner_sql))
        except Exception:
            pass
        logic.subqueries.append(SubqueryInfo(
            context=context, expression=inner_sql, alias=None, logic=sub_logic,
        ))

    def _extract_order_by(self, select, logic: QueryLogic):
        order = select.find(exp.Order)
        if order:
            logic.order_by = [_sql(o) for o in order.expressions]

    def _extract_set_operations(self, tree, logic: QueryLogic):
        op_type = type(tree).__name__.upper()
        if isinstance(tree, exp.Union) and tree.args.get("distinct") is False:
            op_type = "UNION ALL"

        branches = []
        for branch in [tree.left, tree.right]:
            if isinstance(branch, exp.Union):
                sub_logic = QueryLogic(raw_sql=_sql(branch))
                self._extract_set_operations(branch, sub_logic)
                branches.append(to_dict(sub_logic))
            elif isinstance(branch, exp.Select):
                sub_logic = QueryLogic(raw_sql=_sql(branch))
                self._extract_select(branch, sub_logic)
                branches.append(to_dict(sub_logic))
            else:
                branches.append({"raw_sql": _sql(branch)})

        logic.set_operations.append({"type": op_type, "branches": branches})
        first_select = tree.find(exp.Select)
        if first_select:
            self._extract_outputs(first_select, logic)

    def _build_lineage(self, logic: QueryLogic):
        table_filters: dict[str, list[str]] = {}
        for f in logic.filters:
            if f.scope in ("where", "having", "qualify"):
                for col in f.columns:
                    key = col.table or "_unknown_"
                    table_filters.setdefault(key, []).append(f.expression)

        for out in logic.outputs:
            if out.type == "passthrough" and not out.source_columns:
                continue

            depends = []
            filtered = {}
            for src in out.source_columns:
                qname = src.qualified()
                depends.append(qname)
                applicable = []
                if src.table and src.table in table_filters:
                    applicable.extend(table_filters[src.table])
                if "_unknown_" in table_filters:
                    applicable.extend(table_filters["_unknown_"])
                if applicable:
                    filtered[qname] = list(set(applicable))

            if depends or out.type != "passthrough":
                logic.lineage.append(Lineage(
                    output=out.name, expression=out.expression,
                    depends_on=depends, filtered_by=filtered,
                ))

    def _split_conditions(self, node) -> list:
        if isinstance(node, exp.And):
            return self._split_conditions(node.left) + self._split_conditions(node.right)
        return [node]


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------

def to_dict(obj) -> dict:
    """Convert dataclass tree to plain dict, filtering empty fields."""
    if isinstance(obj, dict):
        return {k: to_dict(v) for k, v in obj.items() if v}
    if isinstance(obj, list):
        return [to_dict(i) for i in obj]
    if hasattr(obj, "__dataclass_fields__"):
        result = {}
        for k, v in asdict(obj).items():
            if v is None or v == [] or v == {} or v == "" or v is False:
                continue
            result[k] = v
        return result
    return obj
