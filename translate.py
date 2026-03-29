#!/usr/bin/env python3
"""
SQL Business Logic Extractor — Layer 4: Translate

Converts normalized business definitions into plain English descriptions
using pattern templates + column/table metadata.

No LLM required — deterministic template-based translation.
"""

import json
import re
import sys
from typing import Optional

import sqlglot
from sqlglot import exp

from normalize import (
    BusinessDefinition, extract_definitions, definitions_to_dict,
    BusinessLogicNormalizer, AliasResolver,
)
from extract import SQLBusinessLogicExtractor, to_dict
import metadata


# ---------------------------------------------------------------------------
# Template translators by category
# ---------------------------------------------------------------------------

def _col_desc(qualified: str) -> str:
    """Look up column description, return readable name."""
    return metadata.describe_qualified(qualified)


def _col_desc_short(qualified: str) -> str:
    """Short version — just the description without the qualified name."""
    parts = qualified.split(".")
    if len(parts) == 2:
        desc = metadata.get_column_description(parts[0], parts[1])
        if desc:
            return desc
    return qualified


def _table_desc(table_name: str) -> str:
    desc = metadata.get_table_description(table_name)
    if desc:
        return f"{table_name} ({desc})"
    return table_name


def _value_label(table: str, column: str, value: str) -> str:
    """Look up a value description for categorical columns."""
    desc = metadata.get_value_description(table, column, value)
    if desc:
        return f"{value} ({desc})"
    return str(value)


def _parse_filter_parts(expr: str) -> tuple[str, str, str]:
    """Try to parse 'col op value' from a simple filter expression. Returns (col, op, value)."""
    try:
        parsed = sqlglot.parse_one(expr)
        if isinstance(parsed, exp.EQ):
            return (parsed.left.sql(pretty=False), "=", parsed.right.sql(pretty=False))
        if isinstance(parsed, exp.GT):
            return (parsed.left.sql(pretty=False), ">", parsed.right.sql(pretty=False))
        if isinstance(parsed, exp.GTE):
            return (parsed.left.sql(pretty=False), ">=", parsed.right.sql(pretty=False))
        if isinstance(parsed, exp.LT):
            return (parsed.left.sql(pretty=False), "<", parsed.right.sql(pretty=False))
        if isinstance(parsed, exp.LTE):
            return (parsed.left.sql(pretty=False), "<=", parsed.right.sql(pretty=False))
        if isinstance(parsed, exp.NEQ):
            return (parsed.left.sql(pretty=False), "!=", parsed.right.sql(pretty=False))
    except Exception:
        pass
    return ("", "", "")


def _translate_datediff(bd: BusinessDefinition) -> str:
    """Translate DATEDIFF expressions."""
    try:
        parsed = sqlglot.parse_one(bd.normalized_expression)
        if isinstance(parsed, exp.DateDiff):
            unit_node = parsed.args.get("this")
            expr_node = parsed.args.get("expression")
            unit_node2 = parsed.args.get("unit")

            unit = unit_node.sql(pretty=False).upper() if unit_node else "?"
            col1 = expr_node.sql(pretty=False) if expr_node else "?"
            col2 = unit_node2.sql(pretty=False) if unit_node2 else "?"

            unit_word = {
                "DAY": "days", "YEAR": "years", "MONTH": "months",
                "HOUR": "hours", "MINUTE": "minutes", "SECOND": "seconds",
            }.get(unit, unit.lower() + "s")

            desc1 = _col_desc_short(col1)
            desc2 = _col_desc_short(col2)
            return f"The number of {unit_word} between {desc1} and {desc2}"
    except Exception:
        pass

    # Fallback
    cols = [_col_desc(c) for c in bd.source_columns]
    return f"Date difference calculation using {', '.join(cols) if cols else 'date fields'}"


def _translate_case(bd: BusinessDefinition) -> str:
    """Translate CASE expressions into readable classification rules."""
    try:
        parsed = sqlglot.parse_one(bd.normalized_expression)
        if not isinstance(parsed, exp.Case):
            # Might be wrapped
            case_node = parsed.find(exp.Case)
            if not case_node:
                return f"Conditional classification"
            parsed = case_node

        lines = ["Classified as:"]
        for if_ in parsed.find_all(exp.If):
            cond = if_.this
            result = if_.args.get("true")
            cond_text = _translate_condition(cond)
            result_text = result.sql(pretty=False) if result else "?"
            lines.append(f"  - {result_text} when {cond_text}")

        default = parsed.args.get("default")
        if default:
            lines.append(f"  - {default.sql(pretty=False)} otherwise")

        return "\n".join(lines)
    except Exception:
        return "Conditional classification"


def _translate_condition(node) -> str:
    """Translate a CASE WHEN condition to readable text."""
    if node is None:
        return "?"

    if isinstance(node, exp.LT):
        col = _col_desc_short(node.left.sql(pretty=False))
        val = node.right.sql(pretty=False)
        return f"{col} is less than {val}"

    if isinstance(node, exp.LTE):
        col = _col_desc_short(node.left.sql(pretty=False))
        val = node.right.sql(pretty=False)
        return f"{col} is at most {val}"

    if isinstance(node, exp.GT):
        col = _col_desc_short(node.left.sql(pretty=False))
        val = node.right.sql(pretty=False)
        return f"{col} is greater than {val}"

    if isinstance(node, exp.GTE):
        col = _col_desc_short(node.left.sql(pretty=False))
        val = node.right.sql(pretty=False)
        return f"{col} is at least {val}"

    if isinstance(node, exp.EQ):
        col = _col_desc_short(node.left.sql(pretty=False))
        val = node.right.sql(pretty=False)
        # Try value description
        parts = node.left.sql(pretty=False).split(".")
        if len(parts) == 2:
            vdesc = metadata.get_value_description(parts[0], parts[1], val)
            if vdesc:
                return f"{col} is {vdesc}"
        return f"{col} equals {val}"

    if isinstance(node, exp.Between):
        col = _col_desc_short(node.this.sql(pretty=False))
        low = node.args.get("low")
        high = node.args.get("high")
        low_v = low.sql(pretty=False) if low else "?"
        high_v = high.sql(pretty=False) if high else "?"
        return f"{col} is between {low_v} and {high_v}"

    if isinstance(node, exp.Like):
        col = _col_desc_short(node.this.sql(pretty=False))
        pattern = node.expression.sql(pretty=False) if node.expression else "?"
        return f"{col} matches pattern {pattern}"

    if isinstance(node, exp.Is):
        col = _col_desc_short(node.this.sql(pretty=False))
        if node.find(exp.Not):
            return f"{col} is not null"
        return f"{col} is null"

    if isinstance(node, exp.Not):
        inner = node.this
        if isinstance(inner, exp.Is):
            col = _col_desc_short(inner.this.sql(pretty=False))
            return f"{col} is not null"
        return f"not ({_translate_condition(inner)})"

    if isinstance(node, exp.And):
        left = _translate_condition(node.left)
        right = _translate_condition(node.right)
        return f"{left} and {right}"

    if isinstance(node, exp.Or):
        left = _translate_condition(node.left)
        right = _translate_condition(node.right)
        return f"{left} or {right}"

    # Fallback
    return node.sql(pretty=False)


def _translate_aggregation(bd: BusinessDefinition) -> str:
    """Translate aggregation expressions."""
    subcat = bd.subcategory or ""
    expr_upper = bd.normalized_expression.upper()

    func_word = {
        "count": "Count",
        "sum": "Sum",
        "average": "Average",
        "minimum": "Minimum",
        "maximum": "Maximum",
        "conditional_count": "Conditional count",
        "conditional_sum": "Conditional sum",
    }.get(subcat, "Aggregation")

    if subcat == "count" and "DISTINCT" in expr_upper:
        cols = [_col_desc_short(c) for c in bd.source_columns]
        return f"Count of distinct {', '.join(cols) if cols else 'records'}"

    if subcat == "count" and "*" in bd.normalized_expression:
        return "Count of records"

    if subcat in ("conditional_count", "conditional_sum"):
        # Try to extract the CASE condition
        try:
            parsed = sqlglot.parse_one(bd.normalized_expression)
            case = parsed.find(exp.Case)
            if case:
                ifs = list(case.find_all(exp.If))
                if ifs:
                    cond_text = _translate_condition(ifs[0].this)
                    return f"{func_word} of records where {cond_text}"
        except Exception:
            pass

    cols = [_col_desc_short(c) for c in bd.source_columns]
    if cols:
        return f"{func_word} of {', '.join(cols)}"
    return f"{func_word} of values"


def _translate_window(bd: BusinessDefinition) -> str:
    """Translate window function expressions."""
    subcat = bd.subcategory or ""

    if subcat == "ranking":
        return "Row ranking (used for deduplication or ordering)"
    if subcat == "offset_comparison":
        cols = [_col_desc_short(c) for c in bd.source_columns]
        return f"Previous/next value comparison using {', '.join(cols) if cols else 'columns'}"
    if subcat == "running_aggregate":
        cols = [_col_desc_short(c) for c in bd.source_columns]
        return f"Running total of {', '.join(cols) if cols else 'values'}"

    return f"Window function calculation"


def _translate_arithmetic(bd: BusinessDefinition) -> str:
    """Translate arithmetic expressions."""
    cols = [_col_desc_short(c) for c in bd.source_columns]
    expr = bd.normalized_expression

    if " - " in expr and len(cols) == 2:
        return f"{cols[0]} minus {cols[1]}"
    if " + " in expr and len(cols) == 2:
        return f"{cols[0]} plus {cols[1]}"
    if " * " in expr and len(cols) == 2:
        return f"{cols[0]} multiplied by {cols[1]}"
    if " / " in expr and len(cols) == 2:
        return f"{cols[0]} divided by {cols[1]}"

    if cols:
        return f"Calculation using {', '.join(cols)}"
    return f"Arithmetic calculation"


def _translate_filter(bd: BusinessDefinition) -> str:
    """Translate filter conditions."""
    expr = bd.normalized_expression
    try:
        parsed = sqlglot.parse_one(expr)
        text = _translate_condition(parsed)
        return f"Filter: only include records where {text}"
    except Exception:
        return f"Filter: {expr}"


def _translate_null_handling(bd: BusinessDefinition) -> str:
    cols = [_col_desc_short(c) for c in bd.source_columns]
    if cols:
        return f"Default value when {cols[0]} is missing"
    return "Default value for missing data"


def _translate_string_op(bd: BusinessDefinition) -> str:
    cols = [_col_desc_short(c) for c in bd.source_columns]
    subcat = bd.subcategory or ""
    if subcat == "concatenation":
        return f"Combined text from {', '.join(cols)}" if cols else "Text combination"
    if subcat == "transformation":
        return f"Text transformation of {', '.join(cols)}" if cols else "Text transformation"
    return f"Text operation on {', '.join(cols)}" if cols else "Text operation"


def _translate_type_conversion(bd: BusinessDefinition) -> str:
    cols = [_col_desc_short(c) for c in bd.source_columns]
    return f"Type conversion of {', '.join(cols)}" if cols else "Type conversion"


# ---------------------------------------------------------------------------
# Main translator
# ---------------------------------------------------------------------------

TRANSLATORS = {
    "date_calculation": _translate_datediff,
    "classification": _translate_case,
    "aggregation": _translate_aggregation,
    "window_function": _translate_window,
    "arithmetic": _translate_arithmetic,
    "null_handling": _translate_null_handling,
    "string_operation": _translate_string_op,
    "type_conversion": _translate_type_conversion,
    "numeric_operation": _translate_arithmetic,
    # Filters
    "equality_filter": _translate_filter,
    "comparison_filter": _translate_filter,
    "null_check": _translate_filter,
    "pattern_filter": _translate_filter,
    "range_filter": _translate_filter,
    "membership_filter": _translate_filter,
    "existence_filter": _translate_filter,
    "inequality_filter": _translate_filter,
    "post_aggregation_filter": _translate_filter,
    "window_filter": _translate_filter,
}


def translate_definition(bd: BusinessDefinition) -> str:
    """Translate a single business definition to plain English."""
    translator = TRANSLATORS.get(bd.category)
    if translator:
        return translator(bd)

    # Generic fallback
    if bd.category == "constant":
        return f"Fixed value"
    if bd.category == "subquery":
        return f"Value derived from a sub-query"
    if bd.category == "calculated":
        cols = [_col_desc_short(c) for c in bd.source_columns]
        return f"Calculated from {', '.join(cols)}" if cols else "Calculated value"

    return f"({bd.category})"


def translate_with_context(bd: BusinessDefinition) -> dict:
    """Translate a definition and return a full context dict."""
    description = translate_definition(bd)

    # Source tables in plain English
    source_tables = []
    for t in bd.source_tables:
        source_tables.append(_table_desc(t))

    # Filters in plain English
    filter_descriptions = []
    for f in bd.filters_context:
        try:
            parsed = sqlglot.parse_one(f)
            filter_descriptions.append(_translate_condition(parsed))
        except Exception:
            filter_descriptions.append(f)

    result = {
        "name": bd.name,
        "business_description": description,
        "category": bd.category,
    }
    if bd.subcategory:
        result["subcategory"] = bd.subcategory
    if source_tables:
        result["source"] = ", ".join(source_tables)
    if filter_descriptions:
        result["conditions"] = filter_descriptions
    result["technical_expression"] = bd.normalized_expression

    return result


# ---------------------------------------------------------------------------
# Batch translate
# ---------------------------------------------------------------------------

def translate_query(sql: str, query_label: str = "", dialect: str = None) -> list[dict]:
    """Extract, normalize, and translate all definitions in a SQL query."""
    defs = extract_definitions(sql, query_label=query_label, dialect=dialect)
    return [translate_with_context(d) for d in defs]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Translate SQL business logic to plain English",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=r"""
Examples:
  %(prog)s "SELECT DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) AS los FROM PAT_ENC_HSP e"
  %(prog)s --file query.sql
  %(prog)s --file query.sql --metadata clarity_export.csv
        """,
    )
    parser.add_argument("sql", nargs="?", help="SQL query string")
    parser.add_argument("--file", "-f", help="Read SQL from file")
    parser.add_argument("--stdin", action="store_true", help="Read from stdin")
    parser.add_argument("--metadata", "-m", help="CSV file with column descriptions")
    parser.add_argument("--dialect", "-d", default=None, help="SQL dialect")
    parser.add_argument("--compact", action="store_true", help="Compact JSON")
    parser.add_argument("--text", action="store_true", help="Plain text output instead of JSON")

    args = parser.parse_args()

    if args.metadata:
        metadata.load_csv(args.metadata)

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

    results = translate_query(sql.strip(), dialect=args.dialect)

    if args.text:
        for r in results:
            name = r["name"]
            desc = r["business_description"]
            print(f"\n{name}")
            print(f"  {desc}")
            if "source" in r:
                print(f"  Source: {r['source']}")
            if "conditions" in r:
                for c in r["conditions"]:
                    print(f"  Condition: {c}")
            print(f"  Technical: {r['technical_expression']}")
    else:
        indent = None if args.compact else 2
        print(json.dumps({"definitions": results}, indent=indent))


if __name__ == "__main__":
    main()
