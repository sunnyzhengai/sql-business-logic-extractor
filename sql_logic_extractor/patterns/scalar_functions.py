"""Scalar function patterns: date functions, casts, string wrappers.

Includes unwrappers for tsql-dialect normalization artifacts (TIME_STR_TO_TIME
and similar): these wrap CURRENT_TIMESTAMP and date columns and carry no
semantic content worth surfacing, so we pass through the inner translation.
"""

from sqlglot import exp

from .base import Context, Translation
from .registry import register


@register(name="datediff", node_class=exp.DateDiff, category="calculated", priority=20)
def datediff(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    # tsql-dialect parse: `this` = end date, `expression` = start date, `unit` = interval (a Var).
    unit_node = node.args.get("unit")
    unit_name = unit_node.name if unit_node is not None else "UNIT"
    unit_word = {"DAY": "days", "YEAR": "years", "MONTH": "months",
                 "HOUR": "hours", "MINUTE": "minutes", "SECOND": "seconds",
                 "WEEK": "weeks"}.get(unit_name.upper(), unit_name.lower())
    end = children["this"]
    start = children["expression"]
    out = Translation(
        english=f"Number of {unit_word} between {start.english} and {end.english}",
        category="calculated",
        subcategory="date_difference",
    )
    out.absorb(start)
    out.absorb(end)
    return out


@register(name="current_timestamp", node_class=exp.CurrentTimestamp, category="calculated", priority=15)
def current_timestamp(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    return Translation(
        english="today",
        category="calculated",
        subcategory="current_timestamp",
    )


@register(name="current_date", node_class=exp.CurrentDate, category="calculated", priority=15)
def current_date(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    return Translation(
        english="today",
        category="calculated",
        subcategory="current_date",
    )


@register(name="cast", node_class=exp.Cast, category="passthrough", priority=20)
def cast(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    inner = children["this"]
    # Transparent: CAST is a type conversion, the business meaning rides the
    # underlying value. Composite patterns (percentage) inspect the raw node
    # and attach their own semantics on top.
    return inner


@register(name="tsql_time_to_time", node_class=exp.TimeStrToTime, priority=10)
def tsql_time_to_time(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    inner = children.get("this")
    if inner is not None:
        return inner
    return Translation(english="(date/time)", category="unknown", unknown_nodes=["TimeStrToTime"])


@register(name="tsql_time_to_str", node_class=exp.TimeStrToDate, priority=10)
def tsql_time_to_str(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    inner = children.get("this")
    if inner is not None:
        return inner
    return Translation(english="(date)", category="unknown", unknown_nodes=["TimeStrToDate"])


@register(name="var_literal", node_class=exp.Var, category="literal", priority=20)
def var_literal(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    # sqlglot's Var holds unit keywords like YEAR, DAY, MONTH in DateDiff.
    return Translation(english=node.name, category="literal", subcategory="var")
