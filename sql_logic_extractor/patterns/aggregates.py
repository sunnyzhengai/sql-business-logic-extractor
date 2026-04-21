"""Aggregate function patterns: COUNT, SUM, AVG, MIN, MAX."""

from sqlglot import exp

from .base import Context, Translation
from .registry import register


@register(name="count", node_class=exp.Count, category="aggregate", priority=20)
def count(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    inner = children.get("this")
    if inner is None or inner.subcategory == "star":
        out = Translation(english="Count of rows", category="aggregate", subcategory="count_star")
    else:
        out = Translation(
            english=f"Count of {inner.english}",
            category="aggregate",
            subcategory="count",
        )
    if inner is not None:
        out.absorb(inner)
    return out


@register(name="sum", node_class=exp.Sum, category="aggregate", priority=20)
def sum_agg(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    inner = children["this"]
    out = Translation(
        english=f"Sum of {inner.english}",
        category="aggregate",
        subcategory="sum",
    )
    out.absorb(inner)
    return out


@register(name="avg", node_class=exp.Avg, category="aggregate", priority=20)
def avg(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    inner = children["this"]
    out = Translation(
        english=f"Average of {inner.english}",
        category="aggregate",
        subcategory="avg",
    )
    out.absorb(inner)
    return out


@register(name="min", node_class=exp.Min, category="aggregate", priority=20)
def min_agg(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    inner = children["this"]
    out = Translation(
        english=f"Minimum of {inner.english}",
        category="aggregate",
        subcategory="min",
    )
    out.absorb(inner)
    return out


@register(name="max", node_class=exp.Max, category="aggregate", priority=20)
def max_agg(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    inner = children["this"]
    out = Translation(
        english=f"Maximum of {inner.english}",
        category="aggregate",
        subcategory="max",
    )
    out.absorb(inner)
    return out
