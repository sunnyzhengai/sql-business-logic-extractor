"""Structural patterns: CASE, Window/LAG, binary arithmetic."""

from sqlglot import exp

from .base import Context, Translation
from .registry import register


@register(name="case", node_class=exp.Case, category="case", priority=20)
def case(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    # Simple-case: `this` is the subject (e.g., ADT_PAT_CLASS_C in
    # CASE ADT_PAT_CLASS_C WHEN 1 THEN 'Inpatient' ...). Searched-case:
    # `this` is absent, and each If's condition is a predicate.
    subject = children.get("this")
    default = children.get("default")
    # ``ifs`` isn't pre-translated by the walker (it's a list) — we iterate
    # over the raw node to get per-branch values/labels and translate the
    # condition/label parts on demand.
    branches = []
    if ctx.registry is None:
        return Translation(english="(CASE — walker context missing)", category="case")
    from .walker import translate  # local import to avoid cycle
    for if_node in node.args.get("ifs", []) or []:
        cond_t = translate(if_node.args.get("this"), ctx.child("Case.cond")) if if_node.args.get("this") is not None else None
        then_t = translate(if_node.args.get("true"), ctx.child("Case.then")) if if_node.args.get("true") is not None else None
        branches.append((cond_t, then_t))

    parts = []
    for cond_t, then_t in branches:
        label = then_t.english if then_t else "?"
        if subject is not None and cond_t is not None and cond_t.category == "literal":
            # Simple case: "subject = <value> → label"
            parts.append(f"{cond_t.english} → {label}")
        elif cond_t is not None:
            # Searched case: "when <condition>, label"
            parts.append(f"when {cond_t.english}, {label}")
        else:
            parts.append(f"? → {label}")
    if default is not None:
        parts.append(f"otherwise {default.english}")

    if subject is not None:
        english = f"Mapped from {subject.english}: " + "; ".join(parts)
    else:
        english = "Categorization: " + "; ".join(parts)

    out = Translation(english=english, category="case",
                      subcategory="simple_case" if subject is not None else "searched_case")
    if subject is not None:
        out.absorb(subject)
    if default is not None:
        out.absorb(default)
    for cond_t, then_t in branches:
        if cond_t is not None:
            out.absorb(cond_t)
        if then_t is not None:
            out.absorb(then_t)
    return out


@register(name="window", node_class=exp.Window, category="window", priority=20)
def window(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    inner = children.get("this")
    if inner is None:
        return Translation(english="(window without inner)", category="window", unknown_nodes=["window_no_inner"])

    parts = [inner.english]
    partition_by = node.args.get("partition_by") or []
    order = node.args.get("order")

    from .walker import translate
    if partition_by:
        parts_list = []
        for p in partition_by:
            t = translate(p, ctx.child("Window.partition_by"))
            parts_list.append(t.english)
            inner.absorb(t)
        parts.append(f"for same {', '.join(parts_list)}")
    if order is not None:
        order_parts = []
        for ob in order.args.get("expressions") or []:
            t = translate(ob.args.get("this"), ctx.child("Window.order"))
            order_parts.append(t.english)
            inner.absorb(t)
        if order_parts:
            parts.append(f"ordered by {', '.join(order_parts)}")

    out = Translation(
        english=", ".join(parts),
        category="window",
        subcategory=inner.subcategory or "window",
    )
    out.absorb(inner)
    return out


@register(name="lag", node_class=exp.Lag, category="window", priority=20)
def lag(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    inner = children["this"]
    out = Translation(
        english=f"Previous row's {inner.english}",
        category="window",
        subcategory="lag",
    )
    out.absorb(inner)
    return out


@register(name="lead", node_class=exp.Lead, category="window", priority=20)
def lead(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    inner = children["this"]
    out = Translation(
        english=f"Next row's {inner.english}",
        category="window",
        subcategory="lead",
    )
    out.absorb(inner)
    return out


@register(name="row_number", node_class=exp.RowNumber, category="window", priority=20)
def row_number(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    return Translation(english="row number", category="window", subcategory="row_number")


def _binop(english: str, subcategory: str):
    def tpl(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
        left = children["this"]
        right = children["expression"]
        out = Translation(
            english=f"{left.english} {english} {right.english}",
            category="calculated",
            subcategory=subcategory,
        )
        out.absorb(left)
        out.absorb(right)
        return out
    return tpl


register(name="add", node_class=exp.Add, category="calculated", priority=30)(_binop("plus", "addition"))
register(name="sub", node_class=exp.Sub, category="calculated", priority=30)(_binop("minus", "subtraction"))
register(name="mul", node_class=exp.Mul, category="calculated", priority=30)(_binop("times", "multiplication"))
register(name="div", node_class=exp.Div, category="calculated", priority=30)(_binop("divided by", "division"))


# Comparison operators for CASE-WHEN predicates
register(name="eq", node_class=exp.EQ, category="filter", priority=30)(_binop("=", "equality"))
register(name="neq", node_class=exp.NEQ, category="filter", priority=30)(_binop("≠", "inequality"))
register(name="lt", node_class=exp.LT, category="filter", priority=30)(_binop("<", "lt"))
register(name="lte", node_class=exp.LTE, category="filter", priority=30)(_binop("≤", "lte"))
register(name="gt", node_class=exp.GT, category="filter", priority=30)(_binop(">", "gt"))
register(name="gte", node_class=exp.GTE, category="filter", priority=30)(_binop("≥", "gte"))
