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
    # ``ifs`` isn't pre-translated by the walker (it's a list) -- we iterate
    # over the raw node to get per-branch values/labels and translate the
    # condition/label parts on demand.
    branches = []
    if ctx.registry is None:
        return Translation(english="(CASE -- walker context missing)", category="case")
    from .walker import translate  # local import to avoid cycle
    for if_node in node.args.get("ifs", []) or []:
        cond_t = translate(if_node.args.get("this"), ctx.child("Case.cond")) if if_node.args.get("this") is not None else None
        then_t = translate(if_node.args.get("true"), ctx.child("Case.then")) if if_node.args.get("true") is not None else None
        branches.append((cond_t, then_t))

    parts = []
    for cond_t, then_t in branches:
        label = then_t.english if then_t else "?"
        if subject is not None and cond_t is not None and cond_t.category == "literal":
            # Simple case: "subject = <value> -> label"
            parts.append(f"{cond_t.english} -> {label}")
        elif cond_t is not None:
            # Searched case: "when <condition>, label"
            parts.append(f"when {cond_t.english}, {label}")
        else:
            parts.append(f"? -> {label}")
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
register(name="neq", node_class=exp.NEQ, category="filter", priority=30)(_binop("!=", "inequality"))
register(name="lt", node_class=exp.LT, category="filter", priority=30)(_binop("<", "lt"))
register(name="lte", node_class=exp.LTE, category="filter", priority=30)(_binop("<=", "lte"))
register(name="gt", node_class=exp.GT, category="filter", priority=30)(_binop(">", "gt"))
register(name="gte", node_class=exp.GTE, category="filter", priority=30)(_binop(">=", "gte"))


# ---------------------------------------------------------------------------
# Boolean / null predicates
# ---------------------------------------------------------------------------

@register(name="null_literal", node_class=exp.Null, category="literal", priority=20)
def null_literal(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    return Translation(english="null", category="literal", subcategory="null")


@register(name="is_op", node_class=exp.Is, category="filter", priority=30)
def is_op(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    left = children.get("this")
    right = children.get("expression")
    left_text = left.english if left is not None else "?"
    if right is not None and right.subcategory == "null":
        english = f"{left_text} is null"
    elif right is not None:
        english = f"{left_text} is {right.english}"
    else:
        english = f"{left_text} is ?"
    out = Translation(english=english, category="filter", subcategory="is")
    if left is not None:
        out.absorb(left)
    if right is not None:
        out.absorb(right)
    return out


@register(name="not_op", node_class=exp.Not, category="filter", priority=30)
def not_op(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    inner = children.get("this")
    if inner is None:
        return Translation(english="not ?", category="filter", subcategory="not")
    # Idiomatic rewrites so "NOT (x IS NULL)" reads as "x is not null"
    if " is null" in inner.english:
        english = inner.english.replace(" is null", " is not null")
    elif " between " in inner.english:
        english = inner.english.replace(" between ", " not between ")
    elif " in (" in inner.english:
        english = inner.english.replace(" in (", " not in (")
    else:
        english = f"not {inner.english}"
    out = Translation(english=english, category="filter", subcategory="not")
    out.absorb(inner)
    return out


@register(name="between_op", node_class=exp.Between, category="filter", priority=30)
def between_op(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    what = children.get("this")
    low = children.get("low")
    high = children.get("high")
    what_t = what.english if what is not None else "?"
    low_t = low.english if low is not None else "?"
    high_t = high.english if high is not None else "?"
    out = Translation(
        english=f"{what_t} between {low_t} and {high_t}",
        category="filter", subcategory="between",
    )
    for child in (what, low, high):
        if child is not None:
            out.absorb(child)
    return out


@register(name="in_op", node_class=exp.In, category="filter", priority=30)
def in_op(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    from .walker import translate
    what = children.get("this")
    what_t = what.english if what is not None else "?"
    items = []
    translated_items = []
    for item in node.args.get("expressions") or []:
        t = translate(item, ctx.child("In.item"))
        items.append(t.english)
        translated_items.append(t)
    out = Translation(
        english=f"{what_t} in ({', '.join(items)})",
        category="filter", subcategory="in",
    )
    if what is not None:
        out.absorb(what)
    for t in translated_items:
        out.absorb(t)
    return out


register(name="and_op", node_class=exp.And, category="filter", priority=30)(_binop("and", "and"))
register(name="or_op", node_class=exp.Or, category="filter", priority=30)(_binop("or", "or"))


@register(name="paren", node_class=exp.Paren, category="passthrough", priority=10)
def paren(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    # Transparent -- parentheses carry no semantics; pass the inner through.
    inner = children.get("this")
    if inner is None:
        return Translation(english="()", category="unknown", unknown_nodes=["Paren"])
    return inner


# ---------------------------------------------------------------------------
# Null-handling scalar functions (common in calculated columns)
# ---------------------------------------------------------------------------

@register(name="coalesce", node_class=exp.Coalesce, category="calculated", priority=20)
def coalesce(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    from .walker import translate
    # `this` + `expressions` list hold the fallback chain.
    items = []
    absorbed = []
    first = children.get("this")
    if first is not None:
        items.append(first.english)
        absorbed.append(first)
    for e in node.args.get("expressions") or []:
        t = translate(e, ctx.child("Coalesce.item"))
        items.append(t.english)
        absorbed.append(t)
    english = f"first non-null of ({', '.join(items)})" if items else "first non-null"
    out = Translation(english=english, category="calculated", subcategory="null_handling")
    for t in absorbed:
        out.absorb(t)
    return out


@register(name="nullif", node_class=exp.Nullif, category="calculated", priority=20)
def nullif(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    a = children.get("this")
    b = children.get("expression")
    a_t = a.english if a is not None else "?"
    b_t = b.english if b is not None else "?"
    out = Translation(
        english=f"null when {a_t} = {b_t}, otherwise {a_t}",
        category="calculated", subcategory="null_handling",
    )
    if a is not None:
        out.absorb(a)
    if b is not None:
        out.absorb(b)
    return out


@register(name="distinct_wrap", node_class=exp.Distinct, category="calculated", priority=15)
def distinct_wrap(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    from .walker import translate
    # exp.Distinct wraps its arguments under `expressions` (COUNT DISTINCT x)
    items = []
    absorbed = []
    for e in node.args.get("expressions") or []:
        t = translate(e, ctx.child("Distinct.item"))
        items.append(t.english)
        absorbed.append(t)
    english = f"distinct {', '.join(items)}" if items else "distinct"
    out = Translation(english=english, category="calculated", subcategory="distinct")
    for t in absorbed:
        out.absorb(t)
    return out


# ---------------------------------------------------------------------------
# Subquery / Select plumbing -- keep renders readable instead of Lisp-y
# ---------------------------------------------------------------------------

@register(name="table_ref", node_class=exp.Table, category="passthrough", priority=15)
def table_ref(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    name = node.name or ""
    return Translation(english=name, category="passthrough", base_tables=[name] if name else [])


@register(name="from_clause", node_class=exp.From, category="passthrough", priority=10)
def from_clause(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    inner = children.get("this")
    if inner is None:
        return Translation(english="?", category="passthrough")
    return inner


def _is_correlation_key(node: exp.Expression) -> bool:
    """True if node is a bare `column = column` equi-join -- plumbing, not a row filter."""
    if not isinstance(node, exp.EQ):
        return False
    return isinstance(node.args.get("this"), exp.Column) and isinstance(node.args.get("expression"), exp.Column)


def _strip_correlation_keys(node: exp.Expression):
    """Return a copy of the predicate tree with bare column=column equi-joins removed.
    Returns None if the entire predicate collapses to correlation keys only."""
    if node is None:
        return None
    if _is_correlation_key(node):
        return None
    if isinstance(node, exp.And):
        left = _strip_correlation_keys(node.args.get("this"))
        right = _strip_correlation_keys(node.args.get("expression"))
        if left is None and right is None:
            return None
        if left is None:
            return right
        if right is None:
            return left
        return exp.And(this=left, expression=right)
    if isinstance(node, exp.Paren):
        inner = _strip_correlation_keys(node.args.get("this"))
        if inner is None:
            return None
        return exp.Paren(this=inner)
    return node


@register(name="where_clause", node_class=exp.Where, category="filter", priority=10)
def where_clause(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    raw_inner = node.args.get("this")
    cleaned = _strip_correlation_keys(raw_inner)
    if cleaned is None:
        # Entire WHERE is correlation plumbing -- render as empty.
        return Translation(english="", category="filter", subcategory="where_correlation_only")
    from .walker import translate
    return translate(cleaned, ctx.child("Where.cleaned"))


@register(name="select_expr", node_class=exp.Select, category="calculated", priority=15)
def select_expr(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    from .walker import translate
    raw_exprs = node.args.get("expressions") or []
    expr_ts = []
    for e in raw_exprs:
        expr_ts.append(translate(e, ctx.child("Select.expr")))

    # sqlglot stores FROM under the key "from_" (trailing underscore).
    from_t = children.get("from_") or children.get("from")
    where_t = children.get("where")

    # Suppress placeholder projections (`SELECT 1 FROM ...` / `SELECT * FROM ...`)
    # -- common in EXISTS/UNION wrappers where the projection is noise.
    placeholder_only = (
        from_t is not None
        and len(raw_exprs) == 1
        and (
            (isinstance(raw_exprs[0], exp.Literal) and raw_exprs[0].name == "1")
            or isinstance(raw_exprs[0], exp.Star)
        )
    )

    parts = []
    if not placeholder_only:
        expr_en = ", ".join(t.english for t in expr_ts) or "?"
        parts.append(expr_en)
    if from_t is not None and from_t.english:
        parts.append(f"from {from_t.english}")
    if where_t is not None and where_t.english:
        parts.append(f"where {where_t.english}")
    english = ", ".join(parts) if parts else "?"

    out = Translation(english=english, category="calculated", subcategory="select")
    for t in expr_ts:
        out.absorb(t)
    if from_t is not None:
        out.absorb(from_t)
    if where_t is not None:
        out.absorb(where_t)
    return out


@register(name="subquery", node_class=exp.Subquery, category="calculated", priority=20)
def subquery(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    inner = children.get("this")
    if inner is None:
        return Translation(english="(subquery)", category="calculated", subcategory="subquery")
    out = Translation(english=f"({inner.english})", category="calculated", subcategory="subquery")
    out.absorb(inner)
    return out


@register(name="union", node_class=exp.Union, category="calculated", priority=20)
def union_op(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    left = children.get("this")
    right = children.get("expression")
    left_t = left.english if left is not None else "?"
    right_t = right.english if right is not None else "?"
    out = Translation(
        english=f"either ({left_t}) or ({right_t})",
        category="calculated", subcategory="union",
    )
    if left is not None:
        out.absorb(left)
    if right is not None:
        out.absorb(right)
    return out


@register(name="exists_op", node_class=exp.Exists, category="filter", priority=20)
def exists_op(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    inner = children.get("this")
    if inner is None:
        return Translation(english="at least one row exists", category="filter", subcategory="exists")
    out = Translation(
        english=f"there exists a row where {inner.english}",
        category="filter", subcategory="exists",
    )
    out.absorb(inner)
    return out
