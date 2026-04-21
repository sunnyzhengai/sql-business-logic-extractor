"""Recursive translation walker.

Given a sqlglot AST node and a Context, returns a Translation by looking
up a pattern in the registry and invoking its template with
pre-translated children. Unknown nodes fall back to structural
decomposition — never opaque placeholders — and register themselves in
the result's unknown_nodes list.

Import the specific pattern modules (``columns``, ``aggregates``,
``scalar_functions``, ``structural``) before calling ``translate`` so
their ``@register`` decorators run.
"""

from sqlglot import exp

from .base import Context, Translation
from .registry import registry as default_registry


def translate(node: exp.Expression, ctx: Context) -> Translation:
    """Translate a single AST node recursively."""
    if node is None:
        return Translation(english="(null)", category="unknown")

    if ctx.registry is None:
        ctx.registry = default_registry

    pattern = ctx.registry.find(node)

    if pattern is None:
        return _structural_fallback(node, ctx)

    # Pre-translate direct children keyed by sqlglot arg name, skipping
    # list-valued args (the template will walk them itself if it cares,
    # as CASE and Window do).
    children: dict[str, Translation] = {}
    for key, val in node.args.items():
        if val is None:
            continue
        if isinstance(val, exp.Expression):
            children[key] = translate(val, ctx.child(type(node).__name__ + "." + key))
    return pattern.template(ctx, node, children)


def _structural_fallback(node: exp.Expression, ctx: Context) -> Translation:
    """Unknown pattern: produce structural decomposition + register the node type."""
    node_type = type(node).__name__
    child_translations: list[Translation] = []
    child_labels: list[str] = []

    for key, val in node.args.items():
        if isinstance(val, exp.Expression):
            t = translate(val, ctx.child(f"{node_type}.{key}"))
            child_translations.append(t)
            child_labels.append(t.english)
        elif isinstance(val, list):
            for i, item in enumerate(val):
                if isinstance(item, exp.Expression):
                    t = translate(item, ctx.child(f"{node_type}.{key}[{i}]"))
                    child_translations.append(t)
                    child_labels.append(t.english)

    english = (
        f"{node_type} of [{', '.join(child_labels)}]" if child_labels else f"{node_type}"
    )
    out = Translation(english=english, category="unknown",
                      unknown_nodes=[node_type])
    for t in child_translations:
        out.absorb(t)
    return out
