"""Pattern registry with a decorator-based registration API.

Patterns self-register at import time via ``@register(...)``. The registry
indexes by AST node class for fast dispatch; composite patterns (matching
multi-node shapes like ``CAST(SUM(...))/COUNT(*)*100``) register with a
custom ``match`` callable and are tried after class-indexed patterns.
"""

from typing import Callable, Optional, Type

from sqlglot import exp

from .base import Pattern, Template


class PatternRegistry:
    def __init__(self) -> None:
        self._patterns: list[Pattern] = []
        self._by_class: dict[Type[exp.Expression], list[Pattern]] = {}

    def add(self, pattern: Pattern, node_class: Optional[Type[exp.Expression]] = None) -> None:
        self._patterns.append(pattern)
        self._patterns.sort(key=lambda p: p.priority)
        if node_class is not None:
            self._by_class.setdefault(node_class, []).append(pattern)
            self._by_class[node_class].sort(key=lambda p: p.priority)

    def find(self, node: exp.Expression) -> Optional[Pattern]:
        for p in self._by_class.get(type(node), []):
            if p.match(node):
                return p
        class_indexed = {
            id(p) for lst in self._by_class.values() for p in lst
        }
        for p in self._patterns:
            if id(p) in class_indexed:
                continue
            if p.match(node):
                return p
        return None

    def count(self) -> int:
        return len(self._patterns)


registry = PatternRegistry()


def register(
    *,
    name: str,
    node_class: Optional[Type[exp.Expression]] = None,
    match: Optional[Callable[[exp.Expression], bool]] = None,
    priority: int = 100,
    category: str = "unknown",
) -> Callable[[Template], Template]:
    """Decorator that registers a template as a pattern.

    Either ``node_class`` (class-indexed fast path) or ``match`` (custom
    matcher) must be provided. Both may be combined to refine a class
    match with an additional predicate (e.g., DATEDIFF with YEAR unit).
    """
    if node_class is None and match is None:
        raise ValueError("register() requires node_class or match")

    def decorator(template: Template) -> Template:
        if match is None:
            def effective_match(n: exp.Expression, nc=node_class) -> bool:
                return isinstance(n, nc)
        elif node_class is None:
            effective_match = match
        else:
            custom = match
            def effective_match(n: exp.Expression, nc=node_class, cm=custom) -> bool:
                return isinstance(n, nc) and cm(n)

        registry.add(
            Pattern(
                name=name,
                match=effective_match,
                template=template,
                priority=priority,
                category=category,
            ),
            node_class=node_class,
        )
        return template

    return decorator
