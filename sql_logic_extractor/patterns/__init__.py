"""Pattern library for offline SQL-to-English translation.

Implements the recursive translation principle
(wiki/concepts/recursive-translation-principle.md): translate every SQL
construct by recursion to raw column references, with a growing library
of pattern templates keyed by AST shape. Unknown patterns produce
structural decomposition plus a governance signal — never opaque
fallbacks.

Public API:

    from sql_logic_extractor.patterns import (
        Translation, Context, Pattern,
        PatternRegistry, registry, register,
    )

    @register(name="datediff_year", node_class=exp.DateDiff,
              match=lambda n: _unit(n) == "YEAR", category="calculated")
    def datediff_year(ctx, args):
        return Translation(
            english=f"Number of years between {args['expression'].english} "
                    f"and {args['expression_'].english}",
            category="calculated",
            subcategory="date_difference",
        )

Templates are registered at import time. The recursive walker (built in
the next step) looks up patterns via ``registry.find(node)``.
"""

from .base import Context, Pattern, Template, Translation
from .registry import PatternRegistry, register, registry

# Importing these modules registers their patterns at import time.
from . import columns, aggregates, scalar_functions, structural  # noqa: F401
from .walker import translate

__all__ = [
    "Context",
    "Pattern",
    "PatternRegistry",
    "Template",
    "Translation",
    "register",
    "registry",
    "translate",
]
