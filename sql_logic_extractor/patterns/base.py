"""Core data model for the pattern library.

Three concepts:

- ``Translation``: result of translating one AST node. Carries the English
  text plus metadata (base columns/tables, filters, unknown-node records)
  so parent templates can compose output and governance signals propagate
  upward through the recursion.
- ``Context``: threaded walk state. Schema for base-case lookup, CTE alias
  map for disambiguation, ancestor path for context-dependent decisions,
  and a back-pointer to the registry for recursive calls.
- ``Pattern``: one registry entry = matcher + template. Patterns are tried
  in priority order; first match wins.

See wiki/concepts/recursive-translation-principle.md for the why.
"""

from dataclasses import dataclass, field
from typing import Callable, Optional, TYPE_CHECKING

from sqlglot import exp

if TYPE_CHECKING:
    from .registry import PatternRegistry


@dataclass
class Translation:
    english: str
    category: str = "unknown"
    subcategory: Optional[str] = None
    base_columns: list[str] = field(default_factory=list)
    base_tables: list[str] = field(default_factory=list)
    business_filters: list[str] = field(default_factory=list)
    technical_filters: list[str] = field(default_factory=list)
    unknown_nodes: list[str] = field(default_factory=list)
    unknown_columns: list[str] = field(default_factory=list)

    def absorb(self, child: "Translation") -> None:
        """Roll up a child's metadata into this translation."""
        for col in child.base_columns:
            if col not in self.base_columns:
                self.base_columns.append(col)
        for tbl in child.base_tables:
            if tbl not in self.base_tables:
                self.base_tables.append(tbl)
        self.business_filters.extend(child.business_filters)
        self.technical_filters.extend(child.technical_filters)
        self.unknown_nodes.extend(child.unknown_nodes)
        self.unknown_columns.extend(child.unknown_columns)


@dataclass
class Context:
    schema: dict
    alias_map: dict[str, str] = field(default_factory=dict)
    path: list[str] = field(default_factory=list)
    registry: Optional["PatternRegistry"] = None

    def child(self, node_type: str) -> "Context":
        return Context(
            schema=self.schema,
            alias_map=self.alias_map,
            path=self.path + [node_type],
            registry=self.registry,
        )


# Template signature: (context, args_dict) -> Translation
# args_dict maps sqlglot child-field name ("this", "expression", "unit", ...)
# to the already-translated child Translation.
Template = Callable[[Context, dict[str, "Translation"]], "Translation"]


@dataclass
class Pattern:
    name: str
    match: Callable[[exp.Expression], bool]
    template: Template
    priority: int = 100
    category: str = "unknown"
