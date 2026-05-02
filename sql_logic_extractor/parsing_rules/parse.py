"""Combined parse pipeline: text rules + parse + AST rules.

Single entry point that runs the full preprocessing stack:

    raw SQL  --(text Rules)-->  cleaner SQL
             --(sqlglot)-->     parsed tree
             --(AstRules)-->    normalized tree

Use this anywhere you would have called `sqlglot.parse_one(sql, ...)`
directly. The benefit: T-SQL idioms that need text-level coaxing
(unparseable constructs) AND tree-level normalization (table hints,
optimizer hints) are both handled, and the rule registries are the
single source of truth for what gets transformed.

Returns the parsed tree plus an audit of which rules fired -- useful
for the preflight tool and any "explain why this view was modified"
report.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp

from .ast_rule import apply_all_ast
from .rule import apply_all


@dataclass(frozen=True)
class ParseResult:
    tree: exp.Expression
    text_rules_fired: list[str] = field(default_factory=list)
    ast_rules_fired: list[str] = field(default_factory=list)


def parse_with_rules(sql: str, *, dialect: str = "tsql") -> ParseResult:
    """Run the full preprocessing pipeline and return (tree, rule audit).

    Raises whatever sqlglot.parse_one raises if the post-text-rules SQL
    still doesn't parse -- callers can catch and route to the auto_propose
    pipeline to surface the failing construct as a candidate new rule.
    """
    cleaned, text_fired = apply_all(sql)
    tree = sqlglot.parse_one(cleaned, dialect=dialect)
    tree, ast_fired = apply_all_ast(tree)
    return ParseResult(tree=tree, text_rules_fired=text_fired,
                        ast_rules_fired=ast_fired)
