"""AST-level rule registry -- transforms run on the parsed tree.

Companion to rule.py (text-level regex rules). The split:

  Text-level Rule  ->  applied BEFORE parse, coaxes raw SQL into a form
                       sqlglot can grammar-parse. Used when the failure
                       is at the parser level.
  AstRule          ->  applied AFTER parse, walks the tree and rewrites
                       nodes. Used when the SQL already parses but the
                       tree contains noise (table hints, query options,
                       parser quirks) we want to normalize away.

AST rules are more robust than text rules: they don't care about
whitespace, comment placement, or alias name length. Use these for
anything semantic; reserve text-level Rules strictly for parser-level
workarounds.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable

from sqlglot import exp


@dataclass(frozen=True)
class AstRule:
    """One declarative AST transform.

    Attributes:
        id: stable lowercase_with_underscores identifier. Matches the
            fixture directory name under fixtures/.
        description: human-readable explanation of what the rule does.
        transform: function (Expression -> Expression). Author chooses
            implementation -- sqlglot's `tree.transform(visitor)`,
            manual `node.set(...)` mutations, or full tree rewrites.
    """
    id: str
    description: str
    transform: Callable[[exp.Expression], exp.Expression]

    def apply(self, tree: exp.Expression) -> exp.Expression:
        return self.transform(tree)


def apply_all_ast(tree: exp.Expression,
                  rules: Iterable[AstRule] | None = None) -> tuple[exp.Expression, list[str]]:
    """Apply AST rules in order. Returns (transformed_tree, fired_rule_ids).

    `fired_rule_ids` lists the rules whose transform produced ANY change
    to the SQL emission of the tree. Useful for the preflight tool's
    audit -- shows which AST rules are actually doing work on a corpus.
    """
    if rules is None:
        from .ast_rules import AST_RULES
        rules = AST_RULES

    fired: list[str] = []
    for rule in rules:
        before_sql = tree.sql()
        tree = rule.apply(tree)
        after_sql = tree.sql()
        if before_sql != after_sql:
            fired.append(rule.id)
    return tree, fired
