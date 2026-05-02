"""Rule dataclass + ordered-application helper.

A Rule is a declarative regex transformation: pattern + replacement +
flags. It's "fired" when the substitution actually changes the input
(>= 1 match). Apply order matters -- later rules see SQL that earlier
rules already transformed.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class Rule:
    """One regex-based preprocessing rule.

    Attributes:
        id: stable, lowercase_with_underscores identifier. Used in the
            preflight CSV's `rules_fired` column. MUST match the
            fixture directory name under fixtures/.
        description: human-readable explanation of what construct the
            rule fixes. Surfaces in observability logs.
        pattern: regex (raw string).
        replacement: regex replacement (raw string, supports backrefs).
        flags: bitmask of re.* flags. Default 0 (no flags).
    """
    id: str
    description: str
    pattern: str
    replacement: str
    flags: int = 0

    def apply(self, sql: str) -> tuple[str, int]:
        """Run the substitution. Returns (new_sql, n_substitutions).

        n_substitutions == 0 means the rule did NOT fire (input was
        already clean of this construct).
        """
        return re.subn(self.pattern, self.replacement, sql, flags=self.flags)


def apply_all(sql: str, rules: Iterable[Rule] | None = None) -> tuple[str, list[str]]:
    """Apply rules in order. Returns (clean_sql, fired_rule_ids).

    `fired_rule_ids` is the ordered list of rule ids that produced >= 1
    substitution. Useful for the preflight tool's per-view audit and
    for debugging which rules are actually doing work on a given corpus.
    """
    if rules is None:
        # Lazy import to avoid circular imports at module load.
        from .rules import PARSING_RULES
        rules = PARSING_RULES

    fired: list[str] = []
    for rule in rules:
        sql, n = rule.apply(sql)
        if n > 0:
            fired.append(rule.id)
    return sql, fired
