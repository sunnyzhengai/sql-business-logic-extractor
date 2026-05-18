"""Bank of candidate transforms tried on unknown_failure views.

Each entry is a (rule_id_candidate, description, transform_fn). When a
transform changes the SQL AND the result parses, we propose that
transform as a new parsing rule.

Order matters: cheaper / more specific transforms first, so the
proposal cites the narrowest fix that worked.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class Hypothesis:
    rule_id: str             # candidate rule id if this hypothesis works
    description: str         # what construct it targets
    transform: Callable[[str], str]
    suggested_pattern: str   # raw regex source -- humans can copy into rules.py
    suggested_flags: str     # printable flag combo, e.g. "re.IGNORECASE"


_WS_RE = re.compile(r"\s+")


def _collapse_multiline_brackets(sql: str) -> str:
    def repl(m: re.Match) -> str:
        return "[" + _WS_RE.sub(" ", m.group(1)).strip() + "]"
    return re.sub(r"\[([^\[\]]*)\]", repl, sql, flags=re.DOTALL)


def _strip_table_hints(sql: str) -> str:
    return re.sub(
        r"\bWITH\s*\(\s*(?:NOLOCK|HOLDLOCK|READUNCOMMITTED|"
        r"INDEX\s*\([^)]+\)|FORCESEEK|UPDLOCK|TABLOCK|TABLOCKX|XLOCK)"
        r"(?:\s*,\s*\w+(?:\s*\([^)]+\))?)*\s*\)",
        "", sql, flags=re.IGNORECASE,
    )


def _strip_option_clause(sql: str) -> str:
    return re.sub(r"\bOPTION\s*\([^)]*\)\s*;?", "", sql, flags=re.IGNORECASE)


def _strip_print_statements(sql: str) -> str:
    return re.sub(r"^\s*PRINT\s+[^;]+;?\s*$", "", sql,
                    flags=re.IGNORECASE | re.MULTILINE)


HYPOTHESES: list[Hypothesis] = [
    Hypothesis(
        rule_id="collapse_multiline_bracket_identifiers",
        description=(
            "Some BI tools generate views with newlines inside "
            "[bracket-quoted] identifiers. SQL Server treats internal "
            "whitespace as whitespace-equivalent; collapsing to a single "
            "space lets sqlglot parse without changing semantics."
        ),
        transform=_collapse_multiline_brackets,
        suggested_pattern=r"\[([^\[\]]*)\]",
        suggested_flags="re.DOTALL",
    ),
    Hypothesis(
        rule_id="strip_table_hints",
        description=(
            "T-SQL `WITH (NOLOCK)` / `WITH (HOLDLOCK)` / `WITH (INDEX(...))` "
            "table hints affect locking but not column lineage. Strip "
            "them so sqlglot doesn't trip on hint syntax variants."
        ),
        transform=_strip_table_hints,
        suggested_pattern=(
            r"\bWITH\s*\(\s*(?:NOLOCK|HOLDLOCK|READUNCOMMITTED|INDEX\s*\([^)]+\)|"
            r"FORCESEEK|UPDLOCK|TABLOCK|TABLOCKX|XLOCK)"
            r"(?:\s*,\s*\w+(?:\s*\([^)]+\))?)*\s*\)"
        ),
        suggested_flags="re.IGNORECASE",
    ),
    Hypothesis(
        rule_id="strip_option_clause",
        description=(
            "T-SQL `OPTION (MAXDOP 1)` / `OPTION (FORCE ORDER)` query "
            "hints affect the optimizer but not column extraction. Safe "
            "to strip."
        ),
        transform=_strip_option_clause,
        suggested_pattern=r"\bOPTION\s*\([^)]*\)\s*;?",
        suggested_flags="re.IGNORECASE",
    ),
    Hypothesis(
        rule_id="strip_print_statements",
        description=(
            "PRINT '...' debug statements sometimes leak into scripted "
            "views. They aren't queries; strip them."
        ),
        transform=_strip_print_statements,
        suggested_pattern=r"^\s*PRINT\s+[^;]+;?\s*$",
        suggested_flags="re.IGNORECASE | re.MULTILINE",
    ),
]
