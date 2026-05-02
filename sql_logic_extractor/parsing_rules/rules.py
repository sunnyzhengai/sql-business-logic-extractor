"""The ordered parsing-rule registry.

Each entry is one T-SQL construct sqlglot can't parse natively. The
registry is the single source of truth -- new rules go HERE, with a
matching fixture under fixtures/<rule_id>/. The fixture-driven test
in tests/test_parsing_rules.py validates every entry automatically.

Rule ordering matters: rules higher in the list run first, and later
rules see SQL the earlier ones have already transformed. Group related
rules together and document the order constraint in the description.
"""

import re

from .rule import Rule


PARSING_RULES: list[Rule] = [
    Rule(
        id="create_view_explicit_column_list",
        description=(
            "T-SQL allows `CREATE VIEW name (col1, col2, ...) AS SELECT ...` "
            "where the parenthesized list explicitly renames the SELECT's "
            "outputs. sqlglot doesn't parse this form. Strip the column "
            "list and leave `CREATE VIEW name AS` -- the SELECT body is "
            "what our tools need (source columns), so we lose no useful "
            "info."
        ),
        # Captures: CREATE [OR ALTER] VIEW <[schema].<name>>  (col list)  AS
        # Replaces with: CREATE VIEW <name> AS  (single line, ready for
        # the line-by-line preprocessor's existing CREATE VIEW handler).
        pattern=(
            r"((?:CREATE\s+(?:OR\s+ALTER\s+)?|ALTER\s+)VIEW\s+"
            r"(?:\[?[\w]+\]?\.)?\[?[\w]+\]?)"
            r"\s*\([^)]*\)\s*"
            r"\bAS\b"
        ),
        replacement=r"\1 AS",
        flags=re.IGNORECASE | re.DOTALL,
    ),
]
