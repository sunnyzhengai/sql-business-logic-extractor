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
        id="strip_ssms_preamble",
        description=(
            "SSMS Generate-Scripts prefaces every exported view/proc with "
            "a preamble of `USE [db]`, `GO`, `SET ANSI_NULLS ...`, "
            "`/****** Object: ... ******/`, and similar boilerplate. The "
            "previous approach was line-by-line keyword matching against "
            "a hardcoded list of SET options -- whack-a-mole, miss one "
            "and the whole view stops parsing with a misleading "
            "'invalid expression' error.\n\n"
            "This rule trims everything before the first "
            "`CREATE [OR ALTER|REPLACE] (VIEW|PROCEDURE|PROC|FUNCTION|TRIGGER)` "
            "in one regex pass. No keyword enumeration needed; any "
            "preamble of any shape just disappears. If no CREATE statement "
            "is found (unusual: snippet file, dynamic SQL fragment), the "
            "rule does nothing.\n\n"
            "Pairs with a fix in `preprocess_ssms`'s state machine that "
            "skips a bare standalone `AS` line when it appears right after "
            "the matched CREATE wrapper (the SSMS-formats-long-view-names "
            "case). Without that fix the state machine leaks the bare AS "
            "into the body and sqlglot fails with 'Required keyword: this "
            "missing for class Alias' at the next WITH/SELECT.\n\n"
            "Object metadata (schema, name, script date) is extracted "
            "by `_extract_object_header` in resolve.py BEFORE this rule "
            "runs, so the useful pieces survive the strip. Author / "
            "Description / Revision history in free-form comments is "
            "lost as a tradeoff for parse robustness; can be reinstated "
            "as a separate pre-extraction step if needed."
        ),
        # \A anchors to absolute start-of-string; .*? is non-greedy so
        # we don't accidentally swallow a later CREATE in a comment.
        # The lookahead finds CREATE without consuming it.
        pattern=(
            r"\A.*?"
            r"(?=CREATE\s+(?:OR\s+(?:ALTER|REPLACE)\s+)?"
            r"(?:VIEW|PROCEDURE|PROC|FUNCTION|TRIGGER)\b)"
        ),
        replacement="",
        flags=re.IGNORECASE | re.DOTALL,
    ),
    Rule(
        id="create_view_explicit_column_list",
        description=(
            "T-SQL allows `CREATE VIEW name (col1, col2, ...) AS SELECT ...` "
            "where the parenthesized list explicitly renames the SELECT's "
            "outputs. sqlglot doesn't parse this form. Strip the column "
            "list and leave `CREATE VIEW name AS` -- the SELECT body is "
            "what our tools need (source columns), so we lose no useful "
            "info.\n\n"
            "Identifier matching: `[bracket-quoted]` allows ANY non-`]` "
            "character inside (spaces, slashes, dots in T-SQL identifiers); "
            "bare identifiers stay restricted to \\w. This was the cause of "
            "the first 'Required keyword: this missing for Alias' error "
            "cluster -- views named `[Schema With Space].[Name]` previously "
            "didn't match because the regex only allowed word characters "
            "inside brackets."
        ),
        # Captures: CREATE [OR ALTER] VIEW <schema?.name>  (col list)  AS
        # The schema/name allows either a bracket-quoted identifier
        # (anything except `]`) or a bare word identifier (\w+).
        # Column-list matcher is non-trivial: T-SQL allows ANY characters
        # inside `[bracket-quoted]` column names INCLUDING `)`. So a
        # naive `[^)]*` short-circuits on a column like `[Net (Gross)]`.
        # The alternation `(?:\[[^\]]*\]|[^)])*` consumes either a whole
        # bracket-quoted run (with anything inside) OR one non-`)` char
        # at a time -- correctly skipping over parens nested in column
        # names.
        pattern=(
            r"((?:CREATE\s+(?:OR\s+ALTER\s+)?|ALTER\s+)VIEW\s+"
            r"(?:\[[^\]]+\]|\w+)"
            r"(?:\.(?:\[[^\]]+\]|\w+))?)"
            r"\s*\((?:\[[^\]]*\]|[^)])*\)\s*"
            r"\bAS\b"
        ),
        replacement=r"\1 AS",
        flags=re.IGNORECASE | re.DOTALL,
    ),
]
