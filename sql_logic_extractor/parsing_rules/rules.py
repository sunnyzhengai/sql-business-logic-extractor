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
            r"\s*\((?:\[[^\]]*\]|[^)])*\)"
            # Between `)` and `AS`: previously only `\s*` (whitespace).
            # Yang's MyChart corpus has views where developers put a
            # divider comment / separator line between the closing
            # column-list paren and AS:
            #
            #   CREATE VIEW [name] (cols...)
            #   -- ----------------------
            #   AS
            #
            # With `\s*`, the rule didn't fire because `--` isn't
            # whitespace, and the entire column list (+ AS) leaked into
            # the cleaned SQL body. `[\s\S]*?` is "any character,
            # non-greedy" -- consumes blanks, line comments, block
            # comments, dash separators, anything between `)` and `AS`.
            r"[\s\S]*?"
            r"\bAS\b"
        ),
        replacement=r"\1 AS",
        flags=re.IGNORECASE | re.DOTALL,
    ),
    Rule(
        id="strip_procedure_wrapper",
        description=(
            "Stored procs have a CREATE PROCEDURE wrapper that the view "
            "rules don't fully handle. The strip_ssms_preamble rule trims "
            "everything BEFORE `CREATE PROCEDURE`, leaving the proc-body "
            "intact -- but the proc declaration line ITSELF still contains "
            "parameter declarations (`@p1 INT, @p2 VARCHAR(50) = ...`) "
            "that sqlglot can't parse as part of a SELECT body.\n\n"
            "This rule strips the entire `CREATE [OR ALTER] PROCEDURE name "
            "[(params)] [param_list] AS` opener so what remains is just the "
            "proc body (which is typically a SELECT, possibly wrapped in "
            "BEGIN/END -- handled by the next rule). Sunny's clarification: "
            "her shop's procs are all view-shaped, so the proc body is "
            "always a SELECT or a SELECT-into-temp-table pattern; we don't "
            "have to handle CRUD shapes here.\n\n"
            "Parameter-list pitfall: T-SQL allows table-valued params with "
            "`@param AS TableType READONLY` -- the embedded word `AS` would "
            "trick a naive regex. We anchor on `AS` followed by a SQL body "
            "keyword (BEGIN, SELECT, WITH, DECLARE, RETURN, INSERT, EXEC, "
            "etc.) via lookahead, so the proc body's `AS` is found, not a "
            "parameter type's `AS`."
        ),
        pattern=(
            r"\bCREATE\s+(?:OR\s+ALTER\s+)?"
            r"(?:PROCEDURE|PROC|FUNCTION|TRIGGER)\s+"
            r"(?:\[[^\]]+\]|\w+)"
            r"(?:\.(?:\[[^\]]+\]|\w+))?"
            r"[\s\S]*?"
            r"\bAS\s+"
            r"(?=BEGIN\b|SELECT\b|WITH\b|DECLARE\b|RETURN\b|"
            r"INSERT\b|UPDATE\b|DELETE\b|MERGE\b|EXEC\b)"
        ),
        replacement="",
        flags=re.IGNORECASE | re.DOTALL,
    ),
    Rule(
        id="strip_proc_begin_end_wrapper",
        description=(
            "After strip_procedure_wrapper, a proc body is often wrapped "
            "in BEGIN ... END. Those keywords confuse sqlglot's SELECT "
            "parser -- it falls back to a 'Command' AST node and loses "
            "all structure. Strip the OUTERMOST BEGIN at the start and "
            "the LAST `END` at the end of the cleaned SQL.\n\n"
            "Why not also strip nested BEGIN/END blocks? T-SQL uses "
            "BEGIN/END inside CASE expressions (`CASE WHEN ... END`), "
            "TRY/CATCH blocks, IF/ELSE blocks, etc. Removing them all "
            "would break those constructs. We only touch the outermost "
            "wrapper -- the proc body itself -- which matches Sunny's "
            "all-view-shaped procs."
        ),
        # Anchor BEGIN at the start of remaining text and END at the
        # very end (with optional trailing semicolon / GO).
        pattern=(
            r"\A\s*BEGIN\b\s*([\s\S]*?)\s*\bEND\s*;?\s*(?:\bGO\b\s*)?\Z"
        ),
        replacement=r"\1",
        flags=re.IGNORECASE,
    ),
    Rule(
        id="rewrite_odbc_escape_clause",
        description=(
            "Some SSMS/ODBC-generated T-SQL writes the LIKE escape clause "
            "in ODBC canonical form `{escape '<char>'}` instead of the "
            "native `ESCAPE '<char>'`. sqlglot can't parse the brace form "
            "and fails with `ParseError: Expected }` -- and because the "
            "escape char is often a backslash (`{escape '\\'}`, common when "
            "the LIKE pattern uses `\\[` to match a literal bracket), the "
            "failure lands deep inside a CASE/CONVERT expression.\n\n"
            "Rewrite `{escape '<char>'}` -> `ESCAPE '<char>'`, which sqlglot "
            "parses fine (the backslash string parses correctly in tsql once "
            "it's a normal ESCAPE clause). Only the `escape` ODBC sequence is "
            "touched; other ODBC sequences (`{d ...}`, `{ts ...}`, `{fn ...}`) "
            "are left alone. Seen in views_cookrpt/V_CCHP_MEMBER_APPEALS_FLATFILE."
        ),
        pattern=r"\{\s*escape\s+('[^']*')\s*\}",
        replacement=r"ESCAPE \1",
        flags=re.IGNORECASE,
    ),
    Rule(
        id="strip_set_transaction_isolation",
        description=(
            "Procs/SSMS exports often open with "
            "`SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;` (or another "
            "level). sqlglot can't parse it and fails with "
            "`ParseError: Unknown option ISOLATION`. It's a pure session "
            "setting with no lineage content (like SET NOCOUNT ON, which "
            "DOES parse), so we drop the whole line. Matches the 5 standard "
            "levels (READ UNCOMMITTED/COMMITTED, REPEATABLE READ, "
            "SERIALIZABLE, SNAPSHOT). Seen inside a proc body, so it can "
            "survive the wrapper strip and reach sqlglot."
        ),
        pattern=(
            r"^[ \t]*SET\s+TRANSACTION\s+ISOLATION\s+LEVEL\s+"
            r"(?:READ\s+UNCOMMITTED|READ\s+COMMITTED|REPEATABLE\s+READ"
            r"|SERIALIZABLE|SNAPSHOT)\b[ \t]*;?[ \t]*\r?\n?"
        ),
        replacement="",
        flags=re.IGNORECASE | re.MULTILINE,
    ),
]
