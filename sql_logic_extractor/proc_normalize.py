#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Normalize a CTE-shaped stored procedure into a single view-shaped SELECT.

A large class of "reporting" stored procs are semantically identical to a
view: they stage intermediate results in temp tables and read them back,
never mutating those temp tables after creation. The only thing that keeps
them out of the view-shaped extractor (Phase C) is *syntax* -- the staging
lives in separate top-level statements joined by a name, instead of in a
single statement's WITH clause:

    -- proc form (what we get)
    CREATE PROCEDURE [rpt].[Foo] AS
    BEGIN
        IF OBJECT_ID('tempdb..#stage') IS NOT NULL DROP TABLE #stage;
        SELECT a, b INTO #stage FROM base WHERE x > 0;
        SELECT a, SUM(b) FROM #stage GROUP BY a;
    END

    -- view form (what Phase C can extract)
    CREATE VIEW [rpt].[Foo] AS
    WITH stage AS (SELECT a, b FROM base WHERE x > 0)
    SELECT a, SUM(b) FROM stage GROUP BY a;

`select_into_to_cte` performs exactly that rewrite: each `SELECT ... INTO
#tmp` becomes a CTE named `tmp`, references to `#tmp` are rewritten to the
CTE name, and the single terminal SELECT becomes the main query with the
CTEs prepended in source (dependency) order.

The rewrite is only VALID when the proc obeys the CTE-equivalence
constraint: every temp table is defined exactly once via `SELECT ... INTO`
and never mutated afterwards (no INSERT/UPDATE/MERGE/DELETE into it), and
there is exactly one terminal SELECT. When that constraint is violated the
proc is genuinely not view-shaped (it's an ETL or multi-output proc), and
the function raises `ProcNotViewShaped` so the caller can categorize it
rather than emit a silently-wrong view.

NOTE on table variables: only `#temp` tables are handled. `@TableVar`
staging is rarer and parses differently; it is not rewritten here.
"""

from __future__ import annotations

import re

import sqlglot
from sqlglot import exp
from sqlglot.tokens import TokenType

# T-SQL is the only dialect this transform targets -- temp tables and
# SELECT ... INTO are T-SQL constructs. Exposed as a default arg so callers
# stay consistent with the rest of the pipeline.
_DEFAULT_DIALECT = "tsql"


class ProcNotViewShaped(Exception):
    """Raised when a proc cannot be safely rewritten to one view-shaped SELECT.

    `reason` is a short machine-readable code (e.g. ``"multiple_terminal_selects"``)
    so callers can bucket failures; `detail` carries the offending fragment
    for human-readable logging. The two together make the exception useful
    both as a control-flow signal and as a diagnostic.
    """

    def __init__(self, reason: str, detail: str = "") -> None:
        self.reason = reason
        self.detail = detail
        msg = reason if not detail else f"{reason}: {detail}"
        super().__init__(msg)


# ---- wrapper / guard stripping (regex, pre-parse) ------------------------
#
# sqlglot cannot parse `CREATE PROCEDURE ... AS BEGIN ... END` reliably, and
# it chokes outright on `IF OBJECT_ID(...) IS NOT NULL DROP TABLE #x`
# ("Unsupported If block syntax") -- a single un-parseable IF poisons the
# WHOLE multi-statement parse. So we peel the proc wrapper and remove the
# temp-table guards textually BEFORE handing the body to sqlglot.parse.

# CREATE [OR ALTER] PROC[EDURE] [schema].[name] ... AS <body>
# Captures the object name (group 1) up to the first standalone AS keyword.
_PROC_HEADER_RE = re.compile(
    r"\bCREATE\s+(?:OR\s+ALTER\s+)?PROC(?:EDURE)?\s+"
    r"(?P<name>(?:\[[^\]]+\]|\w+)(?:\.(?:\[[^\]]+\]|\w+))?)"
    r".*?\bAS\b",
    re.IGNORECASE | re.DOTALL,
)

# `IF OBJECT_ID(...) IS NOT NULL DROP TABLE #x` -- the standard temp-table
# guard. Also matches the `BEGIN ... END` wrapped variant of the same.
_TEMP_GUARD_RE = re.compile(
    r"IF\s+OBJECT_ID\s*\([^)]*\)\s+IS\s+NOT\s+NULL\s+"
    r"(?:BEGIN\s+)?DROP\s+TABLE\s+#\w+\s*;?(?:\s+END)?\s*;?",
    re.IGNORECASE,
)

# Bare `DROP TABLE #x` (guard already passed, or unconditional cleanup).
_BARE_DROP_RE = re.compile(
    r"\bDROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?#\w+\s*;?",
    re.IGNORECASE,
)


def _strip_bare_header(sql: str) -> str:
    """Strip bare-text headers that sit above the CREATE PROCEDURE line.

    Many SQL files have un-commented headers like:

        TITLE: Patient Referral Report
        AUTHOR: John Smith
        ========================
        CREATE PROCEDURE [rpt].[Foo] AS ...

    These are not valid SQL and break the parser. This function removes
    everything before the first SQL keyword (CREATE, ALTER, SELECT, WITH,
    DECLARE, SET) or SQL comment (-- or /*).

    Lines that are purely separator characters (===, ---, ***) are also
    removed.
    """
    lines = sql.split("\n")
    # Find the first line that looks like actual SQL or a SQL comment
    _SQL_START_RE = re.compile(
        r"^\s*("
        r"CREATE\b|ALTER\b|SELECT\b|WITH\b|DECLARE\b|SET\b|INSERT\b|"
        r"UPDATE\b|DELETE\b|MERGE\b|EXEC\b|USE\b|IF\b|BEGIN\b|"
        r"--|/\*"
        r")",
        re.IGNORECASE,
    )
    for i, line in enumerate(lines):
        if _SQL_START_RE.match(line):
            if i == 0:
                return sql  # no header to strip
            return "\n".join(lines[i:])
    # No SQL found at all — return as-is, let downstream handle it
    return sql


def _strip_proc_wrapper(sql: str) -> tuple[str | None, str]:
    """Peel the `CREATE PROCEDURE ... AS [BEGIN] ... [END]` wrapper.

    Returns ``(proc_name, body)``. `proc_name` is the captured object name
    (e.g. ``"[rpt].[Foo]"``) or None if there is no CREATE PROCEDURE header
    -- in which case `body` is the input unchanged, so a bare body or a
    plain SELECT still flows through.
    """
    m = _PROC_HEADER_RE.search(sql)
    if not m:
        return None, sql
    proc_name = m.group("name")
    body = sql[m.end():]

    # Drop the outermost BEGIN ... END that wraps the proc body. Only the
    # outer pair: inner BEGIN/END (e.g. inside an IF) belong to constructs
    # we either strip as guards or reject downstream as non-view-shaped.
    body = body.strip()
    begin_match = re.match(r"BEGIN\b", body, re.IGNORECASE)
    if begin_match:
        body = body[begin_match.end():]
        # Trim a trailing END (optionally followed by ; / GO).
        body = re.sub(r"\bEND\s*;?\s*(?:GO\s*)?$", "", body.strip(),
                       flags=re.IGNORECASE)
    return proc_name, body


def _strip_temp_guards(body: str) -> str:
    """Remove `IF OBJECT_ID(...) DROP TABLE #x` guards and bare DROP TABLEs.

    These are pure cleanup statements with no lineage content, and the IF
    form is unparseable by sqlglot -- so we delete them textually before the
    parse step. (Persistent-table DROPs are left alone: those signal an ETL
    proc and we want the downstream parse/validation to surface them.)

    We replace each guard with a `;`, NOT nothing: guards always sit BETWEEN
    statements, and T-SQL lets the preceding statement omit its semicolon.
    Deleting the guard outright would butt the previous statement against the
    next one (e.g. SELECT ... <guard> SELECT ...) and sqlglot, which needs the
    separator, would fail with "Invalid expression". Substituting `;` puts the
    statement terminator exactly where it belongs. Stray/leading `;` just yield
    empty statements, which the parser drops harmlessly.
    """
    body = _TEMP_GUARD_RE.sub(";\n", body)
    body = _BARE_DROP_RE.sub(";\n", body)
    return body


# Statement-starting keywords that, at top level, begin a new statement.
_HARD_STARTERS = {
    TokenType.INSERT, TokenType.UPDATE, TokenType.DELETE, TokenType.MERGE,
    TokenType.DROP, TokenType.CREATE, TokenType.TRUNCATE, TokenType.DECLARE,
    TokenType.WITH,
}
_SET_OPS = {TokenType.UNION, TokenType.EXCEPT, TokenType.INTERSECT}


def _insert_statement_separators(body: str, dialect: str) -> str:
    """Insert `;` at REAL top-level statement boundaries, via sqlglot's
    tokenizer -- not regex.

    T-SQL lets a statement omit its trailing `;`; sqlglot's PARSER needs the
    separator or it reads `SELECT ... SELECT ...` as one broken statement.
    Tokenizing handles strings / comments / parentheses correctly, and a small
    state machine distinguishes a NEW statement from a CONTINUATION:
      - a `SELECT` after UNION/EXCEPT/INTERSECT (set operation)
      - the body `SELECT` of INSERT..SELECT / WITH..SELECT
      - INSERT/UPDATE/DELETE inside a MERGE's WHEN clauses
      - any subquery `SELECT` (paren depth > 0)
    Idempotent: a `;` already present resets the state, so we never double up.
    Tokenizer failure (extremely rare) returns the body unchanged.
    """
    try:
        toks = sqlglot.tokenize(body, dialect=dialect)
    except Exception:
        return body

    depth = 0
    opener = None          # token type that opened the current statement
    saw_select = False     # has the current INSERT/WITH/MERGE consumed its SELECT?
    after_setop = False    # just saw UNION/EXCEPT/INTERSECT [ALL|DISTINCT] -> next SELECT continues
    cuts: list[int] = []   # char offsets to insert `;` before

    for t in toks:
        tt = t.token_type
        if tt == TokenType.L_PAREN:
            depth += 1
            after_setop = False            # a parenthesized set-op branch is self-contained
            continue
        if tt == TokenType.R_PAREN:
            depth = max(0, depth - 1)
            continue
        if depth != 0:
            continue                       # inside parens: subquery/CTE body, never a boundary

        if tt == TokenType.SEMICOLON:
            opener, saw_select, after_setop = None, False, False
            continue
        if tt in _SET_OPS:
            after_setop = True             # UNION / EXCEPT / INTERSECT
            continue
        if tt in (TokenType.ALL, TokenType.DISTINCT):
            continue                       # modifier after a set-op: keep after_setop

        new = False
        if tt in _HARD_STARTERS:
            after_setop = False
            if opener == TokenType.MERGE and tt in (
                    TokenType.INSERT, TokenType.UPDATE, TokenType.DELETE):
                pass                       # MERGE WHEN-clause, not a new statement
            elif opener == TokenType.WITH:
                opener = tt                # WITH..MERGE/INSERT/UPDATE/DELETE: the
                #                            CTE's main statement, not a new one
            elif opener is None:
                opener = tt
            else:
                new = True
        elif tt == TokenType.SELECT:
            if after_setop:
                after_setop = False        # set-operation continuation (UNION [ALL] SELECT)
            elif opener in (TokenType.INSERT, TokenType.WITH) and not saw_select:
                saw_select = True          # INSERT..SELECT / WITH..SELECT body
                #  (MERGE excluded: its source SELECT is parenthesized, so a
                #   top-level SELECT after a MERGE is a NEW statement.)
            elif opener is None:
                opener, saw_select = tt, True
            else:
                new = True
        else:
            after_setop = False            # any other token clears a pending set-op

        if new:
            cuts.append(t.start)
            opener = tt
            saw_select = (tt == TokenType.SELECT)

    if not cuts:
        return body
    out: list[str] = []
    last = 0
    for pos in cuts:
        out.append(body[last:pos])
        out.append(";\n")
        last = pos
    out.append(body[last:])
    return "".join(out)


# Words that turn a BEGIN into a procedural block we must NOT flatten.
_SPECIAL_BLOCK = {"TRY", "CATCH", "TRANSACTION", "TRAN", "DISTRIBUTED"}


def _has_control_flow(body: str, dialect: str) -> bool:
    """True if the body has real control flow -- `IF`/`WHILE` (tokenized as
    VARs, distinct from the `IIF` function) or a `BEGIN TRY/CATCH/TRANSACTION`
    block. Such procs aren't view-shaped; we neither flatten nor mis-describe
    them. Tokenizer-based so the keywords aren't matched inside strings."""
    try:
        toks = sqlglot.tokenize(body, dialect=dialect)
    except Exception:
        return False
    n = len(toks)
    for i, t in enumerate(toks):
        if t.token_type == TokenType.VAR and (t.text or "").upper() in ("IF", "WHILE"):
            return True
        if t.token_type == TokenType.BEGIN and i + 1 < n \
                and (toks[i + 1].text or "").upper() in _SPECIAL_BLOCK:
            return True
    return False


def _strip_block_begin_end(body: str, dialect: str) -> str:
    """Remove plain `BEGIN ... END` block delimiters so the inner statements
    parse (sqlglot treats a BEGIN..END block as an opaque 'Command').

    Done with the tokenizer, so `CASE ... END` is preserved (its END is kept)
    and string/comment content is respected. BAILS (returns the body
    unchanged) when the proc has real control flow -- flattening that would
    change semantics; such procs are genuinely procedural and get rejected.
    """
    if _has_control_flow(body, dialect):
        return body
    try:
        toks = sqlglot.tokenize(body, dialect=dialect)
    except Exception:
        return body

    stack: list[str] = []          # 'case' (keep its END) vs 'begin' (drop its END)
    remove: list[tuple[int, int]] = []
    for t in toks:
        tt = t.token_type
        if tt == TokenType.CASE:
            stack.append("case")
        elif tt == TokenType.BEGIN:
            stack.append("begin")
            remove.append((t.start, t.end))
        elif tt == TokenType.END:
            kind = stack.pop() if stack else None
            if kind == "begin":
                remove.append((t.start, t.end))

    if not remove:
        return body
    out: list[str] = []
    last = 0
    for s, e in sorted(remove):
        out.append(body[last:s])
        out.append(" ")            # keep token separation
        last = e + 1               # token .end is inclusive
    out.append(body[last:])
    return "".join(out)


# ---- AST helpers ---------------------------------------------------------

def _is_temp_table(node: exp.Expression | None) -> bool:
    """True if `node` is (or wraps) a reference to a `#temp` table.

    sqlglot flags temp-table identifiers with ``temporary=True`` and strips
    the leading ``#`` from the name, so we key off that flag. Accepts either
    a Table node or an Into node (the target of `SELECT ... INTO`).
    """
    if isinstance(node, exp.Into):
        node = node.this
    if not isinstance(node, exp.Table):
        return False
    ident = node.this
    return bool(getattr(ident, "args", {}).get("temporary"))


def _cte_name(temp_name: str) -> str:
    """CTE alias for a temp table. The bare name (``#stage`` -> ``stage``)
    keeps the rewrite readable and matches the author's mental model; the
    `#` is already stripped from `temp_name` by sqlglot."""
    return temp_name


def _rewrite_temp_refs(select: exp.Select, defined: dict[str, str]) -> None:
    """Rewrite every `#temp` *reference* in `select` to its CTE alias.

    Mutates the AST in place: clears the `temporary` flag and rebinds the
    identifier name so the reference renders as a plain CTE name instead of
    `#temp`. A reference to a temp that was never defined in this proc means
    the proc consumes an externally-staged temp -- not self-contained, hence
    not view-shaped -- so we raise.
    """
    for tbl in select.find_all(exp.Table):
        if not _is_temp_table(tbl):
            continue
        name = tbl.name
        if name not in defined:
            raise ProcNotViewShaped("undefined_temp_reference", f"#{name}")
        ident = tbl.this
        ident.set("temporary", False)
        ident.set("this", defined[name])


# ---- public entry point --------------------------------------------------

def select_into_to_cte(
    sql: str,
    *,
    dialect: str = _DEFAULT_DIALECT,
    emit_create_view: bool = True,
) -> str:
    """Rewrite a CTE-shaped proc into one view-shaped SELECT.

    Args:
        sql: Full stored-proc text (CREATE PROCEDURE ... AS BEGIN ... END),
            or a bare proc body. SSMS boilerplate (USE/GO/SET-options) is
            tolerated.
        dialect: sqlglot dialect; T-SQL by default.
        emit_create_view: When True (and the proc name was recovered), wrap
            the result as ``CREATE VIEW <name> AS <select>`` so it flows
            through Phase C exactly like a real scripted view. When False,
            returns the bare WITH/SELECT.

    Returns:
        The view-shaped SQL string.

    Raises:
        ProcNotViewShaped: when the CTE-equivalence constraint is violated
            (mutation of a temp, redefinition, write to a persistent table,
            zero or multiple terminal SELECTs, an unsupported statement, or
            a reference to an undefined temp). The exception's `reason` is a
            stable code the caller can use to bucket the proc.
    """
    sql = _strip_bare_header(sql)
    proc_name, body = _strip_proc_wrapper(sql)
    body = _strip_temp_guards(body)

    # Apply the shared parsing-rule registry to the body, so the proc path
    # benefits from the same sqlglot-gap fixes the view path gets via
    # preprocess_ssms (e.g. SET TRANSACTION ISOLATION LEVEL, ODBC {escape}).
    # On an already-unwrapped body the CREATE/BEGIN-END rules are no-ops.
    from .parsing_rules import apply_all
    body, _ = apply_all(body)

    # Strip plain BEGIN..END block delimiters (the proc wrapper, or a block
    # that sits after a DECLARE preamble). Without this sqlglot parses the whole
    # block as an opaque Command. Bails on real control flow / TRY-CATCH.
    body = _strip_block_begin_end(body, dialect)

    # Insert any missing top-level statement separators (T-SQL makes `;`
    # optional; sqlglot's parser requires it). Tokenizer-based, so subqueries /
    # INSERT..SELECT / CTEs / set-ops / MERGE aren't mis-split.
    body = _insert_statement_separators(body, dialect)

    # Parse the (now guard-free) body into its top-level statements. Drop
    # Nones, which sqlglot emits for empty fragments between semicolons.
    try:
        statements = [s for s in sqlglot.parse(body, dialect=dialect) if s is not None]
    except Exception:
        # A parse failure on a body with control flow (IF/WHILE/TRY-CATCH) is a
        # procedural proc -> not view-shaped. A failure WITHOUT control flow is
        # a genuine parser gap we want to surface, so re-raise that.
        if _has_control_flow(body, dialect):
            raise ProcNotViewShaped("procedural")
        raise
    if not statements:
        raise ProcNotViewShaped("empty_body")

    # First pass: classify each statement.
    #   - SELECT ... INTO #tmp   -> a CTE definition
    #   - SELECT (no INTO)       -> a terminal (output) query
    #   - SET ...                -> harmless session option, skipped
    #   - anything else          -> violates the constraint, reject
    cte_defs: list[tuple[str, exp.Select]] = []   # (cte_name, defining select)
    terminals: list[exp.Select] = []
    defined: dict[str, str] = {}                  # temp_name -> cte_name

    for st in statements:
        if isinstance(st, exp.Set):
            # SET NOCOUNT ON / SET ANSI_NULLS ON -- no lineage, skip.
            continue
        if isinstance(st, exp.Declare):
            # DECLARE @v ... -- a local variable. A parameterized proc is just a
            # view with @params; the declarations are setup, not lineage. Skip.
            continue
        if isinstance(st, exp.Select) and st.expressions and all(
                isinstance(e, exp.EQ) and isinstance(e.this, exp.Parameter)
                for e in st.expressions):
            # `SELECT @v = <expr>` -- a variable assignment, not a result set
            # (a result alias `col = <expr>` has a Column on the left, not a
            # Parameter, so real result SELECTs are unaffected). Skip.
            continue
        if isinstance(st, exp.SetOperation):
            # A UNION / EXCEPT / INTERSECT query -- a legitimate view shape, and
            # never a SELECT..INTO staging step. Treat it as a terminal query.
            terminals.append(st)
            continue
        if isinstance(st, exp.Command):
            # sqlglot falls back to Command for T-SQL it can't fully parse:
            #   - DECLARE @var AS DateTime  (complex DECLARE forms)
            #   - SET @var = (SELECT ...)   (variable assignments)
            #   - EXEC / EXECUTE           (proc calls)
            #   - PRINT                    (debug output)
            #   - RETURN                   (proc exit)
            #   - USE [database]           (context switch)
            # These are procedural preamble / postamble with no lineage.
            # Skip them so the proc's real SELECT(s) can be extracted.
            cmd_text = (st.this or "").strip().upper() if isinstance(st.this, str) else ""
            if not cmd_text:
                cmd_text = st.sql(dialect=dialect).strip().upper()
            # Only skip known-safe command patterns. Unknown commands
            # should still raise so we don't silently miss real logic.
            _SAFE_CMD_PREFIXES = (
                "DECLARE", "SET @", "SET NOCOUNT", "SET ANSI",
                "SET XACT", "SET TRANSACTION", "SET QUOTED",
                "SET ARITHABORT", "SET CONCAT_NULL",
                "SET DATEFIRST", "SET DATEFORMAT", "SET DEADLOCK",
                "SET FMTONLY", "SET IDENTITY", "SET LANGUAGE",
                "SET LOCK_TIMEOUT", "SET NUMERIC",
                "SET ROWCOUNT", "SET TEXTSIZE",
                "EXEC", "EXECUTE",
                "PRINT", "RETURN", "USE",
                "RAISERROR", "THROW",
            )
            if any(cmd_text.startswith(p) for p in _SAFE_CMD_PREFIXES):
                continue
            # Unknown Command — reject
            raise ProcNotViewShaped("unsupported_statement",
                                     f"Command: {cmd_text[:60]}")
        if not isinstance(st, exp.Select):
            # INSERT/UPDATE/MERGE/DELETE/CREATE/... -- any
            # of these means the proc isn't a pure stage-and-read.
            raise ProcNotViewShaped("unsupported_statement", type(st).__name__)

        into = st.args.get("into")
        if into is None:
            terminals.append(st)
            continue

        # SELECT ... INTO <target>.
        target = into.this
        if not _is_temp_table(into):
            # Writing to a persistent table is ETL, not a view.
            tgt = target.sql(dialect=dialect) if target is not None else "?"
            raise ProcNotViewShaped("select_into_persistent", tgt)
        temp_name = target.name
        if temp_name in defined:
            # A temp written twice = accumulation/redefinition, not a CTE.
            raise ProcNotViewShaped("temp_redefined", f"#{temp_name}")
        cte = _cte_name(temp_name)
        st.set("into", None)          # strip INTO -> plain SELECT (the CTE body)
        defined[temp_name] = cte
        cte_defs.append((cte, st))

    # Exactly one terminal SELECT maps to a view's single output query.
    if not terminals:
        raise ProcNotViewShaped("no_terminal_select")
    if len(terminals) > 1:
        raise ProcNotViewShaped("multiple_terminal_selects", str(len(terminals)))
    main = terminals[0]

    # Second pass: rewrite #temp references -> CTE names, in every query
    # (CTE bodies may read earlier temps; the terminal reads them too). Done
    # before assembling the WITH so find_all doesn't re-descend into CTEs.
    for _name, cte_select in cte_defs:
        _rewrite_temp_refs(cte_select, defined)
    _rewrite_temp_refs(main, defined)

    # Assemble: prepend CTEs to the terminal SELECT in source order. Source
    # order is dependency order (a temp is defined before it's read), which
    # is exactly the order a WITH clause requires.
    result = main
    for cte_name, cte_select in cte_defs:
        result = result.with_(cte_name, as_=cte_select, dialect=dialect)

    select_sql = result.sql(dialect=dialect, pretty=True)
    if emit_create_view and proc_name:
        return f"CREATE VIEW {proc_name} AS\n{select_sql}"
    return select_sql
