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
    """
    body = _TEMP_GUARD_RE.sub("", body)
    body = _BARE_DROP_RE.sub("", body)
    return body


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
    proc_name, body = _strip_proc_wrapper(sql)
    body = _strip_temp_guards(body)

    # Parse the (now guard-free) body into its top-level statements. Drop
    # Nones, which sqlglot emits for empty fragments between semicolons.
    statements = [s for s in sqlglot.parse(body, dialect=dialect) if s is not None]
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
        if not isinstance(st, exp.Select):
            # INSERT/UPDATE/MERGE/DELETE/DECLARE/IF/WHILE/CREATE/... -- any
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
