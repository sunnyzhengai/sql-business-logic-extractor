"""Per-view structural signatures for the similarity tool.

A signature aggregates information across ALL scopes in a view (main
+ CTEs + subqueries + set-op branches) so two views that wrap the same
logic differently produce identical signatures.

Four axes:

  driver       leaf base table -- recurse through `reads_from_scopes`
               until we hit a scope whose first source is a base table.
  joined_set   set of base table names read across all scopes, MINUS
               the driver. Type-agnostic per the design discussion;
               per-table types are kept separately for the "consistency"
               annotation in cluster output.
  projections  set of "identity strings" for the main scope's columns,
               with each column's source transitively resolved through
               `cte:X.col` references back to base tables. Identity is
               source-based for passthroughs (`src:TABLE.COL`) and
               fingerprint-based for derived columns (`fp:HASH`).
  filters      set of canonical SQL strings for every where/having/
               qualify/business-join_on filter in any scope. Each is
               AND-flattened, equi-keys stripped, and the surviving
               leaves alphabetically sorted to make set membership
               equality-comparable.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional

from sqlglot import exp, parse_one


# Filter kinds that count as "real" row-population filters for L4.
# join_on is included because business predicates often live inside
# JOIN clauses; the equi-key stripping below removes the structural
# parts.
_REAL_FILTER_KINDS = {"where", "having", "qualify", "join_on"}


# ---------- low-level utilities ------------------------------------------

def _bare(name: str) -> str:
    """Strip database/schema qualifier; return the last dot-segment."""
    return (name or "").split(".")[-1].strip()


def _normalize_join_type(jt: str) -> str:
    """Canonicalize a join_type string. Same rules as view_shape_compare:
    INNER (default) | LEFT | RIGHT | FULL | CROSS | <verbatim>.
    """
    s = (jt or "JOIN").upper()
    s = re.sub(r"\bOUTER\b", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    if s in ("JOIN", "INNER JOIN"):
        return "INNER"
    if s == "LEFT JOIN":
        return "LEFT"
    if s == "RIGHT JOIN":
        return "RIGHT"
    if s == "FULL JOIN":
        return "FULL"
    if s == "CROSS JOIN":
        return "CROSS"
    return s.replace(" JOIN", "")


# ---------- driver chain --------------------------------------------------

def _leaf_driver(scopes_by_id: dict, start_scope_id: str) -> str:
    """Walk `reads_from_scopes` recursively until we hit a scope whose
    first reads_from_tables entry is a base table; return that bare name.

    A scope's "driver" is taken as the FIRST entry in reads_from_tables
    if any; otherwise we follow the FIRST `reads_from_scopes` reference
    and recurse. This mirrors `cohort_extract._from_driver` but extends
    it transitively through CTE / derived chains.

    Returns "" if no base-table driver can be found (e.g., main reads
    only from another view via DBLINK -- not handled today).
    """
    visited: set[str] = set()
    current = start_scope_id
    while current and current not in visited:
        visited.add(current)
        scope = scopes_by_id.get(current)
        if not scope:
            return ""
        for t in scope.get("reads_from_tables") or []:
            bare = _bare(t)
            if bare and ":" not in bare:
                return bare
        # No base table at this level; walk into the first scope ref.
        next_id = ""
        for ref in scope.get("reads_from_scopes") or []:
            if ref:
                next_id = ref
                break
        current = next_id
    return ""


# ---------- projection resolution ----------------------------------------

def _resolve_column_source(
    column: dict,
    scopes_by_id: dict,
    visited: Optional[frozenset] = None,
) -> Optional[tuple[str, str]]:
    """Walk a column's `base_columns` through scope references back to
    a base table. Returns `(table, col)` or None.

    For columns with multiple base_columns, returns the first that
    resolves -- enough for identity matching. Calculated columns
    typically have multiple sources; for those we'll fall back to the
    fingerprint identity in `_column_identity`."""
    visited = visited or frozenset()
    for bc in column.get("base_columns") or []:
        if bc.startswith("table:"):
            body = bc[len("table:"):]
            parts = body.rsplit(".", 1)
            if len(parts) == 2:
                return (parts[0].strip(), parts[1].strip())
            continue
        # Otherwise it's a scope-qualified ref like "cte:X.col" or "derived:t.col"
        scope_part, _, col_part = bc.partition(".")
        if not col_part or scope_part in visited:
            continue
        upstream = scopes_by_id.get(scope_part)
        if not upstream:
            continue
        # Find the matching column by name in the upstream scope
        for upstream_col in upstream.get("columns") or []:
            if upstream_col.get("column_name") == col_part:
                resolved = _resolve_column_source(
                    upstream_col, scopes_by_id, visited | {scope_part}
                )
                if resolved:
                    return resolved
                break
    return None


def _column_identity(column: dict, scopes_by_id: dict) -> str:
    """Return a single string identifying this column for projection-set
    comparison. Tries (in order):
      1. Resolved base table source: `src:TABLE.COL` (best for passthroughs)
      2. AST fingerprint:            `fp:HASH`        (best for calculated)
      3. Column name as last resort: `name:COLUMN`
    Star columns are special-cased to `star`.
    """
    name = (column.get("column_name") or "").strip()
    if name == "*":
        return "star"
    src = _resolve_column_source(column, scopes_by_id)
    if src:
        return f"src:{src[0]}.{src[1]}"
    fp = column.get("fingerprint")
    if fp:
        return f"fp:{fp}"
    if name:
        return f"name:{name}"
    return ""


# ---------- filter canonicalization --------------------------------------

# `<token>.<col> = <token>.<col>` -- equi-join key (no operators other
# than `=`, no literals on either side). Stripped from filter text.
_SQL_EQUIJOIN_KEY_RE = re.compile(
    r"^\s*"
    r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*"
    r"\s*=\s*"
    r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*"
    r"\s*$"
)


def _canonicalize_filter(sql_text: str, dialect: str = "tsql") -> str:
    """Parse a filter expression, AND-flatten the top-level tree, drop
    equi-join keys, sort the surviving leaves alphabetically, rejoin
    with " AND ". Returns "" if nothing survives.

    Two views' filter sets are compared by element-wise equality on
    these canonical strings, so order-of-AND and equi-key noise don't
    cause false splits.
    """
    if not sql_text:
        return ""
    try:
        node = parse_one(sql_text, dialect=dialect)
    except Exception:
        return sql_text.strip()
    if node is None:
        return sql_text.strip()
    leaves: list = []

    def _flatten(n) -> None:
        if isinstance(n, exp.And):
            _flatten(n.this)
            _flatten(n.expression)
        else:
            leaves.append(n)

    _flatten(node)
    kept_strs: list[str] = []
    for leaf in leaves:
        # Drop AST-level equi-join keys (column = column).
        if isinstance(leaf, exp.EQ):
            l, r = leaf.this, leaf.expression
            if isinstance(l, exp.Column) and isinstance(r, exp.Column):
                continue
        # Strip table-alias prefixes from every column reference so
        # filters that differ only in alias choice (`CVG.COVERAGE_TYPE_C`
        # vs `C.COVERAGE_TYPE_C`) canonicalize to the same string. The
        # alias is structural noise; the column name is the meaning.
        for col in leaf.find_all(exp.Column):
            col.set("table", None)
        try:
            txt = leaf.sql(dialect=dialect).strip()
        except Exception:
            txt = ""
        if not txt:
            continue
        # Defensive double-check: the regex catches text-form equi-keys
        # if AST analysis misses them.
        if _SQL_EQUIJOIN_KEY_RE.match(txt):
            continue
        kept_strs.append(txt)
    if not kept_strs:
        return ""
    kept_strs.sort()
    return " AND ".join(kept_strs)


# ---------- main signature builder ---------------------------------------

@dataclass(frozen=True)
class ViewSignature:
    """Aggregated structural identity for one view, used by the four
    clustering levels. Frozen + hashable so signatures can sit in sets
    and dict keys directly."""
    view_name: str

    # L1 axis
    driver: str

    # L2 axis (joined_set is type-agnostic for equality;
    # join_types is metadata for cluster reporting)
    joined_set: frozenset[str]
    join_types: tuple[tuple[str, str], ...]   # ((bare_table, type), ...)
    all_tables: frozenset[str]                # driver + joined for L1 containment

    # L3 axis
    projections: frozenset[str]

    # L4 axis
    filters: frozenset[str]


def build_view_signature(view_dict: dict, dialect: str = "tsql") -> ViewSignature:
    """Build a ViewSignature from one ViewV1 JSON dict (one line of
    corpus.jsonl).

    Aggregates across the entire scope tree -- main + every CTE +
    derived + subquery + set-op branch -- so the signature reflects
    the view's full data dependency, not just the outer SELECT.
    """
    view_name = view_dict.get("view_name") or ""
    scopes = view_dict.get("scopes") or []
    if not scopes:
        return ViewSignature(
            view_name=view_name, driver="",
            joined_set=frozenset(), join_types=(), all_tables=frozenset(),
            projections=frozenset(), filters=frozenset(),
        )

    scopes_by_id: dict[str, dict] = {s.get("id") or "": s for s in scopes}

    # ---- driver: walk from main, transit through CTE refs ----
    driver = _leaf_driver(scopes_by_id, "main")
    if not driver:
        # Fall back to whichever scope id starts the corpus's view_outputs,
        # or the first scope. Useful for set-op views.
        view_outputs = view_dict.get("view_outputs") or []
        for vo in view_outputs:
            d = _leaf_driver(scopes_by_id, vo)
            if d:
                driver = d
                break
        if not driver and scopes:
            driver = _leaf_driver(scopes_by_id, scopes[0].get("id") or "")

    # CTE / derived-table aliases declared anywhere in the view -- these
    # appear in `reads_from_tables` as if they were base tables (legacy
    # of how Tool 1 inventory works) and must be filtered out.
    scope_aliases: set[str] = set()
    for s in scopes:
        sid = s.get("id") or ""
        if ":" in sid:
            _kind, alias = sid.split(":", 1)
            if alias:
                scope_aliases.add(alias.upper())

    # ---- all_tables across all scopes (ignore scope refs + CTE aliases) ----
    all_tables: set[str] = set()
    join_types_per_table: dict[str, str] = {}
    for s in scopes:
        for t in s.get("reads_from_tables") or []:
            bare = _bare(t)
            if not bare or ":" in bare or bare.upper() in scope_aliases:
                continue
            all_tables.add(bare)
        for j in s.get("joins") or []:
            rt = _bare(j.get("right_table") or "")
            if not rt or ":" in rt or rt.upper() in scope_aliases:
                continue
            all_tables.add(rt)
            jt = _normalize_join_type(j.get("join_type") or "INNER")
            # If the same table appears in multiple joins, the FIRST type
            # we see wins (deterministic but somewhat arbitrary).
            join_types_per_table.setdefault(rt, jt)

    # joined_set: all_tables MINUS the driver (which goes on L1 axis alone)
    joined_set = frozenset(all_tables - {driver}) if driver else frozenset(all_tables)
    # join_types: only for tables in joined_set, sorted for stable output
    join_types_tuple = tuple(
        sorted(
            (t, jt)
            for t, jt in join_types_per_table.items()
            if t in joined_set
        )
    )

    # ---- projections: main scope's columns, transitively resolved ----
    main_scope = scopes_by_id.get("main")
    projections: set[str] = set()
    if main_scope:
        for col in main_scope.get("columns") or []:
            ident = _column_identity(col, scopes_by_id)
            if ident:
                projections.add(ident)
    # If no main scope, fall back to view_outputs[0]
    if not projections and not main_scope:
        view_outputs = view_dict.get("view_outputs") or []
        for vo in view_outputs:
            sc = scopes_by_id.get(vo)
            if sc:
                for col in sc.get("columns") or []:
                    ident = _column_identity(col, scopes_by_id)
                    if ident:
                        projections.add(ident)
                break

    # ---- filters: every real-filter across all scopes, canonicalized ----
    filters: set[str] = set()
    for s in scopes:
        for f in s.get("filters") or []:
            kind = (f.get("kind") or "where").lower()
            if kind not in _REAL_FILTER_KINDS:
                continue
            canon = _canonicalize_filter(f.get("expression") or "", dialect=dialect)
            if canon:
                filters.add(canon)

    return ViewSignature(
        view_name=view_name,
        driver=driver,
        joined_set=joined_set,
        join_types=join_types_tuple,
        all_tables=frozenset(all_tables),
        projections=frozenset(projections),
        filters=frozenset(filters),
    )
