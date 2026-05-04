"""Per-view feature extraction for shape comparison.

A view's "shape" is computed by walking ALL of its scopes (main + CTEs
+ derived tables + subqueries) and aggregating tables and joins across
the whole scope tree. That way a view with `WITH C1 AS (... FROM
ENCOUNTER ...) SELECT FROM C1 JOIN PATIENT` correctly reports
ENCOUNTER as a fact table, even though main scope alone only reads
from C1 (a scope reference) and PATIENT (a dim).

We also retain per-scope features (`scopes`) so the JSON output can
show readers WHERE each table comes from. Comparison itself is done
on the aggregate axes -- two views are "shape-similar" when their
aggregate fact tables and joins overlap, regardless of how the work
was split across CTEs.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from .dim_filter import DimFilter


def _bare_table(name: str) -> str:
    return (name or "").split(".")[-1].strip()


def _normalize_join_type(jt: str) -> str:
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


def _normalize_on(on_expr: str) -> str:
    if not on_expr:
        return ""
    parts = re.split(r"\s+AND\s+", on_expr, flags=re.IGNORECASE)
    canon_parts: list[str] = []
    for p in parts:
        p = p.strip()
        m = re.match(
            r"^([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)"
            r"\s*=\s*"
            r"([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)$",
            p,
        )
        if m:
            l_tbl, l_col, r_tbl, r_col = m.groups()
            left = f"{l_tbl}.{l_col}"
            right = f"{r_tbl}.{r_col}"
            if left > right:
                left, right = right, left
            canon_parts.append(f"{left}={right}")
        else:
            canon_parts.append(re.sub(r"\s+", " ", p))
    canon_parts.sort()
    return " AND ".join(canon_parts)


@dataclass(frozen=True)
class ScopeFeature:
    """Per-scope shape: tables this scope reads + joins it declares.

    `tables` are bare table names (database/schema stripped). Scope
    references like `cte:X` are NOT included -- they're cross-scope
    edges, not tables. `fact_tables` is `tables` after dim strip.
    """
    id: str
    kind: str
    tables: frozenset[str]
    fact_tables: frozenset[str]
    joins: tuple[tuple[str, str, str], ...]        # (right_table, type, on)
    fact_joins: tuple[tuple[str, str, str], ...]


@dataclass(frozen=True)
class ViewShape:
    """A view's structural fingerprint.

    Aggregate fields (`all_tables`, `fact_tables`, `all_joins`,
    `fact_joins`) are unions over EVERY scope in the view. Comparison
    runs on these. `scopes` carries the per-scope decomposition for
    transparency in the output -- consumers can see which scope a
    given table or join came from.
    """
    view_name: str
    driver_table: str

    all_tables: frozenset[str]
    fact_tables: frozenset[str]
    all_joins: tuple[tuple[str, str, str], ...]
    fact_joins: tuple[tuple[str, str, str], ...]

    scopes: tuple[ScopeFeature, ...]

    @property
    def fact_tables_sig(self) -> tuple[str, ...]:
        return tuple(sorted(self.fact_tables))

    @property
    def fact_joins_sig(self) -> tuple[tuple[str, str, str], ...]:
        return tuple(sorted(self.fact_joins))

    @property
    def all_tables_sig(self) -> tuple[str, ...]:
        return tuple(sorted(self.all_tables))

    @property
    def all_joins_sig(self) -> tuple[tuple[str, str, str], ...]:
        return tuple(sorted(self.all_joins))


def _scope_feature(scope: dict, dim_filter: DimFilter) -> ScopeFeature:
    """Extract per-scope tables/joins from one scope dict."""
    sid = scope.get("id") or ""
    skind = scope.get("kind") or ""

    tables: set[str] = set()
    for t in scope.get("reads_from_tables") or []:
        bare = _bare_table(t)
        if bare:
            tables.add(bare)

    joins: list[tuple[str, str, str]] = []
    for j in scope.get("joins") or []:
        rt = _bare_table(j.get("right_table") or "")
        # Skip scope refs (cte:X, derived:Y, etc.) -- those are cross-scope
        # edges; the underlying tables live in the referenced scope and
        # get picked up when we walk it.
        if not rt or ":" in rt:
            continue
        tables.add(rt)
        canon = (
            rt,
            _normalize_join_type(j.get("join_type")),
            _normalize_on(j.get("on_expression")),
        )
        joins.append(canon)

    fact_tables = {t for t in tables if not dim_filter.is_dim(t)}
    fact_joins = tuple(j for j in joins if j[0] in fact_tables)

    return ScopeFeature(
        id=sid,
        kind=skind,
        tables=frozenset(tables),
        fact_tables=frozenset(fact_tables),
        joins=tuple(joins),
        fact_joins=fact_joins,
    )


def view_shape_from_dict(view_dict: dict, dim_filter: DimFilter) -> ViewShape | None:
    """Build a ViewShape by walking every scope.

    Returns None for views with no scopes (e.g., parse-error views).
    """
    name = view_dict.get("view_name") or ""
    scopes_raw = view_dict.get("scopes") or []
    if not scopes_raw:
        return None

    scope_features = tuple(_scope_feature(s, dim_filter) for s in scopes_raw)

    # Aggregate across scopes.
    all_tables: set[str] = set()
    fact_tables: set[str] = set()
    all_joins: list[tuple[str, str, str]] = []
    fact_joins: list[tuple[str, str, str]] = []
    for sf in scope_features:
        all_tables |= sf.tables
        fact_tables |= sf.fact_tables
        all_joins.extend(sf.joins)
        fact_joins.extend(sf.fact_joins)

    # Driver: main scope's first non-dim table; falls back to first
    # reads_from_tables entry of main if all are dim.
    driver = ""
    main = next((s for s in scopes_raw if s.get("id") == "main"), None)
    if main:
        for t in main.get("reads_from_tables") or []:
            bare = _bare_table(t)
            if bare and not dim_filter.is_dim(bare):
                driver = bare
                break
        if not driver:
            for t in main.get("reads_from_tables") or []:
                bare = _bare_table(t)
                if bare:
                    driver = bare
                    break

    return ViewShape(
        view_name=name,
        driver_table=driver,
        all_tables=frozenset(all_tables),
        fact_tables=frozenset(fact_tables),
        all_joins=tuple(all_joins),
        fact_joins=tuple(fact_joins),
        scopes=scope_features,
    )
