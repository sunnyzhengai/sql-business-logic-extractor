"""Per-view feature extraction for shape comparison.

A view's "shape" is reduced to a small set of canonical features
computed from its main scope (the user-visible output scope). CTE-
internal joins are NOT included in the comparison -- two views with
the same user-visible shape but different internal CTE plumbing should
group together, since governance care about what a report exposes,
not how it's computed.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from .dim_filter import DimFilter


def _bare_table(name: str) -> str:
    """Strip database/schema qualifiers and trailing whitespace."""
    return (name or "").split(".")[-1].strip()


def _normalize_join_type(jt: str) -> str:
    """Canonicalize a join_type string into a small enum.

    The parser produces variants like 'INNER JOIN', 'LEFT OUTER JOIN',
    'CROSS JOIN'. We collapse synonyms so 'INNER JOIN' == 'JOIN' and
    'LEFT OUTER JOIN' == 'LEFT JOIN'. Returns one of:
      INNER | LEFT | RIGHT | FULL | CROSS
    Unknown variants pass through verbatim (uppercased, OUTER stripped).
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


def _normalize_on(on_expr: str) -> str:
    """Canonicalize an ON predicate so that A.x = B.x and B.x = A.x
    compare equal, and aliases are dropped.

    This is intentionally conservative: we only canonicalize the
    common case of `<token>.<col> = <token>.<col>` (and AND-conjunctions
    of those). Anything more complex falls back to whitespace
    normalization. Good enough to catch trivial alias / order
    differences without false-merging real semantic differences."""
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
            # Sort the two sides so order doesn't matter.
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
class ViewShape:
    """A view's structural fingerprint, used for clustering.

    All sets/tuples here are derived from the main-scope's reads_from_*
    and joins. Dim-stripped variants use the filter from `DimFilter`.
    """
    view_name: str
    driver_table: str                     # FROM driver, bare; "" if none
    all_tables: frozenset[str]            # bare, before dim strip
    fact_tables: frozenset[str]           # bare, after dim strip
    all_joins: tuple[tuple[str, str, str], ...]   # canonical (right_table, type, on)
    fact_joins: tuple[tuple[str, str, str], ...]  # joins where right_table is a fact

    # Derived signatures used for clustering keys.
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


def view_shape_from_dict(view_dict: dict, dim_filter: DimFilter) -> ViewShape | None:
    """Build a ViewShape from one ViewV1 JSON dict (one line of corpus.jsonl).

    Returns None for views with no main scope (e.g., parse-error views
    that fell through to _error_view) -- those don't participate in
    comparison.
    """
    name = view_dict.get("view_name") or ""
    scopes = view_dict.get("scopes") or []
    main = next((s for s in scopes if s.get("id") == "main"), None)
    if not main:
        return None

    # All tables: reads_from_tables + every join's right_table (when it's
    # a base table reference, not another scope id like "cte:X").
    all_table_set: set[str] = set()
    for t in main.get("reads_from_tables") or []:
        bare = _bare_table(t)
        if bare:
            all_table_set.add(bare)
    for j in main.get("joins") or []:
        rt = _bare_table(j.get("right_table") or "")
        # Skip scope refs like "cte:X" (they shouldn't have made it into
        # right_table when the resolver mapped CTE refs into reads_from_scopes,
        # but defensively skip anything containing a colon).
        if rt and ":" not in rt:
            all_table_set.add(rt)

    fact_table_set = {t for t in all_table_set if not dim_filter.is_dim(t)}

    all_joins: list[tuple[str, str, str]] = []
    fact_joins: list[tuple[str, str, str]] = []
    for j in main.get("joins") or []:
        rt = _bare_table(j.get("right_table") or "")
        if not rt or ":" in rt:
            continue
        canon = (rt, _normalize_join_type(j.get("join_type")),
                  _normalize_on(j.get("on_expression")))
        all_joins.append(canon)
        if rt in fact_table_set:
            fact_joins.append(canon)

    # Driver table: the first non-dim table mentioned in reads_from_tables,
    # falling back to the first reads_from_tables entry if all are dim.
    driver = ""
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
        all_tables=frozenset(all_table_set),
        fact_tables=frozenset(fact_table_set),
        all_joins=tuple(all_joins),
        fact_joins=tuple(fact_joins),
    )
