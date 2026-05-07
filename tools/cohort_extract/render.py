"""Cohort + filter rendering -- pure functions, no IO.

A "cohort" is a population-level English phrase composed from the
tables a scope reads. We use:

  1. Table short_descriptions sourced from a YAML overlay
     (data/dictionaries/table_short_descriptions.yaml) and/or the
     schema.tables[].short_description field if loaded.
  2. The dim_filter to skip enrichment-only joins (PATIENT, ZC_*,
     CLARITY_*) which don't define the cohort.
  3. A simple compositor: 1 fact table -> its short_desc; 2+ fact
     tables -> "<driver> with <other> and <other2>".

When a scope reads ONLY from upstream scopes (no base tables, e.g.
`SELECT FROM ActiveMembers WHERE ROW = 1`), the cohort is inherited
from the upstream scope -- the renderer uses "<upstream cohort>"
verbatim and lets filters describe the carve-out.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class TableDescriptions:
    """Lookup of {bare_table_name_upper: short_description}.

    Primary source is the clarity_schema.json (built from
    clarity_metadata.csv via csv_to_schema.py): each entry under
    `tables[*].short_description` becomes a lookup. The YAML overlay
    is a SECONDARY source for tables not in the Clarity schema (custom
    fact tables, views, etc.) -- pass via `from_yaml` and merge.
    """
    by_name: dict[str, str]

    @classmethod
    def from_schema(cls, schema: dict) -> "TableDescriptions":
        """Build from a loaded clarity_schema.json dict."""
        by_name: dict[str, str] = {}
        for t in (schema or {}).get("tables", []) or []:
            name = (t.get("name") or "").strip().upper()
            sd = (t.get("short_description") or "").strip()
            if name and sd:
                by_name[name] = sd
        return cls(by_name=by_name)

    @classmethod
    def from_schema_path(cls, path: str | Path) -> "TableDescriptions":
        """Load a schema JSON from disk and build descriptions from it."""
        import json as _json
        try:
            with open(path, "r", encoding="utf-8") as f:
                schema = _json.load(f)
        except (OSError, ValueError):
            return cls.empty()
        return cls.from_schema(schema)

    @classmethod
    def from_yaml(cls, path: str | Path) -> "TableDescriptions":
        """Load from a YAML overlay -- secondary source for tables not
        in the Clarity schema (custom views / fact tables you added)."""
        import yaml
        text = Path(path).read_text(encoding="utf-8")
        raw = yaml.safe_load(text) or {}
        if isinstance(raw, dict):
            return cls(by_name={k.upper(): v for k, v in raw.items()
                                  if isinstance(v, str)})
        return cls.empty()

    @classmethod
    def empty(cls) -> "TableDescriptions":
        return cls(by_name={})

    @classmethod
    def merge(cls, *sources: "TableDescriptions") -> "TableDescriptions":
        """Later sources override earlier ones."""
        merged: dict[str, str] = {}
        for s in sources:
            merged.update(s.by_name)
        return cls(by_name=merged)

    def get(self, table_name: str) -> Optional[str]:
        if not table_name:
            return None
        bare = table_name.split(".")[-1].strip().upper()
        return self.by_name.get(bare)


def _humanize_table_name(name: str) -> str:
    """Fallback when no short_description is available."""
    bare = (name or "").split(".")[-1]
    parts = [p for p in bare.split("_") if p]
    if not parts:
        return bare
    # Lower for natural English; strip trailing _C (numeric code suffix).
    if parts[-1].upper() == "C":
        parts = parts[:-1]
    if not parts:
        return bare.lower()
    return " ".join(p.lower() for p in parts)


def _is_dim(table_name: str, dim_predicates: list) -> bool:
    """Apply the same dim filter rules used by view_shape_compare."""
    if not table_name:
        return False
    bare = table_name.split(".")[-1].strip().upper()
    for fn in dim_predicates:
        if fn(bare):
            return True
    return False


def _table_phrase(name: str, descriptions: TableDescriptions) -> str:
    return descriptions.get(name) or _humanize_table_name(name)


def build_cohort(
    head: str,
    others: list[str],
    upstream_scope_ids: list[str],
    descriptions: TableDescriptions,
) -> str:
    """Compose a cohort phrase from a head entity + zero or more other
    entities + optional upstream scope edges.

    Layer-1 rule (head + leaf):
      - 0 other entities  ->  `<head>`
      - 1 other entity    ->  `<head> with <other>`
      - 2+ other entities ->  `<head>` (avoid arbitrary leaf pick;
                              user can annotate via Layer 2 override)
      - No head AND upstream(s) -> "" (caller renders "same as upstream")
      - No head, no upstream -> "" (nothing to describe)
    """
    if not head:
        return ""
    head_phrase = _table_phrase(head, descriptions)
    if not others:
        return head_phrase
    if len(others) == 1:
        return f"{head_phrase} with {_table_phrase(others[0], descriptions)}"
    return head_phrase


# Filter kinds that constrain the row population. JOIN ON is included
# because business filters often live inside the JOIN clause alongside
# equi-join keys -- we keep the predicate but strip the keys via
# `_strip_equijoin_keys`. EXISTS / IN / unknown kinds are dropped.
_REAL_FILTER_KINDS = {"where", "having", "qualify", "join_on"}


# Pattern for "<word phrase> = <word phrase>" with both sides looking
# like translated column names (letters/digits/spaces only, must start
# with a letter). Quoted strings, numeric literals, and operators other
# than `=` mean this is a real filter, not an equi-join key.
_WORD_PHRASE = r"[A-Za-z][A-Za-z0-9 _]*"
_EQUIJOIN_KEY_RE = re.compile(
    rf"^\s*({_WORD_PHRASE})\s*=\s*({_WORD_PHRASE})\s*$"
)


def _strip_equijoin_keys(text: str) -> str:
    """Remove JOIN-key fragments from a translated predicate's English.

    A JOIN-key fragment looks like `<word phrase> = <word phrase>`
    where both sides are bare translated column names (no quotes, no
    numeric literals, no operators other than `=`). The remaining
    AND-conjoined fragments are real filters and stay.

    Examples:
      "Coverage Identifier = Coverage Identifier and Coverage Type C = 2"
        -> "Coverage Type C = 2"

      "Member Identifier = Patient Identifier and Eff Date <= today"
        -> "Eff Date <= today"
        (the M=P pair is a key relating two tables, dropped)

      "Coverage Type C = 2 and Mem Covered Yn = 'Y'"
        -> unchanged (RHS '2' / "'Y'" don't match the word pattern)

    Splitting is done on the lowercase " and " emitted by the
    engineered translator; OR connectors and BETWEEN-style ranges
    are preserved within each fragment.
    """
    if not text:
        return ""
    # Split on " and " (case-insensitive). The translator's output uses
    # lowercase by convention; match either for safety.
    parts = re.split(r"\s+and\s+", text, flags=re.IGNORECASE)
    kept: list[str] = []
    for p in parts:
        p = p.strip()
        if not p:
            continue
        if _EQUIJOIN_KEY_RE.match(p):
            continue
        kept.append(p)
    return " and ".join(kept)


def render_filter(filter_dict: dict) -> str:
    """Render one filter as a natural-language sentence. For
    join_on-kind filters, equi-key fragments are stripped first so
    only the business predicates within the JOIN clause survive.
    """
    eng = (filter_dict.get("english") or "").strip()
    expr = (filter_dict.get("expression") or "").strip()
    text = eng or expr
    kind = (filter_dict.get("kind") or "where").lower()
    if kind == "join_on":
        text = _strip_equijoin_keys(text)
    return text


def _is_real_filter(filter_dict: dict) -> bool:
    """True for WHERE / HAVING / QUALIFY / JOIN_ON filters. EXISTS /
    IN linkages and unknown kinds are dropped -- they describe how
    scopes connect, not how the cohort is carved.

    JOIN_ON filters that contain ONLY equi-join keys (no business
    predicates) render as empty strings via `_strip_equijoin_keys`,
    so they're filtered out at render time anyway.
    """
    kind = (filter_dict.get("kind") or "where").lower()
    return kind in _REAL_FILTER_KINDS


def _bare(name: str) -> str:
    return (name or "").split(".")[-1].strip()


def _is_pure_label_lookup(table_name: str) -> bool:
    """ZC_* tables in Epic Clarity are pure code-to-label lookups
    (e.g., ZC_TAX_STATE has TAX_STATE_C and NAME). Projecting their
    NAME column dereferences a code that lives in some OTHER table
    (e.g., COVERAGE.SUBSCR_STATE_C); it doesn't add a cohort axis.
    These tables are always excluded from the cohort phrase.
    """
    return (table_name or "").strip().upper().startswith("ZC_")


def _selected_source_tables(scope: dict) -> list[str]:
    """The unique set of base tables that the scope's SELECTED columns
    trace back to, in column-declaration order.

    This is the cohort-defining set:
      - JOIN-only tables (joined for filter context but not projected)
        are excluded -- they're enrichment for grain math, not cohort.
      - ZC_* lookup tables are excluded even when projected -- their
        NAME column is just a label dereference.
      - Empty / unresolved ("?") source markers are skipped silently.
      - Tables referenced through a CTE (column has `cte:X.col` lineage
        and empty base_tables) yield no entry here -- the renderer
        falls back to "same as upstream" via base_datasets.
    """
    seen: set[str] = set()
    out: list[str] = []
    for col in scope.get("columns") or []:
        for t in col.get("base_tables") or []:
            bare = _bare(t)
            if not bare or bare == "?":
                continue
            if _is_pure_label_lookup(bare):
                continue
            if bare not in seen:
                seen.add(bare)
                out.append(bare)
    return out


def _from_driver(scope: dict) -> str:
    """Detect the FROM-clause driver of this scope.

    A "driver" is a table that's in `reads_from_tables` but doesn't
    appear as the right-side of any join -- i.e., it's the leftmost
    FROM target, not added by a JOIN. This is the head entity for the
    cohort phrase.

    Returns "" when the scope has no base-table driver (e.g., a main
    that reads only from CTEs)."""
    join_right_uppers: set[str] = set()
    for j in scope.get("joins") or []:
        rt = _bare(j.get("right_table") or "")
        if rt:
            join_right_uppers.add(rt.upper())
    for t in scope.get("reads_from_tables") or []:
        bare = _bare(t)
        if not bare:
            continue
        if bare.upper() in join_right_uppers:
            continue
        if _is_pure_label_lookup(bare):
            continue
        return bare
    return ""


def view_to_cohorts(
    view: dict,
    descriptions: TableDescriptions,
    dim_predicates: list,
) -> list[dict]:
    """Render every scope of one view as a cohort entry.

    Cohort tables are sourced from the SELECTED columns' base_tables,
    NOT from `reads_from_tables`. This means a JOIN-only table (joined
    for filter context but not projected) is excluded from the cohort
    -- which matches the user's intuition that "the grain is what your
    SELECT clause carves out, not what your FROM clause references."

    The dim filter is bypassed for selected source tables: if you
    projected from a table, it's meaningful by definition. The dim
    filter still applies to JOIN-only tables for the `same_driver` /
    view-shape signal (a separate tool).

    Returns a list of dicts:
        {
          "scope_id":      str,
          "kind":          str,
          "cohort":        str,    # population phrase, "" if upstream-only
          "base_datasets": [str],  # scope ids
          "filters":       [str],  # English filter sentences
        }
    """
    out: list[dict] = []
    for scope in view.get("scopes") or []:
        selected = _selected_source_tables(scope)
        driver = _from_driver(scope)
        upstream = list(scope.get("reads_from_scopes") or [])

        # Fall back to all reads_from_tables for star projection / odd
        # cases where columns don't trace back to base tables.
        if not selected and not upstream:
            all_tables = list(scope.get("reads_from_tables") or [])
            non_dim = [t for t in all_tables if not _is_dim(t, dim_predicates)]
            if not non_dim and all_tables:
                non_dim = all_tables
            selected = non_dim

        # Layer 1 head/leaf rule:
        #  - prefer the FROM driver as the head (when it contributes a
        #    selected column, i.e., it's in `selected`)
        #  - otherwise the first selected source becomes the head
        #  - 0 others -> just `<head>`
        #  - 1 other  -> `<head> with <other>`
        #  - 2+ others -> just `<head>` (avoid arbitrary leaf pick)
        head = ""
        others: list[str] = []
        if driver and driver in selected:
            head = driver
            others = [t for t in selected if t != head]
        elif selected:
            head = selected[0]
            others = selected[1:]

        cohort = build_cohort(head, others, upstream, descriptions)
        # Only WHERE / HAVING / QUALIFY filters are emitted in the cohort
        # view. JOIN ON predicates (kind="join_on") and subquery linkages
        # (kind="exists" / "in") are dropped -- they describe how scopes
        # connect, not how the cohort is carved.
        filters = [
            render_filter(f) for f in (scope.get("filters") or [])
            if _is_real_filter(f)
        ]

        out.append({
            "scope_id": scope.get("id") or "",
            "kind": scope.get("kind") or "",
            "cohort": cohort,
            "base_datasets": upstream,
            "filters": [f for f in filters if f],
        })
    return out


# -- Markdown rendering --------------------------------------------------

def cohorts_to_markdown(view_name: str, cohorts: list[dict]) -> str:
    lines: list[str] = [f"## {view_name}\n"]
    for c in cohorts:
        sid = c["scope_id"]
        kind = c["kind"]
        cohort = c["cohort"]
        base = c["base_datasets"]
        filters = c["filters"]

        header = f"### `{sid}` ({kind})"
        lines.append(header)
        if cohort:
            lines.append(f"- **Cohort:** {cohort}")
        elif base:
            lines.append(f"- **Cohort:** *(same population as {', '.join(base)})*")
        else:
            lines.append(f"- **Cohort:** *(no carved population)*")
        if base and cohort:
            # Both: e.g., scope reads from upstream AND adds tables
            lines.append(f"- **Base dataset(s):** {', '.join(base)}")
        if filters:
            lines.append("- **Filters:**")
            for f in filters:
                lines.append(f"    - {f}")
        else:
            lines.append("- **Filters:** *(none)*")
        lines.append("")
    return "\n".join(lines)
