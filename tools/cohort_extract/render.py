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

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class TableDescriptions:
    """Lookup of {bare_table_name_upper: short_description}."""
    by_name: dict[str, str]

    @classmethod
    def from_yaml(cls, path: str | Path) -> "TableDescriptions":
        import yaml
        text = Path(path).read_text(encoding="utf-8")
        raw = yaml.safe_load(text) or {}
        # Tolerate accidental nested structure; flatten one level if needed.
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
    fact_tables: list[str],
    upstream_scope_ids: list[str],
    descriptions: TableDescriptions,
) -> str:
    """Compose a cohort phrase from tables + upstream scopes.

    - Pure upstream-scope passthrough (no facts of its own): empty
      string -- caller renders "(same population as <upstream>)".
    - Single fact table: that table's short_description.
    - Multiple fact tables: "<driver> with <other> and <other2>".
    - Upstream scope + facts: "<upstream-cohort> enriched with
      <other facts>" -- but only if the renderer has the upstream's
      cohort string. For now, we keep it simple: list all facts.
    """
    if not fact_tables and upstream_scope_ids:
        return ""   # caller signals "same population as <upstream>"
    if not fact_tables:
        return ""   # nothing to say
    phrases = [_table_phrase(t, descriptions) for t in fact_tables]
    if len(phrases) == 1:
        return phrases[0]
    return phrases[0] + " with " + " and ".join(phrases[1:])


def render_filter(filter_dict: dict) -> str:
    """Render one filter as a natural-language sentence.

    Today: just returns the filter's English (already produced by the
    engineered translator in the corpus). Future work could detect
    common patterns like ROW_NUMBER()=1 and rewrite as
    "most recent X per Y" -- requires cross-scope chasing, deferred.
    """
    eng = (filter_dict.get("english") or "").strip()
    expr = (filter_dict.get("expression") or "").strip()
    return eng or expr


def view_to_cohorts(
    view: dict,
    descriptions: TableDescriptions,
    dim_predicates: list,
) -> list[dict]:
    """Render every scope of one view as a cohort entry.

    Cohort-building rule: dims are stripped from the cohort UNLESS
    they are the ONLY tables in the scope. A view that selects only
    from PATIENT *is* a "patients" cohort, even though PATIENT is
    listed as a dim for the (separate) view-shape comparison purpose.

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
        all_tables = list(scope.get("reads_from_tables") or [])
        fact_tables = [t for t in all_tables if not _is_dim(t, dim_predicates)]
        # If stripping dims leaves nothing, the dims ARE the cohort.
        if not fact_tables and all_tables:
            fact_tables = all_tables
        upstream = list(scope.get("reads_from_scopes") or [])
        cohort = build_cohort(fact_tables, upstream, descriptions)
        filters = [render_filter(f) for f in (scope.get("filters") or [])]

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
