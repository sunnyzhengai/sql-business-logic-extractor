"""Infrastructure-view filtering -- exclude metadata/catalog views before analysis.

Some views exist purely to extract metadata for catalogs (Collibra, Atlas,
Purview, ...). They join to dozens of tables to harvest schema or usage
info -- they are not business logic. If we leave them in, they pollute
analysis output: community detection connects tables that have no
business-domain relationship.

This module provides:

  - `is_infrastructure_view(view, name_patterns)` -- predicate on one view
  - `filter_business_views(views, name_patterns)` -- bulk split into
                                                       (kept, excluded_names)
  - `DEFAULT_INFRASTRUCTURE_PATTERNS` -- substring patterns for view names
  - `SYSTEM_SCHEMA_PREFIXES` -- table-source prefixes that indicate infra

Historical note
---------------
These helpers lived inside `tools.operate.validate_graph_pivot` during
Phase 1 of the 2026-05 restructure. They were extracted here in Phase 2a
because filtering infrastructure views is a cross-cutting concern that
any pipeline phase (p10 ingest, p30 analysis) might want to apply.
"""

from __future__ import annotations

from typing import Iterable


# Default substrings (case-insensitive) that mark an infrastructure view.
# Users can append more via the --exclude-pattern CLI argument when running
# `validate_graph_pivot` (and presumably any future analysis CLIs).
DEFAULT_INFRASTRUCTURE_PATTERNS: tuple[str, ...] = (
    "collibra",
    "metadata",
    "catalog",
    "ingest",
)

# System-schema prefixes -- any view that reads from one of these is almost
# certainly infrastructure. We match on the SQL-qualified source name.
SYSTEM_SCHEMA_PREFIXES: tuple[str, ...] = (
    "sys.",
    "information_schema.",
    "INFORMATION_SCHEMA.",
)


def is_infrastructure_view(view: dict, name_patterns: Iterable[str]) -> bool:
    """Return True if a view looks like metadata/catalog infrastructure.

    Two heuristics:
      1. View name contains one of the configured substring patterns
         (case-insensitive match).
      2. Any scope of the view reads from a system schema (sys.*, etc.).

    These are HEURISTICS. They will both miss some infrastructure views and
    occasionally catch a legitimate business view. Callers can pass custom
    name_patterns to tune for their corpus.
    """
    name_lower = (view.get("view_name") or "").lower()
    for pat in name_patterns:
        if pat and pat.lower() in name_lower:
            return True

    for scope in view.get("scopes") or []:
        for table_name in scope.get("reads_from_tables") or []:
            t_lower = (table_name or "").lower()
            for prefix in SYSTEM_SCHEMA_PREFIXES:
                if t_lower.startswith(prefix.lower()):
                    return True
    return False


def filter_business_views(views: list[dict],
                            name_patterns: Iterable[str] | None = None,
                            ) -> tuple[list[dict], list[str]]:
    """Split a view list into (business_views, excluded_view_names).

    The excluded list is reported downstream so users can verify nothing
    important was filtered out by accident. `name_patterns=None` falls back
    to `DEFAULT_INFRASTRUCTURE_PATTERNS`.
    """
    if name_patterns is None:
        name_patterns = DEFAULT_INFRASTRUCTURE_PATTERNS
    patterns = list(name_patterns)

    kept: list[dict] = []
    excluded: list[str] = []
    for v in views:
        if is_infrastructure_view(v, patterns):
            excluded.append(v.get("view_name") or "?")
        else:
            kept.append(v)
    return kept, excluded
