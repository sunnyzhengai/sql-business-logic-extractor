"""Table-name normalization helpers (shared utility).

Small predicates and string-cleanups used wherever the codebase deals
with SQL table identifiers:

  - `bare_table_name(name)` -- strip database/schema prefixes
                                (`Clarity.dbo.PATIENT` -> `PATIENT`)
  - `is_zc_table(name)`    -- detect Epic code-lookup tables (`ZC_*`)
  - `is_cte_or_scope_reference(name)` -- detect non-table scope references
                                          (anything containing a colon)

Historical note
---------------
These helpers were private-by-convention functions inside
`tools.operate.validate_graph_pivot` (`_bare_table_name`, `_is_zc_table`,
`_is_cte_or_scope_reference`). In Phase 2a of the 2026-05 restructure
they were extracted here and the underscore-prefix dropped (they are now
public utilities, used across multiple phases). Callers should
`from tools.shared.table_names import bare_table_name, ...`.
"""

from __future__ import annotations


def bare_table_name(qualified_name: str) -> str:
    """Strip schema/database prefixes from a fully-qualified table name.

    Examples:
        EPIC.PATIENT     -> PATIENT
        Clarity.dbo.ZC_X -> ZC_X
        PATIENT          -> PATIENT
        cte:foo          -> cte:foo   (no change; we filter these out elsewhere)
    """
    if not qualified_name:
        return ""
    # `split(".")[-1]` returns the last segment regardless of how many dots
    # there were. `Clarity.dbo.X` -> ["Clarity", "dbo", "X"][-1] -> "X".
    return qualified_name.split(".")[-1].strip()


def is_zc_table(bare_name: str) -> bool:
    """Heuristic: ZC_* tables are Epic code-lookup tables (decorative).

    These tables are 'leaves' in the join graph -- nothing joins from them
    onward. They contribute attributes (status labels, category names) without
    shaping the cohort. We tag them so we can visually distinguish them.
    """
    return bare_name.upper().startswith("ZC_")


def is_cte_or_scope_reference(name: str) -> bool:
    """Detect scope references that should NOT be treated as table nodes.

    The corpus uses prefixes like `cte:X` or `derived:Y` for non-table
    scopes. A real table name will never contain a colon, so this is a
    safe filter.
    """
    return ":" in (name or "")
