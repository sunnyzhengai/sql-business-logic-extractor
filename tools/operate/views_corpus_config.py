"""Default source-folder configuration for Yang's MyChart corpus.

Used by batch-extract / nested-view-resolution commands to know where
SQL files live. Yang's two folders organize the views by reporting
team / namespace:

  data/views_reporting   -- Reporting team's views (V_*, F_* etc.)
  data/views_cookrpt     -- COOK-RPT-specific views

When the extract pipeline runs against the FULL corpus, it should
glob both folders to pick up every .sql file. The same paths are
also used by view-of-view resolution: when a view's SQL says
`FROM SomeFoundationView`, the renderer / resolver can scan these
folders to find SomeFoundationView's own .sql file.

These paths are relative to the project root. Override at the call
site if Yang's workspace layout changes.
"""

from __future__ import annotations

from pathlib import Path

# Project-root-relative paths to Yang's two SQL-source folders.
# Override these or pass explicit dirs to consuming functions.
VIEW_SOURCE_DIRS: tuple[str, ...] = (
    "data/views_reporting",
    "data/views_cookrpt",
)


def resolve_view_source_dirs(
    project_root: str | Path | None = None,
    overrides: tuple[str, ...] | None = None,
) -> list[Path]:
    """Return absolute paths for the configured view-source folders.

    Parameters
    ----------
    project_root : project root used as the base for relative paths.
        Defaults to the current working directory.
    overrides : optional tuple replacing VIEW_SOURCE_DIRS entirely
        (so a customer with a different layout can pass their own).

    Returns
    -------
    list of Path objects -- not validated to exist (the consumer
    decides whether to require existence or skip missing dirs).
    """
    base = Path(project_root) if project_root else Path.cwd()
    dirs = overrides if overrides is not None else VIEW_SOURCE_DIRS
    return [base / d for d in dirs]


def find_sql_files(
    project_root: str | Path | None = None,
    overrides: tuple[str, ...] | None = None,
) -> list[Path]:
    """Glob every .sql under the configured view-source folders.

    Returns the flat list -- caller can dedupe / sort / filter as
    needed. Folders that don't exist are skipped silently (consistent
    with how the extract pipeline handles missing inputs).
    """
    out: list[Path] = []
    for d in resolve_view_source_dirs(project_root, overrides):
        if not d.is_dir():
            continue
        out.extend(sorted(d.glob("*.sql")))
    return out
