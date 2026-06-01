"""Load external view SQL files into shape-ready ViewV1 dicts.

When the shape renderer encounters a view-of-view reference like
`FROM SomeFoundationView`, we want to expand that reference inline
-- not just hyperlink to it. Inline expansion needs the referenced
view's full scope tree, which means parsing its SQL file.

This module:
  - Globs all .sql files under the configured source folders
    (`data/views_reporting`, `data/views_cookrpt` by default, via
    views_corpus_config.VIEW_SOURCE_DIRS).
  - Parses each via the SAME extract -> resolve pipeline used by
    the production corpus, but produces a MINIMAL ViewV1 dict
    carrying only the fields view_shape consumes (scope tree,
    reads_from_tables / reads_from_scopes, joins with columns).
  - Skips per-column English translation and ZC-lookup resolution
    for speed -- the shape renderer doesn't need either.
  - Tolerates per-file failures: bad SQL, encoding issues, etc.
    log and skip; the rest of the corpus still loads.

Result is a dict keyed by view_name (from file stem) -- the same
convention extract_corpus uses for the main corpus. Consumers
(write_community_shapes via validate_graph_pivot) pass this through
as `external_view_lookup` to build_view_shape, which uses it both
for foreign-view detection AND inline expansion.
"""

from __future__ import annotations

from pathlib import Path

from sql_logic_extractor.extract import SQLBusinessLogicExtractor, to_dict
from sql_logic_extractor.resolve import LineageResolver, preprocess_ssms
from tools.operate.views_corpus_config import find_sql_files


def _read_sql_file(path: Path) -> str:
    """Read a SQL file tolerating common encodings (UTF-8 / UTF-16 LE
    -- the second is what SSMS Generate Scripts emits by default).
    """
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        # SSMS exports are typically UTF-16 LE with BOM.
        pass
    try:
        return path.read_text(encoding="utf-16")
    except UnicodeDecodeError:
        pass
    # Last resort -- decode as latin-1 (lossy but won't error).
    return path.read_text(encoding="latin-1")


def parse_view_for_shape(
    sql_path: str | Path,
    *,
    dialect: str = "tsql",
) -> dict | None:
    """Parse one .sql file into a minimal shape-ready ViewV1 dict.

    Returns None if parsing or resolution fails. Includes only the
    fields v4 build_view_shape consumes -- skips column English
    translation, ZC lookups, and term extraction, all of which are
    expensive and unused for shape rendering.
    """
    path = Path(sql_path)
    try:
        sql = _read_sql_file(path)
    except Exception:
        return None

    clean_sql, _ = preprocess_ssms(sql)
    if not clean_sql or not clean_sql.strip():
        clean_sql = (sql or "").strip()
    if not clean_sql:
        return None

    try:
        extractor = SQLBusinessLogicExtractor(dialect=dialect)
        logic = to_dict(extractor.extract(clean_sql))
    except Exception:
        return None

    try:
        tree = LineageResolver(logic).resolve_all_scoped()
    except Exception:
        return None

    scopes_out: list[dict] = []
    for s in tree.scopes:
        scopes_out.append({
            "id": s.id,
            "kind": s.kind,
            "reads_from_tables": list(s.reads_from_tables or []),
            "reads_from_scopes": list(s.reads_from_scopes or []),
            "joins": [
                {
                    "right_table": j.right_table,
                    "right_alias": j.right_alias,
                    "join_type": j.join_type,
                    "on_expression": j.on_expression,
                    "columns": [
                        {"column": c.column,
                         "table": c.table,
                         "table_alias": c.table_alias}
                        for c in (j.columns or [])
                    ],
                }
                for j in (s.joins or [])
            ],
            # Shape renderer doesn't use these but the corpus dict
            # shape expects them present. Empty lists are fine.
            "columns": [],
            "filters": [],
        })

    return {
        "view_name": path.stem,
        "view_outputs": list(tree.view_outputs or []),
        "scopes": scopes_out,
    }


def load_external_views(
    project_root: str | Path | None = None,
    overrides: tuple[str, ...] | None = None,
    *,
    dialect: str = "tsql",
    verbose: bool = False,
    view_source_dirs: list[str | Path] | tuple[str | Path, ...] | None = None,
) -> dict[str, dict]:
    """Parse every .sql file under the configured source folders.

    Returns a dict mapping view_name (file stem) -> ViewV1 dict.
    Missing folders are silently skipped (no error -- matches
    views_corpus_config's tolerance of partial inputs).

    Per-file failures are skipped: views whose SQL the parser
    can't handle don't break the whole batch. With `verbose=True`,
    each failure is printed to stderr so the user can audit.

    Parameters
    ----------
    view_source_dirs : optional list of explicit folder paths
        (absolute or relative). When provided, OVERRIDES the
        cwd-relative VIEW_SOURCE_DIRS lookup entirely -- useful
        for Fabric setups where Path.cwd() doesn't point at the
        repo root and the SQL files live at known absolute paths.
    project_root, overrides : legacy parameters preserving the
        existing find_sql_files API. Ignored when view_source_dirs
        is set.
    """
    import sys

    out: dict[str, dict] = {}
    if view_source_dirs is not None:
        # Explicit absolute-path mode: glob each provided dir directly
        # without going through the cwd-relative resolver.
        sql_files: list[Path] = []
        for d in view_source_dirs:
            p = Path(d)
            if not p.is_dir():
                if verbose:
                    print(f"  view_resolver: source dir not found: {p}",
                          file=sys.stderr)
                continue
            sql_files.extend(sorted(p.glob("*.sql")))
    else:
        sql_files = find_sql_files(project_root, overrides)
    for path in sql_files:
        view_dict = parse_view_for_shape(path, dialect=dialect)
        if view_dict is None:
            if verbose:
                print(f"  view_resolver: skip {path.name} (parse failed)",
                      file=sys.stderr)
            continue
        # Don't clobber if the same stem appears twice (rare, but
        # possible across the two source folders). First wins.
        view_name = view_dict["view_name"]
        if view_name in out:
            if verbose:
                print(f"  view_resolver: duplicate view_name {view_name!r} "
                      f"from {path}; keeping first occurrence",
                      file=sys.stderr)
            continue
        out[view_name] = view_dict
    if verbose:
        print(f"  view_resolver: loaded {len(out)} external view(s) "
              f"from {len(sql_files)} .sql file(s)",
              file=sys.stderr)
    return out
