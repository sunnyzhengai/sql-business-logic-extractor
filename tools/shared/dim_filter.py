"""Dim-table classifier (shared utility).

Loads a list of dimension/lookup table names from a config file and
exposes `is_dim(table_name) -> bool` -- used by any phase that needs
to distinguish "decorative" (dimension/lookup) tables from
"cohort-shaping" (fact) tables.

Format (one entry per line):
  - Bare names:       PATIENT
  - Suffix wildcards: ZC_*
  - Comments:         lines starting with `#`
  - Blank lines:      ignored

Matching is case-insensitive. Schema/database qualifiers are stripped
before matching: `Clarity.dbo.PATIENT` -> `PATIENT`.

Historical note
---------------
This module was previously `tools.view_shape_compare.dim_filter`. It
moved to `tools.shared.dim_filter` as part of the 2026-05 codebase
restructure (see `tools/PHASES.md`) when the rest of
`tools/view_shape_compare/` was deleted -- its comparison logic was
superseded by the graph-based community detection in `p30_analyze/`.

Cross-phase utilities live in `tools/shared/`, which is what makes
this module's new home. The actively-used parts of `view_shape_compare`
(this file) were preserved; the superseded pieces (`batch.py`,
`clusters.py`, `features.py`) were deleted -- git history preserves
them at the pre-restructure commit if ever needed for reference.

Note: in the longer term, `p30_analyze/` may make this module
obsolete entirely -- the graph topology auto-derives dimension vs
fact classification from degree percentiles (see
`tools/diagnostics/validate_graph_pivot.py::detect_bridge_tables`),
without needing a config file. Keeping `dim_filter` available for
backwards-compatibility with `cohort_extract` and for cases where
a user-curated dim list is preferred over the auto-detected one.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DimFilter:
    exact: frozenset[str]                 # lowercased exact table names
    prefixes: tuple[str, ...]             # lowercased prefixes (for `X_*` rules)

    @classmethod
    def from_file(cls, path: str | Path) -> "DimFilter":
        return cls.from_lines(Path(path).read_text(encoding="utf-8").splitlines())

    @classmethod
    def from_lines(cls, lines) -> "DimFilter":
        exact: set[str] = set()
        prefixes: list[str] = []
        for raw in lines:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            line_lc = line.lower()
            if line_lc.endswith("*"):
                prefixes.append(line_lc[:-1])  # strip trailing *
            else:
                exact.add(line_lc)
        return cls(exact=frozenset(exact), prefixes=tuple(prefixes))

    @classmethod
    def empty(cls) -> "DimFilter":
        return cls(exact=frozenset(), prefixes=())

    def is_dim(self, table_name: str) -> bool:
        """Return True if `table_name` matches a configured dim entry.

        Strips any database.schema.table qualifier before matching.
        Empty / falsy names are treated as non-dim (we only filter what
        we can name)."""
        if not table_name:
            return False
        bare = table_name.split(".")[-1].strip().lower()
        if not bare:
            return False
        if bare in self.exact:
            return True
        return any(bare.startswith(p) for p in self.prefixes)


DEFAULT_DIM_FILTER_PATH = (
    Path(__file__).resolve().parents[2]
    / "data" / "dictionaries" / "dim_tables.txt"
)


def load_default_dim_filter() -> DimFilter:
    """Load the project's default dim-table list. Falls back to an
    empty filter (no tables stripped) if the file isn't found."""
    if DEFAULT_DIM_FILTER_PATH.is_file():
        return DimFilter.from_file(DEFAULT_DIM_FILTER_PATH)
    return DimFilter.empty()
