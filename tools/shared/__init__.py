"""Cross-phase utilities -- not a pipeline phase.

Code in this folder is used by more than one phase. Keeping it out of
any individual phase folder avoids circular imports and makes it clear
that this is shared infrastructure, not domain logic.

What goes here
--------------
- Schema dataclasses used by multiple phases (e.g., ViewV1, ScopeV1
  if they need to be shared between p10 emitting and p20 consuming).
- File I/O helpers (corpus.jsonl readers/writers).
- Common parsing helpers (table-name normalization, scope-id stripping).
- Color palettes / styling constants used across renderers.
- Logger / error types used across the pipeline.

What does NOT go here
---------------------
- Phase-specific logic. Each phase owns its own analysis / rendering /
  synthesis code.
- One-off scripts. Those go in `tools/diagnostics/` or as standalone
  files in the relevant phase.

Read order
----------
- `corpus_io.py`        -- read/write corpus.jsonl (header + view lines)
- `table_names.py`      -- bare-name extraction, ZC detection, scope-id stripping
- `palette.py`          -- shared color constants for visualization

Phase 0 status: skeleton. Code that should live here is currently
duplicated across `extract_corpus/`, `graph_explore/`, and
`diagnostics/validate_graph_pivot.py`. Consolidation happens during
Phase 1+ migration.
"""
