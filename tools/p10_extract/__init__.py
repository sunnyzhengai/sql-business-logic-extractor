"""Phase 1 -- SQL files to corpus.jsonl.

Parses SSMS-exported SQL view files into the typed `ViewV1` structure
(scopes, columns, joins, filters, base_columns), resolves column lineage
through CTEs and subqueries, and emits one JSON object per view to
`corpus.jsonl`.

Consumes
--------
- A directory of `.sql` files (typically SSMS exports: UTF-16-LE with BOM,
  ANSI_NULLS / GO boilerplate).
- `data/dictionaries/zc_values.csv` (optional) for inline ZC-lookup annotations.

Produces
--------
- `corpus.jsonl` -- one JSON line per view; the first line is a header with
  schema version and view count.

Read order
----------
- `parser.py`           -- entry point and CLI
- `view_v1_schema.py`   -- the ViewV1 dataclass definitions
- `resolver.py`         -- column-lineage resolution through CTEs
- `zc_annotator.py`     -- inline lookup annotation

Phase 0 status: this folder is a skeleton. Existing tooling lives in
`tools/extract_corpus/`; migration into this folder will happen in
Phase 1 of the codebase restructure (see `tools/PHASES.md`).
"""
