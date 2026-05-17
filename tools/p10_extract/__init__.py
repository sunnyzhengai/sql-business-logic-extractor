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
- `batch.py`            -- entry point and CLI
- (more files arrive as the parser is refactored from sql_logic_extractor/)

Historical note
---------------
This module was previously named `tools.extract_corpus` ("Tool 11 --
corpus extractor, Phase D scope-correct tree"). It was renamed to
`tools.p10_extract` as part of the 2026-05 codebase restructure that
introduced the seven-phase pipeline naming convention (see
`tools/PHASES.md`).

The CorpusV1 schema (`sql_logic_extractor/corpus_schema.py`) remains
the single source of truth for what this module emits.

It replaced earlier separate-pass tools (now removed or being migrated):
  - `tools/batch_all`             (Tools 1-4 in one resolver pass)
  - `tools/term_extraction/batch` (separate term-extraction pass)
  - `tools/similar_logic_grouper` (separate fingerprint pass)
  - `tools/timing_audit`          (separate timing pass)

CLI
---
    python -m tools.p10_extract.batch <input_dir> [-o corpus.jsonl] [--schema ...]

Notebook
--------
    from tools.p10_extract.batch import extract_corpus
    extract_corpus(input_dir=..., output_path=...)
"""
