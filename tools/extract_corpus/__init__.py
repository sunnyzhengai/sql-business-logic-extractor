"""Tool 11 -- corpus extractor (Phase B of corpus consolidation).

Walks a folder of *.sql views, runs the engine ONCE per view, and
emits a single canonical CorpusV1 artifact as `corpus.jsonl` (one
view per line, streamable). Replaces the multiple separate corpus
walks done today by:

  - tools/batch_all                (Tools 1-4 in one resolver pass)
  - tools/term_extraction/batch    (Term extraction -- separate pass)
  - tools/similar_logic_grouper    (fingerprint -- separate pass)
  - tools/timing_audit             (timing -- separate pass)

The CorpusV1 schema (sql_logic_extractor/corpus_schema.py, Phase A)
is the single source of truth. Phase C (the rematerializer) emits
audience-aligned CSVs from corpus.jsonl, so existing CSV consumers
keep working.
"""
