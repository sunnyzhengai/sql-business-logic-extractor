"""Phase 6 -- capture human-in-the-loop annotations.

Two distinct human roles produce annotations here:

  - BI developers (first pass) review p40 artifacts and confirm /
    reject / enrich findings. They are the domain experts on the SQL
    side and know which views were really intended to be similar.
  - Stewards (second pass) review the BI-dev-validated findings and
    make ratification decisions. Are these the same concept? What is
    the canonical definition? Should we consolidate / retire / build
    a model?

This phase is the SCHEMA + INTAKE for those decisions. The actual UI
or workflow tooling lives outside the codebase (steward meetings,
spreadsheets, Collibra) -- this folder just defines the data shape
and reads it back into the system.

Consumes
--------
- p40_synthesize artifacts (steward looks at these and produces decisions).
- Hand-written or semi-structured human input (markdown, CSV, JSON).

Produces
--------
- `bi_dev_annotations.jsonl`     -- BI-dev review records
- `steward_decisions.jsonl`      -- ratification outcomes
- `intentional_divergences.jsonl` -- view pairs that look similar but
                                     are intentionally different
- `synonyms.jsonl`               -- same-concept-different-names mappings

Read order
----------
- `schemas.py`              -- pydantic / dataclass models for each record type
- `intake_steward.py`       -- parse steward-meeting outputs into records
- `intake_bi_dev.py`        -- parse BI-dev review outputs into records

Design contract
---------------
- Decisions are append-only JSONL. Editing history is preserved as
  superseded records, not in-place edits.
- Every record carries the human author's identity (name / email) and
  a timestamp so future questions ("why was this ratified?") can be
  answered.
- Premature on day one (artifacts exist before annotations), but the
  schema goes in early so feedback can accumulate from the first
  steward meeting.

Phase 0 status: skeleton. No existing tooling absorbs here; this is
all new work that builds on top of the analysis pipeline.
"""
