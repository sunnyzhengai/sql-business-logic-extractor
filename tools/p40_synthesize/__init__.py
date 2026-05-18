"""Phase 4 -- findings to steward-ready artifacts.

Turns raw findings (from p30_analyze) into things a non-technical steward
can read in 5 minutes and make decisions from. This is where deck slide 4
("N distinct definitions of active patient, written by M developers")
actually becomes a deliverable artifact.

Consumes
--------
- p30_analyze findings (communities, primary-community assignments,
  cross-domain spans, variance findings, naming collisions).
- Parts of corpus.jsonl for filter / cohort / English-render details.

Produces
--------
- `community_NN.md` -- one-pager per community: top tables, bridge
  context, primary member views, recommended steward conversation.
- `term_disagreements.md` -- same lexical anchor, different
  implementations across the corpus.
- `cohort_evidence.md` -- English-rendered cohort definitions
  (the 30-minute steward conversation artifact).
- `recruitment_list.md` -- which BI developers wrote each variant
  (joins SQL provenance with author lookup -- requires manual data).

Read order
----------
- `community_packet.py`     -- per-community summary generator
- `term_disagreement.py`    -- naming collision -> markdown report
- `cohort_renderer.py`      -- English renders of SQL cohort filters

Design contract
---------------
- Outputs are plain markdown. No HTML, no JSON-only. Stewards print these.
- Every claim in an artifact has a pointer back to the source view,
  scope, line.
- No LLM-generated descriptions. We don't have LLM access in the
  pilot environment; BI-dev review fills the description gap.

Phase 0 status: skeleton. Existing tooling that will absorb here:
`cohort_extract/` (English filter rendering), `inventory_manifest/`
(used-tables summaries), `report_description_generator/`.
"""
