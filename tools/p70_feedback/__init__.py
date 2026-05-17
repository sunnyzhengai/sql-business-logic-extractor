"""Phase 7 -- apply annotations back into the analysis.

Makes the system improve with use. Each steward decision and BI-dev
annotation becomes data that influences the next p30_analyze run.
This is what turns the tool from a one-shot scan into a compounding
governance asset.

Consumes
--------
- p60_hitl annotation records:
    bi_dev_annotations.jsonl, steward_decisions.jsonl,
    intentional_divergences.jsonl, synonyms.jsonl

Produces
--------
- Updates to the graph + lexical index for the next analyze pass:
    * intentional-divergence pairs suppressed from variance findings
    * synonym links added to the lexical index
    * ratified canonical definitions promoted (visible in artifacts)
    * retired views removed from active scope

Read order
----------
- `apply.py`            -- entry point; reads annotations, updates state
- `synonyms.py`         -- merge synonym records into the lexical index
- `suppression.py`      -- apply intentional-divergence suppressions
- `canonicalization.py` -- promote ratified definitions

Design contract
---------------
- Idempotent: applying the same annotations twice produces the same
  state. Re-runs are safe.
- Annotation records are the source of truth; the graph state is
  derived. Re-running the pipeline always reconstructs the same state
  from raw corpus + annotation history.
- Decisions can be revoked: a later steward record with status='revoked'
  reverses an earlier ratification.

Phase 0 status: skeleton. No existing tooling absorbs here; this is
all new work that depends on p60_hitl being populated.
"""
