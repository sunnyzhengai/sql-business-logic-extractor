---
name: SQL as definitional moments
aliases: [authored-meaning detector, SQL is where definitions live, definitional-moment extraction]
see_also: [govern-authored-meaning, data-vs-metadata-ownership]
sources: []
---

## Definition

The CASE expressions, joins, filters, and aggregations in production SQL are not just transformations — they are the moments when operational definitions are made concrete. `CASE WHEN encounter_type = 'OP' THEN 1` is not data movement; it is the executable operational definition of "outpatient patient" for whichever report contains it. The extractor's real product is a catalog of these definitional moments.

## Why it matters here

Reframes the SQL logic extractor from "a parser that finds transformations" to "the only tool that surfaces authored meaning at the layer where it actually exists." That reframe is the structural moat. Any asset-level catalog (Collibra, Alation, Atlan in default configurations) can list tables, columns, and reports. None of them surface the fact that `encounter.encounter_type = 'OP' AND encounter.status = 'completed'` is a definition of "completed outpatient encounter" authored by a specific query on a specific date by a specific engineer.

Direct implications for the extractor's output schema and Collibra connector design:
- Extract not just lineage edges but **definitional fragments** (CASE arms, filter predicates, join conditions, derived measures).
- Each definitional fragment needs a stable identity, an author (from git blame), a context (the surrounding query and its purpose), and a discoverable surface in the catalog.
- Downstream governance workflows (review, approval, change-impact) attach to these fragments, not to the tables they touch.

## Open questions

- What is the right taxonomy of definitional moments? Candidate buckets: boundary conditions (what counts as "active"), derived flags (is_surgery_patient), cohort definitions (WHERE ... AND ... AND ...), derived measures (SUM with conditions), type coercions, code-set mappings (ICD→business category).
- How stable are fragment identities across SQL refactors? Is there an equivalent of "semantic hash" so a rewritten-but-equivalent CASE is recognized as the same definition?
- How does this interact with dbt models, stored procedures, and view chains where the same definition appears in transformed forms across layers?
- Can the extractor distinguish "incidental" SQL (plumbing, type casts) from "meaningful" SQL (definition authorship)? Fork 4's core claim depends on this distinction being automatable.
