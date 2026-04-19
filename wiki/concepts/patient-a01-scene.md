---
name: Patient A01 — same row, multiple identities
aliases: [multiple identities of one patient, the Patient A01 story]
see_also: [govern-authored-meaning, data-vs-metadata-ownership, governance-vs-compliance-layers]
sources: []
---

## Definition

A single row in the Patient table — MRN A01 — carries multiple semantic identities in parallel. In an outpatient encounter authored by an outpatient clinic, they are an outpatient patient. In a surgical encounter authored by surgery scheduling, they are a surgery patient. In MyChart, the consumer-facing team defines them differently again. Same row, same MRN, different definitions, different authors, different owners.

## Why it matters here

This is the canonical concrete scene for the content site. In one paragraph it kills the intuition that "the Patient table has an owner" and forces the reader toward authored-meaning governance without any jargon. Every healthcare reader has sat in the meeting where someone tried to assign one owner to the Patient table. This story is why that meeting stalled.

Load-bearing points the scene establishes:
1. Ownership at the *container* (table) level is structurally impossible for shared entities.
2. Ownership at the *definition* level (outpatient-patient-as-rendered-by-outpatient-encounter) is tractable — the author is known and the scope is bounded.
3. Compliance-layer ownership (who's accountable for PHI on the Patient row) is a separate question from governance-layer ownership and must not be conflated.

Candidate as the opening scene of Saturday article #1 and of the site homepage.

## Open questions

- How many parallel semantic identities exist for a typical patient in a large health system? (Estimate >10: billing, quality measures, registries, research cohorts, population health, referral networks, care-plan teams, care management, risk stratification, patient engagement...)
- Are there any row-types (reference data, master demographics) where a single definition legitimately dominates, or does multiplicity always leak in once you look carefully?
- How to render this scene in a single diagram for the homepage without making it look like an ontology crime scene?
