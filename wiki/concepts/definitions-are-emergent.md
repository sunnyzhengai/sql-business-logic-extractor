---
name: Definitions are emergent
aliases: [premature canonicalization, definitions discovered not decreed, concept stability is earned]
see_also: [catch-up-vs-spec-first, patient-a01-scene, govern-authored-meaning]
sources: []
---

## Definition

For most concepts in a healthcare analytics environment, the "right" definition is not known upfront. It emerges from building the report, watching it be used, arguing about its result in a meeting, rebuilding it, and watching again. Premature canonicalization — picking one authoritative definition before the concept has been field-tested — is as damaging to a governance program as premature optimization is to a codebase. It suppresses the variation that reveals which definitions are stable and which are legitimately workflow-specific.

## Why it matters here

This is the counterweight to naive spec-first advocacy. Without this idea explicit, the `catch-up-vs-spec-first.md` fork collapses into a straw man where spec-first is obviously correct and catch-up is a legacy workaround.

With this idea explicit, the fork's real shape emerges: **catch-up is not a compromise — it is how definitions are discovered.** Definitions that have been through the catch-up loop are vastly more defensible than definitions decreed by a steward committee that has never had to answer for the report downstream. Spec-first is only appropriate *after* a concept has proven stable through catch-up. Variation is not noise to be eliminated — it is signal about where stability has and hasn't yet emerged.

This idea also tightly couples to `patient-a01-scene.md`. If a single patient has multiple legitimate semantic identities by workflow, then *even in the long run* no one-authoritative-definition outcome is correct. Emergence never fully resolves; it just converges to the right plurality.

## Where emergence converges and where it doesn't

Some concepts do eventually stabilize enough to be canonicalized:
- **Regulatory-driven concepts** — CMS quality measures, HEDIS denominators, 340B eligibility. Pre-canonicalized externally; your job is to match the external spec.
- **Financially audited KPIs** — net revenue, bad debt, DSO. External auditors collapse variance over time.
- **Core demographic facts** — legal name, date of birth, MRN. Already stable by infrastructure convention.

Some concepts never converge and shouldn't:
- **Patient cohort definitions** — always workflow-specific. "Active patient" means different things to clinicians, schedulers, billers, population-health teams.
- **Encounter-level rollups** — outpatient vs. surgery vs. telehealth each need local definitions.
- **Risk stratifications** — multiple legitimate models coexist for different purposes.

The governance program's real job isn't to *eliminate* definitional variation but to distinguish **legitimate plurality** (multiple owners, named and governed) from **accidental drift** (unintended divergence nobody noticed).

## Open questions

- What operational signal distinguishes "this concept is ready to be canonicalized" from "this concept's variation is load-bearing"? Cross-report variance alone isn't enough — two workflows might both use the same SQL by coincidence.
- Can the extractor contribute to this distinction — e.g., flagging when two seemingly-identical definitions actually diverge in rare edge cases, versus when divergent-looking definitions converge in effect?
- How do you communicate "your definition is not canonical, and that's correct" to a clinical or operational stakeholder who wants one authoritative answer? Premature canonicalization is often politically easier than acknowledged plurality.
- Are there concept classes where emergence is *forbidden* (regulatory) and governance must behave as if spec-first has always been required, even when in practice the internal implementation lagged?
