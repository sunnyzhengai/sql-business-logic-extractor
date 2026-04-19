---
name: Governance layer vs. compliance layer
aliases: [governance is not compliance, compliance overlay, PHI is not a governance type]
see_also: [data-vs-metadata-ownership, govern-authored-meaning]
sources: []
---

## Definition

**Compliance** answers: who can see this row, is the access audited, is it encrypted, how long must it be retained, is it masked in non-prod. **Governance** answers: what does this row mean, whose definition are we using, for which workflow, measured how, with what quality expectations. These are different layers operating on different objects with different tools and different owners.

## Why it matters here

Healthcare IT has conflated the two for decades, and the conflation is a major driver of DG stall. When a program says "HIM owns the Patient table," it is almost always answering the compliance question (who's accountable for PHI handling) while pretending it has answered the governance question (whose definition of "patient" is in force here). The governance question goes unanswered and the tool stalls.

Explicit implication: **PHI is not a governance type.** Regulatory classifications (PHI, PCI, FERPA) are compliance overlays — they describe sensitivity classes and operate at the access-control layer. They do not tell you what a column means. Treating "PHI" as a governance attribute is a category error.

The extractor lives in the governance layer. Compliance tooling (DLP, access catalogs, audit logs) belongs elsewhere in the stack and should be kept architecturally separate.

## Open questions

- Where is the clean seam between the two layers in practice? Lineage touches both (compliance cares about where sensitive data flows; governance cares about how it's transformed semantically).
- How do we name the layer distinction in plain language for a non-technical reader, without introducing "compliance" and "governance" as jargon?
- Is there a third layer (operational / data quality) that also gets collapsed into "governance" in conventional usage?
