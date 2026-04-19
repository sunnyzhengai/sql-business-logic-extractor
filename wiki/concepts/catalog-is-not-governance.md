---
name: Catalog is not governance
aliases: [governance theater, library without contents, inventory is not meaning]
see_also: [catalog-first-vs-governance-first, catalog-vs-governance-automation-asymmetry, govern-authored-meaning, data-vs-metadata-ownership]
sources: []
---

## Definition

A populated data catalog answers **"what tables exist."** Governance answers **"what do those tables mean, for which workflow, authored by whom, measured how."** Ingesting databases, BI reports, and column lists into a platform fills the catalog — it does not do the governance. Orgs that conflate the two declare victory when the catalog fills up and never reach governance.

## Why it matters here

This is the mechanism behind most "we implemented Collibra last year and still nothing works" stories. The tool did what it automated — catalog ingestion — and the organization interpreted a full catalog as the project being done. Stewards were named, policies were drafted, and the meaning layer was left blank because nothing in the stack could fill it at scale.

The extractor's entire category exists because this gap was previously unfillable. A catalog shows you an encyclopedia's table of contents. Governance writes the actual encyclopedia entries. Until the extractor, the only way to write those entries was by hand, one concept at a time, by chasing SQL through CTEs — which is why catalogs are populated everywhere and governance is populated almost nowhere.

## Open questions

- What minimum catalog depth is actually useful before governance authoring can begin — or can governance now lead catalog entirely, with the catalog derived from the extractor's output?
- Is there a viable sequencing where an org skips the traditional catalog rollout and goes straight to extractor-driven governance over a chosen domain?
- What does the catalog UI look like when "what it means" is as present as "what it is"? Most catalog products have a definition field that is empty in 95% of rows in practice.
