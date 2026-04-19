---
name: Catalog vs. governance automation asymmetry
aliases: [why governance always came second, the missing governance primitive, automation gap]
see_also: [catalog-is-not-governance, catalog-first-vs-governance-first, sql-as-definitional-moments, govern-authored-meaning]
sources: []
---

## Definition

Catalog ingestion has been automated for more than a decade: connectors to databases, data warehouses, BI tools, file systems, and pipelines emit structural metadata into catalog platforms at scale. Governance authoring — extracting the actual definition of "active patient" or "completed encounter" from the SQL that implements it — has been manual. That asymmetry, not conviction, is why catalog-first sequencing dominates in practice. The extractor changes the primitive.

## Why it matters here

This reframes the SQL logic extractor's competitive category. It is **not a better catalog, a better lineage tool, or a better parser.** It is the first scalable governance-authoring primitive — the missing automation that makes governance-first actually practicable.

Before the extractor, "govern first, then catalog" was an essay topic, because no org could support the manual throughput: every concept has multiple reports, each defines the concept differently through layers of CTEs, joins, and CASE expressions, and a steward hand-reconciling dozens of technical definitions per concept × hundreds of concepts × dozens of reports each is an impossible workload. So orgs defaulted to catalog-first not out of strategic preference but out of what their stack could automate. The extractor is the first tool that attacks the manual side of that asymmetry.

Consequence: the reason "governance first" was usually wrong advice was that it couldn't scale. With automation on the governance side, the advice is now defensible for the first time. That shift in what's operationally possible is the basis of the extractor's category claim.

## Open questions

- What's the automation ceiling? What percentage of governance work can be extracted from code vs. what's irreducibly human (canonical-definition arbitration, ownership assignment, quality SLA negotiation, policy exceptions)?
- As the extractor matures, does the catalog-vs-governance sequencing fork collapse entirely (do both in parallel, powered by extraction) — or does the sequencing still matter politically even when the automation is available?
- Are there parallel automation primitives the field is still missing on the governance side (e.g., quality-rule extraction from code, SLA extraction from pipeline configs)? Where else is the manual/automated asymmetry still unchallenged?
