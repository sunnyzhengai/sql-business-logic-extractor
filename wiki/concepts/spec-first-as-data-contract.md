---
name: Spec-first as data contract
aliases: [contract-first development for data, data contracts, shift-left data governance]
see_also: [catch-up-vs-spec-first, definitions-are-emergent, govern-authored-meaning]
sources: []
---

## Definition

The spec-first posture on the `catch-up-vs-spec-first.md` fork is a specific instance of **data contract** thinking — the broader industry posture that producers and consumers of data should agree on an explicit, enforceable, versioned interface before data flows, in the same way API teams agree on an OpenAPI spec before code is written.

## Why it matters here

Connecting the site's position to the data-contracts conversation does three things: (1) establishes intellectual credibility by naming the existing field rather than implying invention; (2) gives readers familiar with data contracts an immediate onramp; (3) sharpens the site's distinctive contribution by showing where it *diverges* from the mainstream data-contracts argument.

**Mainstream data-contracts argument:** producers (upstream, source systems) must publish explicit, versioned contracts; downstream consumers enforce them at ingest; contracts are the unit of change management. Tools in the space: dbt model contracts, Gable, Datafold contracts, PactFlow-adjacent work.

**Site's distinctive contribution:** mainstream thinking treats contracts as *decreed by producers*. The extractor-enabled posture treats contracts as *earned by promotion* — definitions emerge from production code, stabilize through use, and graduate to contract status only after their stability has been demonstrated. Contracts are a promotion tier, not a starting assumption. This is a meaningful twist on the canonical position, and it's operationally defensible because the extractor makes emergence-tracking possible at scale.

## Open questions

- How does the extractor's output eventually get expressed as a formal contract (dbt contract YAML, Collibra business term + technical metadata)? What's the promotion pipeline?
- Where does this posture agree and disagree with the Zhamak Dehghani / data mesh framing of data products with contracts?
- Do runtime-enforced data contracts survive in healthcare's regulated, slow-moving ETL world, or are they primarily a modern-data-stack phenomenon? What's the migration path for legacy estates?
