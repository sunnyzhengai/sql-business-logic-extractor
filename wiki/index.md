# sql-logic-extractor wiki

Human-curated conceptual knowledge. See [SCHEMA.md](SCHEMA.md) for layout rules.

## Concepts

Foundational frames:

- [Data vs. metadata ownership](concepts/data-vs-metadata-ownership.md) — the earliest fork; two different jobs, two different owners
- [Governance layer vs. compliance layer](concepts/governance-vs-compliance-layers.md) — PHI is not a governance type; governance and compliance are different problems
- [Govern authored meaning, not the container](concepts/govern-authored-meaning.md) — the governable unit is (transformation, context, definition), not table/column/report
- [SQL as definitional moments](concepts/sql-as-definitional-moments.md) — what the extractor actually finds: authored meaning, not just data movement
- [Catalog is not governance](concepts/catalog-is-not-governance.md) — a full library with empty entries is not a governed estate
- [Catalog vs. governance automation asymmetry](concepts/catalog-vs-governance-automation-asymmetry.md) — catalog automated for a decade; governance didn't; the extractor is the missing primitive
- [Definitions are emergent](concepts/definitions-are-emergent.md) — definitions are discovered through use, not decreed; the counterweight to naive spec-first
- [Spec-first as data contract](concepts/spec-first-as-data-contract.md) — intellectual anchor connecting the site's position to the data-contracts field
- [App config IDs as coordination keys](concepts/app-config-id-as-coordination-key.md) — Epic INI-Item as the operational primitive that closes the app-team coordination loop; go-to-market wedge for healthcare Epic shops
- [Recursive translation principle](concepts/recursive-translation-principle.md) — translate every SQL construct by recursion to raw columns; unknown patterns/columns are governance signals, never opaque fallbacks

Canonical scenes:

- [Patient A01 — same row, multiple identities](concepts/patient-a01-scene.md) — the knockout example against container-level ownership
- [Silent upstream break](concepts/silent-upstream-break.md) — the canonical healthcare failure scene: Epic picklist changes on Tuesday, HEDIS submission fails Friday, CEO's slide stays wrong for a quarter

Forks with dedicated pages:

- [Catalog first vs. governance first](concepts/catalog-first-vs-governance-first.md) — the hidden sequencing fork inside the tool-first branch
- [Catch-up vs. spec-first](concepts/catch-up-vs-spec-first.md) — the development-lifecycle fork; extractor-enabled selective promotion as the third path
- [App teams in DG, or out](concepts/app-teams-in-dg-vs-out.md) — the enterprise-scope fork; does DG extend upstream into Epic/Cerner/SaaS app teams?

Content site design:

- [Governance forks catalog](concepts/governance-forks-catalog.md) — the working list of forks that will structure the content site navigation and editorial calendar
- [Article template](concepts/article-template.md) — reusable 8-section structure for weekly Saturday articles

## Decisions

- [2026-04-19 — Fork-based model over stage-based maturity ladder for the content site](decisions/2026-04-19-fork-model-for-content-site.md)
- [2026-04-19 — Thinking in Public (TIP) as publishing posture](decisions/2026-04-19-thinking-in-public-strategy.md)
- [2026-04-19 — Publishing automation architecture](decisions/2026-04-19-publishing-automation-architecture.md)
- [2026-04-20 — Adopt recursive translation + pattern library for offline_translate](decisions/2026-04-20-adopt-recursive-translation.md)

## Recent log

See [log.md](log.md).

## Code structure

Auto-generated from graphify — do not edit by hand:

- [Graph report](../graphify-out/GRAPH_REPORT.md) — god nodes, communities, surprising connections
- [Code wiki](../graphify-out/wiki/index.md) — one article per community and god node
- [Interactive graph](../graphify-out/graph.html) — open in browser

## Raw sources

Drop sources into [raw/](raw/) and run `/ingest` to process.
