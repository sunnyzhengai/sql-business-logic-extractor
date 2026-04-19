---
name: Catalog first vs. governance first (fork)
aliases: [inside-the-tool sequencing fork, inventory-first vs. meaning-first rollout]
see_also: [catalog-is-not-governance, catalog-vs-governance-automation-asymmetry, data-vs-metadata-ownership, govern-authored-meaning, governance-forks-catalog]
sources: []
---

## Definition

After an org has chosen a governance platform (a tool-first outcome on Fork 1), a *second, usually unconscious* sequencing decision sits waiting: do you first operationalize the **catalog** capability (ingest databases, BI reports, columns — inventory the estate) or the **governance** capability (author meanings, attach ownership, ratify definitions)? Most orgs drift to catalog-first by default because catalog automates and governance historically didn't. That default is the mechanism of governance theater.

## Why it matters here

This is likely the most common **hidden** fork in the catalog. It's hidden because the decision is made *by the tool's default workflow*, not by the program. The connectors run, the library fills, stakeholders see progress, the project is declared a success, and the meaning layer stays blank.

The fork matters most for orgs in healthcare with large SQL estates: every concept ("active patient," "completed encounter," "surgery revenue") already has multiple technical definitions across dozens of reports, each buried in CTEs or multi-layer views. Catalog-first never reconciles them — it just lists the reports. Governance-first, once automatable via the extractor, can enumerate every technical definition, surface disagreements, and drive ratification.

## Poll (planned)

> Your org is rolling out a governance platform. Which capability did you actually operationalize first?
>
> A) Catalog first — ingested databases, tables, BI reports; governance will come later.
> B) Governance first — defined concepts, assigned owners, then populated.
> C) Both in parallel — deliberate simultaneous rollout.
> D) Drifted — nobody explicitly decided.

Expect D to be a plurality, A to be a strong second, B to be rare, C to be aspirational-but-rare.

## What happens if you got it wrong

- **A without ever progressing:** governance theater. Full catalog, empty meanings, stewards with nothing to steward.
- **B at manual scale:** heroic pilot, burns out the BI team, stalls after 3–5 domains.
- **B with extractor-scale automation:** the path this site argues for.
- **C without automation:** resource split breaks both sides.
- **D:** default to A outcome with extra confusion.

## Adjacencies

- Downstream of Fork 1 (tool vs. process) — applies to the tool-first branch.
- Upstream of Fork 4 (container vs. authored meaning) — how you choose to scope governance only matters once you've actually operationalized it.
- Resolved by the **automation asymmetry** being closed — see `catalog-vs-governance-automation-asymmetry.md`.

## Open questions

- Is there a cleaner rollout sequence that uses the catalog as *discovery* input to governance-first authoring, without letting catalog-first become the project's terminal state?
- How does this fork interact in orgs that bought a catalog-only tool (Alation-classic, Atlan-classic) vs. a full governance suite (Collibra)? Does the fork disappear or just relocate?
- What poll option captures orgs that went catalog-first deliberately as a staging strategy, with a real (not aspirational) plan to pivot to governance — and actually executed it?
