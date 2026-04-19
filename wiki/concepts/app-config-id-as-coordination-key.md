---
name: Application config IDs as governance coordination keys
aliases: [Epic INI-Item as coordination key, lookup-before-change, piggyback on the ID the analyst already uses]
see_also: [app-teams-in-dg-vs-out, silent-upstream-break, govern-authored-meaning, sql-as-definitional-moments]
sources: []
---

## Definition

Every operational application carries internal IDs that uniquely identify its configuration items (dictionary keys, object IDs, build handles). When those IDs are ingested into the governance catalog and cross-referenced to the downstream queries and reports that depend on them, **an app analyst can look up blast radius before making a change — using the ID they were going to name anyway.** The coordination layer imposes no new process step; it attaches to an existing one.

In Epic specifically, this ID is the **INI-Item Number** — the dictionary index that connects a front-end application element to its backend database column. INI-Item catalog is already materialized in Clarity and can be pulled routinely.

## Why it matters here

This is the operational answer to the hardest open question on the `app-teams-in-dg-vs-out.md` fork: *how do you get app analysts to actually check downstream impact before making a change?* Every answer that requires a new step fails in practice — app teams have their own cadence, change-control processes, and workload; additive governance process gets routed around. The INI-Item mechanism sidesteps that problem because the lookup happens via an ID the analyst already has to identify as part of their normal change.

Concretely, the operational loop is small:

1. **Ingest** the INI-Item catalog from Clarity into Collibra (nightly refresh matches Clarity's cadence).
2. **Cross-reference** each INI-Item to the downstream SQL that touches its corresponding Clarity column. This is the extractor's job — it already knows which queries reference which columns; adding the INI-Item join is a minor enrichment.
3. **Surface** in Collibra: for INI-Item X, show the N reports, M queries, and K business terms that depend on it, including any flagged as regulatory-impact or board-level.
4. **Use** in the app team's existing change workflow: analyst looks up INI-Item → sees blast radius → decides whether to coordinate before release.

That's the feature. It is shockingly close to ship-ready from a design standpoint.

## Product / market implication

Epic is dominant in the US healthcare provider IT market. Every Epic shop has precisely the `silent-upstream-break.md` pain pattern. A Collibra connector that ships with **first-class INI-Item ingestion + extractor-derived downstream cross-reference** is a day-one, concretely-valuable feature that requires the extractor to function — no competitor's Collibra connector does this.

This is the **go-to-market wedge for the healthcare Epic-shop segment.** Not a nice-to-have; a defensible, segment-specific feature where the prerequisite (Yang's extractor) happens to be proprietary and the segment happens to be enormous.

## Pressure test — real gaps to name

- **Coverage.** Not every Epic change has a clean INI-Item. Profile settings, user-level overrides, Reporting Workbench configurations, and some operational parameters don't sit in the Foundation dictionary the same way. The mechanism covers *consequential* changes but isn't universal. Scope this carefully in marketing.
- **Clarity latency.** Clarity typically refreshes nightly. If a change releases before Clarity refreshes, the pre-change state is what the analyst sees. For lookup-before-change this is usually fine (the analyst is checking existing dependencies before they break them), but worth flagging explicitly.
- **Extractor precision.** The "INI-Item → downstream reports" chain has three hops: INI-Item → Clarity column (Epic-provided) → SQL usage (extractor's job) → report / dashboard / submission (extractor's job). Confirm the chain is reliable end-to-end before pitching it.

## Generalization beyond Epic

The pattern isn't Epic-specific — every vendor has internal configuration IDs:

- **Cerner / Oracle Health** — internal ID schemes in the Cerner Millennium dictionary / tenant configuration layer.
- **MEDITECH / Athena / eClinicalWorks** — each has equivalents with varying exposure.
- **Workday, SAP, Kronos, ServiceNow** — each has object IDs at various layers.
- **Custom in-house apps** — may or may not expose one; often don't unless developers intentionally add a governance-friendly surface.

Epic is the most tractable because Clarity pre-packages the catalog. For other vendors the connector needs a per-vendor adapter. Start with Epic because the ingredients are on the shelf.

## Open questions

- Precise list of Epic change classes that don't route through an INI-Item. Where does the mechanism leak coverage?
- What's the right way to handle the case where a change *creates* a new INI-Item (the ID doesn't exist in the catalog yet)? Lookup returns empty — is that interpreted correctly?
- For Cerner shops: is there a single catalog analogous to Clarity's INI-Item table, or is the equivalent data fragmented across several sources?
- For the Collibra connector's MVP: Epic-only, or do we add a second vendor adapter to prove the pattern generalizes before pitching?
- How to present the blast-radius lookup in Collibra's UI so an app analyst (not a data analyst) can use it without training?
