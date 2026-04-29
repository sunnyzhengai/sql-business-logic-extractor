---
name: Governance forks catalog
aliases: [the fork list, content site navigation, DG decision map]
see_also: [data-vs-metadata-ownership, govern-authored-meaning]
sources: []
---

## Definition

The content site is organized around a catalog of plain-language forks — binary or poll-style decisions every data org faces, most without realizing they are decisions. Each fork has a name, a poll, aggregate reader data, a tradeoff writeup, a "what happens if you got it wrong" pattern, and edges to adjacent forks. Stages/maturity are a *derived* view, not the primary navigation.

## Why it matters here

The wiki navigation *is* the fork list. Each Saturday whitepaper instantiates one fork. Reader answers across forks compose into a profile fingerprint — the mirror the reader leaves with. This is the site's differentiator: no DG content property currently lets readers self-locate via decisions they've made, with live peer aggregation.

## Current fork list (working draft — 2026-04-19)

Premise fork (sits above all others):

0. **Govern the data, or govern the metadata?** Most orgs silently pick one owner for both, and fail at the one they didn't mean to take on. See `data-vs-metadata-ownership.md`. Yang confirmed as earliest fork in the reader journey.

Yang's original set:

1. **Tool first, or process first?** Stand up Collibra and load reports in (chicken), or define and govern metadata before ingesting a single report (egg)?
2. **Top-down or bottom-up?** Exec mandate + no-report-ships-without-DG vs. ingest all existing tech debt and clean from there. Third option: middle-out (pick one domain, do both ends).
3. **Dedicated DG team or Govern-It-Yourself?** Central team vs. decentralized stewards. Hub-and-spoke is the accidental-common landing.
4. **Govern by container, or govern by authored meaning?** Every table/column/report gets a steward vs. stewardship attaches to the moment a definition is made concrete. See `govern-authored-meaning.md`. Signature fork — downstream of extractor thesis.
5. **IT owns DG, or Operations owns DG?** In healthcare add Clinical Informatics and Compliance as real options — this is often A/B/C/D, not A/B.
6. **Catalog first, or governance first?** The hidden fork that sits inside the tool-first branch. Most orgs drift to catalog-first by default because catalog automates and governance historically didn't. See `catalog-first-vs-governance-first.md`. Dedicated page because this fork exposes the extractor's real category: the first scalable governance-authoring primitive. See also `catalog-is-not-governance.md` and `catalog-vs-governance-automation-asymmetry.md`.
7. **Catch-up, or spec-first?** The development-lifecycle fork. Extract definitions from SQL that's already written, or author business terms and technical logic before any SQL exists? The site's non-obvious position: neither pure posture is correct — extractor-enabled selective promotion is the third path. See `catch-up-vs-spec-first.md`, `definitions-are-emergent.md`, and `spec-first-as-data-contract.md` for the intellectual anchor.
8. **App teams in DG, or out?** Does governance extend upstream into application teams (Epic build, Cerner config, SaaS admin), or stop at the analytics boundary? Determines whether DG is an analytics-only discipline or an enterprise discipline. Honest middle: coordination-minimal, approval-narrow — auto-notification on downstream-affecting changes, approval only for regulatory-impact class. See `app-teams-in-dg-vs-out.md` and the canonical `silent-upstream-break.md` scene.

Candidate additions (proposed, not yet confirmed):

9. **Definitions first, or lineage first?** Agree what "patient visit" means before tracing the number, or trace the number first and confront definitional ambiguity when it surfaces.
10. **Catalog is truth, or code is truth?** If code is truth, catalog is a derived refreshed view (extractor-native posture). If catalog is truth, it rots.
11. **Quality checks at ingest, or at consumption?** Block bad data at the door, or let it through tagged and let consumers decide.
12. **Start with analytics, or operational systems?** Loudest pain is downstream; leverage is usually upstream.
13. **Govern in the pipeline (CI/PR), or audit after the fact?** DevOps-mature culture vs. compliance-audit culture.
14. **Govern all data points, or govern transformations only?** Steward every field (including passthroughs that just surface a base column unchanged), or scope governance to the places where a transformation/definition was authored (CASE, DATEDIFF, aggregates, derived calculations, filter rules). Related to but distinct from fork 4: fork 4 asks *what makes something governable*; this one asks *where do you bother applying stewardship* once you've adopted a view. Extractor-native posture leans toward transformations-only — passthroughs carry no authored meaning to govern.

Target: 8–12 confirmed forks. Each confirmed fork gets its own concept page as it matures.

## Open questions

- Is fork 0 (data vs. metadata ownership) a fork, a premise, or both — rendered as a fork for UX reasons even though it sits above every other decision? Currently treating as a fork per Yang's call.
- How to handle forks where the "right" answer depends heavily on org size, regulation, or domain — does each fork need a "when this answer applies" qualifier, or do we keep the framing universal?
- What's the cutoff for including a fork — must it be answerable A/B by an executive in under 30 seconds, or is longer deliberation acceptable?
- Naming discipline: every fork name must work in a plain-language poll. No "lineage," "metadata" as first-pass vocabulary unless it has been earned earlier in the reader's journey.
