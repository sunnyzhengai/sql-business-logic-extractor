---
date: 2026-04-19
status: accepted
---

## Context

Designing a personal-brand content site / knowledge base to publish methodology for SQL logic metadata extraction and data governance. Weekly Saturday cadence. Publishing under personal byline (not under any commercial brand, not under employer brand). Wanted a structural frame that would (a) organize the site's knowledge base, (b) drive the editorial calendar, and (c) differentiate from existing DG thought-leadership content.

Two candidate frames evaluated:

1. **Stage-based maturity ladder.** An opinionated 5-stage model (Governance theater → Lineage truth → Impact fluency → Active governance → Adaptive governance) with the SQL extractor positioned as the unlock mechanism between stages 0 and 1.

2. **Fork-based diagnostic map.** A catalog of ~8–12 plain-language decisions every data org faces, most without realizing they are decisions. Polling at each fork; aggregated peer data; profile fingerprint composed from a reader's answers across forks.

## Decision

**Fork-based model.** The wiki is a map of decisions, not a ladder of stages. Maturity is a derived view — how many forks you handled on purpose vs. how many happened to you.

## Alternatives considered

**Stage-based ladder rejected** for the following reasons surfaced in discussion:

- Ladders preach from above. Readers locate themselves in scenes they lived through, not in labels they are assigned.
- Stage names carry jargon that requires a glossary ("lineage," "impact fluency"). The executive writing the check doesn't know these terms and shouldn't need to.
- Aspirational naming sells the destination. People move when wounded, not when shown a brochure.
- A linear ladder denies the reality that an org can be advanced in one domain (reporting) and primitive in another (data science) at the same time.
- Ladders don't demand reader commitment. A poll does. Commitment is the hook.
- A stage model could be rendered as a static PDF. A polling+aggregation+fingerprint model cannot. The interactive nature is the moat.

## Consequences

- **Site navigation is the fork catalog.** Top-level IA is the list of forks, not a stage hierarchy. See `concepts/governance-forks-catalog.md`.
- **Each Saturday whitepaper instantiates one fork.** First four Saturdays: Fork 0 (data vs. metadata ownership) or Fork 1 (tool vs. process), then Fork 4 (container vs. authored meaning) for the signature thesis, then Fork 6 (definitions vs. lineage) for depth, then an org-politics fork for breadth.
- **Plain-language vocabulary required across the site.** "Lineage" and "metadata" are not first-pass vocabulary — they must be earned through the reader's own journey.
- **Reader profile fingerprint is a first-class feature, not a future add.** Answers across forks compose into a portrait; the site shows how the reader compares to peers; the mirror is the artifact readers leave with.
- **Concept pages in `wiki/concepts/` map 1:1 to forks + load-bearing ideas.** New forks or supporting ideas get pages as they stabilize.
- **Forbidden vocabulary discipline** — "maturity level," "stage X," "Level 1–5" language is off-site. If readers want a stage summary, the site derives one from their fork answers on demand.
- **Pedagogy bias** — concrete scenes (e.g., Patient A01) over abstract frameworks. Every fork should be introduced via a scene a reader has lived through.
