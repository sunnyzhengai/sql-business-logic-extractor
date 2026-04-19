---
name: Catch-up vs. spec-first (fork)
aliases: [extract-then-govern vs. govern-then-code, contract-first development for data]
see_also: [definitions-are-emergent, spec-first-as-data-contract, govern-authored-meaning, sql-as-definitional-moments, catalog-vs-governance-automation-asymmetry, governance-forks-catalog]
sources: []
---

## Definition

Two postures toward the **direction of meaning flow** across the software development lifecycle: **catch-up** (extract authored definitions from SQL that's already been written, because that's where meaning actually lives today) or **spec-first** (author business terms and technical logic in the governance platform before any SQL is written — contract-first development applied to data). Most orgs are 100% catch-up, not by choice but by throughput constraint. Pure spec-first is the aspirational ideal and usually fails at execution. The extractor makes a third posture viable: **catch-up as extraction pipeline, spec-first as selective promotion.**

## Why it matters here

This is the development-lifecycle fork. It's orthogonal to the program-sequencing forks (1 tool-vs-process, 6 catalog-vs-governance) and the organizational forks (2, 3, 5). It asks a more fundamental question: **which direction does meaning flow — upstream-to-code, or code-to-upstream?**

The fork is the clean place for the site to take a non-obvious position. The naive reading says "spec-first is obviously correct, catch-up is a legacy workaround." That reading is wrong. Spec-first fails for four compounding reasons:

1. Definitions are **emergent** — see `definitions-are-emergent.md`. You don't know what the "right" definition is until you've built it three times.
2. Definitions are **legitimately plural** — see Patient A01. A single upstream authoritative definition silently wrongs the workflows that needed variation.
3. **Throughput reality.** Healthcare BI is reactive; spec-approval loops get routed around. Governance-as-gatekeeping dies under real deadline pressure.
4. **Spec-reality divergence.** Docs-only specs decay faster than the code they describe, unless runtime-enforced.

Pure catch-up is the current state and doesn't scale — every concept carries multiple uncoordinated technical definitions.

The extractor-enabled third posture: build fast → extract definitions automatically → measure variance across reports to detect stability → promote stable concepts to governed business terms → require spec-first *for those specific concepts* going forward. You earn the right to require spec-first by first proving the concept is stable enough to deserve it. The *earning* is what was impossible without the extractor.

## Poll (planned)

> How do you (or your org) handle the gap between business meaning and SQL today?
>
> A) We're 100% catch-up. It's chaos but it's what we can afford.
> B) We're catch-up today; spec-first is our destination.
> C) We're spec-first for some high-stakes domains (quality measures, financial KPIs); catch-up elsewhere.
> D) We're fully spec-first — business terms are defined before any SQL is written.
> E) I've never thought about this as a choice.

Expected distribution: E plurality, A strong second, B aspirational-third, C rare-but-real, D vanishingly rare.

## What happens if you got it wrong

- **A with no plan to change:** the catch-up tax compounds. Every report's definition of "active patient" diverges silently. Governance stays permanently impossible.
- **B forever (B without actually pivoting):** perpetual "future state." Slides exist, practices don't.
- **D in all domains:** bureaucratic bottleneck. BI teams route around governance. Spec-reality divergence sets in within a quarter.
- **Naive C (wrong concepts chosen for spec-first):** you picked the concepts leadership asked about, not the ones that were actually stable. Canonicalized guesses. Expensive to reverse.
- **Extractor-enabled C (concepts chosen by stability evidence):** the target state. Promotion is earned, not asserted.

## Adjacencies

- Counter-force: `definitions-are-emergent.md` — without which this fork reads as a straw man against catch-up.
- Intellectual anchor: `spec-first-as-data-contract.md` — naming the field this connects to.
- Unblocked by: `catalog-vs-governance-automation-asymmetry.md` — the extractor is what makes extract-then-promote viable at scale.
- Exposed by: `govern-authored-meaning.md` — catch-up works because authored meaning is already in code, waiting to be read.

## Open questions

- What's the operational signal that a concept has become "stable enough" to promote to spec-first? Candidate signals: cross-report variance has converged; number of open tickets on its interpretation has dropped; regulatory/audit pressure has named it.
- Can the extractor measure concept stability directly — e.g., across N reports' definitions of the same concept over time, how much variance exists, is it increasing or decreasing?
- What's the right lifecycle for a concept that was promoted and then proves unstable? Demotion protocol?
- In healthcare specifically: are there concept classes (quality measures, registries) where spec-first is already mandatory regardless of stability, because external regulators have pre-canonicalized the definition?
