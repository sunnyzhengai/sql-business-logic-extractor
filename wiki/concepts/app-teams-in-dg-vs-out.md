---
name: App teams in DG, or out (fork)
aliases: [analytics-only DG vs. enterprise DG, producer accountability, app-analyst seat at the table]
see_also: [silent-upstream-break, app-config-id-as-coordination-key, govern-authored-meaning, catch-up-vs-spec-first, governance-vs-compliance-layers, governance-forks-catalog]
sources: []
---

## Definition

Does data governance extend **upstream into operational application teams** (Epic build, Cerner config, custom app dev, SaaS admin) — or does it stop at the analytics/BI boundary? Traditionally, app teams receive app/workflow requests; BI teams receive reporting requests; the two rarely coordinate. App-driven changes to picklists, category splits, retired codes, and workflow rules silently break downstream reports, often undetected until a regulatory submission fails or an executive asks why a metric moved.

## Why it matters here

This fork determines whether DG is an **analytics-only** discipline or an **enterprise** discipline — and the reader's answer reveals how they think about the scope of "authored meaning."

The sharp truth under the fork: **authored meaning doesn't originate in SQL.** A query that filters `WHERE visit_type = 'OP'` is *interpreting* a definition that was authored upstream by someone who added, split, merged, or retired that picklist value in the operational system. The SQL is downstream of a decision it didn't make. Exclude app teams from DG and you've placed the boundary of governance *after* the point where definitions actually originate — which is structurally incoherent with Fork 4's "govern authored meaning" position.

See `silent-upstream-break.md` for the canonical scene.

## Poll (planned)

> How does your org handle coordination between application changes (Epic build, Cerner config, SaaS admin changes) and downstream reporting?
>
> A) Separate worlds. App team changes things; BI discovers the break from an end-user incident.
> B) Informal — app analysts sometimes loop BI in, depending on who knows whom.
> C) Structured notification — app changes that affect meaning generate an alert to DG stewards.
> D) Structured approval — a defined class of app changes requires DG sign-off before release.
> E) Joint stewardship — app teams are full participants in DG workflows.

Expected distribution: A dominant, B common, C–E rare-to-aspirational.

## The honest third path

The strong "app teams fully in DG" position runs into real friction: scope explosion, expertise mismatch, political territory, and under-specified "participation." The strong "app teams out of DG" position is what's causing the silent-break pattern.

The honest middle is **coordination-minimal, approval-narrow**:

- **Automatic notification** for any app change that affects downstream meaning (picklist values, category splits/merges, retirements, workflow ownership changes, code set updates).
- **Required approval** only for a narrow class: regulatory-impact changes (HEDIS, DSRIP, MIPS, CMS quality measures, state registry submissions), financial-audited KPIs, and board-reported metrics.
- **Everything else:** notify + auto-generate blast-radius report + let downstream teams react before the change lands in prod.

The extractor is the operational primitive that makes this tractable. When an upstream change is registered, the extractor already knows which downstream queries reference the affected element and which reports depend on those queries — blast radius is automatic, not a meeting.

**The hardest piece — getting app analysts to actually check before changing — has a clean answer in Epic shops.** Epic's INI-Item Number is the dictionary key app analysts already have to identify as part of their change process. Ingest the INI-Item catalog from Clarity into Collibra, cross-reference each INI-Item to downstream SQL via the extractor, and the app analyst's existing workflow step *becomes* the lookup-before-change moment. No new process, no new discipline to enforce — piggyback on the ID they were going to name anyway. See `app-config-id-as-coordination-key.md` for the full mechanism, coverage gaps, and go-to-market implications.

## What happens if you got it wrong

- **A forever:** the silent-break pattern compounds. Regulatory submissions become a standing compliance risk.
- **E without automation:** DG becomes a bottleneck on app-team velocity. App teams route around it. Shadow changes proliferate.
- **D applied too broadly:** same failure mode as E — governance becomes gatekeeping, gets routed around.
- **C applied but no one reads the notifications:** failure mode masquerades as success. A dashboard of ignored alerts.
- **C with blast-radius reporting + clear triage paths:** the operational target.

## Adjacencies

- Scope implication of: `govern-authored-meaning.md` — authored meaning extends upstream into app configuration.
- Canonical scene: `silent-upstream-break.md`.
- Orthogonal to: Fork 5 (IT vs. Ops vs. Clinical Informatics owns DG) — program ownership is a different question than table membership.
- Intellectually adjacent to: **data mesh** / **domain ownership** thinking — app teams are "producers" in mesh terms. The site takes the producer-accountability point without needing the full mesh apparatus.

## Open questions

- What exactly qualifies as "an app change that affects downstream meaning"? Is there a rule set the extractor can apply automatically, or does it require manual classification?
- How do you get app-team leadership to opt into notification workflows without executive air cover? Is there a lightweight first-step an analytics director can pilot unilaterally?
- Does Collibra (or equivalent) already have upstream-change-notification primitives that are underused, or is this a genuine tooling gap?
- Is there a maturity sub-ladder inside this fork — e.g., A → C → selective D — that represents a realistic adoption path rather than an all-at-once leap?
