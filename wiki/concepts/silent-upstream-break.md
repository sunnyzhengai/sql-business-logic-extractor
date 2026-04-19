---
name: Silent upstream break
aliases: [the Monday picklist change, Epic sneezes and BI catches pneumonia, the HEDIS report that broke, the CEO's stale slide]
see_also: [app-teams-in-dg-vs-out, app-config-id-as-coordination-key, govern-authored-meaning, patient-a01-scene]
sources: []
---

## Definition

A canonical healthcare DG failure pattern: an operational application team makes a routine configuration change (splits a picklist value, retires a code, reassigns a workflow owner) without coordinating with BI/analytics. Every downstream report that filtered on or joined to the old value silently begins returning incorrect results. The break is invisible for days, weeks, or a full regulatory reporting cycle — discovered only when an end user files an incident, a regulatory submission is rejected, or a leader asks why a trusted metric moved.

## Why it matters here

This is the second-class canonical scene alongside `patient-a01-scene.md`. Patient A01 kills container-level ownership in one paragraph; silent-upstream-break kills analytics-only DG in one paragraph. Every healthcare BI analyst has lived a version of it.

## The scene

Tuesday morning. An Epic analyst splits the "Outpatient" visit-type picklist into three sub-values to support a new clinic workflow. The change passes Epic build review (it's internally sound) and goes to prod with the next release.

Every downstream report that filtered `visit_type = 'Outpatient'` now returns only records created before Tuesday.

- **Day 3:** weekly census report runs. Numbers look low but within tolerance; nobody flags it.
- **Day 10:** outpatient volume dashboard shows a 40% drop. A VP asks the BI lead why. BI lead guesses seasonality.
- **Day 17:** the quarterly HEDIS submission runs. The measure denominator is wrong. Submission is rejected on QA.
- **Day 24:** the CEO's board presentation includes the outpatient volume slide. CEO asks finance why revenue is inconsistent with volume. Several meetings follow.
- **Day 31:** somebody finally traces it to the Tuesday picklist change, four weeks prior.

None of the root-cause chain was visible to anyone until the last 24 hours. The app team didn't know BI depended on that value. BI didn't know the app team had changed it. Governance didn't cover the seam.

## How it would have gone differently

In an Epic shop running the INI-Item coordination mechanism (see `app-config-id-as-coordination-key.md`):

Tuesday morning, same scenario. The Epic analyst identifies the INI-Item they need to modify to split the Outpatient picklist. Before making the change, they look it up in Collibra — the same way they'd look it up in their own documentation. Collibra shows: "Referenced by 47 downstream queries. 4 feed the HEDIS 2026 submission pipeline. 1 feeds the weekly CEO outpatient-volume deck." The analyst pauses the change, pings BI, and a 10-minute coordination call prevents the four-week cascade.

This is not hypothetical. The ingredients — INI-Item catalog in Clarity, extractor-derived query lineage, Collibra as the surfacing UI — exist or are achievable now.

## What this scene establishes

1. **Authored meaning originates upstream**, not in SQL. The definition of "Outpatient" changed; every downstream query inherited the break.
2. **Analytics-only DG is structurally insufficient.** Placing the boundary of governance *after* the point of authorship guarantees silent breaks.
3. **Blast-radius detection is not optional infrastructure.** Without it, coordination requires human tribal knowledge that scales with neither the estate nor the staff turnover.
4. **Regulatory and boardroom pain is the political leverage.** Data leaders trying to justify extended DG scope can't use analytics-only stories to win the fight. The regulatory/board break is the story that wins.

## Open questions

- What's the typical detection-to-root-cause time in healthcare today? (Days? Weeks?) Is it getting better or worse with larger data estates?
- Does the extractor's blast-radius capability (upstream change → affected reports) need anything beyond the existing lineage output, or is it already latent and just unsurfaced?
- In Epic specifically: what app-team change classes most commonly trigger this pattern? Picklist edits, workflow reassignment, note-type changes, billing-rule configuration, order-set modification?
- How to present the blast-radius report in a way app analysts will actually read before releasing a change? (Format, venue, cadence, cognitive load matter.)
