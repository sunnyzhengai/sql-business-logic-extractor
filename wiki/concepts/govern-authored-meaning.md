---
name: Govern authored meaning, not the container
aliases: [govern the transformation not the table, point of semantic authorship, meaning-first governance]
see_also: [data-vs-metadata-ownership, sql-as-definitional-moments, patient-a01-scene, app-teams-in-dg-vs-out, silent-upstream-break, governance-forks-catalog]
sources: []
---

## Definition

The governable unit is the **(transformation, context, definition)** triple — not the table, not the column, not the report. Stewardship attaches to the moment a definition is made concrete in code: the CASE statement that decides who counts as an outpatient patient, the join that pulls in the surgery flag, the filter that bounds an "active patient" cohort.

## Why it matters here

Asset-first governance (every table, column, and report gets a steward) forces stewards to defend containers whose contents they didn't author. Almost no one can do that well, so most stewardship becomes theater. Meaning-first governance attaches ownership to the moment a definition was actually made, which is both knowable (it's in git) and bounded (this definition, this context, this purpose).

This is where the SQL logic extractor becomes structurally irreplaceable rather than merely convenient. Tools that govern at the table or column layer are governing the wrong object. The extractor detects definitional moments — not just data movement — which makes meaning-first governance operationally possible for the first time.

Practical consequence: the unit of governance multiplies (there are more meaningful transformations in a health system than there are tables), but each unit shrinks, has a real author, and can actually be defended.

**Upstream extension.** Taken seriously, this principle implies that authored meaning originates *before* SQL. A query filtering on `visit_type = 'OP'` is interpreting a definition authored upstream in the operational application (Epic build, Cerner config, SaaS admin). That makes app-team changes a first-class governance concern, not an out-of-scope infrastructure event. See `app-teams-in-dg-vs-out.md` for the fork this implies, and `silent-upstream-break.md` for the canonical failure pattern when governance stops at the analytics boundary.

## Open questions

- How to attribute authorship when SQL has been copy-pasted and evolved across dozens of reports over years? Is "current owner of the artifact" enough, or do we need definitional-lineage that predates the current file?
- How does a catalog UI present transformation-level governance without overwhelming users trained on asset lists? What's the default view?
- When does asset-level governance still legitimately apply (reference data, master data, physical sensitivity overlays)?
- What's the right granularity — one governable unit per query, per CTE, per CASE expression, per measure?
