---
date: 2026-05-25
status: proposed
---

## Context

After the MyChart pilot validation (11 views → 4 communities + clean
matrices), Yang reframed the project's evolution as three layers:

- **(a) Catalog and community detection** -- what queries exist, how do
  they cluster by shared substrate?
- **(b) Shape comparison and semantic extraction** -- per-query
  structure, side-by-side comparison, nuance that the unified
  community view flattens out.
- **(c) Model and data comparison** -- "full circle": take proposed
  unified models, run them against real data, show aggregated results
  so stewards can verify semantic equivalence before ratifying a
  consolidation.

This decision formalizes that framing as the project roadmap. The three
layers map cleanly to the data-observability pyramid: identification ->
comparison -> verification. Each layer answers a different steward
question; each is independently shippable; each builds on the previous.

## Decision

Adopt (a)/(b)/(c) as the project's three-layer roadmap, with these
boundaries:

### Layer (a) -- Catalog and community detection

**Question it answers.** "What queries exist in this estate, and which
ones operate on shared data?"

**Status.** Views handled and validated (MyChart pilot, 4 communities).
Procs designed (see `wiki/decisions/2026-05-24-stored-proc-extraction-approach.md`)
but not implemented. The on-prem extract scripts in
`scripts/onprem_extract/` close the manual-upload bottleneck.

**Outstanding work.**

1. Run `tools/operate/survey_proc_categories.py` against the MyChart
   proc corpus to verify the read-only assumption.
2. Implement Phases C / D / E of the proc-extraction plan per survey
   findings.
3. Confirm mixed views+procs flow through the same community-detection
   pipeline without regression (already the design; needs validation
   with a mixed corpus).

**Boundary.** Layer (a) stops at "here are the clusters." It does NOT
say which views inside a cluster should be consolidated -- that's
(b)'s job. It does NOT say whether the consolidation preserves
semantics -- that's (c)'s job.

### Layer (b) -- Shape comparison and semantic extraction

**Question it answers.** "Inside this cluster, what are the structural
nuances between queries -- where do they agree, where do they diverge?"

**Status.** The data exists: `ScopeV1` is already a tree of nested
scopes per view (main / CTE / subquery / derived table / set-op
branch). The renderer in `tools/p50_present/community_html.py`
flattens this tree into a single view-to-tables visualization, which
is great for big-picture clustering but poor for per-query nuance.

**Outstanding work.**

1. NEW per-query shape renderer (`tools/p50_present/query_shape.py`?).
   Renders ONE query's scope tree as a hierarchical diagram --
   CTEs nested inside the main scope, subqueries inline with their
   parent, joins visible per scope. Probably HTML + Mermaid or SVG.
2. Side-by-side comparison view: given two query names, render their
   shape trees in adjacent panes with shared structural elements
   (table refs, join shapes) highlighted across both.
3. Existing community renderer stays untouched -- the per-query and
   side-by-side views complement, don't replace.

**Boundary.** Layer (b) shows structural difference. It does NOT
verify whether two structurally-different queries return the SAME
DATA -- that's (c)'s job. It does NOT propose a consolidation; that's
the modeling spec's job, which already exists for views.

### Layer (c) -- Model and data comparison

**Question it answers.** "Does my proposed unified model produce the
same data as the views it claims to replace?"

**Status.** Not started. Most ambitious. Closes the loop between
static analysis and operational reality.

**Outstanding work.** Phased to avoid scope explosion:

1. **Phase C1 -- Aggregation SQL generation.** Given a proposed model
   and the views it replaces, generate side-by-side aggregation
   queries: row counts, COUNT DISTINCT per key column, SUM/AVG/MIN/MAX
   per numeric column. Output: a `.sql` file + a markdown report
   template the customer's BI environment runs.
2. **Phase C2 -- Result comparison renderer.** Given the result CSV
   from Phase C1's execution, produce a markdown diff report showing
   discrepancies (proposed vs existing) per aggregate. Equivalence
   verdict per metric (within tolerance / divergent).
3. **Phase C3 (deferred / optional) -- Direct execution.** Run the
   aggregation SQL ourselves against a configured DB connection.
   Deferred because (i) requires IT approval for DB access,
   (ii) HIPAA risk for healthcare customers, (iii) couples our tool
   to authentication / network state that's better kept external.

**Boundary.** Layer (c) stops at aggregate-equivalence verdicts. It
does NOT do row-level diff (HIPAA risk for healthcare data; PII
exposure for any sensitive domain). Row-level diff is a separate
feature gated behind explicit data-handling controls.

## Alternatives considered

**A. Roll (b) and (a) together.** Build the per-query shape renderer
inside the community detection step so they ship together. Rejected:
they answer different questions and have different cadences. (a) is
done for views and almost-done for procs; (b) is a fresh capability.
Coupling them slows (a)'s completion.

**B. Build (c) ourselves with embedded DB execution.** Connect to
customer DBs, run aggregation SQL, render the diff in our tool.
Rejected: (i) authentication / network access turns the tool into a
data-comparison service, very different product; (ii) HIPAA /
regulated-data customers can't approve an outside tool reaching into
their warehouse without months of compliance review; (iii) customers
already have BI environments that run SQL -- we should slot in, not
duplicate.

**C. Skip (c) entirely; ship (a) + (b) and call it done.** Rejected:
without (c), stewards have to trust static analysis alone. For the
high-stakes consolidations the tool proposes (replacing 7 views with
one model), some form of data-side verification is the difference
between "we'll think about it" and "we'll ratify it." (c) is the
trust-building feature.

## Consequences

### Ordering

Recommended sequence:

1. Finish (a) for procs. ~3-5 days once the survey runs.
2. Build (b) per-query shape renderer. ~3-5 days.
3. Build (b) side-by-side compare. ~2-3 days.
4. Design (c) Phase C1 + C2 in detail. ~1 day.
5. Build (c) Phase C1 (aggregation SQL generation). ~3-5 days.
6. Build (c) Phase C2 (result comparison renderer). ~3-5 days.
7. (c) Phase C3 -- defer until a customer specifically asks for
   embedded execution; then revisit with their security team.

Each step has a natural pause point. After each, the artifact set is
self-contained and the project remains useful even if work stops
there.

### Pause points

After (a) procs land: customer can ingest mixed corpora and get
community clusters. Useful on its own.

After (b) per-query shape: customer can see per-query nuance and do
side-by-side compare. Useful for steward / data-modeler conversations.

After (c) C1: customer can take generated aggregation SQL and run it
in their own BI environment. Useful even without our diff renderer.

After (c) C2: full closed loop -- diff report shows whether two
shapes return equivalent data.

### What this locks in

- The three layers each get their own `tools/` subtree if needed,
  and the existing structure (`p10_extract`, `p20_index`, ...,
  `p50_present`) already accommodates them. (a) is "catalog + cluster"
  (extract + index + analyze + summarize). (b) is "render per-query"
  (extends `p50_present`). (c) is "validate" (new phase, probably
  `p60_validate`).
- Layer boundaries are STRICT. Don't put data-comparison code in
  the per-query renderer; don't put per-query shape rendering in
  the community detector. Each layer answers one question.
- Customer-facing narrative becomes "catalog → compare → verify."
  Three concrete deliverables a sales conversation can point at.

### Risks / known unknowns

- **Database access in (c).** Even Phase C1 (SQL generation, not
  execution) needs to KNOW the table layouts customers use. Schema
  drift between SQL-as-text and SQL-as-executable is real. The
  generated aggregation SQL needs to be customer-runnable without
  edits -- which means we have to render real schema-qualified table
  names, not the canonicalized identifiers our analysis uses.
- **Per-query shape rendering complexity.** Some views have deeply
  nested CTEs (5+ levels) and dozens of subqueries. The layout
  problem is non-trivial. May need an interactive (zoom / expand)
  renderer rather than a static SVG. Mermaid handles up to ~30
  nodes cleanly; beyond that it gets noisy.
- **Per-query view doesn't obsolete community view.** Both are
  useful; both stay. The community view answers "where are the
  clusters?"; the per-query view answers "what's different inside
  this cluster?" Customers will want to toggle between them.

## See also

- `wiki/decisions/2026-05-24-stored-proc-extraction-approach.md` --
  the proc extension plan for Layer (a).
- `docs/parsing_field_guide.md` -- the operational reference that
  grows as new parse patterns surface in any of the three layers.
- `scripts/onprem_extract/` -- the laptop-side DDL extractors that
  feed Layer (a)'s catalog.
- `tools/p50_present/community_matrix.py` -- the current matrix
  artifact for Layer (a)'s clusters; Layer (b)'s per-query shape
  renderer will be a sibling module.
