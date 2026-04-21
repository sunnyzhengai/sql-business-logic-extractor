---
date: 2026-04-20
status: proposed
---

## Context

`offline_translate.py` (the no-LLM English translator) produces weak descriptions for complex SQL constructs. A 2026-04-20 test run on 11 representative queries from `tests/output/` surfaced a consistent pattern: every high-impact failure mode shared one root cause — flat case-by-case templates miss on nested or composite constructs and fall back to opaque placeholders that drop semantic content actually present in the SQL.

Enumerated findings from that run:

| SQL construct | Current English output | What's lost |
|---|---|---|
| `CASE ADT_PAT_CLASS_C WHEN 1 THEN 'Inpatient' WHEN 2 THEN 'Outpatient' …` | "Categorization with 3 condition(s)" | Every category label, every mapping |
| `COUNT(CASE WHEN DISCH_DISPOSITION_C = 1 THEN 1 END)` | "Count of Discharge Disposition C = 1 Then 1 End" | Raw SQL keywords leaking through |
| `LAG(HOSP_DISCH_TIME) OVER (PARTITION BY PAT_ID ORDER BY HOSP_ADMSN_TIME)` | "Value from previous row" | Column, partition key, and order all dropped |
| `CAST(SUM(CASE WHEN days_to_readmit <= 30 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) * 100` | "Type-converted value" | This is literally a readmission rate |
| `DATEDIFF(DAY, d1.HOSP_DISCH_TIME, d2.HOSP_DISCH_TIME)` | "Number of days between Hospital Discharge Time and Hospital Discharge Time" | Alias disambiguation lost (d1 = index, d2 = readmit) |
| `NOT col IS NULL` mixed into every description | Multi-filter noise appended to every description | Technical-vs-business filter distinction absent |
| `DATEDIFF(YEAR, BIRTH_DATE, GETDATE())` | "Age in years (at Getdate()" | Template formatting bug (missing paren) |

Fixing these case-by-case requires N separate template improvements, with drift risk as the SQL corpus evolves. A structural fix is called for instead.

## Decision

Refactor `offline_translate.py` to adopt **recursive decomposition + a pattern library** as its architecture. The general principle lives in `concepts/recursive-translation-principle.md`.

Three-layer architecture:

1. **Recursive translator.** Walks the sqlglot AST. Base case: raw column reference → schema lookup with enum expansion and abbreviation handling. Inductive case: function or construct node → pattern template applied to recursively-translated sub-expressions.
2. **Pattern library.** Registry of templates keyed by `(node_type, arg_shape)`. Supports single-function patterns (e.g., `DATEDIFF(unit, d1, d2)`) and multi-level composite patterns (e.g., `CAST(SUM(CASE-WHEN-THEN-1-ELSE-0) AS FLOAT) / COUNT(*) * 100` → *percentage*). Keys likely reuse the structural-signature infrastructure already in `compare.py`.
3. **Unknown-pattern handling.** Unknown patterns emit structural decomposition (e.g., "LAG applied to Hospital Discharge Time, partitioned by Patient ID, ordered by Admission Time") as the fallback — never an opaque placeholder — and are logged to a `patterns/needs-authoring/` surface that becomes part of the pipeline output. Unknown-column encounters follow the parallel path on the schema side.

## Alternatives considered

- **Incremental case-by-case template improvements.** Rejected. Each failure is its own fix; the set grows unboundedly; drift compounds; governance coverage stays invisible.
- **LLM-based translation.** Out of scope for the offline path by design — `offline_translate.py` exists precisely to run in restricted environments (Yang's work setting) where LLMs are unavailable.
- **Richer static fallback strings.** Rejected. Produces slightly more legible placeholders (e.g., "Case expression over ADT_PAT_CLASS_C with 4 branches") but still doesn't *recover* semantic content. Same class of failure with fresh lipstick.

## Consequences

- **Pattern library becomes a first-class governance asset.** It is a living semantic specification of the organization's SQL vocabulary. Coverage becomes measurable — "what percentage of our SQL corpus translates from existing templates" is now a tracked metric.
- **Unknown patterns are signals, not failures.** The `patterns/needs-authoring/` surface is a governance backlog: a steward reviews and authors templates, adding to the library.
- **Unknown columns are the parallel signal.** Columns missing from `clarity_schema.yaml` or whatever dictionary is in use become authoring backlog items on the schema side. Same shape.
- **Enum/schema integration moves into the base case.** Column-level description + enum mapping is the terminal recursion step; no more post-processing passes.
- **Test suite shifts to golden-file style.** `tests/test_offline_translate.py` golden-files the expected English output per test case; new SQL constructs added to the corpus fail loudly until a pattern is authored. The test output set generated on 2026-04-20 can seed the initial golden files.
- **Initial migration cost is real but bounded.** Port existing case-by-case templates into the new pattern registry; most become drop-in entries. The recursive walker + unknown-handler + registry are the new code.
- **Resolve-layer alias preservation needs separate attention.** The `d1`/`d2` disambiguation problem ("between Hospital Discharge Time and Hospital Discharge Time") is not solved by translation recursion alone — it needs CTE/subquery alias context preserved through L3. Flagged as a downstream investigation, not a blocker for this refactor.
- **Documentation is automatic.** The pattern library is self-documenting; each template entry contains both the recognition signature and the translation template, so the library doubles as a semantic dictionary for the SQL corpus.

## Open questions

- Pattern identity keys: simple `(name, arity)` vs. full AST hash vs. structural signature. Start with structural signature (reuse compare.py infrastructure); widen if needed.
- Template language: free-text with placeholders vs. structured DSL. Start with free-text; upgrade if validation/reuse pain emerges.
- When composite patterns conflict (same node matches multiple signatures), resolution policy: most-specific wins? Priority-ordered list? Ambiguity flag for steward review?
- Incremental migration vs. clean rewrite. Probably incremental — keep case-by-case paths as leaf templates until the recursive walker is proven end-to-end on the 11-query test set.
- Relationship to the planned `tests/test_offline_translate.py`: should the golden files be part of this refactor's definition-of-done, or follow later? (Recommended: part of this refactor — the regression protection matters as templates are ported.)
