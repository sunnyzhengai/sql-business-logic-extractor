---
name: Recursive translation principle
aliases: [translate-to-column-or-register, pattern library as growing semantic spec, no opaque fallbacks]
see_also: [govern-authored-meaning, sql-as-definitional-moments, 2026-04-20-adopt-recursive-translation]
sources: []
---

## Definition

Every SQL construct translates to plain English by **recursive decomposition** to raw column references. Functions and structural constructs are never translated as flat case-by-case templates — each function or construct applies a template to **already-translated sub-expressions**, not to raw SQL text. When an unknown function, construct, or column is encountered, the system **registers** it (flags for human authoring of a template or description) and emits structural decomposition rather than an opaque placeholder.

No construct is ever translated as `"Value from previous row"`, `"Categorization with N condition(s)"`, or `"Type-converted value"` — because every unknown becomes a structured decomposition plus a governance signal, never a flat fallback.

## Why it matters here

Diagnostic evidence: a 2026-04-20 test run of `offline_translate.py` on 11 representative queries showed that every high-impact failure mode shared one root cause — a complex function or nested construct hitting a case-table miss, with the fallback dropping semantic content that is actually present in the SQL (see `decisions/2026-04-20-adopt-recursive-translation.md` for the enumerated findings).

The recursive principle kills all of those failure modes in one architectural move:

- **Base case:** raw column → schema lookup + enum/abbreviation expansion.
- **Inductive case:** function or construct → template applied to recursively-translated arguments.
- **Unknown case:** register the pattern/column (governance signal); emit structural decomposition as the fallback, never an opaque placeholder.

The pattern library becomes a **living semantic specification of the organization's SQL vocabulary.** It is authored meaning at the template level — every construct ever needed to translate this organization's SQL corpus, governed as a reusable asset. Coverage becomes measurable: the share of the SQL estate that translates using templates already in the library.

## Connection to the site's thesis

This is `govern-authored-meaning.md` applied *inside* the translator itself. The pattern library is authored meaning at a cross-query level: every time a new SQL construct appears in the codebase, an authored template must exist. The library starts small (common patterns) and grows toward completeness through use.

It also makes "unknown pattern encountered" a first-class event. Rather than hiding under a generic fallback, unknown patterns become governance signals: *your SQL corpus uses this construct N times with no authored translation yet — author one.* Unknown-column encounters play the same role on the data-dictionary side.

## Open questions

- **Pattern identity key.** Simple `(function_name, arity)` is fine for single functions. Composite patterns — e.g., `CAST(SUM(...) AS FLOAT) / COUNT(*) * 100` = percentage — need multi-level signatures. Probable approach: reuse the structural-signature infrastructure already in `compare.py`.
- **Template language.** Free-text with placeholders (`{0}`, `{name}`), or a structured DSL? Start with free-text; upgrade when needed.
- **Bootstrap fallback.** When an unknown pattern is registered, what's the default output until a human authors a template? Structural decomposition (e.g., "LAG applied to Hospital Discharge Time, partitioned by Patient ID, ordered by Admission Time") is strictly better than opaque ("Value from previous row"), but may be verbose.
- **Conflict resolution.** When composite patterns overlap (a node matches both "percentage" and "division of aggregates"), most-specific wins? Ordered priority list? Ambiguity flag?
- **Enum / schema integration.** Does enum lookup live in the base case or as a post-processing pass? (Cleaner in the base case — it's still terminal recursion.)
- **Versioning.** As templates improve, historic translations become stale. Re-translation policy: regenerate all, or snapshot-and-keep? Probably regenerate-on-demand with timestamp metadata.
