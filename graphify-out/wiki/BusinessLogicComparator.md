# BusinessLogicComparator

> God node · 33 connections · `archive/compare.py`

## Connections by Relation

### calls
- [[make_report()]] `INFERRED`
- [[main()]] `EXTRACTED`

### contains
- [[compare.py]] `EXTRACTED`
- [[compare.py]] `EXTRACTED`

### method
- [[.compare()]] `EXTRACTED`
- [[._find_semantic_matches()]] `EXTRACTED`
- [[._find_structural_matches()]] `EXTRACTED`
- [[._find_exact_duplicates()]] `EXTRACTED`
- [[.add_query()]] `EXTRACTED`
- [[._cluster_by_table_overlap()]] `EXTRACTED`
- [[.add_definitions()]] `EXTRACTED`
- [[.__init__()]] `EXTRACTED`

### rationale_for
- [[Compares business definitions across SQL queries.]] `EXTRACTED`

### uses
- [[BusinessDefinition]] `INFERRED`
- [[SQL Business Logic Extractor -- parse, normalize, compare, resolve, and translat]] `INFERRED`
- [[TestExactDuplicates]] `INFERRED`
- [[TestComplexComparison]] `INFERRED`
- [[TestStructuralMatches]] `INFERRED`
- [[TestSemanticMatches]] `INFERRED`
- [[Test cases for Layer 3: Comparison -- finding duplicate/similar business logic.]] `INFERRED`
- [[Helper: feed multiple (label, sql) tuples into comparator and return report.]] `INFERRED`
- [[Same DATEDIFF with different aliases -> exact duplicate.]] `INFERRED`
- [[Same WHERE clause in multiple queries -> exact duplicate filter.]] `INFERRED`
- [[Identical CASE logic across queries -> exact duplicate.]] `INFERRED`
- [[Completely different queries -> no exact duplicates.]] `INFERRED`
- [[Same DATEDIFF pattern on different columns -> structural match.]] `INFERRED`
- [[Same CASE branching structure, different thresholds -> structural match.]] `INFERRED`
- [[Two ROW_NUMBER() with different PARTITION BY -> structural match.]] `INFERRED`
- [[Definitions inside CTEs are compared across queries.]] `INFERRED`
- [[Three queries sharing some business logic.]] `INFERRED`
- [[A query should not match against itself in exact duplicates.]] `INFERRED`
- [[Report includes a summary with counts.]] `INFERRED`
- [[Two real-world style reports with overlapping logic.]] `INFERRED`

---

*Part of the graphify knowledge wiki. See [[index]] to navigate.*