"""Tool 16 -- view similarity clustering (the "goldmine" governance tool).

Reads a v3 corpus.jsonl and clusters views by business-logic similarity
across four strict-refinement levels:

  L1: same SUBJECT      one-way driver containment
  L2: same GRAIN        L1 + same joined-table multiset (type-agnostic)
  L3: same PROJECTIONS  L2 + same source-or-fingerprint column set
  L4: same ROWS         L3 + same canonical filter set

L4 ⊆ L3 ⊆ L2 ⊆ L1. Every L4 cluster is also a sub-cluster of an L3
cluster, etc.

Each view's signature aggregates ACROSS its entire scope tree:
projections in main are transitively resolved through CTE references
back to base tables; filters from CTEs flow up; the leaf driver is
chased through the CTE chain until it lands on a base table. So a
view that wraps logic in CTEs and a view that inlines the same logic
produce identical signatures and cluster together at L4.

Per-cluster output also surfaces the join-type variance among members
(consistent vs mixed) -- this catches the common "did one developer
mean to use INNER and the other LEFT?" governance finding.
"""
