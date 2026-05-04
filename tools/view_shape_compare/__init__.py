"""Tool 12 -- view-shape comparison.

Reads a v3 corpus.jsonl, extracts each view's table+join shape (with
dim-table noise stripped), and emits clusters of views that share
structural similarities. Flags applied per cluster are flat -- a single
cluster can carry multiple flags simultaneously.

Flags emitted:
  - table_identical:   same all-table set AND same join multiset
  - fact_identical:    same fact-table set (after dim strip)
  - fact_subset:       one view's facts is a strict subset of another's
  - fact_superset:     mirror of fact_subset
  - fact_overlap:      facts intersect but neither is subset of the other
  - join_identical:    fact-table join multisets match
  - join_subset:       one's fact joins is a strict subset of another's
  - join_topology_differs: same fact tables, different join multisets
  - dim_extension:     same fact tables and joins, but dim tables differ
  - same_driver:       same FROM driver (outermost / leftmost table)

Cluster output is one row per cluster: signature, member views, flags
applicable to the cluster, and per-pair details where asymmetric flags
apply.
"""
