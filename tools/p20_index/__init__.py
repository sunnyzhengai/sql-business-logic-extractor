"""Phase 2 -- corpus.jsonl to unified graph + lexical index.

Builds the cross-view substrate that all downstream analysis runs on.
This is where the "graph pivot" lives -- tables and columns become
global nodes, joins become edges carrying view + scope provenance,
and a parallel lexical index lets us search by name (`pregnant` finds
every column / CTE / alias / comment that mentions the word).

Consumes
--------
- `corpus.jsonl` (output of p10_extract).

Produces
--------
- `graph.pkl` (or in-memory networkx MultiDiGraph) -- unified typed graph
  with nodes: View, Scope, Table, Column; edges with view + scope
  provenance (JOIN, READS_FROM_TABLE, CONTAINS_COLUMN, BELONGS_TO,
  REFERENCES_SCOPE, HAS_SCOPE).
- `lexical_index.json` -- string -> list of node IDs that contain the
  string, with token/stem normalization.

Design contract
---------------
- Tables are GLOBAL: one node per bare table name across the whole corpus.
- Every edge carries `view` and `scope` attributes for provenance.
- CTEs are first-class scope nodes (preserved for governance) but their
  internal joins are still globally visible table-to-table edges
  (flattened for similarity detection).
- The graph IS the index; per-view `corpus.jsonl` lines remain the source
  of truth for drilldown.

Read order
----------
- `graph_builder.py`    -- main entry; turns corpus.jsonl into the graph
- `lexical_index.py`    -- builds the string-to-node search index
- `schema.py`           -- shared node/edge type constants

Phase 0 status: skeleton. Initial graph-building code lives in
`tools/diagnostics/validate_graph_pivot.py` and `tools/graph_explore/`;
migration here will happen in subsequent phases of the restructure.
"""
