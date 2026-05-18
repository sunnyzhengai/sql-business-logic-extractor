"""Phase 3 -- graph to findings.

The analytical core. Detects communities, classifies dimension vs.
cohort-shaping tables, assigns each view to a primary community,
identifies cross-domain views and naming collisions.

Consumes
--------
- The unified graph from p20_index.
- The lexical index from p20_index.

Produces
--------
- Community assignments (Louvain on the table projection, bridges excluded).
- Primary community per view + cross-domain spans list.
- Bridge-table set (auto-detected high-degree dimension nodes).
- Naming-collision findings (same lexical anchor, different community).
- Variance findings (same concept, multiple SQL implementations).

Read order
----------
- `communities.py`      -- main entry; runs Louvain on the projection
- `bridges.py`          -- degree-percentile-based bridge detection
- `primary_community.py` -- assign each view to its dominant community
- `cross_view.py`       -- find views that span multiple communities
- `lexical.py`          -- naming-collision detection across communities

Design contract
---------------
- This phase produces findings, not artifacts. Steward-facing output
  is built by p40_synthesize from these findings.
- All findings carry pointers back to the graph (node/edge IDs) and
  to per-view provenance (which views, which scopes).

Phase 0 status: skeleton. Initial analysis logic lives in
`tools/diagnostics/validate_graph_pivot.py`; production extraction
into this folder will happen in subsequent phases.
"""
