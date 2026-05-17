"""Phase 5 -- artifacts to interactive HTML and visualizations.

Makes the analysis visible. The artifacts BI devs and stewards actually
look at, in their three modes (see user_graph_visualization_preference
memory):

  1. Side-by-side  -- pick N views, render each in its own panel,
                       shared anchors aligned spatially.
  2. Superimposed overlay  -- all selected views on ONE canvas; shared
                              tables appear once, per-view branches in
                              different colors. THE steward-meeting
                              artifact.
  3. Full corpus   -- everything, colored by community detection.
                       Governance-leverage overview.

Consumes
--------
- The unified graph from p20_index.
- Findings + artifacts from p30_analyze and p40_synthesize.

Produces
--------
- HTML files (interactive pyvis renders, self-contained / offline-safe).
- Optional GraphML exports for Gephi.
- Static matplotlib renders for embedding in slides.

Read order
----------
- `overlay.py`          -- the superimposed-overlay renderer (highest priority)
- `community_html.py`   -- per-community focused rendering
- `corpus_overview.py`  -- the full-graph overview
- `exports.py`          -- GraphML / static / other format exporters

Design contract
---------------
- All HTML is CDN-free (`cdn_resources="in_line"`) so it works offline
  on locked-down healthcare laptops.
- Layouts are pre-computed with networkx (kamada_kawai / spring_layout
  with seed) and frozen (physics=off, fixed=true) for instant,
  deterministic rendering -- no animation.
- Bridge tables (auto-detected dimensions) appear in muted gray across
  all renders so they don't visually dominate.

Phase 0 status: skeleton. Existing tooling that will absorb here:
`graph_explore/render.py`. The per-community renderer in
`tools/diagnostics/validate_graph_pivot.py` is a prototype of what
`community_html.py` will be.
"""
