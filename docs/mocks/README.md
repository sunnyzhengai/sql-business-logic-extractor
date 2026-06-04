# Mockups and demos

Self-contained HTML artifacts demonstrating each stage of the design.
Click any title to open it in your browser via `htmlpreview.github.io`.

Bookmark this page (`docs/mocks/` on GitHub) — the list below
auto-updates as new mockups land.

## Design mockups (non-functional, for alignment)

- **[Corpus search](https://htmlpreview.github.io/?https://github.com/sunnyzhengai/sql-business-logic-extractor/blob/main/docs/mocks/corpus_search_mockup.html)**
  — Unified search UX across the parsed corpus.  Try the three chips
  (`ARPB`, `asthma`, `registry`) to see entity-first table lookup,
  semantic search with diagnosis-code resolution, and Epic-concept
  search.  *Not yet implemented; design proposal only.*

## Live demos (rendered from the real code against mock corpora)

- **[Community overview](https://htmlpreview.github.io/?https://github.com/sunnyzhengai/sql-business-logic-extractor/blob/main/docs/mocks/community_overview_demo.html)**
  — Per-community big-picture page: frequency-colored substrate at the
  top, per-view stripes below.  Click a stripe to spotlight that view's
  tables on the substrate.  Hover a node for its view-count.

- **[Corpus landscape map](https://htmlpreview.github.io/?https://github.com/sunnyzhengai/sql-business-logic-extractor/blob/main/docs/mocks/corpus_map_demo.html)**
  — Whole-corpus landscape.  Every table laid out via spring layout,
  colored by Louvain community.  Hover nodes for table name +
  community.  The footer would normally link to each community's
  overview; in the standalone demo those are stubbed.

- **[Per-view shape (v4 unfolding)](https://htmlpreview.github.io/?https://github.com/sunnyzhengai/sql-business-logic-extractor/blob/main/docs/mocks/view_shape_demo.html)**
  — Side-by-side per-view shape comparison.  Each view's panel is
  rendered as a tree-list (one node per row, depth = indent), CTEs
  and subqueries appear as nested clusters, JOIN-clause subqueries
  and CROSS APPLY get their own labeled boxes.

## Navigation hierarchy in real output

When `run_validation` runs against a real corpus, the artifacts
nest like this:

```
corpus_map.html
   ↓ click a community
community_overviews/community_NN_*_overview.html
   ↓ click a per-view stripe (spotlight) or "Open detail" link
community_shapes/community_NN_*_shapes.html#view-<view_name>
```

Three nested zoom levels: corpus → community → view.  The
mockups above demonstrate each level standalone.

## Older artifacts

- `patient_access_view_matrix.md` — early manual mock of the
  matrix output shape (Phase 3 of the original design); kept for
  historical reference.
- `archive/` — superseded design experiments.
