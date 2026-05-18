# Pipeline phases

The pipeline turns raw SQL views into steward-ready governance artifacts
in five phases. Each phase lives in a `pN0_<purpose>/` folder under
`tools/`. The numeric prefix orders them in the file tree; gaps of 10
leave room for inserted phases without renumbering.

This document is the second thing to read when navigating cold (after
`ARCHITECTURE.md` at the repo root). For each phase: what it consumes,
what it produces, which scripts to read first, and what existing
legacy code (if any) it will absorb.

**Ratification is not in this codebase.** Stewards ratify definitions
in Collibra (or whichever catalog the org runs). Our pipeline produces
the evidence packs that steward meetings consume; Collibra owns the
decision record. This is why there is no p60/p70.

---

## Pipeline at a glance

```
       (SQL files in /data/queries or lakehouse)
                       │
                       ▼
    ┌─────────────────────────────────────────┐
    │  p10_extract    SQL  ─►  corpus.jsonl   │   CATALOG
    └─────────────────────────────────────────┘
                       │
                       ▼
    ┌─────────────────────────────────────────┐
    │  p20_index      corpus.jsonl  ─►  graph │   CATALOG
    │                                 + lex_idx│
    └─────────────────────────────────────────┘
                       │
                       ▼
    ┌─────────────────────────────────────────┐
    │  p30_analyze    graph  ─►  findings     │   GOVERN
    └─────────────────────────────────────────┘
                       │
                       ▼
    ┌─────────────────────────────────────────┐
    │  p40_synthesize findings ─► markdown    │   GOVERN
    │                              artifacts   │
    └─────────────────────────────────────────┘
                       │
                       ▼
    ┌─────────────────────────────────────────┐
    │  p50_present    artifacts ─► HTML, viz  │   VISUALIZE
    └─────────────────────────────────────────┘
                       │
                       ▼
              (hand to stewards;
              they ratify in Collibra)
```

Three conceptual layers (CATALOG → GOVERN → VISUALIZE) running
across the five phases. A separate `operate/` sidecar (parser dev,
performance audits, inventory generation) supports all three.

---

## p10_extract  -- SQL to corpus.jsonl

**Consumes:** A directory of SSMS-exported `.sql` files (UTF-16-LE with
BOM, ANSI_NULLS / GO boilerplate). Optionally, `data/dictionaries/zc_values.csv`
for inline ZC-lookup annotation.

**Produces:** `corpus.jsonl` -- one JSON object per view, conforming to
the ViewV1 schema (scopes, columns, joins, filters, base_columns).
First line is a header (`{"schema_version": 3, "n_views": ...}`).

**Read first:** `parser.py` (entry point + CLI).

**Existing tools to absorb here:** `extract_corpus/`, parts of
`auto_propose_rule/` and `comment_audit/`.

---

## p20_index  -- corpus.jsonl to unified graph + lexical index

**Consumes:** `corpus.jsonl` from p10_extract.

**Produces:**
- A unified networkx MultiDiGraph (Table/Column/Scope/View nodes;
  JOIN/READS_FROM_TABLE/CONTAINS_COLUMN/BELONGS_TO/REFERENCES_SCOPE
  edges with view+scope provenance on every edge).
- A lexical index mapping strings to graph nodes (with token + stem
  normalization), enabling "show me everywhere the word 'preg' appears."

**Read first:** `graph_builder.py`.

**Existing tools to absorb here:** `graph_explore/build.py`, the lexical
extraction pieces of `term_extraction/`.

**Design notes:**
- Tables are GLOBAL: one node per bare table name across the corpus.
- CTEs are first-class scope nodes (preserved) but their internal joins
  are still globally visible table edges (flattened for similarity).
- The graph is the index; per-view `corpus.jsonl` lines remain the
  source of truth for drilldown.

---

## p30_analyze  -- graph to findings

**Consumes:** The graph + lexical index from p20_index.

**Produces:**
- Community assignments (Louvain on the table projection,
  bridge tables excluded).
- Primary-community-per-view mapping.
- Cross-domain spans (views touching multiple communities).
- Bridge-table set (auto-detected high-degree dimension nodes).
- Naming-collision findings (same lexical anchor, different community).
- Variance findings (same concept, multiple SQL implementations).

**Read first:** `communities.py`.

**Existing tools to absorb here:** logic concepts from `similarity/`,
but reimplemented as graph operations (most of the old signature-based
similarity code is superseded).

**Design notes:**
- This phase produces findings, NOT artifacts. p40 turns findings into
  steward-readable artifacts.
- All findings carry pointers back to graph nodes/edges and to per-view
  provenance, so drill-down works end-to-end.

---

## p40_synthesize  -- findings to steward-ready artifacts

**Consumes:** p30_analyze findings + parts of corpus.jsonl (for filter
and cohort details).

**Produces:**
- `community_NN.md` -- one page per community: top tables, bridge
  context, primary member views, recommended steward conversation.
- `term_disagreements.md` -- same lexical anchor, different
  implementations.
- `cohort_evidence.md` -- English-rendered cohort definitions (the
  30-minute steward conversation artifact).
- `recruitment_list.md` -- which BI developers wrote which variant
  (joins SQL provenance with author lookup; requires manual data).

**Read first:** `community_packet.py`.

**Existing tools to absorb here:** `cohort_extract/` (English filter
rendering), `inventory_manifest/`, possibly `report_description_generator/`.

**Design notes:**
- Outputs are plain markdown. Stewards print these.
- No LLM-generated text on day one (no LLM access in pilot env). BI-dev
  review fills the description gap.
- Every claim has a pointer back to the source view, scope, line.

---

## p50_present  -- artifacts to interactive HTML and visualizations

**Consumes:** The graph from p20_index + artifacts from p40_synthesize.

**Produces (three modes):**
1. **Side-by-side** -- pick N views, render each in its own panel,
   shared anchors aligned spatially.
2. **Superimposed overlay** -- all selected views on ONE canvas;
   shared tables appear once, per-view branches in different colors.
   **The steward-meeting artifact.**
3. **Full corpus** -- everything, colored by community detection.

**Read first:** `overlay.py` (the superimposed-overlay renderer).

**Existing tools to absorb here:** `graph_explore/render.py`. The
per-community renderer in `tools/diagnostics/validate_graph_pivot.py`
is a working prototype.

**Design notes:**
- All HTML is CDN-free (works offline on locked-down healthcare laptops).
- Layouts pre-computed with networkx (kamada_kawai / spring_layout),
  frozen (physics=off, fixed=true) -- instant, deterministic, no animation.
- Bridge tables shown in muted gray across all renders.

---

## Cross-cutting (not phases)

### `shared/`
Utilities used across multiple phases: schema dataclasses, file I/O,
table-name helpers, color palettes. Code that does NOT belong here:
phase-specific logic (each phase owns its analysis/rendering/synthesis).

### `operate/`
Operations layer -- parser dev, diagnostics, system-health, admin tools.
NOT part of the pipeline. Sidecar to all three pipeline layers
(catalog / govern / visualize). Includes:
- `preflight_check.py` -- triage parse health across a corpus
- `diagnose_parse_failure.py` -- structured parse-failure report for one view
- `auto_propose_rule.py` (+ `auto_propose_rule_hypotheses.py`) -- propose
  parsing-rule additions for failing views
- `timing_audit.py` -- identify slow/timeout views during resolve
- `inventory_manifest.py` -- emit used-table / used-ZC / used-column
  manifests to narrow downstream SSMS extracts
- `validate_graph_pivot.py` -- validation experiment for the graph pivot
- `check_zc_lookups.py` -- triage missing ZC inline annotations

---

## Migration status (as of 2026-05-17)

The phase folders exist as skeletons. Migration of existing tools into
phase folders happens in subsequent restructure phases. Status legend:

- **Pending migration** = will move to the new home, possibly refactored.
- **Pending evaluation** = unclear whether to migrate, archive, or delete.
- **Pending decision** = likely archive or delete, awaiting explicit call.
- **Stable** = lives where it is; no migration planned.

| Legacy tool                       | Likely new home                                | Status                |
| --------------------------------- | ---------------------------------------------- | --------------------- |
| `extract_corpus/`                 | `p10_extract/`                                 | Pending migration     |
| `term_extraction/`                | `p20_index/term_extraction.py` (the lexical-anchor layer alongside `graph_builder.py`) | **Done** (Phase 1h)   |
| `graph_explore/`                  | split: `p20_index/` (build) + `p50_present/` (render) | Pending migration     |
| `similarity/`                     | deleted (superseded by graph approach in `tools/diagnostics/validate_graph_pivot.py`, git history retained); `tools/diagnostics/check_similarity.py` deleted too (its only purpose was triaging similarity output) | **Done** (Phase 1f)   |
| `view_shape_compare/`             | split: `dim_filter.py` -> `tools/shared/`; rest deleted (superseded by `p30_analyze/`, git history retained) | **Done** (Phase 1e)   |
| `cohort_extract/`                 | `p40_synthesize/`                              | Pending migration     |
| `inventory_manifest/`             | `tools/operate/inventory_manifest.py` (re-classified as ops in Phase 1k; audience is BI devs running the system, not stewards) | **Done** (Phase 1c, re-homed Phase 1k) |
| `report_description_generator/`   | `p40_synthesize/`                              | Pending evaluation    |
| `column_lineage_extractor/`       | `p20_index/`                                   | Pending evaluation    |
| `dataset_extract/`                | `p40_synthesize/dataset_extract.py` + `dataset_render.py` (per-view dataflow chains, same pattern as cohort_extract) | **Done** (Phase 1i)   |
| `business_logic_extractor/`       | likely superseded                              | Pending evaluation    |
| `technical_logic_extractor/`      | likely superseded                              | Pending evaluation    |
| `similar_logic_grouper/`          | split: `fingerprint.py` -> `tools/shared/`; batch.py deleted (the same finding is produced by `p30_analyze` from the unified graph) | **Done** (Phase 1g)   |
| `auto_propose_rule/`              | `tools/operate/auto_propose_rule.py` + `auto_propose_rule_hypotheses.py` (parser-dev tool) | **Done** (Phase 1k)   |
| `comment_audit/`                  | deleted (one-off analysis whose findings were absorbed into Tools 3/4 long ago; git history retained) | **Done** (Phase 1j)   |
| `preflight_check/`                | `tools/operate/preflight_check.py` (parser-health triage) | **Done** (Phase 1k)   |
| `timing_audit/`                   | `tools/operate/timing_audit.py` (resolver-timing audit) | **Done** (Phase 1k)   |
| `diagnostics/` (folder)           | renamed to `tools/operate/` -- broader scope (parser dev + admin, not just diagnosis) | **Done** (Phase 1k)   |
| `batch_all.py` (file)             | stays at `tools/batch_all.py` (orchestrator for the 4-tool commercial product line) | Stable                |
| `diagnose_parse_failure.py` (file)| `tools/operate/diagnose_parse_failure.py` (parse-failure triage) | **Done** (Phase 1k)   |
| `tests/`                          | stays at `tools/tests/` (top-level integration tests) | Stable                |
