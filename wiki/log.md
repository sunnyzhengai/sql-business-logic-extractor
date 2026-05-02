# Wiki log

Append-only ledger. Newest at top. One line per event.

Format: `YYYY-MM-DD  <kind>  <summary>  [affected: page1, page2]`

Kinds: `ingest` · `decide` · `commit` · `lint`

---

2026-05-02  commit  f51ad59 Layer 1: parsing-rule registry replaces inline regex strips  [affected: none — code; declarative Rule dataclasses + fixture-driven tests; 188 tests pass]
2026-05-02  commit  cbf8339 Add Tool 5 (similar_logic_grouper) + full-corpus metadata helper  [affected: none — code; AST fingerprinting for cross-view definition dedup; SSMS @Tables/@Columns generator]
2026-05-02  commit  c209065 Tool 4: format technical and business descriptions as paragraphs  [affected: none — code; newline-separated bullets so cells render as readable text]
2026-05-02  commit  95a877a Tool 4 Phase 2: naturalize engineered business_description prose  [affected: none — code; comment promotion + effective-date folding + _YN normalization + keyword-adjective promotion]
2026-05-02  commit  95eb7a9 Tool 4: rename query_summary -> technical_description, add business_description  [affected: none — code; engineered business prose strips technical noise via pattern-library + post-processing]
2026-05-02  commit  6bc4d1a Emit ALL filter predicates verbatim in query_summary  [affected: none — code; engineered Tool 4 summary now keeps the full business slice for no-LLM use]
2026-05-02  commit  abd6e69 Drop view_file from Tools 1, 2, 4 outputs (parallel to Tool 3 trim)  [affected: none — code; goldens re-baselined]
2026-05-02  commit  c188568 Slim Tool 3 CSV: drop view_file, base_columns, base_tables  [affected: none — code; goldens re-baselined]
2026-05-02  commit  a9e43d6 ASCII-sanitize csv_to_schema.py for Windows-browser uploads  [affected: none — code; em-dash mangling crashed Python's UTF-8 source loader]
2026-05-02  commit  e76d51d Auto-detect CSV encoding in csv_to_schema.py  [affected: none — code; SSMS exports as UTF-16 LE]
2026-05-02  commit  5d30682 Add Fabric notebook script for the all-tools batch runner  [affected: none — code; six cells: deps, repo upload, sys.path, batch_all, peek, LLM]
2026-05-02  commit  f14049b Hard-code @Columns list for V_ACTIVE_MEMBERS + bi_complex  [affected: none — code; pre-populated metadata extract for SSMS run]
2026-05-01  commit  e7cfe62 Add optional @Columns filter to Clarity metadata extract  [affected: none — code; narrows to columns actually referenced per Tool 1 manifest]
2026-05-01  commit  e7fa61e Add sys.* fallback layer to Clarity metadata extract  [affected: none — code; Layer B captures custom V_CCHP_* views that aren't in CLARITY_TBL]
2026-04-29  decide  Pivot away from AIVIA platform; 4-tool product line is the commercialization vehicle, with the day-job SSIS-to-Fabric migration as Use Case #1  [affected: planning/monthly/2026-06,07,08.md, planning/weekly/2026-06-01,08-10,08-24.md, planning/daily/2026-04-28.md, view-migration/REQUIREMENTS.md, wiki/decisions/2026-04-19-*.md, wiki/concepts/article-template.md]
2026-04-28  commit  d445008 Add view-migration tooling for SSIS-to-Fabric workstream  [affected: none — new isolated workstream under view-migration/; separate from extractor commercialization]
2026-04-27  commit  94df49f Move CLI scripts to cli/, lift filters to query level, plumb LLM filter context  [affected: none — code; reorganization + L4 filter consolidation + LLM summary semantic upgrade]
2026-04-20  commit  38c50e9 Add INI-Item-aware schema pipeline (CSV → JSON → translator)  [affected: none — code; operationalizes app-config-id-as-coordination-key.md]
2026-04-20  commit  eeb6605 Rewire offline_translate.py on top of the recursive pattern library  [affected: none — code; archives legacy version for reference]
2026-04-20  commit  f26fad2 Add golden tests for recursive offline translator (Step 4)  [affected: none — tests + fixtures]
2026-04-20  commit  cfb300f Implement recursive walker + pattern library (Steps 2+3)  [affected: none — code]
2026-04-20  commit  5e082a4 Scaffold pattern library data model (Step 1)  [affected: none — code]
2026-04-20  commit  bae3727 Adopt recursive translation principle for offline_translate  [affected: recursive-translation-principle, 2026-04-20-adopt-recursive-translation, index]
2026-04-20  decide  Added `recursive-translation-principle` (concept) and `2026-04-20-adopt-recursive-translation` (decision) capturing the architectural pivot for offline_translate.py: recursion to raw-column base case + growing pattern library + unknown patterns/columns as first-class governance signals. Test findings from 11-query run embedded as motivating context.  [affected: recursive-translation-principle, 2026-04-20-adopt-recursive-translation, index]
2026-04-20  commit  9347be5 Log Shipment 1 to work (extract + normalize + resolve + compare_lineage)  [affected: none — work-shipments ledger, repo root]
2026-04-19  commit  5d17bcd Add project instructions, graphify outputs, and Claude Code hook config  [affected: none — project infra]
2026-04-19  commit  4a30ee0 Adopt Thinking in Public strategy and publishing pipeline  [affected: 2026-04-19-thinking-in-public-strategy, 2026-04-19-publishing-automation-architecture, article-template, index, log]
2026-04-19  decide  Adopted Thinking in Public (TIP) as publishing posture and semi-automated three-tier pipeline as publishing architecture. Added `article-template.md` (reusable 8-section structure). Resolved the healthcare-rich-wiki vs. industry-agnostic-public tension via two-layer separation (wiki stays private source; public articles are translations).  [affected: 2026-04-19-thinking-in-public-strategy, 2026-04-19-publishing-automation-architecture, article-template, index]
2026-04-19  commit  c07df14 Seed conceptual wiki with governance-forks framework  [affected: all new concept pages + decision + index + log]
2026-04-19  ingest  Added `app-config-id-as-coordination-key.md` — Yang's Epic INI-Item insight captured. This is the operational primitive that closes the app-team coordination loop: app analysts already identify the INI-Item as part of their change workflow, so lookup-before-change piggybacks on an existing step with no new process. Clarity pre-packages the catalog, extractor supplies the cross-reference. Identified as a go-to-market wedge for healthcare Epic shops — a day-one valuable connector feature no competitor has. Cross-linked from app-teams fork and silent-upstream-break scene (added "how it would have gone differently" section).  [affected: app-config-id-as-coordination-key, app-teams-in-dg-vs-out, silent-upstream-break, index]
2026-04-19  ingest  Added Fork 8 (app teams in DG, or out) with fork page and canonical "silent upstream break" scene (second-class companion to Patient A01). Cross-linked `govern-authored-meaning.md` with the upstream-extension implication. Establishes DG-as-enterprise vs. DG-as-analytics distinction; honest third path is coordination-minimal, approval-narrow. Updated forks catalog and index.  [affected: app-teams-in-dg-vs-out, silent-upstream-break, govern-authored-meaning, governance-forks-catalog, index]
2026-04-19  ingest  Added Fork 7 (catch-up vs. spec-first) with the fork page, the "definitions are emergent" counterweight concept, and the "spec-first as data contract" intellectual anchor. Establishes site's non-obvious position: neither pure posture is correct; extractor-enabled selective promotion is the third path. Updated forks catalog and index.  [affected: catch-up-vs-spec-first, definitions-are-emergent, spec-first-as-data-contract, governance-forks-catalog, index]
2026-04-19  ingest  Added Fork 6 (catalog-first vs. governance-first) with three supporting pages: the fork itself, "catalog is not governance" foundational frame, and the automation-asymmetry insight that reframes extractor's category as the first scalable governance-authoring primitive. Updated forks catalog and index.  [affected: catalog-first-vs-governance-first, catalog-is-not-governance, catalog-vs-governance-automation-asymmetry, governance-forks-catalog, index]
2026-04-19  ingest  Seeded 6 concept pages + 1 decision page from live brainstorm on content-site structure (fork model), data-vs-metadata ownership, governance-vs-compliance layer split, authored-meaning governance, Patient A01 canonical scene, SQL as definitional moments, fork catalog working draft.  [affected: data-vs-metadata-ownership, governance-vs-compliance-layers, govern-authored-meaning, sql-as-definitional-moments, patient-a01-scene, governance-forks-catalog, 2026-04-19-fork-model-for-content-site]
2026-04-17  init  Wiki seeded: SCHEMA.md, index.md, empty concepts/ and decisions/. Graphify code wiki at graphify-out/wiki/ (21 articles).
