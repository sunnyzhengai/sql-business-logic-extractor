# SQL Business Logic Assistant — Full Architecture Plan

**Date:** 2026-06-11
**Status:** Approved, building Phase 1
**Participants:** Sunny + Claude

## Summary

The product has two halves: a **Parser** (batch, builds 3 glossaries from existing SQL) and an **Assistant** (interactive, uses glossaries as tools to answer user questions via cascading search with HITL at every step).

## Core Principle: HITL Everywhere

The system never decides what the user meant. Every step is a question. Escalation to human BI developer is a first-class outcome, not a failure.

## Key Design Decisions Made in This Session

1. **Three glossaries** replace the single corpus.jsonl as the knowledge base:
   - Report Glossary (whole reports, first-pass matching)
   - Business Definition Glossary (reusable logic blocks, mix-and-match)
   - Technical Glossary (schema by domain, build from scratch)

2. **Cascading search** replaces the old v1/v2 split:
   L1 Report → L2 Definition → L3 Technical → L4 Build from scratch → Human handoff

3. **No automatic question decomposition.** System searches, user selects building blocks, user sequences them, user approves each SQL step.

4. **Business Definition boundaries:** INNER joins that change row count = definitional backbone. LEFT joins for enrichment = not. WHERE on business concepts = characteristic filter. WHERE on dates/thresholds = swappable parameter.

5. **Term resolution is layered:** learned glossary (pre-populated from corpus) → pattern recognition (suffix + verb-frame) → LLM with tools (glossary, ICD-10, web) → ask user.

6. **User Library as graph:** User nodes → validates/uses → Definition nodes. Social proof via validation and usage counts. Glossaries grow from both parsing AND user sessions.

7. **Quantifier/date extraction:** Regex pre-fill into editable fields, flag divergence from definition's original values, user always confirms.

8. **Known routes catalog:** Static table paths per category (diagnosis has 4 routes, medication has 1, etc.). User preferences tracked for recommendations.

---

# Part 1: Parser

Ingests existing SQL views/procs and produces three glossaries. Mostly reorganizes existing extraction pipeline output into new formats.

## Glossary 1: Report Glossary

**What it stores:** Whole-report metadata. One entry per view/proc/report.

```yaml
# report_glossary/VW_DIABETIC_COHORT.yaml
report_name: VW_DIABETIC_COHORT
description: "Identifies patients with active diabetes diagnoses for population health reporting"
primary_purpose: "Diabetes cohort identification"
key_metrics: [patient_count, diagnosis_prevalence]
developer: "J. Smith"           # from SQL comments if available
business_requester: null        # from SQL comments if available
created_date: null              # from SQL comments or git history
last_modified: "2024-03-15"     # from git history or file timestamp
parameters:                     # swappable inputs
  - name: icd10_pattern
    default: "E11%"
    description: "ICD-10 code pattern for diabetes type"
  - name: active_only
    default: true
    description: "Filter to active problems only"
tables_used: [PATIENT, PROBLEM_LIST, CLARITY_EDG]
domains: [diagnosis, demographics]
column_count: 8
sql_complexity: medium          # simple/medium/complex based on CTE count, join depth
inline_comments:                # all developer comments extracted from SQL
  - "-- Active diabetes only, excludes gestational"
  - "-- Uses problem list, not encounter dx"
source_sql_path: "views/VW_DIABETIC_COHORT.sql"
```

**What it's for:** First-pass matching. "Does an existing report already answer this question, maybe with different parameters?"

**Existing code that feeds this:** `report_description_generator`, `describe_folders`, corpus.jsonl `report` section, SQL comment extraction.

## Glossary 2: Business Definition Glossary

**What it stores:** Independent, reusable logic blocks. Each entry defines ONE business concept with its backbone (tables, joins, characteristic filters) and optional parameters.

```yaml
# definition_glossary/diabetic_patients_problem_list.yaml
definition_name: diabetic_patients_problem_list
label: "Diabetic patients (active problem list)"
description: "Patients with an active diabetes diagnosis on their problem list, identified by ICD-10 E11.% codes"
domain: diagnosis

# Backbone — the fixed structure that defines this concept
backbone:
  anchor_table: PATIENT
  tables: [PATIENT, PROBLEM_LIST, CLARITY_EDG]
  joins:
    - from: PATIENT
      to: PROBLEM_LIST
      on: "PROBLEM_LIST.PAT_ID = PATIENT.PAT_ID"
      type: INNER           # INNER = affects row count = definitional
      grain_impact: true    # does this join change the output grain?
    - from: PROBLEM_LIST
      to: CLARITY_EDG
      on: "CLARITY_EDG.DX_ID = PROBLEM_LIST.DX_ID"
      type: INNER
      grain_impact: false   # enriches, doesn't change grain
  characteristic_filters:   # the WHERE clauses that DEFINE this concept
    - expression: "CLARITY_EDG.CURRENT_ICD10_LIST LIKE 'E11%'"
      english: "ICD-10 diabetes codes (Type 2)"
      is_definitional: true
    - expression: "PROBLEM_LIST.RESOLVED_DATE IS NULL"
      english: "Active (unresolved) problems only"
      is_definitional: true
  output_grain: patient     # one row per patient

# Parameters — swappable without changing the definition's identity
parameters:
  - name: icd10_pattern
    default: "E11%"
    type: string
    description: "ICD-10 pattern to match"
  - name: active_only
    default: true
    type: boolean
    description: "Filter to active problems (RESOLVED_DATE IS NULL)"

# Provenance — where this definition came from
source_reports: [VW_DIABETIC_COHORT, VW_MEDICATION_DIABETIC]
source_scopes: ["VW_DIABETIC_COHORT::cte:diabetic_patients",
                "VW_MEDICATION_DIABETIC::cte:diabetic_pats"]
equivalent_definitions: [diabetic_patients_encounter_dx]  # same concept, different route

# Usage tracking (grows over time)
validated_by: []            # user IDs who confirmed this definition
used_in_queries: []         # saved query IDs that use this definition
validation_count: 0
usage_count: 0
```

**Boundary rules for the parser** (what makes something a definition):
- An INNER JOIN that changes the output row count → part of the definition backbone
- A LEFT JOIN that only adds columns → NOT part of the backbone (it's enrichment)
- A GROUP BY that changes grain → part of the backbone
- A WHERE clause on a fixed business concept (status codes, ICD codes, department types) → characteristic filter (definitional)
- A WHERE clause on a date range or threshold → parameter (swappable)
- A complete CTE that defines a cohort → one definition
- A complex query where multiple pieces (CTE + WHERE + JOIN) together define a concept → one definition spanning multiple pieces, linked by the tables and filters they share

**What it's for:** Second-pass matching. "No existing report answers this, but these reusable building blocks can be combined."

**Existing code that feeds this:** Corpus scopes, filter extraction, column lineage (passthrough vs. calculated), join analysis. The `pattern_classifier.py` already groups by anchor tables. Need new logic to distinguish backbone vs. parameter filters, and to detect definition boundaries across multi-piece patterns.

## Glossary 3: Technical Glossary

**What it stores:** Schema knowledge organized by domain, with tables categorized as anchor (center of a cluster), satellite (joins from anchor), or dimension (domain-neutral).

```yaml
# technical_glossary/domains.yaml
domains:
  encounters:
    description: "Patient visits, appointments, and clinical encounters"
    anchor_tables:
      - name: PAT_ENC
        description: "All patient encounters/appointments"
        primary_key: PAT_ENC_CSN_ID
        grain: "one row per encounter"
        satellite_tables:
          - name: PAT_ENC_DX
            join: "PAT_ENC_DX.PAT_ENC_CSN_ID = PAT_ENC.PAT_ENC_CSN_ID"
            relationship: "many diagnoses per encounter"
          - name: F_SCHED_APPT
            join: "F_SCHED_APPT.PAT_ENC_CSN_ID = PAT_ENC.PAT_ENC_CSN_ID"
            relationship: "one-to-one scheduling extension"
      - name: PAT_ENC_HSP
        description: "Hospital/inpatient encounters"
        primary_key: PAT_ENC_CSN_ID
        grain: "one row per hospital encounter"
        satellite_tables:
          - name: HSP_ADMIT_DIAG
            join: "HSP_ADMIT_DIAG.PAT_ENC_CSN_ID = PAT_ENC_HSP.PAT_ENC_CSN_ID"
            relationship: "many admission diagnoses per encounter"

  billing:
    description: "Financial transactions, charges, and hospital accounts"
    anchor_tables:
      - name: ARPB_TRANSACTIONS
        description: "Professional billing transactions"
        primary_key: TX_ID
        grain: "one row per transaction"
      - name: HSP_ACCOUNT
        description: "Hospital account/billing summary"
        primary_key: HSP_ACCOUNT_ID
        grain: "one row per hospital account"

  medications:
    description: "Medication orders and prescriptions"
    anchor_tables:
      - name: ORDER_MED
        description: "Medication orders"
        primary_key: ORDER_MED_ID
        grain: "one row per medication order"

  procedures:
    description: "Procedure orders, lab orders, surgeries"
    anchor_tables:
      - name: ORDER_PROC
        description: "Procedure/lab orders"
        primary_key: ORDER_PROC_ID
        grain: "one row per order"
      - name: OR_CASE
        description: "Surgery cases"
        primary_key: OR_CASE_ID
        grain: "one row per surgery"

  referrals:
    description: "Referral orders and tracking"
    anchor_tables:
      - name: REFERRAL
        description: "Referral orders"
        primary_key: REFERRAL_ID
        grain: "one row per referral"

# Domain-neutral dimension tables (applicable to any domain)
dimensions:
  - name: PATIENT
    description: "Patient demographics"
    primary_key: PAT_ID
    joins_to_all_domains: true
  - name: CLARITY_SER
    description: "Providers"
    primary_key: PROV_ID
  - name: CLARITY_DEP
    description: "Departments"
    primary_key: DEPARTMENT_ID
  - name: CLARITY_EDG
    description: "Diagnoses (ICD-10)"
    primary_key: DX_ID
  - name: CLARITY_MEDICATION
    description: "Medications"
    primary_key: MEDICATION_ID
  - name: CLARITY_EAP
    description: "Procedures (CPT/HCPCS)"
    primary_key: PROC_ID
```

**Domain tagging logic for the parser:**
- Tables that appear as FROM/anchor in many views → likely anchor tables
- Tables that only appear in JOINs → likely satellites or dimensions
- Tables referenced across many domains (PATIENT, CLARITY_SER) → dimensions
- Domain assignment based on table prefix + community detection from the existing graphify analysis (PAT_ENC cluster = encounters, ARPB cluster = billing)

**What it's for:** Last resort. "Nothing existing matches. Here are the raw tables and domains — let's build from scratch."

**Existing code that feeds this:** `clarity_schema.yaml` (FK relationships), `table_importance.py` (PageRank for anchor identification), graphify community detection, `table_short_descriptions.yaml`.

---

# Part 2: Assistant

Interactive Streamlit app. Uses the 3 glossaries as tool libraries. Cascading search with HITL.

## Workflow

```
User enters NL question
        │
        ▼
┌─ Level 1: Report Search ──────────────────────────┐
│ Search Report Glossary for matching reports        │
│ Show matches with sample data + aggregates         │
│ User: "Yes this answers it" → save to library, END │
│ User: "No" → escalate to Level 2                   │
└────────────────────────────────────────────────────┘
        │ (no match or user rejected)
        ▼
┌─ Level 2: Definition Search ──────────────────────┐
│ Search Business Definition Glossary                │
│ Show matching definitions with sample data         │
│ User picks building blocks, sequences them         │
│ System builds CTE-chained query step by step       │
│ User validates each step (HITL)                    │
│ User: "Yes" → save to library, END                 │
│ User: "No / can't find blocks" → escalate Level 3  │
└────────────────────────────────────────────────────┘
        │ (no match or user rejected)
        ▼
┌─ Level 3: Technical Search ───────────────────────┐
│ Search Technical Glossary by domain                │
│ Show domain → anchor tables → satellite tables     │
│ User picks domain and tables                       │
│ User: "Yes, these tables" → escalate to Level 4    │
│ User: "Can't find tables" → handoff to human, END  │
└────────────────────────────────────────────────────┘
        │ (user confirmed domain/tables)
        ▼
┌─ Level 4: Build from Scratch ─────────────────────┐
│ Add one table at a time                            │
│ Run SQL after each addition                        │
│ Show sample output + aggregates                    │
│ User validates each step (HITL)                    │
│ Repeat until query is complete                     │
│ User: "Yes" → save to ALL glossaries, END          │
│ User: "Can't get it right" → handoff to human, END │
└────────────────────────────────────────────────────┘
```

## Assistant Tools

### Tool 1: Report Searcher

**Input:** NL question
**Searches:** Report Glossary (TF-IDF over descriptions + key_metrics + inline_comments)
**Output:** Ranked list of matching reports with:
- Report description and purpose
- Parameter values (defaults, can be changed)
- Sample data preview (first 10 rows from mock DB)
- High-level aggregates (row count, key metric values)

**User actions:** Run with different parameters, approve, reject, ask questions.

### Tool 2: Business Definition Searcher

**Input:** NL question (or refined question after Level 1 rejection)
**Searches:** Business Definition Glossary (TF-IDF over labels + descriptions + filter english + domain)
**Also uses:** Synonym expansion (healthcare_synonyms.yaml), learned terms glossary
**Output:** Ranked list of matching definitions, grouped by equivalence:
- Definition label and description
- Backbone tables and joins
- Parameters with defaults
- Validation count / usage count (social proof)
- Sample data if run independently

**User actions:** Select multiple definitions, sequence them, pick base population, set parameters, approve/edit SQL at each step.

### Tool 3: Technical Searcher

**Input:** Domain keywords or table/column names
**Searches:** Technical Glossary (domain → anchor → satellites)
**Also uses:** Term resolver (pattern recognition for unknown terms), FK graph
**Output:** Domain matches with:
- Anchor tables and their satellites
- Join paths (from FK graph)
- Column lists with descriptions

**User actions:** Pick domain, pick tables, pick columns.

### Tool 4: Query Builder (from scratch)

**Input:** Selected domain, tables, columns from Tool 3
**Process:** Adds one table at a time:
1. Start with anchor table → run COUNT(*) → show sample
2. Add first join → run → show how row count changes
3. Add filter → run → show impact
4. Repeat until complete

**User actions:** Approve each addition, edit SQL, add/remove filters, show sample rows.

### Tool 5: Term Resolver (support tool, used by Tools 2-4)

**Layered resolution:**
1. Check learned_terms.yaml (pre-populated from corpus + user confirmations)
2. Pattern recognition (suffix: "-itis" → diagnosis; verb frame: "taking X" → medication)
3. LLM with tools (search glossary, ICD-10 lookup, web search)
4. Ask user ("what kind of thing is this?")

### Tool 6: Quantifier/Date Extractor (support tool)

Regex extraction of thresholds and date ranges from question text. Pre-fills editable fields. Flags divergence from definition's original values.

---

## User Library (Graph)

```
User Node ──validates──→ Definition Node (in Business Definition Glossary)
User Node ──uses──→ Definition Node
User Node ──created──→ Saved Query Node
Saved Query Node ──uses──→ Definition Node (multiple)
```

**Tracked per definition:**
- `validation_count`: how many users confirmed this definition is correct
- `usage_count`: how many saved queries use this definition
- `validated_by`: list of user IDs
- `last_used`: timestamp

**Tracked per saved query:**
- Which definitions it combines
- What parameters were used
- The full SQL chain
- The user who created it
- The result (row count, aggregates)

**Value:** Popular definitions surface first. Conflicting definitions (same concept, different logic) are flagged. New analysts see what peers have validated.

---

## Cascading Save Logic

**Level 1 match (report):** Save user → report edge to User Library.

**Level 2 match (definitions combined):** Save the combination as a new Saved Query in User Library, pointing to the definitions used. If the user modified parameters, save the parameter overrides.

**Level 3+4 (built from scratch):** This creates NEW entries:
- New Business Definitions for each step the user validated (these are now reusable)
- New Report Glossary entry for the complete query
- New learned_terms if unknown terms were resolved
- User Library edges for everything above

The glossaries grow from both parsing AND user sessions.

---

# Mock Environment (for local testing)

## What we build for the mock

| Component | Mock Implementation | Fabric Swap |
|---|---|---|
| Database | SQLite with 500 patients, correlated data | Fabric SQL endpoint |
| Report Glossary | 8 report entries (from mock corpus views) | Parsed from real SQL |
| Definition Glossary | ~15 definitions (extracted from 8 views) | Parsed from real SQL |
| Technical Glossary | Domain-tagged schema from clarity_schema.yaml | Same file |
| User Library | JSON file, local | Graph DB or JSON in Lakehouse |
| Learned Terms | Pre-populated YAML | Same file, grows |
| UI | Streamlit | Streamlit (same) |
| LLM (optional) | OpenAI API or skip | Azure OpenAI |

## Mock Data Correlation

```
500 PATIENT
 └─ 90 have diabetes (PROBLEM_LIST + CLARITY_EDG, E11.%)
     └─ 45 of those have 4+ ED encounters in past year
         └─ 20 of those have no-show PCP in past 6 months

Also present (for contrast):
 - 60 non-diabetic patients with 4+ ED visits
 - 30 diabetic patients with no-show PCP but <4 ED visits
```

## Mock Reports (8 views → Report Glossary)

| Report | Description | Domains |
|---|---|---|
| VW_DIABETIC_COHORT | Diabetes population identification | diagnosis, demographics |
| VW_ED_UTILIZATION | ED visit frequency analysis | encounters, demographics |
| VW_PCP_COMPLIANCE | PCP appointment compliance tracking | encounters, demographics |
| VW_READMISSION_30DAY | 30-day hospital readmission | encounters, billing |
| VW_LOS_REPORT | Length of stay calculation | encounters, billing |
| VW_MEDICATION_DIABETIC | Diabetes medication analysis | diagnosis, medications |
| VW_BILLING_SUMMARY | Financial charge summary | billing |
| VW_REFERRAL_TRACKING | Referral status tracking | referrals |

## Mock Business Definitions (~15, extracted from 8 views)

| Definition | Source View(s) | Domain | Anchor |
|---|---|---|---|
| diabetic_patients_problem_list | VW_DIABETIC_COHORT, VW_MEDICATION_DIABETIC | diagnosis | PATIENT |
| active_problems_only | VW_DIABETIC_COHORT | diagnosis | PROBLEM_LIST |
| ed_encounters | VW_ED_UTILIZATION | encounters | PAT_ENC |
| ed_high_utilizers | VW_ED_UTILIZATION | encounters | PAT_ENC |
| pcp_assignments_active | VW_PCP_COMPLIANCE | encounters | PAT_PCP |
| missed_pcp_visits | VW_PCP_COMPLIANCE | encounters | PAT_ENC |
| hospital_admissions | VW_READMISSION_30DAY, VW_LOS_REPORT | encounters | PAT_ENC_HSP |
| readmission_30day | VW_READMISSION_30DAY | encounters | PAT_ENC_HSP |
| length_of_stay | VW_LOS_REPORT | encounters | PAT_ENC_HSP |
| diabetic_medications | VW_MEDICATION_DIABETIC | medications | ORDER_MED |
| billing_charges | VW_BILLING_SUMMARY | billing | ARPB_TRANSACTIONS |
| active_referrals | VW_REFERRAL_TRACKING | referrals | REFERRAL |

---

# New Files (Build Order)

## Phase 1 — Mock Data + Glossaries (parallel)

| File | Purpose | ~Lines |
|---|---|---|
| `tools/jit/mock/__init__.py` | Package init | 0 |
| `tools/jit/mock/mock_db.py` | SQLite with correlated patient data | 200 |
| `tools/jit/mock/mock_reports.py` | Generate Report Glossary (8 entries) | 200 |
| `tools/jit/mock/mock_definitions.py` | Generate Business Definition Glossary (~15 entries) | 300 |
| `tools/jit/mock/mock_technical.py` | Generate Technical Glossary (domain-tagged schema) | 150 |
| `tools/jit/mock/db_executor.py` | Execute SQL, return dict (Fabric swap point) | 30 |

## Phase 2 — Assistant Tools (sequential)

| File | Purpose | ~Lines |
|---|---|---|
| `tools/jit/search_reports.py` | Tool 1: Report Searcher (TF-IDF over Report Glossary) | 120 |
| `tools/jit/search_definitions.py` | Tool 2: Definition Searcher (TF-IDF + synonym + equivalence) | 200 |
| `tools/jit/search_technical.py` | Tool 3: Technical Searcher (domain → anchor → satellite) | 100 |
| `tools/jit/query_builder_v2.py` | Tool 4: Build from scratch (one table at a time, HITL) | 150 |
| `tools/jit/term_resolver.py` | Tool 5: Layered term resolution | 120 |
| `tools/jit/quantifier_extractor.py` | Tool 6: Regex threshold + date extraction | 60 |
| `tools/jit/router.py` | Intent classifier + cascade controller | 80 |

## Phase 3 — UI (depends on Phase 1+2)

| File | Purpose | ~Lines |
|---|---|---|
| `tools/jit/app/__init__.py` | Package init | 0 |
| `tools/jit/app/boot.py` | Wire all glossaries + DB + tools | 60 |
| `tools/jit/app/graph_panel.py` | FK subgraph + lineage → pyvis HTML | 80 |
| `tools/jit/app/streamlit_app.py` | Main Streamlit page with cascading HITL flow | 400 |

## Phase 4 — Data Files

| File | Purpose |
|---|---|
| `tools/jit/data/route_catalog.yaml` | Static known routes per category |
| `tools/jit/data/learned_terms.yaml` | Pre-populated from corpus, grows |
| `tools/jit/data/route_preferences.yaml` | User route choices (starts empty) |
| `tools/jit/data/user_library.json` | User → definition → query graph |

## Phase 5 — Tests

| File | Purpose |
|---|---|
| `tools/jit/tests/test_mock_db.py` | Data invariants |
| `tools/jit/tests/test_search_reports.py` | Report matching accuracy |
| `tools/jit/tests/test_search_definitions.py` | Definition matching, equivalence grouping |
| `tools/jit/tests/test_search_technical.py` | Domain → table resolution |
| `tools/jit/tests/test_query_builder_v2.py` | Step-by-step SQL generation |
| `tools/jit/tests/test_term_resolver.py` | Pattern recognition, glossary lookup |
| `tools/jit/tests/test_quantifier_extractor.py` | Threshold + date extraction |
| `tools/jit/tests/test_router.py` | Intent classification + cascade |

---

# Verification

1. `pytest tools/jit/tests/ -v` — all tests pass
2. Generate mock data: `python -m tools.jit.mock.mock_db` + `mock_reports` + `mock_definitions` + `mock_technical`
3. `streamlit run tools/jit/app/streamlit_app.py` → test cascading flow:
   - **Level 1 test:** "Show me the diabetes cohort report" → matches VW_DIABETIC_COHORT → run → 90 patients → user approves
   - **Level 2 test:** "How many diabetic patients have >3 ER visits and missed PCP?" → no single report matches → definition search finds 3 building blocks → user selects, sequences, approves each step → 90 → 45 → 20 → 22.2%
   - **Level 3 test:** "patients with Addison's disease" → no report, no definition → pattern recognition ("disease" → diagnosis) → technical glossary shows diagnosis routes → user picks → build from scratch
   - **Level 4 test:** User builds query step by step → validates → saved to all 3 glossaries
   - **Handoff test:** User can't find what they need → system packages context → "Pass to BI developer"
4. Test glossary growth: after Level 4, search again → new definition appears in results

# Fabric Portability

| Local Mock | Fabric Swap |
|---|---|
| `sqlite3.connect("mock.db")` | `pyodbc.connect(fabric_conn_str)` |
| Mock glossaries (YAML/JSON) | Same files, richer content from real SQL |
| `clarity_schema.yaml` | Same or extended |
| Learned terms + route prefs | Same files, carry over |
| User library (JSON) | Same or migrate to graph DB |
| Streamlit | Same app, deploy on Fabric |

---

# Design Discussion Log

Key topics debated during the planning session:

1. **Notebook vs. Streamlit** — Notebook is good for `ask()` calls but bad for HITL conversational flow. Streamlit chosen: all Python, zero frontend, runs in Fabric.

2. **Neo4j vs. NetworkX** — No Neo4j (not available at work). NetworkX in-memory is sufficient. Graph visualization via pyvis (already in repo).

3. **Automatic question decomposition rejected** — LLM-based splitting is brittle (~80% accuracy, fails on rephrasing). Regex splitting is worse. Instead: system searches, user selects building blocks. User IS the decomposer.

4. **Zero-match handling** — When no scope/definition matches, fall back to schema-level table suggestions by domain. System asks user "what kind of thing is this?" Pattern recognition (suffix + verb-frame) helps categorize unknown terms without LLM.

5. **Too-many-matches handling** — Group by equivalence (same anchor tables + similar filters). Flag breadth (scope mentions term but isn't primarily about it). Cap at 5-7 displayed, "show all" expansion.

6. **Term resolution layers** — Learned glossary (pre-populated from corpus at build time) → pattern recognition → LLM with tools (including web search) → ask user. Each confirmation saves to glossary for future.

7. **Quantifier/date pre-fill** — Extract from question text, show interpretation to user, flag divergence from scope's original values. User always confirms.

8. **Column lineage extractor provides definition boundaries** — INNER joins that change row count are definitional. This analysis already exists in the pipeline.
