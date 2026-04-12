# SQL Business Logic Extractor

Automatically extract, document, and compare business logic from SQL queries. Transform complex SQL into structured definitions for data governance, documentation, and semantic layer integration.

## The Problem

Organizations accumulate hundreds of SQL reports over time. Each contains embedded business logic - calculations, filters, transformations - but this logic is:
- **Undocumented** - What does `los_category` actually mean?
- **Inconsistent** - Different teams define "length of stay" differently
- **Hidden** - Logic buried in CTEs, subqueries, and CASE statements
- **Untraceable** - Which source columns feed each output?

**Manual documentation is impractical:** For 900 reports, manual extraction would take 3000+ hours (1.5 FTEs). Nobody has that bandwidth. Data governance stalls.

## The Solution

This tool parses SQL queries and extracts business logic in minutes:

```
SQL Files → L3 (Resolve) → L4 (Translate) → L5 (Compare) → Excel Spreadsheet
```

**Output:**
- Detailed JSON/text files per report (for deep dives)
- Summary Excel spreadsheet grouped by business logic (for steward curation)

## Quick Start: Batch Governance Extract

```bash
# Process all SQL files in a folder → Excel spreadsheet
python3 governance_extract.py ./sql_reports/ \
    --schema clarity_schema.yaml \
    --output governance_summary.xlsx \
    --details-dir output/

# Requires: pip install sqlglot openai openpyxl pyyaml
# Set: export OPENAI_API_KEY="your-key"
```

**Output:**
- `governance_summary.xlsx` - Spreadsheet grouped by business logic term
- `output/details/` - Individual L3/L4 JSON files per report

## Architecture

```
L1 (Parse) → L2 (Normalize) → L3 (Resolve) → L4 (Translate) → L5 (Compare)
     ↓              ↓              ↓               ↓               ↓
 extract.py    normalize.py    resolve.py    llm_translate.py  compare_lineage.py
```

| Layer | Module | Purpose |
|-------|--------|---------|
| L1 | `extract.py` | Parse SQL into AST, extract raw column definitions |
| L2 | `normalize.py` | Normalize expressions, classify calculation types |
| L3 | `resolve.py` | **Resolve full lineage** - trace through CTEs to base tables |
| L4 | `llm_translate.py` | **LLM translation** - English definitions using data dictionary |
| L5 | `compare_lineage.py` | **Compare & detect conflicts** - find duplicates and inconsistencies |
| Main | `governance_extract.py` | **Orchestration** - batch process + Excel export |

## Demo Scenarios

See the `demos/` folder:

| Demo | Type | Purpose |
|------|------|---------|
| `01_basic_extraction/` | Component | Test L3 (resolve) + L4 (translate) on one complex query |
| `02_conflict_detection/` | **Integration** | Test full pipeline via `governance_extract.py` |

## Excel Output Format

The governance spreadsheet groups definitions by business logic term:

| Business Logic Term | Status | # Variations | Report Name | Business Definition | Technical Definition | Source Tables | Assigned To | Review Status |
|---------------------|--------|--------------|-------------|---------------------|---------------------|---------------|-------------|---------------|
| length_of_stay | CONFLICT | 4 | report_finance | Days from admission to discharge | DATEDIFF(DAY, HOSP_ADMSN_TIME, HOSP_DISCH_TIME) | PAT_ENC_HSP | | |
| | | | report_quality | Days from admission to discharge plus one | DATEDIFF(DAY, ADM_DATE_TIME, DISCH_DATE_TIME) + 1 | HSP_ACCOUNT | | |
| | | | report_billing | Days from service to posting | DATEDIFF(DAY, SERVICE_DATE, POST_DATE) | ARPB_TRANSACTIONS | | |

**Status values:**
- `CONFLICT` - Same name, different logic (needs resolution)
- `SIMILAR` - Structurally similar (needs review)
- `CONSISTENT` - Same logic across reports (OK)
- `UNIQUE` - Single definition

## Workflow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        AUTOMATED (minutes)                              │
│                                                                         │
│   governance_extract.py ./sql_reports/ --output governance.xlsx         │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│                        HUMAN-IN-THE-LOOP                                │
│                                                                         │
│   BI Manager divides spreadsheet among stewards                         │
│   Stewards review, approve, or flag for discussion                      │
│   Measurable: 200 terms assigned, 150 completed (75%)                   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│                        COLLIBRA INGESTION                               │
│                                                                         │
│   Report Description     ← L4 query summary                             │
│   Business Definition    ← L4 english_definition                        │
│   Technical Definition   ← L3 expression + filters                      │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

## Data Dictionary

The tool uses `clarity_schema.yaml` (Epic Clarity data dictionary) for:
- Table and column descriptions
- Enum value mappings (e.g., `ADT_PAT_CLASS_C: 1 = Inpatient`)
- Relationship context for LLM translation

## Individual Script Usage

```bash
# L3: Extract lineage from a single SQL query
python3 resolve.py query.sql --output output/query

# L5: Compare multiple queries for conflicts
python3 compare_lineage.py query1.sql query2.sql --output comparison.txt

# L4: Generate English definitions for a single query
python3 llm_translate.py output/query.json --schema clarity_schema.yaml
```

## Requirements

```bash
pip install sqlglot openai openpyxl pyyaml
```

- Python 3.8+
- `sqlglot` - SQL parsing
- `openai` - LLM translation
- `openpyxl` - Excel export
- `pyyaml` - Schema loading

## License

MIT
