# SQL Business Logic Extractor - Demos

This folder contains demonstration scenarios showing how the SQL Business Logic Extractor solves real data governance challenges.

## Demo Scenarios

### `01_basic_extraction/` - Component Test: L3 + L4
**Purpose:** Test L3 (resolve lineage) and L4 (LLM translate) on a single complex query.

- Input: One complex SQL query with CTEs, window functions, CASE statements
- Output: L3 technical lineage + L4 English definitions
- **Tests:** `resolve.py` and `llm_translate.py`

### `02_conflict_detection/` - Integration Test: Full Pipeline
**Purpose:** Test the complete `governance_extract.py` workflow with multiple reports that have conflicting definitions.

- Input: 4 SQL reports from different teams (Finance, Quality, Operations, Billing)
- Output: Excel spreadsheet showing conflicts for `length_of_stay`, `patient_age`, `readmission_flag`
- **Tests:** Full pipeline (L3 → L4 → L5 → Excel)

---

## Quick Start

```bash
# Demo 1: Component test for L3 + L4
cd 01_basic_extraction && ./run_demo.sh

# Demo 2: Integration test for full pipeline (RECOMMENDED FOR COLLIBRA DEMO)
cd 02_conflict_detection && ./run_demo.sh
```

## For Collibra Demo

**Start with Demo 2** - it shows the full value proposition:
1. 4 teams define the same metrics differently
2. Tool detects conflicts automatically
3. Excel output ready for steward curation
4. Fields map directly to Collibra (Report Description, Business Definition, Technical Definition)
