# Demo 1: Basic Extraction - Component Test for L3 + L4

## Purpose

Test L3 (resolve lineage) and L4 (LLM translate) on a single complex SQL query.

## The Story

You have a complex SQL query that calculates referral analytics. It contains:
- 5 nested CTEs (Common Table Expressions)
- Window functions (ROW_NUMBER, LAG, SUM OVER, RANK)
- Multiple CASE statements
- Aggregations and subqueries
- Joins across 6 tables

**Challenge:** How do you document what each output column actually means?

**Solution:**
- L3 (`resolve.py`) traces each column back to its source tables
- L4 (`llm_translate.py`) generates plain English definitions

## Input

`input/complex_referral_analytics.sql` - A 200+ line SQL query typical of healthcare analytics

## Output

| File | Layer | Description |
|------|-------|-------------|
| `output/parsed_complex_referral_analytics.json` | L3 | Technical lineage (machine-readable) |
| `output/parsed_complex_referral_analytics.txt` | L3 | Technical lineage (human-readable) |
| `output/L4_english_definitions.json` | L4 | English definitions (machine-readable) |
| `output/L4_english_definitions.txt` | L4 | English definitions (human-readable) |

## Example Output

### L3 Technical Definition
```json
{
  "column_name": "days_since_last_referral",
  "type": "calculated",
  "resolved_expression": "DATEDIFF(DAY, LAG(REFERRAL_DATE) OVER (...), REFERRAL_DATE)",
  "base_tables": ["REFERRAL"],
  "base_columns": ["REFERRAL.PAT_ID", "REFERRAL.REFERRAL_DATE"]
}
```

### L4 English Definition
```json
{
  "column_name": "days_since_last_referral",
  "english_definition": "Number of days between the current referral and the patient's previous referral",
  "business_domain": "Referral Metrics"
}
```

## How to Run

```bash
./run_demo.sh

# Or manually:
cd ../..
python3 resolve.py demos/01_basic_extraction/input/complex_referral_analytics.sql \
    --output demos/01_basic_extraction/output/parsed_complex_referral_analytics

# Then for English definitions (requires OPENAI_API_KEY):
python3 llm_translate.py demos/01_basic_extraction/output/parsed_complex_referral_analytics.json \
    --schema clarity_schema.yaml \
    --output demos/01_basic_extraction/output/L4_english_definitions
```

## What This Tests

- L3 (`resolve.py`): CTE inlining, window function parsing, filter extraction
- L4 (`llm_translate.py`): Accurate English translation using clarity_schema.yaml
