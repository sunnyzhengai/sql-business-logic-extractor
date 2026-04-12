# Demo 2: Conflict Detection - Integration Test for Full Pipeline

## Purpose

Test the complete `governance_extract.py` workflow - the main entry point that orchestrates L3 → L4 → L5 → Excel export.

**This is the recommended demo for Collibra.**

## The Story

Four different teams in your organization create reports:
- **Finance Team** - Tracks charges and revenue
- **Quality Team** - Monitors patient outcomes
- **Operations Team** - Manages appointments and scheduling
- **Billing Team** - Handles transactions and payments

Each team calculates common metrics like `length_of_stay` and `patient_age`, but **they define them differently**:

| Team | length_of_stay | patient_age |
|------|----------------|-------------|
| Finance | Days from admission to discharge | Age at admission |
| Quality | Days from admission to discharge + 1 | Age at discharge |
| Operations | Hours from appointment to contact | Current age |
| Billing | Days from service to posting | Age at service date |

**Challenge:** Without automated detection, these inconsistencies go unnoticed until reports conflict.

**Solution:** `governance_extract.py` processes all reports and outputs an Excel spreadsheet with conflicts flagged.

## Input

| File | Description |
|------|-------------|
| `input/report_finance.sql` | Finance team's patient stay report |
| `input/report_quality.sql` | Quality team's outcomes report |
| `input/report_operations.sql` | Operations team's scheduling report |
| `input/report_billing.sql` | Billing team's transaction report |

## Output

| File | Description |
|------|-------------|
| `output/governance_summary.xlsx` | Excel spreadsheet grouped by business logic term |
| `output/details/*_L3.json` | L3 lineage per report |
| `output/details/*_L4.json` | L4 English definitions per report |

## Key Findings

### Conflict 1: `length_of_stay` defined 4 different ways

```
❌ Different source tables:
   - report_billing: ARPB_TRANSACTIONS
   - report_finance: PAT_ENC_HSP
   - report_operations: PAT_ENC
   - report_quality: HSP_ACCOUNT

❌ Different expressions:
   - Billing:    DATEDIFF(DAY, SERVICE_DATE, POST_DATE)
   - Finance:    DATEDIFF(DAY, HOSP_ADMSN_TIME, HOSP_DISCH_TIME)
   - Operations: DATEDIFF(HOUR, APPT_TIME, CONTACT_DATE)  ← Hours, not days!
   - Quality:    DATEDIFF(DAY, ADM_DATE_TIME, DISCH_DATE_TIME) + 1  ← Plus one!
```

## How to Run

```bash
./run_demo.sh

# Or manually:
cd ../..
python3 governance_extract.py demos/02_conflict_detection/input/ \
    --schema clarity_schema.yaml \
    --output demos/02_conflict_detection/output/governance_summary.xlsx \
    --details-dir demos/02_conflict_detection/output
```

## What This Tests

- **L3** (`resolve.py`): Lineage resolution for each report
- **L4** (`llm_translate.py`): English definitions + report summaries
- **L5** (`compare_lineage.py`): Conflict detection across reports
- **Excel export**: Grouped output with status flags (CONFLICT/SIMILAR/CONSISTENT/UNIQUE)

## Collibra Integration

The Excel output maps directly to Collibra fields:
- **Report Description** ← L4 query summary
- **Business Definition** ← L4 english_definition
- **Technical Definition** ← L3 expression + filters
