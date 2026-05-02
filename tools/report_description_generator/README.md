# Tool 4 — Report Description Generator

**Status:** scaffolding (May Week 4 build)
**LLM mode:** opt-in (default OFF)
**License features:** `report_description` (always), `report_description_llm` (only when `use_llm=True`)

## What it does

Generates a natural-language description of what a SQL report produces:
- one-paragraph query summary
- primary purpose statement
- list of key computed metrics

Two modes:

- **Engineered (default)** — deterministic template assembled from the
  structured signals (column types, table set, filter clauses). No LLM.
- **LLM** — single LLM call that takes the structured signals and writes
  a fluent paragraph. Requires the `report_description_llm` feature.

Reuses Tool 3's business logic internally, which itself reuses Tool 2's
lineage, which reuses Tool 1's column inventory. Zero duplication.

## Public API

```python
from sql_logic_extractor.products import generate_report_description

# Default: deterministic, no LLM
desc = generate_report_description(sql, schema)
print(desc.technical_description)
print(desc.primary_purpose)
print(desc.key_metrics)

# LLM-enhanced
desc = generate_report_description(sql, schema, use_llm=True, llm_client=client)
```

## Delivery channels

- **CLI** (`cli.py`) — `sle-report <input.sql> --schema <yaml> [--use-llm]`
- **HTTP** (`api.py`) — `POST /api/v1/report/describe` with `{"use_llm": false}`

## Online vs offline

Same function in both. The deterministic template version is the
healthcare-safe default; the LLM version is the upsell.
