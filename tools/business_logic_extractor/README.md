# Tool 3 — Business Logic Extractor

**Status:** scaffolding (May Week 3 build)
**LLM mode:** opt-in (default OFF)
**License features:** `business_logic` (always), `business_logic_llm` (only when `use_llm=True`)

## What it does

For each transformed output column in a SQL view, returns an English-
language definition of what that column represents. Uses the data
dictionary (schema YAML) to resolve column codes and reference values.

Two modes, customer-selected per call:

- **Engineered (default)** — pattern library, deterministic. No LLM
  involved at any point. Healthcare-safe.
- **LLM** — sends the column expression + schema context to an LLM for a
  polished definition. Customer must hold the `business_logic_llm`
  feature. The LLM client library is lazy-imported only when this branch
  runs, so no-LLM customers don't even have the package installed.

Reuses Tool 2's lineage internally.

## Public API

```python
from sql_logic_extractor.products import extract_business_logic

# Default: deterministic, no LLM
bl = extract_business_logic(sql, schema)

# LLM-enhanced (requires business_logic_llm feature)
bl = extract_business_logic(sql, schema, use_llm=True, llm_client=client)
```

## Delivery channels

- **CLI** (`cli.py`) — `sle-business <input.sql> --schema <yaml> [--use-llm]`
- **HTTP** (`api.py`) — `POST /api/v1/business-logic/extract` with `{"use_llm": false}` in body

## Online vs offline

Same function in both. Offline + LLM mode = customer brings their own LLM
API key (BYOK); their data goes only where their LLM is.
