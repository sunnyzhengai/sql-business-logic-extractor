# Tool 2 — Technical Logic Extractor

**Status:** scaffolding (May Week 2 build)
**LLM mode:** not applicable (always deterministic)
**License feature:** `technical_logic`

## What it does

Given a SQL view or query, returns full per-output-column lineage:
- base columns (resolved through CTE chains and derived tables)
- base tables
- WHERE/JOIN/EXISTS filter predicates that constrain each column
- transformation chain through CTE scopes

Reuses Tool 1's column inventory internally — no duplication of logic.

## Public API

```python
from sql_logic_extractor.products import extract_technical_lineage

lineage = extract_technical_lineage(sql, dialect="tsql")
print(f"{len(lineage.inventory.columns)} column refs")
print(f"{len(lineage.resolved_columns)} output columns with lineage")
print(f"{len(lineage.query_filters)} distinct filter predicates")
```

## Delivery channels

- **CLI** (`cli.py`) — `sle-lineage <input.sql> [-o output.json]`
- **HTTP** (`api.py`) — `POST /api/v1/lineage/extract`

## Online vs offline

Same function. Healthcare-safe (deterministic, no LLM ever).
