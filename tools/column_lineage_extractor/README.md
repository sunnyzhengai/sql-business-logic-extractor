# Tool 1 — Column Lineage Extractor

**Status:** scaffolding (May Week 1 build)
**LLM mode:** not applicable (always deterministic)
**License feature:** `columns`

## What it does

Given a SQL view or query, returns a flat list of every distinct
`(database, schema, table, column)` reference. Filters out CTE-internal
identifiers. Resolves table aliases to their underlying tables.

## Public API

```python
from sql_logic_extractor.products import extract_columns

inventory = extract_columns(sql, dialect="tsql")
for c in inventory.columns:
    print(c.qualified())   # e.g. "Clarity.dbo.PATIENT.PAT_ID"
```

## Delivery channels

- **CLI** (`cli.py`) — `sle-columns <input.sql> [-o output.csv]`
- **HTTP** (`api.py`) — `POST /api/v1/columns/extract`

## Online vs offline

Same function. Online = run on your servers; offline = ship as a wheel.
Healthcare-safe deployments work either way: this tool never calls an LLM.
