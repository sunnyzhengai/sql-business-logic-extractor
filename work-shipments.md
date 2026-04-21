# Work shipments

Append-only log of code sent to work (via personal → work email). Newest at top. This is the source of truth for "what's on the work machine now" so future updates know what to re-send.

Each entry records the commit SHA at the time of shipment; compare against current HEAD to see drift.

Target layout at work: **legacy top-level** (extract.py, normalize.py, resolve.py etc. at repo root). Package-relative imports (`from .extract import ...`) are rewritten to top-level form (`from extract import ...`) when emailing.

---

## Shipment 2 — 2026-04-20  (commit: `e4a52b9`)

**Channel:** personal → work email.

**Purpose:** upgrade the offline translator to the recursive pattern-library architecture and enable the Clarity-metadata → JSON-schema pipeline. Shipment 1 files (extract/normalize/resolve/compare_lineage) are unchanged since 2026-04-20 — do **not** re-send those.

**Files sent (as adapted to legacy top-level layout):**

| File at work | Source in repo | Import adjustment |
|---|---|---|
| `offline_translate.py` | `offline_translate.py` | `from sql_logic_extractor.patterns import` → `from patterns import` (one line near the top) |
| `patterns/__init__.py` | `sql_logic_extractor/patterns/__init__.py` | none |
| `patterns/base.py` | `sql_logic_extractor/patterns/base.py` | none |
| `patterns/registry.py` | `sql_logic_extractor/patterns/registry.py` | none |
| `patterns/walker.py` | `sql_logic_extractor/patterns/walker.py` | none |
| `patterns/columns.py` | `sql_logic_extractor/patterns/columns.py` | none |
| `patterns/aggregates.py` | `sql_logic_extractor/patterns/aggregates.py` | none |
| `patterns/scalar_functions.py` | `sql_logic_extractor/patterns/scalar_functions.py` | none |
| `patterns/structural.py` | `sql_logic_extractor/patterns/structural.py` | none |
| `scripts/csv_to_schema.py` | `scripts/csv_to_schema.py` | none (standalone script) |

**Layout at work after this shipment:**

```
<work dir>/
  extract.py              ← Shipment 1 (unchanged)
  normalize.py            ← Shipment 1 (unchanged)
  resolve.py              ← Shipment 1 (unchanged)
  compare_lineage.py      ← Shipment 1 (unchanged)
  offline_translate.py    ← Shipment 2 (replaces any prior version)
  patterns/               ← Shipment 2 (new self-contained package)
    __init__.py
    base.py
    registry.py
    walker.py
    columns.py
    aggregates.py
    scalar_functions.py
    structural.py
  scripts/
    csv_to_schema.py      ← Shipment 2 (utility for generating schema)
  clarity_schema.json     ← generated from your query via csv_to_schema.py
```

**Dependencies at work:** `pip install sqlglot` (only — `pyyaml` is no longer required because you're switching to JSON schemas; `yaml` is imported lazily).

**Runtime contract:** `offline_translate.py <l3_json> --schema clarity_schema.json` (same CLI as before; output JSON shape preserved plus new `ini_items` field on columns that resolve through the schema).

**Check for changes since this shipment:**

```bash
git diff e4a52b9 HEAD -- \
  offline_translate.py \
  sql_logic_extractor/patterns/ \
  scripts/csv_to_schema.py
```

---

## Shipment 1 — 2026-04-20  (commit: `f8500c3`)

**Channel:** personal → work email.

**Purpose:** minimum chain for extraction + compare at work. Extraction honors passthrough filter (normalize.py drops `passthrough` / `star` types). Compare covers all four match modes: exact, structural, semantic, and conflict (same-name-different-logic).

**Files sent (as adapted to legacy top-level layout):**

| File at work | Source in repo | Import adjustment |
|---|---|---|
| `extract.py` | `sql_logic_extractor/extract.py` | none (no project-internal imports) |
| `normalize.py` | `sql_logic_extractor/normalize.py` | `from .extract` → `from extract` |
| `resolve.py` | `sql_logic_extractor/resolve.py` | `from .extract` → `from extract` |
| `compare_lineage.py` | `compare_lineage.py` | none (already top-level) |

**Dependencies at work:** `pip install sqlglot` (no LLM libs; no openai).

**Not shipped** (but may be needed later): `metadata.py`, `offline_translate.py`, `clarity_schema.yaml`, `healthcare_schema.yaml`, `collibra.py`, `batch.py`.

**Check for changes since this shipment:**

```bash
git diff f8500c3 HEAD -- \
  sql_logic_extractor/extract.py \
  sql_logic_extractor/normalize.py \
  sql_logic_extractor/resolve.py \
  compare_lineage.py
```

Any files listed in that output need to be re-sent (with the same import adjustments applied).
