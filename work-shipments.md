# Work shipments

Append-only log of code sent to work (via personal → work email). Newest at top. This is the source of truth for "what's on the work machine now" so future updates know what to re-send.

Each entry records the commit SHA at the time of shipment; compare against current HEAD to see drift.

Target layout at work: **legacy top-level** (extract.py, normalize.py, resolve.py etc. at repo root). Package-relative imports (`from .extract import ...`) are rewritten to top-level form (`from extract import ...`) when emailing.

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
