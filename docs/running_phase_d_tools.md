# Running the Phase D tools

Copy-paste cells for a Fabric notebook (or run via CLI). Outputs are described under each section.

The `extract_corpus` and `extract_corpus_terms` tools both read a folder of `*.sql` files and write structured outputs. Both produce **scope-correct** output as of Phase D — CTE-scope filters do not leak into main-scope columns.

---

## 1. Build the corpus.jsonl (scope-tree shape)

### Fabric notebook

```python
from tools.extract_corpus.batch import extract_corpus

extract_corpus(
    input_dir='/lakehouse/default/Files/views_healthy',
    output_path='/lakehouse/default/Files/outputs/corpus.jsonl',
    schema_path='/lakehouse/default/Files/schemas/clarity_schema.json',  # optional
)
```

### CLI

```bash
python -m tools.extract_corpus.batch /path/to/views_dir \
    -o /path/to/outputs/corpus.jsonl \
    --schema /path/to/clarity_schema.json
```

### Outputs

- `corpus.jsonl` — one header line + one ViewV1 (tree-shaped) per line
- `corpus_progress.txt` — per-view timing log (live tail-able mid-run)

---

## 2. Build scope-correct Terms (governance)

### Fabric notebook

```python
from tools.term_extraction.batch import extract_corpus_terms

extract_corpus_terms(
    input_dir='/lakehouse/default/Files/views_healthy',
    output_path='/lakehouse/default/Files/outputs/terms.json',
)
# Add all_scopes=True to also get CTE-internal terms (default: main only)
```

### CLI

```bash
python -m tools.term_extraction.batch /path/to/views -o /path/to/outputs/terms.json
# Or with --all-scopes to include CTE-internal terms:
python -m tools.term_extraction.batch /path/to/views -o terms.json --all-scopes
```

### Outputs

- `terms.json` — full structured Term records
- `terms.csv` — same data flattened for spreadsheet review (sibling of the .json)

---

## 3. Inspect a sample view from corpus.jsonl

```python
import json

with open('/lakehouse/default/Files/outputs/corpus.jsonl') as f:
    header = json.loads(next(f))
    print(f"Header: {header}\n")
    for line in f:
        view = json.loads(line)
        if view['view_name'] == 'Reporting.V_CCHP_HOClinic_CycleTime.View':  # pick any
            print(f"=== {view['view_name']} ===")
            print(f"view_outputs: {view['view_outputs']}\n")
            for scope in view['scopes']:
                print(f"--- scope: {scope['id']} ({scope['kind']}) ---")
                print(f"  reads_from_tables: {scope['reads_from_tables']}")
                print(f"  reads_from_scopes: {scope['reads_from_scopes']}")
                print(f"  filters: {[(f['kind'], f['expression']) for f in scope['filters']]}")
                for col in scope['columns']:
                    print(f"  col {col['column_name']}:")
                    print(f"    technical: {col['technical_description'][:80]}")
                    print(f"    business:  {col['business_description'][:80]}")
                    print(f"    base_columns: {col['base_columns']}")
            print(f"\n=== business_description (bullet form) ===")
            print(view['report']['business_description'])
            break
```

Note: `view_name` is `path.stem` — strips only the `.sql` extension. So a file named `Reporting.V_CCHP_HOClinic_CycleTime.View.sql` matches `view_name == 'Reporting.V_CCHP_HOClinic_CycleTime.View'` (no `.sql` suffix).

---

## 4. Smoke check (one view, no file IO)

If you just want to see the new tree shape without writing a file:

```python
from pathlib import Path
from sql_logic_extractor.extract import SQLBusinessLogicExtractor, to_dict
from sql_logic_extractor.resolve import LineageResolver, preprocess_ssms
from tools.extract_corpus.batch import _read_sql_file

# _read_sql_file is BOM-aware (handles UTF-16-LE / UTF-16-BE / UTF-8-BOM,
# which SSMS export uses). Plain open().read() will fail with
# "utf-8 codec can't decode byte 0xff" on those files.
sql = _read_sql_file(Path('/path/to/one_view.sql'))

# preprocess_ssms strips SET ANSI_NULLS, GO, header comments so sqlglot
# can parse the underlying CREATE VIEW / SELECT.
clean_sql, _meta = preprocess_ssms(sql)
if not clean_sql.strip():
    clean_sql = sql.strip()

logic = to_dict(SQLBusinessLogicExtractor(dialect='tsql').extract(clean_sql))
tree = LineageResolver(logic).resolve_all_scoped()
for s in tree.scopes:
    print(f"{s.id} ({s.kind}): {len(s.columns)} cols, {len(s.filters)} filters")
    for f in s.filters:
        print(f"  filter [{f.kind}]: {f.expression}")
```

Useful when sanity-checking a specific complex view (e.g., one with deeply nested CTEs). The two preprocessing steps (BOM-aware read + `preprocess_ssms`) are normally hidden inside `extract_corpus`; this cell does them manually because it goes straight to the resolver.

---

## 5. Compare view shapes (table + join similarity)

After you've built `corpus.jsonl`, find views that share table+join structure:

### Fabric notebook

```python
from tools.view_shape_compare.batch import compare_view_shapes

compare_view_shapes(
    corpus_path='/lakehouse/default/Files/outputs/corpus.jsonl',
    output_dir='/lakehouse/default/Files/outputs/view_shapes',
)
```

### CLI

```bash
python -m tools.view_shape_compare.batch /path/to/corpus.jsonl -o /path/to/view_shapes
```

### Outputs (in `output_dir`)

- `clusters.csv` — groups of views that are **structurally identical** (same all_tables AND same all_joins). Each row = one cluster of duplicate-shape views.
- `cross_pairs.csv` — pair-level findings for weaker relationships:
  - `fact_subset` / `fact_superset` — one view's fact tables ⊊ the other's
  - `fact_overlap` — facts intersect; neither subset
  - `same_facts_different_joins` — same fact tables, different join graph
  - `dim_extension` — same fact tables AND fact joins, different dim tables
  - `join_subset` — same fact tables, joins ⊊
  - `same_driver` — same FROM driver but otherwise unrelated (weak signal)
- `features.csv` — per-view shape: driver, fact tables, dim count, joins (useful for spreadsheet review).

Dim-table noise (PATIENT, ZC_*, CLARITY_*) is filtered before fact comparison via `data/dictionaries/dim_tables.txt`. Edit that file to grow the list when you spot false positives. Use `--dim-filter /custom/path.txt` to override.

## 6. Run the tests (optional, requires the `tests/` files)

```bash
# from repo root
python -m pytest tests/ tools/ -q
# expected: 292 passed
```

Specifically the Phase D scope-correctness tests:

```bash
python -m pytest tests/test_resolve_scoped.py \
    tools/extract_corpus/tests/test_batch.py \
    tools/term_extraction/tests/test_batch.py \
    -v
```

---

## What to expect from scope-correct output

For a CTE view like:

```sql
WITH ActivePatients AS (
    SELECT P.PAT_ID
    FROM Clarity.dbo.PATIENT P
    WHERE P.STATUS_C = 1
)
SELECT AP.PAT_ID
FROM ActivePatients AP
WHERE AP.PAT_ID > 100
```

The corpus output produces TWO scopes:

- `cte:ActivePatients` — owns `STATUS_C = 1` only
- `main` — owns `PAT_ID > 100` only; `reads_from_scopes` is `["cte:ActivePatients"]`; main column's `base_columns` is `["cte:ActivePatients.PAT_ID"]`

The CTE filter does **not** appear on the main scope or its columns. This is the fix for the false-grouping risk in similar-term comparison.
