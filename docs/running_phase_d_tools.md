# Running the Phase D tools

Copy-paste cells for a Fabric notebook (or run via CLI). Outputs are described under each section.

The `extract_corpus` and `extract_corpus_terms` tools both read a folder of `*.sql` files and write structured outputs. Both produce **scope-correct** output as of Phase D — CTE-scope filters do not leak into main-scope columns.

---

## 1. Build the corpus.jsonl (scope-tree shape)

### Fabric notebook

```python
from tools.p10_extract.batch import extract_corpus

extract_corpus(
    input_dir='/lakehouse/default/Files/views_healthy',
    output_path='/lakehouse/default/Files/outputs/corpus.jsonl',
    schema_path='/lakehouse/default/Files/schemas/clarity_schema.json',  # optional
)
```

### CLI

```bash
python -m tools.p10_extract.batch /path/to/views_dir \
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
from tools.p20_index.term_extraction import extract_corpus_terms

extract_corpus_terms(
    input_dir='/lakehouse/default/Files/views_healthy',
    output_path='/lakehouse/default/Files/outputs/terms.json',
)
# Add all_scopes=True to also get CTE-internal terms (default: main only)
```

### CLI

```bash
python -m tools.p20_index.term_extraction /path/to/views -o /path/to/outputs/terms.json
# Or with --all-scopes to include CTE-internal terms:
python -m tools.p20_index.term_extraction /path/to/views -o terms.json --all-scopes
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
from tools.p10_extract.batch import _read_sql_file

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

## 5. Compare view shapes  (REMOVED -- see `validate_graph_pivot`)

The `view_shape_compare` tool was removed in the 2026-05 restructure (Phase 1e). Its pairwise-comparison approach was superseded by graph-based community detection in `tools/diagnostics/validate_graph_pivot.py`, which:

- Builds a unified graph from the entire corpus (not view-by-view pairs).
- Runs Louvain community detection to find groups of structurally-related views.
- Auto-detects bridge tables (dimensions / shared lookups) by degree percentile rather than via a hand-curated `dim_tables.txt`.
- Emits per-community HTMLs, a primary-community-per-view assignment, and a cross-domain-views finding.

To run the new approach:

```python
from tools.operate.validate_graph_pivot import run_validation

run_validation(
    corpus_path='/lakehouse/default/Files/outputs/corpus.jsonl',
    output_dir='/lakehouse/default/Files/outputs/graph_pivot_validation',
)
```

Outputs: `graph.html` (overview), `communities/community_NN_*.html` (per-community drill-downs + index.html), `communities.md` (per-community summary + shared dimensions + cross-domain views), `validation_report.md` (verdict + recommendation).

The `dim_filter` utility (config-driven dim classifier) is preserved at `tools.shared.dim_filter` for use by tools that need a user-curated dim list rather than auto-detection (e.g., `cohort_extract`).

## 6. Render each view as a chain of datasets

After you've built `corpus.jsonl`, render each view's CTE / subquery / main scope as a sequence of "datasets" — one per scope, with name, base dataset (lineage edge), data columns (English), and filters (English).

### Fabric notebook

```python
from tools.p40_synthesize.dataset_extract import extract_datasets

extract_datasets(
    corpus_path='/lakehouse/default/Files/outputs/corpus.jsonl',
    output_dir='/lakehouse/default/Files/outputs/datasets',
)
```

### CLI

```bash
python -m tools.p40_synthesize.dataset_extract /path/to/corpus.jsonl -o /path/to/datasets
```

### Outputs (in `output_dir`)

- `datasets.md` — one section per view, each scope rendered as a sub-section. Best for human review or pasting into a wiki.
- `datasets.json` — same data, programmatic. One entry per view containing an ordered list of dataset dicts (`scope_id`, `name`, `kind`, `base_datasets`, `base_tables`, `data_columns`, `filters`).

A typical CTE view renders as:

```markdown
## v_my_view

### Active Patients  *(cte:ActivePatients)*
- **Reads tables:** PATIENT
- **Data columns:**
    - `PAT_ID`: Patient Identifier
- **Filters:**
    - *[where]* Status C = 1

### Age 12 Patients  *(cte:Age12Patients)*
- **Base dataset:** Active Patients
- **Reads tables:** PATIENT
- **Filters:**
    - *[where]* Age Years > 12

### Main query (view output)  *(main)*
- **Base dataset:** Age 12 Patients
- **Reads tables:** PATIENT, CLARITY_SER
- **Data columns:**
    - `PAT_ID`: Patient Identifier
    - `PCPProviderName`: Provider Name
```

CTE-scope filters stay in their CTE — they do NOT pollute downstream datasets. This is the same scope-correctness that powers the graph-pivot validation diagnostic.

## 7. Render each scope as a cohort (population-level governance)

For governance reviews where the question is "which view defines which population?" (vs. "what does each column do?"), use `cohort_extract`. Each scope gets a one-line cohort phrase and its filters in plain English — no column documentation.

### Fabric notebook

```python
from tools.p40_synthesize.cohort_extract import extract_cohorts

extract_cohorts(
    corpus_path='/lakehouse/default/Files/outputs/corpus.jsonl',
    output_dir='/lakehouse/default/Files/outputs/cohorts',
)
```

### CLI

```bash
python -m tools.p40_synthesize.cohort_extract /path/to/corpus.jsonl -o /path/to/cohorts
```

### Outputs

- `cohorts.md` — human-readable. One section per view, each scope rendered as `Cohort: <phrase>` + `Filters: <list>`.
- `cohorts.json` — same data, structured.

Cohort phrases come from `data/dictionaries/table_short_descriptions.yaml`. Long-term, the canonical place is a `TABLE_SHORT_DESCRIPTION` column on `clarity_metadata.csv` (paralleling the column-level `SHORT_DESCRIPTION` we already added). The YAML is the bootstrap / overlay; pass `--table-descriptions /custom/path.yaml` to use a different one.

The same `data/dictionaries/dim_tables.txt` filter applies, with one cohort-specific exception: when ALL tables in a scope are dim, the dims are kept as the cohort (a view that selects only from `PATIENT` IS a "patients" cohort, not empty).

## 8. Run the tests (optional, requires the `tests/` files)

```bash
# from repo root
python -m pytest tests/ tools/ -q
# expected: 292 passed
```

Specifically the Phase D scope-correctness tests:

```bash
python -m pytest tests/test_resolve_scoped.py \
    tools/p10_extract/tests/test_batch.py \
    tools/p20_index/tests/test_term_extraction.py \
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
