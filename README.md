# SQL Logic Extractor — 4-Tool Product Line

Engine + four layered tools that extract structured meaning from SQL views,
for healthcare BI / data-governance teams.

## The 4 tools (each builds on the previous)

| # | Tool | What it produces | Status |
|---|---|---|---|
| 1 | **Column Lineage Extractor** (`tools/column_lineage_extractor/`) | Every (database, schema, table, column) reference in a SQL view, with CTE flattening and alias resolution. Always deterministic; no LLM. | Live (May Week 1, 13 tests) |
| 2 | **Technical Logic Extractor** (`tools/technical_logic_extractor/`) | Per-output-column lineage with WHERE/JOIN/EXISTS filters propagated. Always deterministic; no LLM. | Functional, productization in May Week 2 |
| 3 | **Business Logic Extractor** (`tools/business_logic_extractor/`) | English definition for each transformed column. Default deterministic; optional LLM. | Scaffolded; May Week 3 |
| 4 | **Report Description Generator** (`tools/report_description_generator/`) | Natural-language summary of what the SQL report does. Default deterministic; optional LLM. | Scaffolded; May Week 4 |

Tools 1 and 2 are deterministic by design (no LLM possible). Tools 3 and 4
ship with an `--use-llm` toggle that defaults OFF — the engineered mode
is healthcare-safe (no data leaves the customer's premises, no LLM ever
called).

## Repo layout

```
sql-logic-extractor/
├── sql_logic_extractor/          ← the engine (importable Python package)
│   ├── products.py                # the 4 tool functions
│   ├── license.py                 # feature gating
│   ├── extract.py / normalize.py / resolve.py / translate.py
│   └── patterns/
│
├── tools/                        ← the 4 product wrappers (CLI + HTTP)
│   ├── column_lineage_extractor/
│   ├── technical_logic_extractor/
│   ├── business_logic_extractor/
│   └── report_description_generator/
│
├── case_studies/                 ← real-world deployments / proof points
│   └── ssis_to_fabric_migration/  # Use Case #1 — healthcare BI migration
│
├── data/                         ← schemas + sample SQL + demos
│   ├── schemas/                   # clarity_schema.yaml, healthcare_schema.yaml
│   ├── queries/                   # sample SQL fixtures
│   └── demos/                     # demo flows
│
├── tests/                        ← engine-level tests
├── planning/                     ← monthly / weekly / daily roadmap
├── wiki/                         ← curated concept knowledge base
└── docs/                         ← work shipments + archived code
```

## Quick start

Install the engine:
```bash
pip install -e .
```

Run a tool's CLI on a single SQL file:
```bash
python -m tools.column_lineage_extractor.cli path/to/view.sql -o columns.csv
python -m tools.technical_logic_extractor.cli path/to/view.sql -o lineage.json
python -m tools.business_logic_extractor.cli path/to/view.sql --schema data/schemas/clarity_schema.yaml
python -m tools.report_description_generator.cli path/to/view.sql --schema data/schemas/clarity_schema.yaml
```

For folder/batch processing, see each tool's `batch.py` (where present).

## Tests

```bash
python -m pytest                              # engine + tool tests
python case_studies/ssis_to_fabric_migration/tests/run_tests.py   # case-study fixtures
```

## Planning

The 4-tool roadmap, by month and week, lives in `planning/`. May builds the
tools; June ships the website + 3 monetization tiers (free single-use, paid
subscription, on-prem license); July markets; August onboards first paying
customers + Collibra connector; September adds an AI agent layer.
