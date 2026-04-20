# Extraction Data Models

> 67 nodes · cohesion 0.13

## Key Concepts

- **SQLBusinessLogicExtractor** (185 connections) — `sql_logic_extractor/extract.py`
- **extract.py** (29 connections) — `sql_logic_extractor/extract.py`
- **Filter** (28 connections) — `sql_logic_extractor/extract.py`
- **QueryLogic** (27 connections) — `sql_logic_extractor/extract.py`
- **ColumnRef** (26 connections) — `sql_logic_extractor/extract.py`
- **Aggregation** (25 connections) — `sql_logic_extractor/extract.py`
- **CaseLogic** (25 connections) — `sql_logic_extractor/extract.py`
- **OutputColumn** (25 connections) — `sql_logic_extractor/extract.py`
- **WindowFunc** (25 connections) — `sql_logic_extractor/extract.py`
- **._extract_select()** (18 connections) — `sql_logic_extractor/extract.py`
- **_sql()** (16 connections) — `sql_logic_extractor/extract.py`
- **._extract_outputs()** (15 connections) — `sql_logic_extractor/extract.py`
- **to_dict()** (11 connections) — `sql_logic_extractor/extract.py`
- **DefinitionCatalog** (10 connections) — `sql_logic_extractor/normalize.py`
- **_extract_columns()** (9 connections) — `sql_logic_extractor/extract.py`
- **Return fully qualified TABLE.COLUMN string.** (9 connections) — `sql_logic_extractor/normalize.py`
- **Replace aliases in a SQL expression string with real table names.** (9 connections) — `sql_logic_extractor/normalize.py`
- **Normalize a SQL expression to a canonical form for comparison.     Strips aliase** (9 connections) — `sql_logic_extractor/normalize.py`
- **Replace specific column/table names with placeholders to get the structural patt** (9 connections) — `sql_logic_extractor/normalize.py`
- **Recursively abstract an AST node, replacing columns with <col> and literals with** (9 connections) — `sql_logic_extractor/normalize.py`
- **Hash a normalized expression for exact matching.** (9 connections) — `sql_logic_extractor/normalize.py`
- **Hash an abstracted pattern for structural matching.** (9 connections) — `sql_logic_extractor/normalize.py`
- **Classify an output column into a business logic category.     Returns (category,** (9 connections) — `sql_logic_extractor/normalize.py`
- **A single atomic, normalized business rule extracted from SQL.** (9 connections) — `sql_logic_extractor/normalize.py`
- **Classify a filter into a category.** (9 connections) — `sql_logic_extractor/normalize.py`
- *... and 42 more nodes in this community*

## Relationships

- No strong cross-community connections detected

## Source Files

- `sql_logic_extractor/extract.py`
- `sql_logic_extractor/normalize.py`

## Audit Trail

- EXTRACTED: 280 (36%)
- INFERRED: 489 (64%)
- AMBIGUOUS: 0 (0%)

---

*Part of the graphify knowledge wiki. See [[index]] to navigate.*