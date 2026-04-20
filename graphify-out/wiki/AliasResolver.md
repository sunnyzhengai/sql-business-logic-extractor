# AliasResolver

> God node · 73 connections · `sql_logic_extractor/normalize.py`

## Connections by Relation

### calls
- [[.normalize()]] `EXTRACTED`
- [[.test_01_simple_alias()]] `INFERRED`
- [[.test_02_column_resolution()]] `INFERRED`
- [[.test_03_expression_resolution()]] `INFERRED`
- [[.test_04_same_logic_different_aliases()]] `INFERRED`
- [[.test_05_cte_alias()]] `INFERRED`

### contains
- [[normalize.py]] `EXTRACTED`

### method
- [[.resolve_table()]] `EXTRACTED`
- [[.resolve_expression()]] `EXTRACTED`
- [[.resolve_column()]] `EXTRACTED`
- [[.__init__()]] `EXTRACTED`
- [[._build_map()]] `EXTRACTED`

### rationale_for
- [[Resolves table aliases to real table names within a query.]] `EXTRACTED`

### uses
- [[SQLBusinessLogicExtractor]] `INFERRED`
- [[Filter]] `INFERRED`
- [[QueryLogic]] `INFERRED`
- [[ColumnRef]] `INFERRED`
- [[OutputColumn]] `INFERRED`
- [[CaseLogic]] `INFERRED`
- [[Aggregation]] `INFERRED`
- [[WindowFunc]] `INFERRED`
- [[TestClassification]] `INFERRED`
- [[TestAliasResolution]] `INFERRED`
- [[TestSignatures]] `INFERRED`
- [[Look up column description, return readable name.]] `INFERRED`
- [[Short version -- just the description without the qualified name.]] `INFERRED`
- [[Look up a value description for categorical columns.]] `INFERRED`
- [[Try to parse 'col op value' from a simple filter expression. Returns (col, op, v]] `INFERRED`
- [[Translate DATEDIFF expressions.]] `INFERRED`
- [[Translate CASE expressions into readable classification rules.]] `INFERRED`
- [[Translate a CASE WHEN condition to readable text.]] `INFERRED`
- [[Translate aggregation expressions.]] `INFERRED`
- [[Translate window function expressions.]] `INFERRED`

---

*Part of the graphify knowledge wiki. See [[index]] to navigate.*