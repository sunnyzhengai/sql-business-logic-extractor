# SQLBusinessLogicExtractor

> God node · 185 connections · `sql_logic_extractor/extract.py`

## Connections by Relation

### calls
- [[resolve_query()]] `INFERRED`
- [[extract_definitions()]] `INFERRED`
- [[._extract_sources()]] `EXTRACTED`
- [[._extract_ctes()]] `EXTRACTED`
- [[._add_subquery()]] `EXTRACTED`
- [[._add_subquery_from_select()]] `EXTRACTED`

### contains
- [[extract.py]] `EXTRACTED`

### method
- [[._extract_select()]] `EXTRACTED`
- [[._extract_outputs()]] `EXTRACTED`
- [[.extract()]] `EXTRACTED`
- [[._extract_set_operations()]] `EXTRACTED`
- [[._extract_joins()]] `EXTRACTED`
- [[._extract_where()]] `EXTRACTED`
- [[._extract_having()]] `EXTRACTED`
- [[._extract_case_details()]] `EXTRACTED`
- [[._extract_statement()]] `EXTRACTED`
- [[._extract_window_details()]] `EXTRACTED`
- [[._extract_subqueries()]] `EXTRACTED`
- [[._build_lineage()]] `EXTRACTED`
- [[._extract_group_by()]] `EXTRACTED`
- [[._extract_order_by()]] `EXTRACTED`
- [[._split_conditions()]] `EXTRACTED`
- [[.__init__()]] `EXTRACTED`

### uses
- [[AliasResolver]] `INFERRED`
- [[BusinessLogicNormalizer]] `INFERRED`
- [[BusinessDefinition]] `INFERRED`
- [[ResolvedColumn]] `INFERRED`
- [[ResolvedQuery]] `INFERRED`
- [[CollibraConfig]] `INFERRED`
- [[TestClassification]] `INFERRED`
- [[ScopeRegistry]] `INFERRED`
- [[LineageResolver]] `INFERRED`
- [[DefinitionCatalog]] `INFERRED`
- [[A single atomic, normalized business rule extracted from SQL.]] `INFERRED`
- [[Collection of business definitions from one or more queries.]] `INFERRED`
- [[Resolves table aliases to real table names within a query.]] `INFERRED`
- [[Return fully qualified TABLE.COLUMN string.]] `INFERRED`
- [[Replace aliases in a SQL expression string with real table names.]] `INFERRED`
- [[Normalize a SQL expression to a canonical form for comparison.     Strips aliase]] `INFERRED`
- [[Replace specific column/table names with placeholders to get the structural patt]] `INFERRED`
- [[Recursively abstract an AST node, replacing columns with <col> and literals with]] `INFERRED`
- [[Hash a normalized expression for exact matching.]] `INFERRED`
- [[Hash an abstracted pattern for structural matching.]] `INFERRED`

---

*Part of the graphify knowledge wiki. See [[index]] to navigate.*