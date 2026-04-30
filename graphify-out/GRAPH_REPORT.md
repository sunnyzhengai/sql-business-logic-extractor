# Graph Report - /Users/admin/sql-logic-extractor  (2026-04-29)

## Corpus Check
- 67 files · ~125,382 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1197 nodes · 2749 edges · 102 communities detected
- Extraction: 62% EXTRACTED · 38% INFERRED · 0% AMBIGUOUS · INFERRED: 1037 edges (avg confidence: 0.59)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_Community 0|Community 0]]
- [[_COMMUNITY_Community 1|Community 1]]
- [[_COMMUNITY_Community 2|Community 2]]
- [[_COMMUNITY_Community 3|Community 3]]
- [[_COMMUNITY_Community 4|Community 4]]
- [[_COMMUNITY_Community 5|Community 5]]
- [[_COMMUNITY_Community 6|Community 6]]
- [[_COMMUNITY_Community 7|Community 7]]
- [[_COMMUNITY_Community 8|Community 8]]
- [[_COMMUNITY_Community 9|Community 9]]
- [[_COMMUNITY_Community 10|Community 10]]
- [[_COMMUNITY_Community 11|Community 11]]
- [[_COMMUNITY_Community 12|Community 12]]
- [[_COMMUNITY_Community 13|Community 13]]
- [[_COMMUNITY_Community 14|Community 14]]
- [[_COMMUNITY_Community 15|Community 15]]
- [[_COMMUNITY_Community 16|Community 16]]
- [[_COMMUNITY_Community 17|Community 17]]
- [[_COMMUNITY_Community 18|Community 18]]
- [[_COMMUNITY_Community 19|Community 19]]
- [[_COMMUNITY_Community 20|Community 20]]
- [[_COMMUNITY_Community 21|Community 21]]
- [[_COMMUNITY_Community 22|Community 22]]
- [[_COMMUNITY_Community 23|Community 23]]
- [[_COMMUNITY_Community 24|Community 24]]
- [[_COMMUNITY_Community 25|Community 25]]
- [[_COMMUNITY_Community 26|Community 26]]
- [[_COMMUNITY_Community 27|Community 27]]
- [[_COMMUNITY_Community 28|Community 28]]
- [[_COMMUNITY_Community 29|Community 29]]
- [[_COMMUNITY_Community 30|Community 30]]
- [[_COMMUNITY_Community 31|Community 31]]
- [[_COMMUNITY_Community 32|Community 32]]
- [[_COMMUNITY_Community 33|Community 33]]
- [[_COMMUNITY_Community 34|Community 34]]
- [[_COMMUNITY_Community 35|Community 35]]
- [[_COMMUNITY_Community 36|Community 36]]
- [[_COMMUNITY_Community 37|Community 37]]
- [[_COMMUNITY_Community 38|Community 38]]
- [[_COMMUNITY_Community 39|Community 39]]
- [[_COMMUNITY_Community 40|Community 40]]
- [[_COMMUNITY_Community 41|Community 41]]
- [[_COMMUNITY_Community 42|Community 42]]
- [[_COMMUNITY_Community 43|Community 43]]
- [[_COMMUNITY_Community 44|Community 44]]
- [[_COMMUNITY_Community 45|Community 45]]
- [[_COMMUNITY_Community 46|Community 46]]
- [[_COMMUNITY_Community 47|Community 47]]
- [[_COMMUNITY_Community 48|Community 48]]
- [[_COMMUNITY_Community 49|Community 49]]
- [[_COMMUNITY_Community 50|Community 50]]
- [[_COMMUNITY_Community 51|Community 51]]
- [[_COMMUNITY_Community 52|Community 52]]
- [[_COMMUNITY_Community 53|Community 53]]
- [[_COMMUNITY_Community 54|Community 54]]
- [[_COMMUNITY_Community 55|Community 55]]
- [[_COMMUNITY_Community 56|Community 56]]
- [[_COMMUNITY_Community 57|Community 57]]
- [[_COMMUNITY_Community 58|Community 58]]
- [[_COMMUNITY_Community 59|Community 59]]
- [[_COMMUNITY_Community 60|Community 60]]
- [[_COMMUNITY_Community 61|Community 61]]
- [[_COMMUNITY_Community 62|Community 62]]
- [[_COMMUNITY_Community 63|Community 63]]
- [[_COMMUNITY_Community 64|Community 64]]
- [[_COMMUNITY_Community 65|Community 65]]
- [[_COMMUNITY_Community 66|Community 66]]
- [[_COMMUNITY_Community 67|Community 67]]
- [[_COMMUNITY_Community 68|Community 68]]
- [[_COMMUNITY_Community 69|Community 69]]
- [[_COMMUNITY_Community 70|Community 70]]
- [[_COMMUNITY_Community 71|Community 71]]
- [[_COMMUNITY_Community 72|Community 72]]
- [[_COMMUNITY_Community 73|Community 73]]
- [[_COMMUNITY_Community 74|Community 74]]
- [[_COMMUNITY_Community 75|Community 75]]
- [[_COMMUNITY_Community 76|Community 76]]
- [[_COMMUNITY_Community 77|Community 77]]
- [[_COMMUNITY_Community 78|Community 78]]
- [[_COMMUNITY_Community 79|Community 79]]
- [[_COMMUNITY_Community 80|Community 80]]
- [[_COMMUNITY_Community 81|Community 81]]
- [[_COMMUNITY_Community 82|Community 82]]
- [[_COMMUNITY_Community 83|Community 83]]
- [[_COMMUNITY_Community 84|Community 84]]
- [[_COMMUNITY_Community 85|Community 85]]
- [[_COMMUNITY_Community 86|Community 86]]
- [[_COMMUNITY_Community 87|Community 87]]
- [[_COMMUNITY_Community 88|Community 88]]
- [[_COMMUNITY_Community 89|Community 89]]
- [[_COMMUNITY_Community 90|Community 90]]
- [[_COMMUNITY_Community 91|Community 91]]
- [[_COMMUNITY_Community 92|Community 92]]
- [[_COMMUNITY_Community 93|Community 93]]
- [[_COMMUNITY_Community 94|Community 94]]
- [[_COMMUNITY_Community 95|Community 95]]
- [[_COMMUNITY_Community 96|Community 96]]
- [[_COMMUNITY_Community 97|Community 97]]
- [[_COMMUNITY_Community 98|Community 98]]
- [[_COMMUNITY_Community 99|Community 99]]
- [[_COMMUNITY_Community 100|Community 100]]
- [[_COMMUNITY_Community 101|Community 101]]

## God Nodes (most connected - your core abstractions)
1. `SQLBusinessLogicExtractor` - 243 edges
2. `AliasResolver` - 73 edges
3. `BusinessLogicNormalizer` - 70 edges
4. `BusinessDefinition` - 64 edges
5. `Translation` - 59 edges
6. `extract()` - 40 edges
7. `BusinessLogicComparator` - 33 edges
8. `Filter` - 30 edges
9. `QueryLogic` - 27 edges
10. `ResolvedColumn` - 26 edges

## Surprising Connections (you probably didn't know these)
- `A query with fully resolved lineage for every output column.` --uses--> `SQLBusinessLogicExtractor`  [INFERRED]
  sql_logic_extractor/resolve.py → /Users/admin/sql-logic-extractor/sql_logic_extractor/extract.py
- `Register a scope's outputs.` --uses--> `SQLBusinessLogicExtractor`  [INFERRED]
  sql_logic_extractor/resolve.py → /Users/admin/sql-logic-extractor/sql_logic_extractor/extract.py
- `SQL Business Logic Extractor -- parse, normalize, compare, resolve, and translat` --uses--> `SQLBusinessLogicExtractor`  [INFERRED]
  sql_logic_extractor/__init__.py → /Users/admin/sql-logic-extractor/sql_logic_extractor/extract.py
- `SQL Business Logic Extractor -- parse, normalize, compare, resolve, and translat` --uses--> `BusinessLogicNormalizer`  [INFERRED]
  sql_logic_extractor/__init__.py → /Users/admin/sql-logic-extractor/sql_logic_extractor/normalize.py
- `SQL Business Logic Extractor -- parse, normalize, compare, resolve, and translat` --uses--> `BusinessDefinition`  [INFERRED]
  sql_logic_extractor/__init__.py → /Users/admin/sql-logic-extractor/sql_logic_extractor/normalize.py

## Communities

### Community 0 - "Community 0"
Cohesion: 0.02
Nodes (98): fmt_sql(), Pretty-print SQL using sqlglot., SQLBusinessLogicExtractor, Recursively register nested CTEs and subqueries., Parse, extract, and resolve lineage for a SQL query.      Raises ValueError for, Serialize a ResolvedFilter, nesting any resolved subquery lineage., Format resolved query as human-readable text., Derive output filename from view name or input file. (+90 more)

### Community 1 - "Community 1"
Cohesion: 0.03
Nodes (123): _is_notebook(), _load_schema(), main(), CLI entry point for Tool 3 -- Business logic extractor., Read a SQL file, handling SSMS's default UTF-16 LE BOM and other     common enco, _read_sql_file(), SQL Business Logic Extractor -- parse, normalize, compare, resolve, and translat, current_license() (+115 more)

### Community 2 - "Community 2"
Cohesion: 0.05
Nodes (96): describe_column(), describe_qualified(), get_column_description(), get_table_description(), get_value_description(), load_csv(), Return 'Description (TABLE.COLUMN)' or just 'TABLE.COLUMN' if no description., Describe a 'TABLE.COLUMN' string. (+88 more)

### Community 3 - "Community 3"
Cohesion: 0.05
Nodes (83): batch_process(), BatchResult, build_business_logic(), build_manifest(), build_transformations(), _classify_error(), _error_row(), _filter_text() (+75 more)

### Community 4 - "Community 4"
Cohesion: 0.06
Nodes (75): avg(), count(), max_agg(), min_agg(), Aggregate function patterns: COUNT, SUM, AVG, MIN, MAX., sum_agg(), Context, Pattern (+67 more)

### Community 5 - "Community 5"
Cohesion: 0.05
Nodes (65): ComparisonReport, ConflictGroup, _def_to_dict(), _describe_group(), format_report(), LineageComparator, main(), MatchGroup (+57 more)

### Community 6 - "Community 6"
Cohesion: 0.06
Nodes (47): BusinessLogicComparator, ComparisonReport, _compute_group_similarity(), _def_summary(), _describe_differences(), _describe_group(), main(), MatchGroup (+39 more)

### Community 7 - "Community 7"
Cohesion: 0.1
Nodes (23): CTEDef, _extract_columns(), _get_alias(), _is_aggregate(), _is_case(), _is_literal(), _is_simple_column(), _is_star() (+15 more)

### Community 8 - "Community 8"
Cohesion: 0.19
Nodes (36): Aggregation, CaseLogic, ColumnRef, Filter, OutputColumn, QueryLogic, WindowFunc, _abstract_node() (+28 more)

### Community 9 - "Community 9"
Cohesion: 0.08
Nodes (23): _dedupe_resolved_filters(), LineageResolver, Register a scope's outputs., Register a scope's outputs., Look up an output definition in a scope., Get business-relevant filters for a scope as intermediate dicts.          Each r, Check if a name refers to a base table (not a CTE/subquery)., Check if a name refers to a CTE or subquery. (+15 more)

### Community 10 - "Community 10"
Cohesion: 0.07
Nodes (22): Test cases for Lineage Resolution -- tracing every output to base table.column., Column passes through 2 CTEs to base table., DATEDIFF in CTE A, passthrough in CTE B, selected in main., CASE in CTE B references calculated column from CTE A., Filters from multiple CTEs accumulate., Column from derived table resolves to base table., Outer WHERE on derived table + inner WHERE both captured., CTE joined with base table -- both resolve correctly. (+14 more)

### Community 11 - "Community 11"
Cohesion: 0.09
Nodes (33): classify_business_domain(), describe_column_ref(), expand_abbreviations(), format_output(), get_column_description(), get_enum_value(), load_schema(), main() (+25 more)

### Community 12 - "Community 12"
Cohesion: 0.11
Nodes (21): export_neo4j_direct(), export_to_neo4j(), GraphData, import_to_neo4j_direct(), main(), parse_l3_file(), Write graph data to Neo4j CSV format., Main export function.      Args:         input_path: Path to folder containing L (+13 more)

### Community 13 - "Community 13"
Cohesion: 0.12
Nodes (23): build_manifest(), _build_qualifier_map(), _error_row(), extract_view_refs(), _flatten_cte_columns(), _is_notebook(), main(), _qualify_table() (+15 more)

### Community 14 - "Community 14"
Cohesion: 0.14
Nodes (24): _canonical_filter(), classify_business_domain(), _dedupe_filters(), _filter_text(), format_output(), _is_correlation_key(), load_schema(), main() (+16 more)

### Community 15 - "Community 15"
Cohesion: 0.14
Nodes (20): _build_llm_context(), classify_business_domain(), _filter_text(), load_schema(), make_llm_client(), Translate L3 filter predicates by walking each with the registry.     Distinguis, Best-effort domain bucket from name/table heuristics. Useful as a     grouping s, Translate one resolved column (from the L3 resolver) into a     business-logic d (+12 more)

### Community 16 - "Community 16"
Cohesion: 0.16
Nodes (19): _build_manifest_rows(), _build_qualifier_map(), _build_transformation_rows(), extract_to_csvs(), extract_view(), _filter_text(), main(), _manifest_error_row() (+11 more)

### Community 17 - "Community 17"
Cohesion: 0.17
Nodes (12): execute_cypher(), format_results_for_display(), get_graph_stats(), get_neo4j_driver(), nl_to_cypher(), Get cached Neo4j driver., Convert natural language question to Cypher query., Convert Neo4j types to clean, readable Python types. (+4 more)

### Community 18 - "Community 18"
Cohesion: 0.29
Nodes (6): Per-column ground-truth lineage tests for queries/bi_complex/input.sql.  Rule ap, The query produces exactly 19 output columns., Every bi_complex output column resolves to its exact set of base tables., resolved(), test_column_base_tables(), test_column_count()

### Community 19 - "Community 19"
Cohesion: 0.43
Nodes (6): _child_env(), list_queries(), main(), Inject project root onto PYTHONPATH so subprocesses can import     `sql_logic_ex, Always launch the resolver as the installed package — _child_env() puts the, _resolve_cmd()

### Community 20 - "Community 20"
Cohesion: 0.4
Nodes (1): HTTP entry point for Tool 3 -- Business logic extractor (online SaaS).  Implemen

### Community 21 - "Community 21"
Cohesion: 1.0
Nodes (2): csv_to_schema(), main()

### Community 22 - "Community 22"
Cohesion: 1.0
Nodes (0): 

### Community 23 - "Community 23"
Cohesion: 1.0
Nodes (0): 

### Community 24 - "Community 24"
Cohesion: 1.0
Nodes (0): 

### Community 25 - "Community 25"
Cohesion: 1.0
Nodes (1): LLM-backed translator. Lazy-imports the client lib so a no-LLM     install doesn

### Community 26 - "Community 26"
Cohesion: 1.0
Nodes (1): Ungated core for Tool 3. Tool 4's core calls this directly.

### Community 27 - "Community 27"
Cohesion: 1.0
Nodes (1): Tool 3 -- English business definition for each transformed column.     `use_llm=

### Community 28 - "Community 28"
Cohesion: 1.0
Nodes (1): Deterministic report summary built from the structured signals.      TODO (May W

### Community 29 - "Community 29"
Cohesion: 1.0
Nodes (1): LLM-backed summary. Lazy-imports the client lib.      TODO (May Week 4): port cl

### Community 30 - "Community 30"
Cohesion: 1.0
Nodes (1): Ungated core for Tool 4.

### Community 31 - "Community 31"
Cohesion: 1.0
Nodes (1): Tool 4 -- natural-language description of what the SQL report does.     `use_llm

### Community 32 - "Community 32"
Cohesion: 1.0
Nodes (1): Re-encode the SSMS marker SQL as UTF-16 LE with a BOM in /tmp so the     runner

### Community 33 - "Community 33"
Cohesion: 1.0
Nodes (1): Tool 1 -- enumerate every (database, schema, table, column) the SQL     referenc

### Community 34 - "Community 34"
Cohesion: 1.0
Nodes (1): Ungated core for Tool 2. Tool 3's core calls this directly.

### Community 35 - "Community 35"
Cohesion: 1.0
Nodes (1): Tool 2 -- per-output-column lineage with WHERE/JOIN/EXISTS filter     propagatio

### Community 36 - "Community 36"
Cohesion: 1.0
Nodes (1): Pattern-library translator -- pure deterministic logic, no LLM.      TODO (May W

### Community 37 - "Community 37"
Cohesion: 1.0
Nodes (1): LLM-backed translator. Lazy-imports the client lib so a no-LLM     install doesn

### Community 38 - "Community 38"
Cohesion: 1.0
Nodes (1): Ungated core for Tool 3. Tool 4's core calls this directly.

### Community 39 - "Community 39"
Cohesion: 1.0
Nodes (1): Tool 3 -- English business definition for each transformed column.     `use_llm=

### Community 40 - "Community 40"
Cohesion: 1.0
Nodes (1): Deterministic report summary built from the structured signals.      TODO (May W

### Community 41 - "Community 41"
Cohesion: 1.0
Nodes (1): LLM-backed summary. Lazy-imports the client lib.      TODO (May Week 4): port cl

### Community 42 - "Community 42"
Cohesion: 1.0
Nodes (1): Ungated core for Tool 4.

### Community 43 - "Community 43"
Cohesion: 1.0
Nodes (1): Tool 4 -- natural-language description of what the SQL report does.     `use_llm

### Community 44 - "Community 44"
Cohesion: 1.0
Nodes (1): Extract transformed (non-passthrough) columns from a folder of SQL views.  Singl

### Community 45 - "Community 45"
Cohesion: 1.0
Nodes (1): Read a SQL file, handling SSMS's default UTF-16 LE BOM.

### Community 46 - "Community 46"
Cohesion: 1.0
Nodes (1): Return a short label describing the transformation in this expression.     Retur

### Community 47 - "Community 47"
Cohesion: 1.0
Nodes (1): Find the top-level SELECT inside a parsed CREATE VIEW (or bare SELECT).

### Community 48 - "Community 48"
Cohesion: 1.0
Nodes (1): Render a node's SQL for the CSV, collapsing whitespace and truncating     if abs

### Community 49 - "Community 49"
Cohesion: 1.0
Nodes (1): Parse one view; return a list of CSV rows, one per transformed column.

### Community 50 - "Community 50"
Cohesion: 1.0
Nodes (1): Remove USE / GO / SET-option statements and header block comments that     SSMS

### Community 51 - "Community 51"
Cohesion: 1.0
Nodes (1): (database, schema, table) for a sqlglot Table node. In sqlglot's MySQL-     root

### Community 52 - "Community 52"
Cohesion: 1.0
Nodes (1): {alias_or_name (lowercased): (db, schema, table)} so we can resolve any     alia

### Community 53 - "Community 53"
Cohesion: 1.0
Nodes (1): For each CTE, qualify its body separately and emit:         {(cte_alias_lower, c

### Community 54 - "Community 54"
Cohesion: 1.0
Nodes (1): Parse one view file → list of manifest rows. Importable for per-file     use in

### Community 55 - "Community 55"
Cohesion: 1.0
Nodes (1): Notebook-callable entry point. Walks input_dir for *.sql files, parses     each,

### Community 56 - "Community 56"
Cohesion: 1.0
Nodes (1): Notebook-callable entry point. Walks input_dir for *.sql files, parses     each,

### Community 57 - "Community 57"
Cohesion: 1.0
Nodes (1): (database, schema, table) for a sqlglot Table node. In sqlglot's MySQL-     root

### Community 58 - "Community 58"
Cohesion: 1.0
Nodes (1): {alias_or_name (lowercased): (db, schema, table)} so we can resolve any     alia

### Community 59 - "Community 59"
Cohesion: 1.0
Nodes (1): For each CTE, qualify its body separately and emit:         {(cte_alias_lower, c

### Community 60 - "Community 60"
Cohesion: 1.0
Nodes (1): Parse one view file → list of manifest rows. Importable for per-file     use in

### Community 61 - "Community 61"
Cohesion: 1.0
Nodes (1): Notebook-callable entry point. Walks input_dir for *.sql files, parses     each,

### Community 62 - "Community 62"
Cohesion: 1.0
Nodes (1): True if the filter is a bare `column = column` relational key, not a row-restric

### Community 63 - "Community 63"
Cohesion: 1.0
Nodes (1): Parse and re-emit with table qualifiers stripped, for dedup purposes.

### Community 64 - "Community 64"
Cohesion: 1.0
Nodes (1): Drop correlation keys and collapse filters that differ only in alias qualifiers.

### Community 65 - "Community 65"
Cohesion: 1.0
Nodes (1): Load and index the clarity_schema.yaml for fast lookup.      Returns:         {

### Community 66 - "Community 66"
Cohesion: 1.0
Nodes (1): Get description for a table.

### Community 67 - "Community 67"
Cohesion: 1.0
Nodes (1): Get description for a column.

### Community 68 - "Community 68"
Cohesion: 1.0
Nodes (1): Get enum value mappings for a ZC_ table.

### Community 69 - "Community 69"
Cohesion: 1.0
Nodes (1): Build context string for a single resolved column.      Args:         resolved_c

### Community 70 - "Community 70"
Cohesion: 1.0
Nodes (1): Translate a single resolved column to English using LLM.      Args:         reso

### Community 71 - "Community 71"
Cohesion: 1.0
Nodes (1): Generate a summary of the entire SQL query based on column definitions.      Arg

### Community 72 - "Community 72"
Cohesion: 1.0
Nodes (1): Translate all columns in an L5 output file to English and generate query summary

### Community 73 - "Community 73"
Cohesion: 1.0
Nodes (1): Format translation results for output.      Args:         results: Dict with 'su

### Community 74 - "Community 74"
Cohesion: 1.0
Nodes (1): Use the package form when available (repo dev); otherwise the flat form (work la

### Community 75 - "Community 75"
Cohesion: 1.0
Nodes (1): Parse a bare SQL fragment (not wrapped in SELECT) and translate.

### Community 76 - "Community 76"
Cohesion: 1.0
Nodes (1): Convert dataclass tree to plain dict, filtering empty fields.

### Community 77 - "Community 77"
Cohesion: 1.0
Nodes (1): Load clarity_schema.yaml as the raw dict. The pattern library builds     its own

### Community 78 - "Community 78"
Cohesion: 1.0
Nodes (1): Classify the business domain based on column name, expression, and base tables.

### Community 79 - "Community 79"
Cohesion: 1.0
Nodes (1): Parse a resolved SQL expression and walk it with the pattern registry.      Fall

### Community 80 - "Community 80"
Cohesion: 1.0
Nodes (1): Translate L3 filter predicates by walking each with the registry.      Distingui

### Community 81 - "Community 81"
Cohesion: 1.0
Nodes (1): Parse a bare SQL fragment (not wrapped in SELECT) and translate.

### Community 82 - "Community 82"
Cohesion: 1.0
Nodes (1): Classify the business domain based on column name, expression, and base tables.

### Community 83 - "Community 83"
Cohesion: 1.0
Nodes (1): Parse a resolved SQL expression and walk it with the pattern registry.      Fall

### Community 84 - "Community 84"
Cohesion: 1.0
Nodes (1): Translate L3 filter predicates by walking each with the registry.      Distingui

### Community 85 - "Community 85"
Cohesion: 1.0
Nodes (1): Parse a bare SQL fragment (not wrapped in SELECT) and translate.

### Community 86 - "Community 86"
Cohesion: 1.0
Nodes (1): Load and index the clarity_schema.yaml for fast lookup.

### Community 87 - "Community 87"
Cohesion: 1.0
Nodes (1): Expand common abbreviations in column/table names.

### Community 88 - "Community 88"
Cohesion: 1.0
Nodes (1): Get description for a column, fall back to abbreviation expansion.

### Community 89 - "Community 89"
Cohesion: 1.0
Nodes (1): Get enum value name from ZC_ table.

### Community 90 - "Community 90"
Cohesion: 1.0
Nodes (1): Translate DATEDIFF expressions.

### Community 91 - "Community 91"
Cohesion: 1.0
Nodes (1): Translate CASE expressions.

### Community 92 - "Community 92"
Cohesion: 1.0
Nodes (1): Translate aggregate functions (SUM, AVG, COUNT, etc.).

### Community 93 - "Community 93"
Cohesion: 1.0
Nodes (1): Translate window functions (ROW_NUMBER, RANK, LAG, etc.).

### Community 94 - "Community 94"
Cohesion: 1.0
Nodes (1): Get a human-readable description for a column reference.

### Community 95 - "Community 95"
Cohesion: 1.0
Nodes (1): Classify the business domain based on column name and context.

### Community 96 - "Community 96"
Cohesion: 1.0
Nodes (1): Translate a SQL expression to plain English.

### Community 97 - "Community 97"
Cohesion: 1.0
Nodes (1): Translate filter conditions to plain English.

### Community 98 - "Community 98"
Cohesion: 1.0
Nodes (1): Translate a single resolved column to English using templates.

### Community 99 - "Community 99"
Cohesion: 1.0
Nodes (1): Generate a summary of the entire SQL query.

### Community 100 - "Community 100"
Cohesion: 1.0
Nodes (1): Translate all columns in an L3 output file to English.      This is the main ent

### Community 101 - "Community 101"
Cohesion: 1.0
Nodes (1): Format translation results for output.

## Knowledge Gaps
- **270 isolated node(s):** `Read SQL handling SSMS's default UTF-16 LE BOM and other encodings.`, `Run Tool 2 on one file, shape into transformation rows. Skip     truly-trivial p`, `Folder mode entry point. Returns 0 on success, 1 on usage error.`, `Read a SQL file, handling SSMS's default UTF-16 LE BOM and other     common enco`, `One row per Table node referenced (catches SELECT *, EXISTS, etc.)     that the` (+265 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Community 22`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 23`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 24`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 25`** (1 nodes): `LLM-backed translator. Lazy-imports the client lib so a no-LLM     install doesn`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 26`** (1 nodes): `Ungated core for Tool 3. Tool 4's core calls this directly.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 27`** (1 nodes): `Tool 3 -- English business definition for each transformed column.     `use_llm=`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 28`** (1 nodes): `Deterministic report summary built from the structured signals.      TODO (May W`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 29`** (1 nodes): `LLM-backed summary. Lazy-imports the client lib.      TODO (May Week 4): port cl`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 30`** (1 nodes): `Ungated core for Tool 4.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 31`** (1 nodes): `Tool 4 -- natural-language description of what the SQL report does.     `use_llm`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 32`** (1 nodes): `Re-encode the SSMS marker SQL as UTF-16 LE with a BOM in /tmp so the     runner`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 33`** (1 nodes): `Tool 1 -- enumerate every (database, schema, table, column) the SQL     referenc`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 34`** (1 nodes): `Ungated core for Tool 2. Tool 3's core calls this directly.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 35`** (1 nodes): `Tool 2 -- per-output-column lineage with WHERE/JOIN/EXISTS filter     propagatio`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 36`** (1 nodes): `Pattern-library translator -- pure deterministic logic, no LLM.      TODO (May W`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 37`** (1 nodes): `LLM-backed translator. Lazy-imports the client lib so a no-LLM     install doesn`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 38`** (1 nodes): `Ungated core for Tool 3. Tool 4's core calls this directly.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 39`** (1 nodes): `Tool 3 -- English business definition for each transformed column.     `use_llm=`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 40`** (1 nodes): `Deterministic report summary built from the structured signals.      TODO (May W`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 41`** (1 nodes): `LLM-backed summary. Lazy-imports the client lib.      TODO (May Week 4): port cl`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 42`** (1 nodes): `Ungated core for Tool 4.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 43`** (1 nodes): `Tool 4 -- natural-language description of what the SQL report does.     `use_llm`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 44`** (1 nodes): `Extract transformed (non-passthrough) columns from a folder of SQL views.  Singl`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 45`** (1 nodes): `Read a SQL file, handling SSMS's default UTF-16 LE BOM.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 46`** (1 nodes): `Return a short label describing the transformation in this expression.     Retur`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 47`** (1 nodes): `Find the top-level SELECT inside a parsed CREATE VIEW (or bare SELECT).`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 48`** (1 nodes): `Render a node's SQL for the CSV, collapsing whitespace and truncating     if abs`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 49`** (1 nodes): `Parse one view; return a list of CSV rows, one per transformed column.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 50`** (1 nodes): `Remove USE / GO / SET-option statements and header block comments that     SSMS`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 51`** (1 nodes): `(database, schema, table) for a sqlglot Table node. In sqlglot's MySQL-     root`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 52`** (1 nodes): `{alias_or_name (lowercased): (db, schema, table)} so we can resolve any     alia`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 53`** (1 nodes): `For each CTE, qualify its body separately and emit:         {(cte_alias_lower, c`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 54`** (1 nodes): `Parse one view file → list of manifest rows. Importable for per-file     use in`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 55`** (1 nodes): `Notebook-callable entry point. Walks input_dir for *.sql files, parses     each,`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 56`** (1 nodes): `Notebook-callable entry point. Walks input_dir for *.sql files, parses     each,`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 57`** (1 nodes): `(database, schema, table) for a sqlglot Table node. In sqlglot's MySQL-     root`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 58`** (1 nodes): `{alias_or_name (lowercased): (db, schema, table)} so we can resolve any     alia`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 59`** (1 nodes): `For each CTE, qualify its body separately and emit:         {(cte_alias_lower, c`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 60`** (1 nodes): `Parse one view file → list of manifest rows. Importable for per-file     use in`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 61`** (1 nodes): `Notebook-callable entry point. Walks input_dir for *.sql files, parses     each,`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 62`** (1 nodes): `True if the filter is a bare `column = column` relational key, not a row-restric`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 63`** (1 nodes): `Parse and re-emit with table qualifiers stripped, for dedup purposes.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 64`** (1 nodes): `Drop correlation keys and collapse filters that differ only in alias qualifiers.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 65`** (1 nodes): `Load and index the clarity_schema.yaml for fast lookup.      Returns:         {`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 66`** (1 nodes): `Get description for a table.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 67`** (1 nodes): `Get description for a column.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 68`** (1 nodes): `Get enum value mappings for a ZC_ table.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 69`** (1 nodes): `Build context string for a single resolved column.      Args:         resolved_c`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 70`** (1 nodes): `Translate a single resolved column to English using LLM.      Args:         reso`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 71`** (1 nodes): `Generate a summary of the entire SQL query based on column definitions.      Arg`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 72`** (1 nodes): `Translate all columns in an L5 output file to English and generate query summary`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 73`** (1 nodes): `Format translation results for output.      Args:         results: Dict with 'su`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 74`** (1 nodes): `Use the package form when available (repo dev); otherwise the flat form (work la`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 75`** (1 nodes): `Parse a bare SQL fragment (not wrapped in SELECT) and translate.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 76`** (1 nodes): `Convert dataclass tree to plain dict, filtering empty fields.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 77`** (1 nodes): `Load clarity_schema.yaml as the raw dict. The pattern library builds     its own`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 78`** (1 nodes): `Classify the business domain based on column name, expression, and base tables.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 79`** (1 nodes): `Parse a resolved SQL expression and walk it with the pattern registry.      Fall`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 80`** (1 nodes): `Translate L3 filter predicates by walking each with the registry.      Distingui`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 81`** (1 nodes): `Parse a bare SQL fragment (not wrapped in SELECT) and translate.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 82`** (1 nodes): `Classify the business domain based on column name, expression, and base tables.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 83`** (1 nodes): `Parse a resolved SQL expression and walk it with the pattern registry.      Fall`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 84`** (1 nodes): `Translate L3 filter predicates by walking each with the registry.      Distingui`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 85`** (1 nodes): `Parse a bare SQL fragment (not wrapped in SELECT) and translate.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 86`** (1 nodes): `Load and index the clarity_schema.yaml for fast lookup.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 87`** (1 nodes): `Expand common abbreviations in column/table names.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 88`** (1 nodes): `Get description for a column, fall back to abbreviation expansion.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 89`** (1 nodes): `Get enum value name from ZC_ table.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 90`** (1 nodes): `Translate DATEDIFF expressions.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 91`** (1 nodes): `Translate CASE expressions.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 92`** (1 nodes): `Translate aggregate functions (SUM, AVG, COUNT, etc.).`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 93`** (1 nodes): `Translate window functions (ROW_NUMBER, RANK, LAG, etc.).`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 94`** (1 nodes): `Get a human-readable description for a column reference.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 95`** (1 nodes): `Classify the business domain based on column name and context.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 96`** (1 nodes): `Translate a SQL expression to plain English.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 97`** (1 nodes): `Translate filter conditions to plain English.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 98`** (1 nodes): `Translate a single resolved column to English using templates.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 99`** (1 nodes): `Generate a summary of the entire SQL query.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 100`** (1 nodes): `Translate all columns in an L3 output file to English.      This is the main ent`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 101`** (1 nodes): `Format translation results for output.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `SQLBusinessLogicExtractor` connect `Community 0` to `Community 1`, `Community 2`, `Community 3`, `Community 6`, `Community 7`, `Community 8`, `Community 9`?**
  _High betweenness centrality (0.371) - this node is a cross-community bridge._
- **Why does `SQL Business Logic Extractor -- parse, normalize, compare, resolve, and translat` connect `Community 1` to `Community 0`, `Community 2`, `Community 3`, `Community 4`, `Community 6`?**
  _High betweenness centrality (0.157) - this node is a cross-community bridge._
- **Why does `resolve_query()` connect `Community 3` to `Community 0`, `Community 1`, `Community 2`, `Community 5`, `Community 7`, `Community 9`, `Community 10`, `Community 16`, `Community 18`?**
  _High betweenness centrality (0.144) - this node is a cross-community bridge._
- **Are the 216 inferred relationships involving `SQLBusinessLogicExtractor` (e.g. with `ResolvedFilter` and `ResolvedColumn`) actually correct?**
  _`SQLBusinessLogicExtractor` has 216 INFERRED edges - model-reasoned connections that need verification._
- **Are the 65 inferred relationships involving `AliasResolver` (e.g. with `SQLBusinessLogicExtractor` and `QueryLogic`) actually correct?**
  _`AliasResolver` has 65 INFERRED edges - model-reasoned connections that need verification._
- **Are the 61 inferred relationships involving `BusinessLogicNormalizer` (e.g. with `SQLBusinessLogicExtractor` and `QueryLogic`) actually correct?**
  _`BusinessLogicNormalizer` has 61 INFERRED edges - model-reasoned connections that need verification._
- **Are the 60 inferred relationships involving `BusinessDefinition` (e.g. with `SQLBusinessLogicExtractor` and `QueryLogic`) actually correct?**
  _`BusinessDefinition` has 60 INFERRED edges - model-reasoned connections that need verification._