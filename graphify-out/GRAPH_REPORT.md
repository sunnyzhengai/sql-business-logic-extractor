# Graph Report - /Users/admin/sql-logic-extractor  (2026-04-19)

## Corpus Check
- 24 files · ~81,396 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 660 nodes · 1759 edges · 15 communities detected
- Extraction: 64% EXTRACTED · 36% INFERRED · 0% AMBIGUOUS · INFERRED: 635 edges (avg confidence: 0.53)
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

## God Nodes (most connected - your core abstractions)
1. `SQLBusinessLogicExtractor` - 185 edges
2. `AliasResolver` - 73 edges
3. `BusinessLogicNormalizer` - 70 edges
4. `BusinessDefinition` - 64 edges
5. `extract()` - 39 edges
6. `BusinessLogicComparator` - 33 edges
7. `Filter` - 28 edges
8. `QueryLogic` - 27 edges
9. `ResolvedColumn` - 26 edges
10. `ColumnRef` - 26 edges

## Surprising Connections (you probably didn't know these)
- `SQLBusinessLogicExtractor` --uses--> `Pretty-print SQL using sqlglot.`  [INFERRED]
  sql_logic_extractor/extract.py → tests/dump_all.py
- `SQLBusinessLogicExtractor` --uses--> `Test cases for SQL Business Logic Extractor Healthcare analytics focus -- Epic C`  [INFERRED]
  sql_logic_extractor/extract.py → tests/test_queries.py
- `SQLBusinessLogicExtractor` --uses--> `Plain column select -- no transformations.`  [INFERRED]
  sql_logic_extractor/extract.py → tests/test_queries.py
- `SQLBusinessLogicExtractor` --uses--> `Column aliases -- still passthrough.`  [INFERRED]
  sql_logic_extractor/extract.py → tests/test_queries.py
- `SQLBusinessLogicExtractor` --uses--> `Simple WHERE -- single filter condition.`  [INFERRED]
  sql_logic_extractor/extract.py → tests/test_queries.py

## Communities

### Community 0 - "Community 0"
Cohesion: 0.07
Nodes (61): SQLBusinessLogicExtractor, AliasResolver, BusinessLogicNormalizer, A fully resolved output column with its complete transformation chain., ResolvedColumn, def_categories(), defs_by_name(), extract_logic() (+53 more)

### Community 1 - "Community 1"
Cohesion: 0.04
Nodes (48): extract(), filter_exprs(), names(), Test cases for SQL Business Logic Extractor Healthcare analytics focus -- Epic C, Literal/constant value in SELECT., Standard inner join -- encounter to patient., Left join -- encounters with optional diagnosis., Multiple joins with calculated LOS (length of stay). (+40 more)

### Community 2 - "Community 2"
Cohesion: 0.06
Nodes (48): BusinessLogicComparator, ComparisonReport, _compute_group_similarity(), _def_summary(), _describe_differences(), _describe_group(), main(), MatchGroup (+40 more)

### Community 3 - "Community 3"
Cohesion: 0.06
Nodes (47): ComparisonReport, ConflictGroup, _def_to_dict(), _describe_group(), format_report(), LineageComparator, main(), MatchGroup (+39 more)

### Community 4 - "Community 4"
Cohesion: 0.08
Nodes (56): batch_process(), BatchResult, _classify_error(), main(), _progress(), Process all SQL files matching pattern under path.      Args:         path: Dire, Write combined Collibra import files., Write a summary report. (+48 more)

### Community 5 - "Community 5"
Cohesion: 0.19
Nodes (35): Aggregation, CaseLogic, ColumnRef, Filter, OutputColumn, QueryLogic, WindowFunc, _abstract_node() (+27 more)

### Community 6 - "Community 6"
Cohesion: 0.07
Nodes (22): Test cases for Lineage Resolution -- tracing every output to base table.column., Column passes through 2 CTEs to base table., DATEDIFF in CTE A, passthrough in CTE B, selected in main., CASE in CTE B references calculated column from CTE A., Filters from multiple CTEs accumulate., Column from derived table resolves to base table., Outer WHERE on derived table + inner WHERE both captured., CTE joined with base table -- both resolve correctly. (+14 more)

### Community 7 - "Community 7"
Cohesion: 0.15
Nodes (35): describe_column(), describe_qualified(), get_column_description(), get_table_description(), get_value_description(), load_csv(), Return 'Description (TABLE.COLUMN)' or just 'TABLE.COLUMN' if no description., Describe a 'TABLE.COLUMN' string. (+27 more)

### Community 8 - "Community 8"
Cohesion: 0.12
Nodes (17): CTEDef, _extract_columns(), _get_alias(), _is_aggregate(), _is_case(), _is_literal(), _is_simple_column(), _is_star() (+9 more)

### Community 9 - "Community 9"
Cohesion: 0.09
Nodes (33): classify_business_domain(), describe_column_ref(), expand_abbreviations(), format_output(), get_column_description(), get_enum_value(), load_schema(), main() (+25 more)

### Community 10 - "Community 10"
Cohesion: 0.1
Nodes (17): LineageResolver, Recursively register nested CTEs and subqueries., Register a scope's outputs., Look up an output definition in a scope., Get all business-relevant filters for a scope (WHERE, HAVING, QUALIFY, and non-e, Check if a name refers to a base table (not a CTE/subquery)., Check if a name refers to a CTE or subquery., Get alias->table mapping for a scope. (+9 more)

### Community 11 - "Community 11"
Cohesion: 0.11
Nodes (21): export_neo4j_direct(), export_to_neo4j(), GraphData, import_to_neo4j_direct(), main(), parse_l3_file(), Write graph data to Neo4j CSV format., Main export function.      Args:         input_path: Path to folder containing L (+13 more)

### Community 12 - "Community 12"
Cohesion: 0.15
Nodes (19): build_column_context(), format_output(), get_column_description(), get_enum_values(), get_table_description(), load_schema(), main(), Translate a single resolved column to English using LLM.      Args:         reso (+11 more)

### Community 13 - "Community 13"
Cohesion: 0.17
Nodes (12): execute_cypher(), format_results_for_display(), get_graph_stats(), get_neo4j_driver(), nl_to_cypher(), Get cached Neo4j driver., Convert natural language question to Cypher query., Convert Neo4j types to clean, readable Python types. (+4 more)

### Community 14 - "Community 14"
Cohesion: 0.67
Nodes (2): fmt_sql(), Pretty-print SQL using sqlglot.

## Knowledge Gaps
- **83 isolated node(s):** `Load and index the clarity_schema.yaml for fast lookup.`, `Expand common abbreviations in column/table names.`, `Get description for a column, fall back to abbreviation expansion.`, `Get enum value name from ZC_ table.`, `Translate DATEDIFF expressions.` (+78 more)
  These have ≤1 connection - possible missing edges or undocumented components.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `SQLBusinessLogicExtractor` connect `Community 0` to `Community 1`, `Community 2`, `Community 4`, `Community 5`, `Community 8`, `Community 10`, `Community 14`?**
  _High betweenness centrality (0.439) - this node is a cross-community bridge._
- **Why does `resolve_query()` connect `Community 4` to `Community 0`, `Community 1`, `Community 3`, `Community 6`, `Community 7`, `Community 8`, `Community 10`?**
  _High betweenness centrality (0.254) - this node is a cross-community bridge._
- **Why does `BusinessDefinition` connect `Community 2` to `Community 0`, `Community 4`, `Community 5`?**
  _High betweenness centrality (0.105) - this node is a cross-community bridge._
- **Are the 164 inferred relationships involving `SQLBusinessLogicExtractor` (e.g. with `resolve_query()` and `extract_definitions()`) actually correct?**
  _`SQLBusinessLogicExtractor` has 164 INFERRED edges - model-reasoned connections that need verification._
- **Are the 65 inferred relationships involving `AliasResolver` (e.g. with `.test_01_simple_alias()` and `.test_02_column_resolution()`) actually correct?**
  _`AliasResolver` has 65 INFERRED edges - model-reasoned connections that need verification._
- **Are the 61 inferred relationships involving `BusinessLogicNormalizer` (e.g. with `Look up column description, return readable name.` and `Short version -- just the description without the qualified name.`) actually correct?**
  _`BusinessLogicNormalizer` has 61 INFERRED edges - model-reasoned connections that need verification._
- **Are the 60 inferred relationships involving `BusinessDefinition` (e.g. with `Look up column description, return readable name.` and `Short version -- just the description without the qualified name.`) actually correct?**
  _`BusinessDefinition` has 60 INFERRED edges - model-reasoned connections that need verification._