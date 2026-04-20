# LLM Translation

> 20 nodes · cohesion 0.15

## Key Concepts

- **llm_translate.py** (10 connections) — `llm_translate.py`
- **build_column_context()** (6 connections) — `llm_translate.py`
- **translate_query()** (6 connections) — `llm_translate.py`
- **translate_column()** (5 connections) — `llm_translate.py`
- **load_schema()** (4 connections) — `llm_translate.py`
- **summarize_query()** (4 connections) — `llm_translate.py`
- **format_output()** (3 connections) — `llm_translate.py`
- **get_column_description()** (3 connections) — `llm_translate.py`
- **get_enum_values()** (3 connections) — `llm_translate.py`
- **get_table_description()** (3 connections) — `llm_translate.py`
- **main()** (3 connections) — `llm_translate.py`
- **Translate a single resolved column to English using LLM.      Args:         reso** (1 connections) — `llm_translate.py`
- **Load and index the clarity_schema.yaml for fast lookup.      Returns:         {** (1 connections) — `llm_translate.py`
- **Generate a summary of the entire SQL query based on column definitions.      Arg** (1 connections) — `llm_translate.py`
- **Translate all columns in an L5 output file to English and generate query summary** (1 connections) — `llm_translate.py`
- **Format translation results for output.      Args:         results: Dict with 'su** (1 connections) — `llm_translate.py`
- **Get description for a table.** (1 connections) — `llm_translate.py`
- **Get description for a column.** (1 connections) — `llm_translate.py`
- **Get enum value mappings for a ZC_ table.** (1 connections) — `llm_translate.py`
- **Build context string for a single resolved column.      Args:         resolved_c** (1 connections) — `llm_translate.py`

## Relationships

- No strong cross-community connections detected

## Source Files

- `llm_translate.py`

## Audit Trail

- EXTRACTED: 56 (95%)
- INFERRED: 3 (5%)
- AMBIGUOUS: 0 (0%)

---

*Part of the graphify knowledge wiki. See [[index]] to navigate.*