"""Tool 13 -- dataset extractor.

Reads a v3 corpus.jsonl and renders each view as a chain of
"datasets" -- one per scope (CTE / derived / subquery / main). Each
dataset has:

  - name           the scope's CTE alias humanized (or "Main query" for the outer SELECT)
  - kind           "main" | "cte" | "derived" | "subquery" | "lateral" | etc.
  - base_datasets  upstream scope IDs (the dataflow edge)
  - reads_tables   base tables this scope reads directly
  - data_columns   each column's name + business_description
  - filters        each filter's English translation, kept-with-its-kind

Two output formats:
  - datasets.json  programmatic / structured
  - datasets.md    human-readable, one section per view

The data needed to produce both is already in `corpus.jsonl`; this
tool is pure presentation.
"""
