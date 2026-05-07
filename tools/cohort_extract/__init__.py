"""Tool 14 -- cohort extractor.

Reads a v3 corpus.jsonl and renders each scope as a "cohort" -- a
population-level description of what rows that scope carves out. Unlike
dataset_extract (which documents every column), cohort_extract emits
just two things per scope:

  - cohort   "what population is this": composed from the scope's tables'
             short_descriptions ("patients with encounters", "orders",
             "encounters" ...).
  - filters  natural-language list of what's being kept/excluded, using
             each filter's English translation already in the corpus.

Used for governance reviews where the question is "which view defines
which population?" -- not "which column does what".
"""
