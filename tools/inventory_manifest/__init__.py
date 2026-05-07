"""Tool 15 -- inventory manifest.

Reads a v3 corpus.jsonl and emits a deduped list of every table and
(table, column) pair referenced across the corpus, plus paste-ready
SQL fragments. The output drives:

  - extract_clarity_metadata.sql   (filtered by used tables / columns)
  - extract_zc_values.sql          (filtered by used ZC tables only)

Cuts both queries from "scan everything in CLARITY" to "scan only what
your 130 views touch" -- typically a 10-100x reduction in the result
set and runtime.
"""
