"""Tool 10 -- corpus-level term extraction.

Walks a folder of *.sql views, runs the resolver, and emits one Term
per qualifying output column. The resulting terms.json is the input
to Phase 3 (governance bucket comparison).
"""
