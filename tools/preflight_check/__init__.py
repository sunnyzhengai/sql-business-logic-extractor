"""Preflight check -- triage parse health BEFORE running Tools 1-5.

Walks a folder of *.sql views, applies the parsing-rule registry, and
attempts a sqlglot parse. Classifies each view into clean / needs_rule
/ unknown_failure and emits a triage CSV. Use this to know what
fraction of a new corpus is processable without burning resolver time
on views you already know will fail.
"""
