"""Tool 9 -- per-view resolver timing audit with hard timeouts.

Identifies pathological views (resolver hangs / slow paths) in one pass
across a corpus. For each view, runs the parse + resolve step with a
hard wall-clock deadline. Anything over the deadline is recorded as
'timeout' and the audit continues to the next view -- no manual bisect,
no kernel hangs.

Use BEFORE run_all on a new corpus to identify which views to set aside
or investigate.
"""
