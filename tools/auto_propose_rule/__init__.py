"""Tool 7 -- automatically propose new parsing rules for unknown_failure views.

When a view fails preflight (status=unknown_failure), this tool tries
two strategies in order:

  1. Hypothesis sweep: a bank of candidate transforms (collapse
     multi-line brackets, strip NOLOCK/OPTION/PRINT, etc.). If any
     unblocks the parse, propose a rule with that hypothesis as the
     pattern.

  2. Token isolation: extract a redacted window around the failing
     line/col + the rule registry's pre-pass output. The human
     reviewer writes the rule; this tool just gives them the precise
     construct.

Output: one markdown proposal per failing view in
sql_logic_extractor/parsing_rules/proposed/<view_stem>.md.

NEVER auto-merges into rules.py -- wrong rules silently corrupt SQL.
The human reviews the proposal, validates the hypothesis (re-runs
preflight), then promotes it via the standard "add a rule" flow:
fixture pair + Rule(...) entry.
"""
