# Proposed parsing rule: NEEDS HUMAN INVESTIGATION

**Source view:** `fully_broken.sql`
**Status:** no canned hypothesis unblocked the parse -- this is a NEW
T-SQL construct sqlglot can't handle. The construct must be identified
manually below, then a Rule + fixture pair authored.

## sqlglot error

```
ParseError: Invalid expression / Unexpected token. Line N, Col: N.
  SELECT NOT VALID [4m)[0m))) STATEMENT '***'
```

Failing position: line 1, col 18

## Redacted context window (5 lines around the failing line)

```
>>    1: SELECT NOT VALID )))) STATEMENT '***'
                         ^
```

## Next steps

1. Identify the offending T-SQL construct from the context window.
2. Decide if it's:
   - **A drop**: the construct is irrelevant to column extraction
     (e.g. PRINT, SET, table hints) -- write a strip rule.
   - **A rewrite**: the construct has a sqlglot-parseable equivalent
     -- write a substitution rule.
   - **An sqlglot bug**: file an upstream issue. May need a sqlglot
     pin or workaround in the meantime.
3. Author a fixture pair under
   `sql_logic_extractor/parsing_rules/fixtures/<new_rule_id>/`
4. Append a `Rule(...)` to `parsing_rules/rules.py`.
5. Re-run preflight; this view should move to `needs_rule`.
