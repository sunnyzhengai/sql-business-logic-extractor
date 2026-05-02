"""Parsing rule registry -- regex-based SQL preprocessing rules.

This package replaces ad-hoc `re.sub(...)` calls in `preprocess_ssms`
with a list of declarative `Rule` objects. Each rule is one T-SQL
construct sqlglot can't parse natively; the rule transforms the SQL
into something sqlglot can handle without changing semantics for our
extraction needs.

Adding a new rule:
    1. Create `fixtures/<rule_id>/input.sql` (failing example)
    2. Create `fixtures/<rule_id>/expected_clean.sql` (post-rule output)
    3. Append a Rule(...) entry to PARSING_RULES in rules.py
    4. The fixture-driven test in tests/test_parsing_rules.py picks it
       up automatically -- no ad-hoc test wiring required.

The registry is the SINGLE place new T-SQL idioms get added. No more
hand-rolled `re.sub` lines in resolve.py.
"""

from .rule import Rule, apply_all
from .rules import PARSING_RULES

__all__ = ["Rule", "PARSING_RULES", "apply_all"]
