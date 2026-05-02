"""Fixture-driven tests for the parsing-rule registry.

Contract: every Rule in PARSING_RULES MUST have a matching fixture
directory at sql_logic_extractor/parsing_rules/fixtures/<rule_id>/
containing input.sql + expected_clean.sql. The test below loads each
fixture, runs apply_all, and asserts byte-equal output. Adding a new
rule = adding one entry + one fixture pair -- the test discovers it
automatically, no test wiring required.

Also asserts:
- post-rule SQL parses cleanly via sqlglot (the whole point)
- the rule actually fires on its fixture (no dead rules)
- a hand-written "clean" input is unchanged by the registry (no-op
  property -- rules don't accidentally munge already-clean SQL)
"""

from pathlib import Path

import pytest
import sqlglot

from sql_logic_extractor.parsing_rules import PARSING_RULES, apply_all


_FIXTURES_DIR = (
    Path(__file__).resolve().parent.parent.parent
    / "sql_logic_extractor" / "parsing_rules" / "fixtures"
)


def _rule_ids() -> list[str]:
    return [r.id for r in PARSING_RULES]


@pytest.mark.parametrize("rule_id", _rule_ids())
def test_each_rule_has_fixture_dir(rule_id):
    """Every rule MUST ship with input.sql + expected_clean.sql."""
    fdir = _FIXTURES_DIR / rule_id
    assert fdir.is_dir(), f"missing fixture dir: {fdir}"
    assert (fdir / "input.sql").is_file(), f"missing {fdir}/input.sql"
    assert (fdir / "expected_clean.sql").is_file(), \
        f"missing {fdir}/expected_clean.sql"


@pytest.mark.parametrize("rule_id", _rule_ids())
def test_rule_fixture_round_trip(rule_id):
    """The registry applied to input.sql produces expected_clean.sql."""
    fdir = _FIXTURES_DIR / rule_id
    input_sql = (fdir / "input.sql").read_text()
    expected = (fdir / "expected_clean.sql").read_text()
    actual, fired = apply_all(input_sql)
    assert rule_id in fired, (
        f"rule {rule_id} did NOT fire on its own fixture -- either the "
        f"fixture is wrong or the rule is dead"
    )
    assert actual == expected, (
        f"rule {rule_id} output diverged from expected_clean.sql.\n"
        f"--- expected ---\n{expected}\n--- actual ---\n{actual}"
    )


@pytest.mark.parametrize("rule_id", _rule_ids())
def test_rule_output_parses_with_sqlglot(rule_id):
    """The point of the registry: post-rule SQL is parseable by sqlglot.
    If this fails, either the rule is wrong or sqlglot has a deeper
    issue with the underlying construct."""
    fdir = _FIXTURES_DIR / rule_id
    input_sql = (fdir / "input.sql").read_text()
    actual, _ = apply_all(input_sql)
    try:
        sqlglot.parse_one(actual, dialect="tsql")
    except Exception as e:
        pytest.fail(f"post-rule SQL still doesn't parse: {e}")


def test_clean_sql_is_noop():
    """A SQL fragment that doesn't trigger any rule is returned unchanged.
    Guards against accidental rule overreach (e.g. a regex too greedy
    that mangles innocent SQL)."""
    clean_sql = (
        "SELECT P.PAT_ID, P.PAT_NAME "
        "FROM Clarity.dbo.PATIENT P "
        "WHERE P.STATUS_C = 1"
    )
    out, fired = apply_all(clean_sql)
    assert fired == [], f"clean SQL should fire no rules; got: {fired}"
    assert out == clean_sql


def test_apply_all_is_idempotent():
    """Applying the registry twice produces the same result as once.
    A rule whose output still matches its own pattern is broken."""
    fdir = _FIXTURES_DIR / "create_view_explicit_column_list"
    sql = (fdir / "input.sql").read_text()
    once, _ = apply_all(sql)
    twice, _ = apply_all(once)
    assert once == twice, "rules are not idempotent -- one fires on its own output"
