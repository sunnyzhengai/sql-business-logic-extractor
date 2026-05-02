"""Fixture-driven tests for the AST-rule registry.

Mirrors the contract enforced on text-level rules in test_rules.py:
every AstRule MUST ship with input.sql + expected_clean.sql under
fixtures/<rule_id>/. The fixture-driven test:

  1. Parses input.sql.
  2. Runs the rule's transform.
  3. Parses expected_clean.sql.
  4. Compares the EMITTED SQL of both trees (round-trip via sqlglot's
     emitter abstracts away formatting differences in the fixture
     file -- so handwritten expected_clean.sql can have any
     reasonable indentation).
  5. Asserts the rule actually fired (no dead AST rules in the registry).
"""

from pathlib import Path

import pytest
import sqlglot

from sql_logic_extractor.parsing_rules import AST_RULES, apply_all_ast


_FIXTURES_DIR = (
    Path(__file__).resolve().parent.parent.parent
    / "sql_logic_extractor" / "parsing_rules" / "fixtures"
)


def _ast_rule_ids() -> list[str]:
    return [r.id for r in AST_RULES]


@pytest.mark.parametrize("rule_id", _ast_rule_ids())
def test_each_ast_rule_has_fixture_dir(rule_id):
    fdir = _FIXTURES_DIR / rule_id
    assert fdir.is_dir(), f"missing fixture dir: {fdir}"
    assert (fdir / "input.sql").is_file()
    assert (fdir / "expected_clean.sql").is_file()


@pytest.mark.parametrize("rule_id", _ast_rule_ids())
def test_ast_rule_round_trip(rule_id):
    """Apply rule to input.sql; emitted SQL must match expected_clean.sql
    after both go through sqlglot's emitter (which abstracts formatting)."""
    fdir = _FIXTURES_DIR / rule_id
    input_sql = (fdir / "input.sql").read_text()
    expected_sql = (fdir / "expected_clean.sql").read_text()

    rule = next(r for r in AST_RULES if r.id == rule_id)

    in_tree = sqlglot.parse_one(input_sql, dialect="tsql")
    transformed = rule.apply(in_tree)
    expected_tree = sqlglot.parse_one(expected_sql, dialect="tsql")

    actual_emit = transformed.sql(dialect="tsql")
    expected_emit = expected_tree.sql(dialect="tsql")
    assert actual_emit == expected_emit, (
        f"AST rule {rule_id} produced unexpected output.\n"
        f"--- expected ---\n{expected_emit}\n"
        f"--- actual ---\n{actual_emit}"
    )


@pytest.mark.parametrize("rule_id", _ast_rule_ids())
def test_ast_rule_actually_fires(rule_id):
    """The rule must produce a CHANGE on its own input fixture. A rule
    that's a no-op on its fixture is dead -- either the fixture doesn't
    exercise the rule or the rule's transform is broken."""
    fdir = _FIXTURES_DIR / rule_id
    input_sql = (fdir / "input.sql").read_text()
    rule = next(r for r in AST_RULES if r.id == rule_id)
    in_tree = sqlglot.parse_one(input_sql, dialect="tsql")
    before = in_tree.sql()
    after = rule.apply(in_tree).sql()
    assert before != after, f"rule {rule_id} did not change its own fixture"


def test_apply_all_ast_is_noop_on_clean_sql():
    """Clean SQL with no hints / options should fire NO ast rules."""
    clean = sqlglot.parse_one(
        "SELECT P.PAT_ID FROM Clarity.dbo.PATIENT P WHERE P.STATUS_C = 1",
        dialect="tsql",
    )
    _, fired = apply_all_ast(clean)
    assert fired == [], f"clean SQL should fire no rules; got {fired}"


def test_apply_all_ast_idempotent():
    """Running the registry twice gives the same result as once. Catches
    AST rules that re-introduce the very pattern they're supposed to
    strip."""
    in_tree = sqlglot.parse_one(
        "SELECT P.PAT_ID FROM Clarity.dbo.PATIENT P WITH (NOLOCK) "
        "WHERE P.STATUS_C = 1 OPTION (MAXDOP 1)",
        dialect="tsql",
    )
    once, _ = apply_all_ast(in_tree)
    twice, _ = apply_all_ast(once)
    assert once.sql() == twice.sql()
