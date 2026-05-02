"""Tests for the combined parse pipeline.

Asserts the full stack does what it says: text rules pre-strip, sqlglot
parses, AST rules post-normalize. Every rule that fires shows up in the
audit lists so callers can answer "why was this view modified?".
"""

import sqlglot

from sql_logic_extractor.parsing_rules import parse_with_rules


def test_text_rule_fires_on_create_view_explicit_cols():
    """The CREATE VIEW (cols) AS form is unparseable by sqlglot
    natively; the text rule must rewrite it before parse."""
    sql = """CREATE VIEW dbo.foo (
        A, B
    )
    AS
    SELECT C.A, C.B FROM Clarity.dbo.SOURCE C"""
    result = parse_with_rules(sql)
    assert "create_view_explicit_column_list" in result.text_rules_fired
    assert isinstance(result.tree, sqlglot.expressions.Expression)


def test_ast_rule_fires_on_table_hints():
    """WITH (NOLOCK) parses fine but the AST rule should strip it."""
    sql = (
        "SELECT P.PAT_ID FROM Clarity.dbo.PATIENT P WITH (NOLOCK) "
        "WHERE P.STATUS_C = 1"
    )
    result = parse_with_rules(sql)
    assert "drop_table_hints" in result.ast_rules_fired
    assert "NOLOCK" not in result.tree.sql(dialect="tsql").upper()


def test_clean_sql_fires_no_rules():
    sql = "SELECT P.PAT_ID FROM Clarity.dbo.PATIENT P WHERE P.STATUS_C = 1"
    result = parse_with_rules(sql)
    assert result.text_rules_fired == []
    assert result.ast_rules_fired == []


def test_both_layers_fire_when_appropriate():
    """A view that needs BOTH text- and AST-level fixes runs both."""
    sql = """CREATE VIEW dbo.foo (A) AS
    SELECT P.PAT_ID AS A FROM Clarity.dbo.PATIENT P WITH (NOLOCK)"""
    result = parse_with_rules(sql)
    assert "create_view_explicit_column_list" in result.text_rules_fired
    assert "drop_table_hints" in result.ast_rules_fired
