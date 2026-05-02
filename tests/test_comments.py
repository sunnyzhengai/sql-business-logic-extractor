"""Tests for sql_logic_extractor.comments.

Asserts:
- All four comment shapes extract correctly (line, block, multi-line, inline)
- Comments inside string literals are NOT picked up
- The stripped SQL preserves byte-aligned positions (line/col round-trip)
- Each intent classifier branch fires on the right shape
- Re-parsing the stripped SQL succeeds where the original failed (the
  whole point of strip-and-extract for the parse-recovery path)
"""

import sqlglot

from sql_logic_extractor.comments import (
    Comment,
    classify_intent,
    classify_intent_raw,
    extract_comments,
)


# ============================================================
# Extraction
# ============================================================

def test_line_comment_extracted():
    sql = "SELECT 1  -- this is a label\nFROM dual"
    stripped, comments = extract_comments(sql)
    assert len(comments) == 1
    c = comments[0]
    assert c.kind == "line"
    assert c.text == "this is a label"
    assert c.line == 1
    # Line comments are replaced with same-length spaces; line/col grid intact.
    assert "this is a label" not in stripped
    assert len(stripped) == len(sql)


def test_block_comment_extracted_inline():
    sql = "WHERE STATUS_C = 5 /* Denied */ AND X = 1"
    stripped, comments = extract_comments(sql)
    assert len(comments) == 1
    assert comments[0].kind == "block"
    assert comments[0].text == "Denied"
    assert "Denied" not in stripped
    assert len(stripped) == len(sql)


def test_multiline_block_comment_preserves_newlines():
    sql = "SELECT 1\n/* this comment\n   spans\n   three lines */\nFROM dual"
    stripped, comments = extract_comments(sql)
    assert len(comments) == 1
    assert comments[0].kind == "block"
    assert "spans" in comments[0].text
    # Newlines INSIDE the block comment must stay so downstream line numbers
    # don't shift.
    assert stripped.count("\n") == sql.count("\n")


def test_comment_inside_string_literal_not_extracted():
    sql = "SELECT 'I -- am a string', '/* not a comment */' FROM dual"
    stripped, comments = extract_comments(sql)
    assert comments == []
    assert stripped == sql


def test_multiple_comments_keep_relative_positions():
    sql = (
        "/* doc at top */\n"
        "SELECT X.A  -- inline label\n"
        "     , Y.B  /* Managed Care */\n"
        "FROM T X"
    )
    _, comments = extract_comments(sql)
    assert len(comments) == 3
    assert [c.kind for c in comments] == ["block", "line", "block"]
    assert comments[0].line == 1 and comments[0].col == 1
    assert comments[1].line == 2
    assert comments[2].line == 3


# ============================================================
# Intent classification
# ============================================================

def test_intent_label_for_short_inline_block():
    assert classify_intent_raw("block", "Denied") == "label"
    assert classify_intent_raw("block", "Managed Care") == "label"


def test_intent_doc_for_multiline_block():
    assert classify_intent_raw("block", "First line\nSecond line\nThird line") == "doc"


def test_intent_section_header_for_separator_runs():
    assert classify_intent_raw("line", "============= Adjustment Overpayment =============") == "section_header"
    assert classify_intent_raw("line", "------ Section X ------") == "section_header"
    assert classify_intent_raw("block", "==== block-style header ====") == "section_header"


def test_intent_audit_for_metadata_keys():
    assert classify_intent_raw("line", "Author: Yang Zheng") == "audit"
    assert classify_intent_raw("block", "Modified Date: 2026-05-02") == "audit"
    assert classify_intent_raw("line", "Revision: 2.1") == "audit"


def test_intent_todo_for_keywords():
    assert classify_intent_raw("line", "TODO: handle null pat_id") == "todo"
    assert classify_intent_raw("block", "FIXME -- joins are slow here") == "todo"


def test_intent_unclassified_fallthrough():
    assert classify_intent_raw("line", "trailing note about something") == "unclassified"
    assert classify_intent_raw("block", "") == "unclassified"


def test_classify_intent_dataclass_wrapper():
    c = Comment(text="Denied", line=1, col=1, kind="block",
                intent="unclassified", raw="/* Denied */")
    assert classify_intent(c) == "label"


# ============================================================
# Round-trip: stripped SQL is parseable by sqlglot
# ============================================================

def test_stripped_sql_parses_cleanly():
    """Comments are sometimes the WHOLE reason a parse fails (e.g. inline
    comment that confuses a regex preprocessor). After stripping, the
    resulting SQL must still parse."""
    sql = (
        "/* author note */\n"
        "SELECT P.PAT_ID  -- primary key\n"
        "     , P.STATUS_C /* Denied */\n"
        "FROM Clarity.dbo.PATIENT P\n"
        "WHERE P.STATUS_C = 5  -- limit to denied\n"
    )
    stripped, _ = extract_comments(sql)
    sqlglot.parse_one(stripped, dialect="tsql")  # raises on failure


def test_stripped_sql_position_invariant():
    """For any character position in the source that is NOT inside a
    comment, the corresponding position in the stripped SQL is the same
    character. Critical for re-anchoring AST nodes back to source lines."""
    sql = "SELECT /* inline */ A FROM T"
    stripped, _ = extract_comments(sql)
    # Position of 'A' must be the same in source and stripped.
    a_in_src = sql.index("A FROM T")
    a_in_stripped = stripped.index("A FROM T")
    assert a_in_src == a_in_stripped
