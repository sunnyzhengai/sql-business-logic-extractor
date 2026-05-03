"""Tests for sql_logic_extractor.term_extraction.term:extract_terms."""

from sql_logic_extractor.term_extraction import extract_terms
from sql_logic_extractor.term_extraction.term import Term


# ============================================================
# Inclusion rule
# ============================================================

def test_pure_passthrough_id_column_dropped():
    """`SELECT P.PAT_ID FROM ...` with no filters -> column dropped:
    passthrough + no filters + structural-only name."""
    cols = [{
        "column_name": "PAT_ID",
        "column_type": "passthrough",
        "resolved_expression": "P.PAT_ID",
        "base_tables": ["PATIENT"],
        "base_columns": ["PATIENT.PAT_ID"],
        "filters": [],
    }]
    terms = extract_terms("V_TEST", cols)
    assert terms == []


def test_passthrough_with_descriptive_alias_kept():
    """Same column but aliased descriptively -> kept (alias has meaning)."""
    cols = [{
        "column_name": "Pregnant",
        "column_type": "passthrough",
        "resolved_expression": "P.IS_PREGNANT_YN",
        "base_tables": ["PATIENT"],
        "base_columns": ["PATIENT.IS_PREGNANT_YN"],
        "filters": [],
    }]
    terms = extract_terms("V_TEST", cols)
    assert len(terms) == 1
    assert terms[0].name_tokens == frozenset({"pregnant"})


def test_passthrough_with_filters_kept():
    """Even bare passthrough kept when query-level filters define population."""
    cols = [{
        "column_name": "PAT_ID",
        "column_type": "passthrough",
        "resolved_expression": "P.PAT_ID",
    }]
    terms = extract_terms("V_TEST", cols, query_filters=["P.STATUS_C = 1 /* Active */"])
    assert len(terms) == 1
    assert terms[0].has_filters
    assert "P.STATUS_C = 1 /* Active */" in terms[0].filters


def test_calculated_column_always_kept():
    """Non-passthrough types are always kept regardless of name."""
    cols = [{
        "column_name": "X",   # structural-only after tokenize
        "column_type": "calculated",
        "resolved_expression": "CASE WHEN P.STATUS_C = 5 THEN 'Y' ELSE 'N' END",
    }]
    terms = extract_terms("V_TEST", cols)
    assert len(terms) == 1
    assert terms[0].column_type == "calculated"


# ============================================================
# Field plumbing
# ============================================================

def test_view_filters_propagate_to_each_term():
    cols = [
        {"column_name": "Pregnant", "column_type": "passthrough"},
        {"column_name": "DueDate", "column_type": "calculated"},
    ]
    qf = ["P.STATUS_C = 1", "P.IS_VALID_PAT_YN = 'Y'"]
    terms = extract_terms("V_TEST", cols, query_filters=qf)
    assert len(terms) == 2
    for t in terms:
        # Each term picks up the view-level filters
        assert "P.STATUS_C = 1" in t.filters
        assert "P.IS_VALID_PAT_YN = 'Y'" in t.filters


def test_per_column_filters_merged_with_view_filters():
    cols = [{
        "column_name": "X",
        "column_type": "calculated",
        "resolved_expression": "expr",
        "filters": [{"expression": "X.LAB_RESULT = 'POS'"}],
    }]
    terms = extract_terms("V_TEST", cols, query_filters=["P.A = 1"])
    assert len(terms) == 1
    assert "P.A = 1" in terms[0].filters
    assert "X.LAB_RESULT = 'POS'" in terms[0].filters


def test_author_notes_carried_through():
    cols = [{
        "column_name": "Pregnant",
        "column_type": "passthrough",
        "author_notes": ["see ICD-10 Z33.1", "verified by clinical team"],
    }]
    terms = extract_terms("V_TEST", cols)
    assert terms[0].author_notes == (
        "see ICD-10 Z33.1", "verified by clinical team"
    )


def test_author_notes_csv_string_form_handled():
    """When read from CSV, author_notes arrives as a "; "-joined string;
    the extractor must split it back into a tuple."""
    cols = [{
        "column_name": "Pregnant",
        "column_type": "passthrough",
        "author_notes": "see ICD-10 Z33.1 | verified by clinical team",
    }]
    terms = extract_terms("V_TEST", cols)
    assert "see ICD-10 Z33.1" in terms[0].author_notes
    assert "verified by clinical team" in terms[0].author_notes


def test_to_dict_is_json_friendly():
    """Frozensets and tuples must become lists for JSON serialization."""
    cols = [{
        "column_name": "Pregnant",
        "column_type": "passthrough",
        "base_tables": ["PATIENT"],
    }]
    t = extract_terms("V_TEST", cols)[0]
    d = t.to_dict()
    assert isinstance(d["name_tokens"], list)
    assert isinstance(d["base_tables"], list)
    # Round-trips through json
    import json
    json.dumps(d)


# ============================================================
# Real-world patterns
# ============================================================

def test_pregnancy_columns_with_different_aliases_produce_same_tokens():
    """Cross-view pregnancy columns with different alias styles should
    have OVERLAPPING name_tokens -- the precondition for clustering."""
    a_cols = [{"column_name": "IS_PREGNANT_YN", "column_type": "calculated",
                "resolved_expression": "CASE WHEN ... END"}]
    b_cols = [{"column_name": "PregnancyFlag", "column_type": "calculated",
                "resolved_expression": "CASE WHEN ... END"}]
    c_cols = [{"column_name": "[Pregnant Indicator]", "column_type": "calculated",
                "resolved_expression": "..."}]

    a = extract_terms("V_A", a_cols)[0]
    b = extract_terms("V_B", b_cols)[0]
    c = extract_terms("V_C", c_cols)[0]

    # All three should share the canonical "pregnant" token.
    assert "pregnant" in a.name_tokens
    assert "pregnant" in b.name_tokens
    assert "pregnant" in c.name_tokens
    # And their bags are equal in this case.
    assert a.name_tokens == b.name_tokens == c.name_tokens
