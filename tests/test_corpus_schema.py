"""Tests for sql_logic_extractor.corpus_schema.

Asserts the contract callers depend on:
- Round-trip serialization (Corpus -> dict -> Corpus is identity-preserving)
- JSONL streaming round-trip
- Schema version mismatch raises with a clear error
- expand_view() actually denormalizes (filters and tables flattened)
- Compact form really IS compact (no duplicated filters in storage)
- Additive backwards-compat: extra/unknown fields are ignored
"""

import json

import pytest

from sql_logic_extractor.corpus_schema import (
    SCHEMA_VERSION,
    ColumnV1,
    CorpusV1,
    InventoryRefV1,
    ReportV1,
    TermV1,
    ViewLevelV1,
    ViewV1,
    corpus_from_dict,
    corpus_from_jsonl_lines,
    corpus_to_dict,
    corpus_to_jsonl_lines,
    expand_view,
    validate_corpus_dict,
)


# ---------- helpers --------------------------------------------------------

def _make_sample_corpus() -> CorpusV1:
    """One view, two columns, the kind of structure a real extractor
    would produce. Used across multiple tests for shared fixtures."""
    return CorpusV1(
        schema_version=SCHEMA_VERSION,
        views=(
            ViewV1(
                view_name="V_PREGNANT_PATIENTS",
                view_level=ViewLevelV1(
                    filters=("P.STATUS_C = 1", "P.IS_VALID_PAT_YN = 'Y'"),
                    tables_referenced=("PATIENT", "COVERAGE", "DIAGNOSIS"),
                    view_level_notes=("Top-of-file doc comment",),
                    report=ReportV1(
                        technical_description="Tech desc...",
                        business_description="Business desc...",
                        primary_purpose="Pregnancy report",
                        key_metrics=("Pregnant", "DueDate"),
                        column_count=2,
                    ),
                ),
                columns=(
                    ColumnV1(
                        column_name="Pregnant",
                        column_type="calculated",
                        resolved_expression="CASE WHEN ... END",
                        base_tables_idx=(0, 2),    # PATIENT, DIAGNOSIS
                        base_columns=("PATIENT.PAT_ID", "DIAGNOSIS.DX_CODE"),
                        filters_inherited=True,
                        filters_extra=(),
                        english_definition="Patient is pregnant",
                        author_notes=("see ICD-10 Z33.1",),
                        term=TermV1(
                            name_tokens=("pregnant",),
                            is_passthrough=False,
                            name_is_structural=False,
                            has_filters=True,
                        ),
                        fingerprint="a3b9c2d4e5f6",
                    ),
                    ColumnV1(
                        column_name="DueDate",
                        column_type="calculated",
                        resolved_expression="DATEADD(...)",
                        base_tables_idx=(0,),       # PATIENT only
                        base_columns=("PATIENT.LMP_DATE",),
                        filters_inherited=True,
                        filters_extra=("P.LMP_DATE IS NOT NULL",),
                        english_definition="Estimated due date",
                        term=TermV1(
                            name_tokens=("date",),
                            has_filters=True,
                        ),
                        fingerprint="b4d8e3a2f1c7",
                    ),
                ),
                inventory=(
                    InventoryRefV1(table="PATIENT", column="PAT_ID",
                                     database="Clarity", schema="dbo"),
                    InventoryRefV1(table="PATIENT", column="LMP_DATE",
                                     database="Clarity", schema="dbo"),
                    InventoryRefV1(table="DIAGNOSIS", column="DX_CODE",
                                     database="Clarity", schema="dbo"),
                ),
            ),
        ),
    )


# ---------- round-trip ----------------------------------------------------

def test_corpus_dict_round_trip_is_identity():
    c = _make_sample_corpus()
    d = corpus_to_dict(c)
    c2 = corpus_from_dict(d)
    assert c == c2


def test_corpus_dict_serializes_to_json_cleanly():
    """to_dict output must be json.dumps-friendly (no tuples / sets)."""
    c = _make_sample_corpus()
    s = json.dumps(corpus_to_dict(c))
    # Round-trip through json
    d = json.loads(s)
    c2 = corpus_from_dict(d)
    assert c == c2


def test_jsonl_round_trip_is_identity():
    c = _make_sample_corpus()
    lines = list(corpus_to_jsonl_lines(c))
    # Header + one line per view
    assert len(lines) == 1 + len(c.views)
    c2 = corpus_from_jsonl_lines(iter(lines))
    assert c == c2


def test_jsonl_first_line_is_header_with_version():
    c = _make_sample_corpus()
    lines = list(corpus_to_jsonl_lines(c))
    header = json.loads(lines[0])
    assert header["schema_version"] == SCHEMA_VERSION
    assert header["n_views"] == len(c.views)


# ---------- versioning ----------------------------------------------------

def test_validate_rejects_missing_version():
    with pytest.raises(ValueError, match="schema_version"):
        validate_corpus_dict({"views": []})


def test_validate_rejects_wrong_version():
    with pytest.raises(ValueError, match="unsupported schema_version"):
        validate_corpus_dict({"schema_version": 999, "views": []})


def test_jsonl_rejects_wrong_version_header():
    bad = [
        json.dumps({"schema_version": 999, "n_views": 0}),
    ]
    with pytest.raises(ValueError, match="unsupported schema_version"):
        corpus_from_jsonl_lines(iter(bad))


def test_corpus_from_dict_ignores_unknown_fields():
    """Additive-evolution contract: future versions can add fields;
    old readers ignore them and keep working."""
    d = corpus_to_dict(_make_sample_corpus())
    d["future_field_added_in_v1_1"] = "ignored by this reader"
    d["views"][0]["another_future_field"] = {"any": "shape"}
    # Should not raise; should round-trip the known fields cleanly.
    c2 = corpus_from_dict(d)
    assert c2.views[0].view_name == "V_PREGNANT_PATIENTS"


# ---------- compact form really is compact -------------------------------

def test_compact_form_does_not_duplicate_view_filters():
    """The whole point of normalization: filters are stored ONCE per view
    even though they apply to every column."""
    c = _make_sample_corpus()
    s = json.dumps(corpus_to_dict(c))
    # P.STATUS_C = 1 should appear EXACTLY ONCE in the serialized JSON
    # (in view_level.filters), not duplicated per column.
    assert s.count("P.STATUS_C = 1") == 1
    assert s.count("P.IS_VALID_PAT_YN = 'Y'") == 1


def test_compact_form_does_not_duplicate_table_names():
    """tables_referenced is a single de-duped list; columns reference
    by index, not by repeating the strings."""
    c = _make_sample_corpus()
    s = json.dumps(corpus_to_dict(c))
    # PATIENT appears once in tables_referenced + once per inventory ref
    # but NOT in every column's base_tables (because base_tables_idx
    # is the compact form). 1 (tables_referenced) + 2 (inventory) = 3.
    assert s.count('"PATIENT"') == 3


# ---------- expand_view denormalization -----------------------------------

def test_expand_view_resolves_table_indices():
    c = _make_sample_corpus()
    expanded = expand_view(c.views[0])
    # First column's base_tables_idx was (0, 2) -> ["PATIENT", "DIAGNOSIS"]
    assert expanded["columns"][0]["base_tables"] == ["PATIENT", "DIAGNOSIS"]
    # Second column's base_tables_idx was (0,) -> ["PATIENT"]
    assert expanded["columns"][1]["base_tables"] == ["PATIENT"]


def test_expand_view_inflates_inherited_filters():
    """When a column has filters_inherited=True, expanded form lists
    ALL view-level filters PLUS any filters_extra."""
    c = _make_sample_corpus()
    expanded = expand_view(c.views[0])

    # First column inherits all view filters; no extras.
    f1 = expanded["columns"][0]["filters"]
    assert "P.STATUS_C = 1" in f1
    assert "P.IS_VALID_PAT_YN = 'Y'" in f1
    assert len(f1) == 2

    # Second column inherits view filters PLUS its own extra.
    f2 = expanded["columns"][1]["filters"]
    assert "P.STATUS_C = 1" in f2
    assert "P.IS_VALID_PAT_YN = 'Y'" in f2
    assert "P.LMP_DATE IS NOT NULL" in f2
    assert len(f2) == 3


def test_expand_view_drops_compact_only_fields():
    """The denormalized form should not expose internal index fields."""
    c = _make_sample_corpus()
    expanded = expand_view(c.views[0])
    for col in expanded["columns"]:
        assert "base_tables_idx" not in col
        assert "filters_inherited" not in col
        assert "filters_extra" not in col


def test_expand_view_filters_inherited_false_excludes_view_filters():
    """When a column opts out of inheritance, it shows ONLY its
    column-specific filters."""
    view = ViewV1(
        view_name="V",
        view_level=ViewLevelV1(
            filters=("VIEW_FILTER",),
            tables_referenced=("T",),
        ),
        columns=(
            ColumnV1(
                column_name="X",
                base_tables_idx=(0,),
                filters_inherited=False,
                filters_extra=("ONLY_THIS",),
            ),
        ),
    )
    expanded = expand_view(view)
    assert expanded["columns"][0]["filters"] == ["ONLY_THIS"]


# ---------- empty / minimal cases ----------------------------------------

def test_empty_corpus_round_trips():
    c = CorpusV1()
    assert c.schema_version == SCHEMA_VERSION
    c2 = corpus_from_dict(corpus_to_dict(c))
    assert c == c2


def test_view_with_no_columns_or_inventory_round_trips():
    """A view that failed to resolve still has a valid CorpusV1 entry."""
    c = CorpusV1(views=(ViewV1(view_name="V_FAILED"),))
    c2 = corpus_from_dict(corpus_to_dict(c))
    assert c == c2
