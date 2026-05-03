"""Tests for sql_logic_extractor.corpus_schema (v3 tree shape).

Asserts the contract callers depend on:
- Round-trip serialization (Corpus -> dict -> Corpus is identity-preserving)
- JSONL streaming round-trip with versioned header
- Schema version mismatch raises with a clear error
- Tree-shaped storage: scopes own filters; filters are NOT duplicated
  on columns; cross-scope dataflow goes through scope-qualified base_columns
- Additive backwards-compat: extra/unknown fields are ignored
"""

import json

import pytest

from sql_logic_extractor.corpus_schema import (
    SCHEMA_VERSION,
    ColumnV1,
    CorpusV1,
    FilterV1,
    InventoryRefV1,
    ReportV1,
    ScopeV1,
    TermV1,
    ViewV1,
    corpus_from_dict,
    corpus_from_jsonl_lines,
    corpus_to_dict,
    corpus_to_jsonl_lines,
    validate_corpus_dict,
)


# ---------- helpers --------------------------------------------------------

def _make_sample_corpus() -> CorpusV1:
    """One view with two scopes (a CTE and main). Filters live ONLY on
    their owning scope. Main column references CTE column via
    scope-qualified base_columns. Mirrors what the v3 extractor produces."""
    cte_scope = ScopeV1(
        id="cte:ActivePatients",
        kind="cte",
        filters=(
            FilterV1(expression="P.STATUS_C = 1", english="Status is active",
                      kind="where"),
        ),
        columns=(
            ColumnV1(
                column_name="PAT_ID",
                column_type="passthrough",
                technical_description="P.PAT_ID",
                business_description="Patient identifier",
                business_domain="Patient Demographics",
                base_columns=("table:PATIENT.PAT_ID",),
                base_tables=("PATIENT",),
                fingerprint="aaaa1111",
            ),
        ),
        reads_from_scopes=(),
        reads_from_tables=("PATIENT",),
    )
    main_scope = ScopeV1(
        id="main",
        kind="main",
        filters=(
            FilterV1(expression="AP.PAT_ID > 100", english="Identifier exceeds 100",
                      kind="where"),
        ),
        columns=(
            ColumnV1(
                column_name="PAT_ID",
                column_type="passthrough",
                technical_description="AP.PAT_ID",
                business_description="Patient identifier",
                business_domain="Patient Demographics",
                base_columns=("cte:ActivePatients.PAT_ID",),
                base_tables=(),
                author_notes=("Top-of-file note",),
                term=TermV1(name_tokens=("patient", "identifier"),
                              is_passthrough=True, name_is_structural=False),
                fingerprint="bbbb2222",
            ),
        ),
        reads_from_scopes=("cte:ActivePatients",),
        reads_from_tables=(),
    )
    return CorpusV1(
        schema_version=SCHEMA_VERSION,
        views=(
            ViewV1(
                view_name="V_ACTIVE_PATIENTS",
                report=ReportV1(
                    technical_description="Scope: cte:ActivePatients (cte)\n  ...\n\nScope: main (main)\n  ...",
                    business_description="Scope: cte:ActivePatients (cte)\n  ...",
                    primary_purpose="Row-level extraction",
                    key_metrics=(),
                    column_count=1,
                ),
                view_level_notes=("Top-of-file doc comment",),
                scopes=(cte_scope, main_scope),
                view_outputs=("main",),
                inventory=(
                    InventoryRefV1(table="PATIENT", column="PAT_ID",
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
    d = json.loads(s)
    c2 = corpus_from_dict(d)
    assert c == c2


def test_jsonl_round_trip_is_identity():
    c = _make_sample_corpus()
    lines = list(corpus_to_jsonl_lines(c))
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
    bad = [json.dumps({"schema_version": 999, "n_views": 0})]
    with pytest.raises(ValueError, match="unsupported schema_version"):
        corpus_from_jsonl_lines(iter(bad))


def test_corpus_from_dict_ignores_unknown_fields():
    """Additive-evolution contract: future versions add fields; old
    readers ignore them and keep working."""
    d = corpus_to_dict(_make_sample_corpus())
    d["future_field_added_later"] = "ignored by this reader"
    d["views"][0]["another_future_field"] = {"any": "shape"}
    d["views"][0]["scopes"][0]["yet_another"] = ["x"]
    c2 = corpus_from_dict(d)
    assert c2.views[0].view_name == "V_ACTIVE_PATIENTS"


# ---------- tree shape: filters live on scopes, not columns --------------

def test_filters_live_on_scopes_not_columns():
    """The whole point of the v3 redesign: each scope owns its own
    filters. Columns do NOT carry filter context."""
    c = _make_sample_corpus()
    view = c.views[0]

    # Each scope has its own filter; no filter appears in both scopes.
    cte_filters = {f.expression for f in view.scopes[0].filters}
    main_filters = {f.expression for f in view.scopes[1].filters}
    assert cte_filters == {"P.STATUS_C = 1"}
    assert main_filters == {"AP.PAT_ID > 100"}
    assert cte_filters.isdisjoint(main_filters)

    # ColumnV1 has NO filter-related fields. Verify by attribute check.
    col = view.scopes[0].columns[0]
    assert not hasattr(col, "filters_inherited")
    assert not hasattr(col, "filters_extra")
    assert not hasattr(col, "filters")


def test_base_columns_are_scope_qualified():
    """Cross-scope dataflow goes through scope-qualified base_columns,
    not flat lists."""
    c = _make_sample_corpus()
    view = c.views[0]
    main_col = next(c for s in view.scopes if s.id == "main"
                     for c in s.columns)
    # main column reads from the CTE scope
    assert "cte:ActivePatients.PAT_ID" in main_col.base_columns

    cte_col = next(c for s in view.scopes if s.id.startswith("cte:")
                    for c in s.columns)
    # CTE column reads from a base table
    assert any(b.startswith("table:") for b in cte_col.base_columns)


def test_filter_strings_appear_only_in_their_owning_scope():
    """JSON serialization should not duplicate filter strings."""
    c = _make_sample_corpus()
    s = json.dumps(corpus_to_dict(c))
    # CTE-scope filter appears once (in cte:ActivePatients.filters)
    assert s.count("P.STATUS_C = 1") == 1
    # Main-scope filter appears once (in main.filters)
    assert s.count("AP.PAT_ID > 100") == 1


# ---------- empty / minimal cases ----------------------------------------

def test_empty_corpus_round_trips():
    c = CorpusV1()
    assert c.schema_version == SCHEMA_VERSION
    c2 = corpus_from_dict(corpus_to_dict(c))
    assert c == c2


def test_view_with_no_scopes_round_trips():
    """A view that failed to resolve still has a valid CorpusV1 entry."""
    c = CorpusV1(views=(ViewV1(view_name="V_FAILED"),))
    c2 = corpus_from_dict(corpus_to_dict(c))
    assert c == c2
