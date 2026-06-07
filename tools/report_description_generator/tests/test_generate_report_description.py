"""Golden-path tests for Tool 4 -- Report Description Generator.

Same shape as Tool 3's test suite:
- engineered mode produces a non-empty summary deterministically
- license gate fires for LLM mode without `report_description_llm`
- LLM lazy-import does NOT trigger when use_llm=False
- composition: ReportDescription embeds BusinessLogic embeds
  TechnicalLineage embeds ColumnInventory (full chain)
"""

import os

import pytest

from sql_logic_extractor.products import (
    generate_report_description,
    ReportDescription,
    BusinessLogic,
    TechnicalLineage,
    ColumnInventory,
)
from sql_logic_extractor.license import LicenseError, reset_license_cache


def test_engineered_mode_returns_report_description_object():
    sql = "SELECT P.PAT_ID FROM Clarity.dbo.PATIENT P"
    desc = generate_report_description(sql, {})
    assert isinstance(desc, ReportDescription)
    assert isinstance(desc.business_logic, BusinessLogic)
    assert isinstance(desc.business_logic.lineage, TechnicalLineage)
    assert isinstance(desc.business_logic.lineage.inventory, ColumnInventory)
    assert desc.use_llm is False


def test_engineered_mode_produces_non_empty_summary():
    sql = """
    SELECT
        R.REFERRAL_ID,
        CASE WHEN R.STATUS_C = 5 THEN 'Denied' ELSE 'Other' END AS LABEL
    FROM Clarity.dbo.REFERRAL R
    WHERE R.STATUS_C IN (1, 2, 5)
    """
    desc = generate_report_description(sql, {})
    assert desc.technical_description
    assert "REFERRAL" in desc.technical_description
    # The CASE column should appear as a key metric
    assert "LABEL" in desc.key_metrics


def test_engineered_mode_reflects_filter_slice():
    """Filter narratives MUST influence the deterministic summary --
    the difference between describing the query's shape vs its intent."""
    sql = """
    SELECT P.PAT_ID FROM Clarity.dbo.PATIENT P WHERE P.STATUS_C = 1
    """
    desc = generate_report_description(sql, {})
    assert "Constrained by" in desc.technical_description or "filter" in desc.technical_description.lower(), \
        f"Engineered summary should mention the filter slice; got: {desc.technical_description}"


def test_default_use_llm_is_false():
    """Healthcare-safe default: no LLM unless explicitly opted in."""
    sql = "SELECT P.PAT_ID FROM Clarity.dbo.PATIENT P"
    desc = generate_report_description(sql, {})
    assert desc.use_llm is False
    assert "[LLM error" not in desc.technical_description


def test_engineered_mode_does_not_import_llm_libs():
    """Structural guarantee: engineered mode must not pull any vendor LLM SDK
    into sys.modules. Auditable for hospital procurement. Covers both
    google.genai (Gemini) and openai (Azure/OpenAI)."""
    import sys
    for mod in list(sys.modules):
        if mod.startswith(("google.genai", "openai")):
            del sys.modules[mod]
    sql = "SELECT P.PAT_ID FROM Clarity.dbo.PATIENT P"
    generate_report_description(sql, {})  # engineered, default
    assert "google.genai" not in sys.modules, \
        "google.genai must NOT be loaded for engineered-mode calls"
    assert "openai" not in sys.modules, \
        "openai must NOT be loaded for engineered-mode calls"


class _StubLLMClient:
    """Fake provider-neutral adapter: returns a canned summary dict for any
    prompt. Lets the LLM path run with no SDK and no network."""

    def complete_json(self, system_prompt: str, user_prompt: str, *,
                      temperature: float = 0.3) -> dict:
        return {
            "technical_description": "Stub technical description.",
            "business_description": "Stub business description.",
            "primary_purpose": "Stub purpose.",
            "key_metrics": ["STUB_METRIC"],
            "english_definition": "Stub column definition.",
        }


def test_llm_mode_uses_injected_client_without_sdk():
    """With a stub client injected, Tool 4's LLM mode fills its description
    fields from the stub -- end-to-end LLM path, no SDK / no network."""
    sql = "SELECT R.REFERRAL_ID FROM Clarity.dbo.REFERRAL R"
    desc = generate_report_description(sql, {}, use_llm=True,
                                       llm_client=_StubLLMClient())
    assert desc.use_llm is True
    assert desc.technical_description == "Stub technical description."
    assert desc.business_description == "Stub business description."
    assert "[LLM error" not in desc.technical_description


def test_llm_mode_blocked_without_feature():
    reset_license_cache()
    os.environ["SLE_FEATURES"] = "report_description"  # no _llm flag
    try:
        sql = "SELECT P.PAT_ID FROM Clarity.dbo.PATIENT P"
        with pytest.raises(LicenseError) as excinfo:
            generate_report_description(sql, {}, use_llm=True)
        assert "report_description_llm" in str(excinfo.value)
    finally:
        del os.environ["SLE_FEATURES"]
        reset_license_cache()


def test_window_function_classified_as_ranked():
    """ROW_NUMBER / windowed analysis should drive the primary_purpose."""
    sql = """
    SELECT
        R.REFERRAL_ID,
        ROW_NUMBER() OVER (PARTITION BY R.PATIENT_ID ORDER BY R.STATUS_C DESC) AS RowNum
    FROM Clarity.dbo.REFERRAL R
    """
    desc = generate_report_description(sql, {})
    assert "Ranked" in desc.primary_purpose or "window" in desc.primary_purpose.lower(), \
        f"Window function should drive Ranked/windowed purpose; got: {desc.primary_purpose}"


def test_aggregate_function_classified_as_aggregated():
    """SUM / COUNT / aggregate functions should drive the primary_purpose."""
    sql = """
    SELECT
        R.PATIENT_ID,
        COUNT(*) AS REFERRAL_COUNT
    FROM Clarity.dbo.REFERRAL R
    GROUP BY R.PATIENT_ID
    """
    desc = generate_report_description(sql, {})
    assert "Aggregated" in desc.primary_purpose or "aggregate" in desc.primary_purpose.lower(), \
        f"Aggregate function should drive Aggregated purpose; got: {desc.primary_purpose}"


def test_composition_full_chain():
    """Tool 4's output MUST contain Tools 1-3's outputs nested inside.
    This is the full layered-composition contract."""
    sql = """
    SELECT P.PAT_ID, P.PAT_NAME FROM Clarity.dbo.PATIENT P WHERE P.STATUS_C = 1
    """
    desc = generate_report_description(sql, {})
    # Tool 3's column translations
    assert desc.business_logic.column_translations
    # Tool 2's resolved columns
    assert desc.business_logic.lineage.resolved_columns
    # Tool 1's column inventory
    qualifs = {c.qualified() for c in desc.business_logic.lineage.inventory.columns}
    assert "Clarity.dbo.PATIENT.PAT_ID" in qualifs


def test_key_metrics_lists_computed_columns_only():
    """Passthrough columns are NOT key metrics. CASE / window / aggregate / calculated are."""
    sql = """
    SELECT
        R.REFERRAL_ID,
        R.STATUS_C,
        CASE WHEN R.STATUS_C = 5 THEN 'D' ELSE 'O' END AS LABEL
    FROM Clarity.dbo.REFERRAL R
    """
    desc = generate_report_description(sql, {})
    # LABEL is computed -> should be in key_metrics
    assert "LABEL" in desc.key_metrics
    # REFERRAL_ID is passthrough -> should NOT be in key_metrics
    assert "REFERRAL_ID" not in desc.key_metrics
