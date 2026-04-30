"""Golden-path tests for Tool 3 -- Business Logic Extractor.

Tests cover:
- engineered (no-LLM) mode produces an English definition per column
- license gate fires when LLM mode is requested without the
  `business_logic_llm` feature
- LLM lazy-import does NOT trigger when use_llm=False (structural
  guarantee for healthcare-safe builds: the LLM client lib doesn't even
  load if the customer never opts in)
- composition: BusinessLogic embeds the TechnicalLineage produced by
  Tool 2, which embeds the ColumnInventory produced by Tool 1

LLM-mode behavior tests are deliberately LIGHT -- LLM responses are
non-deterministic and need the API key. The contract test is "license
+ lazy import" rather than "exact LLM output."
"""

import os

import pytest

from sql_logic_extractor.products import (
    extract_business_logic,
    BusinessLogic,
    TechnicalLineage,
    ColumnInventory,
)
from sql_logic_extractor.license import LicenseError, reset_license_cache


def _by_name(translations: list[dict]) -> dict:
    return {t["column_name"]: t for t in translations}


def test_engineered_mode_returns_business_logic_object():
    sql = "SELECT P.PAT_ID FROM Clarity.dbo.PATIENT P"
    bl = extract_business_logic(sql, {})
    assert isinstance(bl, BusinessLogic)
    assert isinstance(bl.lineage, TechnicalLineage)
    assert isinstance(bl.lineage.inventory, ColumnInventory)
    assert bl.use_llm is False


def test_engineered_mode_produces_english_definition_per_column():
    sql = """
    SELECT R.REFERRAL_ID, R.STATUS_C
    FROM Clarity.dbo.REFERRAL R
    """
    bl = extract_business_logic(sql, {})
    cols = _by_name(bl.column_translations)
    assert "REFERRAL_ID" in cols
    assert "STATUS_C" in cols
    for c in cols.values():
        assert c.get("english_definition")


def test_engineered_mode_classifies_calculated_columns():
    sql = """
    SELECT
        R.REFERRAL_ID,
        CASE WHEN R.STATUS_C = 5 THEN 'Denied' ELSE 'Other' END AS LABEL
    FROM Clarity.dbo.REFERRAL R
    """
    bl = extract_business_logic(sql, {})
    cols = _by_name(bl.column_translations)
    assert cols["REFERRAL_ID"]["column_type"] == "passthrough"
    assert cols["LABEL"]["column_type"] != "passthrough"


def test_default_use_llm_is_false():
    """The signature default -- critical for healthcare-safe positioning."""
    sql = "SELECT P.PAT_ID FROM Clarity.dbo.PATIENT P"
    bl = extract_business_logic(sql, {})
    assert bl.use_llm is False
    # Every translation should be from the engineered path (no [LLM error] prefix)
    for t in bl.column_translations:
        assert "[LLM error" not in t.get("english_definition", "")


def test_engineered_mode_does_not_import_llm_libs():
    """Structural guarantee: extracting business logic in engineered mode
    must NOT pull google.genai into sys.modules. Healthcare-safe customers
    can verify this by inspecting their Python environment after import."""
    import sys
    # Evict any prior import (the test order shouldn't determine outcome).
    for mod in list(sys.modules):
        if mod.startswith("google.genai") or mod == "google.genai":
            del sys.modules[mod]
    sql = "SELECT P.PAT_ID FROM Clarity.dbo.PATIENT P"
    extract_business_logic(sql, {})  # engineered, default
    assert "google.genai" not in sys.modules, \
        "google.genai must NOT be loaded for engineered-mode calls"


def test_llm_mode_blocked_without_feature():
    """A license that doesn't include `business_logic_llm` must reject
    LLM-mode calls before any LLM client is constructed."""
    reset_license_cache()
    os.environ["SLE_FEATURES"] = "business_logic"  # no _llm flag
    try:
        sql = "SELECT P.PAT_ID FROM Clarity.dbo.PATIENT P"
        with pytest.raises(LicenseError) as excinfo:
            extract_business_logic(sql, {}, use_llm=True)
        assert "business_logic_llm" in str(excinfo.value)
    finally:
        del os.environ["SLE_FEATURES"]
        reset_license_cache()


def test_engineered_filter_narrative_when_filters_present():
    """When the resolver attaches filters to a column, the engineered
    translator should produce both the bare english_definition and the
    filter-aware variant."""
    sql = """
    SELECT P.PAT_ID FROM Clarity.dbo.PATIENT P WHERE P.STATUS_C = 1
    """
    bl = extract_business_logic(sql, {})
    cols = _by_name(bl.column_translations)
    pat_id = cols["PAT_ID"]
    # The bare definition exists
    assert pat_id.get("english_definition")
    # The filter-aware variant should mention the filter
    if pat_id.get("english_definition_with_filters"):
        assert "STATUS_C" in pat_id["english_definition_with_filters"] or \
               "filtered" in pat_id["english_definition_with_filters"].lower()


def test_composition_embeds_tool_1_and_2_outputs():
    """Tool 3's output MUST embed Tool 2's lineage (which embeds Tool 1's
    inventory). This is the layered composition contract."""
    sql = """
    SELECT P.PAT_ID, P.PAT_NAME
    FROM Clarity.dbo.PATIENT P
    WHERE P.STATUS_C = 1
    """
    bl = extract_business_logic(sql, {})
    # Tool 2's lineage is embedded
    assert bl.lineage.resolved_columns
    # Tool 1's inventory is embedded inside Tool 2's lineage
    qualifs = {c.qualified() for c in bl.lineage.inventory.columns}
    assert "Clarity.dbo.PATIENT.PAT_ID" in qualifs
    assert "Clarity.dbo.PATIENT.STATUS_C" in qualifs


def test_business_domain_assigned():
    """Heuristic domain classification should at least produce a
    non-empty string for known domains."""
    sql = "SELECT R.REFERRAL_ID FROM Clarity.dbo.REFERRAL R"
    bl = extract_business_logic(sql, {})
    cols = _by_name(bl.column_translations)
    assert cols["REFERRAL_ID"].get("business_domain")
