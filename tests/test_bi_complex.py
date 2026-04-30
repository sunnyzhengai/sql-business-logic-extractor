"""Test cases anchored on queries/bi_complex/input.sql.

Locks down the end-to-end pipeline for a realistic healthcare query with:
- Three-deep CTE chain (ReferralDenials -> AugDenialSla -> AugHistorySla)
- EXISTS with UNION ALL (Layer 1-3 decomposition)
- Join-key-only nested-CTE dependency (AugHistorySla -> AugDenialSla)
- Calculated IIF expressions, window functions, passthroughs

The xfail test at the bottom documents the known lineage gap: today the
extractor only captures value-flow base columns; join keys and filter
columns across CTE hops are not yet surfaced in base_tables.

Run: python3 -m pytest tests/test_bi_complex.py -v
"""

from pathlib import Path

import pytest

from sql_logic_extractor.extract import SQLBusinessLogicExtractor, to_dict
from sql_logic_extractor.resolve import resolve_query


SQL_PATH = Path(__file__).resolve().parent.parent / "data" / "queries" / "bi_complex" / "input.sql"


@pytest.fixture(scope="module")
def sql() -> str:
    return SQL_PATH.read_text()


@pytest.fixture(scope="module")
def logic(sql) -> dict:
    return to_dict(SQLBusinessLogicExtractor(dialect="tsql").extract(sql))


@pytest.fixture(scope="module")
def resolved(sql):
    r = resolve_query(sql, dialect="tsql")
    return {c.name: c for c in r.columns}


# ============================================================
# Layer 1 extraction — structural completeness
# ============================================================

class TestExtraction:

    def test_all_four_ctes_captured(self, logic):
        names = [c["name"] for c in logic.get("ctes", [])]
        assert names == [
            "ReferralDenials", "AugDenialSla",
            "AugHistorySla", "ReferralHistoryItemValues",
        ]

    def test_no_unknown_containers(self, logic):
        """Layer 3 gap detector should be clean for this query."""
        assert logic.get("unknown_containers", []) == []

    def test_exists_union_decomposed(self, logic):
        """ReferralDenials' EXISTS(UNION ALL) produces two 'exists' subqueries."""
        rd = next(c for c in logic["ctes"] if c["name"] == "ReferralDenials")
        contexts = [s.get("context") for s in rd["logic"].get("subqueries", [])]
        assert contexts.count("exists") == 2


# ============================================================
# L3 resolve — column-level dataflow lineage
# ============================================================

class TestBasicLineage:

    def test_passthrough_through_cte(self, resolved):
        """REFERRAL_ID flows main -> ReferralDenials -> V_CCHP_AuthHeader_Fact."""
        col = resolved["REFERRAL_ID"]
        assert "V_CCHP_AuthHeader_Fact" in col.base_tables
        assert any("Referralid" in bc for bc in col.base_columns)

    def test_augdenialsla_passthrough(self, resolved):
        """aug.AUTH_REQUEST_ID traces to V_CCHP_UMAuthorizationRequest_Fact."""
        col = resolved["AUTH_REQUEST_ID"]
        assert "V_CCHP_UMAuthorizationRequest_Fact" in col.base_tables

    def test_calculated_iif_resolves(self, resolved):
        """RFI_DATE's IIF inlines authx columns from history fact."""
        col = resolved["RFI_DATE"]
        assert col.type == "calculated"
        assert "V_CCHP_UMAuthorizationHistory_Fact" in col.base_tables
        assert col.resolved_expression.startswith("CASE WHEN")

    def test_window_function_partition_order_survives(self, resolved):
        """DENIAL_ROW keeps its IIF sentinel ORDER BY when inlined."""
        col = resolved["DENIAL_ROW"]
        assert col.type == "window"
        assert "99991231" in col.resolved_expression


# ============================================================
# Transformation chain — CTE hops visible
# ============================================================

class TestTransformationChain:

    def test_rfi_date_goes_through_aughistorysla(self, resolved):
        scopes = [step["scope"] for step in resolved["RFI_DATE"].transformation_chain]
        assert "AugHistorySla" in scopes

    def test_auth_request_id_goes_through_augdenialsla(self, resolved):
        scopes = [step["scope"] for step in resolved["AUTH_REQUEST_ID"].transformation_chain]
        assert "AugDenialSla" in scopes


# ============================================================
# Filter capture and EXISTS-subquery nesting (Option 1)
# ============================================================

class TestFilterLineage:

    def test_aughistorysla_where_filter_propagates(self, resolved):
        """RFI_DATE carries the outer CTE's WHERE predicate."""
        exprs = [f.expression for f in resolved["RFI_DATE"].filters]
        assert any("UM_STATUS" in e and "'Denied'" in e for e in exprs)

    def test_exists_union_branches_resolved_as_subqueries(self, resolved):
        """The EXISTS filter on REFERRAL_ID has two resolved subqueries —
        one per UNION ALL branch — each pointing at its own base table."""
        col = resolved["REFERRAL_ID"]
        exists_filter = next(
            (f for f in col.filters if "EXISTS" in f.expression and "UNION ALL" in f.expression),
            None,
        )
        assert exists_filter is not None
        assert len(exists_filter.subqueries) == 2
        nested_tables = {t for sq in exists_filter.subqueries for t in sq.base_tables}
        assert {"REFERRAL_HIST", "REFERRAL_BED_DAY"}.issubset(nested_tables)


# ============================================================
# Full nested lineage — scope-contribs carry-over through CTE hops
# ============================================================

class TestFullNestedLineage:

    def test_rfi_date_pulls_in_all_five_base_tables(self, resolved):
        """Under the 'include nested' rule, RFI_DATE depends on five Clarity tables:
        - V_CCHP_UMAuthorizationHistory_Fact  (direct expr + outer WHERE)
        - V_CCHP_UMAuthorization_Fact         (AugHistorySla join keys)
        - V_CCHP_UMAuthorizationRequest_Fact  (source of augd.AUTH_REQUEST_ID)
        - Clarity.dbo.REFERRAL                (AugDenialSla join partner)
        - V_CCHP_UMAuthorizationRequestStatusHistory_Fact  (AugDenialSla EXISTS)
        """
        expected = {
            "V_CCHP_UMAuthorizationHistory_Fact",
            "V_CCHP_UMAuthorization_Fact",
            "V_CCHP_UMAuthorizationRequest_Fact",
            "REFERRAL",
            "V_CCHP_UMAuthorizationRequestStatusHistory_Fact",
        }
        assert expected.issubset(set(resolved["RFI_DATE"].base_tables))
