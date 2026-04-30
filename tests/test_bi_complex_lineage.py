"""Per-column ground-truth lineage tests for queries/bi_complex/input.sql.

Rule applied (row-existence model):
- A column's lineage includes every base Clarity table whose rows are
  mandatory for producing the column's output row (value or NULL).
- Mandatory = FROM driver, INNER joins (recursively through CTEs),
  WHERE/HAVING/QUALIFY predicates, EXISTS/IN subqueries.
- LEFT/RIGHT/FULL join right-sides are NOT mandatory for left-side rows —
  they only contribute when a column's value itself traces through that side.

Main query shape:
    FROM ReferralDenials rd
    LEFT JOIN AugDenialSla aug      ON aug.REFERRAL_ID = rd.REFERRAL_ID
    LEFT JOIN ReferralHistoryItemValues rhiv ON ...
    LEFT JOIN AugHistorySla ahs     ON ...

All main joins are LEFT, so aug/rhiv/ahs contribs only reach columns that
originate from those respective CTEs. The main driver (rd → ReferralDenials)
contributes to every main column via row-existence.

Run: python3 -m pytest tests/test_bi_complex_lineage.py -v
"""

from pathlib import Path

import pytest

from sql_logic_extractor.resolve import resolve_query


SQL_PATH = Path(__file__).resolve().parent.parent / "data" / "queries" / "bi_complex" / "input.sql"


# Per-CTE base-table contributions under the row-existence rule.
RD_BASE = {
    "V_CCHP_AuthHeader_Fact",     # ReferralDenials FROM driver
    "REFERRAL_HIST",              # ReferralDenials WHERE EXISTS branch 1
    "REFERRAL_BED_DAY",           # ReferralDenials WHERE EXISTS branch 2
}
AUG_BASE = RD_BASE | {
    "V_CCHP_UMAuthorizationRequest_Fact",               # AugDenialSla FROM driver
    "REFERRAL",                                         # AugDenialSla INNER JOIN rfl
    "V_CCHP_UMAuthorizationRequestStatusHistory_Fact",  # AugDenialSla WHERE EXISTS
}
RHIV_BASE = RD_BASE | {
    "REFERRAL_HISTORY",       # ReferralHistoryItemValues INNER JOIN RFLH
    "RFL_HX_ACT",             # INNER JOIN RFLHA
    "RFL_HX_ITEM_CHANGE",     # INNER JOIN RFLHIC
    "RFL_HX_NEW_VAL",         # INNER JOIN RFLHNV
}
# AHS chains through AugDenialSla (its FROM driver), so inherits AUG_BASE too.
AHS_BASE = AUG_BASE | {
    "V_CCHP_UMAuthorization_Fact",          # AugHistorySla INNER JOIN aut
    "V_CCHP_UMAuthorizationHistory_Fact",   # AugHistorySla INNER JOIN authx + WHERE
}


EXPECTED: dict[str, set[str]] = {
    # rd.*  (7 columns — all ReferralDenials passthroughs)
    "REFERRAL_ID":        RD_BASE,
    "COVERAGE_ID":        RD_BASE,
    "ENTRY_DATE":         RD_BASE,
    "START_DATE":         RD_BASE,
    "PRIORITY":           RD_BASE,
    "REFERRAL_PROV_ID":   RD_BASE,
    "REFERRING_PROV_ID":  RD_BASE,
    # aug.*  (4 columns — AugDenialSla passthroughs)
    "AUTH_REQUEST_ID":    AUG_BASE,
    "AUG_RECEIVED_DTTM":  AUG_BASE,
    "LOB_ID":             AUG_BASE,
    "RFL_TYPE_C":         AUG_BASE,
    # rhiv.*  (3 columns — ReferralHistoryItemValues passthroughs)
    "ITEM_CHANGE":        RHIV_BASE,
    "NEW_VALUE_EXTERNAL": RHIV_BASE,
    "ACTION_DTTM":        RHIV_BASE,
    # ahs.*  (5 columns — AugHistorySla calculated/window)
    "DENIAL_ROW":         AHS_BASE,
    "DENIAL_DATE":        AHS_BASE,
    "DENIAL_REASON":      AHS_BASE,
    "RFI_ROW":            AHS_BASE,
    "RFI_DATE":           AHS_BASE,
}


@pytest.fixture(scope="module")
def resolved():
    r = resolve_query(SQL_PATH.read_text(), dialect="tsql")
    return {c.name: c for c in r.columns}


@pytest.mark.parametrize("col_name,expected_tables", sorted(EXPECTED.items()))
def test_column_base_tables(resolved, col_name, expected_tables):
    """Every bi_complex output column resolves to its exact set of base tables."""
    col = resolved.get(col_name)
    assert col is not None, f"Column {col_name} not found in resolved output"
    actual = set(col.base_tables)
    assert actual == expected_tables, (
        f"\n{col_name}:\n"
        f"  missing  : {sorted(expected_tables - actual)}\n"
        f"  unexpected: {sorted(actual - expected_tables)}"
    )


def test_column_count(resolved):
    """The query produces exactly 19 output columns."""
    assert len(resolved) == len(EXPECTED)
    assert set(resolved.keys()) == set(EXPECTED.keys())
