"""Tests for the engineered column-ref translator enhancements:
  - short_description in the schema is preferred over description
  - ZC_<X>.NAME projections translate to the table's domain
    (e.g., ZC_APPT_STATUS.NAME -> "Appointment Status")
"""

from sqlglot import exp, parse_one

from sql_logic_extractor.patterns import Context, translate
from sql_logic_extractor.patterns.columns import _zc_domain_english


def _translate_column_ref(sql_frag: str, schema: dict) -> str:
    """Parse a fragment, walk its first column ref through the engineered
    translator, return the English string."""
    node = parse_one(sql_frag, dialect="tsql")
    if isinstance(node, exp.Select) and node.selects:
        node = node.selects[0]
    if isinstance(node, exp.Alias):
        node = node.this
    return translate(node, Context(schema=schema)).english


# ---------- short_description preference --------------------------------

def test_short_description_preferred_over_description():
    schema = {
        "tables": [
            {
                "name": "PATIENT",
                "columns": [
                    {
                        "name": "PAT_ID",
                        "description": "The unique identifier for a patient in the master file.",
                        "short_description": "Patient ID",
                    },
                ],
            }
        ]
    }
    assert _translate_column_ref("PATIENT.PAT_ID", schema) == "Patient ID"


def test_falls_back_to_description_when_short_description_missing():
    schema = {
        "tables": [
            {
                "name": "PATIENT",
                "columns": [
                    {
                        "name": "PAT_ID",
                        "description": "Patient identifier",
                        # no short_description
                    },
                ],
            }
        ]
    }
    assert _translate_column_ref("PATIENT.PAT_ID", schema) == "Patient identifier"


def test_falls_back_to_abbreviation_expansion_when_no_schema_entry():
    """Empty schema -> name-fragment expansion still works."""
    out = _translate_column_ref("PATIENT.PAT_ID", schema={})
    assert "Patient" in out


# ---------- ZC_<X>.NAME heuristic ---------------------------------------

def test_zc_table_name_yields_humanized_domain():
    assert _zc_domain_english("ZC_APPT_STATUS") == "Appointment Status"
    assert _zc_domain_english("ZC_PAT_STATUS") == "Patient Status"
    assert _zc_domain_english("zc_appt_status") == "Appointment Status"
    assert _zc_domain_english("ENCOUNTER") is None
    assert _zc_domain_english("") is None
    assert _zc_domain_english(None) is None


def test_zc_dot_name_translates_to_table_domain_without_schema():
    """ZC_<X>.NAME projections short-circuit the schema lookup -- the
    table name carries the semantics."""
    out = _translate_column_ref("ZC_APPT_STATUS.NAME", schema={})
    assert out == "Appointment Status"


def test_zc_dot_name_overrides_schema_description():
    """Even if the schema has a generic description for ZC_*.NAME (which
    Clarity often does -- 'Name' or 'Display name'), the heuristic wins."""
    schema = {
        "tables": [
            {
                "name": "ZC_APPT_STATUS",
                "columns": [
                    {"name": "NAME", "description": "Name", "short_description": "Name"},
                ],
            }
        ]
    }
    assert _translate_column_ref("ZC_APPT_STATUS.NAME", schema) == "Appointment Status"


def test_non_name_column_on_zc_table_uses_normal_path():
    """ZC_<X>.<X>_C (the numeric code) is NOT the ZC.NAME shortcut --
    it falls through to schema lookup / abbreviation expansion."""
    out = _translate_column_ref("ZC_APPT_STATUS.APPT_STATUS_C", schema={})
    # Should NOT be "Appointment Status" -- that's reserved for the NAME column.
    assert out != "Appointment Status"
