"""Tests for tools/cohort_extract.

Covers the user's three stated examples:
  1. Simple: SELECT * FROM PATIENT WHERE LAST_NAME='Jackson'
     -> cohort: patients; filters: <last name = 'Jackson'>
  2. Join: PATIENT JOIN PAT_ENC WHERE CONTACT_DATE BETWEEN ...
     -> cohort: patients with encounters; filters: <date between>
  3. CTE: LatestVisits AS (FROM PAT_ENC ...) SELECT FROM LV WHERE ROW_NUM=1
     -> cohort1 (CTE): encounters; cohort2 (main): inherited from CTE
"""

import json
from pathlib import Path

from tools.cohort_extract.batch import extract_cohorts
from tools.cohort_extract.render import (
    TableDescriptions,
    _strip_equijoin_keys,
    build_cohort,
    cohorts_to_markdown,
    view_to_cohorts,
)
from tools.extract_corpus.batch import extract_corpus
from tools.view_shape_compare.dim_filter import DimFilter


# ---------- pure-function units ------------------------------------------

def test_strip_equijoin_keys_drops_word_word_pairs():
    """Pure key fragments like `<X> = <X>` (both sides bare word
    phrases) get dropped; real predicates stay."""
    text = ("Coverage Identifier = Coverage Identifier "
            "and Coverage Type C = 2 "
            "and Mem Covered Yn = 'Y'")
    out = _strip_equijoin_keys(text)
    assert "Coverage Identifier = Coverage Identifier" not in out
    assert "Coverage Type C = 2" in out
    assert "Mem Covered Yn = 'Y'" in out


def test_strip_equijoin_keys_drops_cross_table_keys_too():
    """`Member Identifier = Patient Identifier` -- two different
    translated phrases but both bare word patterns -- is also a key
    relationship between tables, dropped."""
    text = "Member Identifier = Patient Identifier and Eff Date <= today"
    out = _strip_equijoin_keys(text)
    assert "Member Identifier = Patient Identifier" not in out
    assert "Eff Date <= today" in out


def test_strip_equijoin_keys_keeps_filters_with_quoted_or_numeric_rhs():
    """RHS with quotes ('Y') or digits (2) is a literal value, not a
    column reference -- the predicate stays."""
    assert _strip_equijoin_keys("Status C = 1") == "Status C = 1"
    assert _strip_equijoin_keys("Mem Covered Yn = 'Y'") == "Mem Covered Yn = 'Y'"
    assert _strip_equijoin_keys("Last Name = 'Jackson'") == "Last Name = 'Jackson'"


def test_strip_equijoin_keys_keeps_non_equality_predicates():
    """Operators other than `=` (>=, <=, BETWEEN, etc.) mean it's a
    real comparison, never a key."""
    assert _strip_equijoin_keys("Eff Date >= today") == "Eff Date >= today"
    assert _strip_equijoin_keys("Eff Date <= today") == "Eff Date <= today"


def test_strip_equijoin_keys_returns_empty_when_only_keys():
    """A predicate that's pure equi-join keys (no business content)
    strips to empty string. Caller drops the filter entirely then."""
    assert _strip_equijoin_keys(
        "Coverage Identifier = Coverage Identifier "
        "and Member Identifier = Patient Identifier"
    ) == ""


def test_table_descriptions_lookup_case_insensitive():
    td = TableDescriptions(by_name={"PATIENT": "patients", "PAT_ENC": "encounters"})
    assert td.get("PATIENT") == "patients"
    assert td.get("patient") == "patients"
    assert td.get("Clarity.dbo.PATIENT") == "patients"
    assert td.get("UNKNOWN_TABLE") is None


def test_table_descriptions_from_schema_dict():
    """The PRIMARY source for table descriptions: a clarity_schema.json
    dict whose `tables[*].short_description` field is populated (sourced
    from the TABLE_SHORT_DESCRIPTION column of clarity_metadata.csv)."""
    schema = {
        "tables": [
            {"name": "PATIENT",
             "description": "Long verbose Clarity intro...",
             "short_description": "patients"},
            {"name": "PAT_ENC",
             "short_description": "encounters"},
            {"name": "TABLE_WITH_NO_SHORT_DESC",
             "description": "Only the long form populated"},
        ]
    }
    td = TableDescriptions.from_schema(schema)
    assert td.get("PATIENT") == "patients"
    assert td.get("PAT_ENC") == "encounters"
    # Tables without short_description are NOT in the lookup --
    # caller falls back to humanized table name.
    assert td.get("TABLE_WITH_NO_SHORT_DESC") is None


def test_table_descriptions_merge_schema_overrides_yaml(tmp_path):
    """Schema wins over YAML overlay on conflict; YAML still
    contributes for tables not in the schema."""
    yaml_path = tmp_path / "overlay.yaml"
    yaml_path.write_text("PATIENT: legacy\nCUSTOM_TABLE: custom thing\n")

    schema = {"tables": [{"name": "PATIENT", "short_description": "patients"}]}
    yaml_td = TableDescriptions.from_yaml(yaml_path)
    schema_td = TableDescriptions.from_schema(schema)
    merged = TableDescriptions.merge(yaml_td, schema_td)

    # Schema overrode YAML for PATIENT
    assert merged.get("PATIENT") == "patients"
    # YAML still provided CUSTOM_TABLE (not in schema)
    assert merged.get("CUSTOM_TABLE") == "custom thing"


def test_build_cohort_head_only_when_no_others():
    td = TableDescriptions(by_name={"PATIENT": "patients"})
    assert build_cohort("PATIENT", [], [], td) == "patients"


def test_build_cohort_head_with_one_other():
    td = TableDescriptions(by_name={"PATIENT": "patients", "PAT_ENC": "encounters"})
    assert build_cohort("PATIENT", ["PAT_ENC"], [], td) == "patients with encounters"


def test_build_cohort_head_only_when_two_or_more_others():
    """Layer-1 rule: with 2+ others, fall back to head only -- avoid
    arbitrary leaf pick. User can override with Layer 2 if needed."""
    td = TableDescriptions(by_name={
        "COVERAGE": "coverages", "CLARITY_LOC": "locations",
        "CLARITY_SER": "providers", "CVG_SUBSCR_ADDR": "subscriber addresses",
    })
    cohort = build_cohort(
        "COVERAGE",
        ["CLARITY_LOC", "CLARITY_SER", "CVG_SUBSCR_ADDR"],
        [], td,
    )
    assert cohort == "coverages"


def test_build_cohort_falls_back_to_humanized_name():
    td = TableDescriptions.empty()
    assert build_cohort("WEIRD_TABLE", [], [], td) == "weird table"


def test_build_cohort_returns_empty_when_no_head():
    """A scope with no head (no base-table driver, no selected
    sources) yields empty cohort -- caller renders 'same as <upstream>'."""
    td = TableDescriptions.empty()
    assert build_cohort("", [], ["cte:Foo"], td) == ""
    assert build_cohort("", [], [], td) == ""


# ---------- end-to-end cases --------------------------------------------

def _run_cohorts(tmp_path: Path, sql: str, view_name: str = "v_test"):
    """Helper: write SQL, extract corpus, run cohort_extract."""
    views = tmp_path / "views"
    views.mkdir(exist_ok=True)
    (views / f"{view_name}.sql").write_text(sql)
    corpus = tmp_path / "corpus.jsonl"
    extract_corpus(str(views), str(corpus))
    out = tmp_path / "cohorts_out"
    extract_cohorts(str(corpus), str(out))
    return json.loads((out / "cohorts.json").read_text())


def _scope(doc, view_name, scope_id):
    view = next(v for v in doc["views"] if v["view_name"] == view_name)
    return next(c for c in view["cohorts"] if c["scope_id"] == scope_id)


def test_simple_query_one_table_one_filter(tmp_path):
    """User's first example: SELECT * FROM PATIENT WHERE LAST_NAME='Jackson'"""
    doc = _run_cohorts(
        tmp_path,
        "SELECT * FROM Clarity.dbo.PATIENT WHERE LAST_NAME = 'Jackson'",
        view_name="v_jackson",
    )
    main = _scope(doc, "v_jackson", "main")
    assert main["cohort"] == "patients"
    assert any("Last Name" in f and "Jackson" in f for f in main["filters"])


def test_join_query_explicit_columns_picks_selected_tables(tmp_path):
    """User's second example: explicit projection picks PATIENT name +
    PAT_ENC date -> cohort = 'patients with encounters' (both contribute
    selected columns, so both belong to the cohort)."""
    doc = _run_cohorts(
        tmp_path,
        """
        SELECT P.PAT_NAME, PE.CONTACT_DATE
        FROM Clarity.dbo.PATIENT P
        INNER JOIN Clarity.dbo.PAT_ENC PE ON P.PAT_ID = PE.PAT_ID
        WHERE PE.CONTACT_DATE BETWEEN '2026-01-01' AND '2026-01-31'
        """,
        view_name="v_jan_visits",
    )
    main = _scope(doc, "v_jan_visits", "main")
    assert "patients" in main["cohort"]
    assert "encounters" in main["cohort"]
    # Filter should mention Contact Date and the date range
    assert any("Contact Date" in f for f in main["filters"])


def test_join_only_table_with_no_projection_is_excluded(tmp_path):
    """Reverse of the above: PATIENT joined for the WHERE filter, but
    no patient column projected -> cohort is just "encounters". Tables
    that don't contribute SELECTED columns are enrichment, not cohort."""
    doc = _run_cohorts(
        tmp_path,
        """
        SELECT PE.CONTACT_DATE, PE.PAT_ENC_CSN_ID
        FROM Clarity.dbo.PATIENT P
        INNER JOIN Clarity.dbo.PAT_ENC PE ON P.PAT_ID = PE.PAT_ID
        WHERE P.LAST_NAME = 'Jackson'
        """,
        view_name="v_jackson_visits",
    )
    main = _scope(doc, "v_jackson_visits", "main")
    assert main["cohort"] == "encounters"   # PATIENT joined, not projected
    # Filter still surfaces the patient-level WHERE
    assert any("Last Name" in f and "Jackson" in f for f in main["filters"])


def test_head_is_from_driver_when_driver_contributes_a_column(tmp_path):
    """Driver = the FROM-clause leftmost. When the driver contributes a
    selected column (e.g., PATIENT.NAME), it's the head and any other
    selected entity is a leaf."""
    doc = _run_cohorts(
        tmp_path,
        """
        SELECT P.PAT_NAME, PE.CONTACT_DATE
        FROM Clarity.dbo.PATIENT P
        INNER JOIN Clarity.dbo.PAT_ENC PE ON P.PAT_ID = PE.PAT_ID
        """,
        view_name="v_head_driver",
    )
    main = _scope(doc, "v_head_driver", "main")
    # Head = PATIENT (driver), leaf = PAT_ENC. Head leads the phrase.
    assert main["cohort"].startswith("patients ")
    assert "encounters" in main["cohort"]


def test_two_or_more_others_falls_back_to_head(tmp_path):
    """When 2+ entities besides the head contribute selected columns,
    the heuristic falls back to head only -- avoid arbitrary leaf pick.
    User can annotate via Layer 2 override (not yet shipped)."""
    doc = _run_cohorts(
        tmp_path,
        """
        SELECT P.PAT_NAME, PE.CONTACT_DATE, OP.ORDER_ID, D.DX_ID
        FROM Clarity.dbo.PATIENT P
        INNER JOIN Clarity.dbo.PAT_ENC PE ON P.PAT_ID = PE.PAT_ID
        INNER JOIN Clarity.dbo.ORDER_PROC OP ON OP.PAT_ENC_CSN_ID = PE.PAT_ENC_CSN_ID
        INNER JOIN Clarity.dbo.DIAGNOSIS D ON D.PAT_ID = P.PAT_ID
        """,
        view_name="v_many_entities",
    )
    main = _scope(doc, "v_many_entities", "main")
    assert main["cohort"] == "patients"


def test_join_on_business_filters_kept_keys_stripped(tmp_path):
    """JOIN ON predicates often mix equi-join keys (X = X structurally)
    with real business filters. Keys are dropped; business predicates
    stay. Both WHERE and the JOIN-clause filter should land in the
    cohort filter list."""
    doc = _run_cohorts(
        tmp_path,
        """
        SELECT P.PAT_ID, PE.CONTACT_DATE
        FROM Clarity.dbo.PATIENT P
        INNER JOIN Clarity.dbo.PAT_ENC PE
            ON P.PAT_ID = PE.PAT_ID
            AND PE.STATUS_C = 1
        WHERE P.IS_VALID_PAT_YN = 'Y'
        """,
        view_name="v_join_filter_kept",
    )
    main = _scope(doc, "v_join_filter_kept", "main")
    joined = " | ".join(main["filters"])
    # WHERE filter survives
    assert "Is Valid Pat Yn" in joined or "'Y'" in joined
    # JOIN ON business predicate survives
    assert "Status C = 1" in joined
    # Equi-join key (Patient Identifier = Patient Identifier) does NOT
    assert "Patient Identifier = Patient Identifier" not in joined


def test_select_distinct_grain_three_tables(tmp_path):
    """User's main example: PATIENT JOIN PAT_ENC JOIN PAT_ENC_DX JOIN
    CLARITY_EDG, projecting patient name + ICD10_CODE. Cohort sources:
    PATIENT (name) and CLARITY_EDG (ICD10_CODE). The two JOIN-only
    tables (PAT_ENC, PAT_ENC_DX) carve the row grain but don't appear
    in the cohort phrase."""
    doc = _run_cohorts(
        tmp_path,
        """
        SELECT P.PAT_NAME, EDG.CURRENT_ICD10_LIST AS ICD10_CODE
        FROM Clarity.dbo.PATIENT P
        INNER JOIN Clarity.dbo.PAT_ENC PE ON P.PAT_ID = PE.PAT_ID
        INNER JOIN Clarity.dbo.PAT_ENC_DX PED ON PED.PAT_ENC_CSN_ID = PE.PAT_ENC_CSN_ID
        INNER JOIN Clarity.dbo.CLARITY_EDG EDG ON EDG.DX_ID = PED.DX_ID
        """,
        view_name="v_pt_icd10",
    )
    main = _scope(doc, "v_pt_icd10", "main")
    assert "patients" in main["cohort"]
    assert "ICD10 codes" in main["cohort"]
    # The intermediate join tables are NOT in the cohort
    assert "encounter" not in main["cohort"].lower()


def test_cte_pattern_carves_population_per_scope(tmp_path):
    """User's third example: LatestVisits CTE then main with row_num=1."""
    doc = _run_cohorts(
        tmp_path,
        """
        WITH LatestVisits AS (
            SELECT PE.PAT_ID,
                   PE.CONTACT_DATE,
                   ROW_NUMBER() OVER (PARTITION BY PE.PAT_ID ORDER BY PE.CONTACT_DATE DESC) AS ROW_NUM
            FROM Clarity.dbo.PAT_ENC PE
            WHERE PE.CONTACT_DATE BETWEEN '2026-01-01' AND '2026-01-31'
        )
        SELECT * FROM LatestVisits LV WHERE LV.ROW_NUM = 1
        """,
        view_name="v_latest_visits",
    )
    cte = _scope(doc, "v_latest_visits", "cte:LatestVisits")
    main = _scope(doc, "v_latest_visits", "main")

    # CTE: cohort = encounters; filter = the date range
    assert cte["cohort"] == "encounters"
    assert any("Contact Date" in f for f in cte["filters"])

    # Main: reads ONLY from the CTE -> empty cohort phrase, base_datasets points at CTE
    assert main["cohort"] == ""
    assert main["base_datasets"] == ["cte:LatestVisits"]
    # Filter ROW_NUM = 1 (mechanical for now; window-pattern detection deferred)
    assert any("1" in f for f in main["filters"])


def test_markdown_output_renders_each_scope(tmp_path):
    doc_path_setup = _run_cohorts(
        tmp_path,
        "SELECT * FROM Clarity.dbo.PATIENT WHERE LAST_NAME = 'Jackson'",
        view_name="v_jackson",
    )
    md = (tmp_path / "cohorts_out" / "cohorts.md").read_text()
    assert "## v_jackson" in md
    assert "**Cohort:** patients" in md
    assert "Last Name" in md
    assert "Jackson" in md
