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
    build_cohort,
    cohorts_to_markdown,
    view_to_cohorts,
)
from tools.extract_corpus.batch import extract_corpus
from tools.view_shape_compare.dim_filter import DimFilter


# ---------- pure-function units ------------------------------------------

def test_table_descriptions_lookup_case_insensitive():
    td = TableDescriptions(by_name={"PATIENT": "patients", "PAT_ENC": "encounters"})
    assert td.get("PATIENT") == "patients"
    assert td.get("patient") == "patients"
    assert td.get("Clarity.dbo.PATIENT") == "patients"
    assert td.get("UNKNOWN_TABLE") is None


def test_build_cohort_single_table():
    td = TableDescriptions(by_name={"PATIENT": "patients"})
    assert build_cohort(["PATIENT"], [], td) == "patients"


def test_build_cohort_two_tables_joined():
    td = TableDescriptions(by_name={"PATIENT": "patients", "PAT_ENC": "encounters"})
    assert build_cohort(["PATIENT", "PAT_ENC"], [], td) == "patients with encounters"


def test_build_cohort_falls_back_to_humanized_name():
    td = TableDescriptions.empty()
    assert build_cohort(["WEIRD_TABLE"], [], td) == "weird table"


def test_build_cohort_returns_empty_when_only_upstream():
    """A scope that reads ONLY from another scope (no base tables)
    yields empty cohort -- caller renders 'same as <upstream>'."""
    td = TableDescriptions.empty()
    assert build_cohort([], ["cte:Foo"], td) == ""


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
