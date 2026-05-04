"""End-to-end tests for tools/dataset_extract.

The user's stated pattern -- CTE1, CTE2 reads CTE1, main reads CTE2,
each with its own WHERE -- must render as three datasets, each carrying
ONLY its own scope's filters. Plus the main scope's PCP join with
CLARITY_SER.
"""

import json
from pathlib import Path

from tools.dataset_extract.batch import extract_datasets
from tools.dataset_extract.render import (
    Dataset,
    datasets_to_json_dict,
    datasets_to_markdown,
    humanize_scope_id,
    scope_to_dataset,
    view_to_datasets,
)
from tools.extract_corpus.batch import extract_corpus


# ---------- humanize_scope_id pure-function unit tests -------------------

def test_humanize_main_scope():
    assert humanize_scope_id("main") == "Main query (view output)"


def test_humanize_camel_case_cte():
    assert humanize_scope_id("cte:ActivePatients") == "Active Patients"
    assert humanize_scope_id("cte:Age12Patients") == "Age 12 Patients"


def test_humanize_snake_case_cte():
    assert humanize_scope_id("cte:active_patients") == "Active Patients"


def test_humanize_unknown_form_passes_through():
    assert humanize_scope_id("subquery:0") == "0"  # synthetic id, no real name
    assert humanize_scope_id("derived:t") == "T"


# ---------- scope -> Dataset (unit) -------------------------------------

def test_scope_to_dataset_carries_only_its_own_filters():
    fake_scope = {
        "id": "cte:ActivePatients",
        "kind": "cte",
        "reads_from_tables": ["PATIENT"],
        "reads_from_scopes": [],
        "filters": [
            {"expression": "P.STATUS_C = 1",
             "english": "Status C = 1", "kind": "where"},
        ],
        "columns": [
            {"column_name": "PAT_ID", "column_type": "passthrough",
             "technical_description": "P.PAT_ID",
             "business_description": "Patient identifier"},
        ],
    }
    d = scope_to_dataset(fake_scope)
    assert d.name == "Active Patients"
    assert d.kind == "cte"
    assert d.base_datasets == ()
    assert d.base_tables == ("PATIENT",)
    assert len(d.filters) == 1
    assert d.filters[0].english == "Status C = 1"
    assert len(d.data_columns) == 1
    assert d.data_columns[0].english == "Patient identifier"


# ---------- end-to-end: the user's CTE1->CTE2->main pattern --------------

def _seed_user_pattern(views_dir: Path) -> None:
    views_dir.mkdir(parents=True, exist_ok=True)
    (views_dir / "v_pcp_for_age12_active.sql").write_text("""
WITH ActivePatients AS (
    SELECT P.PAT_ID
    FROM Clarity.dbo.PATIENT P
    WHERE P.STATUS_C = 1
),
Age12Patients AS (
    SELECT AP.PAT_ID
    FROM ActivePatients AP
    INNER JOIN Clarity.dbo.PATIENT P ON P.PAT_ID = AP.PAT_ID
    WHERE P.AGE_YEARS > 12
)
SELECT A12.PAT_ID, S.PROV_NAME AS PCPProviderName
FROM Age12Patients A12
INNER JOIN Clarity.dbo.PATIENT P ON P.PAT_ID = A12.PAT_ID
INNER JOIN Clarity.dbo.CLARITY_SER S ON S.PROV_ID = P.CUR_PCP_PROV_ID
""")


def _run(tmp_path: Path):
    views = tmp_path / "views"
    _seed_user_pattern(views)
    corpus = tmp_path / "corpus.jsonl"
    extract_corpus(str(views), str(corpus))
    out = tmp_path / "datasets_out"
    extract_datasets(str(corpus), str(out))
    return out


def test_three_datasets_emitted_with_correct_lineage(tmp_path):
    out = _run(tmp_path)
    doc = json.loads((out / "datasets.json").read_text())
    assert doc["n_views"] == 1
    view = doc["views"][0]
    assert view["view_name"] == "v_pcp_for_age12_active"

    by_scope = {d["scope_id"]: d for d in view["datasets"]}
    assert "cte:ActivePatients" in by_scope
    assert "cte:Age12Patients" in by_scope
    assert "main" in by_scope

    # CTE1 -> only its own filter
    cte1 = by_scope["cte:ActivePatients"]
    assert cte1["base_datasets"] == []
    assert "PATIENT" in cte1["base_tables"]
    cte1_filters = " | ".join(f["english"] for f in cte1["filters"])
    assert "Status C = 1" in cte1_filters
    assert "Age" not in cte1_filters

    # CTE2 -> reads CTE1, only its own filter
    cte2 = by_scope["cte:Age12Patients"]
    assert "Active Patients" in cte2["base_datasets"]
    cte2_filters = " | ".join(f["english"] for f in cte2["filters"])
    assert "Age" in cte2_filters
    assert "Status C = 1" not in cte2_filters

    # main -> reads CTE2, joins CLARITY_SER
    main = by_scope["main"]
    assert "Age 12 Patients" in main["base_datasets"]
    assert "CLARITY_SER" in main["base_tables"]
    col_names = {c["name"] for c in main["data_columns"]}
    assert col_names == {"PAT_ID", "PCPProviderName"}


def test_markdown_output_renders_dataset_sections(tmp_path):
    out = _run(tmp_path)
    md = (out / "datasets.md").read_text()
    # Each dataset has its own ### header with its humanized name
    assert "### Active Patients" in md
    assert "### Age 12 Patients" in md
    assert "### Main query (view output)" in md
    # And the lineage edge is shown for downstream scopes
    assert "**Base dataset:** Active Patients" in md
    assert "**Base dataset:** Age 12 Patients" in md


# ---------- skip-views-with-no-scopes ------------------------------------

def test_view_with_no_scopes_is_skipped(tmp_path):
    """A view that fails to parse falls through to error_view (no
    scopes); dataset_extract should skip it cleanly, not crash."""
    views = tmp_path / "views"
    views.mkdir()
    (views / "good.sql").write_text(
        "SELECT P.PAT_ID FROM Clarity.dbo.PATIENT P WHERE P.STATUS_C = 1"
    )
    (views / "broken.sql").write_text("SELECT NOT VALID )))) HERE")

    corpus = tmp_path / "corpus.jsonl"
    extract_corpus(str(views), str(corpus))
    out = tmp_path / "datasets_out"
    extract_datasets(str(corpus), str(out))

    doc = json.loads((out / "datasets.json").read_text())
    names = {v["view_name"] for v in doc["views"]}
    assert "good" in names
    assert "broken" not in names
    assert doc["n_skipped"] >= 1
