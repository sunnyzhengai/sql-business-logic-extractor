"""End-to-end tests for tools/similarity.

Pins the three dry-run shapes from the design discussion:
  Shape 1: A is a table-superset of B -> L1 only.
  Shape 2: CTE-wrapped vs flat, semantically identical -> L4.
  Shape 3: complex multi-CTE vs flat slice -> L1 only.
"""

import json
from pathlib import Path

from tools.p10_extract.batch import extract_corpus
from tools.similarity.batch import extract_similarity_clusters
from tools.similarity.signatures import (
    ViewSignature,
    build_view_signature,
    _canonicalize_filter,
    _column_identity,
    _leaf_driver,
)


# ---------- unit tests on signature helpers -----------------------------

def test_leaf_driver_chases_through_cte():
    """A scope chain main -> cte:X -> base table TBL should resolve to TBL."""
    scopes_by_id = {
        "main": {
            "id": "main",
            "reads_from_scopes": ["cte:Inner"],
            "reads_from_tables": [],
        },
        "cte:Inner": {
            "id": "cte:Inner",
            "reads_from_scopes": [],
            "reads_from_tables": ["PATIENT"],
        },
    }
    assert _leaf_driver(scopes_by_id, "main") == "PATIENT"


def test_leaf_driver_handles_nested_cte_chain():
    """main -> cte:A -> cte:B -> TBL."""
    scopes_by_id = {
        "main": {"id": "main", "reads_from_scopes": ["cte:A"], "reads_from_tables": []},
        "cte:A": {"id": "cte:A", "reads_from_scopes": ["cte:B"], "reads_from_tables": []},
        "cte:B": {"id": "cte:B", "reads_from_scopes": [], "reads_from_tables": ["ENCOUNTER"]},
    }
    assert _leaf_driver(scopes_by_id, "main") == "ENCOUNTER"


def test_canonicalize_filter_strips_alias_prefixes():
    """Two filters that differ only in alias choice should canonicalize
    to the same string."""
    a = _canonicalize_filter("CVG.COVERAGE_TYPE_C = 2 AND CVG.STATUS_C = 1")
    b = _canonicalize_filter("C.COVERAGE_TYPE_C = 2 AND C.STATUS_C = 1")
    assert a == b
    # And neither should contain the alias prefix.
    assert "CVG." not in a
    assert "C." not in a


def test_canonicalize_filter_strips_equijoin_keys_and_sorts():
    canon = _canonicalize_filter(
        "P.PAT_ID = E.PAT_ID AND P.STATUS_C = 1 AND E.ENC_DATE > '2020-01-01'"
    )
    # Equi-key gone; remaining predicates alphabetically sorted
    assert "PAT_ID = PAT_ID" not in canon
    assert "P.PAT_ID = E.PAT_ID" not in canon
    assert "STATUS_C = 1" in canon
    assert "ENC_DATE >" in canon
    # Same predicates in different order should produce the same canon
    canon2 = _canonicalize_filter(
        "E.ENC_DATE > '2020-01-01' AND P.STATUS_C = 1 AND P.PAT_ID = E.PAT_ID"
    )
    assert canon == canon2


def test_column_identity_passthrough_uses_source():
    col = {
        "column_name": "PAT_ID",
        "column_type": "passthrough",
        "base_columns": ["table:PATIENT.PAT_ID"],
        "fingerprint": None,
    }
    assert _column_identity(col, scopes_by_id={}) == "src:PATIENT.PAT_ID"


def test_column_identity_falls_back_to_fingerprint():
    col = {
        "column_name": "ROW_NUM",
        "column_type": "window",
        "base_columns": [],
        "fingerprint": "abc123def456",
    }
    assert _column_identity(col, scopes_by_id={}) == "fp:abc123def456"


def test_column_identity_resolves_through_cte():
    """A main column with base_columns=['cte:Inner.PAT_ID'] should
    transitively resolve via the cte's column to its base source."""
    scopes_by_id = {
        "cte:Inner": {
            "id": "cte:Inner",
            "columns": [
                {"column_name": "PAT_ID",
                 "base_columns": ["table:PATIENT.PAT_ID"],
                 "fingerprint": None},
            ],
        },
    }
    main_col = {
        "column_name": "PAT_ID",
        "column_type": "passthrough",
        "base_columns": ["cte:Inner.PAT_ID"],
        "fingerprint": None,
    }
    assert _column_identity(main_col, scopes_by_id) == "src:PATIENT.PAT_ID"


# ---------- end-to-end shape tests --------------------------------------

def _seed(views_dir: Path, name: str, sql: str) -> None:
    views_dir.mkdir(parents=True, exist_ok=True)
    (views_dir / f"{name}.sql").write_text(sql)


def _run(tmp_path: Path):
    views = tmp_path / "views"
    corpus = tmp_path / "corpus.jsonl"
    extract_corpus(str(views), str(corpus))
    out = tmp_path / "sim_out"
    extract_similarity_clusters(str(corpus), str(out))
    return out


def _load(out_dir: Path, level: str) -> dict:
    return json.loads((out_dir / f"clusters_{level}.json").read_text())


def _cluster_with_members(doc: dict, expected_members: set[str]) -> dict | None:
    for c in doc["clusters"]:
        if set(c["members"]) == expected_members:
            return c
    return None


# Shape 1: A superset of B in tables, DIFFERENT drivers -> no L1 cluster
# (strict driver equality).

def test_shape1_different_drivers_do_not_l1_cluster(tmp_path):
    """v_a drives from PATIENT, v_b drives from ENCOUNTER. Even though
    ENCOUNTER appears in v_a's all_tables, strict-driver-equality L1
    keeps them in separate clusters. Per the design choice to avoid
    over-clustering in healthcare corpora where everything chains
    through shared fact tables."""
    views = tmp_path / "views"
    _seed(views, "v_a",
        "SELECT P.PAT_ID, E.ENC_DATE, D.DX_ID "
        "FROM Clarity.dbo.PATIENT P "
        "INNER JOIN Clarity.dbo.ENCOUNTER E ON E.PAT_ID = P.PAT_ID "
        "INNER JOIN Clarity.dbo.DIAGNOSIS D ON D.PAT_ENC_CSN_ID = E.PAT_ENC_CSN_ID")
    _seed(views, "v_b",
        "SELECT E.ENC_ID, D.DX_ID "
        "FROM Clarity.dbo.ENCOUNTER E "
        "INNER JOIN Clarity.dbo.DIAGNOSIS D ON D.PAT_ENC_CSN_ID = E.PAT_ENC_CSN_ID")
    out = _run(tmp_path)

    # Different drivers (PATIENT vs ENCOUNTER) -> separate L1 clusters.
    l1 = _load(out, "L1")
    assert _cluster_with_members(l1, {"v_a", "v_b"}) is None
    # And L2-L4 are also separate (joined_sets differ regardless).
    for level in ("L2", "L3", "L4"):
        doc = _load(out, level)
        assert _cluster_with_members(doc, {"v_a", "v_b"}) is None


# Shape 2: CTE-wrapped vs flat, semantically identical -- L4

def test_shape2_cte_vs_flat_clusters_at_l4(tmp_path):
    views = tmp_path / "views"
    _seed(views, "v_a_cte", """
        WITH active_patients AS (
            SELECT P.PAT_ID, P.PAT_NAME, P.BIRTH_DATE
            FROM Clarity.dbo.PATIENT P
            WHERE P.PAT_STATUS_C = 1
        )
        SELECT AP.PAT_ID, AP.PAT_NAME, AP.BIRTH_DATE
        FROM active_patients AP
    """)
    _seed(views, "v_b_flat", """
        SELECT P.PAT_ID, P.PAT_NAME, P.BIRTH_DATE
        FROM Clarity.dbo.PATIENT P
        WHERE P.PAT_STATUS_C = 1
    """)
    out = _run(tmp_path)

    # Both views should be in the same cluster at every level
    for level in ("L1", "L2", "L3", "L4"):
        doc = _load(out, level)
        cluster = _cluster_with_members(doc, {"v_a_cte", "v_b_flat"})
        assert cluster is not None, (
            f"v_a_cte/v_b_flat should cluster at {level}, but didn't. Got: {doc}"
        )


# Shape 3: Multi-CTE vs flat slice, DIFFERENT drivers -> no L1 cluster.

def test_shape3_multi_cte_vs_flat_slice_different_drivers(tmp_path):
    """v_complex drives from T1 (via cte1), v_flat_slice drives from T3.
    Different drivers -> different L1 clusters under strict equality."""
    views = tmp_path / "views"
    _seed(views, "v_complex", """
        WITH cte1 AS (
            SELECT t1.A, t1.B FROM Clarity.dbo.T1 t1 WHERE t1.STATUS_C = 1
        ),
        cte2 AS (
            SELECT t2.A, t2.C FROM Clarity.dbo.T2 t2 WHERE t2.TYPE_C = 5
        ),
        cte3 AS (
            SELECT t3.A, t3.D FROM Clarity.dbo.T3 t3 WHERE t3.FLAG_C = 'Y'
            UNION ALL
            SELECT t4.A, t4.D FROM Clarity.dbo.T4 t4 WHERE t4.FLAG_C = 'Y'
        )
        SELECT cte1.A, cte2.C, cte3.D
        FROM cte1
        INNER JOIN cte2 ON cte1.A = cte2.A
        INNER JOIN cte3 ON cte1.A = cte3.A
    """)
    _seed(views, "v_flat_slice", """
        SELECT t3.D, t1.B
        FROM Clarity.dbo.T3 t3
        INNER JOIN Clarity.dbo.T1 t1 ON t1.A = t3.A
        WHERE t1.STATUS_C = 1 AND t3.FLAG_C = 'Y'
    """)
    out = _run(tmp_path)

    # Different drivers (T1 vs T3) -> separate L1 clusters.
    l1 = _load(out, "L1")
    assert _cluster_with_members(l1, {"v_complex", "v_flat_slice"}) is None


# ---------- bonus: join-type consistency annotation ----------------------

def test_l2_cluster_flags_mixed_join_types(tmp_path):
    """Two views with the same tables but INNER vs LEFT join should
    cluster at L2 (type-agnostic) and have join_type_consistency = 'mixed'."""
    views = tmp_path / "views"
    _seed(views, "v_inner",
        "SELECT P.PAT_ID, E.ENC_DATE FROM Clarity.dbo.PATIENT P "
        "INNER JOIN Clarity.dbo.ENCOUNTER E ON E.PAT_ID = P.PAT_ID")
    _seed(views, "v_left",
        "SELECT P.PAT_ID, E.ENC_DATE FROM Clarity.dbo.PATIENT P "
        "LEFT JOIN Clarity.dbo.ENCOUNTER E ON E.PAT_ID = P.PAT_ID")
    out = _run(tmp_path)
    l2 = _load(out, "L2")
    cluster = _cluster_with_members(l2, {"v_inner", "v_left"})
    assert cluster is not None
    assert cluster["join_type_consistency"] == "mixed"


# ---------- features file ------------------------------------------------

def test_features_file_has_all_views(tmp_path):
    views = tmp_path / "views"
    _seed(views, "v1", "SELECT P.PAT_ID FROM Clarity.dbo.PATIENT P")
    _seed(views, "v2", "SELECT E.ENC_ID FROM Clarity.dbo.ENCOUNTER E")
    out = _run(tmp_path)
    feats = json.loads((out / "features.json").read_text())
    names = {v["view_name"] for v in feats["views"]}
    assert names == {"v1", "v2"}
