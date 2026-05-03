"""Tests for tools/extract_corpus/batch.py (v3 scope-tree extractor).

Asserts the corpus extractor's contract:
- Produces JSONL with one header + one ViewV1 per line.
- Header has the current SCHEMA_VERSION and matching n_views.
- Round-trip: read JSONL back into CorpusV1; tree shape preserved.
- Each view emits at least a "main" scope. CTEs / subqueries become
  their own scopes.
- Filters live on their owning scope; no cross-scope inheritance.
- Errors on a single view don't kill the whole run.
- Per-column governance metadata (terms, fingerprints, author_notes)
  populated on main-scope columns.
"""

import json
from pathlib import Path

from sql_logic_extractor.corpus_schema import (
    SCHEMA_VERSION,
    corpus_from_jsonl_lines,
)
from tools.extract_corpus.batch import extract_corpus


# ---------- helpers --------------------------------------------------------

def _seed_views(views_dir: Path) -> None:
    views_dir.mkdir(parents=True, exist_ok=True)
    (views_dir / "v_active.sql").write_text(
        "/* This view returns active patients. */\n"
        "SELECT P.PAT_ID, P.PAT_NAME AS Name\n"
        "FROM Clarity.dbo.PATIENT P\n"
        "WHERE P.STATUS_C = 1\n"
        "  AND P.IS_VALID_PAT_YN = 'Y'\n"
    )
    (views_dir / "v_pregnant.sql").write_text(
        "SELECT P.PAT_ID, P.IS_PREGNANT_YN AS Pregnant\n"
        "FROM Clarity.dbo.PATIENT P\n"
        "WHERE P.STATUS_C = 1\n"
    )


def _main_scope(view):
    return next(s for s in view.scopes if s.id == "main")


# ---------- end-to-end round-trip -----------------------------------------

def test_extract_corpus_writes_valid_jsonl(tmp_path):
    views = tmp_path / "views"
    _seed_views(views)
    out = tmp_path / "corpus.jsonl"

    extract_corpus(str(views), str(out))

    assert out.is_file()
    lines = out.read_text().splitlines()
    assert len(lines) == 3   # header + 2 views
    header = json.loads(lines[0])
    assert header["schema_version"] == SCHEMA_VERSION
    assert header["n_views"] == 2


def test_corpus_round_trip_through_corpus_schema(tmp_path):
    views = tmp_path / "views"
    _seed_views(views)
    out = tmp_path / "corpus.jsonl"
    extract_corpus(str(views), str(out))

    corpus = corpus_from_jsonl_lines(iter(out.read_text().splitlines()))
    assert corpus.schema_version == SCHEMA_VERSION
    assert len(corpus.views) == 2
    names = {v.view_name for v in corpus.views}
    assert names == {"v_active", "v_pregnant"}


# ---------- tree shape ----------------------------------------------------

def test_each_view_has_main_scope(tmp_path):
    views = tmp_path / "views"
    _seed_views(views)
    out = tmp_path / "corpus.jsonl"
    extract_corpus(str(views), str(out))
    corpus = corpus_from_jsonl_lines(iter(out.read_text().splitlines()))
    for view in corpus.views:
        scope_ids = {s.id for s in view.scopes}
        assert "main" in scope_ids
        assert "main" in view.view_outputs


def test_filters_live_on_main_scope_for_simple_views(tmp_path):
    """Simple views without CTEs put their WHERE filters on main."""
    views = tmp_path / "views"
    _seed_views(views)
    out = tmp_path / "corpus.jsonl"
    extract_corpus(str(views), str(out))
    corpus = corpus_from_jsonl_lines(iter(out.read_text().splitlines()))

    for view in corpus.views:
        main = _main_scope(view)
        exprs = " | ".join(f.expression for f in main.filters)
        assert "STATUS_C = 1" in exprs


def test_cte_filters_stay_on_their_cte_scope(tmp_path):
    """The user's stated requirement, end-to-end: CTE-scope filters
    do not leak into main."""
    views = tmp_path / "views"
    views.mkdir()
    (views / "v_cte.sql").write_text("""
        WITH ActivePatients AS (
            SELECT P.PAT_ID
            FROM Clarity.dbo.PATIENT P
            WHERE P.STATUS_C = 1
        )
        SELECT AP.PAT_ID
        FROM ActivePatients AP
        WHERE AP.PAT_ID > 100
    """)
    out = tmp_path / "corpus.jsonl"
    extract_corpus(str(views), str(out))
    corpus = corpus_from_jsonl_lines(iter(out.read_text().splitlines()))
    view = corpus.views[0]

    cte = next(s for s in view.scopes if s.id == "cte:ActivePatients")
    main = _main_scope(view)
    cte_exprs = " | ".join(f.expression for f in cte.filters)
    main_exprs = " | ".join(f.expression for f in main.filters)
    assert "STATUS_C = 1" in cte_exprs
    assert "PAT_ID > 100" not in cte_exprs
    assert "PAT_ID > 100" in main_exprs
    assert "STATUS_C = 1" not in main_exprs

    # Main column points at the CTE via scope-qualified base_columns.
    assert any(b.startswith("cte:ActivePatients.")
               for c in main.columns for b in c.base_columns)
    assert "cte:ActivePatients" in main.reads_from_scopes


# ---------- per-column data populated -------------------------------------

def test_main_columns_have_fingerprints_and_terms(tmp_path):
    views = tmp_path / "views"
    _seed_views(views)
    out = tmp_path / "corpus.jsonl"
    extract_corpus(str(views), str(out))
    corpus = corpus_from_jsonl_lines(iter(out.read_text().splitlines()))

    for view in corpus.views:
        main = _main_scope(view)
        for col in main.columns:
            assert col.term is not None
            if col.technical_description:
                assert col.fingerprint is not None
                assert len(col.fingerprint) >= 8


def test_pregnancy_column_token_is_canonicalized(tmp_path):
    """Spot check: column aliased `Pregnant` should have name_tokens
    containing 'pregnant' after the synonym pipeline."""
    views = tmp_path / "views"
    _seed_views(views)
    out = tmp_path / "corpus.jsonl"
    extract_corpus(str(views), str(out))
    corpus = corpus_from_jsonl_lines(iter(out.read_text().splitlines()))

    pregnant_view = next(v for v in corpus.views if v.view_name == "v_pregnant")
    main = _main_scope(pregnant_view)
    pregnant_col = next(c for c in main.columns if c.column_name == "Pregnant")
    assert "pregnant" in pregnant_col.term.name_tokens


# ---------- robustness ----------------------------------------------------

def test_one_failing_view_does_not_kill_run(tmp_path):
    views = tmp_path / "views"
    views.mkdir()
    (views / "good.sql").write_text(
        "SELECT P.PAT_ID FROM Clarity.dbo.PATIENT P WHERE P.STATUS_C = 1"
    )
    (views / "broken.sql").write_text("SELECT NOT VALID )))) HERE")
    out = tmp_path / "corpus.jsonl"
    extract_corpus(str(views), str(out))

    corpus = corpus_from_jsonl_lines(iter(out.read_text().splitlines()))
    assert len(corpus.views) == 2
    by_name = {v.view_name: v for v in corpus.views}
    assert "broken" in by_name
    assert "PARSE/RESOLVE ERROR" in by_name["broken"].report.technical_description
    assert "good" in by_name
    # Good view should have at least one main-scope column.
    assert _main_scope(by_name["good"]).columns


def test_progress_file_written_per_view(tmp_path):
    views = tmp_path / "views"
    _seed_views(views)
    out = tmp_path / "corpus.jsonl"
    extract_corpus(str(views), str(out))

    progress = tmp_path / "corpus_progress.txt"
    assert progress.is_file()
    text = progress.read_text()
    assert "v_active" in text
    assert "v_pregnant" in text
