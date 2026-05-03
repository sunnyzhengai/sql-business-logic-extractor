"""Tests for tools/extract_corpus/batch.py.

Asserts the corpus extractor's contract:
- Produces a JSONL file with one header line + one view per line.
- Header has schema_version=1 and n_views matching the corpus size.
- Round-trip: read the JSONL back into CorpusV1, assert structural
  invariants (compact form, table indices resolve, filters split).
- Errors on a single view don't kill the whole run.
- Filters are stored ONCE on the view (compact), not duplicated per
  column in storage.
"""

import json
from pathlib import Path

from sql_logic_extractor.corpus_schema import (
    SCHEMA_VERSION,
    corpus_from_jsonl_lines,
    expand_view,
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


# ---------- end-to-end round-trip -----------------------------------------

def test_extract_corpus_writes_valid_jsonl(tmp_path):
    views = tmp_path / "views"
    _seed_views(views)
    out = tmp_path / "corpus.jsonl"

    extract_corpus(str(views), str(out))

    assert out.is_file()
    lines = out.read_text().splitlines()
    # 1 header + 2 views
    assert len(lines) == 3
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


# ---------- compact form invariants ---------------------------------------

def test_view_filters_appear_once_in_storage(tmp_path):
    """Compact form: P.STATUS_C = 1 should be in view_level.filters
    ONCE, not duplicated across each column."""
    views = tmp_path / "views"
    _seed_views(views)
    out = tmp_path / "corpus.jsonl"
    extract_corpus(str(views), str(out))

    raw = out.read_text()
    # Each view has the filter; 2 views means 2 occurrences MAX in the
    # view_level.filters lists. NOT 2 * (number of columns).
    assert raw.count("P.STATUS_C = 1") <= 2 + 2  # +2 slack for any quoted echo

    corpus = corpus_from_jsonl_lines(iter(raw.splitlines()))
    for view in corpus.views:
        # Confirm filters are at the view level, not duplicated per column.
        assert "P.STATUS_C = 1" in view.view_level.filters
        # Per-column filters_extra should be empty for these views (all
        # filters are view-level).
        for col in view.columns:
            assert col.filters_extra == ()


def test_expand_view_resolves_indices_and_inflates_filters(tmp_path):
    views = tmp_path / "views"
    _seed_views(views)
    out = tmp_path / "corpus.jsonl"
    extract_corpus(str(views), str(out))

    corpus = corpus_from_jsonl_lines(iter(out.read_text().splitlines()))
    for view in corpus.views:
        expanded = expand_view(view)
        for col in expanded["columns"]:
            # base_tables_idx -> base_tables (resolved names)
            assert "base_tables_idx" not in col
            assert "base_tables" in col
            for t in col["base_tables"]:
                assert isinstance(t, str)
            # filters inflated to the full inherited+extra list
            assert "filters_inherited" not in col
            assert "filters" in col


# ---------- per-column data populated -------------------------------------

def test_columns_have_fingerprints_and_terms(tmp_path):
    views = tmp_path / "views"
    _seed_views(views)
    out = tmp_path / "corpus.jsonl"
    extract_corpus(str(views), str(out))
    corpus = corpus_from_jsonl_lines(iter(out.read_text().splitlines()))

    for view in corpus.views:
        for col in view.columns:
            # Term should be present (even if empty / structural).
            assert col.term is not None
            # Fingerprint is set for non-empty resolved expressions.
            if col.resolved_expression:
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
    pregnant_col = next(c for c in pregnant_view.columns if c.column_name == "Pregnant")
    assert "pregnant" in pregnant_col.term.name_tokens


# ---------- robustness ----------------------------------------------------

def test_one_failing_view_does_not_kill_run(tmp_path):
    views = tmp_path / "views"
    views.mkdir()
    (views / "good.sql").write_text(
        "SELECT P.PAT_ID FROM Clarity.dbo.PATIENT P WHERE P.STATUS_C = 1"
    )
    (views / "broken.sql").write_text(
        "SELECT NOT VALID )))) HERE"
    )
    out = tmp_path / "corpus.jsonl"
    extract_corpus(str(views), str(out))

    corpus = corpus_from_jsonl_lines(iter(out.read_text().splitlines()))
    assert len(corpus.views) == 2
    by_name = {v.view_name: v for v in corpus.views}
    # The broken view exists in the corpus with an error placeholder.
    assert "broken" in by_name
    assert "PARSE/RESOLVE ERROR" in by_name["broken"].view_level.report.technical_description
    # The good view processed normally.
    assert "good" in by_name
    assert by_name["good"].columns   # has columns


def test_progress_file_written_per_view(tmp_path):
    views = tmp_path / "views"
    _seed_views(views)
    out = tmp_path / "corpus.jsonl"
    extract_corpus(str(views), str(out))

    progress = tmp_path / "corpus_progress.txt"
    assert progress.is_file()
    text = progress.read_text()
    # Both views should appear in the progress log
    assert "v_active" in text
    assert "v_pregnant" in text
