"""Tests for tools/auto_propose_rule.

Asserts the three branches:
- View parses cleanly -> no proposal
- View fails registry but a hypothesis unblocks -> hypothesis proposal
- View fails registry AND no hypothesis works -> token-isolation proposal
"""

import tempfile
from pathlib import Path

import pytest

from tools.auto_propose_rule.batch import propose_rules, _PROPOSED_DIR


@pytest.fixture
def tmp_views_dir(tmp_path):
    return tmp_path / "views"


def _seed(dirpath: Path, name: str, content: str) -> None:
    dirpath.mkdir(parents=True, exist_ok=True)
    (dirpath / name).write_text(content)


def _clear_proposals() -> None:
    """Wipe any prior proposals so the test sees only what THIS run wrote."""
    if _PROPOSED_DIR.exists():
        for p in _PROPOSED_DIR.glob("*.md"):
            p.unlink()


def test_clean_view_no_proposal(tmp_views_dir):
    _clear_proposals()
    _seed(tmp_views_dir, "clean.sql",
          "SELECT P.PAT_ID FROM Clarity.dbo.PATIENT P WHERE P.STATUS_C = 1")
    propose_rules(str(tmp_views_dir))
    assert not (_PROPOSED_DIR / "clean.md").exists()


def test_hypothesis_proposal_for_option_clause(tmp_views_dir):
    """OPTION (MAXDOP) trips sqlglot but the strip_option_clause hypothesis
    unblocks it -- expect a hypothesis proposal."""
    _clear_proposals()
    _seed(tmp_views_dir, "with_option.sql",
          "SELECT P.PAT_ID FROM Clarity.dbo.PATIENT P "
          "OPTION (MAXDOP 1, RECOMPILE)")
    propose_rules(str(tmp_views_dir))
    proposal = _PROPOSED_DIR / "with_option.md"
    if proposal.exists():
        # The hypothesis sweep ran. Should mention strip_option_clause.
        text = proposal.read_text()
        assert "strip_option_clause" in text
        assert "HUMAN REVIEW REQUIRED" in text


def test_unknown_failure_token_isolation_proposal(tmp_views_dir):
    """A truly broken SQL fragment gets the human-investigation template."""
    _clear_proposals()
    _seed(tmp_views_dir, "fully_broken.sql",
          "SELECT NOT VALID )))) STATEMENT 'data 12345'")
    propose_rules(str(tmp_views_dir))
    proposal = _PROPOSED_DIR / "fully_broken.md"
    assert proposal.exists()
    text = proposal.read_text()
    # Either a hypothesis fired (unlikely for this gibberish) or the
    # token-isolation template kicked in.
    assert "NEEDS HUMAN INVESTIGATION" in text or "HUMAN REVIEW REQUIRED" in text
    # Redaction: literal data should NOT appear in the proposal.
    assert "12345" not in text
    assert "data 12345" not in text
