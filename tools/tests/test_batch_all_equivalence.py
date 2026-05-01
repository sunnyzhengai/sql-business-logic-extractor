"""Equivalence tests for tools/batch_all.py.

batch_all runs the engine ONCE per view to produce all 4 tools' outputs
in a single pass. This test asserts that the output is BIT-IDENTICAL to
running each tool's individual batch processor separately. If the two
diverge, batch_all has drifted from the per-tool semantics and we want
to catch that immediately -- accuracy is the headline guarantee.

Uses the same V_ACTIVE_MEMBERS and bi_complex fixtures that each tool's
golden tests use. Per-tool fixtures already pin each tool's output
shape; this test pins the equivalence between fast-path and slow-path.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tools.batch_all import _process_view_all_tools
from tools.column_lineage_extractor.batch import _process_view as tool1_process
from tools.technical_logic_extractor.batch import _process_view as tool2_process
from tools.business_logic_extractor.batch import _process_view as tool3_process
from tools.report_description_generator.batch import _process_view as tool4_process


# Reuse Tool 2's fixtures as the cross-tool integration corpus.
FIXTURES_DIR = (Path(__file__).resolve().parent.parent /
                  "technical_logic_extractor" / "tests" / "fixtures")


def _discover_fixtures() -> list[Path]:
    if not FIXTURES_DIR.is_dir():
        return []
    return sorted(c for c in FIXTURES_DIR.iterdir()
                    if c.is_dir() and (c / "input.sql").is_file())


@pytest.mark.parametrize("fixture_dir", _discover_fixtures(),
                         ids=lambda p: p.name)
def test_batch_all_matches_individual_tools(fixture_dir: Path) -> None:
    """For one view, batch_all must produce the same rows for each tool
    as that tool's individual batch processor would on the same input."""
    input_sql = fixture_dir / "input.sql"

    # Fast path: single resolver pass via batch_all.
    fast = _process_view_all_tools(input_sql, schema={}, use_llm=False,
                                      llm_client=None, dialect="tsql")

    # Slow path: each tool's individual processor (no sharing).
    slow_tool1 = tool1_process(input_sql, dialect="tsql")
    slow_tool2 = tool2_process(input_sql, dialect="tsql")
    slow_tool3 = tool3_process(input_sql, schema={}, use_llm=False,
                                  llm_client=None, dialect="tsql")
    slow_tool4 = [tool4_process(input_sql, schema={}, use_llm=False,
                                  llm_client=None, dialect="tsql")]

    assert fast["tool1"] == slow_tool1, \
        f"Tool 1 output diverges: fast={fast['tool1']!r} vs slow={slow_tool1!r}"
    assert fast["tool2"] == slow_tool2, \
        f"Tool 2 output diverges (first row): fast={fast['tool2'][:1]!r} vs slow={slow_tool2[:1]!r}"
    assert fast["tool3"] == slow_tool3, \
        f"Tool 3 output diverges (first row): fast={fast['tool3'][:1]!r} vs slow={slow_tool3[:1]!r}"
    assert fast["tool4"] == slow_tool4, \
        f"Tool 4 output diverges: fast={fast['tool4']!r} vs slow={slow_tool4!r}"
