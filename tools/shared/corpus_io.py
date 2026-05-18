"""Corpus I/O -- read/write the corpus.jsonl file produced by p10_extract.

corpus.jsonl is the source-of-truth artifact passed between phases. The
file format is JSON-lines: each line is one JSON object. The first line
is a HEADER (schema version + view count); every subsequent line is one
ViewV1 dict.

Historical note
---------------
The `load_corpus` function lived inside `tools.operate.validate_graph_pivot`
during Phase 1 of the 2026-05 restructure. In Phase 2a it was extracted
here because the corpus loader is needed by every downstream phase
(p20_index, p30_analyze, p40_synthesize) -- it's a cross-cutting
utility, not validation-specific code.
"""

from __future__ import annotations

import json
from pathlib import Path


def load_corpus(corpus_path: str | Path) -> tuple[dict, list[dict]]:
    """Read a corpus.jsonl file and split it into (header, list-of-views).

    The header is the first line and contains metadata about the corpus
    (schema version, view count). It is not strictly needed for graph
    construction or analysis, but downstream code uses it for sanity
    checks and reports.

    Returns
    -------
    header  : dict  -- the first-line metadata object (`{"schema_version": ..., "n_views": ...}`)
    views   : list  -- one ViewV1 dict per remaining line
    """
    path = Path(corpus_path)
    header: dict = {}
    views: list[dict] = []

    # Open with UTF-8 because corpora may contain SSMS-exported text. Python's
    # default `open()` mode is text mode, which gives us strings back.
    with path.open(encoding="utf-8") as f:
        first_line = f.readline().strip()
        if first_line:
            # The header line is regular JSON; loads() parses a JSON string.
            header = json.loads(first_line)

        for line in f:
            line = line.strip()
            if not line:
                # Skip blank lines defensively; jsonl files shouldn't have them
                # but real-world files sometimes do.
                continue
            views.append(json.loads(line))

    return header, views
