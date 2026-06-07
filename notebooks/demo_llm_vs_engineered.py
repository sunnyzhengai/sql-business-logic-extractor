#!/usr/bin/env python3
"""Proof-of-value demo: engineered vs LLM-polished view descriptions.

Runs Tool 4 (report description generator) twice on the same SQL view --
once in the deterministic "engineered" mode (no LLM), once in LLM mode --
and prints the two business descriptions side by side so the contrast is
obvious. This is the artifact for showing stakeholders *why* the LLM mode
is worth it: same pipeline, same metadata, far more readable prose.

DATA SAFETY: this operates on SQL view definitions + the Clarity data
dictionary (schema metadata only). It never connects to a database and
never sends patient rows -- only table/column names, descriptions, and
query logic. See wiki for the metadata-only governance posture.

Setup (one time):
    pip install -e ".[ai-openai,business]"
    export SLE_LLM_PROVIDER=openai
    export OPENAI_API_KEY=sk-...            # your personal key
    # optional: export OPENAI_MODEL=gpt-4o-mini   (default)

Usage:
    python3 notebooks/demo_llm_vs_engineered.py [path/to/view.sql]
"""

import os
import sys
import textwrap
from pathlib import Path

# Make the package importable whether run from the repo root or this folder:
# add the repo root (the parent of notebooks/) to sys.path before importing.
_REPO_ROOT = str(Path(__file__).resolve().parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from sql_logic_extractor.products import generate_report_description
from tools.report_description_generator.cli import _load_schema


# Defaults chosen to showcase the contrast: a rich, multi-CTE referral
# analytics view against the mockup Clarity data dictionary.
_DEFAULT_VIEW = "tests/clarity_schema_tests/sql/complex_referral_analytics.sql"
_SCHEMA_PATH = "data/schemas/clarity_schema.yaml"


def _print_block(title: str, body: str) -> None:
    """Print a labeled, word-wrapped block so long descriptions stay readable
    in a terminal."""
    print(f"\n{'=' * 70}\n{title}\n{'=' * 70}")
    for paragraph in (body or "(empty)").splitlines() or [""]:
        print("\n".join(textwrap.wrap(paragraph, width=70)) or "")


def main() -> int:
    """Run engineered + LLM descriptions for one view and print both."""
    # Resolve the view + schema relative to the repo root so the demo runs
    # from any working directory.
    view_path = sys.argv[1] if len(sys.argv) > 1 else str(Path(_REPO_ROOT) / _DEFAULT_VIEW)
    schema_path = str(Path(_REPO_ROOT) / _SCHEMA_PATH)
    sql = Path(view_path).read_text(encoding="utf-8", errors="replace")
    schema = _load_schema(schema_path)

    print(f"View:   {view_path}")
    print(f"Schema: {schema_path}")

    # 1) Engineered mode -- deterministic, no LLM, no key required.
    engineered = generate_report_description(sql, schema)
    _print_block("ENGINEERED (mechanical, no LLM)", engineered.business_description)

    # 2) LLM mode -- requires a configured provider (here: OpenAI). If no key
    #    is set yet, explain how to enable it rather than erroring out.
    if not (os.environ.get("OPENAI_API_KEY") or os.environ.get("SLE_LLM_PROVIDER")):
        print("\n" + "-" * 70)
        print("LLM mode skipped: no LLM provider configured.")
        print("To enable, set:  export SLE_LLM_PROVIDER=openai")
        print("                 export OPENAI_API_KEY=sk-...")
        return 0

    llm = generate_report_description(sql, schema, use_llm=True)
    _print_block("LLM-POLISHED (OpenAI)", llm.business_description)
    _print_block("LLM primary_purpose", llm.primary_purpose)
    print(f"\nLLM key_metrics: {llm.key_metrics}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
