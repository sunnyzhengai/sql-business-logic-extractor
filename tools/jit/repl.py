#!/usr/bin/env python3
"""Interactive JIT Q&A REPL.

Run from the repo root:
    python -m tools.jit.repl /path/to/corpus.jsonl

Or with LLM synthesis:
    python -m tools.jit.repl /path/to/corpus.jsonl --llm openai

Type questions, get answers. Type 'quit' or Ctrl-D to exit.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO_ROOT = str(Path(__file__).resolve().parents[2])
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


def main():
    parser = argparse.ArgumentParser(description="Interactive JIT Q&A")
    parser.add_argument("corpus", help="Path to corpus.jsonl")
    parser.add_argument("--schema", default=None,
                        help="Path to clarity_schema.yaml (optional)")
    parser.add_argument("--llm", default=None,
                        help="LLM provider for synthesis (openai / azure-openai / gemini)")
    args = parser.parse_args()

    from tools.jit.ask import build_index, ask
    from tools.shared.corpus_io import load_corpus
    from tools.jit.pattern_classifier import (
        classify_view, classify_corpus, summarize_corpus_patterns,
    )

    print("Building index...")
    build_index(args.corpus, schema_path=args.schema, llm_provider=args.llm)

    _, views = load_corpus(args.corpus)
    views_by_name = {v.get("view_name", ""): v for v in views}

    print()
    print("Commands:")
    print("  (any question)     — ask about the corpus")
    print("  /patterns          — show capture pattern summary")
    print("  /patterns <view>   — show patterns for one view")
    print("  /views <pattern>   — list views matching a pattern")
    print("  quit               — exit")
    print("-" * 50)
    print()

    pattern_groups = classify_corpus(views)

    while True:
        try:
            question = input("ask> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye!")
            break
        if not question:
            continue
        if question.lower() in ("quit", "exit", "q"):
            print("Bye!")
            break

        # --- Built-in commands ---
        if question == "/patterns":
            print()
            print(summarize_corpus_patterns(views))
            print()
            continue

        if question.startswith("/patterns "):
            vname = question[len("/patterns "):].strip()
            view = views_by_name.get(vname)
            if not view:
                # Case-insensitive search
                for k, v in views_by_name.items():
                    if k.upper() == vname.upper():
                        view = v
                        break
            if view:
                patterns = classify_view(view)
                print()
                if patterns:
                    print(f"## {view.get('view_name', vname)}")
                    print()
                    for p in patterns:
                        print(f"  - **{p.label}** (anchor: `{p.anchor_table}`)")
                        print(f"    {p.description}")
                else:
                    print(f"No capture patterns matched for {vname}")
                print()
            else:
                print(f"\nView '{vname}' not found.\n")
            continue

        if question.startswith("/views "):
            pattern_name = question[len("/views "):].strip().lower()
            # Allow partial/label match
            matched_key = None
            for key in pattern_groups:
                if key == pattern_name or pattern_name in key:
                    matched_key = key
                    break
            if not matched_key:
                # Try matching on label
                from tools.jit.pattern_classifier import _PATTERN_DEFINITIONS
                for pdef in _PATTERN_DEFINITIONS:
                    if pattern_name in pdef["label"].lower():
                        matched_key = pdef["name"]
                        break
            if matched_key and matched_key in pattern_groups:
                group_views = pattern_groups[matched_key]
                print(f"\n## Views matching '{matched_key}' ({len(group_views)})\n")
                for v in group_views[:20]:
                    report = v.get("report") or {}
                    purpose = report.get("primary_purpose", "")
                    print(f"  - `{v.get('view_name', '?')}`"
                          f"{' — ' + purpose if purpose else ''}")
                if len(group_views) > 20:
                    print(f"  ... and {len(group_views) - 20} more")
                print()
            else:
                print(f"\nPattern '{pattern_name}' not found. Try /patterns to see available.\n")
            continue

        # --- Regular question ---
        print()
        result = ask(question)
        print(result.to_markdown())
        print()


if __name__ == "__main__":
    main()
