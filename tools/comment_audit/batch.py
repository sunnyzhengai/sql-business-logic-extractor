#!/usr/bin/env python3
"""Tool 8 -- per-view comment audit + corpus-level intent histogram.

Notebook usage:

    from tools.comment_audit.batch import audit_comments
    audit_comments(input_dir='/lakehouse/default/Files/views',
                    output_csv='/lakehouse/default/Files/outputs/comment_audit.csv')

Outputs TWO CSVs:

  comment_audit.csv (per-view):
    view_name, n_comments, n_label, n_doc, n_section_header,
    n_audit, n_todo, n_unclassified

  comment_audit_samples.csv (one row per comment):
    view_name, line, col, kind, intent, text_redacted

The samples CSV is REDACTED by default (string literals -> '***',
numbers -> N) so it's safe to share. It's the empirical input you'll
use to decide which intents are worth surfacing in Tool 3 / Tool 4.
"""

import argparse
import csv
import re
import sys
from collections import Counter
from pathlib import Path

from sql_logic_extractor.comments import extract_comments


# ---------- file reader (matches engine's BOM handling) -------------------

def _read_sql_file(path: Path) -> str:
    raw = path.read_bytes()
    if raw.startswith(b"\xff\xfe"):
        return raw.decode("utf-16-le")[1:]
    if raw.startswith(b"\xfe\xff"):
        return raw.decode("utf-16-be")[1:]
    if raw.startswith(b"\xef\xbb\xbf"):
        return raw.decode("utf-8")[1:]
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("utf-16-le", errors="replace")


# ---------- redaction (literals -> '***', numbers -> N) -------------------

_STRING_LIT_RE = re.compile(r"'(?:[^']|'')*'")
_NUMBER_LIT_RE = re.compile(r"\b\d+\b")


def _redact(text: str) -> str:
    text = _STRING_LIT_RE.sub("'***'", text)
    text = _NUMBER_LIT_RE.sub("N", text)
    # Newlines collapse to a single space so each comment fits in one CSV cell.
    return " ".join(text.split())


# ---------- core audit -----------------------------------------------------

INTENTS = ("label", "doc", "section_header", "audit", "todo", "unclassified")


def audit_comments(input_dir: str,
                    output_csv: str = "comment_audit.csv",
                    samples_csv: str | None = None) -> int:
    """Walk views, extract comments, write per-view counts + (optional)
    sampled rows for human review.

    `samples_csv` defaults to <output_csv stem>_samples.csv if not given.
    """
    in_dir = Path(input_dir)
    if not in_dir.is_dir():
        print(f"Error: {in_dir} is not a directory", file=sys.stderr)
        return 1
    sql_files = sorted(in_dir.glob("*.sql"))
    if not sql_files:
        print(f"Error: no .sql files in {in_dir}", file=sys.stderr)
        return 1

    out_main = Path(output_csv)
    if samples_csv is None:
        samples_csv = str(out_main.with_name(out_main.stem + "_samples.csv"))
    out_samples = Path(samples_csv)
    out_main.parent.mkdir(parents=True, exist_ok=True)
    out_samples.parent.mkdir(parents=True, exist_ok=True)

    per_view_rows: list[dict] = []
    sample_rows: list[dict] = []
    corpus_intent_counter: Counter[str] = Counter()

    for path in sql_files:
        try:
            sql = _read_sql_file(path)
        except Exception as e:
            per_view_rows.append({
                "view_name": path.stem, "n_comments": 0,
                **{f"n_{i}": 0 for i in INTENTS},
                "read_error": f"{type(e).__name__}: {e}",
            })
            continue

        try:
            _, comments = extract_comments(sql)
        except Exception as e:
            per_view_rows.append({
                "view_name": path.stem, "n_comments": 0,
                **{f"n_{i}": 0 for i in INTENTS},
                "read_error": f"extract_comments raised {type(e).__name__}: {e}",
            })
            continue

        intent_counts = Counter(c.intent for c in comments)
        corpus_intent_counter.update(intent_counts)
        per_view_rows.append({
            "view_name": path.stem,
            "n_comments": len(comments),
            **{f"n_{i}": intent_counts.get(i, 0) for i in INTENTS},
            "read_error": "",
        })
        for c in comments:
            sample_rows.append({
                "view_name": path.stem,
                "line": c.line,
                "col": c.col,
                "kind": c.kind,
                "intent": c.intent,
                "text_redacted": _redact(c.text)[:240],
            })

    # ---- write the per-view CSV ----
    main_fields = (
        ["view_name", "n_comments"]
        + [f"n_{i}" for i in INTENTS]
        + ["read_error"]
    )
    with out_main.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=main_fields)
        writer.writeheader()
        writer.writerows(per_view_rows)

    # ---- write the samples CSV ----
    sample_fields = ["view_name", "line", "col", "kind", "intent", "text_redacted"]
    with out_samples.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=sample_fields)
        writer.writeheader()
        writer.writerows(sample_rows)

    # ---- console summary (the value of this tool is THIS at-a-glance) ----
    n_views = len(per_view_rows)
    n_total = sum(corpus_intent_counter.values())
    print(f"\nComment audit: {n_views} views, {n_total} comment(s) total")
    print(f"  -> per-view counts:  {out_main}")
    print(f"  -> redacted samples: {out_samples}")
    print("\nCorpus-level intent histogram:")
    for intent in INTENTS:
        n = corpus_intent_counter.get(intent, 0)
        bar = "#" * min(40, n // max(1, n_total // 40)) if n else ""
        print(f"  {intent:>16}: {n:>5}  {bar}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract + classify comments across a folder of SQL views."
    )
    parser.add_argument("input_dir", help="Folder containing view *.sql files")
    parser.add_argument("-o", "--output", default="comment_audit.csv")
    parser.add_argument("--samples", default=None,
                          help="Path for the per-comment samples CSV "
                                "(default: <output stem>_samples.csv)")
    args = parser.parse_args()
    return audit_comments(args.input_dir, args.output, args.samples)


def _is_notebook() -> bool:
    return "ipykernel" in sys.argv[0] or "ipykernel" in " ".join(sys.argv[1:])


if __name__ == "__main__":
    if _is_notebook():
        print("Notebook environment detected -- call audit_comments("
              "input_dir=..., output_csv=...) from a cell.")
    else:
        sys.exit(main())
