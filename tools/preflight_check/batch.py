#!/usr/bin/env python3
"""Tool 6 -- preflight parse-health triage for a folder of SQL views.

Workflow before running Tools 1-5 on a new corpus:

    from tools.preflight_check.batch import preflight
    preflight(input_dir='/lakehouse/default/Files/views',
               output_csv='/lakehouse/default/Files/outputs/preflight_check.csv')

Output CSV (one row per view):

    view_name, status, rules_fired, error_line, error_col, error_message

Status values:
  - clean             -- parses without any registry rule firing
  - needs_rule        -- one or more registry rules fired and parse succeeded
  - unknown_failure   -- registry tried, parse still fails (action item:
                          run diagnose_parse_failure on this view + propose
                          a new rule)
  - read_error        -- couldn't even read the file (encoding / permissions)

The error_message field is REDACTED (string literals -> '***', numbers
-> N) so the CSV is safe to share with engineering or paste into a
ticket.

CLI usage:
    python -m tools.preflight_check.batch <input_dir> [-o out.csv]
"""

import argparse
import csv
import re
import sys
from pathlib import Path

import sqlglot

from sql_logic_extractor.parsing_rules import apply_all
from sql_logic_extractor.resolve import preprocess_ssms


# ---------- redaction (matches diagnose_parse_failure) ---------------------

_STRING_LIT_RE = re.compile(r"'(?:[^']|'')*'")
_NUMBER_LIT_RE = re.compile(r"\b\d+\b")


def _redact(text: str) -> str:
    text = _STRING_LIT_RE.sub("'***'", text)
    text = _NUMBER_LIT_RE.sub("N", text)
    return text


# ---------- file reader ----------------------------------------------------

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


_LINE_COL_RE = re.compile(r"[Ll]ine\s+(\d+),\s*[Cc]ol(?:umn)?\s*:?\s*(\d+)")


def _extract_line_col(error_msg: str) -> tuple[str, str]:
    m = _LINE_COL_RE.search(error_msg or "")
    return (m.group(1), m.group(2)) if m else ("", "")


# ---------- per-view classification ----------------------------------------

def classify_view(path: Path, dialect: str = "tsql") -> dict:
    """Run one view through the rule registry + parser. Return a dict
    matching the CSV row schema."""
    base = {
        "view_name": path.stem,
        "status": "",
        "rules_fired": "",
        "error_line": "",
        "error_col": "",
        "error_message": "",
    }
    try:
        sql = _read_sql_file(path)
    except Exception as e:
        base.update(status="read_error",
                     error_message=f"{type(e).__name__}: {e}")
        return base

    # preprocess_ssms internally calls apply_all + does the line-by-line
    # SSMS-boilerplate strip. We want to know which rules fired, so call
    # apply_all separately first.
    _, fired = apply_all(sql)
    try:
        clean, _meta = preprocess_ssms(sql)
    except Exception as e:
        base.update(status="unknown_failure",
                     rules_fired=", ".join(fired),
                     error_message=f"preprocess_ssms raised: {type(e).__name__}: {e}")
        return base

    try:
        sqlglot.parse_one(clean, dialect=dialect)
    except Exception as e:
        msg = str(e)
        line, col = _extract_line_col(msg)
        base.update(
            status="unknown_failure",
            rules_fired=", ".join(fired),
            error_line=line,
            error_col=col,
            error_message=_redact(msg)[:300],
        )
        return base

    base.update(
        status="needs_rule" if fired else "clean",
        rules_fired=", ".join(fired),
    )
    return base


# ---------- batch entry point ----------------------------------------------

def preflight(input_dir: str,
               output_csv: str = "preflight_check.csv",
               *, dialect: str = "tsql") -> int:
    in_dir = Path(input_dir)
    if not in_dir.is_dir():
        print(f"Error: {in_dir} is not a directory", file=sys.stderr)
        return 1
    sql_files = sorted(in_dir.glob("*.sql"))
    if not sql_files:
        print(f"Error: no .sql files in {in_dir}", file=sys.stderr)
        return 1

    rows: list[dict] = []
    for path in sql_files:
        rows.append(classify_view(path, dialect=dialect))

    out = Path(output_csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["view_name", "status", "rules_fired",
                   "error_line", "error_col", "error_message"]
    with out.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    # Console summary -- the value of preflight is THIS at-a-glance count.
    counts: dict[str, int] = {}
    for r in rows:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    print(f"\nPreflight: {len(rows)} views -> {out}")
    for status in ("clean", "needs_rule", "unknown_failure", "read_error"):
        n = counts.get(status, 0)
        if n:
            print(f"  {status:>16}: {n}")

    if counts.get("unknown_failure", 0):
        print("\nNext step: run diagnose_parse_failure on each "
              "unknown_failure view to propose a new parsing rule.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Triage parse-health for a folder of SQL views."
    )
    parser.add_argument("input_dir", help="Folder containing view *.sql files")
    parser.add_argument("-o", "--output", default="preflight_check.csv")
    parser.add_argument("-d", "--dialect", default="tsql")
    args = parser.parse_args()
    return preflight(args.input_dir, args.output, dialect=args.dialect)


def _is_notebook() -> bool:
    return "ipykernel" in sys.argv[0] or "ipykernel" in " ".join(sys.argv[1:])


if __name__ == "__main__":
    if _is_notebook():
        print("Notebook environment detected -- call preflight("
              "input_dir=..., output_csv=...) from a cell.")
    else:
        sys.exit(main())
