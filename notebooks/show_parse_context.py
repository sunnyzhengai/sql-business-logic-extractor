"""Pinpoint the REAL batch outcome + parse-error context for ONE .sql file.

Mirrors exactly what the description batch does per file type:
  - VIEW  -> preprocess_ssms + parse
  - PROC  -> select_into_to_cte (which may legitimately SKIP it as not
             view-shaped, e.g. it has DECLARE/INSERT/multiple results)
and reports the true outcome:
  - BATCH: ok            -> parses; would be described
  - BATCH: SKIPPED       -> not view-shaped (NOT an error) + the reason
  - BATCH: ERROR         -> a real parser gap + the ~10 lines around it
                            (in the text sqlglot actually parsed, error line `>>`)

In-kernel cell. Edit PATH to one file from the scan, run, paste the output.
"""

import re
import sys

REPO_DIR = "/lakehouse/default/Files"
PATH = "/lakehouse/default/Files/data/views_cookrpt/<failing_file>.sql"   # EDIT
CONTEXT_BEFORE = 9

if REPO_DIR not in sys.path:
    sys.path.insert(0, REPO_DIR)

import sqlglot
from tools.shared.sql_loader import read_sql_robust
from sql_logic_extractor.resolve import preprocess_ssms
from sql_logic_extractor.parsing_rules import apply_all
from sql_logic_extractor.proc_normalize import (
    ProcNotViewShaped, select_into_to_cte,
    _strip_proc_wrapper, _strip_temp_guards,
)


def _show_context(text: str, err: Exception) -> None:
    print("BATCH: ERROR ->", str(err).splitlines()[0])
    m = re.search(r"[Ll]ine (\d+)", str(err))
    lines = text.splitlines()
    if not m:
        print("(no line number; first 25 lines of what was parsed:)")
        for i, ln in enumerate(lines[:25]):
            print(f"{i + 1:4d}    {ln}")
        return
    n = int(m.group(1))
    lo, hi = max(0, n - 1 - CONTEXT_BEFORE), min(len(lines), n + 1)
    print(f"--- parsed lines {lo + 1}..{hi}  (>> = sqlglot's error line) ---")
    for i in range(lo, hi):
        print(f"{i + 1:4d}{'  >>' if i + 1 == n else '    '} {lines[i]}")


raw = read_sql_robust(PATH)
is_proc = bool(re.search(r"\bCREATE\s+(?:OR\s+ALTER\s+)?PROC", raw, re.IGNORECASE))
print(f"file: {PATH.split('/')[-1]}  | chars: {len(raw)}  | is_proc: {is_proc}\n")

if is_proc:
    # Replicate select_into_to_cte's body prep so we can show context if the
    # body itself won't parse; then ask select_into_to_cte for the verdict.
    _, body = _strip_proc_wrapper(raw)
    body = _strip_temp_guards(body)
    body, _ = apply_all(body)
    try:
        sqlglot.parse(body, dialect="tsql")          # the parse select_into_to_cte does
    except Exception as e:
        _show_context(body, e)                       # a real parser gap in the proc body
    else:
        try:
            select_into_to_cte(raw)
            print("BATCH: ok  (normalizes cleanly to a view -> would be described)")
        except ProcNotViewShaped as e:
            print(f"BATCH: SKIPPED  (not view-shaped) -- reason: {e.reason}"
                  + (f"  | detail: {e.detail}" if e.detail else ""))
            print("This is NOT a parser error -- it's correctly out of scope "
                  "(ETL / variables / multiple result sets).")
else:
    clean, _ = preprocess_ssms(raw)
    try:
        sqlglot.parse_one(clean, dialect="tsql")
        print("BATCH: ok  (view parses -> would be described)")
    except Exception as e:
        _show_context(clean, e)
