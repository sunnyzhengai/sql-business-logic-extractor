"""Pinpoint the REAL parse-error location for ONE .sql file.

The scanner captured only the single line sqlglot pointed at -- but that's
where the parser GAVE UP, not necessarily the cause, and preprocessing shifts
line numbers vs the raw file. This runs the same preprocess + parse the pipeline
uses, then prints the full sqlglot error PLUS the ~10 lines around it IN THE
PREPROCESSED text (with the error line marked `>>`), so you see the true culprit.

In-kernel cell. Edit PATH to one failing file, run, paste the output.
"""

import re
import sys

REPO_DIR = "/lakehouse/default/Files"
PATH = "/lakehouse/default/Files/data/views_cookrpt/<failing_file>.sql"   # EDIT
CONTEXT_BEFORE = 9     # lines to show before the error line

if REPO_DIR not in sys.path:
    sys.path.insert(0, REPO_DIR)

import sqlglot
from tools.shared.sql_loader import read_sql_robust
from sql_logic_extractor.resolve import preprocess_ssms

raw = read_sql_robust(PATH)
is_proc = bool(re.search(r"\bCREATE\s+(?:OR\s+ALTER\s+)?PROC", raw, re.IGNORECASE))
print(f"file: {PATH.split('/')[-1]}  | chars: {len(raw)}  | is_proc: {is_proc}")

# Same preprocessing the pipeline applies (SSMS strip + parsing-rule registry).
clean, _ = preprocess_ssms(raw)

# Use parse() (not parse_one) so multi-statement proc bodies don't error just
# for having several statements -- we want the REAL construct error.
try:
    sqlglot.parse(clean, dialect="tsql")
    print("\nParses clean after preprocessing. (The pipeline error may be "
          "in a later step, or already fixed by a rule you've copied.)")
except Exception as e:
    print("\nERROR:", str(e).splitlines()[0])
    m = re.search(r"[Ll]ine (\d+)", str(e))
    lines = clean.splitlines()
    if m:
        n = int(m.group(1))
        lo = max(0, n - 1 - CONTEXT_BEFORE)
        hi = min(len(lines), n + 1)
        print(f"\n--- preprocessed lines {lo + 1}..{hi} (>> = sqlglot's error line) ---")
        for i in range(lo, hi):
            print(f"{i + 1:4d}{'  >>' if i + 1 == n else '    '} {lines[i]}")
    else:
        print("(no line number in the error; showing first 25 preprocessed lines)")
        for i, ln in enumerate(lines[:25]):
            print(f"{i + 1:4d}    {ln}")
