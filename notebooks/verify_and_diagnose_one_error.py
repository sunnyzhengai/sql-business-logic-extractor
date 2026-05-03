"""Fabric notebook helper -- when timing_audit shows X errors and you
pushed a fix that didn't reduce X, run these cells to figure out why.

Three cells:

  Cell A -- Verify the new code is actually on Fabric (upload sanity).
  Cell B -- Drop the module cache + re-run timing_audit in ONE cell.
  Cell C -- Pick the first error view, run preprocess + sqlglot manually,
            print the redacted cleaned SQL + the actual sqlglot error.
            Tells you what the parser is REALLY choking on.
"""


# %% [Cell A: verify the latest rules.py is uploaded]

import os

p = '/lakehouse/default/Files/sql_logic_extractor/parsing_rules/rules.py'
print('exists?', os.path.isfile(p))

if os.path.isfile(p):
    content = open(p).read()
    # The b8547c2 fix uses `[^\]]+` (negated char class to allow ANY chars
    # inside [bracket-quoted] identifiers including spaces / slashes).
    print('has new bracket pattern?', r'[^\]]+' in content)
    print('size:', os.path.getsize(p), 'bytes (b8547c2 version is ~2.0 KB)')
    print()
    if r'[^\]]+' not in content:
        print(">>> The OLD rules.py is still uploaded. Re-download from:")
        print(">>>   https://github.com/sunnyzhengai/sql-business-logic-extractor"
              "/blob/main/sql_logic_extractor/parsing_rules/rules.py")
else:
    print(">>> File not found. Upload the new rules.py to the path above.")


# %% [Cell B: drop module cache + re-run timing_audit  (must be ONE cell)]

import sys

# CRITICAL: cache drop and imports must be in the SAME cell. If you split
# them across cells and only run the import cell, Python uses the cached
# (old) module and silently ignores the new one on disk.
for mod in list(sys.modules):
    if mod.startswith('sql_logic_extractor') or mod.startswith('tools'):
        del sys.modules[mod]

from tools.timing_audit.batch import audit_timing
audit_timing(
    input_dir='/lakehouse/default/Files/views',
    output_csv='/lakehouse/default/Files/outputs/timing_audit.csv',
    timeout_sec=30,
)


# %% [Cell C: deep-diagnose the FIRST error view]
#
# When 21 errors stays at 21 after a "fix", run this. It picks the first
# 'error'-status view from the audit CSV and shows you exactly what
# preprocess_ssms produces and what sqlglot complains about. The redacted
# output is safe to share.

import sys

for mod in list(sys.modules):
    if mod.startswith('sql_logic_extractor') or mod.startswith('tools'):
        del sys.modules[mod]

import csv
import os
import re

import sqlglot

from sql_logic_extractor.parsing_rules import apply_all
from sql_logic_extractor.resolve import preprocess_ssms


AUDIT = '/lakehouse/default/Files/outputs/timing_audit.csv'
VIEWS_DIR = '/lakehouse/default/Files/views'

# Find the first 'error' view
err_name = None
with open(AUDIT, encoding='utf-8-sig') as f:
    for row in csv.DictReader(f):
        if row['status'] == 'error':
            err_name = row['view_name']
            break

if err_name is None:
    print("No 'error' rows in the audit -- nothing to diagnose.")
else:
    print(f"Diagnosing: {err_name}\n")

    # Locate the .sql file (case-insensitive prefix match)
    path = None
    for f in os.listdir(VIEWS_DIR):
        if f.lower().startswith(err_name.lower()) and f.lower().endswith('.sql'):
            path = os.path.join(VIEWS_DIR, f)
            break

    if path is None:
        print(f"  Could not locate file under {VIEWS_DIR}")
    else:
        raw = open(path, 'rb').read()
        for bom in (b'\xff\xfe', b'\xfe\xff', b'\xef\xbb\xbf'):
            if raw.startswith(bom):
                raw = raw[len(bom):]
                break
        try:
            text = raw.decode('utf-8', errors='replace')
        except Exception:
            text = raw.decode('utf-16-le', errors='replace')

        # Stage 1: which rules fire?
        _, fired = apply_all(text)
        print(f"Text rules fired: {fired or '(none)'}")

        # Stage 2: full preprocess
        clean, _ = preprocess_ssms(text)
        print(f"\nFirst 8 lines of cleaned SQL (literals -> '***', numbers -> N):")

        def _redact(s: str) -> str:
            s = re.sub(r"'(?:[^']|'')*'", "'***'", s)
            s = re.sub(r"\b\d+\b", "N", s)
            return s

        for i, line in enumerate(clean.splitlines()[:8], 1):
            print(f"  {i:3}: {_redact(line)}")

        # Stage 3: parse attempt
        print()
        try:
            sqlglot.parse_one(clean, dialect='tsql')
            print("PARSES OK -- if this view still shows error in the audit, "
                  "the audit ran with stale rules. Re-run Cell B.")
        except Exception as e:
            print(f"FAILS: {type(e).__name__}: {str(e)[:300]}")
