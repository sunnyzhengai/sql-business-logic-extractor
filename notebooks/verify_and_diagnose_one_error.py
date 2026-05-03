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

        def _redact(s: str) -> str:
            s = re.sub(r"'(?:[^']|'')*'", "'***'", s)
            s = re.sub(r"\b\d+\b", "N", s)
            return s

        # File-level stats so we can tell if the file is truncated.
        all_lines = text.splitlines()
        print(f"File has {len(all_lines)} total lines, {len(text)} chars\n")

        # Stage 1a: RAW SQL (first 40 lines, redacted).
        print("--- RAW SQL (first 40 lines, redacted) ---")
        for i, line in enumerate(all_lines[:40], 1):
            print(f"  {i:3}: {_redact(line)}")
        if len(all_lines) > 40:
            print(f"  ... ({len(all_lines) - 40} more lines)")

        # Stage 1b: Anchor search -- find every line that COULD anchor
        # preprocess_ssms's body_started flag. If this list is empty, we
        # know why clean_sql ended up empty.
        anchor_re = re.compile(r"^\s*(CREATE\s+(?:OR\s+ALTER\s+)?|ALTER\s+)?"
                                  r"(VIEW|PROCEDURE|PROC|FUNCTION|SELECT|WITH|AS\s*$)",
                                  re.IGNORECASE)
        print("\n--- Anchor lines (CREATE/ALTER/SELECT/WITH/standalone AS) ---")
        anchor_hits = []
        for i, line in enumerate(all_lines, 1):
            if anchor_re.match(line):
                anchor_hits.append((i, line))
                print(f"  L{i:>4}: {_redact(line)}")
        if not anchor_hits:
            print("  (NONE -- this view has no CREATE/SELECT/WITH/standalone AS")
            print("   line. preprocess_ssms drops the whole file as a result.)")
            # Hidden-character dump: scan for lines that LOOK like they should
            # be anchors (contain CREATE/SELECT/WITH/AS as substrings, even if
            # the full anchor regex didn't match) and print their character
            # codes. Catches BOMs, non-breaking spaces, zero-width chars, and
            # other invisible junk that prevents the anchor match.
            print("\n  Hidden-character dump for suspected anchor lines:")
            keywords = ("CREATE", "SELECT", "WITH ", "FROM ", "  AS ", " AS\n")
            shown = 0
            for i, line in enumerate(all_lines, 1):
                line_upper = line.upper()
                if any(kw in line_upper for kw in keywords):
                    codes = " ".join(f"U+{ord(c):04X}" for c in line[:20])
                    print(f"    L{i:>4} repr: {line[:80]!r}")
                    print(f"           codes (first 20 chars): {codes}")
                    shown += 1
                    if shown >= 5:
                        break
            if shown == 0:
                print("    (no line contains any SQL keyword -- file is genuinely")
                print("     not SQL. Probably mangled by the export pipeline.)")

        # Stage 2: which rules fire?
        _, fired = apply_all(text)
        print(f"\nText rules fired: {fired or '(none)'}")

        # Stage 3: full preprocess + cleaned-SQL output
        clean, _ = preprocess_ssms(text)
        cleaned_lines = clean.splitlines()
        print(f"\n--- CLEANED SQL (post preprocess_ssms): "
              f"{len(cleaned_lines)} lines, {len(clean)} chars ---")
        if not clean.strip():
            print("  (EMPTY -- preprocess_ssms dropped everything. The line-by-")
            print("   line code never found a 'body start' anchor: CREATE VIEW")
            print("   line didn't match the engine's create-match regex AND no")
            print("   line began with SELECT or WITH at column 0.)")
        else:
            for i, line in enumerate(cleaned_lines[:20], 1):
                print(f"  {i:3}: {_redact(line)}")

        # Stage 4: parse attempt
        print()
        try:
            sqlglot.parse_one(clean, dialect='tsql')
            print("PARSES OK -- if this view still shows error in the audit, "
                  "the audit ran with stale rules. Re-run Cell B.")
        except Exception as e:
            print(f"FAILS: {type(e).__name__}: {str(e)[:300]}")
