"""Fabric notebook helper -- run preprocess_ssms on ONE file with a
GUARANTEED-FRESH import and print the cleaned SQL line-by-line.

When you've synced a fix to sql_logic_extractor/resolve.py but the
parse error still shows the same content, this cell tells you whether
the fix is actually reaching the file in question or whether the
fix doesn't match this file's shape.

Edit `view_path` to point at one of the failing views, run, paste
back the first 20 lines of cleaned output.
"""


# %% [Cell: inspect preprocess_ssms output on one specific file]

import sys

# Force-reload the module so any stale import in another cell can't
# serve us old code. Both `sql_logic_extractor` AND any tool that
# imported `preprocess_ssms` need to be evicted -- otherwise their
# bound reference to the old function survives.
for mod in list(sys.modules):
    if mod.startswith('sql_logic_extractor') or mod.startswith('tools'):
        del sys.modules[mod]

import sqlglot
from sql_logic_extractor.resolve import preprocess_ssms


# ---- EDIT THIS PATH to one failing view -----------------------------------
view_path = f"{REPO_ROOT}/data/mychart_views/V_CCHCS_DXP_HP_Mychart_PBI.sql"
# ----------------------------------------------------------------------------


# Verify the fix is loaded -- look for the sentinel string from a4e38bc.
import inspect
src = inspect.getsource(preprocess_ssms)
print(f"Fix loaded?  {'any(l.strip()' in src}")
print()

# Read the file.
raw = open(view_path, encoding='utf-8').read()
print(f"Raw SQL: {len(raw)} chars, {raw.count(chr(10)) + 1} lines")
print()

# Run preprocess.
cleaned, meta = preprocess_ssms(raw)

print(f"=== Metadata extracted ===")
for k, v in meta.items():
    print(f"  {k}: {v}")
print()

print(f"=== First 30 lines of CLEANED SQL ===")
for i, line in enumerate(cleaned.split('\n')[:30], 1):
    # Use repr() so leading/trailing whitespace is visible.
    print(f"  {i:3d}: {line!r}")
print()

print(f"=== sqlglot.parse(dialect='tsql') verdict ===")
try:
    sqlglot.parse(cleaned, dialect='tsql')
    print("OK -- this view parses cleanly. The error is in some OTHER cell")
    print("     that's using a cached/stale preprocess_ssms.")
except Exception as e:
    print(f"FAIL: {type(e).__name__}: {e}")
    print("     The fix didn't catch this file's specific shape.")
    print("     Paste the first 30 lines printed above so we can adjust.")
