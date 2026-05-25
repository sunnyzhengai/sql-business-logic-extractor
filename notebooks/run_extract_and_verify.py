"""Run extract_corpus and verify the result IN THE SAME CELL.

Yang's saga: probe writes succeed, streaming writes succeed, but
extract_corpus claims it wrote corpus_v2.jsonl and the file isn't on
disk. Three things to rule out:

  1. An exception swallowed somewhere in extract that lets the
     "written to" print run anyway.
  2. A path-resolution discrepancy between the call site and the
     check (different working directories, symlinks, mount aliases).
  3. The file IS being written, just to a different directory than
     expected.

This cell runs extract_corpus + verifies + lists the outputs/ dir
all in one place, so the truth surfaces before any portal-refresh
or path-typo confusion intervenes.
"""


# %% [Cell: run extract + verify in one place]

import sys
import os
import datetime

# Drop module cache so we're definitely on the latest batch.py
for mod in list(sys.modules):
    if mod.startswith('sql_logic_extractor') or mod.startswith('tools'):
        del sys.modules[mod]


# ---- EDIT IF DIFFERENT --------------------------------------------------
INPUT_DIR   = "/lakehouse/default/Files/data/mychart_views/"
CORPUS_PATH = "/lakehouse/default/Files/outputs/corpus_v3.jsonl"   # fresh name
OUTPUTS_DIR = "/lakehouse/default/Files/outputs"
# -------------------------------------------------------------------------


# Step 1: confirm what's in outputs/ BEFORE we run.
print(f"=== {OUTPUTS_DIR} contents BEFORE ===")
before = set(os.listdir(OUTPUTS_DIR)) if os.path.isdir(OUTPUTS_DIR) else set()
for f in sorted(before):
    print(f"  {f}")
print()

# Step 2: confirm the batch.py we're about to use.
import inspect
from tools.p10_extract.batch import extract_corpus
src = inspect.getsource(extract_corpus)
# Look for the staging sentinel from the BROKEN commit (0c4cc81) to
# detect a stale batch.py sync. The reverted (35531a1) version has
# no `_stage_via_tmp` variable.
stage_logic_present = "_stage_via_tmp" in src
print(f"batch.py has the (broken) staging logic? {stage_logic_present}")
if stage_logic_present:
    print("  >>> You're on the buggy version. Re-sync tools/p10_extract/batch.py")
    print("  >>> from GitHub (commit 35531a1 or later).")
print()

# Step 3: run extract + capture the return value.
print(f"=== Running extract_corpus -> {CORPUS_PATH} ===")
result = extract_corpus(
    input_dir=INPUT_DIR,
    output_path=CORPUS_PATH,
    dialect="tsql",
)
print(f"  extract_corpus returned: {result!r}")
print()

# Step 4: immediately verify the file landed.
print(f"=== Verification ===")
print(f"  path:    {CORPUS_PATH}")
print(f"  exists?  {os.path.exists(CORPUS_PATH)}")
if os.path.exists(CORPUS_PATH):
    print(f"  size:    {os.path.getsize(CORPUS_PATH)} bytes")
    print(f"  mtime:   {datetime.datetime.fromtimestamp(os.path.getmtime(CORPUS_PATH))}")
    # Peek at the first 2 lines so we know the content is valid JSONL.
    print(f"  first 2 lines:")
    with open(CORPUS_PATH) as fh:
        for i, line in enumerate(fh):
            if i >= 2:
                break
            print(f"    {line.rstrip()[:160]}")
else:
    print(f"  >>> FILE DOES NOT EXIST. extract_corpus returned without raising,")
    print(f"  >>> but the file isn't on disk. Something is silently swallowing")
    print(f"  >>> the write or the path resolves elsewhere.")
print()

# Step 5: full listing AFTER. Diff against BEFORE to see what got added.
print(f"=== {OUTPUTS_DIR} contents AFTER ===")
after = set(os.listdir(OUTPUTS_DIR))
new_files = after - before
gone_files = before - after
for f in sorted(after):
    full = os.path.join(OUTPUTS_DIR, f)
    try:
        size = os.path.getsize(full)
        mtime = datetime.datetime.fromtimestamp(os.path.getmtime(full))
        marker = " <-- NEW" if f in new_files else ""
        print(f"  {f:50s}  {size:>10d} bytes  {mtime}{marker}")
    except Exception as e:
        print(f"  {f:50s}  ERROR: {e}")
if gone_files:
    print(f"\nFiles that DISAPPEARED during this run: {sorted(gone_files)}")

# Step 6: also check if the file landed in /tmp (in case staging is still
# active or working_dir is unexpected).
print()
print(f"=== /tmp scan for any corpus or extract artifacts ===")
for f in sorted(os.listdir("/tmp")):
    if "corpus" in f.lower() or "extract" in f.lower() or f.endswith(".jsonl"):
        full = os.path.join("/tmp", f)
        try:
            size = os.path.getsize(full)
            mtime = datetime.datetime.fromtimestamp(os.path.getmtime(full))
            print(f"  /tmp/{f:50s}  {size:>10d} bytes  {mtime}")
        except Exception as e:
            print(f"  /tmp/{f:50s}  ERROR: {e}")
