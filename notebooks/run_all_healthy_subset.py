"""Fabric notebook helper -- run all 4 tools on the views that
timing_audit marked 'ok'. Skips error/timeout views so the run
completes predictably.

Workflow:
  1. timing_audit produces /lakehouse/.../outputs/timing_audit.csv with
     per-view status (ok / timeout / error).
  2. THIS cell builds a /lakehouse/.../views_healthy/ folder containing
     ONLY the 'ok' views, then runs run_all on it.
  3. The error/timeout views stay in the original /views/ folder for
     separate investigation (re-export from SSMS, parsing-rule work,
     etc.). You're not blocked from getting production output.

Each `# %%` block is one notebook cell.
"""


# %% [Cell A: copy 'ok' views to a subset folder + run all 4 tools on it]

import sys

# Drop cached modules so the latest uploaded code takes effect
for mod in list(sys.modules):
    if mod.startswith('sql_logic_extractor') or mod.startswith('tools'):
        del sys.modules[mod]

import csv
import os
import shutil
import time

# EDIT these if your paths differ
SOURCE_VIEWS = '/lakehouse/default/Files/views'
HEALTHY_VIEWS = '/lakehouse/default/Files/views_healthy'
AUDIT_CSV = '/lakehouse/default/Files/outputs/timing_audit.csv'
OUTPUT_DIR = '/lakehouse/default/Files/outputs/run_all_healthy'
SCHEMA = '/lakehouse/default/Files/schemas/clarity_schema.json'

# Build a folder containing ONLY views that timing_audit marked 'ok'
os.makedirs(HEALTHY_VIEWS, exist_ok=True)
for f in os.listdir(HEALTHY_VIEWS):
    os.remove(os.path.join(HEALTHY_VIEWS, f))   # clear any prior copy

# Map source filenames once (case-insensitive)
src_files = {f.lower(): f for f in os.listdir(SOURCE_VIEWS) if f.lower().endswith('.sql')}

ok_count = 0
with open(AUDIT_CSV, encoding='utf-8-sig') as f:
    for row in csv.DictReader(f):
        if row['status'] != 'ok':
            continue
        name = (row['view_name'] + '.sql').lower()
        actual = src_files.get(name) or next(
            (v for k, v in src_files.items() if k.startswith(row['view_name'].lower())),
            None,
        )
        if actual:
            shutil.copy(os.path.join(SOURCE_VIEWS, actual),
                        os.path.join(HEALTHY_VIEWS, actual))
            ok_count += 1

print(f"Copied {ok_count} healthy views to {HEALTHY_VIEWS}\n")

# Run all 4 tools on the healthy subset
t0 = time.time()
from tools.batch_all import run_all
run_all(
    input_dir=HEALTHY_VIEWS,
    output_dir=OUTPUT_DIR,
    schema_path=SCHEMA,
    use_llm=False,
    dialect='tsql',
)
print(f"\nrun_all elapsed: {time.time() - t0:.1f}s")
print(f"Outputs:           {OUTPUT_DIR}")
print(f"Per-view progress: {OUTPUT_DIR}/run_all_progress.txt")


# %% [Cell B (optional): monitor progress from a SECOND notebook]
#
# Run this in a SEPARATE notebook (not the one running run_all). The
# notebook running run_all blocks its own kernel, so progress can't be
# polled from the same notebook.
#
# To monitor: open another notebook in the workspace, attach the same
# lakehouse, and run this cell. It prints the current state of the
# progress log.

p = '/lakehouse/default/Files/outputs/run_all_healthy/run_all_progress.txt'
import os
if os.path.isfile(p):
    print(open(p).read())
else:
    print(f"No progress file yet at {p}")
