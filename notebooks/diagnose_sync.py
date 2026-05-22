"""Fabric notebook -- diagnose "I synced files but graphs didn't change."

If you've updated files from GitHub and re-run the pipeline but the
output HTMLs look the same as before, run this in your Fabric notebook
to pinpoint where the chain is broken. Each `# %%` block is one cell.

This script answers three questions:

  1. Is Python actually loading the file you synced?
     (Or is it finding an OLDER copy in a different folder?)

  2. Does the file Python loaded contain the new code?
     (Or did the sync save a stale / partial version?)

  3. Did the orchestrator's output files actually use the new code?
     (Or is Cell 8 writing to a different output_dir than you're
     viewing, or the browser caching an old HTML?)

If all three pass: the files and code are correct -- your browser is
showing a cached version. Force-reload the HTML (Ctrl+Shift+R or
Cmd+Shift+R) or open it in a new tab.
"""


# %% [Cell 1: sys.path + module-cache reset]
#
# Same prep as Cell 3 of run_graph_pipeline_fabric.py. Run this BEFORE
# the diagnostic cell so we're inspecting freshly-loaded modules.

import sys

REPO_ROOT = '/lakehouse/default/Files'   # or '/lakehouse/default/Files/sql-business-logic-extractor'
                                          # if you have the wrapped-folder layout
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

for mod in list(sys.modules):
    if mod.startswith('tools') or mod.startswith('sql_logic_extractor'):
        del sys.modules[mod]

print("Module cache cleared.")


# %% [Cell 2: which file is Python loading, and does it have the new functions?]

import os

import tools.p50_present.community_html as ch
import tools.p50_present.view_html as vh

print("=== Which files is Python loading? ===")
print(f"community_html: {ch.__file__}")
print(f"view_html:      {vh.__file__}")
print()

print("=== Does community_html have the Phase 3d functions? ===")
expected = ['inject_legend', 'inject_views_sidebar', 'inject_subgraph_isolation_script',
            'render_community_html', 'render_overview_html']
for fn in expected:
    print(f"  has {fn}? {hasattr(ch, fn)}")
print()

print("=== File sizes (sanity check) ===")
print(f"  community_html.py size: {os.path.getsize(ch.__file__):,} bytes  (expect ~30,000+)")
print(f"  view_html.py     size: {os.path.getsize(vh.__file__):,} bytes  (expect ~10,000+)")
print()

# Triage:
#   - inject_legend / inject_views_sidebar BOTH False?
#     -> Python is loading an OLD community_html.py. Either you have a
#        duplicate copy elsewhere on sys.path, or your sync overwrote
#        the wrong file. Compare the path printed above against where
#        you saved the new file from GitHub.
#
#   - Files load from where you expect, functions exist?
#     -> Code is fine; proceed to Cell 3 to check the output files.


# %% [Cell 3: did the orchestrator's output files actually use the new code?]
#
# Change OUTPUT_DIR to whatever you passed to run_validation in Cell 8.

import os

OUTPUT_DIR = '/lakehouse/default/Files/outputs/graph_pivot_validation'

comm_dir = os.path.join(OUTPUT_DIR, 'communities')
if not os.path.isdir(comm_dir):
    print(f"NOT FOUND: {comm_dir}")
    print("Cell 8 may have written to a different output_dir, or didn't run successfully.")
else:
    htmls = sorted(
        f for f in os.listdir(comm_dir)
        if f.endswith('.html') and f.startswith('community_')
    )
    print(f"Found {len(htmls)} per-community HTMLs in {comm_dir}")
    print()
    print("Phase 3 markers (each should be True on rerun with the new code):")
    print()
    print(f"{'file':<60} {'legend':>8} {'sidebar':>10} {'isolation':>11}")
    print("-" * 95)
    for name in htmls[:8]:
        p = os.path.join(comm_dir, name)
        content = open(p, encoding='utf-8').read()
        print(f"{name:<60}"
              f" {('legend-injected' in content):>8}"
              f" {('views-sidebar-injected' in content):>10}"
              f" {('subgraph-isolation-injected' in content):>11}")
    if len(htmls) > 8:
        print(f"...({len(htmls) - 8} more)")

# Triage:
#   - All three columns True?
#     -> The HTMLs are correct. Open one in a NEW browser tab (or
#        force-reload with Ctrl+Shift+R) to bypass the cached version.
#
#   - One or more False?
#     -> Cell 8 used the old code or wrote to a different output_dir.
#        (a) Confirm output_dir in your Cell 8 matches OUTPUT_DIR above.
#        (b) Re-run Cell 1 (this script's cell 1) to ensure module cache
#            is clear, then re-run Cell 8 of the pipeline notebook.
#
#   - "NOT FOUND" for the communities directory?
#     -> The pipeline didn't write here. Check the output_dir you
#        passed to run_validation, and confirm Cell 8 ran without errors.
