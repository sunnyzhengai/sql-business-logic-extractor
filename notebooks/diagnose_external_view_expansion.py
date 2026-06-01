"""Diagnose why view-of-view inline expansion isn't firing.

When a consuming view's panel doesn't show the foundation views it
references (no `View: V_FOO` clusters appearing in
community_NN_*_shapes.html), the typical causes are:

  1. validate_graph_pivot.py wasn't synced -- the local copy
     doesn't call load_external_views, so no external lookup is
     passed to write_community_shapes, and inline expansion can
     never fire.
  2. The data/views_reporting / data/views_cookrpt folders aren't
     at the path the resolver expects (Path.cwd() varies across
     Fabric notebook setups).
  3. Folders exist but contain zero .sql files.
  4. Files exist but all fail to parse (rare; the resolver tolerates
     parse failures silently to avoid breaking the batch).
  5. View names don't normalize to the same bare key between the
     corpus and the external lookup (e.g., `Reporting.V_FOO.View`
     in corpus vs. `V_FOO.sql` filename).

This notebook walks each step and prints what would otherwise be
silent. Run it AFTER syncing the recursive-expansion commit's three
files (view_resolver.py, view_shape.py, validate_graph_pivot.py) but
BEFORE running run_validation, to confirm load_external_views works
in your workspace.
"""


# %% [Cell: diagnose external view expansion]

import os
import sys
from pathlib import Path

# Force-reload Python modules so freshly-synced code takes effect.
# Without this, Python's import cache may keep the previous version.
for mod in list(sys.modules):
    if mod.startswith("sql_logic_extractor") or mod.startswith("tools"):
        del sys.modules[mod]

from tools.operate.views_corpus_config import (
    VIEW_SOURCE_DIRS,
    resolve_view_source_dirs,
    find_sql_files,
)
from tools.operate.view_resolver import load_external_views

print("=" * 72)
print("External-view expansion diagnostic")
print("=" * 72)

# Step 1: where is the notebook running from? VIEW_SOURCE_DIRS are
# RELATIVE paths -- if cwd doesn't sit at the repo root, the
# resolver looks in the wrong place.
print()
print(f"[1] cwd:                  {os.getcwd()}")
print(f"[1] VIEW_SOURCE_DIRS:     {VIEW_SOURCE_DIRS}")

# Step 2: where does each configured dir resolve to, and does it
# exist on the filesystem?
print()
print("[2] resolved source dirs:")
for p in resolve_view_source_dirs():
    exists = p.is_dir()
    print(f"      {p}")
    print(f"        exists: {exists}")
    if exists:
        sql_count = len(list(p.glob('*.sql')))
        print(f"        .sql files in this folder: {sql_count}")

# Step 3: how many .sql files does find_sql_files actually pick up?
# Use this to confirm step-2 file counts add up.
sql_files = find_sql_files()
print()
print(f"[3] total .sql files found by find_sql_files: {len(sql_files)}")
if sql_files:
    print(f"    first few:")
    for p in sql_files[:5]:
        print(f"      {p.name}")
    if len(sql_files) > 5:
        print(f"      ... and {len(sql_files) - 5} more")

# Step 4: load_external_views in verbose mode -- prints each
# parse failure to stderr so we can see what (if anything) is
# being silently dropped.
print()
print("[4] load_external_views (verbose=True; failures print to stderr)...")
ext = load_external_views(verbose=True)
print(f"    -> {len(ext)} view(s) loaded into external_view_lookup")
if ext:
    print(f"    sample view names (first 10):")
    for name in list(ext)[:10]:
        n_scopes = len(ext[name].get("scopes") or [])
        print(f"      {name}  (scopes={n_scopes})")

# Step 5: name-matching diagnostic. Build a sample foreign_view_lookup
# and check what bare keys it produces. This is what view-of-view
# detection uses internally to match `FROM SomeView` references.
print()
print("[5] sample bare-key normalization for the first 5 loaded views:")
from tools.p50_present.view_shape import _bare_view_key
for name in list(ext)[:5]:
    bare = _bare_view_key(name)
    print(f"      {name!r:60s} -> bare key {bare!r}")

print()
print("=" * 72)
print("Verdict:")
print("=" * 72)

if not any(p.is_dir() for p in resolve_view_source_dirs()):
    print(
        "  None of the configured source folders exist at the resolved\n"
        "  cwd-relative paths. Common in Fabric notebooks where\n"
        "  Path.cwd() doesn't point at the repo root.\n"
        "\n"
        "  Fix: pass absolute paths via view_source_dirs. Two options:\n"
        "\n"
        "  (a) For ad-hoc testing, call load_external_views directly:\n"
        "        ext = load_external_views(view_source_dirs=[\n"
        "          '/lakehouse/default/Files/views_reporting',\n"
        "          '/lakehouse/default/Files/views_cookrpt',\n"
        "        ], verbose=True)\n"
        "      Replace the paths with wherever you uploaded the .sql\n"
        "      files in Fabric.\n"
        "\n"
        "  (b) For the production run, pass view_source_dirs to\n"
        "      run_validation:\n"
        "        result = run_validation(\n"
        "          corpus_path=..., output_dir=...,\n"
        "          view_source_dirs=[\n"
        "            '/lakehouse/default/Files/views_reporting',\n"
        "            '/lakehouse/default/Files/views_cookrpt',\n"
        "          ],\n"
        "        )\n"
        "\n"
        "  Don't know where Fabric put them? Run this to search:\n"
        "    import subprocess\n"
        "    for needle in ('views_reporting', 'views_cookrpt'):\n"
        "        r = subprocess.run(\n"
        "          ['find', '/lakehouse/default', '-type', 'd',\n"
        "            '-name', needle],\n"
        "          capture_output=True, text=True, timeout=30)\n"
        "        print(r.stdout or f'(none for {needle})')"
    )
elif not sql_files:
    print(
        "  Folders exist but contain no .sql files. Double-check the\n"
        "  file extension (must be lowercase '.sql') and that the\n"
        "  files are at the top level of the folder (find_sql_files\n"
        "  does NOT recurse into subfolders -- by design)."
    )
elif not ext:
    print(
        "  Files exist but all failed to parse. Scroll up to see\n"
        "  individual failure reasons printed by verbose=True. Common\n"
        "  causes: SSMS preamble that preprocess_ssms didn't catch, or\n"
        "  T-SQL constructs sqlglot rejects in 'tsql' dialect."
    )
else:
    print(
        f"  external_view_lookup populated with {len(ext)} view(s). If\n"
        f"  run_validation still doesn't show inline expansion in your\n"
        f"  community_shapes/ output, the most likely remaining cause is:\n"
        f"  validate_graph_pivot.py wasn't synced (the local copy doesn't\n"
        f"  call load_external_views or doesn't pass external_view_lookup\n"
        f"  to write_community_shapes). Verify the file's contents include\n"
        f"  'from tools.operate.view_resolver import load_external_views'\n"
        f"  near the top."
    )
print("=" * 72)
