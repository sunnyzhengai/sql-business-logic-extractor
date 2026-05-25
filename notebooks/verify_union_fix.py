"""Fabric notebook cell -- bypass corpus.jsonl, parse one view fresh,
print the extractor's CTE sources directly.

When the user updates extract.py to fix a bug, then re-runs the
corpus extract, and the resulting matrix STILL shows the bug --
there are several places it could be stuck:

  a. extract.py didn't sync to OneLake.
  b. extract.py synced, but the corpus extract cell loaded the OLD
     version before the sync (module cache).
  c. extract.py + corpus extract worked correctly, but the validate
     orchestrator is reading a stale corpus.jsonl path.
  d. The fix is loaded everywhere but doesn't cover this specific
     SQL shape.

This cell isolates (a) and (d). It force-evicts the module cache,
re-imports the extractor fresh, parses ONE view file directly into
a logic dict (no corpus.jsonl in between), and prints the sources
per CTE. If the union-branch tables appear here, the fix is loaded
and the gap is somewhere downstream (b or c). If they don't,
either the fix isn't loaded OR the SQL shape is one the fix
doesn't catch.

Edit view_path to the failing view, run, paste the output back.
"""


# %% [Cell: verify union-branch fix on one view, bypassing corpus.jsonl]

import sys
import inspect

for mod in list(sys.modules):
    if mod.startswith('sql_logic_extractor') or mod.startswith('tools'):
        del sys.modules[mod]

from sql_logic_extractor.extract import SQLBusinessLogicExtractor, to_dict


# ---- EDIT THIS PATH to the view that's missing tables --------------------
view_path = "/lakehouse/default/Files/data/mychart_views/v_cchp_member.sql"
# ---------------------------------------------------------------------------


# Step 1: confirm the fix is in the loaded code. Looks for the
# sentinel comment from commit fc904a6.
src = inspect.getsource(SQLBusinessLogicExtractor._extract_set_operations)
fix_loaded = "Promote nested-union sources" in src or "logic.sources.extend(sub_logic.sources)" in src
print(f"Fix loaded?  {fix_loaded}")
print()

if not fix_loaded:
    print(
        ">>> The extract.py fix isn't in the loaded module. Either it didn't\n"
        ">>> sync to OneLake, or sys.modules wasn't cleared. Re-check the file\n"
        ">>> at the path your notebook imports `sql_logic_extractor` from."
    )

# Step 2: parse the view file directly into a logic dict.
sql = open(view_path, encoding="utf-8").read()
logic = to_dict(SQLBusinessLogicExtractor(dialect="tsql").extract(sql))

print(f"=== Top-level sources for {view_path.split('/')[-1]} ===")
top_sources = [s.get("name") for s in logic.get("sources") or []]
for s in top_sources:
    print(f"  {s}")
print(f"  ({len(top_sources)} table(s))")
print()

# Step 3: walk each CTE, print its sources + set_operations.
print(f"=== CTEs and their sources ===")
ctes = logic.get("ctes") or []
if not ctes:
    print("  (no CTEs in this view)")
for cte in ctes:
    name = cte.get("name") or "?"
    cte_logic = cte.get("logic") or {}
    sources = [s.get("name") for s in cte_logic.get("sources") or []]
    set_ops = cte_logic.get("set_operations") or []
    print(f"  CTE {name}:")
    print(f"    sources:        {sources}")
    print(f"    n_set_ops:      {len(set_ops)}")
    if set_ops:
        for i, op in enumerate(set_ops):
            print(f"    set_op[{i}] type: {op.get('type')}, "
                  f"branches: {len(op.get('branches') or [])}")
            for j, branch in enumerate(op.get('branches') or []):
                branch_sources = [s.get("name") for s in branch.get("sources") or []]
                print(f"      branch[{j}] sources: {branch_sources}")
    print()

print(
    "Interpretation:\n"
    "  - If the failing tables appear in 'CTE X: sources:' above,\n"
    "    the fix is loaded. Re-extract the corpus AGAIN (after this\n"
    "    cache drop) and re-run validate.\n"
    "  - If they appear ONLY in 'branch[N] sources' but NOT in the\n"
    "    CTE's top-level sources, the fix didn't merge them into the\n"
    "    parent. Module cache is stale OR the fix didn't sync.\n"
    "  - If they don't appear ANYWHERE, the extractor isn't seeing\n"
    "    them at all in this SQL shape -- different bug, share output."
)
