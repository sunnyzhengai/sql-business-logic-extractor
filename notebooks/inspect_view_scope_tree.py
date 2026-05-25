"""Fabric notebook helper -- dump the scope tree of ONE view from
corpus.jsonl so we can diagnose extraction gaps.

When a community-table list is missing tables that the SQL clearly
reads (e.g. UNION branches inside a CTE), the question is: did the
corpus extractor capture them? This cell prints every scope of a
named view with its kind, reads_from_tables, and reads_from_scopes.

If a UNION's branch tables show up in some scope -> graph builder
issue. If they're absent everywhere -> extractor issue. Run on one
problematic view; the output bounds where the fix belongs.

Usage in a notebook cell: edit view_name, then run.
"""


# %% [Cell: dump scope tree for one view]

import json


# ---- EDIT these to point at YOUR corpus + the view you're inspecting ----
corpus_path = "/lakehouse/default/Files/outputs/corpus.jsonl"
view_name   = "v_cchp_member"   # whichever view has the missing tables
# -------------------------------------------------------------------------


# Find the view entry in corpus.jsonl. First line is the header; the
# rest are one ViewV1 per line.
view_entry = None
with open(corpus_path, encoding="utf-8") as fh:
    for line in fh:
        line = line.strip()
        if not line:
            continue
        obj = json.loads(line)
        # Skip header records (have a "schema_version" key, not a "view_name").
        if "schema_version" in obj and "view_name" not in obj:
            continue
        if obj.get("view_name") == view_name:
            view_entry = obj
            break

if view_entry is None:
    print(f"View not found: {view_name}")
    print("Available view names (first 20):")
    with open(corpus_path, encoding="utf-8") as fh:
        names = []
        for line in fh:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if "view_name" in obj:
                names.append(obj["view_name"])
            if len(names) >= 20:
                break
    for n in names:
        print(f"  {n}")
else:
    print(f"=== Scope tree for {view_name} ===")
    print(f"Number of scopes: {len(view_entry.get('scopes') or [])}\n")

    for scope in view_entry.get("scopes") or []:
        sid = scope.get("id") or "?"
        kind = scope.get("kind") or "?"
        rft = list(scope.get("reads_from_tables") or [])
        rfs = list(scope.get("reads_from_scopes") or [])
        n_cols = len(scope.get("columns") or [])
        n_filters = len(scope.get("filters") or [])
        n_joins = len(scope.get("joins") or [])

        print(f"  scope id: {sid!r}")
        print(f"    kind:               {kind}")
        print(f"    reads_from_tables:  {rft}")
        print(f"    reads_from_scopes:  {rfs}")
        print(f"    columns: {n_cols},  filters: {n_filters},  joins: {n_joins}")
        print()

    # Quick verdict.
    all_tables_across_scopes: set[str] = set()
    for scope in view_entry.get("scopes") or []:
        all_tables_across_scopes.update(scope.get("reads_from_tables") or [])
    print(f"--- Union of all reads_from_tables across ALL scopes ---")
    for t in sorted(all_tables_across_scopes):
        print(f"  {t}")
    print()
    print(
        "If a table you see in the SQL is MISSING from the union above,\n"
        "the extractor isn't capturing it -- the gap is in extract.py.\n"
        "If a table is in the union above but missing from the matrix,\n"
        "the gap is in the graph builder or matrix renderer."
    )
