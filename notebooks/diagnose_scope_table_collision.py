"""Diagnose the "missing tables" bug: real FROM-clause tables get
silently demoted to CTE/scope references when their bare name collides
with a CTE / derived-scope name defined in the same view.

The data path that triggers it (graph_builder.py:185):

  scope.reads_from_tables == ['SomeTable', ...]   (resolver says: real table)
            |
            v
  bare_table_name('SomeTable')  ->  'SomeTable'
            |
            v
  'SomeTable' in scope_names_in_view  ->  True
            |
            v
  add REFERENCES_SCOPE edge   (NOT a READS_FROM_TABLE edge)
            |
            v
  view_to_tables() filter walks only READS_FROM_TABLE / JOIN / BELONGS_TO
            |
            v
  table is invisible to community matrix's table axis

`scope_names_in_view` is the set of bare scope IDs in the view (with
`cte:` / `derived:` / `union:0` / ... prefixes stripped). Any base
table whose name happens to match one of those bare scope names is
swallowed.

This notebook tests that hypothesis against ONE view in your
corpus.jsonl. Edit the two ALL-CAPS constants below, run the cell,
read the verdict.
"""


# %% [Cell: diagnose scope/table name collision for one view]

import json
import os

# ---- EDIT to point at YOUR setup ----------------------------------------
# Path to the freshly-extracted corpus that you just regenerated in step 2
# of the re-extract workflow. In Fabric this is typically:
#   /lakehouse/default/Files/outputs/corpus.jsonl
CORPUS_PATH = '/lakehouse/default/Files/outputs/corpus.jsonl'

# Name of the view whose table axis is missing entries.
VIEW_NAME = 'VW_REPLACE_ME'

# Bare names (no schema prefix) of the two (or more) tables that you
# expect to see in the matrix's table axis but don't. Compare without
# schema -- e.g., 'PAT_ENC', not 'Clarity.dbo.PAT_ENC'. Names are
# matched case-insensitively below.
MISSING_TABLES = ['TABLE_A', 'TABLE_B']
# -------------------------------------------------------------------------


# ---- Sanity: corpus file must exist and be non-empty --------------------
if not os.path.isfile(CORPUS_PATH):
    raise FileNotFoundError(
        f"corpus.jsonl not found at {CORPUS_PATH!r}. Did you re-extract "
        f"in step 2 of the workflow? Check the path or rerun extract."
    )

size = os.path.getsize(CORPUS_PATH)
print(f"corpus.jsonl found:  {CORPUS_PATH}")
print(f"size:                {size:,} bytes")
print()


# ---- Stream the corpus, find the view in question -----------------------
# corpus.jsonl format: first line is a header (schema_version / n_views),
# each subsequent line is one ViewV1 dict serialized to JSON.
view = None
with open(CORPUS_PATH, encoding='utf-8') as f:
    header = json.loads(next(f))
    print(f"schema_version:      {header.get('schema_version')}")
    print(f"n_views in corpus:   {header.get('n_views')}")
    print()
    for line in f:
        candidate = json.loads(line)
        if candidate.get('view_name') == VIEW_NAME:
            view = candidate
            break

if view is None:
    raise ValueError(
        f"view {VIEW_NAME!r} not found in corpus. Check spelling, or "
        f"verify the view was successfully extracted (it may have failed "
        f"parsing -- such views are absent from corpus.jsonl)."
    )

scopes = view.get('scopes') or []
print(f"view {VIEW_NAME!r}: {len(scopes)} scope(s)")
print()


# ---- Build the same scope-name set the graph builder uses ---------------
# graph_builder._collect_scope_names strips the 'cte:' / 'derived:' /
# 'union:0' / etc. prefix and keeps just the bare name. We mirror that
# logic exactly so the collision check produces the same result it
# would inside the real pipeline.
scope_bare_names = set()
for s in scopes:
    scope_id = s.get('id') or ''
    bare = scope_id.split(':')[-1].strip()
    if bare:
        scope_bare_names.add(bare)

print(f"Scope IDs in this view ({len(scopes)} total):")
for s in scopes:
    print(f"  - {s.get('id')!r}  kind={s.get('kind')!r}")
print()

print(f"Bare scope names the graph builder will use as the collision set:")
print(f"  {sorted(scope_bare_names)}")
print()


# ---- Also gather every reads_from_tables entry across all scopes --------
# Helps confirm the resolver actually surfaced the missing tables.
# Stage 3 (resolve) was supposed to do this; if a missing table is NOT
# in reads_from_tables anywhere, the bug is upstream of the graph
# builder (extract.py or resolve.py), not the collision hypothesis.
all_reads_from_tables = []
for s in scopes:
    for t in (s.get('reads_from_tables') or []):
        bare_t = t.split('.')[-1].strip()
        all_reads_from_tables.append((s.get('id'), t, bare_t))

print(f"All reads_from_tables entries across all scopes ({len(all_reads_from_tables)} total):")
for scope_id, raw, bare in all_reads_from_tables:
    print(f"  {scope_id:30s}  raw={raw!r:40s}  bare={bare!r}")
print()


# ---- Per-missing-table verdict ------------------------------------------
# Two questions per table:
#   Q1: Does Stage 3 (resolve) actually list this table in any scope's
#       reads_from_tables?  If NO -> upstream bug, the collision
#       hypothesis is wrong.
#   Q2: Does the bare name collide with a scope name in this view?
#       If YES -> this IS the graph_builder demotion bug.
print("=" * 72)
print(f"Verdict for {VIEW_NAME!r}:")
print("=" * 72)

bare_lower = {b.lower() for _, _, b in all_reads_from_tables}
scope_bare_lower = {n.lower() for n in scope_bare_names}

for missing in MISSING_TABLES:
    m_lower = missing.lower()
    in_reads_from = m_lower in bare_lower
    collides = m_lower in scope_bare_lower

    print()
    print(f"  Missing table: {missing!r}")
    print(f"    Q1: in some scope's reads_from_tables? "
          f"{'YES' if in_reads_from else 'NO'}")
    print(f"    Q2: collides with a CTE/derived scope name? "
          f"{'YES' if collides else 'NO'}")

    if not in_reads_from:
        print(f"    -> Upstream bug. Stage 3 (resolve) didn't surface this "
              f"table. The graph builder never had a chance to demote it. "
              f"Run trace_one_view.py on {VIEW_NAME!r} to find which stage "
              f"is dropping it.")
    elif collides:
        print(f"    -> CONFIRMED: graph builder demotion bug. The table "
              f"is in reads_from_tables, but its bare name matches a "
              f"scope in this view, so graph_builder.py:185 silently "
              f"converts the READS_FROM_TABLE edge to REFERENCES_SCOPE. "
              f"view_to_tables() then can't see it -> missing from "
              f"matrix table axis.")
    else:
        print(f"    -> Different bug. Table is in reads_from_tables and "
              f"does NOT collide with a scope name. Suspect a downstream "
              f"filter: tools.p50_present.community_matrix._is_real_"
              f"table_name / _is_unresolved_view_reference / the "
              f"DEFAULT_TABLE_SKIP_LIST. Test each filter directly with "
              f"the bare name.")

print()
print("=" * 72)
