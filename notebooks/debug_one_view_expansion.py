"""Debug why a specific view's view-of-view references aren't
inline-expanding. Run when probe_external_view_paths.py confirms
the path + name normalization are set up correctly but a particular
view's CTE / subquery reference still shows as a placeholder
without an expanded cluster.

The notebook walks the full pipeline for ONE view:

  1. Load the corpus, find the host view, dump its raw scopes
     (reads_from_tables / reads_from_scopes / joins) -- so we
     can see what the extractor actually captured for the view.
  2. Load external_view_lookup from the working absolute paths
     and report the bare-key map.
  3. For each reads_from_tables / join.right_table entry across
     ALL the host's scopes, normalize via _bare_view_key and
     report whether it would hit the external lookup. This is
     where you spot 'the reference looks like FOO but the file
     stem is FOOBAR' kind of mismatches.
  4. Run build_view_shape on the host with external_view_lookup
     and walk every scope_ref placeholder. For each placeholder,
     print:
       - the placeholder's table / target_view_name
       - whether target_view_name is in external_view_lookup
       - whether the inline-expansion ran (i.e., did any
         `external:{target}/...` scope appear in shape.scopes?)
  5. Final verdict block names which step failed for each
     unresolved placeholder.

Run AFTER syncing commit 93bea31 (view_resolver gains
view_source_dirs param) and confirming probe_external_view_paths
shows working paths. Tweak the three constants at the top of
Cell 1 (HOST_VIEW_NAME, CORPUS_PATH, VIEW_SOURCE_DIRS) for your
setup.
"""


# %% [Cell 1: load corpus + find the host view + dump its scopes]

import sys
import json
from pathlib import Path

for mod in list(sys.modules):
    if mod.startswith("sql_logic_extractor") or mod.startswith("tools"):
        del sys.modules[mod]

# ---- EDIT to point at YOUR setup ---------------------------------
HOST_VIEW_NAME = "REPLACE_ME"  # the consumer view that has the unexpanded ref
CORPUS_PATH = "/lakehouse/default/Files/outputs/corpus.jsonl"
VIEW_SOURCE_DIRS = [
    "/lakehouse/default/Files/data/views_reporting",
    "/lakehouse/default/Files/data/views_cookrpt",
]
# ------------------------------------------------------------------

corpus_path = Path(CORPUS_PATH)

# Look-up the host in TWO places:
#   1. The corpus.jsonl (pre-extracted views -- the typical case)
#   2. The view_source_dirs folders (foundation views NOT in the
#      corpus -- e.g., when the corpus is the 11-view pilot but
#      the view you're debugging is one of the 196 foundation
#      views from data/views_*).
# If found in (2), we parse the SQL file on-demand into a ViewV1
# dict using the same view_resolver code used at validation time.
host_view: dict | None = None
host_source: str = ""

if corpus_path.is_file():
    with corpus_path.open(encoding="utf-8") as f:
        next(f)  # header line
        for line in f:
            v = json.loads(line)
            if v.get("view_name") == HOST_VIEW_NAME:
                host_view = v
                host_source = f"corpus ({CORPUS_PATH})"
                break

if host_view is None:
    # Fall back: scan the foundation folders for {HOST_VIEW_NAME}.sql
    # and parse it on-demand.
    from tools.operate.view_resolver import parse_view_for_shape
    for d in VIEW_SOURCE_DIRS:
        candidate = Path(d) / f"{HOST_VIEW_NAME}.sql"
        if candidate.is_file():
            host_view = parse_view_for_shape(candidate)
            if host_view is not None:
                host_source = f"foundation file ({candidate})"
                break

if host_view is None:
    print(f"Host view {HOST_VIEW_NAME!r} not found in either:")
    print(f"  - corpus: {CORPUS_PATH}")
    for d in VIEW_SOURCE_DIRS:
        print(f"  - foundation: {d}/{HOST_VIEW_NAME}.sql")
    print()
    if corpus_path.is_file():
        print("Available view names in the corpus:")
        with corpus_path.open(encoding="utf-8") as f:
            next(f)
            for line in f:
                print(f"  {json.loads(line).get('view_name')!r}")
        print()
    # Also list what's in the foundation folders (first 30 of each).
    for d in VIEW_SOURCE_DIRS:
        dp = Path(d)
        if dp.is_dir():
            files = sorted(dp.glob("*.sql"))
            print(f"Available .sql files in {d} "
                  f"({len(files)} total, first 30):")
            for f in files[:30]:
                print(f"  {f.stem!r}")
            print()
    raise SystemExit

print(f"Host view loaded from: {host_source}")
print()

print("=" * 72)
print(f"[1] Host view {HOST_VIEW_NAME!r} -- raw corpus scopes")
print("=" * 72)
for s in host_view.get("scopes") or []:
    print(f"  scope id={s.get('id')!r}  kind={s.get('kind')!r}")
    rft = s.get("reads_from_tables") or []
    rfs = s.get("reads_from_scopes") or []
    print(f"    reads_from_tables ({len(rft)}): {rft}")
    print(f"    reads_from_scopes ({len(rfs)}): {rfs}")
    for j in s.get("joins") or []:
        print(f"    join: right_table={j.get('right_table')!r} "
              f"right_alias={j.get('right_alias')!r} "
              f"join_type={j.get('join_type')!r}")


# %% [Cell 2: load external_view_lookup and show normalization]

print()
print("=" * 72)
print(f"[2] external_view_lookup from {VIEW_SOURCE_DIRS}")
print("=" * 72)

from tools.operate.view_resolver import load_external_views
from tools.p50_present.view_shape import _bare_view_key

ext = load_external_views(view_source_dirs=VIEW_SOURCE_DIRS, verbose=False)
print(f"  loaded {len(ext)} view(s)")

ext_bare_keys = {_bare_view_key(name): name for name in ext}
print(f"  distinct bare keys: {len(ext_bare_keys)}")
if ext_bare_keys:
    print(f"  sample bare keys (first 10):")
    for k in sorted(ext_bare_keys)[:10]:
        print(f"    {k!r:40s} -> {ext_bare_keys[k]!r}")


# %% [Cell 3: per-reference normalization check across host's scopes]

print()
print("=" * 72)
print(f"[3] Reference normalization for {HOST_VIEW_NAME!r}")
print("=" * 72)
print()
print(f"  For every reads_from_tables / join.right_table in the host,")
print(f"  normalize via _bare_view_key and check the external lookup.")
print()

unresolved_refs: list[tuple[str, str, str]] = []   # (scope_id, ref_raw, bare)
matched_refs: list[tuple[str, str, str]] = []
for s in host_view.get("scopes") or []:
    scope_id = s.get("id") or ""
    for ref in (s.get("reads_from_tables") or []):
        bare = _bare_view_key(ref or "")
        hit = ext_bare_keys.get(bare)
        marker = "HIT " if hit else "miss"
        print(f"  [{marker}] scope={scope_id:30s} ref={ref!r:30s} "
              f"bare={bare!r:25s} -> {hit!r}")
        (matched_refs if hit else unresolved_refs).append(
            (scope_id, ref, bare),
        )
    for j in (s.get("joins") or []):
        rt = j.get("right_table") or ""
        bare = _bare_view_key(rt)
        hit = ext_bare_keys.get(bare)
        marker = "HIT " if hit else "miss"
        print(f"  [{marker}] scope={scope_id:30s} join={rt!r:30s} "
              f"bare={bare!r:25s} -> {hit!r}")
        (matched_refs if hit else unresolved_refs).append(
            (scope_id, rt, bare),
        )

print()
print(f"  Summary: {len(matched_refs)} hit(s) / {len(unresolved_refs)} miss(es)")


# %% [Cell 4: build the host's shape and check placeholders + expansion]

print()
print("=" * 72)
print(f"[4] build_view_shape({HOST_VIEW_NAME!r}) -- placeholders + expansion")
print("=" * 72)

from tools.p50_present.view_shape import build_view_shape

shape = build_view_shape(
    host_view,
    external_view_lookup=ext,
    max_expansion_depth=1,
)
print(f"  shape has {len(shape.scopes)} scope(s) after build:")
for s in shape.scopes:
    print(f"    scope id={s.id!r}  kind={s.kind!r}  label={s.label!r}")
    for n in s.nodes:
        marker = ""
        if n.kind == "scope_ref":
            marker = "  [scope_ref"
            if n.target_view_name:
                marker += f", target_view={n.target_view_name!r}"
            if n.target_scope_id:
                marker += f", target_scope={n.target_scope_id!r}"
            marker += "]"
        print(f"      node {n.id}: {n.table!r:25s} "
              f"alias={n.alias!r:8s} role={n.role!r:6s}{marker}")
    for e in s.edges:
        print(f"      edge {e.source_id} -> {e.target_id} "
              f"({e.join_type!r})")

print()
print(f"  cross-scope edges ({len(shape.cross_scope_edges)}):")
for e in shape.cross_scope_edges:
    print(f"    {e.source_id} -> {e.target_id}  scope={e.scope_id!r}  "
          f"join_type={e.join_type!r}")


# %% [Cell 5: which scope_ref placeholders DID NOT expand inline?]

print()
print("=" * 72)
print("[5] Unexpanded placeholders -- the verdict")
print("=" * 72)
print()

# Collect every scope_ref placeholder and check if its target made
# it into the shape as an `external:<target>/...` scope.
placeholders: list = []
for s in shape.scopes:
    if s.id.startswith("external:"):
        continue   # already an expanded inner scope -- not a host placeholder
    for n in s.nodes:
        if n.kind == "scope_ref":
            placeholders.append((s.id, n))

if not placeholders:
    print("  No scope_ref placeholders found in the host's own scopes.")
    print("  This means build_view_shape didn't detect any foreign-view")
    print("  references inside the host. Re-check Cell 3 -- if there are")
    print("  HITs, they should have produced placeholders.")
else:
    for host_scope_id, p in placeholders:
        expected_prefix = f"external:{p.target_view_name}" if p.target_view_name else None
        expanded = any(
            s.id.startswith((expected_prefix or "<<<no-target>>>") + "/")
            for s in shape.scopes
        )
        status = "EXPANDED" if expanded else "NOT EXPANDED"
        print(f"  [{status}] host_scope={host_scope_id!r} "
              f"placeholder={p.table!r}")
        print(f"     target_view_name={p.target_view_name!r}")
        print(f"     target_scope_id={p.target_scope_id!r}")
        if not expanded:
            # Diagnose:
            if not p.target_view_name:
                print(f"     -> target_view_name is empty. The host's")
                print(f"        reference didn't match the foreign-view")
                print(f"        lookup; likely the bare key normalization")
                print(f"        differs between corpus and disk. Re-check")
                print(f"        Cell 3.")
            elif p.target_view_name not in ext:
                print(f"     -> target_view_name {p.target_view_name!r} is")
                print(f"        NOT a key in external_view_lookup. Listed")
                print(f"        keys (first 10):")
                for k in list(ext)[:10]:
                    print(f"          {k!r}")
            else:
                print(f"     -> target view is in external_view_lookup")
                print(f"        but no `external:{p.target_view_name}/...`")
                print(f"        scope appeared in shape.scopes. Possible")
                print(f"        causes: the inner view's build returned")
                print(f"        zero scopes (parse-fail or empty), OR a")
                print(f"        cycle guard skipped it. Inspect ext[{p.target_view_name!r}]:")
                inner = ext.get(p.target_view_name) or {}
                print(f"          inner scopes count: {len(inner.get('scopes') or [])}")
                print(f"          inner view_outputs: {inner.get('view_outputs')}")

print()
print("=" * 72)
