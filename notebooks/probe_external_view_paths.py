"""Find the working absolute path for view-of-view expansion AND
verify the view-name normalization matches between corpus and disk.

When the cwd-relative VIEW_SOURCE_DIRS lookup turns up zero files
(typical in Fabric notebooks), the cause is usually a mismatch
between Path.cwd() and where the SQL files were actually uploaded.
This notebook:

  1. Probes a list of CANDIDATE absolute paths -- common Fabric
     mount points and the workspace-prefixed forms -- and reports
     which ones contain .sql files.
  2. For the first working pair, runs load_external_views with the
     verbose flag so any parse failures surface.
  3. Loads the corpus.jsonl (default path; override if yours is
     different), pulls each view's view_name, and checks how many
     of the corpus's view references would MATCH an external view
     after _bare_view_key normalization. This is the test of
     whether your corpus view references like
     `FROM Reporting.V_FOO` will actually trigger the inline
     expansion for the foundation view `V_FOO.sql`.

Run after syncing commit 93bea31 (view_resolver gains
view_source_dirs param). The Cell 4 final printout tells you
exactly the view_source_dirs= list to paste into run_validation.
"""


# %% [Cell 1: probe candidate absolute paths]

import sys
from pathlib import Path

for mod in list(sys.modules):
    if mod.startswith("sql_logic_extractor") or mod.startswith("tools"):
        del sys.modules[mod]

# Edit if your Fabric mount uses an unusual name. The list covers
# the typical Fabric path variants Yang and other shops have hit.
CANDIDATE_PATHS = [
    # Yang's display-style path from Fabric UI:
    "SZ_SQL_Logic/default/Files/data/views_reporting",
    "SZ_SQL_Logic/default/Files/data/views_cookrpt",
    # Typical Fabric mount under /lakehouse/default:
    "/lakehouse/default/Files/data/views_reporting",
    "/lakehouse/default/Files/data/views_cookrpt",
    # Workspace-named lakehouse mount:
    "/lakehouse/SZ_SQL_Logic/default/Files/data/views_reporting",
    "/lakehouse/SZ_SQL_Logic/default/Files/data/views_cookrpt",
    # Without the data/ prefix:
    "/lakehouse/default/Files/views_reporting",
    "/lakehouse/default/Files/views_cookrpt",
]

print("=" * 72)
print("[1] Probing candidate absolute paths")
print("=" * 72)
working_paths: list[str] = []
for cand in CANDIDATE_PATHS:
    p = Path(cand)
    exists = p.is_dir()
    if exists:
        sql_count = len(list(p.glob("*.sql")))
        marker = "OK " if sql_count > 0 else "EMPTY"
        print(f"  [{marker}] {cand}  ({sql_count} .sql)")
        if sql_count > 0:
            working_paths.append(cand)
    else:
        print(f"  [---] {cand}  (not a directory)")

# Also enumerate /lakehouse/ at the top level so you can spot the
# right path even if it's not in the candidate list.
print()
print("Top-level under /lakehouse/ (for orientation):")
lakehouse = Path("/lakehouse")
if lakehouse.is_dir():
    for child in sorted(lakehouse.iterdir())[:20]:
        print(f"    {child}{'/' if child.is_dir() else ''}")
else:
    print("    /lakehouse not mounted; you may be running off-Fabric")


# %% [Cell 2: load external views from working paths + verbose parse]

print()
print("=" * 72)
print("[2] load_external_views against the working paths")
print("=" * 72)

if not working_paths:
    print(
        "  No working paths found in step 1.\n"
        "  Edit CANDIDATE_PATHS at the top of Cell 1 to add the real\n"
        "  absolute paths and re-run -- or paste a `find` command to\n"
        "  locate them:\n"
        "    import subprocess\n"
        "    r = subprocess.run(\n"
        "      ['find', '/lakehouse', '-type', 'd',\n"
        "        '-name', 'views_reporting'],\n"
        "      capture_output=True, text=True, timeout=30)\n"
        "    print(r.stdout or '(none)')"
    )
else:
    from tools.operate.view_resolver import load_external_views

    print(f"  Using working paths:")
    for p in working_paths:
        print(f"    {p}")
    print()
    ext = load_external_views(
        view_source_dirs=working_paths,
        verbose=True,
    )
    print()
    print(f"  -> {len(ext)} view(s) parsed successfully into "
          f"external_view_lookup")
    if ext:
        print(f"  Sample (first 5):")
        for name in list(ext)[:5]:
            n_scopes = len(ext[name].get("scopes") or [])
            n_tables = sum(
                len(s.get("reads_from_tables") or [])
                for s in ext[name].get("scopes") or []
            )
            print(f"    {name}  scopes={n_scopes}  tables={n_tables}")


# %% [Cell 3: name normalization -- will references in the corpus match?]

print()
print("=" * 72)
print("[3] Name normalization check vs. the corpus")
print("=" * 72)

# Edit if your corpus is at a different path.
CORPUS_PATH = "/lakehouse/default/Files/outputs/corpus.jsonl"

corpus_view_names: list[str] = []
corpus_references: list[tuple[str, str]] = []   # (host_view, referenced)

corpus_path_p = Path(CORPUS_PATH)
if not corpus_path_p.is_file():
    print(f"  corpus.jsonl not found at {CORPUS_PATH!r}")
    print(f"  Edit CORPUS_PATH at the top of Cell 3 and re-run.")
else:
    import json
    with corpus_path_p.open(encoding="utf-8") as f:
        next(f)   # header line
        for line in f:
            v = json.loads(line)
            vn = v.get("view_name") or ""
            if vn:
                corpus_view_names.append(vn)
                for s in v.get("scopes") or []:
                    for t in (s.get("reads_from_tables") or []):
                        corpus_references.append((vn, t))
                    for j in (s.get("joins") or []):
                        rt = j.get("right_table") or ""
                        if rt:
                            corpus_references.append((vn, rt))
    print(f"  corpus has {len(corpus_view_names)} view(s), "
          f"{len(corpus_references)} table/view references total")

# Bare-key normalization preview.
if "ext" in dir() and ext and corpus_view_names:
    from tools.p50_present.view_shape import _bare_view_key

    ext_bare = {_bare_view_key(n) for n in ext}
    print()
    print(f"  external_view_lookup bare-keys (first 10):")
    for k in sorted(ext_bare)[:10]:
        print(f"    {k}")

    # Which references in the corpus would HIT an external view?
    hits: dict[str, int] = {}
    for host, ref in corpus_references:
        bare = _bare_view_key(ref) if ref else ""
        if bare and bare in ext_bare:
            hits[bare] = hits.get(bare, 0) + 1
    print()
    print(f"  Matched references (corpus -> external_view_lookup):")
    print(f"  {len(hits)} distinct external view(s) would inline-expand")
    for k, count in sorted(hits.items(), key=lambda x: -x[1])[:10]:
        print(f"    {k}  ({count} reference(s))")


# %% [Cell 4: final printout -- the view_source_dirs= line to use]

print()
print("=" * 72)
print("[4] Final: the view_source_dirs= argument for run_validation")
print("=" * 72)

if not working_paths:
    print(
        "  Couldn't find a working path. Re-edit Cell 1's CANDIDATE_PATHS\n"
        "  with the real location and re-run."
    )
else:
    print()
    print("  Paste this into your run_validation call:")
    print()
    print("    result = run_validation(")
    print("        corpus_path='...',")
    print("        output_dir='...',")
    print("        view_source_dirs=[")
    for p in working_paths:
        print(f"            {p!r},")
    print("        ],")
    print("    )")
