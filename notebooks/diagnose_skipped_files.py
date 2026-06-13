# %% [markdown]
# # Diagnose Skipped Files
#
# Finds files that are skipped by the parser ("not view shape")
# and shows WHY they're being skipped.

# %% Setup — run this cell first
import sys, os
# Adjust this path to where you cloned the repo in Fabric
REPO_ROOT = "/lakehouse/default/Files/sql-business-logic-extractor"
sys.path.insert(0, REPO_ROOT)
os.chdir(REPO_ROOT)
print(f"Working directory: {os.getcwd()}")
print(f"Python path includes: {REPO_ROOT}")

# Quick import test
from sql_logic_extractor.proc_normalize import select_into_to_cte, ProcNotViewShaped
print("Imports OK")

# %% Configure folders
from pathlib import Path

VIEW_DIRS = [
    "/lakehouse/default/Files/data/views_reporting",
    "/lakehouse/default/Files/data/views_cookrpt",
]
PROC_DIRS = [
    "/lakehouse/default/Files/data/procs_reporting",
    "/lakehouse/default/Files/data/procs_cookrpt",
]

ALL_DIRS = VIEW_DIRS + PROC_DIRS

# Verify folders exist
for d in ALL_DIRS:
    p = Path(d)
    if p.exists():
        n = len(list(p.glob("*.sql")))
        print(f"  OK: {d} ({n} files)")
    else:
        print(f"  MISSING: {d}")

# %% Scan all files and classify
import re
from collections import Counter
from sql_logic_extractor.proc_normalize import select_into_to_cte, ProcNotViewShaped

results = {"ok": [], "skipped": [], "errored": []}

# Detect if file is a proc (CREATE PROC) or view (CREATE VIEW / bare SELECT)
_IS_PROC_RE = re.compile(
    r"CREATE\s+(OR\s+ALTER\s+)?PROC(EDURE)?\b", re.IGNORECASE)

for folder in ALL_DIRS:
    p = Path(folder)
    if not p.exists():
        continue
    is_proc_folder = "proc" in folder.lower()

    for f in sorted(p.glob("*.sql")):
        sql = f.read_text(encoding="utf-8-sig", errors="replace")

        # Determine if this file is a proc
        is_proc = is_proc_folder or bool(_IS_PROC_RE.search(sql[:500]))

        if is_proc:
            try:
                normalized = select_into_to_cte(sql)
                results["ok"].append({"file": f.name, "folder": folder})
            except ProcNotViewShaped as e:
                results["skipped"].append({
                    "file": f.name, "folder": folder,
                    "reason": str(e.reason) if hasattr(e, 'reason') else str(e),
                    "sql": sql,
                })
            except Exception as e:
                results["errored"].append({
                    "file": f.name, "folder": folder,
                    "error": str(e)[:150], "sql": sql,
                })
        else:
            # Views — try parsing with sqlglot to check if valid
            try:
                import sqlglot
                parsed = sqlglot.parse(sql, dialect="tsql")
                if parsed:
                    results["ok"].append({"file": f.name, "folder": folder})
                else:
                    results["skipped"].append({
                        "file": f.name, "folder": folder,
                        "reason": "empty parse result", "sql": sql,
                    })
            except Exception as e:
                results["errored"].append({
                    "file": f.name, "folder": folder,
                    "error": str(e)[:150], "sql": sql,
                })

print(f"\nOK: {len(results['ok'])}  Skipped: {len(results['skipped'])}  Errored: {len(results['errored'])}")

# %% Analyze skipped files — group by reason
reason_counts = Counter()
reason_samples = {}

for s in results["skipped"]:
    reason = s.get("reason", "unknown")
    reason_counts[reason] += 1
    if reason not in reason_samples:
        reason_samples[reason] = s

print(f"\nSkipped files by reason ({len(results['skipped'])} total):\n")
for reason, count in reason_counts.most_common():
    sample = reason_samples[reason]
    print(f"  {count:>4}x  {reason}")
    print(f"         example: {sample['file']}")
    print()

# %% Analyze skipped files — what do they start with?
first_keyword_counts = Counter()
first_keyword_samples = {}

for s in results["skipped"]:
    sql = s["sql"].strip()
    if not sql:
        first_keyword_counts["(empty)"] += 1
        continue

    # Get first non-comment line
    first_line = ""
    for line in sql.split("\n"):
        stripped = line.strip()
        if stripped and not stripped.startswith("--") and not stripped.startswith("/*"):
            first_line = stripped
            break

    words = first_line.split()
    first_word = words[0].upper() if words else "(empty)"
    if first_word in ("CREATE", "ALTER") and len(words) > 1:
        first_word = f"{first_word} {words[1].upper()}"

    first_keyword_counts[first_word] += 1
    if first_word not in first_keyword_samples:
        first_keyword_samples[first_word] = {"file": s["file"], "line": first_line[:80]}

print(f"\nSkipped files by first SQL keyword:\n")
for kw, count in first_keyword_counts.most_common():
    sample = first_keyword_samples.get(kw, {})
    print(f"  {count:>4}x  {kw:30s}  example: {sample.get('file', '')}")
    print()

# %% Show 3 full samples of the most common skipped pattern
top_reason = reason_counts.most_common(1)[0][0] if reason_counts else None
if top_reason:
    print(f"\n{'='*70}")
    print(f"Top skipped reason: {top_reason}")
    print(f"{'='*70}\n")

    count = 0
    for s in results["skipped"]:
        if s.get("reason") == top_reason:
            print(f"--- {s['file']} ({s['folder'].split('/')[-1]}) ---")
            for line in s["sql"].split("\n")[:25]:
                print(f"  {line.rstrip()}")
            print(f"  ... ({len(s['sql'])} chars total)")
            print()
            count += 1
            if count >= 3:
                break

# %% Per-folder breakdown
print(f"\nPer-folder breakdown:\n")
for folder in ALL_DIRS:
    folder_ok = sum(1 for x in results["ok"] if x["folder"] == folder)
    folder_skip = sum(1 for x in results["skipped"] if x["folder"] == folder)
    folder_err = sum(1 for x in results["errored"] if x["folder"] == folder)
    total = folder_ok + folder_skip + folder_err
    name = Path(folder).name
    print(f"  {name:25s}  total={total:>4}  ok={folder_ok:>4}  "
          f"skipped={folder_skip:>4}  errored={folder_err:>4}")

# %% Error patterns
if results["errored"]:
    error_patterns = Counter()
    error_samples = {}
    for e in results["errored"]:
        msg = e["error"]
        msg_clean = re.sub(r"line \d+", "line N", msg)
        msg_clean = re.sub(r"col \d+", "col N", msg_clean)[:80]
        error_patterns[msg_clean] += 1
        if msg_clean not in error_samples:
            error_samples[msg_clean] = e["file"]

    print(f"\nError patterns ({len(results['errored'])} total):\n")
    for pat, count in error_patterns.most_common():
        print(f"  {count:>4}x  {pat}")
        print(f"         file: {error_samples[pat]}")
        print()
