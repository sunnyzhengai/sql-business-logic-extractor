# %% [markdown]
# # Diagnose Skipped Files
#
# Finds files that are skipped by the parser ("not view shape")
# and shows WHY they're being skipped — what they start with,
# what patterns they contain.

# %% Setup
import sys
sys.path.insert(0, "/lakehouse/default/Files/sql-business-logic-extractor")

from pathlib import Path
from collections import Counter

# %% Configure folders
VIEW_DIRS = [
    "/lakehouse/default/Files/data/views_reporting",
    "/lakehouse/default/Files/data/views_cookrpt",
]
PROC_DIRS = [
    "/lakehouse/default/Files/data/procs_reporting",
    "/lakehouse/default/Files/data/procs_cookrpt",
]

ALL_DIRS = VIEW_DIRS + PROC_DIRS

# %% Scan all files and classify: ok / skipped / errored
from sql_logic_extractor.proc_normalize import normalize_proc

results = {"ok": [], "skipped": [], "errored": []}

for folder in ALL_DIRS:
    p = Path(folder)
    if not p.exists():
        print(f"MISSING: {folder}")
        continue
    for f in sorted(p.glob("*.sql")):
        sql = f.read_text(encoding="utf-8-sig", errors="replace")
        try:
            result = normalize_proc(sql)
            if result is None:
                results["skipped"].append({"file": f.name, "folder": folder, "sql": sql})
            else:
                results["ok"].append(f.name)
        except Exception as e:
            results["errored"].append({"file": f.name, "folder": folder, "error": str(e)[:100]})

print(f"OK: {len(results['ok'])}  Skipped: {len(results['skipped'])}  Errored: {len(results['errored'])}")

# %% Analyze skipped files — what do they start with?
first_keyword_counts = Counter()
first_line_samples = {}

for s in results["skipped"]:
    sql = s["sql"].strip()
    # Skip empty
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

    # Get first keyword
    first_word = first_line.split()[0].upper() if first_line else "(empty)"
    # Normalize: CREATE VIEW, CREATE PROC, ALTER, etc.
    if first_word == "CREATE":
        second = first_line.split()[1].upper() if len(first_line.split()) > 1 else ""
        first_word = f"CREATE {second}"
    elif first_word == "ALTER":
        second = first_line.split()[1].upper() if len(first_line.split()) > 1 else ""
        first_word = f"ALTER {second}"

    first_keyword_counts[first_word] += 1
    if first_word not in first_line_samples:
        first_line_samples[first_word] = {"file": s["file"], "line": first_line[:80]}

print(f"\nSkipped files by first keyword ({len(results['skipped'])} total):\n")
for kw, count in first_keyword_counts.most_common():
    sample = first_line_samples.get(kw, {})
    print(f"  {count:>4}x  {kw:30s}  example: {sample.get('file', '')}")
    print(f"         {sample.get('line', '')[:70]}")
    print()

# %% Show 5 full samples of the most common skipped pattern
top_pattern = first_keyword_counts.most_common(1)[0][0] if first_keyword_counts else None
if top_pattern:
    print(f"\n{'='*70}")
    print(f"Top skipped pattern: {top_pattern}")
    print(f"{'='*70}\n")

    count = 0
    for s in results["skipped"]:
        sql = s["sql"].strip()
        first_line = ""
        for line in sql.split("\n"):
            stripped = line.strip()
            if stripped and not stripped.startswith("--") and not stripped.startswith("/*"):
                first_line = stripped
                break

        first_word = first_line.split()[0].upper() if first_line else ""
        if first_word == "CREATE":
            second = first_line.split()[1].upper() if len(first_line.split()) > 1 else ""
            first_word = f"CREATE {second}"
        elif first_word == "ALTER":
            second = first_line.split()[1].upper() if len(first_line.split()) > 1 else ""
            first_word = f"ALTER {second}"

        if first_word == top_pattern:
            print(f"--- {s['file']} ---")
            # Show first 20 lines
            for line in sql.split("\n")[:20]:
                print(f"  {line.rstrip()}")
            print(f"  ... ({len(sql)} chars total)")
            print()
            count += 1
            if count >= 5:
                break

# %% Show error patterns
if results["errored"]:
    error_patterns = Counter()
    for e in results["errored"]:
        # Normalize error message
        msg = e["error"]
        # Strip file-specific details
        import re
        msg = re.sub(r"line \d+", "line N", msg)
        msg = re.sub(r"col \d+", "col N", msg)
        error_patterns[msg[:60]] += 1

    print(f"\nError patterns ({len(results['errored'])} total):\n")
    for pat, count in error_patterns.most_common():
        example = next(e for e in results["errored"] if e["error"][:60] == pat)
        print(f"  {count:>4}x  {pat}")
        print(f"         file: {example['file']}")
        print()

# %% Per-folder breakdown
print(f"\nPer-folder breakdown:\n")
for folder in ALL_DIRS:
    ok = sum(1 for f in results["ok"])  # not quite right, need folder
    skipped = [s for s in results["skipped"] if s["folder"] == folder]
    errored = [e for e in results["errored"] if e["folder"] == folder]
    total = len(skipped) + len(errored)
    # Count OK for this folder
    p = Path(folder)
    total_files = len(list(p.glob("*.sql"))) if p.exists() else 0
    ok_count = total_files - len(skipped) - len(errored)
    print(f"  {Path(folder).name:25s}  total={total_files:>4}  ok={ok_count:>4}  "
          f"skipped={len(skipped):>4}  errored={len(errored):>4}")
