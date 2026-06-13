# %% [markdown]
# # Diagnose Skipped Files
#
# Finds files that are skipped by the parser ("not view shape")
# and diagnoses WHY using both structural analysis and LLM.

# %% Cell 1: Setup — run this first
import sys, os
REPO_ROOT = "/lakehouse/default/Files"
sys.path.insert(0, REPO_ROOT)
os.chdir(REPO_ROOT)
print(f"Working directory: {os.getcwd()}")
from sql_logic_extractor.proc_normalize import select_into_to_cte, ProcNotViewShaped
print("Imports OK")

# %% Cell 2: Configure folders + verify
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

for d in ALL_DIRS:
    p = Path(d)
    if p.exists():
        n = len(list(p.glob("*.sql")))
        print(f"  OK: {d} ({n} files)")
    else:
        print(f"  MISSING: {d}")

# %% Cell 3: Scan all files — classify as ok / skipped / errored
import re
from collections import Counter
from sql_logic_extractor.proc_normalize import select_into_to_cte, ProcNotViewShaped

results = {"ok": [], "skipped": [], "errored": []}

_IS_PROC_RE = re.compile(
    r"CREATE\s+(OR\s+ALTER\s+)?PROC(EDURE)?\b", re.IGNORECASE)

for folder in ALL_DIRS:
    p = Path(folder)
    if not p.exists():
        continue
    is_proc_folder = "proc" in folder.lower()

    for f in sorted(p.glob("*.sql")):
        sql = f.read_text(encoding="utf-8-sig", errors="replace")
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

# %% Cell 4: Skipped files by reason
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

# %% Cell 5: Skipped files by first SQL keyword
first_keyword_counts = Counter()
first_keyword_samples = {}

for s in results["skipped"]:
    sql = s["sql"].strip()
    if not sql:
        first_keyword_counts["(empty)"] += 1
        continue

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

# %% Cell 6: Per-folder breakdown
print(f"\nPer-folder breakdown:\n")
for folder in ALL_DIRS:
    folder_ok = sum(1 for x in results["ok"] if x["folder"] == folder)
    folder_skip = sum(1 for x in results["skipped"] if x["folder"] == folder)
    folder_err = sum(1 for x in results["errored"] if x["folder"] == folder)
    total = folder_ok + folder_skip + folder_err
    name = Path(folder).name
    print(f"  {name:25s}  total={total:>4}  ok={folder_ok:>4}  "
          f"skipped={folder_skip:>4}  errored={folder_err:>4}")

# %% Cell 7: What statement types are in the "unsupported_statement" files?
import sqlglot
from sqlglot import exp
from sql_logic_extractor.proc_normalize import (
    _strip_proc_wrapper, _strip_temp_guards,
    _strip_block_begin_end, _insert_statement_separators,
)
from sql_logic_extractor.parsing_rules import apply_all

type_counts = Counter()
type_samples = {}

for s in results["skipped"]:
    if "unsupported" not in s.get("reason", ""):
        continue
    _, body = _strip_proc_wrapper(s["sql"])
    body = _strip_temp_guards(body)
    body, _ = apply_all(body)
    body = _strip_block_begin_end(body, "tsql")
    body = _insert_statement_separators(body, "tsql")

    try:
        stmts = [st for st in sqlglot.parse(body, dialect="tsql") if st is not None]
    except:
        type_counts["(parse_failed)"] += 1
        continue

    for st in stmts:
        if not isinstance(st, (exp.Select, exp.Set, exp.Declare, exp.SetOperation)):
            tname = type(st).__name__
            type_counts[tname] += 1
            if tname not in type_samples:
                type_samples[tname] = {
                    "file": s["file"],
                    "sql_fragment": st.sql("tsql")[:100],
                }

print(f"Unsupported statement types:\n")
for tname, count in type_counts.most_common():
    sample = type_samples.get(tname, {})
    print(f"  {count:>4}x  {tname}")
    print(f"         file: {sample.get('file', '?')}")
    print(f"         sql:  {sample.get('sql_fragment', '?')}")
    print()

# %% Cell 8: Show 3 full samples of the top skipped reason
top_reason = reason_counts.most_common(1)[0][0] if reason_counts else None
if top_reason:
    print(f"{'='*70}")
    print(f"Top skipped reason: {top_reason}")
    print(f"{'='*70}\n")

    count = 0
    for s in results["skipped"]:
        if s.get("reason") == top_reason:
            print(f"--- {s['file']} ({Path(s['folder']).name}) ---")
            for line in s["sql"].split("\n")[:25]:
                print(f"  {line.rstrip()}")
            print(f"  ... ({len(s['sql'])} chars total)\n")
            count += 1
            if count >= 3:
                break

# %% Cell 9: Error patterns (for the errored files)
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

# %% Cell 10: LLM diagnosis — what's in the skipped files and how to fix
import openai

client = openai.OpenAI()  # uses OPENAI_API_KEY from env

# Collect 3 samples per skip reason
samples_by_reason = {}
for s in results["skipped"]:
    reason = s.get("reason", "unknown")
    if reason not in samples_by_reason:
        samples_by_reason[reason] = []
    if len(samples_by_reason[reason]) < 3:
        truncated = "\n".join(s["sql"].split("\n")[:150])
        samples_by_reason[reason].append({
            "file": s["file"],
            "sql": truncated,
        })

print(f"Analyzing {len(samples_by_reason)} skip reasons with LLM...\n")

for reason, samples in samples_by_reason.items():
    sql_block = ""
    for i, s in enumerate(samples):
        sql_block += f"\n--- File: {s['file']} ---\n{s['sql']}\n"

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{
            "role": "system",
            "content": (
                "You are a SQL parser expert. The user's parser skips certain "
                "stored procedures because it can't convert them to view-shaped "
                "SELECTs. Analyze the SQL samples and answer:\n"
                "1. What SQL pattern causes the skip?\n"
                "2. Is there a SELECT that could be extracted as the 'view output'?\n"
                "3. What specific T-SQL construct is blocking the parser?\n"
                "Be concise — 3-5 sentences per sample group."
            )
        }, {
            "role": "user",
            "content": f"Skip reason: '{reason}' ({reason_counts[reason]} files)\n\nSample SQL:\n{sql_block}"
        }],
        temperature=0.1,
        max_tokens=500,
    )

    diagnosis = response.choices[0].message.content
    print(f"{'='*70}")
    print(f"REASON: {reason} ({reason_counts[reason]} files)")
    print(f"{'='*70}")
    print(diagnosis)
    print()

# %% Cell 11: LLM diagnosis for ERRORED files too
if results["errored"]:
    error_samples_by_pattern = {}
    for e in results["errored"]:
        msg = e["error"]
        msg_clean = re.sub(r"line \d+", "line N", msg)
        msg_clean = re.sub(r"col \d+", "col N", msg_clean)[:80]
        if msg_clean not in error_samples_by_pattern:
            error_samples_by_pattern[msg_clean] = []
        if len(error_samples_by_pattern[msg_clean]) < 2:
            truncated = "\n".join(e["sql"].split("\n")[:100])
            error_samples_by_pattern[msg_clean].append({
                "file": e["file"],
                "sql": truncated,
                "error": e["error"],
            })

    print(f"Analyzing {len(error_samples_by_pattern)} error patterns with LLM...\n")

    for pattern, samples in error_samples_by_pattern.items():
        sql_block = ""
        for s in samples:
            sql_block += f"\n--- File: {s['file']} ---\nError: {s['error']}\n{s['sql']}\n"

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{
                "role": "system",
                "content": (
                    "You are a SQL parser expert using sqlglot to parse T-SQL. "
                    "The parser threw an error on these files. Analyze:\n"
                    "1. What T-SQL syntax caused the error?\n"
                    "2. Is this a sqlglot limitation or a real syntax issue?\n"
                    "3. Suggest a fix (pre-processing regex, or sqlglot workaround).\n"
                    "Be concise — 3-5 sentences."
                )
            }, {
                "role": "user",
                "content": f"Error pattern: '{pattern}' ({error_patterns[pattern]} files)\n\n{sql_block}"
            }],
            temperature=0.1,
            max_tokens=500,
        )

        diagnosis = response.choices[0].message.content
        print(f"{'='*70}")
        print(f"ERROR: {pattern} ({error_patterns[pattern]} files)")
        print(f"{'='*70}")
        print(diagnosis)
        print()
