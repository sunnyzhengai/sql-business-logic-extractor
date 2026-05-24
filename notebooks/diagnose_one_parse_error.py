"""Fabric notebook helper -- diagnose ONE specific .sql file's parse failure.

Use this when Cell 4 / Cell 5 reports parse errors on the MyChart pilot
(or any small corpus) and you want to see exactly what sqlglot is
choking on for a named view, without going through the timing_audit
CSV workflow.

Paste each `# %% [Cell ...]` section into its own Fabric notebook cell.

Cell A picks ONE file by path and runs the full chain:
    raw read -> preprocess_ssms -> apply_all parsing rules -> sqlglot
At each stage, on failure, it prints the cleaned SQL up to that point
plus the exact error. The output is safe to share -- table/column
identifiers in the SQL are NOT redacted, so review before pasting into
a public chat if your views have sensitive names.

Cell B is the same diagnostic in a loop over every .sql file in a
directory -- useful to see how many distinct error patterns there are
in the pilot. Prints a one-line-per-file verdict (ok / preprocess
fail / rules fail / sqlglot fail + first ~80 chars of error message).
"""


# %% [Cell A: deep-diagnose ONE specific .sql file]
#
# Edit `view_path` to point at the file you want to diagnose. Then run
# this cell. Output ends with one of three banners:
#
#   ✅ SUCCESS -- the file parses now (rules + preprocess fix it).
#   ❌ PREPROCESS / RULES FAILURE -- our cleanup code itself crashes.
#   ❌ SQLGLOT FAILURE -- the cleaned SQL is still unparseable.
#
# For SQLGLOT FAILURE, the printed line + column tell you what to fix.
# Common patterns and likely fixes:
#   - OUTER APPLY / CROSS APPLY  -> sqlglot needs dialect="tsql" (set below)
#   - PIVOT / UNPIVOT             -> dialect="tsql" usually handles it
#   - Unaliased subquery in FROM  -> may need a preprocess rule
#   - OPENQUERY / OPENROWSET      -> sqlglot doesn't model these; skip

import sys

# Drop module cache so any in-session edits to parsing_rules pick up.
for mod in list(sys.modules):
    if mod.startswith('sql_logic_extractor') or mod.startswith('tools'):
        del sys.modules[mod]

import sqlglot
from sql_logic_extractor.resolve import preprocess_ssms
from sql_logic_extractor.parsing_rules import apply_all


# ---- EDIT THIS PATH to the failing file -----------------------------------
view_path = f"{REPO_ROOT}/data/mychart_views/<your_failing_view>.sql"
# ----------------------------------------------------------------------------


print(f"Diagnosing: {view_path}\n")

# Stage 1: raw read
raw = open(view_path, encoding='utf-8').read()
print("--- Raw SQL (first 60 lines) ---")
for i, line in enumerate(raw.splitlines()[:60], 1):
    print(f"  {i:3d}: {line}")
print()

# Stage 2: preprocess_ssms (resolve SSMS-isms like [bracketed] names)
try:
    cleaned, meta = preprocess_ssms(raw)
    print(f"--- preprocess_ssms OK ({len(cleaned)} chars output) ---")
except Exception as e:
    print(f"❌ PREPROCESS FAILURE: {type(e).__name__}: {e}")
    raise SystemExit()

# Stage 3: apply_all parsing rules (project-specific regex cleanups)
try:
    cleaned, applied = apply_all(cleaned)
    print(f"--- apply_all OK (rules applied: {applied or '(none)'}) ---")
except Exception as e:
    print(f"❌ RULES FAILURE: {type(e).__name__}: {e}")
    raise SystemExit()

# Stage 4: sqlglot parse (try generic dialect, then T-SQL)
print("\n--- sqlglot.parse (no dialect) ---")
try:
    sqlglot.parse(cleaned)
    print("✅ Parses without a dialect hint.")
except Exception as e:
    print(f"  fails: {type(e).__name__}: {e}")

print("\n--- sqlglot.parse(dialect='tsql') ---")
try:
    sqlglot.parse(cleaned, dialect='tsql')
    print("✅ Parses with dialect='tsql' -- the fix may be to set this in the pipeline.")
except Exception as e:
    print(f"❌ SQLGLOT FAILURE (tsql): {type(e).__name__}: {e}")

# Stage 5: print the cleaned SQL around the reported error position
# so the eye can land on what sqlglot is reading.
print("\n--- Cleaned SQL around error position ---")
print("(Look for the line/col in the error message above and find it here.)\n")
for i, line in enumerate(cleaned.splitlines()[:80], 1):
    print(f"  {i:3d}: {line}")


# %% [Cell B: triage ALL .sql files in a directory at once]
#
# Same diagnostic, looped. Prints one line per file with the verdict and
# (for failures) the first ~80 chars of the error message. Quick way to
# see whether all the parse failures share one error pattern or fragment
# across several distinct ones.

import os
import sys
import traceback

for mod in list(sys.modules):
    if mod.startswith('sql_logic_extractor') or mod.startswith('tools'):
        del sys.modules[mod]

import sqlglot
from sql_logic_extractor.resolve import preprocess_ssms
from sql_logic_extractor.parsing_rules import apply_all


# ---- EDIT THIS PATH to the directory ----------------------------------------
corpus_dir = f"{REPO_ROOT}/data/mychart_views"
# -----------------------------------------------------------------------------


print(f"Triaging: {corpus_dir}\n")
results: list[tuple[str, str, str]] = []  # (filename, verdict, error_excerpt)

for filename in sorted(f for f in os.listdir(corpus_dir) if f.endswith('.sql')):
    path = os.path.join(corpus_dir, filename)
    raw = open(path, encoding='utf-8').read()

    try:
        cleaned, _ = preprocess_ssms(raw)
    except Exception as e:
        results.append((filename, 'preprocess_fail',
                        f"{type(e).__name__}: {str(e)[:80]}"))
        continue

    try:
        cleaned, _ = apply_all(cleaned)
    except Exception as e:
        results.append((filename, 'rules_fail',
                        f"{type(e).__name__}: {str(e)[:80]}"))
        continue

    try:
        sqlglot.parse(cleaned, dialect='tsql')
        results.append((filename, 'ok', ''))
    except Exception as e:
        results.append((filename, 'sqlglot_fail',
                        f"{type(e).__name__}: {str(e)[:80]}"))

# Tally
tally: dict[str, int] = {}
for _, verdict, _ in results:
    tally[verdict] = tally.get(verdict, 0) + 1

print("Verdict tally:")
for v, n in sorted(tally.items(), key=lambda kv: -kv[1]):
    print(f"  {n:>3}  {v}")
print()

print("Per-file verdicts:")
for filename, verdict, err in results:
    icon = '✅' if verdict == 'ok' else '❌'
    if err:
        print(f"  {icon} {filename:<40s} {verdict}  {err}")
    else:
        print(f"  {icon} {filename:<40s} {verdict}")
