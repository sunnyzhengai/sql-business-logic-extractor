"""Diagnose why a specific stored proc fails to parse.

When extract_corpus reports `n_failed > 0` on a folder of stored
procs, the question is always: WHICH proc-specific construct is
tripping the parser?  The two most common reasons are:

  1. strip_procedure_wrapper's lookahead missed the first keyword
     after `AS` (e.g., the body starts with `SET NOCOUNT ON` or
     `IF EXISTS(...)` instead of one of the keywords the rule
     currently anchors on: BEGIN | SELECT | WITH | DECLARE | RETURN
     | INSERT | UPDATE | DELETE | MERGE | EXEC).  Fix: widen the
     lookahead.

  2. The proc body has T-SQL flow control sqlglot can't parse
     even in tsql dialect -- nested IF/ELSE around multiple
     statements, WHILE loops, dynamic EXEC sp_executesql.  This
     class of proc isn't view-shaped and may need to be skipped.

This script reads a SINGLE specified proc, hops past everything
before `CREATE PROCEDURE`, and prints the declaration line plus
the first 15 lines of the body so we can see exactly which
pattern is breaking.  Paste the output back in the chat.
"""


# %% [Cell: diagnose one failing proc]

import os
import re

# ---- EDIT these two constants for your setup -------------------------
PROC_FOLDER = "/lakehouse/default/Files/data/<your_proc_folder>"
FILE_NAME = "Behavioral_Intake_PBI.sql"
# ----------------------------------------------------------------------

path = os.path.join(PROC_FOLDER, FILE_NAME)
if not os.path.isfile(path):
    raise FileNotFoundError(
        f"Not found: {path}\n"
        f"Edit PROC_FOLDER and FILE_NAME at the top of the cell."
    )

# Read tolerating SSMS's UTF-16-LE default encoding.
def _read_with_encoding(p: str) -> str:
    for enc in ("utf-8", "utf-16", "latin-1"):
        try:
            with open(p, encoding=enc) as f:
                return f.read()
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError(
        "utf-8", b"", 0, 1, f"Could not decode {p}"
    )

text = _read_with_encoding(path)
print(f"File size: {len(text):,} chars")
print()

# Skip to CREATE [OR ALTER] (PROCEDURE|PROC|FUNCTION|TRIGGER).
m = re.search(
    r"\bCREATE\s+(?:OR\s+ALTER\s+)?"
    r"(?:PROC(?:EDURE)?|FUNCTION|TRIGGER)\b",
    text, re.IGNORECASE,
)
if not m:
    print("!! No CREATE PROCEDURE/PROC/FUNCTION/TRIGGER found in this file !!")
    print()
    print("First 30 lines (for context -- maybe a different shape):")
    print("-" * 60)
    for line in text.splitlines()[:30]:
        print(line)
    raise SystemExit

print(f"Found CREATE at character offset {m.start():,}")
print()

snippet = text[m.start():]

# Find the FIRST AS keyword. (Strictly speaking, T-SQL table-valued
# parameters can contain `AS TableType` inside the param list, which
# is a known false-positive risk for the strip_procedure_wrapper
# regex. If we hit such a case here we'll see it in the printed
# declaration block.)
as_match = re.search(r"\bAS\b", snippet, re.IGNORECASE)

if not as_match:
    print("!! No AS keyword found after CREATE -- unusual shape !!")
    print()
    print("First 1000 chars from CREATE:")
    print("-" * 60)
    print(snippet[:1000])
    raise SystemExit

print("=" * 60)
print("CREATE through (and including) AS")
print("=" * 60)
print(snippet[:as_match.end()])
print()
print("=" * 60)
print("First 15 lines AFTER AS")
print("=" * 60)
body_lines = snippet[as_match.end():].splitlines()
# Skip leading blank lines so the first 'meaningful' line shows up.
non_blank_lines_shown = 0
for i, line in enumerate(body_lines):
    print(f"{i + 1:3d}: {line}")
    if line.strip():
        non_blank_lines_shown += 1
    if non_blank_lines_shown >= 15:
        break

print()
print("=" * 60)
print("What to look for")
print("=" * 60)
print(
    "  - The first NON-BLANK / NON-COMMENT line after AS is the\n"
    "    body's opening keyword. If it's anything other than\n"
    "    BEGIN | SELECT | WITH | DECLARE | RETURN | INSERT |\n"
    "    UPDATE | DELETE | MERGE | EXEC, the strip_procedure_wrapper\n"
    "    rule's lookahead doesn't match -- I'll widen it.\n"
    "  - If the body has IF/ELSE around multiple statements,\n"
    "    WHILE loops, or EXEC sp_executesql for dynamic SQL,\n"
    "    that's a T-SQL flow-control proc that won't be view-\n"
    "    shaped no matter how cleanly we strip the wrapper.\n"
    "  - Paste the output back in the chat -- the fix is\n"
    "    usually one regex tweak."
)
