"""Try to normalize ONE stored proc into a view-shaped SELECT.

Companion to diagnose_failing_proc.py. Once the diagnostic shows a proc is
a stage-and-read shape (SELECT ... INTO #tmp; ...; final SELECT), this cell
runs select_into_to_cte against it to see whether it cleanly rewrites to a
single CREATE VIEW ... WITH ... SELECT.

Two outcomes:

  1. It prints `VIEW-SHAPED` plus the rewritten SQL -- the proc obeys the
     CTE-equivalence constraint (every temp defined once via SELECT INTO,
     never mutated, exactly one terminal SELECT). Paste the output back and
     we can run it through _build_view to confirm lineage.

  2. It prints `NOT view-shaped` plus a `reason` code -- the proc breaks the
     constraint and genuinely isn't a view:
       unsupported_statement     -> an INSERT/UPDATE/MERGE/DECLARE/WHILE etc.
       select_into_persistent    -> SELECT ... INTO a real (non-#) table = ETL
       temp_redefined            -> the same #temp written twice
       multiple_terminal_selects -> returns more than one result set
       no_terminal_select        -> stages temps but never returns
       undefined_temp_reference  -> reads a #temp staged outside this proc

Edit the two constants, run the cell, paste the output back in the chat.
"""


# %% [Cell: normalize one proc -> view]

import os

from sql_logic_extractor.proc_normalize import ProcNotViewShaped, select_into_to_cte

# ---- EDIT these two constants for your setup -------------------------
PROC_FOLDER = "/lakehouse/default/Files/data/<your_proc_folder>"  # no trailing comma!
FILE_NAME = "Behavioral_Intake_PBI.sql"
# ----------------------------------------------------------------------

path = os.path.join(PROC_FOLDER, FILE_NAME)
if not os.path.isfile(path):
    raise FileNotFoundError(
        f"Not found: {path}\n"
        f"Edit PROC_FOLDER and FILE_NAME at the top of the cell."
    )


# Read tolerating SSMS's UTF-16-LE default encoding (same reader shape as
# diagnose_failing_proc.py so the two cells behave identically on a file).
def _read_with_encoding(p: str) -> str:
    for enc in ("utf-8", "utf-16", "latin-1"):
        try:
            with open(p, encoding=enc) as f:
                return f.read()
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("utf-8", b"", 0, 1, f"Could not decode {p}")


sql = _read_with_encoding(path)
print(f"File: {FILE_NAME}  ({len(sql):,} chars)")
print("=" * 60)

try:
    # dialect defaults to 'tsql'; emit_create_view=True wraps the result as
    # CREATE VIEW [schema].[name] AS ... so it flows through Phase C exactly
    # like a real scripted view.
    view_sql = select_into_to_cte(sql)
    print("VIEW-SHAPED  ✓")
    print("=" * 60)
    print(view_sql)
except ProcNotViewShaped as e:
    # The reason code tells us WHICH part of the CTE-equivalence constraint
    # the proc broke -- i.e. which bucket it really belongs in.
    print("NOT view-shaped")
    print("=" * 60)
    print(f"  reason: {e.reason}")
    if e.detail:
        print(f"  detail: {e.detail}")
    print()
    print("Paste this reason back in the chat -- it tells us whether the")
    print("'temp tables are just CTEs' assumption holds for THIS proc, or")
    print("whether it's genuinely a reporting/ETL/multi-output proc.")
