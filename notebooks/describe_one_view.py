"""Run Tool 4 on ONE real view -- engineered vs LLM, side by side.

Prereq: run fabric_llm_setup.py Cell 2 FIRST in the same notebook session.
That sets sys.path + the provider/key env vars; this cell reuses them
(generate_report_description builds the LLM client from those env vars
automatically, so you don't paste the key again).

Unlike demo_llm_vs_engineered.py (which is pinned to the mockup
clarity_schema.yaml), this points at any view file you choose and lets the
schema be optional -- pass SCHEMA_PATH only when you have a real Clarity data
dictionary; leave it None to describe straight from the SQL logic.

Paste into a Fabric cell, edit the EDIT block, run.
"""

# %% [Cell: describe one view -- engineered vs LLM]

from pathlib import Path

# ============================================================
# EDIT
# ============================================================
REPO_DIR = "/lakehouse/default/Files/<your_repo_folder>"   # same as the setup cell
VIEW_PATH = "/lakehouse/default/Files/data/<folder>/<YourView>.sql"  # the view to describe
SCHEMA_PATH = None   # e.g. "/lakehouse/.../clarity_schema.yaml"; None = describe from SQL only
# ============================================================

import sys
if REPO_DIR not in sys.path:
    sys.path.insert(0, REPO_DIR)

from sql_logic_extractor.products import generate_report_description

# Read the view SQL, tolerating SSMS's UTF-16 default.
sql = Path(VIEW_PATH).read_text(encoding="utf-8", errors="replace")

# Schema is optional. With a real Clarity dictionary the descriptions get
# richer (table/column meanings); without it, Tool 4 works off the SQL alone.
if SCHEMA_PATH:
    from tools.report_description_generator.cli import _load_schema
    schema = _load_schema(SCHEMA_PATH)
else:
    schema = {}

print(f"View:   {VIEW_PATH}")
print(f"Schema: {SCHEMA_PATH or '(none -- SQL logic only)'}")

# 1) Engineered mode -- deterministic, no LLM, no key needed.
engineered = generate_report_description(sql, schema)
print("\n" + "=" * 70)
print("ENGINEERED (mechanical, no LLM)")
print("=" * 70)
print(engineered.business_description or "(empty)")

# 2) LLM mode -- uses the provider/key from the setup cell's env vars.
llm = generate_report_description(sql, schema, use_llm=True)
print("\n" + "=" * 70)
print("LLM-POLISHED")
print("=" * 70)
print(llm.business_description or "(empty)")
print(f"\nprimary_purpose: {llm.primary_purpose}")
print(f"key_metrics:     {llm.key_metrics}")

# ---- Testing a stored PROC instead of a view? -------------------------
# If VIEW_PATH is a reporting proc (SELECT ... INTO #tmp; ...; final SELECT),
# normalize it to view-shaped SQL FIRST, then describe that:
#
#     from sql_logic_extractor.proc_normalize import select_into_to_cte
#     sql = select_into_to_cte(sql)          # raises ProcNotViewShaped if not view-shaped
#
# then re-run the two generate_report_description calls above on `sql`.
