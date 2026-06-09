"""One-shot diagnostic: why does a given .sql describe as empty?

Paste into a Fabric cell, point SQL_PATH at the file that came back empty,
run. It reports: how the file reads, whether it's a proc / has temp tables,
how many columns the extractor finds (the key number -- 0 means there's
nothing for the LLM to describe), and whether running it through
select_into_to_cte first fixes it. Paste the output back.
"""

# ============================================================
# EDIT
# ============================================================
REPO_DIR = "/lakehouse/default/Files"
SQL_PATH = "/lakehouse/default/Files/data/views_reporting/CCHPCHICComplexCareDiagnosis_PBI.sql"
SCHEMA_PATH = "/lakehouse/default/Files/data/dictionaries/clarity_schema.json"
# ============================================================

import re
import sys
if REPO_DIR not in sys.path:
    sys.path.insert(0, REPO_DIR)

from tools.shared.sql_loader import read_sql_robust
from tools.report_description_generator.cli import _load_schema
from sql_logic_extractor.products import generate_report_description
from sql_logic_extractor.proc_normalize import ProcNotViewShaped, select_into_to_cte

sql = read_sql_robust(SQL_PATH)
print("=== READ ===")
print("length:", len(sql))
print("is CREATE PROCEDURE? ", bool(re.search(r"\bCREATE\s+(?:OR\s+ALTER\s+)?PROC", sql, re.I)))
print("has SELECT ... INTO #temp? ", bool(re.search(r"\bINTO\s+#", sql, re.I)))
print("--- first 600 chars ---")
print(sql[:600])

schema = _load_schema(SCHEMA_PATH)
print("\n=== schema tables loaded:", len(schema.get("tables", [])), "===")

# Engineered (no LLM) isolates extraction from the LLM. If THIS is empty with
# 0 columns, the parser found nothing -- the SQL isn't view-shaped as-is.
print("\n=== AS-IS, engineered (no LLM) ===")
try:
    eng = generate_report_description(sql, schema, use_llm=False)
    ncols = len(getattr(eng.business_logic, "column_translations", []) or [])
    print("columns found:", ncols)
    print("business_description:", repr((eng.business_description or "")[:200]))
except Exception as e:
    print("ERROR:", type(e).__name__, str(e)[:200])

# THE KEY TEST: run WITH the LLM. If business_description is empty, the real
# error is hidden in technical_description as "[LLM error: <Type>: <msg>]".
print("\n=== AS-IS, WITH LLM (use_llm=True) ===")
try:
    llm = generate_report_description(sql, schema, use_llm=True)
    print("business_description:", repr((llm.business_description or "")[:200]))
    print("primary_purpose:", repr(llm.primary_purpose))
    print("technical_description (HOLDS ANY LLM ERROR):",
          repr((llm.technical_description or "")[:300]))
except Exception as e:
    print("ERROR:", type(e).__name__, str(e)[:300])
