"""In-kernel driver for the PARSE-ERROR SCAN -- paste into a Fabric cell and run.

Fast and FREE (no LLM): runs every .sql in the 4 folders through the same
parse/extract path the description batch uses, catches failures, and writes a
report grouping every DISTINCT parser error (with counts + example files +
the offending line). Send Claude the report -- then all the parsing gaps get
fixed in one pass instead of one-at-a-time.

Run in the kernel (Shift+Enter), not via !python. Edit the EDIT block, run.
"""

import sys

# ============================================================
# EDIT
# ============================================================
REPO_DIR = "/lakehouse/default/Files"
VIEW_DIRS = [
    "/lakehouse/default/Files/data/views_reporting",
    "/lakehouse/default/Files/data/views_cookrpt",
]
PROC_DIRS = [
    "/lakehouse/default/Files/data/procs_reporting",
    "/lakehouse/default/Files/data/procs_cookrpt",
]
SCHEMA_PATH = "/lakehouse/default/Files/data/dictionaries/clarity_schema.json"
OUT_PATH = "/lakehouse/default/Files/outputs/parse_errors.md"
# ============================================================

if REPO_DIR not in sys.path:
    sys.path.insert(0, REPO_DIR)

from tools.report_description_generator.describe_folders import scan_parse_errors

scan_parse_errors(
    view_dirs=VIEW_DIRS,
    proc_dirs=PROC_DIRS,
    schema_path=SCHEMA_PATH,
    out_path=OUT_PATH,
)
