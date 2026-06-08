"""In-kernel driver for the batch description run -- PASTE INTO A FABRIC CELL and run.

Run this IN the notebook kernel (Shift+Enter), NOT via !python. A subprocess
doesn't share the notebook's /lakehouse mount (file reads come back empty) and
may use a different Python without the %pip libraries. Running in the kernel
gives you BOTH the libraries and the Lakehouse mount.

All the real logic lives in tools/report_description_generator/describe_folders.py;
this cell just sets the import path and calls it with your folders. (Importing
describe_folders also loads your .env automatically, so the LLM key is picked up.)

Edit the EDIT block, then run.
"""

import sys

# ============================================================
# EDIT -- your four folders, the schema, and where to write output.
# ============================================================
REPO_DIR = "/lakehouse/default/Files"     # the folder that CONTAINS tools/ and sql_logic_extractor/
VIEW_DIRS = [
    "/lakehouse/default/Files/data/views_a",
    "/lakehouse/default/Files/data/views_b",
]
PROC_DIRS = [
    "/lakehouse/default/Files/data/procs_a",
    "/lakehouse/default/Files/data/procs_b",
]
SCHEMA_PATH = "/lakehouse/default/Files/data/dictionaries/clarity_schema.json"
OUT_PATH = "/lakehouse/default/Files/outputs/descriptions.md"
LIMIT = 5          # quick dry run; set to None to process ALL files
# ============================================================

# Make the package importable from the kernel (this runs in-process, so it works).
if REPO_DIR not in sys.path:
    sys.path.insert(0, REPO_DIR)

from tools.report_description_generator.describe_folders import describe_folders

describe_folders(
    view_dirs=VIEW_DIRS,
    proc_dirs=PROC_DIRS,
    schema_path=SCHEMA_PATH,
    out_path=OUT_PATH,
    limit=LIMIT,
)
