"""Tiny driver for the batch description run -- so you don't type a long command.

Edit the paths in the EDIT block below (once), then run from a Fabric cell:

    !python /lakehouse/default/Files/<your_repo_folder>/run_describe.py

That's the whole command -- no long module path or arguments to mistype. All
the real logic lives in tools/report_description_generator/describe_folders.py;
this file just calls it with your folders.
"""

import sys
from pathlib import Path

# This file sits at the repo root, so its own folder IS the repo root. Add it
# to the import path so `tools` / `sql_logic_extractor` import no matter what
# directory the notebook runs from.
_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# Load the LLM key/provider from a .env next to this file (if present).
try:
    from dotenv import load_dotenv
    load_dotenv(_REPO_ROOT / ".env")
except ImportError:
    pass

from tools.report_description_generator.describe_folders import describe_folders

# ============================================================
# EDIT -- your four folders, the schema, and where to write output.
# Put each path on its own line so it's easy to check.
# ============================================================
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

describe_folders(
    view_dirs=VIEW_DIRS,
    proc_dirs=PROC_DIRS,
    schema_path=SCHEMA_PATH,
    out_path=OUT_PATH,
    limit=LIMIT,
)
